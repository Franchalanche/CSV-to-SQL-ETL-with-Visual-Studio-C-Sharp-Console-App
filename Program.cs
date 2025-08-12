using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.SqlClient;   // <-- this one


namespace ChewyEligibilityArchiver
{
    internal class Program
    {
        // === SETTINGS ===
        static readonly string SourceFolder = @"\\winhsqlelgblty1\EligibilityFiles\Chewy-EFiles\Archive";
        static readonly string[] Patterns = new[] { "*.csv", "*.txt" }; // add "*.tsv" if needed
        static readonly SearchOption Search = SearchOption.TopDirectoryOnly; // change to AllDirectories if needed
        static readonly string ConnectionString = "Server=WINHSQLDWPROD;Database=WorkBench;Integrated Security=True;Encrypt=True;TrustServerCertificate=True;";
        static readonly string DestinationTable = "WorkBench.dbo.Chewy_Eligibility_Archive_Staging";
        static readonly int BatchSize = 20000;
        static readonly int NotifyAfter = 50000;

        // Canonical 21-column order
        static readonly string[] Canonical = new[]
        {
            "MEMBER_ID","LAST NAME","FIRST NAME","MIDDLE NAME","DATE OF BIRTH","RELATIONSHIP_TO_EMP",
            "EMPLOYMENT_START_DATE","HEALTH_PLAN_ELIG_EFF_START_DATE","HEALTH_PLAN_ELIG_EFF_END_DATE",
            "ACTIVE_INDICATOR","ADDRESS_1","ADDRESS_2","CITY","STATE","ZIP",
            "PRIMARY_CONTACT_PHONE","SECONDARY_CONTACT_PHONE","PREFERRED_EMAIL_ADDRESS",
            "BUSINESS UNIT","HEALTH_PLAN","COMPANY_IDENTIFIER"
        };

        // Header normalization map -> canonical
        static readonly Dictionary<string, string> Map = new(StringComparer.OrdinalIgnoreCase)
        {
            ["member_id"] = "MEMBER_ID",
            ["member id"] = "MEMBER_ID",
            ["id number"] = "MEMBER_ID",
            ["last name"] = "LAST NAME",
            ["first name"] = "FIRST NAME",
            ["middle name"] = "MIDDLE NAME",
            ["date of birth"] = "DATE OF BIRTH",
            ["relationship to emp"] = "RELATIONSHIP_TO_EMP",
            ["employment start date"] = "EMPLOYMENT_START_DATE",
            ["health plan elig eff start date"] = "HEALTH_PLAN_ELIG_EFF_START_DATE",
            ["health_plan_ elig_eff_start_date"] = "HEALTH_PLAN_ELIG_EFF_START_DATE",
            ["health_plan_elig_eff_start_date"] = "HEALTH_PLAN_ELIG_EFF_START_DATE",
            ["health plan elig eff end date"] = "HEALTH_PLAN_ELIG_EFF_END_DATE",
            ["health_plan_ elig_eff_end_date"] = "HEALTH_PLAN_ELIG_EFF_END_DATE",
            ["health_plan_elig_eff_end_date"] = "HEALTH_PLAN_ELIG_EFF_END_DATE",
            ["active indicator"] = "ACTIVE_INDICATOR",
            ["address 1"] = "ADDRESS_1",
            ["address_1"] = "ADDRESS_1",
            ["address 2"] = "ADDRESS_2",
            ["address_2"] = "ADDRESS_2",
            ["city"] = "CITY",
            ["state"] = "STATE",
            ["zip"] = "ZIP",
            ["primary contact phone"] = "PRIMARY_CONTACT_PHONE",
            ["secondary contact phone"] = "SECONDARY_CONTACT_PHONE",
            ["preferred email address"] = "PREFERRED_EMAIL_ADDRESS",
            ["business unit"] = "BUSINESS UNIT",
            ["health plan"] = "HEALTH_PLAN",
            ["company identifier"] = "COMPANY_IDENTIFIER",
        };

        static void Main(string[] args)
        {
            var files = Patterns.SelectMany(p => Directory.EnumerateFiles(SourceFolder, p, Search)).ToList();
            Console.WriteLine($"Found {files.Count} files under {SourceFolder}");

            long totalRows = 0;
            foreach (var file in files)
            {
                try
                {
                    var rows = ProcessFile(file);
                    totalRows += rows;
                    Console.WriteLine($"  -> {Path.GetFileName(file)}: {rows:N0} row(s) loaded.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[WARN] {Path.GetFileName(file)} skipped: {ex.Message}");
                }
            }

            Console.WriteLine($"\nAll done. Total rows: {totalRows:N0}");
        }

        static long ProcessFile(string path)
        {
            // 1) Detect encoding (BOM-aware) and delimiter from header
            var enc = DetectEncoding(path);
            var (delimiter, header) = DetectDelimiterAndHeader(path, enc);
            if (string.IsNullOrWhiteSpace(header))
                throw new InvalidDataException("Empty or unreadable header.");

            // 2) Prepare DataTable and SqlBulkCopy
            using var conn = new SqlConnection(ConnectionString);
            conn.Open();

            using var bulk = new SqlBulkCopy(conn,
                SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.FireTriggers,
                null)
            {
                DestinationTableName = DestinationTable,
                BatchSize = BatchSize,
                NotifyAfter = NotifyAfter
            };

            var table = BuildDataTable();
            foreach (DataColumn col in table.Columns)
                bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
            bulk.SqlRowsCopied += (_, e) => Console.WriteLine($"   {Path.GetFileName(path)}: {e.RowsCopied:N0} rows copied...");

            // 3) Configure CsvHelper
            using var reader = new StreamReader(path, enc, detectEncodingFromByteOrderMarks: true);
            var cfg = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = delimiter,
                HasHeaderRecord = true,
                BadDataFound = null,
                MissingFieldFound = null,
                IgnoreBlankLines = true,
                TrimOptions = TrimOptions.Trim
            };

            using var csv = new CsvReader(reader, cfg);

            // Read header
            if (!csv.Read() || !csv.ReadHeader())
                throw new InvalidDataException("No header row present.");

            var origHeaders = csv.HeaderRecord ?? Array.Empty<string>();
            var indexMap = BuildHeaderMap(origHeaders);

            // 4) Stream rows → DataTable → SqlBulkCopy
            long rows = 0;
            while (csv.Read())
            {
                var reportDate = GetReportDateFromPath(path);  // <-- compute once
                var row = table.NewRow();

                foreach (var col in Canonical)
                {
                    if (col.Equals("COMPANY_IDENTIFIER", StringComparison.OrdinalIgnoreCase))
                    {
                        row[col] = "Chewy";
                        continue;
                    }

                    row["ReportDate"] = reportDate.HasValue ? reportDate.Value : (object)DBNull.Value;  // <-- NEW
                    row["SourceFile"] = Path.GetFileName(path);

                    if (indexMap.TryGetValue(col, out var idx) && idx >= 0)
                    {
                        var value = csv.GetField(idx);
                        row[col] = string.IsNullOrWhiteSpace(value) ? DBNull.Value : value;
                    }
                    else
                    {
                        // Old files missing BUSINESS UNIT → NULL
                        row[col] = DBNull.Value;
                    }

                }


                rows++;

                if (table.Rows.Count >= BatchSize)
                {
                    bulk.WriteToServer(table);
                    table.Clear();
                }
            }

            if (table.Rows.Count > 0)
            {
                bulk.WriteToServer(table);
                table.Clear();
            }

            return rows;
        }

        static (string Delimiter, string Header) DetectDelimiterAndHeader(string path, Encoding enc)
        {
            using var sr = new StreamReader(path, enc, true);
            var first = sr.ReadLine() ?? string.Empty;
            var delimiter = first.Contains('\t') ? "\t" : ",";
            return (delimiter, first);
        }
        static DateTime? GetReportDateFromPath(string path)
        {
            // Look for 8 consecutive digits near the end: yyyyMMdd
            // Examples: ChewyWIN_20230403.csv, TEST_ChewyWIN_20240828.txt, ChewyWIN_20250811_v2.csv
            var file = Path.GetFileName(path);
            var span = file.AsSpan();

            // Walk backwards to find the last 8-digit run
            for (int i = span.Length - 1; i >= 7; i--)
            {
                // quick check for 8 digits ending at i (exclusive of extension)
                if (!char.IsDigit(span[i - 0]) ||
                    !char.IsDigit(span[i - 1]) ||
                    !char.IsDigit(span[i - 2]) ||
                    !char.IsDigit(span[i - 3]) ||
                    !char.IsDigit(span[i - 4]) ||
                    !char.IsDigit(span[i - 5]) ||
                    !char.IsDigit(span[i - 6]) ||
                    !char.IsDigit(span[i - 7]))
                    continue;

                var slice = span.Slice(i - 7, 8);
                if (DateTime.TryParseExact(slice, "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture,
                                           System.Globalization.DateTimeStyles.None, out var dt))
                    return dt;
            }
            return null; // couldn’t parse
        }

        static Encoding DetectEncoding(string path)
        {
            using var fs = File.OpenRead(path);
            var bom = new byte[4];
            var n = fs.Read(bom, 0, 4);

            if (n >= 2)
            {
                if (bom[0] == 0xFF && bom[1] == 0xFE) return Encoding.Unicode;          // UTF-16 LE
                if (bom[0] == 0xFE && bom[1] == 0xFF) return Encoding.BigEndianUnicode; // UTF-16 BE
            }
            if (n >= 3 && bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF) return Encoding.UTF8; // UTF-8 BOM
            return Encoding.UTF8; // safe fallback
        }

        static DataTable BuildDataTable()
        {
            var dt = new DataTable();
            foreach (var c in Canonical) dt.Columns.Add(c, typeof(string));
            dt.Columns.Add("ReportDate", typeof(DateTime));  // <-- NEW
            dt.Columns.Add("SourceFile", typeof(string));
            return dt;
        }


        static Dictionary<string, int> BuildHeaderMap(string[] headersRaw)
        {
            // Normalize originals to lookups
            var normalized = headersRaw.Select(NormalizeKey).ToArray();

            // Map original index -> canonical
            var idxToCanonical = new Dictionary<int, string>();
            for (int i = 0; i < normalized.Length; i++)
            {
                var key = normalized[i];
                if (Map.TryGetValue(key, out var canonical))
                {
                    idxToCanonical[i] = canonical;
                }
                else
                {
                    // Accept direct canonical match (case-insensitive)
                    var maybe = Canonical.FirstOrDefault(c => c.Equals(headersRaw[i], StringComparison.OrdinalIgnoreCase));
                    if (maybe is not null) idxToCanonical[i] = maybe;
                }
            }

            // Build canonical -> source index map
            var result = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach (var c in Canonical)
            {
                var match = idxToCanonical.FirstOrDefault(p => p.Value.Equals(c, StringComparison.OrdinalIgnoreCase));
                result[c] = match.Equals(default(KeyValuePair<int, string>)) ? -1 : match.Key;
            }
            return result;
        }

        static string NormalizeKey(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return string.Empty;
            var sb = new StringBuilder(s.Length);
            var prevSpace = false;
            foreach (var ch in s)
            {
                var c = char.IsLetterOrDigit(ch) ? char.ToLowerInvariant(ch) : ' ';
                if (c == ' ')
                {
                    if (!prevSpace) sb.Append(' ');
                    prevSpace = true;
                }
                else
                {
                    sb.Append(c);
                    prevSpace = false;
                }
            }
            return sb.ToString().Trim();
        }
    }
}
