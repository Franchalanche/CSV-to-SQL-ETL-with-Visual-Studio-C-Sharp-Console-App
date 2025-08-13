using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.SqlClient;

namespace ChewyEligibilityArchiver
{
    internal class Program
    {
        // === SETTINGS ===
        static readonly string SourceFolder = @"\\winhsqlelgblty1\EligibilityFiles\Chewy-EFiles\Archive";
        static readonly string[] Patterns = new[] { "*.csv" }; // add "*.tsv" if needed
        static readonly SearchOption Search = SearchOption.TopDirectoryOnly; // or AllDirectories

        // SQL
        const string ConnectionString =
            "Server=WINHSQLDWPROD;Database=WorkBench;Integrated Security=True;Encrypt=True;TrustServerCertificate=True;";
        const string DestinationTable = "WorkBench.dbo.Chewy_Eligibility_Archive_Staging";

        // Bulk behavior - trying to avoid timeouts
        const int FlushThresholdRows = 250_000;  // smaller flushes
        const int BulkBatchSize = 50_000;   // smaller batches
        const int NotifyAfter = 50_000;

        // Toggle RowHash (for dedupe later). If your table doesn't have RowHash, leave true; it won't be mapped.
        const bool ComputeHash = true;

        // Canonical business columns (must match your logical schema)
        static readonly string[] Canonical = new[]
        {
            "MEMBER_ID","LAST NAME","FIRST NAME","MIDDLE NAME","DATE OF BIRTH","RELATIONSHIP_TO_EMP",
            "EMPLOYMENT_START_DATE","HEALTH_PLAN_ELIG_EFF_START_DATE","HEALTH_PLAN_ELIG_EFF_END_DATE",
            "ACTIVE_INDICATOR","ADDRESS_1","ADDRESS_2","CITY","STATE","ZIP",
            "PRIMARY_CONTACT_PHONE","SECONDARY_CONTACT_PHONE","PREFERRED_EMAIL_ADDRESS",
            "BUSINESS UNIT","HEALTH_PLAN","COMPANY_IDENTIFIER"
        };

        // Header variations -> canonical
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

        // Fields used to compute RowHash (exclude ReportDate/SourceFile/LoadDT)
        static readonly string[] FieldsForHash = Canonical;

        static void Main(string[] args)
        {
            var files = Patterns.SelectMany(p => Directory.EnumerateFiles(SourceFolder, p, Search)).ToList();
            Console.WriteLine($"Found {files.Count} files under {SourceFolder}");

            var accumulator = BuildDataTable();   // includes Canonical + ReportDate + SourceFile + RowHash
            long totalRows = 0;

            foreach (var file in files)
            {
                try
                {
                    var added = BufferFile(file, accumulator);
                    totalRows += added;
                    Console.WriteLine($"  -> {Path.GetFileName(file)}: {added:N0} row(s) buffered.");

                    if (accumulator.Rows.Count >= FlushThresholdRows)
                    {
                        using var conn = new SqlConnection(ConnectionString);
                        conn.Open();
                        FlushToSql(accumulator, conn);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[WARN] {Path.GetFileName(file)} skipped: {ex.Message}");
                }
            }

            if (accumulator.Rows.Count > 0)
            {
                using var conn = new SqlConnection(ConnectionString);
                conn.Open();
                FlushToSql(accumulator, conn);
            }

            Console.WriteLine($"\nAll done. Total rows buffered: {totalRows:N0}");
        }

        // --- Read a single file and append its rows to the accumulator ---
        static long BufferFile(string path, DataTable table)
        {
            var enc = DetectEncoding(path);
            var (delimiter, header) = DetectDelimiterAndHeader(path, enc);
            if (string.IsNullOrWhiteSpace(header))
                throw new InvalidDataException("Empty or unreadable header.");

            // Read with sharing (in case another proc has the file open)
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var reader = new StreamReader(fs, enc, detectEncodingFromByteOrderMarks: true);

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

            if (!csv.Read() || !csv.ReadHeader())
                throw new InvalidDataException("No header row present.");

            var indexMap = BuildHeaderMap(csv.HeaderRecord ?? Array.Empty<string>());

            var reportDate = GetReportDateFromPath(path);
            var sourceFile = Path.GetFileName(path);
            long rows = 0;

            while (csv.Read())
            {
                var row = table.NewRow();

                // Business fields
                foreach (var col in Canonical)
                {
                    if (col.Equals("COMPANY_IDENTIFIER", StringComparison.OrdinalIgnoreCase))
                    {
                        row[col] = "Chewy";
                        continue;
                    }

                    if (indexMap.TryGetValue(col, out var idx) && idx >= 0)
                    {
                        var value = csv.GetField(idx);
                        row[col] = string.IsNullOrWhiteSpace(value) ? DBNull.Value : value;
                    }
                    else
                    {
                        row[col] = DBNull.Value; // e.g., BUSINESS UNIT in old files
                    }
                }

                // Metadata
                row["ReportDate"] = reportDate.HasValue ? reportDate.Value : (object)DBNull.Value;
                row["SourceFile"] = sourceFile;

                // Optional hash for dedupe (if your dest table doesn't have RowHash, it just won't be mapped)
                row["RowHash"] = ComputeHash ? ComputeRowHash(row) : DBNull.Value;

                table.Rows.Add(row);
                rows++;
            }

            return rows;
        }


        // --- Bulk copy accumulator to SQL with safe, auto-generated mappings ---
        static void FlushToSql(DataTable table, SqlConnection conn)
        {
            if (table.Rows.Count == 0) return;

            // Build mappings from intersection (as in your current version)
            var destCols = GetDestinationColumns(conn, DestinationTable);
            var dtCols = table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
            var toMap = dtCols.Where(c => destCols.Contains(c, StringComparer.OrdinalIgnoreCase)).ToList();

            // Log any DT columns that won’t be mapped (absent in destination)
            var missingInDest = dtCols.Except(toMap, StringComparer.OrdinalIgnoreCase).ToList();
            if (missingInDest.Count > 0)
                Console.WriteLine($"[INFO] Not mapped (absent in destination): {string.Join(", ", missingInDest)}");

            // Lighter options for staging
            var options = SqlBulkCopyOptions.TableLock;

            int maxRetries = 2;
            for (int attempt = 1; attempt <= maxRetries + 1; attempt++)
            {
                try
                {
                    using var bulk = new SqlBulkCopy(conn, options, null)
                    {
                        DestinationTableName = DestinationTable,
                        BatchSize = BulkBatchSize,
                        NotifyAfter = NotifyAfter,
                        BulkCopyTimeout = 0 // infinite; or set to 600 for 10 min
                    };

                    foreach (var colName in toMap)
                        bulk.ColumnMappings.Add(colName, colName);

                    bulk.SqlRowsCopied += (_, e) =>
                        Console.WriteLine($"   Bulk copied {e.RowsCopied:N0} rows...");

                    bulk.WriteToServer(table);
                    Console.WriteLine($"Flushed {table.Rows.Count:N0} rows to {DestinationTable}");
                    table.Clear();
                    return; // success
                }
                catch (SqlException ex) when (IsTimeout(ex) && attempt <= maxRetries)
                {
                    int delayMs = 2000 * attempt;
                    Console.WriteLine($"[WARN] Bulk copy timeout (attempt {attempt}/{maxRetries}). Retrying in {delayMs} ms...");
                    System.Threading.Thread.Sleep(delayMs);
                    continue;
                }
                catch (Exception ex)
                {
                    // Don’t attribute this to the current file; this is a flush failure
                    Console.WriteLine($"[ERROR] Bulk copy failed: {ex.Message}");
                    throw;
                }
            }
        }

        static bool IsTimeout(SqlException ex)
        {
            //  -2 = timeout. Also match “Execution Timeout Expired” text just in case.
            return ex.Number == -2 || ex.Message.IndexOf("Execution Timeout Expired", StringComparison.OrdinalIgnoreCase) >= 0;
        }


        // --- Helpers: schema, encoding, header, hashing, etc. ---
        static HashSet<string> GetDestinationColumns(SqlConnection conn, string fullyQualifiedTable)
        {
            // Expect "db.schema.table" or "schema.table" or "table"
            // We'll query INFORMATION_SCHEMA for reliability
            var parts = fullyQualifiedTable.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            string db, schema, table;
            if (parts.Length == 3) { db = parts[0]; schema = parts[1]; table = parts[2]; }
            else if (parts.Length == 2) { db = conn.Database; schema = parts[0]; table = parts[1]; }
            else { db = conn.Database; schema = "dbo"; table = parts[0]; }

            var cmdText = $@"
SELECT COLUMN_NAME
FROM [{db}].INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table;";
            using var cmd = new SqlCommand(cmdText, conn);
            cmd.Parameters.AddWithValue("@schema", schema);
            cmd.Parameters.AddWithValue("@table", table);

            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            using var rdr = cmd.ExecuteReader();
            while (rdr.Read())
                set.Add(rdr.GetString(0));
            return set;
        }

        static (string Delimiter, string Header) DetectDelimiterAndHeader(string path, Encoding enc)
        {
            using var sr = new StreamReader(path, enc, true);
            var first = sr.ReadLine() ?? string.Empty;
            var delimiter = first.Contains('\t') ? "\t" : ",";
            return (delimiter, first);
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
            dt.Columns.Add("ReportDate", typeof(DateTime)); // optional, auto-mapped if exists
            dt.Columns.Add("SourceFile", typeof(string));   // optional, auto-mapped if exists
            dt.Columns.Add("RowHash", typeof(byte[]));      // optional, auto-mapped if exists
            return dt;
        }

        static Dictionary<string, int> BuildHeaderMap(string[] headersRaw)
        {
            var normalized = headersRaw.Select(NormalizeKey).ToArray();
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
                    var maybe = Canonical.FirstOrDefault(c => c.Equals(headersRaw[i], StringComparison.OrdinalIgnoreCase));
                    if (maybe is not null) idxToCanonical[i] = maybe;
                }
            }

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

        static DateTime? GetReportDateFromPath(string path)
        {
            var file = Path.GetFileName(path);
            var span = file.AsSpan();

            for (int i = span.Length - 1; i >= 7; i--)
            {
                if (!char.IsDigit(span[i - 0]) || !char.IsDigit(span[i - 1]) || !char.IsDigit(span[i - 2]) || !char.IsDigit(span[i - 3]) ||
                    !char.IsDigit(span[i - 4]) || !char.IsDigit(span[i - 5]) || !char.IsDigit(span[i - 6]) || !char.IsDigit(span[i - 7]))
                    continue;

                var slice = span.Slice(i - 7, 8);
                if (DateTime.TryParseExact(slice, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
                    return dt;
            }
            return null;
        }

        static byte[] ComputeRowHash(DataRow r)
        {
            if (!ComputeHash) return Array.Empty<byte>();
            var sb = new StringBuilder(1024);
            foreach (var c in FieldsForHash)
            {
                var v = r[c] == DBNull.Value ? "" : Convert.ToString(r[c])!;
                sb.Append(v.Trim().ToUpperInvariant()).Append('|');
            }
            using var sha = SHA256.Create();
            return sha.ComputeHash(Encoding.UTF8.GetBytes(sb.ToString()));
        }
    }
}
