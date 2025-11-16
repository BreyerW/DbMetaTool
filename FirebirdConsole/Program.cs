using FirebirdSql.Data.FirebirdClient;
using FirebirdSql.Data.Isql;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System.Diagnostics;
using System.Text;

namespace DbMetaTool
{
	public static class Program
	{
		// Przykładowe wywołania:
		// DbMetaTool build-db --db-dir "C:\db\fb5" --scripts-dir "C:\scripts"
		// DbMetaTool export-scripts --connection-string "..." --output-dir "C:\out"
		// DbMetaTool update-db --connection-string "..." --scripts-dir "C:\scripts"
		public static int Main(string[] args)
		{
			if (args.Length == 0)
			{
				Console.WriteLine("Użycie:");
				Console.WriteLine("  build-db --db-dir <ścieżka> --scripts-dir <ścieżka>");
				Console.WriteLine("  export-scripts --connection-string <connStr> --output-dir <ścieżka>");
				Console.WriteLine("  update-db --connection-string <connStr> --scripts-dir <ścieżka>");
				return 1;
			}

			try
			{
				var command = args[0].ToLowerInvariant();

				switch (command)
				{
					case "build-db":
					{
						string dbDir = GetArgValue(args, "--db-dir");
						string scriptsDir = GetArgValue(args, "--scripts-dir");

						BuildDatabase(dbDir, scriptsDir);
						Console.WriteLine("Baza danych została zbudowana pomyślnie.");
						return 0;
					}

					case "export-scripts":
					{
						string connStr = GetArgValue(args, "--connection-string");
						string outputDir = GetArgValue(args, "--output-dir");

						ExportScripts(connStr, outputDir);
						Console.WriteLine("Skrypty zostały wyeksportowane pomyślnie.");
						return 0;
					}

					case "update-db":
					{
						string connStr = GetArgValue(args, "--connection-string");
						string scriptsDir = GetArgValue(args, "--scripts-dir");

						UpdateDatabase(connStr, scriptsDir);
						Console.WriteLine("Baza danych została zaktualizowana pomyślnie.");
						return 0;
					}

					default:
						Console.WriteLine($"Nieznane polecenie: {command}");
						return 1;
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine("Błąd: " + ex.Message);
				return -1;
			}
		}

		private static string GetArgValue(string[] args, string name)
		{
			int idx = Array.IndexOf(args, name);
			if (idx == -1 || idx + 1 >= args.Length)
				throw new ArgumentException($"Brak wymaganego parametru {name}");
			return args[idx + 1];
		}

		/// <summary>
		/// Buduje nową bazę danych Firebird 5.0 na podstawie skryptów.
		/// Tworzy plik bazy (jeśli nie istnieje) używając narzędzia isql tylko dla stworzenia pliku bazy,
		/// resztę wykonuje przy pomocy EF Core (otwarcie połączenia i wykonanie skryptów).
		/// W razie błędu następuje rollback.
		/// </summary>
		public static void BuildDatabase(string databaseDirectory, string scriptsDirectory)
		{
			if (string.IsNullOrWhiteSpace(databaseDirectory))
				throw new ArgumentException("databaseDirectory is required", nameof(databaseDirectory));
			if (string.IsNullOrWhiteSpace(scriptsDirectory))
				throw new ArgumentException("scriptsDirectory is required", nameof(scriptsDirectory));
			if (!Directory.Exists(scriptsDirectory))
				throw new DirectoryNotFoundException($"Katalog skryptów nie istnieje: {scriptsDirectory}");

			// Decide DB file path: if databaseDirectory looks like a file (.fdb) use it, otherwise create file inside the directory
			string dbFile;
			if (Path.HasExtension(databaseDirectory) && Path.GetExtension(databaseDirectory).Equals(".fdb", StringComparison.OrdinalIgnoreCase))
			{
				dbFile = databaseDirectory;
			}
			else
			{
				Directory.CreateDirectory(databaseDirectory);
				dbFile = Path.Combine(databaseDirectory, "app.fdb");
			}

			Console.WriteLine("[1/3] Przygotowanie pliku bazy: " + dbFile);

			// Create database file if missing using isql (via ProcessStartInfo)
			if (!File.Exists(dbFile))
			{
				Console.WriteLine("Tworzenie pliku bazy danych przy użyciu isql...");

				// isql path can be overridden via ISQL_PATH env var
				var isqlPath = Environment.GetEnvironmentVariable("ISQL_PATH") ?? "C:\\Program Files\\Firebird\\Firebird_5_0\\isql.exe";

				// Firebird server host to prefix the database path in CREATE DATABASE statement.
				var host = Environment.GetEnvironmentVariable("FB_HOST") ?? "localhost";

				// Credentials for DB creation (can be changed to suit environment)
				var sysUser = Environment.GetEnvironmentVariable("FB_SYS_USER") ?? "localuser";
				var sysPass = Environment.GetEnvironmentVariable("FB_SYS_PASS") ?? "masterkey";

				// Build CREATE DATABASE statement
				var createSql = $"create user {sysUser} password '{sysPass}';{Environment.NewLine}commit;{Environment.NewLine}CREATE DATABASE '{dbFile}' page_size 8192;{Environment.NewLine}QUIT;{Environment.NewLine}";

				var psi = new ProcessStartInfo
				{
					FileName = isqlPath,
					Arguments = "-q",
					RedirectStandardInput = true,
					RedirectStandardOutput = true,
					RedirectStandardError = true,
					UseShellExecute = false,
					CreateNoWindow = true
				};

				Console.WriteLine($"Uruchamiam isql: {isqlPath}");

				try
				{
					using var proc = Process.Start(psi) ?? throw new InvalidOperationException("Nie można uruchomić isql procesu.");
					// write create statement to stdin
					proc.StandardInput.Write(createSql);
					proc.StandardInput.Flush();
					proc.StandardInput.Close();

					// read outputs
					var stdOut = proc.StandardOutput.ReadToEnd();
					var stdErr = proc.StandardError.ReadToEnd();

					proc.WaitForExit();

					Console.WriteLine("[isql output]");
					if (!string.IsNullOrWhiteSpace(stdOut))
						Console.WriteLine(stdOut);
					if (!string.IsNullOrWhiteSpace(stdErr))
						Console.WriteLine("[isql error output]" + Environment.NewLine + stdErr);

					if (proc.ExitCode != 0)
					{
						throw new Exception($"isql zakończył się kodem {proc.ExitCode}. Nie utworzono bazy.");
					}

					// verify file created
					if (!File.Exists(dbFile))
					{
						// Some server configurations place DB elsewhere or require different permissions.
						throw new FileNotFoundException("Nie znaleziono pliku bazy po wykonaniu isql. Sprawdź konfigurację serwera i uprawnienia.", dbFile);
					}

					Console.WriteLine("Plik bazy został utworzony przez isql.");
				}
				catch (Exception ex)
				{
					Console.WriteLine("Błąd tworzenia bazy danych przy użyciu isql: " + ex.Message);
					Console.WriteLine("Sugestia: upewnij się, że isql jest dostępny w PATH lub ustaw zmienną środowiskową ISQL_PATH.\n" +
									  "Również sprawdź, czy serwer Firebird działa i akceptuje połączenia z hosta (domyślnie 'localhost').");
					throw;
				}
			}
			else
			{
				Console.WriteLine("Plik bazy istnieje i zostanie użyty.");
			}

			// Build EF Core connection string to the created DB.
			var efConnBuilder = new FbConnectionStringBuilder
			{
				Database = dbFile,
				DataSource = Environment.GetEnvironmentVariable("FB_HOST") ?? "localhost",
				UserID = Environment.GetEnvironmentVariable("FB_SYS_USER") ?? "localuser",
				Password = Environment.GetEnvironmentVariable("FB_SYS_PASS") ?? "masterkey",
				Dialect = 3,
				ServerType = FbServerType.Default,
				Charset = "UTF8"
			};
			string efConnectionString = efConnBuilder.ToString();

			// Avoid printing secret in logs; show masked version for diagnostics
			Console.WriteLine("Debug: efConnectionString: " + efConnectionString.Replace(efConnBuilder.Password, "******"));
			Console.WriteLine("[2/3] Otwieranie połączenia EF Core...");

			var optionsBuilder = new DbContextOptionsBuilder<MetadataContext>();
			optionsBuilder.UseFirebird(efConnectionString);

			using var ctx = new MetadataContext(optionsBuilder.Options);
			try
			{
				ctx.Database.OpenConnection();
				using var transaction = ctx.Database.BeginTransaction();
				var conn = ctx.Database.GetDbConnection();

				// cast provider connection to FbConnection for FbScript usage
				if (!(conn is FbConnection fbConn))
					throw new InvalidOperationException("Provider connection is not a Firebird connection (FbConnection).");

				Console.WriteLine("[3/3] Wykonywanie skryptów z katalogu: " + scriptsDirectory);

				var files = Directory.GetFiles(scriptsDirectory, "*.sql").OrderBy(Path.GetFileName).ToList();
				if (files.Count == 0)
				{
					Console.WriteLine("Brak plików .sql w katalogu skryptów.");
				}

				// get underlying FbTransaction to attach to FbCommand (if available)
				FbTransaction fbTx = null;
				try
				{
					var dbTx = transaction.GetDbTransaction();
					if (dbTx is FbTransaction t) fbTx = t;
				}
				catch
				{
					// ignore
				}

				foreach (var file in files)
				{
					Console.WriteLine($"Wykonywanie: {Path.GetFileName(file)}");
					var content = File.ReadAllText(file, Encoding.UTF8);
					if (string.IsNullOrWhiteSpace(content))
					{
						Console.WriteLine("  (pusty plik - pominięto)");
						continue;
					}

					try
					{
						// Use FbScript to properly split and handle SET TERM and procedure blocks
						var script = new FbScript(content);
						script.Parse();

						foreach (var stmt in script.Results)
						{
							var sql = (stmt.Text ?? string.Empty).Trim();
							if (string.IsNullOrEmpty(sql))
								continue;

							using var fbCmd = new FbCommand(sql, fbConn);
							if (fbTx != null)
								fbCmd.Transaction = fbTx;
							fbCmd.CommandTimeout = 0;
							fbCmd.ExecuteNonQuery();
						}

						Console.WriteLine("  OK");
					}
					catch (Exception exFile)
					{
						Console.WriteLine($"Błąd podczas wykonywania pliku {Path.GetFileName(file)}: {exFile.Message}");
						try
						{
							transaction.Rollback();
							Console.WriteLine("Transakcja cofnięta z powodu błędu.");
						}
						catch (Exception rbEx)
						{
							Console.WriteLine("Błąd podczas cofania transakcji: " + rbEx.Message);
						}
						throw;
					}
				}

				transaction.Commit();
				Console.WriteLine("Wszystkie skrypty wykonane. Transakcja zatwierdzona.");
			}
			catch (Exception ex)
			{
				Console.WriteLine("BuildDatabase nieudane: " + ex.Message);
				throw;
			}
			finally
			{
				try { ctx.Database.CloseConnection(); } catch { }
			}

			// Local helpers
			static string EscapeArg(string value)
			{
				// simple escape for args without quotes inside
				if (string.IsNullOrEmpty(value)) return "\"\"";
				return value.Contains(' ') ? $"\"{value}\"" : value;
			}
		}

		/// <summary>
		/// Generuje skrypty metadanych z istniejącej bazy danych Firebird 5.0.
		/// Uses EF Core DbContext to obtain a provider connection, then queries Firebird system tables
		/// to export simple DDL for domains, tables (with columns) and stored procedures.
		/// </summary>
		public static void ExportScripts(string connectionString, string outputDirectory)
		{
			if (string.IsNullOrWhiteSpace(connectionString))
				throw new ArgumentException("connectionString is required", nameof(connectionString));
			if (string.IsNullOrWhiteSpace(outputDirectory))
				throw new ArgumentException("outputDirectory is required", nameof(outputDirectory));

			Directory.CreateDirectory(outputDirectory);

			var optionsBuilder = new DbContextOptionsBuilder<MetadataContext>();
			optionsBuilder.UseFirebird(connectionString);

			using var ctx = new MetadataContext(optionsBuilder.Options);
			try
			{
				Console.WriteLine("Eksport: otwieranie połączenia...");
				ctx.Database.OpenConnection();
				var conn = ctx.Database.GetDbConnection();

				var combined = new StringBuilder();

				// --- DOMAINS ---
				// Export domains, but include those system-named (RDB$...) if they are actually used by table columns.
				Console.WriteLine("Eksport: domeny...");
				var domainsSql = new StringBuilder();
				using (var cmd = conn.CreateCommand())
				{
					cmd.CommandText = @"
SELECT 
  rf.rdb$field_name AS fld_name,
  rf.rdb$field_type AS fld_type,
  rf.rdb$field_sub_type AS fld_sub_type,
  rf.rdb$field_length AS fld_length,
  rf.rdb$character_length AS c_length,
  rf.rdb$field_precision AS precise,
  rf.rdb$field_scale AS fld_scale,
  rf.rdb$default_source AS default_src,
  rf.rdb$character_set_id AS charset_id
FROM RDB$FIELDS rf
-- do not export system-named RDB$... fields
WHERE rf.rdb$field_name NOT STARTING WITH 'RDB$'
  AND (rf.rdb$system_flag = 0 OR rf.rdb$system_flag IS NULL)
ORDER BY rf.rdb$field_name";
					using var rdr = cmd.ExecuteReader();
					while (rdr.Read())
					{
						string rawName = TrimFb(rdr["fld_name"] as string);
						if (string.IsNullOrEmpty(rawName))
							continue;

						try
						{
							int fldType = Convert.ToInt32(rdr["fld_type"] ?? 0);
							int fldSubType = rdr["fld_sub_type"] == DBNull.Value ? 0 : Convert.ToInt32(rdr["fld_sub_type"]);
							int fldLength = rdr["fld_length"] == DBNull.Value ? 0 : Convert.ToInt32(rdr["fld_length"]);
							int charLen = rdr["c_length"] == DBNull.Value ? 0 : Convert.ToInt32(rdr["c_length"]);
							int precision = rdr["precise"] == DBNull.Value ? 0 : Convert.ToInt32(rdr["precise"]);
							int scale = rdr["fld_scale"] == DBNull.Value ? 0 : Convert.ToInt32(rdr["fld_scale"]);
							string defaultSrc = TrimFb(rdr["default_src"] as string);

							string domainName = rawName;
							string typeSql = MapFirebirdType(fldType, fldSubType, fldLength, charLen, precision, scale);

							var line = new StringBuilder();
							line.Append("-- Domain: ");
							line.Append(EscapeIdent(domainName));
							line.AppendLine();
							line.Append("CREATE DOMAIN ");
							line.Append(EscapeIdent(domainName));
							line.Append(" AS ");
							line.Append(typeSql);

							if (!string.IsNullOrWhiteSpace(defaultSrc))
							{
								var ds = defaultSrc.Trim();
								if (!ds.StartsWith("DEFAULT", StringComparison.OrdinalIgnoreCase))
									ds = " DEFAULT " + ds;
								line.Append(" ");
								line.Append(ds);
							}

							line.Append(";");
							domainsSql.AppendLine(line.ToString());
						}
						catch (Exception ex)
						{
							Console.WriteLine($"Warning exporting domain '{rawName}': {ex.Message}");
						}
					}
				}

				combined.AppendLine("-- =================================================");
				combined.AppendLine("-- DOMAINS");
				combined.AppendLine("-- =================================================");
				combined.AppendLine(domainsSql.ToString());

				// --- EXCEPTIONS (Firebird) ---
				Console.WriteLine("Eksport: exceptions...");
				var exceptionsSql = new StringBuilder();
				using (var cmd = conn.CreateCommand())
				{
					// user-created exceptions: exclude system names starting with RDB$
					cmd.CommandText = @"
SELECT rdb$exception_name AS exc_name, rdb$message AS message
FROM rdb$exceptions
WHERE rdb$exception_name NOT STARTING WITH 'RDB$'
ORDER BY rdb$exception_name";
					using var rdr = cmd.ExecuteReader();
					while (rdr.Read())
					{
						string rawName = TrimFb(rdr["exc_name"] as string);
						if (string.IsNullOrEmpty(rawName))
							continue;

						try
						{
							string message = TrimFb(rdr["message"] as string) ?? string.Empty;
							if (string.IsNullOrWhiteSpace(message))
							{
								// Do not export exceptions with empty messages (not useful)
								continue;
							}

							// escape single quotes for SQL literal
							var escaped = message.Replace("'", "''");

							exceptionsSql.AppendLine("-- Exception: " + EscapeIdent(rawName));
							exceptionsSql.Append("CREATE EXCEPTION ");
							exceptionsSql.Append(EscapeIdent(rawName));
							exceptionsSql.Append(" '");
							exceptionsSql.Append(escaped);
							exceptionsSql.AppendLine("';");
						}
						catch (Exception ex)
						{
							// report only to console; do not persist non-firebird exception details
							Console.WriteLine($"Warning exporting exception '{rawName}': {ex.Message}");
						}
					}
				}

				combined.AppendLine("-- =================================================");
				combined.AppendLine("-- EXCEPTIONS");
				combined.AppendLine("-- =================================================");
				combined.AppendLine(exceptionsSql.ToString());

				// --- TABLES ---
				Console.WriteLine("Eksport: tabele i kolumny...");
				var tablesSql = new StringBuilder();

				using (var cmd = conn.CreateCommand())
				{
					cmd.CommandText = @"
SELECT rdb$relation_name AS rel_name
FROM rdb$relations
WHERE (rdb$view_blr IS NULL) AND (rdb$system_flag = 0 OR rdb$system_flag IS NULL)
ORDER BY rdb$relation_name";
					using var rdr = cmd.ExecuteReader();
					var tableNames = new List<string>();
					while (rdr.Read())
					{
						tableNames.Add(TrimFb(rdr["rel_name"] as string));
					}

					foreach (var table in tableNames)
					{
						try
						{
							var colDefinitions = new List<string>();

							using var cmdCols = conn.CreateCommand();
							cmdCols.CommandText = @"
SELECT rf.rdb$field_name AS fld_name,
       rf.rdb$field_type AS fld_type,
       rf.rdb$field_sub_type AS fld_sub_type,
       rf.rdb$field_length AS fld_length,
       rf.rdb$character_length AS c_length,
       rf.rdb$field_precision AS precise,
       rf.rdb$field_scale AS fld_scale,
       rf.rdb$default_source AS default_src,
       rfs.rdb$field_source AS field_source,
       rfs.rdb$field_position AS field_pos,
       rfs.rdb$field_name AS column_name
FROM rdb$relation_fields rfs
LEFT JOIN rdb$fields rf ON rf.rdb$field_name = rfs.rdb$field_source
WHERE rfs.rdb$relation_name = @table
ORDER BY rfs.rdb$field_position";

							var p = cmdCols.CreateParameter();
							p.ParameterName = "@table";
							p.Value = table;
							cmdCols.Parameters.Add(p);

							using var rdrCols = cmdCols.ExecuteReader();
							while (rdrCols.Read())
							{
								string columnName = TrimFb(rdrCols["column_name"] as string);
								string fieldSource = TrimFb(rdrCols["field_source"] as string);
								int fldType = rdrCols["fld_type"] == DBNull.Value ? 0 : Convert.ToInt32(rdrCols["fld_type"]);
								int fldSubType = rdrCols["fld_sub_type"] == DBNull.Value ? 0 : Convert.ToInt32(rdrCols["fld_sub_type"]);
								int fldLength = rdrCols["fld_length"] == DBNull.Value ? 0 : Convert.ToInt32(rdrCols["fld_length"]);
								int charLen = rdrCols["c_length"] == DBNull.Value ? 0 : Convert.ToInt32(rdrCols["c_length"]);
								int precision = rdrCols["precise"] == DBNull.Value ? 0 : Convert.ToInt32(rdrCols["precise"]);
								int scale = rdrCols["fld_scale"] == DBNull.Value ? 0 : Convert.ToInt32(rdrCols["fld_scale"]);
								string defaultSrc = TrimFb(rdrCols["default_src"] as string);

								string colTypeSql;

								// If the column references a field_source that is a user domain (not RDB$...),
								// emit the domain name. If the field_source is system-generated (starts with RDB$)
								// or missing, fall back to concrete mapping via rf.*.
								if (!string.IsNullOrEmpty(fieldSource) && !fieldSource.Equals(columnName, StringComparison.OrdinalIgnoreCase))
								{
									if (fieldSource.StartsWith("RDB$", StringComparison.OrdinalIgnoreCase))
									{
										// system-generated descriptor -> map to concrete SQL type
										colTypeSql = MapFirebirdType(fldType, fldSubType, fldLength, charLen, precision, scale);
									}
									else
									{
										// user-created domain name -> reference the domain
										colTypeSql = EscapeIdent(fieldSource);
									}
								}
								else
								{
									// no field_source (or same as column name) -> concrete mapping
									colTypeSql = MapFirebirdType(fldType, fldSubType, fldLength, charLen, precision, scale);
								}

								var colDef = new StringBuilder();
								colDef.Append(EscapeIdent(columnName));
								colDef.Append(" ");
								colDef.Append(colTypeSql);

								if (!string.IsNullOrWhiteSpace(defaultSrc))
								{
									var ds = defaultSrc.Trim();
									if (!ds.StartsWith("DEFAULT", StringComparison.OrdinalIgnoreCase))
										ds = " DEFAULT " + ds;
									colDef.Append(" ");
									colDef.Append(ds);
								}

								colDefinitions.Add(colDef.ToString());
							}

							// attempt to detect primary key columns
							var pkCols = new List<string>();
							using (var cmdPk = conn.CreateCommand())
							{
								cmdPk.CommandText = @"
SELECT seg.rdb$field_name as col_name
FROM rdb$indices idx
JOIN rdb$index_segments seg ON seg.rdb$index_name = idx.rdb$index_name
JOIN rdb$relation_constraints rc ON rc.rdb$index_name = idx.rdb$index_name
WHERE rc.rdb$relation_name = @table AND rc.rdb$constraint_type = 'PRIMARY KEY'
ORDER BY seg.rdb$field_position";
								var pp = cmdPk.CreateParameter();
								pp.ParameterName = "@table";
								pp.Value = table;
								cmdPk.Parameters.Add(pp);

								using var rdrPk = cmdPk.ExecuteReader();
								while (rdrPk.Read())
								{
									pkCols.Add(TrimFb(rdrPk["col_name"] as string));
								}
							}

							var createTable = new StringBuilder();
							createTable.AppendLine("-- Table: " + EscapeIdent(table));
							createTable.Append("CREATE TABLE ");
							createTable.Append(EscapeIdent(table));
							createTable.AppendLine(" (");
							createTable.AppendLine("  " + string.Join("," + Environment.NewLine + "  ", colDefinitions));

							/*if (pkCols.Count > 0)
							{
								createTable.AppendLine(",");
								createTable.AppendLine("  CONSTRAINT " + EscapeIdent(table + "_PK") + " PRIMARY KEY (" + string.Join(", ", pkCols.Select(EscapeIdent)) + ")");
							}*/

							createTable.AppendLine(");");
							tablesSql.AppendLine(createTable.ToString());
						}
						catch (Exception ex)
						{
							Console.WriteLine($"Warning exporting table '{table}': {ex.Message}");
						}
					}
				}

				combined.AppendLine("-- =================================================");
				combined.AppendLine("-- TABLES");
				combined.AppendLine("-- =================================================");
				combined.AppendLine(tablesSql.ToString());

				// --- PROCEDURES (unchanged, wrapped with SET TERM) ---
				Console.WriteLine("Eksport: procedury...");
				var procsSql = new StringBuilder();
				var procBlocks = new StringBuilder();

				using (var cmd = conn.CreateCommand())
				{
					cmd.CommandText = @"
SELECT p.rdb$procedure_name AS proc_name, p.rdb$procedure_source AS source_text
FROM rdb$procedures p
WHERE p.rdb$system_flag = 0 OR p.rdb$system_flag IS NULL
ORDER BY p.rdb$procedure_name";
					using var rdr = cmd.ExecuteReader();
					while (rdr.Read())
					{
						string procName = TrimFb(rdr["proc_name"] as string);

						string source = null;
						var srcObj = rdr["source_text"];
						if (srcObj != DBNull.Value && srcObj != null)
						{
							if (srcObj is string s)
								source = s;
							else if (srcObj is byte[] b)
								source = Encoding.UTF8.GetString(b).TrimEnd('\0');
							else
								source = srcObj.ToString();
						}

						List<string> inParams = null;
						List<string> outParams = null;

						try
						{
							inParams = new List<string>();
							outParams = new List<string>();

							using var cmdParams = conn.CreateCommand();
							cmdParams.CommandText = @"
SELECT pp.rdb$parameter_name AS param_name,
       pp.rdb$parameter_type AS param_type,
       pp.rdb$field_source AS field_source,
       rf.rdb$field_type AS fld_type,
       rf.rdb$field_sub_type AS fld_sub_type,
       rf.rdb$field_length AS fld_length,
       rf.rdb$character_length AS c_length,
       rf.rdb$field_precision AS precise,
       rf.rdb$field_scale AS fld_scale
FROM rdb$procedure_parameters pp
LEFT JOIN rdb$fields rf ON rf.rdb$field_name = pp.rdb$field_source
WHERE pp.rdb$procedure_name = @proc
ORDER BY pp.rdb$parameter_number";
							var par = cmdParams.CreateParameter();
							par.ParameterName = "@proc";
							par.Value = procName;
							cmdParams.Parameters.Add(par);

							using var rdrParams = cmdParams.ExecuteReader();
							while (rdrParams.Read())
							{
								string paramName = TrimFb(rdrParams["param_name"] as string);
								int paramType = rdrParams["param_type"] == DBNull.Value ? 0 : Convert.ToInt32(rdrParams["param_type"]);
								string fieldSource = TrimFb(rdrParams["field_source"] as string);

								int fldType = rdrParams["fld_type"] == DBNull.Value ? 0 : Convert.ToInt32(rdrParams["fld_type"]);
								int fldSubType = rdrParams["fld_sub_type"] == DBNull.Value ? 0 : Convert.ToInt32(rdrParams["fld_sub_type"]);
								int fldLength = rdrParams["fld_length"] == DBNull.Value ? 0 : Convert.ToInt32(rdrParams["fld_length"]);
								int charLen = rdrParams["c_length"] == DBNull.Value ? 0 : Convert.ToInt32(rdrParams["c_length"]);
								int precision = rdrParams["precise"] == DBNull.Value ? 0 : Convert.ToInt32(rdrParams["precise"]);
								int scale = rdrParams["fld_scale"] == DBNull.Value ? 0 : Convert.ToInt32(rdrParams["fld_scale"]);

								string typeSql;

								// If a field_source is present and is a user-defined domain name (not starting with RDB$)
								// emit the domain name. If field_source starts with RDB$ treat it as a generated descriptor
								// and map to a concrete SQL type using rf.* columns. If no field_source, fallback to mapping.
								if (!string.IsNullOrEmpty(fieldSource) && !fieldSource.Equals(paramName, StringComparison.OrdinalIgnoreCase))
								{
									if (fieldSource.StartsWith("RDB$", StringComparison.OrdinalIgnoreCase))
									{
										// system/generated descriptor -> use concrete mapping if available
										if (fldType > 0)
											typeSql = MapFirebirdType(fldType, fldSubType, fldLength, charLen, precision, scale);
										else
											typeSql = "VARCHAR(255) CHARACTER SET UTF8";
									}
									else
									{
										// user domain -> reference it
										typeSql = EscapeIdent(fieldSource);
									}
								}
								else if (fldType > 0)
								{
									typeSql = MapFirebirdType(fldType, fldSubType, fldLength, charLen, precision, scale);
								}
								else
								{
									typeSql = "VARCHAR(255) CHARACTER SET UTF8";
								}

								var decl = $"{EscapeIdent(paramName)} {typeSql}";
								if (paramType == 1)
									outParams.Add(decl);
								else
									inParams.Add(decl);
							}
						}
						catch (Exception ex)
						{
							Console.WriteLine($"Warning exporting procedure params for '{procName}': {ex.Message}");
						}

						if (!string.IsNullOrEmpty(source))
						{
							var trimmed = source.Trim();
							string block;
							if (trimmed.StartsWith("CREATE", StringComparison.OrdinalIgnoreCase) ||
								trimmed.StartsWith("ALTER", StringComparison.OrdinalIgnoreCase))
							{
								block = trimmed;
							}
							else
							{
								var sb = new StringBuilder();
								sb.Append("CREATE PROCEDURE ");
								sb.Append(EscapeIdent(procName));

								if (inParams != null && inParams.Count > 0)
								{
									sb.AppendLine();
									sb.AppendLine("(");
									for (int i = 0; i < inParams.Count; i++)
									{
										sb.Append("  " + inParams[i] + (i + 1 < inParams.Count ? "," : ""));
										sb.AppendLine();
									}
									sb.AppendLine(")");
								}

								if (outParams != null && outParams.Count > 0)
								{
									sb.AppendLine("RETURNS");
									sb.AppendLine("(");
									for (int i = 0; i < outParams.Count; i++)
									{
										sb.Append("  " + outParams[i] + (i + 1 < outParams.Count ? "," : ""));
										sb.AppendLine();
									}
									sb.AppendLine(")");
								}

								sb.AppendLine("AS");
								sb.AppendLine(trimmed);
								if (!trimmed.TrimEnd().EndsWith(";"))
									sb.AppendLine(";");
								block = sb.ToString();
							}

							int lastSemi = block.LastIndexOf(';');
							if (lastSemi >= 0)
								block = block.Substring(0, lastSemi) + "^";
							else
								block = block + "^";

							procBlocks.AppendLine("-- Procedure: " + EscapeIdent(procName));
							procBlocks.AppendLine(block);
							procBlocks.AppendLine();
						}
					}
				}

				if (procBlocks.Length > 0)
				{
					procsSql.AppendLine("SET TERM ^ ;");
					procsSql.Append(procBlocks.ToString());
					procsSql.AppendLine("SET TERM ; ^");
				}

				combined.AppendLine("-- =================================================");
				combined.AppendLine("-- PROCEDURES");
				combined.AppendLine("-- =================================================");
				combined.AppendLine(procsSql.ToString());

				// write single output file
				var outPath = Path.Combine(outputDirectory, "metadata.sql");
				File.WriteAllText(outPath, combined.ToString(), Encoding.UTF8);
				Console.WriteLine("Eksport metadanych zakończony: " + outPath);
			}
			catch (Exception ex)
			{
				Console.WriteLine("Eksport nieudany: " + ex.Message);
				throw;
			}
			finally
			{
				try { ctx.Database.CloseConnection(); } catch { }
			}
		}

		/// <summary>
		/// Aktualizuje istniejącą bazę danych Firebird 5.0 na podstawie skryptów.
		/// Executes each .sql file's content directly (no parsing) inside a single transaction.
		/// Any error causes rollback.
		/// </summary>
		public static void UpdateDatabase(string connectionString, string scriptsDirectory)
		{
			if (string.IsNullOrWhiteSpace(connectionString))
				throw new ArgumentException("connectionString is required", nameof(connectionString));
			if (string.IsNullOrWhiteSpace(scriptsDirectory))
				throw new ArgumentException("scriptsDirectory is required", nameof(scriptsDirectory));
			if (!Directory.Exists(scriptsDirectory))
				throw new DirectoryNotFoundException($"Scripts directory not found: {scriptsDirectory}");

			var optionsBuilder = new DbContextOptionsBuilder<MetadataContext>();
			optionsBuilder.UseFirebird(connectionString);

			using var ctx = new MetadataContext(optionsBuilder.Options);
			try
			{
				ctx.Database.OpenConnection();
				using var transaction = ctx.Database.BeginTransaction();
				var conn = ctx.Database.GetDbConnection();

				// cast provider connection to FbConnection for FbScript usage
				if (!(conn is FbConnection fbConn))
					throw new InvalidOperationException("Provider connection is not a Firebird connection (FbConnection).");

				// get underlying FbTransaction to attach to FbCommand (if available)
				FbTransaction fbTx = null;
				try
				{
					var dbTx = transaction.GetDbTransaction();
					if (dbTx is FbTransaction t) fbTx = t;
				}
				catch
				{
					// ignore
				}

				// Execute each .sql file content using FbScript
				var files = Directory.GetFiles(scriptsDirectory, "*.sql").OrderBy(Path.GetFileName).ToList();
				if (files.Count == 0)
				{
					Console.WriteLine("Brak plików .sql w folderze.");
				}

				foreach (var file in files)
				{
					Console.WriteLine($"Wykonywanie pliku: {Path.GetFileName(file)}");
					var content = File.ReadAllText(file, Encoding.UTF8);
					if (string.IsNullOrWhiteSpace(content))
					{
						Console.WriteLine("(pusty plik, pominięto)");
						continue;
					}

					try
					{
						var script = new FbScript(content);
						script.Parse();
						foreach (var stmt in script  .Results)
						{
							var sql = (stmt.Text ?? string.Empty).Trim();
							if (string.IsNullOrEmpty(sql))
								continue;
							using var fbCmd = new FbCommand(sql, fbConn);
							if (fbTx != null)
								fbCmd.Transaction = fbTx;
							fbCmd.CommandTimeout = 0;
							fbCmd.ExecuteNonQuery();
						}

						Console.WriteLine("Plik wykonany poprawnie");
					}
					catch (Exception ex)
					{
						Console.WriteLine("Błąd podczas wykonywania skryptu: " + ex.Message);
						try
						{
							transaction.Rollback();
							Console.WriteLine("Transakcja cofnięta.");
						}
						catch (Exception rbEx)
						{
							Console.WriteLine("Rollback nieudany: " + rbEx.Message);
						}
						throw;
					}
				}

				transaction.Commit();
				Console.WriteLine("UpdateDatabase: transakcja zapisana.");
			}
			catch (Exception ex)
			{
				Console.WriteLine("UpdateDatabase nieudany: " + ex.Message);
				// attempt rollback if transaction active
				try
				{
					// If BeginTransaction returned a transaction, try rollback:
					ctx.Database.RollbackTransaction();
					Console.WriteLine("UpdateDatabase: transakcja cofnięta.");
				}
				catch (Exception rbEx)
				{
					Console.WriteLine("Rollback nieudany: " + rbEx.Message);
				}
				throw;
			}
			finally
			{
				try { ctx.Database.CloseConnection(); } catch { }
			}
		}

		// ---------- helper types / methods ----------
		private class MetadataContext : DbContext
		{
			public MetadataContext(DbContextOptions options) : base(options) { }
		}

		private static string TrimFb(string s)
		{
			if (string.IsNullOrEmpty(s))
				return s;
			// Firebird system strings are often padded with spaces; also remove trailing nulls
			return s.TrimEnd('\0').Trim();
		}

		private static string EscapeIdent(string name)
		{
			// Normalize input
			if (string.IsNullOrEmpty(name))
				return name;

			name = TrimFb(name);

			// If the identifier is quoted, strip surrounding quotes for normalization,
			// we will re-quote only if necessary after cleaning.
			bool originallyQuoted = name.Length >= 2 && name.StartsWith("\"") && name.EndsWith("\"");
			if (originallyQuoted)
			{
				// remove outer quotes and unescape internal doubled quotes for processing
				name = name.Substring(1, name.Length - 2).Replace("\"\"", "\"");
			}

			// Validate unquoted identifier: must start with uppercase A-Z and remaining chars A-Z0-9
			bool isSimple = false;
			if (name.Length > 0)
			{
				char first = name[0];
				if (first >= 'A' && first <= 'Z')
				{
					isSimple = true;
					for (int i = 1; i < name.Length; i++)
					{
						char ch = name[i];
						if (!((ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')))
						{
							isSimple = false;
							break;
						}
					}
				}
			}

			if (isSimple)
				return name;

			// Otherwise return quoted identifier (escape internal quotes)
			return "\"" + name.Replace("\"", "\"\"") + "\"";
		}

		private static string MapFirebirdType(int fldType, int fldSubType, int fldLength, int charLength, int precision, int scale)
		{
			// Basic mapping for common Firebird types. This is intentionally conservative.
			switch (fldType)
			{
				case 7:  // SMALLINT
					return "SMALLINT";
				case 8:  // INTEGER
					return "INTEGER";
				case 9:
					return "QUAD";
				case 10:
					return "FLOAT";
				case 11:
					return "D_FLOAT";
				case 12:
					return "DATE";
				case 13:
					return "TIME";
				case 14: // CHAR
					if (charLength > 0)
						return $"CHAR({charLength}) CHARACTER SET UTF8";
					return $"CHAR({fldLength}) CHARACTER SET UTF8";
				case 16: // NUMERIC / DECIMAL / BIGINT
					if (precision > 0)
					{
						int sc = Math.Abs(scale);
						return $"DECIMAL({precision},{sc})";
					}
					// if sub-type indicates numeric, map to DECIMAL otherwise BIGINT
					if (fldSubType != 0)
						return $"DECIMAL({fldLength},0)";
					return "BIGINT";
				case 27:
					return "DOUBLE PRECISION";
				case 35:
					return "TIMESTAMP";
				case 37: // VARCHAR
					if (charLength > 0)
						return $"VARCHAR({charLength}) CHARACTER SET UTF8";
					return $"VARCHAR({fldLength}) CHARACTER SET UTF8";
				case 40:
					return "CSTRING";
				case 261: // BLOB
					if (fldSubType == 1)
						return "BLOB SUB_TYPE TEXT";
					return "BLOB";
				default:
					// fallback: use numeric length
					if (charLength > 0)
						return $"VARCHAR({charLength}) CHARACTER SET UTF8";
					if (fldLength > 0)
						return $"VARCHAR({fldLength}) CHARACTER SET UTF8";
					return "VARCHAR(255) CHARACTER SET UTF8";
			}
		}
	}
}
