using Npgsql;
using OmniaMigrationTool.Queries;
using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace OmniaMigrationTool.Services
{
    internal class ImportService
    {
        private readonly string _folderPath;
        private readonly string _tenantCode;
        private readonly string _connectionString;

        public ImportService(string folderPath, string tenantCode, string connectionString)
        {
            _folderPath = folderPath;
            _tenantCode = tenantCode;
            _connectionString = connectionString;
        }

        public async Task Import()
        {
            string targetSchema = null;

            var builder = new NpgsqlConnectionStringBuilder(_connectionString);
            using (var conn = new NpgsqlConnection(builder.ConnectionString))
            {
                await conn.OpenAsync();

                using (var command = new NpgsqlCommand(TargetQueries.TenantSchemaQuery, conn))
                {
                    command.CommandTimeout = 360;
                    command.Parameters.Add(new NpgsqlParameter("@code", _tenantCode));

                    targetSchema = (await command.ExecuteScalarAsync()) as string;
                }
            }

            var outputMessageBuilder = new StringBuilder();
            var commandPipeline = new StringBuilder();

            Console.WriteLine($"Readind from folder: {_folderPath}");

            foreach (var file in Directory.EnumerateFiles(_folderPath, "*.csv", SearchOption.AllDirectories))
            {
                commandPipeline.Append($@"-c ""\copy {targetSchema}.{Path.GetFileNameWithoutExtension(file)} FROM '{Path.Combine(_folderPath, file)}' WITH DELIMITER ',' CSV HEADER"" ");
            }

            using (var process = new Process
            {
                EnableRaisingEvents = true,
                StartInfo = new ProcessStartInfo("cmd.exe")
                {
                    Arguments = $@"/c ""SET PGPASSWORD={builder.Password}&& {Path.Combine(Directory.GetCurrentDirectory(), "Tools\\psql.exe")} -U {builder.Username} -p {builder.Port} -h {builder.Host} -d {builder.Database} {commandPipeline.ToString()}",
                    WindowStyle = ProcessWindowStyle.Hidden,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true
                }
            })
            {
                process.OutputDataReceived += (s, e) => Console.WriteLine(e.Data);
                process.ErrorDataReceived += (s, e) => outputMessageBuilder.AppendLine(e.Data);

                process.Start();
                process.BeginOutputReadLine();
                process.BeginErrorReadLine();
                process.WaitForExit();

                if (process.ExitCode != 0)
                    throw new InvalidOperationException(outputMessageBuilder.ToString());
            }
        }
    }
}