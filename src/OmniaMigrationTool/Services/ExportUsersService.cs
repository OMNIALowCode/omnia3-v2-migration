using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace OmniaMigrationTool.Services
{
    internal class ExportUsersService
    {
        private static readonly Stopwatch stopwatch = new Stopwatch();

        private readonly Guid _sourceTenant;
        private readonly string _connectionString;

        public ExportUsersService(string tenant, string connectionString)
        {
            _sourceTenant = Guid.Parse(tenant);
            _connectionString = connectionString;
        }

        public async Task Export()
        {
            var tempDir = new TempDirectory();

            Console.WriteLine($"Writing to folder: {tempDir.Path}");

            stopwatch.Start();

            await Process(tempDir.Path);

            stopwatch.Stop();

            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);
        }

        private async Task Process(string outputPath)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();

                using (var fs = new FileStream(Path.Combine(outputPath, "users.csv"), FileMode.OpenOrCreate, FileAccess.Write))
                {
                    using (var sw = new StreamWriter(fs))
                    {
                        await sw.WriteLineAsync("email,role");

                        var usersInRolesQueryResult = await GetUsersInRoles(connection);

                        var groupedData = usersInRolesQueryResult.GroupBy(r => r.Email);

                        foreach (var userData in groupedData)
                        {
                            await sw.WriteLineAsync($"{userData.Key},{string.Join('|', userData.Select(r => r.DomainRole))}");
                        }
                    }
                }
            }
        }

        private async Task<List<UsersInRolesQueryResult>> GetUsersInRoles(SqlConnection connection)
        {
            var result = new List<UsersInRolesQueryResult>();
            using (var command = new SqlCommand(Queries.SourceQueries.UsersInRolesQuery(_sourceTenant), connection))
            {
                try
                {
                    command.CommandTimeout = 360;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var email = reader.GetString(reader.GetOrdinal("Email"));
                            var role = reader.GetString(reader.GetOrdinal("DomainRole"));

                            result.Add(new UsersInRolesQueryResult { Email = email, DomainRole = role });
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.GetType().ToString());
                    Console.WriteLine(ex.Message);
                }
            }

            return result;
        }



        private class UsersInRolesQueryResult
        {
            public string Email { get; set; }
            public string DomainRole { get; set; }
        }
    }
}
