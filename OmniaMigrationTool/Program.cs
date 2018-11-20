using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace OmniaMigrationTool
{
    internal class Program
    {
        private static Stopwatch stopwatch = new Stopwatch();

        private static void Main(string[] args)
        {
            AsyncMain(args).GetAwaiter().GetResult();
            Console.ReadKey();
        }

        private static async Task AsyncMain(string[] args)
        {
            using (var conn = new SqlConnection("Data Source=sqlsrvmymis66ey4j7eutvtc.database.windows.net;Initial Catalog=sqldbmymis66ey4j7eutvtc;user id=MyMisMaster;password=4FXsJMDlp5JWHIzk;MultipleActiveResultSets=True;Connection Timeout=60"))
            {
                await conn.OpenAsync();

                using (var command = new SqlCommand(@"SELECT * FROM [5b59faa8-3e4c-4d3e-82c8-2aecd1207a70].[MisEntities] me
                    INNER JOIN [5b59faa8-3e4c-4d3e-82c8-2aecd1207a70].[MisEntities_Agent] a on me.ID = a.ID
                    LEFT JOIN (SELECT * FROM (
                        SELECT av.MisEntityID, ak.Name, coalesce(foreignme.code, av.VALUE) AS Code
                        FROM [5b59faa8-3e4c-4d3e-82c8-2aecd1207a70].AttributeKeys ak
                        INNER JOIN [5b59faa8-3e4c-4d3e-82c8-2aecd1207a70].MisEntityTypes t ON ak.MisEntityTypeID = t.ID
                        INNER JOIN [5b59faa8-3e4c-4d3e-82c8-2aecd1207a70].vwAttributeValues av ON ak.ID = av.AttributeKeyID
                        LEFT JOIN [5b59faa8-3e4c-4d3e-82c8-2aecd1207a70].RelationalRules rr ON ak.ID = rr.PKID
                        LEFT JOIN [5b59faa8-3e4c-4d3e-82c8-2aecd1207a70].RelationalRuleInstances rri on rr.ID = rri.RelationalRuleID and rri.PKID = av.id
                        LEFT JOIN [5b59faa8-3e4c-4d3e-82c8-2aecd1207a70].[MisEntities] foreignme on rri.FKID = foreignme.ID
                        WHERE t.Code = 'Employee'
                        ) AS p
                        PIVOT (MIN([Code]) FOR [Name] IN (Company, Primavera, OutOfOffice, PrevYearHolidays)) as pvt
                    ) AS eav ON eav.MisEntityID = me.ID", conn))
                {
                    //command.CommandType = CommandType.StoredProcedure;
                    //command.Parameters.Add(new SqlParameter("@LastModifiedDate", modifiedLast));

                    stopwatch.Start();

                    try
                    {
                        var tempDir = new TempDirectory();

                        Console.WriteLine($"Writing to folder: {tempDir.Path}");

                        var sb = new StringBuilder();
                        sb.AppendLine("identifier,version,body,created_at,updated_at");

                        var jsonSettings = new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() };

                        command.CommandTimeout = 360;

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                var mapping = new Dictionary<string, object>
                                {
                                    {"_code", reader.GetString(reader.GetOrdinal("Code"))},
                                    {"_name", reader.GetString(reader.GetOrdinal("Name"))},
                                    {"usesPreviousYearHolidays", false}
                                };

                                //if (!reader.IsDBNull(reader.GetOrdinal("PrevYearHolidays")))
                                //{
                                //    mapping.Add("usesPreviousYearHolidays", reader.GetBoolean(reader.GetOrdinal("PrevYearHolidays")));
                                //}
                                //else
                                //{
                                //}

                                if (!reader.IsDBNull(reader.GetOrdinal("Company")))
                                {
                                    mapping.Add("defaultCompany", reader.GetString(reader.GetOrdinal("Company")));
                                }
                                else
                                {
                                    mapping.Add("defaultCompany", null);
                                }

                                sb.AppendLine($"{mapping["_code"]},1,\"{JsonConvert.SerializeObject(mapping, jsonSettings).Replace("\"", "\"\"")}\",{DateTime.UtcNow.ToString("yyyy-MM-dd hh:mm:ss.ff")},{DateTime.UtcNow.ToString("yyyy-MM-dd hh:mm:ss.ff")}");
                            }
                        }

                        File.WriteAllText(Path.Combine(tempDir.Path, "employees.csv"), sb.ToString());
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.GetType().ToString());
                        Console.WriteLine(ex.Message);
                    }

                    stopwatch.Stop();

                    Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);
                }
            }
        }
    }
}