using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using System.Linq;
using Microsoft.Extensions.CommandLineUtils;
using Omnia.Libraries.GenericExtensions;
using System.IO;

namespace OmniaMigrationTool
{
    internal class Program
    {
        private static Stopwatch stopwatch = new Stopwatch();
        private static JsonSerializerSettings jsonSettings = new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() };
        private static string correlationId = Guid.NewGuid().ToString("N");
        private static string eventMetadata = @"{""""eventClrType"""": """"Omnia.Libraries.Core.Events.EntityDataCreated""""}";

        private static int Main(string[] args)
        {
            var app = new CommandLineApplication();

            app.Command("export", (command) =>
            {
                command.Description = "Export data from source system.";
                //command.HelpOption("-?|-h|--help");

                command.OnExecute(() =>
            {
                AsyncMain(args).GetAwaiter().GetResult();
                Console.ReadKey();
                return 0;
            });
            });

            app.Command("import", (command) =>
            {
                command.Description = "Import data to destination system.";

                command.OnExecute(() =>
                {
                    Import();
                    Console.ReadKey();
                    return 0;
                });
            });

            return app.Execute(args);
        }

        private static async Task AsyncMain(string[] args)
        {
            var sourceTenant = Guid.Parse("5b59faa8-3e4c-4d3e-82c8-2aecd1207a70");
            var tempDir = new TempDirectory();

            Console.WriteLine($"Writing to folder: {tempDir.Path}");

            // EMPLOYEE
            // ---------------------------------------------
            var employeeErpConfigDefinition = new EntityMapDefinition("MisEntityItem", "EmployeeERPConfig",
                "GenericEntity", "EmployeeERPConfiguration",
                "ERPCode,Vehicle,Department,DepartmentDescription,Job,JobDescription,Manager,Primavera,ExpensesManager".Split(",")
                    .Select(c => new EntityMapDefinition.AttributeMap(c, c)).ToList());

            employeeErpConfigDefinition
                .Attributes.Add(new EntityMapDefinition.AttributeMap("Code", "_code"));
            employeeErpConfigDefinition
                .Attributes.Add(new EntityMapDefinition.AttributeMap("ERPCostCenter", "CostCenter"));
            employeeErpConfigDefinition
                .Attributes.Add(new EntityMapDefinition.AttributeMap("CompanyCode", "company"));

            var employeeDefinition = new EntityMapDefinition("Agent", "Employee",
                "Agent", "Employee",
                "Primavera,OutOfOffice,PrevYearHolidays,usesPreviousYearHolidays"
                    .Split(",")
                    .Select(c => new EntityMapDefinition.AttributeMap(c, c)).ToList(),
                new List<EntityMapDefinition>()
                {
                    employeeErpConfigDefinition
                });

            employeeDefinition
                .Attributes.Add(new EntityMapDefinition.AttributeMap("Code", "_code"));
            employeeDefinition
                .Attributes.Add(new EntityMapDefinition.AttributeMap("Name", "_name"));
            employeeDefinition
                .Attributes.Add(new EntityMapDefinition.AttributeMap("Company", "defaultCompany"));

            // COMPANY
            // ---------------------------------------------

            var companyAttributes = new List<EntityMapDefinition.AttributeMap>()
            {
                new EntityMapDefinition.AttributeMap("Code", "_code"),
                new EntityMapDefinition.AttributeMap("Name", "_name"),
                new EntityMapDefinition.AttributeMap("BaseCurrency", "Currency"),
                //new EntityMapDefinition.AttributeMap("ExpenseReportApprover", ""),
                //new EntityMapDefinition.AttributeMap("CashAdvanceApprover", ""),
                new EntityMapDefinition.AttributeMap("BaseLocation", "BaseLocation"),
                //new EntityMapDefinition.AttributeMap("HRApprover", ""),
                //new EntityMapDefinition.AttributeMap("NumberOfDays", ""),
                //new EntityMapDefinition.AttributeMap("PrevYearVacationsLimitDay", ""),
                //new EntityMapDefinition.AttributeMap("PrevYearVacationsLimitMonth", ""),
                //new EntityMapDefinition.AttributeMap("SemesterContractVacationsLimitDay", ""),
                //new EntityMapDefinition.AttributeMap("SemesterContractVacationsLimitMonth", ""),
                //new EntityMapDefinition.AttributeMap("CanSeeHolidaysCreate", ""),
                //new EntityMapDefinition.AttributeMap("Market", ""),
                //new EntityMapDefinition.AttributeMap("Primavera", ""),
                //new EntityMapDefinition.AttributeMap("HolidayEvents", ""),
                //new EntityMapDefinition.AttributeMap("OvertimeEvents", ""),
                //new EntityMapDefinition.AttributeMap("AbsenceEvents", "")
            };

            var companyDefinition = new EntityMapDefinition("Agent", "myCompany",
                "Agent", "Company",
                companyAttributes);

            // COMPANY CONFIGURATIONS
            // ---------------------------------------------

            var companyConfigFromCompanyAttributes = new List<EntityMapDefinition.AttributeMap>()
            {
                new EntityMapDefinition.AttributeMap("Code", "_code"),
                new EntityMapDefinition.AttributeMap("Name", "_name"),
                new EntityMapDefinition.AttributeMap("Code", "Company"),
                new EntityMapDefinition.AttributeMap("Primavera", "Primavera"),

                new EntityMapDefinition.AttributeMap("BaseCurrency", "Currency"),
                //new EntityMapDefinition.AttributeMap("ExpenseReportApprover", ""),
                //new EntityMapDefinition.AttributeMap("CashAdvanceApprover", ""),
                new EntityMapDefinition.AttributeMap("BaseLocation", "BaseLocation"),
                //new EntityMapDefinition.AttributeMap("HRApprover", ""),
                //new EntityMapDefinition.AttributeMap("NumberOfDays", ""),
                //new EntityMapDefinition.AttributeMap("PrevYearVacationsLimitDay", ""),
                //new EntityMapDefinition.AttributeMap("PrevYearVacationsLimitMonth", ""),
                //new EntityMapDefinition.AttributeMap("SemesterContractVacationsLimitDay", ""),
                //new EntityMapDefinition.AttributeMap("SemesterContractVacationsLimitMonth", ""),
                //new EntityMapDefinition.AttributeMap("CanSeeHolidaysCreate", ""),
                //new EntityMapDefinition.AttributeMap("Market", ""),

                //new EntityMapDefinition.AttributeMap("HolidayEvents", ""),
                //new EntityMapDefinition.AttributeMap("OvertimeEvents", ""),
                //new EntityMapDefinition.AttributeMap("AbsenceEvents", "")
            };

            var companyConfigFromCompanyDefinition = new EntityMapDefinition("Agent", "myCompany",
                "GenericEntity", "CompanyConfigurations",
                companyConfigFromCompanyAttributes);

            var companyConfigFromPrimaveraAttributes = new List<EntityMapDefinition.AttributeMap>()
            {
                new EntityMapDefinition.AttributeMap("Code", "_code"),
                //new EntityMapDefinition.AttributeMap("Name", "_name"),
                //new EntityMapDefinition.AttributeMap("Code", "Company"),
                new EntityMapDefinition.AttributeMap("TipoPlataforma", "TipoPlataforma"),

                //new EntityMapDefinition.AttributeMap("ExpenseReportApprover", ""),
                //new EntityMapDefinition.AttributeMap("CashAdvanceApprover", ""),
                //new EntityMapDefinition.AttributeMap("HRApprover", ""),
                //new EntityMapDefinition.AttributeMap("NumberOfDays", ""),
                //new EntityMapDefinition.AttributeMap("PrevYearVacationsLimitDay", ""),
                //new EntityMapDefinition.AttributeMap("PrevYearVacationsLimitMonth", ""),
                //new EntityMapDefinition.AttributeMap("SemesterContractVacationsLimitDay", ""),
                //new EntityMapDefinition.AttributeMap("SemesterContractVacationsLimitMonth", ""),
                //new EntityMapDefinition.AttributeMap("CanSeeHolidaysCreate", ""),
                //new EntityMapDefinition.AttributeMap("Market", ""),

                //new EntityMapDefinition.AttributeMap("HolidayEvents", ""),
                //new EntityMapDefinition.AttributeMap("OvertimeEvents", ""),
                //new EntityMapDefinition.AttributeMap("AbsenceEvents", "")
            };

            var companyConfigFromPrimaveraDefinition = new EntityMapDefinition("UserDefinedEntity", "Primavera",
                "GenericEntity", "CompanyConfigurations",
                companyConfigFromPrimaveraAttributes);

            stopwatch.Start();

            await Process(tempDir.Path, sourceTenant,
                new List<EntityMapDefinition>
                {
                    employeeDefinition,
                    companyDefinition,
                    companyConfigFromCompanyDefinition,
                    companyConfigFromPrimaveraDefinition
                });

            stopwatch.Stop();

            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);
        }

        private static async Task Process(string outputPath, Guid sourceTenant, IList<EntityMapDefinition> definitions)
        {
            using (var conn = new SqlConnection("Data Source=sqlsrvmymis66ey4j7eutvtc.database.windows.net;Initial Catalog=sqldbmymis66ey4j7eutvtc;user id=MyMisMaster;password=4FXsJMDlp5JWHIzk;MultipleActiveResultSets=True;Connection Timeout=60"))
            {
                await conn.OpenAsync();

                using (var fs = new FileStream(Path.Combine(outputPath, "event_store.csv"), FileMode.OpenOrCreate, FileAccess.Write))
                {
                    using (var sw = new StreamWriter(fs))
                    {
                        await sw.WriteLineAsync("id,created_at,created_by,entity_id,definition_identifier,identifier,is_removed,version,event,metadata,message,correlation_id");

                        var group = definitions.GroupBy(g => g.TargetCode);
                        foreach (var item in group)
                        {
                            await ProcessEntity(outputPath, sourceTenant, conn, item.AsEnumerable(), sw);
                        }
                    }
                }
            }
        }

        private static async Task ProcessEntity(string outputPath, Guid sourceTenant, SqlConnection conn, IEnumerable<EntityMapDefinition> definitions, StreamWriter eventStoreStream)
        {
            var sb = new StringBuilder();
            sb.AppendLine("identifier,version,body,created_at,updated_at");

            var mappingCollection = new List<Dictionary<string, object>>();

            var targetCode = definitions.First().TargetCode;

            foreach (var definition in definitions)
            {
                var mappingResult = await MapEntity(sourceTenant, conn, definition);

                foreach (var result in mappingResult)
                {
                    var elementInCollection = mappingCollection.FirstOrDefault(m => m["_code"].Equals(result["_code"]));
                    if (elementInCollection == null)
                        mappingCollection.Add(result);
                    else
                        elementInCollection.Merge(result);
                }
            }

            using (var fs = new FileStream(Path.Combine(outputPath, $"{targetCode.ToSnakeCase()}.csv"), FileMode.OpenOrCreate, FileAccess.Write))
            {
                using (var entityStream = new StreamWriter(fs))
                {
                    foreach (var mapping in mappingCollection)
                    {
                        var data = JsonConvert.SerializeObject(mapping, jsonSettings).Replace("\"", "\"\"");

                        await eventStoreStream.WriteLineAsync($@"{Guid.NewGuid()},{DateTime.UtcNow.ToString("yyyy-MM-dd hh:mm:ss.ff")},migrationtool@omnia,{Guid.NewGuid()},{targetCode},{mapping["_code"]},false,1,""{{""""data"""":{data}}}"",""{eventMetadata}"",'{targetCode}' with code '{mapping["_code"]}' has been migrated,{correlationId}");
                        await entityStream.WriteLineAsync($"{mapping["_code"]},1,\"{data}\",{DateTime.UtcNow.ToString("yyyy-MM-dd hh:mm:ss.ff")},{DateTime.UtcNow.ToString("yyyy-MM-dd hh:mm:ss.ff")}");
                    }
                }
            }
        }

        private static async Task<IList<Dictionary<string, object>>> MapEntity(Guid sourceTenant, SqlConnection conn, EntityMapDefinition definition)
        {
            var result = new List<Dictionary<string, object>>();
            var itemDictionary = new Dictionary<string, List<ItemProcessed>>();

            foreach (var item in definition.Items)
                itemDictionary.Add(item.SourceCode, await GetItems(sourceTenant, conn, item));

            using (var command = new SqlCommand(
                Queries.SourceQueries.EntityQuery(sourceTenant,
                    definition.SourceKind,
                    definition.Attributes.Select(c => c.Source).ToArray()), conn))
            {
                command.Parameters.Add(new SqlParameter("@code", definition.SourceCode));
                try
                {
                    command.CommandTimeout = 360;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var sourceEntityId = reader.GetInt64(reader.GetOrdinal("ID"));
                            var mapping = new Dictionary<string, object>();

                            foreach (var attribute in definition.Attributes)
                                MapAttribute(mapping, reader, attribute.Source, attribute.Target);

                            foreach (var item in definition.Items)
                            {
                                mapping[item.SourceCode] = itemDictionary[item.SourceCode].Where(i => i.ParentId.Equals(sourceEntityId))
                                    .Select(i => i.Data);
                            }

                            result.Add(mapping);
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

        private static void Import()
        {
            var outputMessageBuilder = new StringBuilder();

            var process = new Process
            {
                EnableRaisingEvents = true,
                StartInfo = new ProcessStartInfo("cmd.exe")
                {
                    Arguments = @"/c ""SET PGPASSWORD=NB_2012#&& D:\GitProjects\MigrationTool\OmniaMigrationTool\Tools\psql.exe -U NumbersBelieve@omnia3test -p 5432 -h omnia3test.postgres.database.azure.com -d Testing -c ""\copy _0c010f91ae8842ac94de3dca692f2dad_business.event_store FROM 'C:\Users\luisbarbosa\AppData\Local\Temp\tmp19B0.tmp\event_store.csv' WITH DELIMITER ',' CSV HEADER""",
                    //WindowStyle = ProcessWindowStyle.Hidden
                    UseShellExecute = false,
                    CreateNoWindow = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true
                }
            };

            process.OutputDataReceived += (s, e) => Console.WriteLine(e.Data);
            process.ErrorDataReceived += (s, e) => outputMessageBuilder.AppendLine(e.Data);

            // Start process and handlers
            process.Start();
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            process.WaitForExit();

            if (process.ExitCode != 0)
                throw new InvalidOperationException(outputMessageBuilder.ToString());
        }

        private static async Task<List<ItemProcessed>> GetItems(Guid sourceTenant, SqlConnection conn,
            EntityMapDefinition definition)
        {
            var result = new List<ItemProcessed>();
            using (var command = new SqlCommand(
                Queries.SourceQueries.EntityQuery(sourceTenant,
                    definition.SourceKind,
                    definition.Attributes.Select(c => c.Source).ToArray()
                        ), conn))
            {
                command.Parameters.Add(new SqlParameter("@code",
                    definition.SourceCode
                ));

                command.CommandTimeout = 360;

                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var mapping = new Dictionary<string, object>();

                        foreach (var attribute in definition.Attributes)
                            MapAttribute(mapping, reader, attribute.Source, attribute.Target);

                        var parentId = reader.GetInt64(reader.GetOrdinal("MisEntityID"));
                        result.Add(new ItemProcessed(parentId, mapping));
                    }
                }
            }

            return result;
        }

        private static void MapAttribute(IDictionary<string, object> data, IDataRecord reader, string sourceAttribute, string targetAttribute)
        {
            if (!reader.IsDBNull(reader.GetOrdinal(sourceAttribute)))
            {
                data.Add(targetAttribute, reader.GetString(reader.GetOrdinal(sourceAttribute)));
            }
        }

        internal class ItemProcessed
        {
            public ItemProcessed(long parentId, Dictionary<string, object> data)
            {
                ParentId = parentId;
                Data = data;
            }

            public long ParentId { get; }

            public Dictionary<string, object> Data { get; }
        }
    }
}