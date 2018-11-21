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
using System.Linq;
using Microsoft.Extensions.CommandLineUtils;

namespace OmniaMigrationTool
{
    internal class Program
    {
        private static Stopwatch stopwatch = new Stopwatch();

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


            //// COMPANY CONFIGURATIONS
            //// ---------------------------------------------

            //var companyConfigAttributes = new List<EntityMapDefinition.AttributeMap>()
            //{
            //    new EntityMapDefinition.AttributeMap("Code", "_code"),
            //    new EntityMapDefinition.AttributeMap("Name", "_name"),
            //    new EntityMapDefinition.AttributeMap("Code", "Company"),
            //    new EntityMapDefinition.AttributeMap("Primavera", "Primavera"),

            //    new EntityMapDefinition.AttributeMap("BaseCurrency", "Currency"),
            //    //new EntityMapDefinition.AttributeMap("ExpenseReportApprover", ""),
            //    //new EntityMapDefinition.AttributeMap("CashAdvanceApprover", ""),
            //    new EntityMapDefinition.AttributeMap("BaseLocation", "BaseLocation"),
            //    //new EntityMapDefinition.AttributeMap("HRApprover", ""),
            //    //new EntityMapDefinition.AttributeMap("NumberOfDays", ""),
            //    //new EntityMapDefinition.AttributeMap("PrevYearVacationsLimitDay", ""),
            //    //new EntityMapDefinition.AttributeMap("PrevYearVacationsLimitMonth", ""),
            //    //new EntityMapDefinition.AttributeMap("SemesterContractVacationsLimitDay", ""),
            //    //new EntityMapDefinition.AttributeMap("SemesterContractVacationsLimitMonth", ""),
            //    //new EntityMapDefinition.AttributeMap("CanSeeHolidaysCreate", ""),
            //    //new EntityMapDefinition.AttributeMap("Market", ""),
                
            //    //new EntityMapDefinition.AttributeMap("HolidayEvents", ""),
            //    //new EntityMapDefinition.AttributeMap("OvertimeEvents", ""),
            //    //new EntityMapDefinition.AttributeMap("AbsenceEvents", "")
            //};

            //var companyConfigDefinition = new EntityMapDefinition("Agent", "myCompany",
            //    "GenericEntity", "CompanyConfigurations",
            //    companyConfigAttributes);


            stopwatch.Start();

            using (var conn = new SqlConnection("Data Source=sqlsrvmymis66ey4j7eutvtc.database.windows.net;Initial Catalog=sqldbmymis66ey4j7eutvtc;user id=MyMisMaster;password=4FXsJMDlp5JWHIzk;MultipleActiveResultSets=True;Connection Timeout=60"))
            {
                await conn.OpenAsync();

                await ProcessEntity(tempDir.Path, sourceTenant, conn,
                        employeeDefinition);

                await ProcessEntity(tempDir.Path, sourceTenant, conn,
                    companyDefinition);
            }

            stopwatch.Stop();

            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);
        }

        private static async Task ProcessEntity(string outputPath, Guid sourceTenant, SqlConnection conn, EntityMapDefinition definition)
        {
            var itemDictionary = new Dictionary<string, List<ItemProcessed>>();

            foreach(var item in definition.Items)
                itemDictionary.Add(item.SourceCode, await GetItems(sourceTenant, conn, item));

            using (var command = new SqlCommand(
                Queries.SourceQueries.EntityQuery(sourceTenant,
                    definition.SourceKind,
                    definition.Attributes.Select(c => c.Source).ToArray()), conn))
            {
                command.Parameters.Add(new SqlParameter("@code", definition.SourceCode));


                try
                {


                    var sb = new StringBuilder();
                    sb.AppendLine("identifier,version,body,created_at,updated_at");

                    var jsonSettings = new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() };

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


                            sb.AppendLine($"{mapping["_code"]},1,\"{JsonConvert.SerializeObject(mapping, jsonSettings).Replace("\"", "\"\"")}\",{DateTime.UtcNow.ToString("yyyy-MM-dd hh:mm:ss.ff")},{DateTime.UtcNow.ToString("yyyy-MM-dd hh:mm:ss.ff")}");
                        }
                    }

                    File.WriteAllText(Path.Combine(outputPath, $"{definition.TargetCode}.csv"), sb.ToString());
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.GetType().ToString());
                    Console.WriteLine(ex.Message);
                }


            }
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