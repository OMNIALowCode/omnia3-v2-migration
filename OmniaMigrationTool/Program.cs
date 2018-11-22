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


            // EXPENSE REPORT
            // ---------------------------------------------

            var expenseRefundRequestAttributes = new List<EntityMapDefinition.AttributeMap>()
            {
                new EntityMapDefinition.AttributeMap("Code", "_code"),
                new EntityMapDefinition.AttributeMap("Amount", "_amount", EntityMapDefinition.AttributeMap.AttributeType.Decimal,EntityMapDefinition.AttributeMap.AttributeType.Decimal),
                new EntityMapDefinition.AttributeMap("Quantity", "_quantity", EntityMapDefinition.AttributeMap.AttributeType.Decimal,EntityMapDefinition.AttributeMap.AttributeType.Decimal),
                new EntityMapDefinition.AttributeMap("DateOccurred", "ExpenseDate", EntityMapDefinition.AttributeMap.AttributeType.Date,EntityMapDefinition.AttributeMap.AttributeType.Date),

                //new EntityMapDefinition.AttributeMap("ResourceName"
                new EntityMapDefinition.AttributeMap("ExpenseDetails","Details"),
                //new EntityMapDefinition.AttributeMap("Description",""
                //new EntityMapDefinition.AttributeMap("ResourceType
                //new EntityMapDefinition.AttributeMap("FileUpload
                new EntityMapDefinition.AttributeMap("LimitItem", "ItemLimit"),
                //new EntityMapDefinition.AttributeMap("Incidence", ""
                new EntityMapDefinition.AttributeMap("VatValue", "VATValue"),
                new EntityMapDefinition.AttributeMap("ERPCostCenter","CostCenter"),
                new EntityMapDefinition.AttributeMap("ERPGeneralAccount","GeneralAccount"),
                //new EntityMapDefinition.AttributeMap("ERPAnalytics
                new EntityMapDefinition.AttributeMap("VATPercentage","VATTax"),
                new EntityMapDefinition.AttributeMap("ExpenseAmount","AmountExpenseCurrency"),
                new EntityMapDefinition.AttributeMap("LicensePlate","CompanyVehicle"),
                new EntityMapDefinition.AttributeMap("DeslocationPurpose","DeslocationPurpose"),
                new EntityMapDefinition.AttributeMap("DeslocationDistance","DeslocationDistance"),
                //new EntityMapDefinition.AttributeMap("Deslocation","")
                new EntityMapDefinition.AttributeMap("UnitValue","UnitValue"),
                new EntityMapDefinition.AttributeMap("EmployeeVehicle","OtherVehicle"),
                //new EntityMapDefinition.AttributeMap("IsFoodAllowanceType
                new EntityMapDefinition.AttributeMap("IsOwnCarType","IsOwnCarExpense"),
                //new EntityMapDefinition.AttributeMap("IsSubsistenceAllowanceType
                new EntityMapDefinition.AttributeMap("ERPVAT", "VAT"),
                //new EntityMapDefinition.AttributeMap("Vehicle2
                //new EntityMapDefinition.AttributeMap("Vehicle
                //new EntityMapDefinition.AttributeMap("ExpenseSupplier
                new EntityMapDefinition.AttributeMap("IsCompanyCarType","IsCompanyCarExpense")

            };

            var expenseRefundRequestDefinition = new EntityMapDefinition("Commitment", "ExpenseRefundRequest",
                "Commitment", "ExpenseRefundRequest",
                expenseRefundRequestAttributes);

            var expenseReportAttributes = new List<EntityMapDefinition.AttributeMap>()
            {
                new EntityMapDefinition.AttributeMap("Code", "_code"),
                new EntityMapDefinition.AttributeMap("Number", "_number", EntityMapDefinition.AttributeMap.AttributeType.Long, EntityMapDefinition.AttributeMap.AttributeType.Int),
                new EntityMapDefinition.AttributeMap("NumberSerieCode", "_serie"),
                new EntityMapDefinition.AttributeMap("CompanyCode", "company"),
                new EntityMapDefinition.AttributeMap("ApprovalStageCode", "ApprovalStage",
                    valueMapping: new List<EntityMapDefinition.AttributeMap.AttributeValueMap>()
                    {
                        new EntityMapDefinition.AttributeMap.AttributeValueMap("ExpenseReport_Pending", "Draft"),
                        new EntityMapDefinition.AttributeMap.AttributeValueMap("ExpenseReport_ProjectApprove", "ProjectApprove"),
                        new EntityMapDefinition.AttributeMap.AttributeValueMap("ExpenseReport_ManagerApprove", "ManagerApprove"),
                        new EntityMapDefinition.AttributeMap.AttributeValueMap("ExpenseReport_FinanceApprove", "FinanceApprove"),
                        new EntityMapDefinition.AttributeMap.AttributeValueMap("ExpenseReport_Completed", "Completed")
                    }),

                new EntityMapDefinition.AttributeMap("Employee", "Employee"),
                new EntityMapDefinition.AttributeMap("ApproveBy", "Approver"),
                new EntityMapDefinition.AttributeMap("Amount", "Amount"),
                //new EntityMapDefinition.AttributeMap("FileUpload // TODO
                new EntityMapDefinition.AttributeMap("ExpenseProject", "Project"),
                new EntityMapDefinition.AttributeMap("DueDate", "_date",EntityMapDefinition.AttributeMap.AttributeType.Text, EntityMapDefinition.AttributeMap.AttributeType.Date),
                //new EntityMapDefinition.AttributeMap("ApprovalDefinedEmployee
                //new EntityMapDefinition.AttributeMap("VacationEmployeeRepl
                //new EntityMapDefinition.AttributeMap("ERPDocumentIdentifier // TODO: Lidar com multiplicidade //ERPTreasuryDocumentId ou ERPBankDocumentId
                new EntityMapDefinition.AttributeMap("ExpenseDetails", "ExpenseDetails"),
                new EntityMapDefinition.AttributeMap("Location","Location"),
                new EntityMapDefinition.AttributeMap("ExpenseCurrency","Currency"),
                new EntityMapDefinition.AttributeMap("Rate","Rate"),
                new EntityMapDefinition.AttributeMap("DocumentDate","DocumentDate"),
                //new EntityMapDefinition.AttributeMap("ChargeThirdParty
                //new EntityMapDefinition.AttributeMap("ChargeThirdPartyNotes
                //new EntityMapDefinition.AttributeMap("CompanyBaseCurrency
                new EntityMapDefinition.AttributeMap("CreditCard","CreditCard"),
                //new EntityMapDefinition.AttributeMap("VatAmount
                //new EntityMapDefinition.AttributeMap("DefaultCreditCard 
                //new EntityMapDefinition.AttributeMap("ERPSimpleIdentifier // TODO: Lidar com multiplicidade //ERPIntegrations
                //new EntityMapDefinition.AttributeMap("ERPOtherDoc // TODO: Lidar com multiplicidade //ERPIntegrations
                //new EntityMapDefinition.AttributeMap("IsInvertedRateCalc
                //new EntityMapDefinition.AttributeMap("DefaultVehicle
                //new EntityMapDefinition.AttributeMap("Department
                new EntityMapDefinition.AttributeMap("UseCreditCard","UseCreditCard"),
                //new EntityMapDefinition.AttributeMap("EmployeeERPCode"
                new EntityMapDefinition.AttributeMap("Primavera", "Primavera"),
                //new EntityMapDefinition.AttributeMap("TeamApprover
                //new EntityMapDefinition.AttributeMap("DepartmentsWithAccess

            };

            var expenseReportDefinition = new EntityMapDefinition("Interaction", "ExpenseReport",
                "Document", "ExpenseReport",
                expenseReportAttributes,
                commitments: new List<EntityMapDefinition>
                {
                    expenseRefundRequestDefinition
                });


            stopwatch.Start();

            await Process(tempDir.Path, sourceTenant,
                new List<EntityMapDefinition>
                {
                    employeeDefinition,
                    companyDefinition,
                    expenseReportDefinition
                });

            stopwatch.Stop();

            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);
        }

        private static async Task Process(string outputPath, Guid sourceTenant, IList<EntityMapDefinition> definitions)
        {
            using (var conn = new SqlConnection("Data Source=sqlsrvmymis66ey4j7eutvtc.database.windows.net;Initial Catalog=sqldbmymis66ey4j7eutvtc;user id=MyMisMaster;password=4FXsJMDlp5JWHIzk;MultipleActiveResultSets=True;Connection Timeout=60"))
            {
                await conn.OpenAsync();

                var group = definitions.GroupBy(g => g.TargetCode);

                foreach (var item in group)
                {
                    var itemDefinitions = item.ToList();
                    await ProcessEntity(outputPath, sourceTenant, conn,
                        itemDefinitions);
                }
            }
        }

        private static async Task ProcessEntity(string outputPath, Guid sourceTenant, SqlConnection conn, IList<EntityMapDefinition> definitions)
        {
            var sb = new StringBuilder();
            sb.AppendLine("identifier,version,body,created_at,updated_at");

            var jsonSettings = new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() };

            IList<Dictionary<string, object>> mappingCollection = new List<Dictionary<string, object>>();

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

            foreach (var mapping in mappingCollection)
                sb.AppendLine($"{mapping["_code"]},1,\"{JsonConvert.SerializeObject(mapping, jsonSettings).Replace("\"", "\"\"")}\",{DateTime.UtcNow.ToString("yyyy-MM-dd hh:mm:ss.ff")},{DateTime.UtcNow.ToString("yyyy-MM-dd hh:mm:ss.ff")}");

            File.WriteAllText(Path.Combine(outputPath, $"{definitions.First().TargetCode}.csv"), sb.ToString());
        }

        private static async Task<IList<Dictionary<string, object>>> MapEntity(Guid sourceTenant, SqlConnection conn, EntityMapDefinition definition)
        {
            var result = new List<Dictionary<string, object>>();
            var itemDictionary = new Dictionary<string, List<ItemProcessed>>();
            var commitmentDictionary = new Dictionary<string, List<ItemProcessed>>();

            foreach (var item in definition.Items)
                itemDictionary.Add(item.SourceCode, await GetItems(sourceTenant, conn, item));

            foreach (var item in definition.Commitments)
                commitmentDictionary.Add(item.SourceCode, await GetCommitments(sourceTenant, conn, item));

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
                                MapAttribute(mapping, reader, attribute);

                            foreach (var item in definition.Items)
                            {
                                mapping[item.SourceCode] = itemDictionary[item.SourceCode].Where(i => i.ParentId.Equals(sourceEntityId))
                                    .Select(i => i.Data);
                            }

                            foreach (var item in definition.Commitments)
                            {
                                mapping[item.SourceCode] = commitmentDictionary[item.SourceCode].Where(i => i.ParentId.Equals(sourceEntityId))
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
                    Arguments = @"/c ""SET PGPASSWORD=NB_2012#&& D:\GitProjects\MigrationTool\OmniaMigrationTool\Tools\psql.exe -U NumbersBelieve@omnia3test -p 5432 -h omnia3test.postgres.database.azure.com -d Testing -c ""\copy _0c010f91ae8842ac94de3dca692f2dad_business.company FROM 'C:\Users\luisbarbosa\AppData\Local\Temp\tmp19E9.tmp\Company.csv' WITH DELIMITER ',' CSV HEADER""",
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
                            MapAttribute(mapping, reader, attribute);

                        var parentId = reader.GetInt64(reader.GetOrdinal("MisEntityID"));
                        result.Add(new ItemProcessed(parentId, mapping));
                    }
                }
            }

            return result;
        }

        private static async Task<List<ItemProcessed>> GetCommitments(Guid sourceTenant, SqlConnection conn,
            EntityMapDefinition definition)
        {
            var result = new List<ItemProcessed>();
            using (var command = new SqlCommand(
                Queries.SourceQueries.TransactionalEntityQuery(sourceTenant,
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
                            MapAttribute(mapping, reader, attribute);

                        var parentId = reader.GetInt64(reader.GetOrdinal("InteractionID"));
                        result.Add(new ItemProcessed(parentId, mapping));
                    }
                }
            }

            return result;
        }

        private static void MapAttribute(IDictionary<string, object> data, IDataRecord reader, EntityMapDefinition.AttributeMap attribute)
        {
            if (reader.IsDBNull(reader.GetOrdinal(attribute.Source))) return;

            switch (attribute.SourceType)
            {
                case EntityMapDefinition.AttributeMap.AttributeType.Long:
                    data.Add(attribute.Target, MapValue(reader.GetInt64(reader.GetOrdinal(attribute.Source))));
                    break;
                case EntityMapDefinition.AttributeMap.AttributeType.Int:
                    data.Add(attribute.Target, MapValue(reader.GetInt32(reader.GetOrdinal(attribute.Source))));
                    break;
                case EntityMapDefinition.AttributeMap.AttributeType.Decimal:
                    data.Add(attribute.Target, MapValue(reader.GetDecimal(reader.GetOrdinal(attribute.Source))));
                    break;
                case EntityMapDefinition.AttributeMap.AttributeType.Date:
                    data.Add(attribute.Target, MapValue(reader.GetDateTime(reader.GetOrdinal(attribute.Source))));
                    break;
                case EntityMapDefinition.AttributeMap.AttributeType.Boolean:
                    data.Add(attribute.Target, MapValue(reader.GetBoolean(reader.GetOrdinal(attribute.Source))));
                    break;
                default:
                    data.Add(attribute.Target, MapValue(reader.GetString(reader.GetOrdinal(attribute.Source))));
                    break;
            }

            object MapValue(object value)
            {
                if (attribute.ValueMapping == null) return value;
                var targetValue = attribute.ValueMapping.FirstOrDefault(v => v.Source.Equals(value))?.Target;
                return targetValue ?? value;
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