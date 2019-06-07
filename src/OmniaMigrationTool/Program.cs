using System;
using Microsoft.Extensions.CommandLineUtils;
using System.Globalization;
using OmniaMigrationTool.Services;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System.IO;
using System.Collections.Generic;

namespace OmniaMigrationTool
{
    internal class Program
    {
        private static readonly string _correlationId = Guid.NewGuid().ToString("N");
        private static readonly string _eventMetadata = @"{""""eventClrType"""": """"Omnia.Libraries.Core.Events.EntityDataCreated""""}";

        private static int Main(string[] args)
        {
            CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;
            CultureInfo.DefaultThreadCurrentUICulture = CultureInfo.InvariantCulture;

            var app = new CommandLineApplication();

            app.HelpOption("-h | --help");

            app.Command("template", (command) =>
            {
                command.HelpOption("-h | --help");
                command.Description = "Create mapping file template from the Source Tenant model.";

                var tenantOption = command.Option("-t | --tenant", "Export template tenant", CommandOptionType.SingleValue);
                var connectionStringOption = command.Option("-c | --connection-string", "Export template connection string", CommandOptionType.SingleValue);

                command.OnExecute(() =>
                {
                    var service = new TemplateService(tenantOption.Value(), connectionStringOption.Value());
                    service.GenerateTemplate().GetAwaiter().GetResult();
                    Console.ReadKey();
                    return 0;
                });
            });

            app.Command("export", (command) =>
            {
                command.HelpOption("-h | --help");
                command.Description = "Export data from source system.";

                var tenantOption = command.Option("-t | --tenant", "Tenant to export", CommandOptionType.SingleValue);
                var mappingOption = command.Option("-m | --mapping", "Mapping file", CommandOptionType.SingleValue);
                var connectionStringOption = command.Option("-c | --connection-string", "Connection string", CommandOptionType.SingleValue);

                command.OnExecute(() =>
                {
                    var tenant = tenantOption.Value();
                    var jsonSettings = new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() };
                    var mappings = JsonConvert.DeserializeObject<List<EntityMapDefinition>>(File.ReadAllText(mappingOption.Value()), jsonSettings);
                    var seriesProcessor = new SeriesProcessor(mappings, tenant, jsonSettings, _correlationId, _eventMetadata);
                    var service = new ExportService(tenant, connectionStringOption.Value(), _correlationId, _eventMetadata, mappings, seriesProcessor, jsonSettings);
                    service.Export().GetAwaiter().GetResult();
                    Console.ReadKey();
                    return 0;
                });
            });

            app.Command("import", (command) =>
            {
                command.HelpOption("-h | --help");
                command.Description = "Import data to destination system.";

                var folderOption = command.Option("-f | --folder", "Import folder path", CommandOptionType.SingleValue);
                var tenantOption = command.Option("-t | --tenant", "Import to tenant", CommandOptionType.SingleValue);
                var connectionStringOption = command.Option("-c | --connection-string", "Connection string", CommandOptionType.SingleValue);

                command.OnExecute(() =>
                {
                    var service = new ImportService(folderOption.Value(), tenantOption.Value(), connectionStringOption.Value());
                    service.Import().GetAwaiter().GetResult();
                    Console.WriteLine("Import finished successfully.");
                    Console.ReadKey();
                    return 0;
                });
            });


            app.Command("export-files", (command) =>
            {
                command.HelpOption("-h | --help");
                command.Description = "Export files from source system.";

                var tenantOption = command.Option("-t | --tenant", "Tenant", CommandOptionType.SingleValue);
                var connectionStringOption = command.Option("-c | --connection-string", "Connection string", CommandOptionType.SingleValue);
                var encryptionKeyOption = command.Option("-ek | --encryption-key", "Export storage Encryption Key", CommandOptionType.SingleValue);

                command.OnExecute(() =>
                {
                    var service = new ExportBlobsService(tenantOption.Value(), connectionStringOption.Value(), encryptionKeyOption.Value());
                    service.Export().GetAwaiter().GetResult();

                    Console.ReadKey();
                    return 0;
                });
            });

            app.Command("export-users", (command) =>
            {
                command.HelpOption("-h | --help");
                command.Description = "Export users data from source system.";

                var tenantOption = command.Option("-t | --tenant", "Tenant", CommandOptionType.SingleValue);
                var connectionStringOption = command.Option("-c | --connection-string", "Connection string", CommandOptionType.SingleValue);

                command.OnExecute(() =>
                {
                    var service = new ExportUsersService(tenantOption.Value(), connectionStringOption.Value());
                    service.Export().GetAwaiter().GetResult();

                    Console.ReadKey();
                    return 0;
                });
            });

            app.Command("import-users", (command) =>
            {
                command.HelpOption("-h | --help");
                command.Description = "Import users data from source system.";

                var folderOption = command.Option("-f | --folder", "Import folder path", CommandOptionType.SingleValue);
                var tenantOption = command.Option("-t | --tenant", "Import to tenant", CommandOptionType.SingleValue);
                var apiAddressOption = command.Option("-e | --endpoint", "Import API endpoint", CommandOptionType.SingleValue);
                var clientIdOption = command.Option("-ci | --client-id", "API Client's ID", CommandOptionType.SingleValue);
                var clientSecretOption = command.Option("-cs | --client-secret ", "API Client's Secret", CommandOptionType.SingleValue);

                command.OnExecute(() =>
                {
                    var service = new ImportUsersService(folderOption.Value(), tenantOption.Value(), apiAddressOption.Value(), clientIdOption.Value(), clientSecretOption.Value());
                    service.Import().GetAwaiter().GetResult();

                    Console.ReadKey();
                    return 0;
                });
            });

            app.Command("import-files", (command) =>
            {
                command.HelpOption("-h | --help");
                command.Description = "Export files from source system.";

                var mappingsFolderOption = command.Option("-m | --mappings", "Mappings folder", CommandOptionType.SingleValue);
                var filesFolderOption = command.Option("-f | --folder", "Files folder", CommandOptionType.SingleValue);
                var tenantOption = command.Option("-t | --tenant", "Import to tenant", CommandOptionType.SingleValue);
                var connectionStringOption = command.Option("-c | --connection-string", "Import connection string", CommandOptionType.SingleValue);

                command.OnExecute(() =>
                {
                    var service = new ImportBlobsService(mappingsFolderOption.Value(), filesFolderOption.Value(), tenantOption.Value(), connectionStringOption.Value());
                    service.Import().GetAwaiter().GetResult();

                    Console.ReadKey();
                    return 0;
                });
            });




            return app.Execute(args);
        }
    }
}