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
        private static string _correlationId = Guid.NewGuid().ToString("N");
        private static string _eventMetadata = @"{""""eventClrType"""": """"Omnia.Libraries.Core.Events.EntityDataCreated""""}";

        private static int Main(string[] args)
        {
            CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;
            CultureInfo.DefaultThreadCurrentUICulture = CultureInfo.InvariantCulture;

            var app = new CommandLineApplication();

            app.Command("template", (command) =>
            {
                command.Description = "Create mapping file template";

                var tenantOption = command.Option("--t", "Export template tenant", CommandOptionType.SingleValue);
                var connectionStringOption = command.Option("--c", "Export template connection string", CommandOptionType.SingleValue);

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
                command.Description = "Export data from source system.";

                var tenantOption = command.Option("--t", "Export tenant", CommandOptionType.SingleValue);
                var mappingOption = command.Option("--m", "Export maping", CommandOptionType.SingleValue);
                var connectionStringOption = command.Option("--c", "Export connection string", CommandOptionType.SingleValue);

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
                command.Description = "Import data to destination system.";

                var folderOption = command.Option("--f", "Import folder path", CommandOptionType.SingleValue);
                var tenantOption = command.Option("--t", "Import tenant", CommandOptionType.SingleValue);
                var connectionStringOption = command.Option("--c", "Import connection string", CommandOptionType.SingleValue);

                command.OnExecute(() =>
                {
                    var service = new ImportService(folderOption.Value(), tenantOption.Value(), connectionStringOption.Value());
                    service.Import().GetAwaiter().GetResult();
                    Console.WriteLine("Import finished successfully.");
                    Console.ReadKey();
                    return 0;
                });
            });

            app.Command("export-users", (command) =>
            {
                command.Description = "Export users data from source system.";

                var tenantOption = command.Option("--t", "Export tenant", CommandOptionType.SingleValue);
                var connectionStringOption = command.Option("--c", "Export connection string", CommandOptionType.SingleValue);

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
                command.Description = "Import users data from source system.";

                var folderOption = command.Option("--f", "Import folder path", CommandOptionType.SingleValue);
                var tenantOption = command.Option("--t", "Import tenant", CommandOptionType.SingleValue);
                var apiAddressOption = command.Option("--e", "Import API endpoint", CommandOptionType.SingleValue);
                var clientIdOption = command.Option("--clientId", "API Client's ID", CommandOptionType.SingleValue);
                var clientSecretOption = command.Option("--clientSecret ", "API Client's Secret", CommandOptionType.SingleValue);

                command.OnExecute(() =>
                {
                    var service = new ImportUsersService(folderOption.Value(), tenantOption.Value(), apiAddressOption.Value(), clientIdOption.Value(), clientSecretOption.Value());
                    service.Import().GetAwaiter().GetResult();

                    Console.ReadKey();
                    return 0;
                });
            });

            return app.Execute(args);
        }
    }
}