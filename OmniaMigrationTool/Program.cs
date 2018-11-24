using System;
using Microsoft.Extensions.CommandLineUtils;
using System.Globalization;
using OmniaMigrationTool.Services;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace OmniaMigrationTool
{
    internal class Program
    {
        private static JsonSerializerSettings _jsonSettings = new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() };
        private static string _correlationId = Guid.NewGuid().ToString("N");
        private static string _eventMetadata = @"{""""eventClrType"""": """"Omnia.Libraries.Core.Events.EntityDataCreated""""}";

        private static int Main(string[] args)
        {
            CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;
            CultureInfo.DefaultThreadCurrentUICulture = CultureInfo.InvariantCulture;

            var app = new CommandLineApplication();

            app.Command("export", (command) =>
            {
                command.Description = "Export data from source system.";

                var tenantOption = command.Option("--t", "Export tenant", CommandOptionType.SingleValue);
                var connectionStringOption = command.Option("--c", "Export connection string", CommandOptionType.SingleValue);

                command.OnExecute(() =>
                {
                    var seriesProcessor = new SeriesProcessor(_jsonSettings, _correlationId, _eventMetadata);
                    var service = new ExportService(tenantOption.Value(), connectionStringOption.Value(), _correlationId, _eventMetadata, seriesProcessor, _jsonSettings);
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

            return app.Execute(args);
        }
    }
}