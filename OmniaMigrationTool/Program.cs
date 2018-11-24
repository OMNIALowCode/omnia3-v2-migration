using System;
using Microsoft.Extensions.CommandLineUtils;
using System.Globalization;
using OmniaMigrationTool.Services;

namespace OmniaMigrationTool
{
    internal class Program
    {
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
                    var service = new ExportService(app.Out, tenantOption.Value(), connectionStringOption.Value());
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