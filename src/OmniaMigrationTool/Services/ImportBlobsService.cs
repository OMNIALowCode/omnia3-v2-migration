using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;


namespace OmniaMigrationTool.Services
{
    public class ImportBlobsService
    {
        private static readonly Stopwatch stopwatch = new Stopwatch();

        private readonly string _mappingsFolderPath;
        private readonly string _filesFolderPath;
        private readonly string _sourceTenant;
        private readonly string _connectionString;

        public ImportBlobsService(string mappingsFolderPath, string filesFolderPath, string tenant, string connectionString)
        {
            _mappingsFolderPath = mappingsFolderPath;
            _filesFolderPath = filesFolderPath;
            _sourceTenant = tenant;
            _connectionString = connectionString;
        }

        public async Task Import()
        {
            Console.WriteLine($"Reading from folder: {_mappingsFolderPath}");

            stopwatch.Start();

            await Process(_mappingsFolderPath, _filesFolderPath);

            stopwatch.Stop();

            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);
        }

        private async Task Process(string mappingsFolderPath, string filesFolderPath)
        {
            try
            {
                var filesMapping = File.ReadAllLines(Path.Combine(mappingsFolderPath, "file_mapping.csv"));
                var exportedFiles = Directory.EnumerateFiles(Path.Combine(filesFolderPath, "files"));

                var blobContainer = await GetBlobContainer(_connectionString, _sourceTenant);

                for (var i = 1; i < filesMapping.Length; i++)
                {
                    var fileMappingSplit = filesMapping[i].Split(',');

                    if (fileMappingSplit.Length != 2 ||
                        fileMappingSplit.Any(f => string.IsNullOrEmpty(f.Trim()))) continue;

                    var sourceFilesNames = fileMappingSplit[0].Replace("Binary/", "").Split(";");
                    var targetFilesNames = fileMappingSplit[1].Split(";");

                    if (sourceFilesNames.Length != targetFilesNames.Length)
                        throw new Exception($"Number of source files ({sourceFilesNames.Length}) has to be equal to target files ({targetFilesNames.Length})");

                    for (var f = 0; f<sourceFilesNames.Length; f++)
                    {
                        var sourceFileName = sourceFilesNames[f].Replace(":", "%3A");
                        var targetFileName = targetFilesNames[f];
                        Console.Write($"Importing file: {sourceFileName}.");

                        var exportedFilePath = exportedFiles.FirstOrDefault(file => file.EndsWith($"\\{sourceFileName}", StringComparison.InvariantCultureIgnoreCase));
                        if (string.IsNullOrEmpty(exportedFilePath))
                        {
                            Console.WriteLine($"File not found in exported files folder - ignored.");
                            continue;
                        }

                        Console.WriteLine();

                        await UploadFile(blobContainer, exportedFilePath, targetFileName);
                        System.Threading.Thread.Sleep(100);

                    }

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.GetType().ToString());
                Console.WriteLine(ex.Message);
            }
        }

        private async Task<CloudBlobContainer> GetBlobContainer(string connectionString, string sourceTenant)
        {
            var storageAccount = CloudStorageAccount.Parse(this._connectionString);

            var blobClient = storageAccount.CreateCloudBlobClient();

            var container = blobClient.GetContainerReference($"{this._sourceTenant.ToLowerInvariant()}-prd");
            await container.CreateIfNotExistsAsync();

            return container;
        }

        private static Task UploadFile(CloudBlobContainer blobContainer, string fileToImportPath, string targetFileName)
        {
            var uploadPath = $"Application/Data/{targetFileName}";
            var blobToUpload = blobContainer.GetBlockBlobReference(uploadPath);
            return blobToUpload.UploadFromFileAsync(fileToImportPath);
        }
    }
}
