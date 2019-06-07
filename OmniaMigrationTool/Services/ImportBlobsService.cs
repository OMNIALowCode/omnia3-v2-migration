using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace OmniaMigrationTool.Services
{
    public class ImportBlobsService
    {
        private static Stopwatch stopwatch = new Stopwatch();

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

                for (int i = 1; i < filesMapping.Length; i++)
                {
                    var fileMappingSplit = filesMapping[i].Split(',');

                    if (fileMappingSplit.Length == 2 && fileMappingSplit.All(f => !string.IsNullOrEmpty(f.Trim())))
                    {
                        var sourceFileName = fileMappingSplit[0].Replace("Binary/", "");
                        var targetFileName = fileMappingSplit[1];

                        Console.Write($"Importing file: {sourceFileName}.");

                        var exportedFilePath = exportedFiles.FirstOrDefault(f => f.EndsWith($"\\{sourceFileName}", StringComparison.InvariantCultureIgnoreCase));
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

        private Task UploadFile(CloudBlobContainer blobContainer, string fileToImportPath, string targetFileName)
        {
            var uploadPath = $"Application/Data/{targetFileName}";
            CloudBlockBlob blobToUpload = blobContainer.GetBlockBlobReference(uploadPath);
            return blobToUpload.UploadFromFileAsync(fileToImportPath);
        }
    }
}
