using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace OmniaMigrationTool.Services
{
    public class ExportBlobsService
    {
        private static readonly Stopwatch _stopwatch = new Stopwatch();

        private readonly Guid _sourceTenant;
        private readonly string _connectionString;
        private readonly string _encryptionKey;

        public ExportBlobsService(string tenant, string connectionString, string encryptionKey)
        {
            _sourceTenant = Guid.Parse(tenant);
            _connectionString = connectionString;
            _encryptionKey = encryptionKey;
        }

        public async Task Export()
        {
            var tempDir = new TempDirectory();

            Console.WriteLine($"Writing to folder: {tempDir.Path}");

            Directory.CreateDirectory(Path.Combine(tempDir.Path, "files"));

            _stopwatch.Start();

            await Process(tempDir.Path);

            _stopwatch.Stop();

            Console.WriteLine("Time elapsed: {0}", _stopwatch.Elapsed);
        }

        private async Task Process(string outputPath)
        {
            try
            {
                var storageAccount = CloudStorageAccount.Parse(_connectionString);

                var blobClient = storageAccount.CreateCloudBlobClient();

                var container = blobClient.GetContainerReference($"{_sourceTenant.ToString("N").ToLowerInvariant()}");

                BlobContinuationToken blobContinuationToken = null;
                do
                {
                    var results = await container.ListBlobsSegmentedAsync("Binary", true, BlobListingDetails.All, null, blobContinuationToken, null, null);

                    blobContinuationToken = results.ContinuationToken;
                    foreach (var listBlobItem in results.Results)
                    {
                        var file = (CloudBlockBlob) listBlobItem;
                        var fileName = GetFileName("Binary", file.Name);
                        fileName = fileName.Replace(":", "%3A");
                        var destinationFile = Path.Combine(outputPath, $"files\\{fileName}");
                        Console.WriteLine("Downloading file {0}", fileName);

                        using (var encryptedMemoryStream = new MemoryStream())
                        {
                            await file.DownloadToStreamAsync(encryptedMemoryStream);

                            var encryptedArray = encryptedMemoryStream.ToArray();
                            var decryptedStream = new MemoryStream(DecryptByteArray(encryptedArray, _encryptionKey));
                            await File.WriteAllBytesAsync(destinationFile, decryptedStream.ToArray());
                        }
                    }
                } while (blobContinuationToken != null);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.GetType().ToString());
                Console.WriteLine(ex.Message);
            }
        }

        private static string GetFileName(string sourceFolder, string sourceFileName) => sourceFileName.Replace($"{sourceFolder}/", "");

        // File decryption (code copied from OMNIA v2)
        private static byte[] DecryptByteArray(byte[] encryptedArray, string key)
        {
            using (var rijAlg = Rijndael.Create())
            {
                rijAlg.Key = Encoding.ASCII.GetBytes(key);
                rijAlg.IV = Encoding.ASCII.GetBytes(key.Substring(0, 16));
                rijAlg.Mode = CipherMode.CBC;
                rijAlg.Padding = PaddingMode.PKCS7;

                try
                {
                    using (var decryptor = rijAlg.CreateDecryptor(rijAlg.Key, rijAlg.IV))
                    {
                        return PerformCryptography(decryptor, encryptedArray);
                    }
                }
                catch (CryptographicException)
                {
                    try
                    {
                        //If a CryptoException is thrown, try to decrypt with padding mode as zero (the padding mode used on older encrypted files).
                        rijAlg.Padding = PaddingMode.Zeros;
                        using (var decryptor = rijAlg.CreateDecryptor(rijAlg.Key, rijAlg.IV))
                        {
                            return PerformCryptography(decryptor, encryptedArray);
                        }
                    }
                    catch (CryptographicException)
                    {
                        // If can't decrypt due to CryptoException return the original data
                        return encryptedArray;
                    }
                }
            }
        }

        private static byte[] PerformCryptography(ICryptoTransform cryptoTransform, byte[] data)
        {
            using (var memoryStream = new MemoryStream())
            {
                using (var cryptoStream = new CryptoStream(memoryStream, cryptoTransform, CryptoStreamMode.Write))
                {
                    cryptoStream.Write(data, 0, data.Length);

                    cryptoStream.FlushFinalBlock();

                    return memoryStream.ToArray();
                }
            }
        }
    }
}
