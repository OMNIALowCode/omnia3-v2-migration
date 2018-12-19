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
        private static Stopwatch stopwatch = new Stopwatch();

        private readonly Guid _sourceTenant;
        private readonly string _connectionString;
        private readonly string _encryptionKey = "9f6e0ed6a74f3a02be0c1effa300b834";

        public ExportBlobsService(string tenant, string connectionString)
        {
            _sourceTenant = Guid.Parse(tenant);
            _connectionString = connectionString;
        }

        public async Task Export()
        {
            var tempDir = new TempDirectory();

            Console.WriteLine($"Writing to folder: {tempDir.Path}");

            Directory.CreateDirectory(Path.Combine(tempDir.Path, "files"));

            stopwatch.Start();

            await Process(tempDir.Path);

            stopwatch.Stop();

            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);
        }

        private async Task Process(string outputPath)
        {
            try
            {
                var storageAccount = CloudStorageAccount.Parse(this._connectionString);

                var blobClient = storageAccount.CreateCloudBlobClient();

                var container = blobClient.GetContainerReference($"{this._sourceTenant.ToString("N").ToLowerInvariant()}");

                BlobContinuationToken blobContinuationToken = null;
                do
                {
                    var results = await container.ListBlobsSegmentedAsync("Binary", true, BlobListingDetails.All, null, blobContinuationToken, null, null);

                    blobContinuationToken = results.ContinuationToken;
                    foreach (CloudBlockBlob file in results.Results)
                    {
                        var fileName = GetFileName("Binary", file.Name);
                        var destinationFile = Path.Combine(outputPath, $"files/{fileName}");
                        Console.WriteLine("Downloading file {0}", fileName);

                        using (MemoryStream encryptedMemoryStream = new MemoryStream())
                        {
                            await file.DownloadToStreamAsync(encryptedMemoryStream);

                            byte[] encryptedArray = encryptedMemoryStream.ToArray();
                            var decryptedStream = new MemoryStream(DecryptByteArray(encryptedArray, this._encryptionKey));

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

        private string GetFileName(string sourceFolder, string sourceFileName) => sourceFileName.Replace($"{sourceFolder}/", "");

        // File decryption (code copied from OMNIA v2)
        public static byte[] DecryptByteArray(byte[] encryptedArray, string key)
        {
            using (Rijndael rijAlg = Rijndael.Create())
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
                catch (System.Security.Cryptography.CryptographicException)
                {
                    //If a CryptoException is thrown, try to decrypt with padding mode as zero (the padding mode used on older encrypted files).
                    rijAlg.Padding = PaddingMode.Zeros;
                    using (var decryptor = rijAlg.CreateDecryptor(rijAlg.Key, rijAlg.IV))
                    {
                        return PerformCryptography(decryptor, encryptedArray);
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
