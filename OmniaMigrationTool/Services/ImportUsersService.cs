﻿using Microsoft.AspNetCore.JsonPatch;
using Microsoft.Extensions.Caching.Memory;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OmniaMigrationTool.Services
{
    internal class ImportUsersService
    {
        private static Stopwatch stopwatch = new Stopwatch();

        private readonly string _folderPath;
        private readonly string _tenantCode;
        private readonly string _apiEndpoint;
        private readonly ApiClient _apiClient;

        public ImportUsersService(string folderPath, string tenant, string apiEndpoint, string clientId, string cliendSecret)
        {
            _folderPath = folderPath;
            _tenantCode = tenant;
            _apiEndpoint = apiEndpoint;
            _apiClient = new ApiClient(new Uri(new Uri(apiEndpoint), "/api/v1/"), new Uri(new Uri(apiEndpoint), "/identity/"), clientId, cliendSecret, new MemoryCache(new MemoryCacheOptions()));
        }

        public async Task Import()
        {
            Console.WriteLine($"Reading from folder: {_folderPath}");

            stopwatch.Start();

            await Process(_folderPath);

            stopwatch.Stop();

            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);
        }

        private async Task Process(string folderPath)
        {
            try
            {
                var usersFileContent = File.ReadAllLines(Path.Combine(folderPath, "users.csv"));

                var rolesData = ProcessFileContent(usersFileContent);

                foreach (var role in rolesData)
                {
                    await UpdateRole(role.Key, role.Value);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.GetType().ToString());
                Console.WriteLine(ex.Message);
            }
        }

        private Dictionary<string, List<string>> ProcessFileContent(string[] fileContent)
        {
            var rolesData = new Dictionary<string, List<string>>();

            for (int i = 1; i < fileContent.Length; i++)
            {
                var lineContent = fileContent[i].Split(',');

                var email = lineContent[0];
                var roles = lineContent[1];

                if (string.IsNullOrEmpty(roles.Trim()))
                    continue;

                foreach (var role in roles.Split('|'))
                {
                    if (!rolesData.ContainsKey(role))
                    {
                        rolesData.Add(role, new List<string>());
                    }

                    rolesData[role].Add(email);
                }
            }

            return rolesData;
        }

        private Task UpdateRole(string role, List<string> users)
        {
            JsonPatchDocument patch = new JsonPatchDocument();

            foreach (var user in users)
            {
                patch.Add("/subjects/-", new { username = user });
            }

            var serializedPatch = JsonConvert.SerializeObject(patch);

            return this._apiClient.PatchAsync(_tenantCode, "PRD", "Security", "AuthorizationRole", role, serializedPatch);
        }
    }
}
