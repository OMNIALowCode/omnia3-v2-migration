using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using OmniaMigrationTool.Queries;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;
using static OmniaMigrationTool.EntityMapDefinition;

namespace OmniaMigrationTool.Services
{
    internal class TemplateService
    {
        private static readonly Dictionary<string, string> v2KindMapper = new Dictionary<string, string>
        {
            { "MisEntityItem", "GenericEntity" },
            { "UserDefinedEntity", "GenericEntity" },
            { "Resource", "Resource" },
            { "Interaction", "Document" },
            { "Event", "Event" },
            { "Commitment", "Commitment" },
            { "Agent", "Agent" },
        };

        private readonly Guid _sourceTenant;
        private readonly string _connectionString;

        public TemplateService(string tenant, string connectionString)
        {
            _sourceTenant = Guid.Parse(tenant);
            _connectionString = connectionString;
        }

        public async Task GenerateTemplate()
        {
            var definitions = new List<EntityMapDefinition>();

            using (var conn = new SqlConnection(_connectionString))
            {
                await conn.OpenAsync();

                await GetEntities(definitions, conn);
                //await GetEntityAttributes(definitions, conn);
            }

            await File.WriteAllTextAsync(new TempDirectory().Path, JsonConvert.SerializeObject(definitions, new JsonSerializerSettings
            {
                ContractResolver = new CamelCasePropertyNamesContractResolver(),
                Formatting = Formatting.Indented
            }).Replace("\"", "\"\""));
        }

        private async Task GetEntities(List<EntityMapDefinition> definitions, SqlConnection conn)
        {
            using (var command = new SqlCommand(TemplateQueries.EntityQuery(_sourceTenant), conn))
            {
                try
                {
                    command.CommandTimeout = 360;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var typeCode = reader.GetString(reader.GetOrdinal("TypeCode"));
                            var typeKind = reader.GetString(reader.GetOrdinal("Kind"));
                            definitions.Add(new EntityMapDefinition(
                                typeCode,
                                typeKind,
                                typeCode,
                                v2KindMapper[typeKind],
                                new List<AttributeMap>()
                                ));
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.GetType().ToString());
                    Console.WriteLine(ex.Message);
                }
            }
        }

        private async Task GetEntityAttributes(List<EntityMapDefinition> definitions, SqlConnection conn)
        {
            using (var command = new SqlCommand(TemplateQueries.AttributesQuery(_sourceTenant), conn))
            {
                try
                {
                    command.CommandTimeout = 360;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var typeCode = reader.GetString(reader.GetOrdinal("TypeCode"));
                            var typeKind = reader.GetString(reader.GetOrdinal("Kind"));
                            definitions.Add(new EntityMapDefinition(
                                typeCode,
                                typeKind,
                                typeCode,
                                v2KindMapper[typeKind],
                                new List<AttributeMap>()
                                ));
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.GetType().ToString());
                    Console.WriteLine(ex.Message);
                }
            }
        }
    }
}