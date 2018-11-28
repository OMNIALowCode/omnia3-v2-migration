using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using OmniaMigrationTool.Extensions;
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
            { "Item", "GenericEntity" },
            { "UserDefinedEntity", "GenericEntity" },
            { "Resource", "Resource" },
            { "Interaction", "Document" },
            { "Event", "Event" },
            { "Commitment", "Commitment" },
            { "Agent", "Agent" },
        };

        private static readonly Dictionary<string, AttributeMap.AttributeType> sourceTypeMapper = new Dictionary<string, AttributeMap.AttributeType>
        {
            { "ST", AttributeMap.AttributeType.Text },
            { "BO", AttributeMap.AttributeType.Boolean },
            { "DE", AttributeMap.AttributeType.Decimal },
            { "DT", AttributeMap.AttributeType.Date },
            { "IN", AttributeMap.AttributeType.Long },
            { "PS", AttributeMap.AttributeType.Text },
            { "VP", AttributeMap.AttributeType.Text },
            { "BD", AttributeMap.AttributeType.Text },
        };

        private static readonly Dictionary<string, AttributeMap.AttributeType> targetTypeMapper = new Dictionary<string, AttributeMap.AttributeType>
        {
            { "ST", AttributeMap.AttributeType.Text },
            { "BO", AttributeMap.AttributeType.Boolean },
            { "DE", AttributeMap.AttributeType.Decimal },
            { "DT", AttributeMap.AttributeType.Date },
            { "IN", AttributeMap.AttributeType.Int },
            { "PS", AttributeMap.AttributeType.Text },
            { "VP", AttributeMap.AttributeType.Text },
            { "BD", AttributeMap.AttributeType.Text },
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
            var tempDir = new TempDirectory();

            Console.WriteLine($"Writing to folder: {tempDir.Path}");

            var definitions = new Dictionary<string, EntityMapDefinition>();

            List<(string, string)> items = null;
            List<(string, string)> commitments = null;
            List<(string, string)> events = null;

            using (var conn = new SqlConnection(_connectionString))
            {
                await conn.OpenAsync();

                await GetEntities(definitions, conn);
                await GetEntityAttributes(definitions, conn);

                var itemsTask = GetItems(conn);
                var commitmentsTask = GetCommitments(conn);
                var eventsTask = GetEvents(conn);

                items = await itemsTask;
                commitments = await commitmentsTask;
                events = await eventsTask;
            }

            foreach (var item in items)
                definitions.MoveTo(item.Item1, item.Item2);

            foreach (var commitment in commitments)
                definitions.MoveTo(commitment.Item1, commitment.Item2);

            foreach (var evt in events)
                definitions.MoveTo(evt.Item1, evt.Item2);

            await File.WriteAllTextAsync(Path.Combine(tempDir.Path, $"{_sourceTenant}_mapping.json"), JsonConvert.SerializeObject(definitions.Values, new JsonSerializerSettings
            {
                ContractResolver = new CamelCasePropertyNamesContractResolver(),
                Formatting = Formatting.Indented
            }));

            Console.WriteLine("Template export finished successfully.");
        }

        private async Task GetEntities(Dictionary<string, EntityMapDefinition> definitions, SqlConnection conn)
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

                            definitions.Add(typeCode, new EntityMapDefinition(
                                typeKind,
                                typeCode,
                                v2KindMapper[typeKind],
                                typeCode,
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

        private async Task GetEntityAttributes(Dictionary<string, EntityMapDefinition> definitions, SqlConnection conn)
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
                            var code = reader.GetString(reader.GetOrdinal("Name"));
                            var dataType = reader.GetString(reader.GetOrdinal("DataType")).Substring(0, 2);

                            var cardinalityPos = reader.GetOrdinal("Cardinality");

                            if (definitions.ContainsKey(typeCode))
                                definitions[typeCode].Attributes.Add(new AttributeMap(
                                    code,
                                    this.TransformToV3Code(code),
                                    sourceTypeMapper[dataType],
                                    targetTypeMapper[dataType],
                                    sourceCardinality: reader.IsDBNull(cardinalityPos) ? null : reader.GetString(cardinalityPos)
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

        private async Task<List<(string, string)>> GetItems(SqlConnection conn)
        {
            var result = new List<(string, string)>();

            using (var command = new SqlCommand(TemplateQueries.ItemsQuery(_sourceTenant), conn))
            {
                try
                {
                    command.CommandTimeout = 360;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            result.Add((
                                reader.GetString(reader.GetOrdinal("TypeCode")),
                                reader.GetString(reader.GetOrdinal("ItemTypeCode"))
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

            return result;
        }

        private async Task<List<(string, string)>> GetCommitments(SqlConnection conn)
        {
            var result = new List<(string, string)>();

            using (var command = new SqlCommand(TemplateQueries.CommitmentsQuery(_sourceTenant), conn))
            {
                try
                {
                    command.CommandTimeout = 360;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            result.Add((
                                reader.GetString(reader.GetOrdinal("TypeCode")),
                                reader.GetString(reader.GetOrdinal("CommitmentTypeCode"))
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

            return result;
        }

        private async Task<List<(string, string)>> GetEvents(SqlConnection conn)
        {
            var result = new List<(string, string)>();

            using (var command = new SqlCommand(TemplateQueries.EventsQuery(_sourceTenant), conn))
            {
                try
                {
                    command.CommandTimeout = 360;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            result.Add((
                                reader.GetString(reader.GetOrdinal("TypeCode")),
                                reader.GetString(reader.GetOrdinal("EventTypeCode"))
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

            return result;
        }

        private string TransformToV3Code(string name)
        {
            if (name.Equals("Name", StringComparison.InvariantCulture))
                return "_name";

            if (name.Equals("Code", StringComparison.InvariantCulture))
                return "_code";

            if (name.Equals("Inactive", StringComparison.InvariantCulture))
                return "_inactive";

            if (name.Equals("Description", StringComparison.InvariantCulture))
                return "_description";

            return name;
        }
    }
}