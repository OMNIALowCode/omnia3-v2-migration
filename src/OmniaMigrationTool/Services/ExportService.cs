using Newtonsoft.Json;
using Omnia.Libraries.GenericExtensions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace OmniaMigrationTool.Services
{
    internal class ExportService
    {
        private static readonly Stopwatch Stopwatch = new Stopwatch();

        private readonly Guid _sourceTenant;
        private readonly string _connectionString;
        private readonly string _correlationId;
        private readonly string _eventMetadata;
        private readonly IList<EntityMapDefinition> _definitions;
        private readonly SeriesProcessor _seriesProcessor;
        private readonly JsonSerializerSettings _jsonSettings;

        public ExportService(string tenant, string connectionString, string correlationId, string eventMetadata, IList<EntityMapDefinition> definitions, SeriesProcessor seriesProcessor, JsonSerializerSettings jsonSettings)
        {
            _sourceTenant = Guid.Parse(tenant);
            _definitions = definitions;
            _connectionString = connectionString;
            _correlationId = correlationId;
            _eventMetadata = eventMetadata;
            _seriesProcessor = seriesProcessor;
            _jsonSettings = jsonSettings;
        }

        public async Task Export()
        {
            var tempDir = new TempDirectory();

            Console.WriteLine($"Writing to folder: {tempDir.Path}");

            Stopwatch.Start();

            await Process(tempDir.Path);

            Stopwatch.Stop();

            Console.WriteLine("Time elapsed: {0}", Stopwatch.Elapsed);
        }

        private async Task Process(string outputPath)
        {
            using (var conn = new SqlConnection(_connectionString))
            {
                await conn.OpenAsync();

                using (var fs = new FileStream(Path.Combine(outputPath, "event_store.csv"), FileMode.OpenOrCreate, FileAccess.Write))
                {
                    using (var sw = new StreamWriter(fs))
                    {
                        await sw.WriteLineAsync("id,created_at,created_by,entity_id,definition_identifier,identifier,is_removed,version,event,metadata,message,correlation_id");

                        // Process Series
                        await _seriesProcessor.ProcessAsync(outputPath, conn, sw);

                        using (var fileMappingStream = new FileStream(Path.Combine(outputPath, "file_mapping.csv"),
                            FileMode.OpenOrCreate, FileAccess.Write))
                        {
                            using (var fileMappingStreamWriter = new StreamWriter(fileMappingStream))
                            {
                                // Process Entities
                                var group = _definitions.GroupBy(g => g.TargetCode);
                                foreach (var item in group)
                                {
                                    await ProcessEntity(outputPath, conn, item.AsEnumerable(), sw, fileMappingStreamWriter);
                                }
                            }
                        }
                    }
                }
            }
        }

        private async Task ProcessEntity(string outputPath, SqlConnection conn, IEnumerable<EntityMapDefinition> definitions, StreamWriter eventStoreStream, StreamWriter fileMappingStream)
        {
            var mappingCollection = new List<Dictionary<string, object>>();

            var targetCode = definitions.First().TargetCode;

            foreach (var definition in definitions)
            {
                var mappingResult = await MapEntity(conn, definition, fileMappingStream);

                foreach (var result in mappingResult)
                {
                    var elementInCollection = mappingCollection.FirstOrDefault(m => m["_code"].Equals(result["_code"]));
                    if (elementInCollection == null)
                        mappingCollection.Add(result);
                    else
                        elementInCollection.Merge(result);
                }
            }

            using (var fs = new FileStream(Path.Combine(outputPath, $"{targetCode.ToSnakeCase()}.csv"), FileMode.OpenOrCreate, FileAccess.Write))
            {
                using (var entityStream = new StreamWriter(fs))
                {
                    await entityStream.WriteLineAsync("identifier,version,body,created_at,updated_at");

                    foreach (var mapping in mappingCollection)
                    {
                        var entityId = Guid.NewGuid();
                        var eventMessage = $@"'{targetCode}' with code '{mapping["_code"]}' has been migrated";
                        var data = JsonConvert.SerializeObject(mapping, _jsonSettings).Replace("\"", "\"\"");

                        var eventData = $@"{{""""data"""":{data},""""classifier"""":""""{targetCode}"""",""""entityId"""":""""{entityId}"""",""""identifier"""":""""{mapping["_code"]}"""",""""layer"""":""""business"""", """"message"""":""""{eventMessage}"""",""""version"""":1}}";

                        await eventStoreStream.WriteLineAsync($@"{Guid.NewGuid()},{DateTime.UtcNow.ToString("yyyy-MM-dd hh:mm:ss.ff")},migrationtool@omnia,{entityId},{targetCode},{mapping["_code"]},false,1,""{eventData}"",""{_eventMetadata}"",{eventMessage},{_correlationId}");
                        await entityStream.WriteLineAsync($"{mapping["_code"]},1,\"{data}\",{DateTime.UtcNow.ToString("yyyy-MM-dd hh:mm:ss.ff")},{DateTime.UtcNow.ToString("yyyy-MM-dd hh:mm:ss.ff")}");
                    }
                }
            }
        }

        private async Task<IList<Dictionary<string, object>>> MapEntity(SqlConnection conn, EntityMapDefinition definition, StreamWriter fileMappingStream)
        {
            var currentNumber = 1;

            var result = new List<Dictionary<string, object>>();
            var itemDictionary = new Dictionary<string, List<ItemProcessed>>();
            var commitmentDictionary = new Dictionary<string, List<ItemProcessed>>();
            var eventDictionary = new Dictionary<string, List<ItemProcessed>>();
            

            foreach (var item in definition.Items)
                itemDictionary.Add(item.SourceCode, await GetItems(conn, item, definition, fileMappingStream));

            foreach (var item in definition.Commitments)
                commitmentDictionary.Add(item.SourceCode, await GetTransactionalEntity(conn, item, definition, fileMappingStream));

            foreach (var item in definition.Events)
                eventDictionary.Add(item.SourceCode, await GetTransactionalEntity(conn, item, definition, fileMappingStream));

            var cardinalityDictionary = await GetAttributesWithCardinalityN(definition.SourceCode, conn);
            var approvalTrailDictionary = await GetApprovalTrails(conn, definition.SourceCode, definition.Trail, definition, fileMappingStream);

            using (var command = new SqlCommand(
                Queries.SourceQueries.EntityQuery(_sourceTenant,
                    definition.SourceKind,
                    definition.Attributes.Where(att => att.SourceCardinality == "1").Select(c => MapSourceColumn(c.Source)).Distinct().ToArray()), conn))
            {
                command.Parameters.Add(new SqlParameter("@code", definition.SourceCode));
                try
                {
                    command.CommandTimeout = 360;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var sourceEntityId = reader.GetInt64(reader.GetOrdinal("ID"));
                            var mapping = new Dictionary<string, object>();

                            foreach (var attribute in definition.Attributes)
                            {
                                if (attribute.SourceCardinality == "1")
                                    MapAttribute(mapping, reader, attribute);
                                else if (cardinalityDictionary.ContainsKey(sourceEntityId))
                                    mapping[attribute.Target] = cardinalityDictionary[sourceEntityId][attribute.Source];
                            }

                            foreach (var item in definition.Items)
                            {
                                var data = itemDictionary[item.SourceCode].Where(i => i.ParentId.Equals(sourceEntityId))
                                    .Select(i => i.Data);

                                foreach (var parentAttribute in item.Attributes.Where(a => a.Source.StartsWith("Parent.")))
                                {
                                    foreach (var dataElement in data)
                                    {
                                        var attributeName = parentAttribute.Source.Split('.')[1];
                                        if (mapping.ContainsKey(attributeName))
                                            dataElement[parentAttribute.Target] = mapping[attributeName];
                                    }
                                }

                                mapping[item.TargetCode] = data;
                            }

                            foreach (var item in definition.Commitments)
                            {
                                var data = commitmentDictionary[item.SourceCode].Where(i => i.ParentId.Equals(sourceEntityId))
                                    .Select(i => i.Data);

                                foreach (var parentAttribute in item.Attributes.Where(a => a.Source.StartsWith("Parent.")))
                                {
                                    foreach (var dataElement in data)
                                    {
                                        var attributeName = parentAttribute.Source.Split('.')[1];
                                        if (mapping.ContainsKey(attributeName))
                                            dataElement[parentAttribute.Target] = mapping[attributeName];
                                    }
                                }

                                mapping[item.TargetCode] = data;
                            }

                            foreach (var item in definition.Events)
                            {
                                var data = eventDictionary[item.SourceCode].Where(i => i.ParentId.Equals(sourceEntityId))
                                    .Select(i => i.Data);

                                foreach (var parentAttribute in item.Attributes.Where(a => a.Source.StartsWith("Parent.")))
                                {
                                    foreach (var dataElement in data)
                                    {
                                        var attributeName = parentAttribute.Source.Split('.')[1];
                                        if (mapping.ContainsKey(attributeName))
                                            dataElement[parentAttribute.Target] = mapping[attributeName];
                                    }
                                }

                                mapping[item.TargetCode] = data;
                            }

                            if (definition.Trail != null && approvalTrailDictionary.ContainsKey(sourceEntityId))
                            {
                                mapping[definition.Trail.TargetCode] = approvalTrailDictionary[sourceEntityId];
                            }

                            // Rewrite series in case of documents
                            if (definition.TargetKind.EqualsIgnoringCase("Document") && definition.SourceKind.EqualsIgnoringCase("Interaction"))
                            {
                                if (mapping.ContainsKey("_serie"))
                                {
                                    mapping["_serie"] = $"{reader.GetString(reader.GetOrdinal("CompanyCode"))}_{mapping["_serie"]}";
                                    mapping["_code"] = $"{mapping["_serie"]}{mapping["_number"]}";
                                }
                                else if (!mapping.ContainsKey("_code"))
                                    mapping["_code"] = Guid.NewGuid().ToString("N");
                            }
                            else if (definition.TargetKind.EqualsIgnoringCase("Document"))
                            {
                                mapping["_serie"] = "A";
                                mapping["_number"] = currentNumber++;
                            }

                            foreach (var attribute in definition.Attributes.Where(a =>
                                a.TargetType.Equals(EntityMapDefinition.AttributeMap.AttributeType.File)))
                                if (mapping.ContainsKey(attribute.Target))
                                    mapping[attribute.Target] = await MapFile(mapping[attribute.Target]?.ToString(), definition.TargetCode,
                                    mapping["_code"]?.ToString(), fileMappingStream);

                            result.Add(mapping);
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

        private async Task<List<ItemProcessed>> GetItems(SqlConnection conn, EntityMapDefinition definition, EntityMapDefinition parentDefinition, StreamWriter fileMappingStream)
        {
            Console.WriteLine($"GetItems: {definition.SourceCode}");

            var result = new List<ItemProcessed>();
            using (var command = new SqlCommand(
                Queries.SourceQueries.EntityQuery(_sourceTenant,
                    definition.SourceKind,
                    definition.Attributes.Where(att => att.SourceCardinality == "1")
                        .Select(c => MapSourceColumn(c.Source))
                        .Distinct()
                        .ToArray()
                        ), conn))
            {
                command.Parameters.Add(new SqlParameter("@code",
                    definition.SourceCode
                ));

                command.CommandTimeout = 360;

                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var mapping = new Dictionary<string, object>();

                        foreach (var attribute in definition.Attributes)
                            MapAttribute(mapping, reader, attribute);

                        foreach (var attribute in definition.Attributes.Where(a =>
                            a.TargetType.Equals(EntityMapDefinition.AttributeMap.AttributeType.File)))
                            if (mapping.ContainsKey(attribute.Target))
                                mapping[attribute.Target] = await MapFile(mapping[attribute.Target]?.ToString(), parentDefinition.TargetCode,
                                mapping["_code"]?.ToString(), fileMappingStream);

                        if (!mapping.ContainsKey("_name") && mapping.ContainsKey("_code"))
                            mapping.Add("_name", mapping["_code"]);

                        var parentId = reader.GetInt64(reader.GetOrdinal("MisEntityID"));
                        result.Add(new ItemProcessed(parentId, mapping));
                    }
                }
            }

            return result;
        }

        private async Task<List<ItemProcessed>> GetTransactionalEntity(SqlConnection conn, EntityMapDefinition definition, EntityMapDefinition parentDefinition, StreamWriter fileMappingStream)
        {
            var result = new List<ItemProcessed>();
            using (var command = new SqlCommand(
                Queries.SourceQueries.TransactionalEntityQuery(_sourceTenant,
                    definition.SourceKind,
                    definition.Attributes.Where(att => att.SourceCardinality == "1")
                        .Select(c => MapSourceColumn(c.Source))
                        .Distinct()
                        .ToArray()
                ), conn))
            {
                command.Parameters.Add(new SqlParameter("@code",
                    definition.SourceCode
                ));

                command.CommandTimeout = 360;

                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var mapping = new Dictionary<string, object>();

                        foreach (var attribute in definition.Attributes)
                            MapAttribute(mapping, reader, attribute);

                        foreach (var attribute in definition.Attributes.Where(a =>
                            a.TargetType.Equals(EntityMapDefinition.AttributeMap.AttributeType.File)))
                            if (mapping.ContainsKey(attribute.Target))
                                mapping[attribute.Target] = await MapFile(mapping[attribute.Target]?.ToString(), parentDefinition.TargetCode,
                                mapping["_code"]?.ToString(), fileMappingStream);

                        var parentId = reader.GetInt64(reader.GetOrdinal("InteractionID"));
                        result.Add(new ItemProcessed(parentId, mapping));
                    }
                }
            }

            return result;
        }

        private static string MapSourceColumn(string sourceColumnName)
        {
            if (sourceColumnName.StartsWith("Parent."))
                return sourceColumnName.Replace("Parent.", "");

            return sourceColumnName;
        }

        private async Task<Dictionary<long, Dictionary<string, List<string>>>> GetAttributesWithCardinalityN(string sourceCode, SqlConnection conn)
        {
            var result = new Dictionary<long, Dictionary<string, List<string>>>();
            using (var command = new SqlCommand(
                Queries.SourceQueries.CardinalityQuery(_sourceTenant), conn))
            {
                command.Parameters.Add(new SqlParameter("@code",
                    sourceCode
                ));

                command.CommandTimeout = 360;

                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var parentId = reader.GetInt64(reader.GetOrdinal("MisEntityID"));
                        var name = reader.GetString(reader.GetOrdinal("Name"));
                        var code = reader.GetString(reader.GetOrdinal("Code"));
                        if (result.ContainsKey(parentId))
                        {
                            var entity = result[parentId];
                            if (entity.ContainsKey(name))
                                entity[name].Add(code);
                            else
                                entity[name] = new List<string> { code };
                        }
                        else
                            result.Add(parentId, new Dictionary<string, List<string>>
                            {
                                { name, new List<string> { code } }
                            });
                    }
                }
            }

            return result;
        }

        private async Task<Dictionary<long, List<Dictionary<string, object>>>> GetApprovalTrails(SqlConnection conn, string sourceCode, EntityMapDefinition trail, EntityMapDefinition parentDefinition, StreamWriter fileMappingStream)
        {
            var result = new Dictionary<long, List<Dictionary<string, object>>>();

            if (trail == null)
                return result;

            using (var command = new SqlCommand(
                Queries.SourceQueries.ApprovalTrailQuery(_sourceTenant), conn))
            {
                command.Parameters.Add(new SqlParameter("@code",
                    sourceCode
                ));

                command.CommandTimeout = 360;

                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var parentId = reader.GetInt64(reader.GetOrdinal("ParentID"));

                        var mapping = new Dictionary<string, object>();

                        foreach (var attribute in trail.Attributes)
                            MapAttribute(mapping, reader, attribute);

                        if (!mapping.ContainsKey("_code"))
                            mapping.Add("_code", Guid.NewGuid());

                        if (!mapping.ContainsKey("_name"))
                            mapping.Add("_name", $"Approval_{mapping["_code"]}");

                        foreach (var attribute in trail.Attributes.Where(a =>
                            a.TargetType.Equals(EntityMapDefinition.AttributeMap.AttributeType.File)))
                            if (mapping.ContainsKey(attribute.Target))
                                mapping[attribute.Target] = await MapFile(mapping[attribute.Target]?.ToString(), parentDefinition.TargetCode,
                                    mapping["_code"]?.ToString(), fileMappingStream);

                        if (result.ContainsKey(parentId))
                            result[parentId].Add(mapping);
                        else
                            result.Add(parentId, new List<Dictionary<string, object>> { { mapping } });
                    }
                }
            }

            return result;
        }

        private static async Task<string> MapFile(
            string value, string targetDefinitionCode, string targetCode, StreamWriter fileMappingStream)
        {
            await fileMappingStream.WriteLineAsync($"{value},{value.Replace("Binary", $"{targetDefinitionCode}/{targetCode}")}");
            return value.Replace("Binary", targetCode);
        }

        private static void MapAttribute(IDictionary<string, object> data, IDataRecord reader, EntityMapDefinition.AttributeMap attribute)
        {
            var sourceColumnName = MapSourceColumn(attribute.Source);

            if (reader.IsDBNull(reader.GetOrdinal(sourceColumnName))) return;

            switch (attribute.SourceType)
            {
                case EntityMapDefinition.AttributeMap.AttributeType.Long:
                    data.Add(attribute.Target, Map(reader.GetInt64(reader.GetOrdinal(sourceColumnName))));
                    break;

                case EntityMapDefinition.AttributeMap.AttributeType.Int:
                    data.Add(attribute.Target, Map(reader.GetInt32(reader.GetOrdinal(sourceColumnName))));
                    break;

                case EntityMapDefinition.AttributeMap.AttributeType.Decimal:
                    data.Add(attribute.Target, Map(reader.GetDecimal(reader.GetOrdinal(sourceColumnName))));
                    break;

                case EntityMapDefinition.AttributeMap.AttributeType.Date:
                    data.Add(attribute.Target, Map(reader.GetDateTime(reader.GetOrdinal(sourceColumnName))));
                    break;

                case EntityMapDefinition.AttributeMap.AttributeType.Boolean:
                    data.Add(attribute.Target, Map(reader.GetBoolean(reader.GetOrdinal(sourceColumnName))));
                    break;

                default:
                    data.Add(attribute.Target, Map(reader.GetString(reader.GetOrdinal(sourceColumnName))));
                    break;
            }

            object Map(object value)
            {
                value = MapValue(value);

                try
                {
                    switch (attribute.TargetType)
                    {
                        case EntityMapDefinition.AttributeMap.AttributeType.Int:
                            if (value is int) return value;
                            return Convert.ToInt32(value);

                        case EntityMapDefinition.AttributeMap.AttributeType.Long:
                            if (value is long) return value;
                            return Convert.ToInt64(value);

                        case EntityMapDefinition.AttributeMap.AttributeType.Decimal:
                            if (value is decimal) return value;
                            return Convert.ToDecimal(value);

                        case EntityMapDefinition.AttributeMap.AttributeType.Date:
                            if (value is DateTime) return value;
                            return Convert.ToDateTime(value);

                        case EntityMapDefinition.AttributeMap.AttributeType.Boolean:
                            if (value is bool) return value;
                            return Convert.ToBoolean(Convert.ToInt16(value));

                        default:
                            if (attribute.Target.Equals("_code"))
                                return value.ToString().Substring(0, Math.Min(31, value.ToString().Length)); // TODO: Deal the the difference of size in codes
                            return value.ToString();
                    }
                }
                catch (SqlException)
                {
                    Console.WriteLine($"Error mapping value ´{value}´");
                    throw;
                }
            }

            object MapValue(object value)
            {
                if (attribute.ValueMapping == null) return value;
                var targetValue = attribute.ValueMapping.FirstOrDefault(v => v.Source.Equals(value))?.Target;
                return targetValue ?? value;
            }
        }

        private class ItemProcessed
        {
            public ItemProcessed(long parentId, Dictionary<string, object> data)
            {
                ParentId = parentId;
                Data = data;
            }

            public long ParentId { get; }

            public Dictionary<string, object> Data { get; }
        }
    }
}