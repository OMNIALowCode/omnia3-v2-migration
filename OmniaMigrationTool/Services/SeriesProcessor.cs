using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Linq;
using Omnia.Libraries.GenericExtensions;

namespace OmniaMigrationTool.Services
{
    internal class SeriesProcessor
    {
        private readonly IList<EntityMapDefinition> _definitions;
        private readonly Guid _sourceTenant;
        private readonly JsonSerializerSettings _jsonSettings;
        private readonly string _correlationId;
        private readonly string _eventMetadata;

        public SeriesProcessor(IList<EntityMapDefinition> definitions, string sourceTenant, JsonSerializerSettings jsonSettings, string correlationId, string eventMetadata)
        {
            _definitions = definitions;
            _sourceTenant = Guid.Parse(sourceTenant);
            _jsonSettings = jsonSettings;
            _correlationId = correlationId;
            _eventMetadata = eventMetadata;
        }

        public async Task ProcessAsync(string outputPath, SqlConnection conn, StreamWriter eventStoreStream)
        {
            var series = await ProcessAsync(conn);

            var groupedSeries = series.Where(num => _definitions.Any(d => d.TargetCode == num.TypeCode)).GroupBy(g => g.TypeCode);

            foreach (var group in groupedSeries)
            {
                var serieClassifier = $"{group.Key}Serie";

                using (var fs = new FileStream(Path.Combine(outputPath, $"{serieClassifier.ToSnakeCase()}.csv"), FileMode.OpenOrCreate, FileAccess.Write))
                {
                    using (var entityStream = new StreamWriter(fs))
                    {
                        await entityStream.WriteLineAsync("identifier,version,body,created_at,updated_at,_last_number_used");

                        foreach (var mapping in group)
                        {
                            var serieCode = $"{mapping.CompanyCode}_{mapping.ShortCode}";
                            var entityId = Guid.NewGuid();
                            var eventMessage = $@"'{group.Key}Serie' with code '{serieCode}' has been migrated";
                            var targetDataObject = new
                            {
                                _code = serieCode,
                                _name = serieCode,
                                _description = "Serie imported using Omnia Migration Tool",
                                _inactive = true, //Because the series from data imported shouldn't be used
                                _startingNumber = mapping.LastUsedValue
                            };

                            var data = JsonConvert.SerializeObject(targetDataObject, _jsonSettings).Replace("\"", "\"\"");

                            var eventData = $@"{{""""data"""":{data},""""classifier"""":""""{serieClassifier}"""",""""entityId"""":""""{entityId}"""",""""identifier"""":""""{serieCode}"""",""""layer"""":""""business"""", """"message"""":""""{eventMessage}"""",""""version"""":1}}";

                            await eventStoreStream.WriteLineAsync($@"{Guid.NewGuid()},{DateTime.UtcNow.ToString("yyyy-MM-dd hh:mm:ss.ff")},migrationtool@omnia,{entityId},{serieClassifier},{serieCode},false,1,""{eventData}"",""{_eventMetadata}"",{eventMessage},{_correlationId}");
                            await entityStream.WriteLineAsync($"{serieCode},1,\"{data}\",{DateTime.UtcNow.ToString("yyyy-MM-dd hh:mm:ss.ff")},{DateTime.UtcNow.ToString("yyyy-MM-dd hh:mm:ss.ff")},{targetDataObject._startingNumber}");
                        }
                    }
                }
            }
        }

        private async Task<IList<Numerator>> ProcessAsync(SqlConnection conn)
        {
            var result = new List<Numerator>();
            using (var command = new SqlCommand(Queries.SourceQueries.NumeratorsQuery(_sourceTenant), conn))
            {
                try
                {
                    command.CommandTimeout = 360;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            result.Add(new Numerator
                            {
                                CompanyCode = reader.GetString(reader.GetOrdinal("CompanyCode")),
                                ShortCode = reader.GetString(reader.GetOrdinal("ShortCode")),
                                TypeCode = reader.GetString(reader.GetOrdinal("TypeCode")),
                                LastUsedValue = reader.GetInt64(reader.GetOrdinal("LastUsedValue"))
                            });
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

        private class Numerator
        {
            public string TypeCode { get; set; }

            public string CompanyCode { get; set; }

            public string ShortCode { get; set; }

            public long LastUsedValue { get; set; }
        }
    }
}