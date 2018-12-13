using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Text;

namespace OmniaMigrationTool
{
    internal class EntityMapDefinition
    {
        public EntityMapDefinition(string sourceKind, string sourceCode, string targetKind, string targetCode, IList<AttributeMap> attributes,
            IList<EntityMapDefinition> items = null,
            IList<EntityMapDefinition> commitments = null,
            IList<EntityMapDefinition> events = null
            )
        {
            SourceKind = sourceKind;
            SourceCode = sourceCode;
            TargetKind = targetKind;
            TargetCode = targetCode;
            Attributes = attributes;
            Items = items ?? new List<EntityMapDefinition>();
            Commitments = commitments ?? new List<EntityMapDefinition>();
            Events = events ?? new List<EntityMapDefinition>();
        }

        public string SourceKind { get; }

        public string SourceCode { get; }

        public string TargetKind { get; }

        public string TargetCode { get; }

        public IList<AttributeMap> Attributes { get; }

        public IList<EntityMapDefinition> Items { get; }

        public IList<EntityMapDefinition> Commitments { get; }

        public IList<EntityMapDefinition> Events { get; }

        public EntityMapDefinition Trail { get; set; }

        internal class AttributeMap
        {
            public AttributeMap(string source, string target,
                AttributeType sourceType = AttributeType.Text,
                AttributeType targetType = AttributeType.Text,
                IList<AttributeValueMap> valueMapping = null,
                string sourceCardinality = "1"
                )
            {
                Source = source;
                Target = target;
                SourceType = sourceType;
                TargetType = targetType;
                ValueMapping = valueMapping ?? new List<AttributeValueMap>();
                SourceCardinality = sourceCardinality ?? "1";
            }

            public string Source { get; }

            public string Target { get; }

            public AttributeType SourceType { get; }

            public AttributeType TargetType { get; }

            public IList<AttributeValueMap> ValueMapping { get; }

            public string SourceCardinality { get; }

            [JsonConverter(typeof(StringEnumConverter))]
            public enum AttributeType
            {
                Text,
                Int,
                Long,
                Decimal,
                Date,
                Boolean
            }

            internal class AttributeValueMap
            {
                public AttributeValueMap(object source, object target)
                {
                    Source = source;
                    Target = target;
                }

                public object Source { get; }

                public object Target { get; }
            }
        }
    }
}