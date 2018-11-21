using System;
using System.Collections.Generic;
using System.Text;

namespace OmniaMigrationTool
{
    internal class EntityMapDefinition
    {

        public EntityMapDefinition(string sourceKind, string sourceCode, string targetKind, string targetCode, IList<AttributeMap> attributes)
        : this(sourceKind, sourceCode, targetKind, targetCode, attributes, new List<EntityMapDefinition>())
        {

        }
        public EntityMapDefinition(string sourceKind, string sourceCode, string targetKind, string targetCode, IList<AttributeMap> attributes, IList<EntityMapDefinition> items)
        {
            SourceKind = sourceKind;
            SourceCode = sourceCode;
            TargetKind = targetKind;
            TargetCode = targetCode;
            Attributes = attributes;
            Items = items;
        }

        public string SourceKind { get; }
        public string SourceCode { get; }

        public string TargetKind { get; }
        public string TargetCode { get; }
        public IList<AttributeMap> Attributes { get; }

        public IList<EntityMapDefinition> Items { get; }

        internal class AttributeMap
        {
            public AttributeMap(string source, string target)
            {
                Source = source;
                Target = target;
            }

            public string Source { get; }
            public string Target { get; }
        }
    }


}
