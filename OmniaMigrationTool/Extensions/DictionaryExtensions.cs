using System.Collections.Generic;

namespace OmniaMigrationTool.Extensions
{
    public static class DictionaryExtensions
    {
        public static void MoveTo(this Dictionary<string, EntityMapDefinition> source, string parentKey, string childKey)
        {
            source[parentKey].Items.Add(source[childKey]);
            source.Remove(childKey);
        }
    }
}