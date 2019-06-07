using System.Collections.Generic;

namespace OmniaMigrationTool.Extensions
{
    public static class DictionaryExtensions
    {
        public static void Merge<TKey, TValue>(this IDictionary<TKey, TValue> target, IDictionary<TKey, TValue> source)
        {
            if (source == null || target == null)
                return;

            foreach (var item in source)
            {
                if (!target.ContainsKey(item.Key))
                    target.Add(item.Key, item.Value);
                else
                    target[item.Key] = item.Value;
            }
        }
    }
}
