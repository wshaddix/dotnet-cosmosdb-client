using System;
using Newtonsoft.Json;

namespace Client.Extensions
{
    public static class ObjectExtensions
    {
        public static T DeepCopy<T>(this object instance)
        {
            instance.EnsureNotNull(nameof(instance));

            var serializedObject = JsonConvert.SerializeObject(instance);
            return JsonConvert.DeserializeObject<T>(serializedObject);
        }

        public static void EnsureNotNull(this object instance, string className)
        {
            if (null == instance)
            {
                throw new ArgumentNullException(className);
            }
        }

        public static bool IsNull(this object instance)
        {
            return null == instance;
        }
    }
}