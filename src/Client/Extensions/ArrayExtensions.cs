using System;

namespace Client.Extensions
{
    public static class ArrayExtensions
    {
        public static void EnsureHasElements(this string[] array, string variableName)
        {
            if (null == array || array.Length == 0)
            {
                throw new ArgumentException($"{variableName} cannot be null or empty");
            }
        }
    }
}