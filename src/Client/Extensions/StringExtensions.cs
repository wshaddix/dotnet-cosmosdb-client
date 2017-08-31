using System;

namespace Client.Extensions
{
    public static class StringExtensions
    {
        public static void EnsureExists(this string value, string variableName)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new ArgumentException($"{variableName} cannot be null or empty");
            }
        }

        public static bool Exists(this string value)
        {
            return !string.IsNullOrWhiteSpace(value);
        }

        public static string ToTitleCase(this string str)
        {
            var tokens = str.Split(new[] { " " }, StringSplitOptions.RemoveEmptyEntries);
            for (var i = 0; i < tokens.Length; i++)
            {
                var token = tokens[i];
                tokens[i] = token.Substring(0, 1).ToUpper() + token.Substring(1).ToLower();
            }

            return string.Join(" ", tokens);
        }
    }
}