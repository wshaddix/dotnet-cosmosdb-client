using System;

namespace Client.Extensions
{
    public static class IntegerExtensions
    {
        public static void EnsureGreaterThan(this int value, int comparedTo, string name)
        {
            if (value <= comparedTo)
            {
                throw new ArgumentOutOfRangeException($"{name} must be greater than {comparedTo}");
            }
        }

        public static void EnsureGreaterThanOrEqualTo(this int value, int comparedTo, string name)
        {
            if (value < comparedTo)
            {
                throw new ArgumentOutOfRangeException($"{name} must be greater than or equal to {comparedTo}");
            }
        }
    }
}