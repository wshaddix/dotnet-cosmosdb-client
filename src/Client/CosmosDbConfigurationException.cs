using System;

namespace Client
{
    [FullyCovered]
    public class CosmosDbConfigurationException : Exception
    {
        public CosmosDbConfigurationException(string message) : base(message)
        {
            
        }
    }
}