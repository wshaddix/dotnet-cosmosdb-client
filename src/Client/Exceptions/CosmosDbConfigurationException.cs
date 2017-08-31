using System;

namespace Client
{
    public class CosmosDbConfigurationException : Exception
    {
        public CosmosDbConfigurationException(string message) : base(message)
        {
        }
    }
}