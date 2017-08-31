using Client.Exceptions;
using Client.Extensions;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Dynamic.Core;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace Client
{
    public class CosmosDbClient
    {
        private readonly string _collectionId;
        private readonly string _microserviceName;
        private readonly string _databaseId;
        private readonly string _namespace;
        private readonly Uri _collectionUri;
        private readonly DocumentClient _documentClient;
        private readonly JsonSerializer _jsonSerializer;

        public CosmosDbClient(Uri serviceEndpoint,
                              string authKey,
                              string databaseId,
                              string collectionId,
                              string[] preferredLocations,
                              string microserviceName = null)
        {
            serviceEndpoint.EnsureNotNull(nameof(serviceEndpoint));
            authKey.EnsureExists(nameof(authKey));
            databaseId.EnsureExists(nameof(databaseId));
            collectionId.EnsureExists(nameof(collectionId));
            preferredLocations.EnsureHasElements(nameof(preferredLocations));

            _databaseId = databaseId;
            _collectionId = collectionId;
            _microserviceName = string.IsNullOrWhiteSpace(microserviceName) ? string.Empty : microserviceName.Trim();
            _namespace = string.IsNullOrWhiteSpace(_microserviceName)
                ? string.Empty
                : string.Concat(_microserviceName, ".");

            if (_microserviceName.Contains("."))
            {
                throw new CosmosDbConfigurationException($"The {nameof(microserviceName)} cannot contain a period.");
            }
            _collectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);

#if DEBUG
            // if we are debugging locally and are using the documentdb emulator we have to turn off endpoint discovery
            var connectionPolicy = new ConnectionPolicy { EnableEndpointDiscovery = false };

#else
            var connectionPolicy = new ConnectionPolicy();

            // if we are in dev/stg/prod then we want to enable preferred locations for geo-distribution
            foreach (var preferredLocation in preferredLocations)
            {
                connectionPolicy.PreferredLocations.Add(preferredLocation);
            }
#endif
            _documentClient = new DocumentClient(serviceEndpoint, authKey, connectionPolicy);

            _jsonSerializer = new JsonSerializer { ContractResolver = new InternalPropertyContractResolver() };
        }

        public async Task DeleteByIdAsync<T>(string id)
        {
            // get the entity by id. this will perform input validation and namespace validation for us
            var entity = GetByIdAsync<T>(id);

            // generate a self-link of the document
            var selflink = UriFactory.CreateDocumentUri(_databaseId, _collectionId, id);

            // delete the document
            await _documentClient.DeleteDocumentAsync(selflink);
        }

        public async Task<T> GetByIdAsync<T>(string id)
        {
            // ensure the id is present
            id.EnsureExists(nameof(id));

            // try to fetch the document by id
            DocumentResponse<JObject> documentResponse;

            try
            {
                documentResponse = await _documentClient.ReadDocumentAsync<JObject>(UriFactory.CreateDocumentUri(_databaseId, _collectionId, id));
            }
            catch (DocumentClientException ex)
            {
                // if the document was not found throw our own exception
                if (ex.Message.Contains("Resource Not Found"))
                {
                    throw new EntityNotFoundException($"An entity with id {id} was not found in the data store.");
                }

                throw;
            }

            var jObject = documentResponse.Document;

            // if the document does not have an EntityType defined then just return it b/c we can't validate the namespace the only way the entity type
            // property would be missing is if the document was saved manually outside of this CosmosDbClient class, because our Save method injects it
            // every time. This is just here for safety.
            if (jObject["EntityType"] == null)
            {
                return jObject.ToObject<T>(_jsonSerializer);
            }

            // get the document's entity type
            var entityType = jObject.Value<string>("EntityType");

            // parse out the document's namespace from the entity type (any text before the (.) character)
            var documentNamespace = string.Empty;

            if (entityType.Contains("."))
            {
                documentNamespace = entityType.Substring(0, entityType.IndexOf(".", StringComparison.Ordinal));
            }

            // if the document's namespace matches the configured microservice name then return it
            if (documentNamespace.Equals(_microserviceName))
            {
                return jObject.ToObject<T>(_jsonSerializer);
            }

            // the document's namespace doesn't match the configured microservice name so throw an error
            throw new EntityNotFoundException($"An entity with id {id} was found in the data store but is not in the '{_microserviceName}' namespace.");
        }

        public T Get<T>(Expression<Func<T, bool>> predicate)
        {
            // validate input param(s)
            predicate.EnsureNotNull(nameof(predicate));

            // use the documentdb client to generate the IOrderedQueryable<T> using it's linq provider
            var documentQuery = CreateDocumentQuery(predicate);

            // extract just the query from the CosmosDB IQueryable<T>
            var dynamicQuery = JsonConvert.DeserializeObject<dynamic>(documentQuery.ToString());
            var queryText = (string)dynamicQuery.query;

            // if any of the predicate conditions contain the "Id" property we need to change the case to "id" so documentdb will work properly
            queryText = CorrectCasingOfDocumentQuery(queryText);

            // inject the namespace predicate to prevent us from reading other microservice's documents
            queryText = $"{queryText} AND root[\"EntityType\"] = '{_namespace}{typeof(T).Name}'";

            // execute the query without using generics so that we can just get a JObject back
            var jObject = Enumerable.AsEnumerable(_documentClient.CreateDocumentQuery<JObject>(_collectionUri, queryText))
                .FirstOrDefault();

            return null == jObject ? default(T) : jObject.ToObject<T>(_jsonSerializer);
        }

        private static string CorrectCasingOfDocumentQuery(string documentQuery)
        {
            if (documentQuery.Contains("root[\"Id\"]"))
            {
                documentQuery = documentQuery.Replace("root[\"Id\"]", "root[\"id\"]");
            }
            return documentQuery;
        }

        private IQueryable<T> CreateDocumentQuery<T>(Expression<Func<T, bool>> scopedWhereClause)
        {
            return _documentClient.CreateDocumentQuery<T>(_collectionUri).Where(scopedWhereClause);
        }

        public (IEnumerable<T> data, int totalCount, int totalPages) List<T>(int page, int pageSize, string sortBy, Expression<Func<T, bool>> predicate)
        {
            // validate input param(s)
            page.EnsureGreaterThan(0, nameof(page));
            pageSize.EnsureGreaterThan(0, nameof(pageSize));
            sortBy.EnsureExists(nameof(sortBy));
            predicate.EnsureNotNull(nameof(predicate));

            int totalCount;

            // use the cosmos client to generate the IOrderedQueryable<T> using it's linq provider
            var documentQuery = CreateDocumentQuery(predicate);

            // add the dynamic order by clause to the document query
            documentQuery = AddDynamicOrderByClause(documentQuery, sortBy);

            // extract just the query from the CosmosDB IQueryable<T>
            var dynamicQuery = JsonConvert.DeserializeObject<dynamic>(documentQuery.ToString());
            var queryText = (string)dynamicQuery.query;

            // if any of the predicate conditions contain the "Id" property we need to change the case to "id" so documentdb will work properly
            queryText = CorrectCasingOfDocumentQuery(queryText);

            // inject the namespace predicate to prevent us from reading other microservice's documents since this query has an ORDER BY claus, we have
            // to inject the code just before the ORDER BY
            var queryParts = queryText.Split(new[] { "ORDER BY" }, StringSplitOptions.None);

            queryText = $"{queryParts[0]} AND root[\"EntityType\"] = '{_namespace}{typeof(T).Name}' ORDER BY {queryParts[1]}";

            // query for just the Ids of the documents that match the where clause so that we can get the total number of documents
            var ids = QueryForIds(queryText);

            // capture the count of how many documents match the WHERE clause
            totalCount = ids.Count;

            // calculate how many docs we need to skip in the final query
            var skip = (page - 1) * pageSize;

            // grab the order by clause from the stringifiedQuery so that we can apply it to the final query
            var startIndex = queryText.IndexOf("ORDER BY", StringComparison.Ordinal);
            var length = queryText.Length - startIndex - 1;
            var orderByClause = queryText.Substring(startIndex, length);

            var finalQuery =
                $"SELECT * FROM root WHERE root.id IN ('{string.Join("','", ids.Skip(skip).Take(pageSize))}')" + " " + orderByClause.Replace("\\\"", "'");

            var jObjects = _documentClient.CreateDocumentQuery<JObject>(_collectionUri, finalQuery).ToList();

            var dataList = jObjects.Select(jObject => jObject.ToObject<T>(_jsonSerializer)).ToList();

            // calculate the number of total pages
            var actualPages = totalCount / (double)pageSize;
            var totalPages = (int)Math.Ceiling(actualPages);

            return (data: dataList, totalCount: totalCount, totalPages: totalPages);
        }

        private List<string> QueryForIds(string documentQuery)
        {
            var options = new FeedOptions
            {
                MaxItemCount = int.MaxValue
            };

            // we want to change the query to return only the value of the id property, not the property itself (so we get an IEnumerable<string> of
            // ids back instead of an IEnumerable<dynamic> that has an id property and a value)
            var idStringQuery = documentQuery.Replace("SELECT * FROM", "SELECT VALUE root.id FROM");

            // get a list of ids that match the query
            return _documentClient.CreateDocumentQuery<string>(_collectionUri, idStringQuery, options)
                .ToList();
        }

        private static IQueryable<T> AddDynamicOrderByClause<T>(IQueryable<T> documentQuery, string sortBy)
        {
            string propertyName;

            // cosmos db only supports sorting by one column so just grab the first item
            var sortColumn = sortBy.Split(',')[0];

            // if the sort option starts with "-" we order descending, otherwise ascending
            if (sortColumn.StartsWith("-", StringComparison.Ordinal))
            {
                propertyName = sortColumn.Remove(0, 1).ToTitleCase();
                documentQuery = documentQuery.OrderBy($"{propertyName} DESC");
            }
            else
            {
                propertyName = sortColumn.ToTitleCase();
                documentQuery = documentQuery.OrderBy(propertyName);
            }

            return documentQuery;
        }

        public async Task SaveAsync(object data)
        {
            // validate input param(s)
            data.EnsureNotNull(nameof(data));

            // if there is an "Id" property we need to change it to "id" so that it works with Azure Document Db
            var jObject = JObject.FromObject(data, _jsonSerializer);
            jObject["Id"]?.Rename("id");

            // inject the entity type here including the microservice namespace
            var entityType = data.GetType().Name;
            jObject["EntityType"] = $"{_namespace}{entityType}";

            await _documentClient.UpsertDocumentAsync(_collectionUri, jObject).ConfigureAwait(false);
        }

        private sealed class InternalPropertyContractResolver : DefaultContractResolver
        {
            protected override IList<JsonProperty> CreateProperties(Type type, MemberSerialization memberSerialization)
            {
                var jsonProperties = new List<JsonProperty>();
                var properties = type.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);

                foreach (var property in properties)
                {
                    var jsonProperty = CreateProperty(property, memberSerialization);
                    jsonProperty.Writable = HasSetMethod(property);
                    jsonProperty.Readable = true;
                    jsonProperties.Add(jsonProperty);
                }

                return jsonProperties;
            }

            private static bool HasSetMethod(PropertyInfo propertyInfo)
            {
                return propertyInfo.GetSetMethod(true) != null;
            }
        }
    }
}