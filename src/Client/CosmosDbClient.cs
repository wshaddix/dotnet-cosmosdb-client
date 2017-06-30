using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Dynamic.Core;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Extensions;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;

namespace Client
{
    public class CosmosDbClient
    {
        private readonly string _collectionId;
        private readonly string _databaseId;
        private readonly Uri _collectionUri;
        private readonly DocumentClient _documentClient;
        private readonly JsonSerializerSettings _jsonSerializerSettings;

        public CosmosDbClient(Uri serviceEndpoint, string authKey, string databaseId, string collectionId, string[] preferredLocations)
        {
            serviceEndpoint.EnsureNotNull(nameof(serviceEndpoint));
            authKey.EnsureExists(nameof(authKey));
            databaseId.EnsureExists(nameof(databaseId));
            collectionId.EnsureExists(nameof(collectionId));
            preferredLocations.EnsureHasElements(nameof(preferredLocations));

            _databaseId = databaseId;
            _collectionId = collectionId;

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

            _jsonSerializerSettings = new JsonSerializerSettings { ContractResolver = new InternalPropertyContractResolver() };
        }

        public async Task DeleteAsync(string id)
        {
            // validate input param(s)
            id.EnsureExists(nameof(id));

            // generate a self-link of the document
            var selflink = UriFactory.CreateDocumentUri(_databaseId, _collectionId, id);

            // delete the document
            await _documentClient.DeleteDocumentAsync(selflink);
        }

        public T Get<T>(Expression<Func<T, bool>> predicate)
        {
            // validate input param(s)
            predicate.EnsureNotNull(nameof(predicate));

            // use the documentdb client to generate the IOrderedQueryable<T> using it's linq provider
            var documentQuery = CreateDocumentQuery(predicate);

            // if any of the predicate conditions contain the "Id" property we need to change the case to "id" so documentdb will work properly
            var stringifiedQuery = CorrectCasingOfDocumentQuery(documentQuery.ToString());

            // convert the query to a dynamic so that we can easily pull just the query text
            var jsonQuery = JsonConvert.DeserializeObject<dynamic>(stringifiedQuery);

            // execute the query without using generics so that we can just get a JObject back
            var jObject = Enumerable.AsEnumerable(_documentClient.CreateDocumentQuery<JObject>(_collectionUri, (string)jsonQuery.query))
                .FirstOrDefault();

            return null == jObject ? default(T) : JsonConvert.DeserializeObject<T>(jObject.ToString(), _jsonSerializerSettings);
        }

        private static string CorrectCasingOfDocumentQuery(string documentQuery)
        {
            if (documentQuery.Contains("root[\\\"Id\\\"]"))
            {
                documentQuery = documentQuery.Replace("root[\\\"Id\\\"]", "root[\\\"id\\\"]");
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

            // if any of the predicate conditions contain the "Id" property we need to change the case to "id" so documentdb will work properly
            var stringifiedQuery = CorrectCasingOfDocumentQuery(documentQuery.ToString());

            // query for just the Ids of the documents that match the where clause so that we can get the total number of documents
            var ids = QueryForIds(stringifiedQuery);

            // capture the count of how many documents match the WHERE clause
            totalCount = ids.Count;

            // calculate how many docs we need to skip in the final query
            var skip = (page - 1) * pageSize;

            // grab the order by clause from the stringifiedQuery so that we can apply it to the final query
            var startIndex = stringifiedQuery.IndexOf("ORDER BY", StringComparison.Ordinal);
            var length = stringifiedQuery.Length - startIndex - 3;
            var orderByClause = stringifiedQuery.Substring(startIndex, length);

            var finalQuery =
                $"SELECT * FROM root WHERE root.id IN ('{string.Join("','", ids.Skip(skip).Take(pageSize))}')" + " " + orderByClause.Replace("\\\"", "'");

            var jObjects = _documentClient.CreateDocumentQuery<JObject>(_collectionUri, finalQuery).ToList();

            var dataList = jObjects.Select(jObject => JsonConvert.DeserializeObject<T>(jObject.ToString(), _jsonSerializerSettings)).ToList();

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

            // convert the query to a dynamic so that we can easily pull just the query text
            var idQuery = JsonConvert.DeserializeObject<dynamic>(idStringQuery);

            // get a list of ids that match the query
            return _documentClient.CreateDocumentQuery<string>(_collectionUri, (string)idQuery.query, options)
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

            // we're saving classes with internal properties (domain entities) so we need to include the internally scoped properties when we convert
            // the object to a JObject
            var serializer = new JsonSerializer { ContractResolver = new InternalPropertyContractResolver() };

            // if there is an "Id" property we need to change it to "id" so that it works with Azure Document Db
            var jObject = JObject.FromObject(data, serializer);
            jObject["Id"]?.Rename("id");

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