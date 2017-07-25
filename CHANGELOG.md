# 1.0.2
* Updated Webcom.Common.Extension version to 1.0.2

# 1.1.0
* Added a feature where you can optionally specify a microservice name in the `CosmosDbClient` constructor. If set, this name will be added to every document as part of an auto-injected `EntityType` property when an object is saved. If set, this will also be used an any predicates where documents are searched, so in effect, it will scope your search to only those documents `where EntityType = <microservice name>.<entity type>`
* Added a `GetByIdAsync<T>(string id)` method to the `CosmosDbClient` class since it's such a common scenario to fetch a document by `Id`. This is a more terse and more performant way versus using the `public T Get<T>(Expression<Func<T, bool>> predicate)` method
* Optimized serialization/deserialization of documents to and from Entities by removing an unnecessary `ToString()` call on every method that returns a document from CosmosDb

# 2.0.0
* `DeleteAsync` was renamed to `DeleteByIdAsync<T>` and now requires the generic `<T>` to be passed. This is a breaking change and was made so that deleting by id get's the same namespace checks that `GetByIdAsync<T>` does