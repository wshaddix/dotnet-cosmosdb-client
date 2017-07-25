using Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Tests
{
    public class CosmosDbTests : IDisposable
    {
        private const string AuthKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        private const string CollectionId = "default";
        private const string DatabaseId = "testing";
        private readonly CosmosDbClient _cosmosDb;
        private readonly string[] _preferredLocations = { "" };
        private readonly Uri _serviceEndpoint = new Uri("https://localhost:8081");

        public CosmosDbTests()
        {
            _cosmosDb = CreateCosmosDbClient(serviceEndpoint: _serviceEndpoint,
                authKey: AuthKey,
                databaseId: DatabaseId,
                collectionId: CollectionId,
                preferredLocations: _preferredLocations,
                microserviceName: null);

            DeleteAllDatabaseDocuments();
        }

        ~CosmosDbTests()
        {
            ReleaseUnmanagedResources();
        }

        [Fact]
        public void A_Microservice_Name_Cannot_Contain_A_Period_Character()
        {
            var exception = Record.Exception(() =>
            {
                CreateCosmosDbClient(serviceEndpoint: _serviceEndpoint,
                    authKey: AuthKey,
                    databaseId: DatabaseId,
                    collectionId: CollectionId,
                    preferredLocations: _preferredLocations,
                    microserviceName: "Periods.in.The.namE");
            });

            // verify that we get a proper exception explaining that the microservice name cannot contain a period
            Assert.NotNull(exception);
            Assert.IsType<CosmosDbConfigurationException>(exception);
            Assert.True(exception.Message.Equals("The microserviceName cannot contain a period."),
                $"The exception message was: {exception.Message}");
        }

        [Fact]
        public async Task An_Entity_That_Does_Not_Have_An_EntityType_Property_Can_Be_Fetched_By_Id()
        {
            var testObject = new TestObjectNoEntityType()
            {
                Id = Guid.NewGuid().ToString("N"),
                Age = new Random().Next(18, 100),
                Aliases = new List<string> { Faker.Name.First(), Faker.Name.First(), Faker.Name.First() },
                FirstName = Faker.Name.First()
            };

            // save the test object
            await _cosmosDb.SaveAsync(testObject);

            // fetch the test object by it's id
            var existingObject = await _cosmosDb.GetByIdAsync<TestObject>(testObject.Id);

            // ensure that the object that was saved is the same as the object that was created initially
            AssertObjectsAreTheSame(testObject, existingObject);
        }

        [Fact]
        public async Task Can_Create_And_Update_An_Object()
        {
            // create a test object
            var testObject = CreateTestObject();

            // save the test object
            await _cosmosDb.SaveAsync(testObject);

            // fetch the test object by it's id
            var existingObject = await _cosmosDb.GetByIdAsync<TestObject>(testObject.Id);

            // update the object
            existingObject.Age = 105;
            existingObject.FirstName = "Updated First Name";
            existingObject.Aliases = new List<string> { "Updated Alias 1", "Updated Alias 2", "Updated Alias 3" };

            // save the updated object
            await _cosmosDb.SaveAsync(existingObject);

            // fetch the test object by it's id
            var updatedObject = await _cosmosDb.GetByIdAsync<TestObject>(testObject.Id);

            // verify the updated object's state was persisted
            Assert.True(updatedObject.Id.Equals(existingObject.Id));
            Assert.True(updatedObject.Age.Equals(105));
            Assert.True(updatedObject.FirstName.Equals("Updated First Name"));
            Assert.True(updatedObject.Aliases[0].Equals("Updated Alias 1"));
            Assert.True(updatedObject.Aliases[1].Equals("Updated Alias 2"));
            Assert.True(updatedObject.Aliases[2].Equals("Updated Alias 3"));

        }

        [Fact]
        public async Task Can_Delete_An_Existing_Object()
        {
            // create a test object
            var testObject = CreateTestObject();

            // save the test object
            await _cosmosDb.SaveAsync(testObject);

            // delete the test object
            await _cosmosDb.DeleteByIdAsync<TestObject>(testObject.Id);

            // if the test does not return an error it's all good
        }

        [Fact]
        public async Task Can_Fetch_A_List_Of_Objects()
        {
            // create 21 documents
            for (var i = 0; i < 21; i++)
            {
                // create a test object
                var testObject = CreateTestObject();

                // set the name to a unique value that we can use to filter on when we fetch the list
                testObject.FirstName = "ListUnitTest";

                // save the test object
                await _cosmosDb.SaveAsync(testObject);

            }

            // fetch the list of test objects
            var result = _cosmosDb.List<TestObject>(1, 4, "Age", t => t.FirstName == "ListUnitTest");

            // verify that we have 21 total records
            Assert.True(result.totalCount == 21);

            // verify that we have 6 pages
            Assert.True(result.totalPages == 6);

            // verify that 4 records came back
            Assert.True(result.data.Count() == 4);
        }

        [Fact]
        public async Task Can_Get_An_Existing_Object_By_Id()
        {
            // create a test object
            var testObject = CreateTestObject();

            // save the test object
            await _cosmosDb.SaveAsync(testObject);

            // fetch the test object by it's id
            var existingObject = await _cosmosDb.GetByIdAsync<TestObject>(testObject.Id);

            // verify that the properties match
            AssertObjectsAreTheSame(testObject, existingObject);

        }

        [Fact]
        public async Task Can_Inject_Entity_Type_With_A_Microservice_Name_Specified()
        {
            // setup a test microservice namespace
            var microServiceName = "TestMicroservice";

            // use a microservice specific cosmos db client
            var cosmosDb = CreateCosmosDbClient(serviceEndpoint: _serviceEndpoint,
                authKey: AuthKey,
                databaseId: DatabaseId,
                collectionId: CollectionId,
                preferredLocations: _preferredLocations,
                microserviceName: microServiceName);

            // create a test object
            var testObject = CreateTestObject();

            // store the test object's type
            var entityType = $"{microServiceName}.{testObject.GetType().Name}";

            // save the test object
            await cosmosDb.SaveAsync(testObject);

            // fetch the test object by it's id
            var existingObject = await cosmosDb.GetByIdAsync<TestObject>(testObject.Id);

            // verify that it's EntityType property is set correctly
            Assert.True(existingObject.EntityType?.Equals(entityType), $"The entity type should be {entityType} but was {existingObject.EntityType}");
        }

        [Fact]
        public async Task Can_Inject_Entity_Type_With_No_Microservice_Name_Specified()
        {
            // create a test object
            var testObject = CreateTestObject();

            // store the test object's type
            var entityType = testObject.GetType().Name;

            // save the test object
            await _cosmosDb.SaveAsync(testObject);

            // fetch the test object by it's id
            var existingObject = await _cosmosDb.GetByIdAsync<TestObject>(testObject.Id);

            // verify that it's EntityType property is set correctly
            Assert.True(existingObject.EntityType?.Equals(entityType), $"The entity type should be {entityType} but was {existingObject.EntityType}");
        }

        [Fact]
        public async Task Can_Save_A_New_Object()
        {
            // create a test object
            var testObject = CreateTestObject();

            // save the test object
            await _cosmosDb.SaveAsync(testObject);

            // if the test does not return an error it's all good

        }

        [Fact]
        public async Task Can_Page_Results_And_Get_Total_Records_Along_With_Page_Size_And_Page_Count()
        {
            // create 10 documents
            for (var i = 0; i < 10; i++)
            {
                // create a test object
                var testObject = CreateTestObject();

                // set the name to a unique value that we can use to filter on when we fetch the list
                testObject.FirstName = "PagingUnitTest";

                // save the test object
                await _cosmosDb.SaveAsync(testObject);

            }

            // fetch the list of test objects
            var result = _cosmosDb.List<TestObject>(1, 5, "Age", t => t.FirstName == "PagingUnitTest");

            // verify that we got back 2 pages, pageSize of 5 with 10 total documents
            Assert.NotNull(result);
            Assert.True(result.totalPages.Equals(2), $"Total pages should have been 2 but was {result.totalPages}");
            Assert.True(result.totalCount.Equals(10), $"Total count should have been 10 but was {result.totalCount}");
            Assert.True(result.data.Count().Equals(5), $"Data count should have been 5 but was {result.data.Count()}");
        }
        [Fact]
        public async Task Can_Sort_A_List_Of_Objects_Ascending()
        {
            // create 10 documents
            for (var i = 0; i < 10; i++)
            {
                // create a test object
                var testObject = CreateTestObject();

                // set the name to a unique value that we can use to filter on when we fetch the list
                testObject.FirstName = "AscSortListUnitTest";

                // set the age of the test object to our counter value so that we can verify the order later
                testObject.Age = i;

                // save the test object
                await _cosmosDb.SaveAsync(testObject);

            }

            // fetch the list of test objects
            var result = _cosmosDb.List<TestObject>(1, int.MaxValue, "Age", t => t.FirstName == "AscSortListUnitTest");

            // verify that the first object is the least age
            Assert.True(result.data.First().Age == 0);

            // verify that the last object is the max age
            Assert.True(result.data.Last().Age == 9);
        }

        [Fact]
        public async Task Can_Sort_A_List_Of_Objects_Descending()
        {
            // create 10 documents
            for (var i = 0; i < 10; i++)
            {
                // create a test object
                var testObject = CreateTestObject();

                // set the name to a unique value that we can use to filter on when we fetch the list
                testObject.FirstName = "AscSortListUnitTest";

                // set the age of the test object to our counter value so that we can verify the order later
                testObject.Age = i;

                // save the test object
                await _cosmosDb.SaveAsync(testObject);

            }

            // fetch the list of test objects
            var result = _cosmosDb.List<TestObject>(1, int.MaxValue, "-Age", t => t.FirstName == "AscSortListUnitTest");

            // verify that the first object is the max age
            Assert.True(result.data.First().Age == 9);

            // verify that the last object is the least age
            Assert.True(result.data.Last().Age == 0);
        }

        public void Dispose()
        {
            ReleaseUnmanagedResources();
            GC.SuppressFinalize(this);
        }

        [Fact]
        public async Task Getting_An_Entity_By_Id_That_Doesnt_Exist_Gives_An_Expected_Error()
        {
            // try to fetch an entity by id that does not exist
            var id = Guid.NewGuid().ToString("N");
            var exception = await Record.ExceptionAsync(async () =>
            {
                await _cosmosDb.GetByIdAsync<TestObject>(id);
            });

            // verify that the test microservice cosmos db client was not able to read the document that was created in another namespace
            Assert.NotNull(exception);
            Assert.IsType<EntityNotFoundException>(exception);
            Assert.True(exception.Message.Equals($"An entity with id {id} was not found in the data store."),
                $"The exception message was: {exception.Message}");
        }

        [Fact]
        public async Task One_Microservice_Namespace_Cannot_Read_From_Another_Microservices_Namespace_By_Get_Predicate()
        {
            // create a test object
            var testObject = CreateTestObject();

            // save the test object
            await _cosmosDb.SaveAsync(testObject);

            // setup a test microservice namespace
            var microServiceName = "TestMicroservice";

            // use a microservice specific cosmos db client
            var cosmosDb = CreateCosmosDbClient(serviceEndpoint: _serviceEndpoint,
                authKey: AuthKey,
                databaseId: DatabaseId,
                collectionId: CollectionId,
                preferredLocations: _preferredLocations,
                microserviceName: microServiceName);

            // try to fetch the test object using a predicate using the test microservice cosmos db client
            var existingObject = cosmosDb.Get<TestObject>(to => to.Id.Equals(testObject.Id));

            // verify that the test microservice cosmos db client was not able to read the document that was created in another namespace
            Assert.Null(existingObject);
        }

        [Fact]
        public async Task One_Microservice_Namespace_Cannot_Read_From_Another_Microservices_Namespace_By_Id()
        {
            // create a test object
            var testObject = CreateTestObject();

            // save the test object
            await _cosmosDb.SaveAsync(testObject);

            // setup a test microservice namespace
            var microServiceName = "TestMicroservice";

            // use a microservice specific cosmos db client
            var cosmosDb = CreateCosmosDbClient(serviceEndpoint: _serviceEndpoint,
                authKey: AuthKey,
                databaseId: DatabaseId,
                collectionId: CollectionId,
                preferredLocations: _preferredLocations,
                microserviceName: microServiceName);

            // fetch the test object by it's id using the test microservice cosmos db client. record the exception that should take place
            var exception = await Record.ExceptionAsync(async () =>
            {
                await cosmosDb.GetByIdAsync<TestObject>(testObject.Id);
            });

            // verify that the test microservice cosmos db client was not able to read the document that was created in another namespace
            Assert.NotNull(exception);
            Assert.IsType<EntityNotFoundException>(exception);
            Assert.True(exception.Message.Equals($"An entity with id {testObject.Id} was found in the data store but is not in the '{microServiceName}' namespace."),
                $"The exception message was: {exception.Message}");
        }

        [Fact]
        public async Task One_Microservice_Namespace_Cannot_Read_From_Another_Microservices_Namespace_By_List()
        {
            // create a test object
            var testObject = CreateTestObject();

            // save the test object
            await _cosmosDb.SaveAsync(testObject);

            // setup a test microservice namespace
            var microServiceName = "TestMicroservice";

            // use a microservice specific cosmos db client
            var cosmosDb = CreateCosmosDbClient(serviceEndpoint: _serviceEndpoint,
                authKey: AuthKey,
                databaseId: DatabaseId,
                collectionId: CollectionId,
                preferredLocations: _preferredLocations,
                microserviceName: microServiceName);

            // try to fetch the test object from a list using the test microservice cosmos db client
            var objectList = cosmosDb.List<TestObject>(1, Int32.MaxValue, "Age", to => to.Id.Equals(testObject.Id));

            // verify that the test microservice cosmos db client was not able to read the document that was created in another namespace
            Assert.True(objectList.totalCount.Equals(0), $"Expected 0 items in the list but there were {objectList.totalPages}");
        }

        private static void AssertObjectsAreTheSame(TestObject testObject, TestObject existingObject)
        {
            Assert.NotNull(existingObject);
            Assert.True(testObject.Id.Equals(existingObject.Id), "The Id doesn't match.");
            Assert.True(testObject.Age.Equals(existingObject.Age), "The Age doesn't match.");
            Assert.True(testObject.FirstName.Equals(existingObject.FirstName), "The FirstName doesn't match.");
            Assert.True(testObject.Aliases[0].Equals(existingObject.Aliases[0]));
            Assert.True(testObject.Aliases[1].Equals(existingObject.Aliases[1]));
            Assert.True(testObject.Aliases[2].Equals(existingObject.Aliases[2]));
        }

        private static void AssertObjectsAreTheSame(TestObjectNoEntityType testObject, TestObject existingObject)
        {
            Assert.NotNull(existingObject);
            Assert.True(testObject.Id.Equals(existingObject.Id), "The Id doesn't match.");
            Assert.True(testObject.Age.Equals(existingObject.Age), "The Age doesn't match.");
            Assert.True(testObject.FirstName.Equals(existingObject.FirstName), "The FirstName doesn't match.");
            Assert.True(testObject.Aliases[0].Equals(existingObject.Aliases[0]));
            Assert.True(testObject.Aliases[1].Equals(existingObject.Aliases[1]));
            Assert.True(testObject.Aliases[2].Equals(existingObject.Aliases[2]));
        }

        private static TestObject CreateTestObject()
        {
            return new TestObject
            {
                Id = Guid.NewGuid().ToString("N"),
                Age = new Random().Next(18, 100),
                Aliases = new List<string> { Faker.Name.First(), Faker.Name.First(), Faker.Name.First() },
                FirstName = Faker.Name.First()
            };
        }

        private CosmosDbClient CreateCosmosDbClient(Uri serviceEndpoint, string authKey, string databaseId, string collectionId, string[] preferredLocations, string microserviceName)
        {
            return new CosmosDbClient(serviceEndpoint: serviceEndpoint,
                authKey: authKey,
                databaseId: databaseId,
                collectionId: collectionId,
                preferredLocations: preferredLocations,
                microserviceName: microserviceName);
        }

        private void DeleteAllDatabaseDocuments()
        {
            // start with a clean database by deleting all the existing data
            var result1 = _cosmosDb.List<TestObject>(1, int.MaxValue, "Age", to => true);
            Task.WaitAll(result1.data.Select(testObject => _cosmosDb.DeleteByIdAsync<TestObject>(testObject.Id)).ToArray());

            var result2 = _cosmosDb.List<TestObjectNoEntityType>(1, int.MaxValue, "Age", to => true);
            Task.WaitAll(result2.data
                .Select(testObjectNoEntityType => _cosmosDb.DeleteByIdAsync<TestObjectNoEntityType>(testObjectNoEntityType.Id))
                .ToArray());
        }

        private void ReleaseUnmanagedResources()
        {
            DeleteAllDatabaseDocuments();
        }
    }
}