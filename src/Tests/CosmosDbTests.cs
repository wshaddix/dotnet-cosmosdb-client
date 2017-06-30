using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Client;
using Xunit;

namespace Tests
{
    public class CosmosDbTests : IDisposable
    {
        private const string AuthKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        private const string CollectionId = "default";
        private const string DatabaseId = "testing";
        private readonly CosmosDbClient _cosmosDb;
        private readonly List<string> _idsToDelete = new List<string>();
        private readonly string[] _preferredLocations = { "" };
        private readonly Uri _serviceEndpoint = new Uri("https://localhost:8081");

        public CosmosDbTests()
        {
            // create a new cosmos db client for the tests to run against
            _cosmosDb = new CosmosDbClient(serviceEndpoint: _serviceEndpoint,
                authKey: AuthKey,
                databaseId: DatabaseId,
                collectionId: CollectionId,
                preferredLocations: _preferredLocations);

            // start with a clean database by deleting all the existing data
            var result = _cosmosDb.List<TestObject>(1, int.MaxValue, "Age", to => true);
            Task.WaitAll(result.data.Select(testObject => _cosmosDb.DeleteAsync(testObject.Id)).ToArray());
        }

        ~CosmosDbTests()
        {
            ReleaseUnmanagedResources();
        }

        [Fact]
        public async Task Can_Create_And_Update_An_Object()
        {
            // create a test object
            var testObject = CreateTestObject();

            // save the test object
            await _cosmosDb.SaveAsync(testObject);

            // fetch the test object by it's id
            var existingObject = _cosmosDb.Get<TestObject>(to => to.Id.Equals(testObject.Id));

            // update the object
            existingObject.Age = 105;
            existingObject.FirstName = "Updated First Name";
            existingObject.Aliases = new List<string> { "Updated Alias 1", "Updated Alias 2", "Updated Alias 3" };

            // save the updated object
            await _cosmosDb.SaveAsync(existingObject);

            // fetch the test object by it's id
            var updatedObject = _cosmosDb.Get<TestObject>(to => to.Id.Equals(testObject.Id));

            // verify the updated object's state was persisted
            Assert.True(updatedObject.Id.Equals(existingObject.Id));
            Assert.True(updatedObject.Age.Equals(105));
            Assert.True(updatedObject.FirstName.Equals("Updated First Name"));
            Assert.True(updatedObject.Aliases[0].Equals("Updated Alias 1"));
            Assert.True(updatedObject.Aliases[1].Equals("Updated Alias 2"));
            Assert.True(updatedObject.Aliases[2].Equals("Updated Alias 3"));

            // add the test object to the list of objects to delete after all the tests run
            _idsToDelete.Add(testObject.Id);
        }

        [Fact]
        public async Task Can_Delete_An_Existing_Object()
        {
            // create a test object
            var testObject = CreateTestObject();

            // save the test object
            await _cosmosDb.SaveAsync(testObject);

            // delete the test object
            await _cosmosDb.DeleteAsync(testObject.Id);

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

                // add the test object to the list of objects to delete after all the tests run
                _idsToDelete.Add(testObject.Id);
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

                // add the test object to the list of objects to delete after all the tests run
                _idsToDelete.Add(testObject.Id);
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

                // add the test object to the list of objects to delete after all the tests run
                _idsToDelete.Add(testObject.Id);
            }

            // fetch the list of test objects
            var result = _cosmosDb.List<TestObject>(1, int.MaxValue, "-Age", t => t.FirstName == "AscSortListUnitTest");

            // verify that the first object is the max age
            Assert.True(result.data.First().Age == 9);

            // verify that the last object is the least age
            Assert.True(result.data.Last().Age == 0);
        }

        [Fact]
        public async Task Can_Get_An_Existing_Object_By_Id()
        {
            // create a test object
            var testObject = CreateTestObject();

            // save the test object
            await _cosmosDb.SaveAsync(testObject);

            // fetch the test object by it's id
            var existingObject = _cosmosDb.Get<TestObject>(to => to.Id.Equals(testObject.Id));

            // verify that the properties match
            Assert.True(testObject.Id.Equals(existingObject.Id));
            Assert.True(testObject.Age.Equals(existingObject.Age));
            Assert.True(testObject.FirstName.Equals(existingObject.FirstName));
            Assert.True(testObject.Aliases[0].Equals(existingObject.Aliases[0]));
            Assert.True(testObject.Aliases[1].Equals(existingObject.Aliases[1]));
            Assert.True(testObject.Aliases[2].Equals(existingObject.Aliases[2]));

            // add the test object to the list of objects to delete after all the tests run
            _idsToDelete.Add(testObject.Id);
        }

        [Fact]
        public async Task Can_Save_A_New_Object()
        {
            // create a test object
            var testObject = CreateTestObject();

            // save the test object
            await _cosmosDb.SaveAsync(testObject);

            // if the test does not return an error it's all good

            // add the test object to the list of objects to delete after all the tests run
            _idsToDelete.Add(testObject.Id);
        }

        public void Dispose()
        {
            ReleaseUnmanagedResources();
            GC.SuppressFinalize(this);
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

        private void ReleaseUnmanagedResources()
        {
            // delete all of the objects that were created in the database
            _idsToDelete.ForEach(async id => await _cosmosDb.DeleteAsync(id));
        }
    }
}