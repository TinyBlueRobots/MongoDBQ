# MongoDBQ
A MongoDB message queue that supports locking, retries, scheduling, and deduplication


[![NuGet Version](http://img.shields.io/nuget/v/MongoDBQ.svg?style=flat)](https://www.nuget.org/packages/MongoDBQ/)

## Usage

### Server
```csharp
using MongoDBQ;

record TestData(Guid Data);

var client = new MongoClient("mongodb://localhost:27017");
var db = client.GetDatabase("test");
var collection = db.GetCollection<Message<TestData>>("messages");
var mongoDBQ = new MongoDBQ<TestData>(collection, 5, TimeSpan.FromSeconds(5), TimeSpan.FromMinutes(1));
var testData = new TestData(Guid.NewGuid());
var message = new Message<TestData>(testData);
await mongoDBQ.Enqueue(message);
```

### Client
```csharp
using MongoDBQ;

record TestData(Guid Data);

var client = new MongoClient("mongodb://localhost:27017");
var db = client.GetDatabase("test");
var collection = db.GetCollection<Message<TestData>>("messages");
var mongoDBQ = new MongoDBQ<TestData>(collection, 5, TimeSpan.FromSeconds(5), TimeSpan.FromMinutes(1));
while (true)
  {
    var messages = await mongoDBQ.Dequeue(10);
    //Sleep if there are no messages?
    foreach (var message in messages)
      {
        try
            {
              //do something with message.Body
              await mongoDBQ.Complete(message); //mark as completed
            }

            catch (System.Exception)
            {
              //we'll backoff and retry this later
              message.ScheduledEnqueueTime = DateTime.UtcNow.AddMinutes(5);
              await mongoDBQ.Fail(message);
              //this makes the message available for retry, alternatively use mongoDBQ.Delete(message) if this is terminal
            }
      }
  }
```

`MongoDBQ` supports these parameters:
- `maxDeliveryCount`: The maximum number of times a message can be delivered before it is considered poisoned.
- `lockDuration`: The duration for which a message should be locked after being dequeued. When the lock expires it will be available for dequeueing again.
- `expireAfter`: Optional time after which a completed message should be removed from the collection.

When creating a `Message`:
- Set the `Id` property to enable deduplication; I use [Identifiable](https://github.com/seanterry/Identifiable) to create a deterministic name-based GUID from properties of the message body.
- Set `ScheduledEnqueueTime` to schedule the message for processing in the future.
