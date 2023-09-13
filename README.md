# MongoDBQ

A MongoDB message queue that supports locking, retries, scheduling, deduplication, and CosmosDB

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
using MongoDB.Driver;

record TestData(Guid Data);

var client = new MongoClient("mongodb://localhost:27017");
var db = client.GetDatabase("test");
var collection = db.GetCollection<Message<TestData>>("messages");
var mongoDBQ = new MongoDBQ<TestData>(collection, 5, TimeSpan.FromSeconds(5), TimeSpan.FromMinutes(1));
while (true)
  {
    var message = await mongoDBQ.Dequeue();
    //Sleep if message is null?
    try
      {
        //do something with message.Body
        await mongoDBQ.Complete(message); //mark as completed
      }

      catch (System.Exception)
      {
        //Backoff and retry it later
        message.ScheduledEnqueueTime = DateTime.UtcNow.AddMinutes(5);
        await mongoDBQ.Fail(message);
        //this makes the message available for retry, alternatively use mongoDBQ.Delete(message) if this is terminal
      }
  }
```

`MongoDBQ` constructor parameters:

- `maxDeliveryCount`: The maximum number of times a message can be delivered before it is considered poisoned.
- `lockDuration`: The duration for which a message should be locked after being dequeued. When the lock expires, the message becomes available for dequeueing again.
- `expireAfter`: Optional time after which a completed message should be removed from the collection.
- `cosmosDB`: Enable CosmosDB specific features like Time-To-Live (TTL) for automatic removal of expired messages.

`MongoDBQ.Dequeue` parameters:

- `count`: The number of messages to dequeue from the queue. The method will attempt to dequeue the specified number of messages but may return fewer messages if there are not enough available.
- `partitionKey`: The optional partition key to use for dequeueing messages. If provided, the method will only dequeue messages with a matching partition key. This is useful for processing messages in parallel while ensuring that messages with the same partition key are processed in order. It could also allows you to have messages for different services in the same collection.
- `cancellationToken`: An optional cancellation token that can be used to cancel the dequeue operation. If the provided cancellation token is triggered, the method will stop dequeueing messages and return the dequeued messages up to that point.
- `autoComplete`: A boolean parameter indicating whether the dequeued messages should be automatically marked as completed after they are dequeued. If set to true, the method will mark the dequeued messages as completed, preventing them from being dequeued again. If set to false, the dequeued messages will remain in the queue, and you will need to manually complete or fail them based on your processing logic.

`Message` properties:

- Set the `Id` property to enable deduplication; I use [Identifiable](https://github.com/seanterry/Identifiable) to create a deterministic name-based GUID from properties of the message body.
- Set `ScheduledEnqueueTime` to schedule the message for processing in the future.
- Set `PartitionKey` to partition messages in the collection and then use `MongoDBQ.Dequeue(partitionKey)`.

### Optimistic completion

Greater performance can be achieved by dequeueing batches, using `autoComplete`, and reversing the completion by calling `mongoDBQ.Fail(message)` for those that raise an error. Ensure this occurs within the expiry window if you're using TTLs.
