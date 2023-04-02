using MongoDB.Driver;
using MongoDBQ;
using System.Diagnostics;

var client = new MongoClient("mongodb://localhost:27017");
var db = client.GetDatabase("test");
var collection = db.GetCollection<Message<TestData>>("messages");
var mongoDBQ = new MongoDBQ<TestData>(collection, 5, TimeSpan.FromSeconds(5), TimeSpan.FromMinutes(1));

var messageCount = 5000;
Enumerable.Range(0, messageCount).ToList().ForEach(i => mongoDBQ.Enqueue(new Message<TestData>(new TestData(Guid.NewGuid()))));

var stopwatch = Stopwatch.StartNew();
var loop = true;
while (loop)
{
  var message = await mongoDBQ.Dequeue();
  if (message == null)
  {
    loop = false;
  }
  else
  {
    Console.WriteLine($"Dequeued message {message.Id}");
    await mongoDBQ.Complete(message);
  }
}
Console.WriteLine($"Dequeued {messageCount / (stopwatch.ElapsedMilliseconds / 1000)} messages per second");

record TestData(Guid Data);
