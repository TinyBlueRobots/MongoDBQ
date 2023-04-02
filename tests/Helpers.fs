[<AutoOpen>]
module Helpers

open MongoDB.Driver
open MongoDBQ
open System
open Expecto

type TestData = { Data: string }

type TestApi(maxDeliveryCount, lockDuration, ?cosmosDB) =
  do Mongo.dropAllCollections ()
  let client = MongoClient "mongodb://localhost:27017"
  let db = client.GetDatabase "test"
  let collection = db.GetCollection<Message<TestData>> "messages"

  let mongoDBQ =
    MongoDBQ(collection, maxDeliveryCount, lockDuration, TimeSpan.FromSeconds 10, cosmosDB |> Option.defaultValue false)

  member _.ReadActiveMessages() =
    let filter = Builders.Filter.And [| Builders.Filter.Eq("Completed", Nullable()) |]
    collection.Find(filter).ToList().ToArray()

  member _.ReadAllMessages() =
    collection.Find(FilterDefinition.Empty).ToList().ToArray()

  member _.Enqueue message = mongoDBQ.Enqueue(message).Result
  member _.Dequeue() = mongoDBQ.Dequeue().Result
  member _.Complete message = mongoDBQ.Complete(message).Result
  member _.Fail message = mongoDBQ.Fail(message).Result
  member _.Delete message = mongoDBQ.Delete(message).Result

module Expect =
  let objectsEqual actual expected =
    let comparer = ObjectsComparer.Comparer()
    comparer.IgnoreMember "Timestamp"

    let diffs =
      comparer.CalculateDifferences(actual, expected)
      |> Seq.map string
      |> String.concat Environment.NewLine

    Expect.equal diffs "" ""
