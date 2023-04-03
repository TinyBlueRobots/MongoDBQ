module PartitionKey

open Expecto
open System
open MongoDBQ

[<Tests>]
let tests =
  test "PartitionKey" {
    let message1 =
      Message({ Data = Guid.NewGuid().ToString() }, PartitionKey = "partition1")

    let message2 =
      Message({ Data = Guid.NewGuid().ToString() }, PartitionKey = "partition2")

    let testApi = TestApi(1, TimeSpan.FromSeconds 1)
    testApi.Enqueue message1 |> ignore
    testApi.Enqueue message2 |> ignore
    let dequeuedMessage2 = testApi.Dequeue "partition2"
    let dequeuedMessage1 = testApi.Dequeue "partition1"
    Expect.objectsEqual message1.Body dequeuedMessage1.Body
    Expect.objectsEqual message2.Body dequeuedMessage2.Body
  }
