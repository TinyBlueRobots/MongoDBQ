module CosmosDB

open Expecto
open System
open MongoDBQ

[<Tests>]
let tests =
  test "CosmosDB" {
    let now = DateTime.UtcNow.AddMilliseconds -1
    let message = Message({ Data = Guid.NewGuid().ToString() })
    let testApi = TestApi(1, TimeSpan.FromSeconds 1, true)
    testApi.Enqueue message |> ignore
    testApi.Complete message |> ignore
    let completedMessage = testApi.ReadAllMessages() |> Seq.head
    Expect.isGreaterThan completedMessage._ttl 0 "completed"
  }
