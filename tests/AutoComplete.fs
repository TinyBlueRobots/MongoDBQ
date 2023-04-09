module AutoComplete

open Expecto
open System
open MongoDBQ

[<Tests>]
let tests =
  test "AutoComplete" {
    let now = DateTime.UtcNow.AddMilliseconds -1
    let message = Message({ Data = Guid.NewGuid().ToString() })
    let testApi = TestApi(1, TimeSpan.FromSeconds 1)
    testApi.Enqueue message |> ignore
    testApi.Dequeue true |> ignore
    let completedMessage = testApi.ReadAllMessages() |> Seq.head
    Expect.isGreaterThanOrEqual completedMessage.Completed.Value now "completed"
  }
