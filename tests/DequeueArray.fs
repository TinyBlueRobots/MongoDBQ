module DequeueArray

open Expecto
open System
open MongoDBQ

[<Tests>]
let tests =
  test "DequeueArray" {
    let message1 = Message({ Data = Guid.NewGuid().ToString() })
    let message2 = Message({ Data = Guid.NewGuid().ToString() })
    let testApi = TestApi(1, TimeSpan.FromSeconds 1)
    testApi.Enqueue message1 |> ignore
    testApi.Enqueue message2 |> ignore
    let dequeuedMessages = testApi.Dequeue(2)
    Expect.objectsEqual message1.Body dequeuedMessages[0].Body
    Expect.objectsEqual message2.Body dequeuedMessages[1].Body
  }
