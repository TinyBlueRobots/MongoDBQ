module PeekArray

open Expecto
open System
open MongoDBQ

[<Tests>]
let tests =
  test "PeekArray" {
    let message1 = Message({ Data = Guid.NewGuid().ToString() })
    let message2 = Message({ Data = Guid.NewGuid().ToString() })
    let testApi = TestApi(1, TimeSpan.FromSeconds 1)
    testApi.Enqueue message1 |> ignore
    testApi.Enqueue message2 |> ignore
    let dequeuedMessages = testApi.Peek 2
    Expect.objectsEqual message1.Body dequeuedMessages[0].Body
    Expect.objectsEqual message2.Body dequeuedMessages[1].Body
    Expect.equal message1.LockedUntil DateTime.MinValue "locked until"
    Expect.equal message2.LockedUntil DateTime.MinValue "locked until"
    Expect.equal message1.DeliveryCount 0 "delivery count"
    Expect.equal message2.DeliveryCount 0 "delivery count"
  }
