module Complete

open Expecto
open System
open MongoDBQ

[<Tests>]
let tests =
  test "Complete" {
    let now = DateTime.UtcNow.AddMilliseconds -1
    let message = Message({ Data = Guid.NewGuid().ToString() })
    let testApi = TestApi(1, TimeSpan.FromSeconds 1)

    //enqueue twice to check for duplicate id
    let result = testApi.Enqueue message
    Expect.isTrue result "enqueued"
    let result = testApi.Enqueue message
    Expect.isFalse result "enqueued"

    //check message in db
    let enqueuedMessage = testApi.ReadActiveMessages() |> Seq.head
    Expect.equal enqueuedMessage.Id message.Id "id"
    Expect.objectsEqual enqueuedMessage.Body message.Body
    Expect.isGreaterThanOrEqual enqueuedMessage.Created now "created"

    //check dequeued message
    let dequeuedMessage = testApi.Dequeue 10 |> Seq.head
    Expect.equal message.Id dequeuedMessage.Id "id"
    Expect.objectsEqual message.Body dequeuedMessage.Body
    Expect.equal dequeuedMessage.DeliveryCount 1 "delivery count"
    Expect.isGreaterThanOrEqual dequeuedMessage.LockedUntil enqueuedMessage.Created "lock time"

    //check completed message in db
    let completed = testApi.Complete dequeuedMessage
    let completedMessage = testApi.ReadAllMessages() |> Seq.head
    Expect.isTrue completed "completed"
    Expect.isGreaterThanOrEqual completedMessage.Completed.Value now "completed"
  }
