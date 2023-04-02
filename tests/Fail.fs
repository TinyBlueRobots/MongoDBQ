module Fail

open Expecto
open System
open MongoDBQ
open System.Threading.Tasks

[<Tests>]
let tests =
  test "Fail" {
    let message = Message({ Data = Guid.NewGuid().ToString() })
    let testApi = TestApi(3, TimeSpan.FromMilliseconds 100)
    testApi.Enqueue message |> ignore
    let dequeuedMessage = testApi.Dequeue()

    //check failed message in db
    let failed = testApi.Fail dequeuedMessage
    let failedMessage = testApi.ReadAllMessages() |> Seq.head
    Expect.isTrue failed "failed"
    Expect.isFalse failedMessage.Completed.HasValue "completed"
    Expect.equal failedMessage.DeliveryCount 1 "retry count"
    Expect.equal failedMessage.LockedUntil DateTime.MinValue "lock time"

    //dequeue again
    testApi.Dequeue() |> ignore

    //message cannot be dequeued because it's locked
    Expect.isNull (testApi.Dequeue()) "dequeued"

    //wait for lock to expire
    Task.Delay(100).Wait()

    //dequeue again
    let dequeuedMessage = testApi.Dequeue()
    Expect.equal dequeuedMessage.DeliveryCount 3 "delivery count"

    //wait for lock to expire
    Task.Delay(100).Wait()

    //fail again
    testApi.Fail dequeuedMessage |> ignore

    //message cannot be dequeued because retry count is exceeded
    Expect.isNull (testApi.Dequeue()) "dequeued"

    //message is still in the queue
    let message = testApi.ReadAllMessages() |> Seq.head
    Expect.equal message.DeliveryCount 3 "messages"

    //delete message
    let deleted = testApi.Delete message
    Expect.isTrue deleted "deleted"
    Expect.isEmpty (testApi.ReadAllMessages()) "messages"
  }
