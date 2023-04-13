module DequeueArray

open Expecto
open System
open MongoDBQ

[<Tests>]
let tests =
  test "DequeueArray" {
    let now = DateTime.UtcNow.AddMilliseconds -1
    let message1 = Message({ Data = Guid.NewGuid().ToString() })
    let message2 = Message({ Data = Guid.NewGuid().ToString() })
    let testApi = TestApi(1, TimeSpan.FromSeconds 1)
    testApi.Enqueue message1 |> ignore
    testApi.Enqueue message2 |> ignore
    let dequeuedMessages = testApi.Dequeue(2)
    Expect.objectsEqual message1.Body dequeuedMessages[0].Body
    Expect.objectsEqual message2.Body dequeuedMessages[1].Body

    //check completed message in db
    let completed = testApi.Complete dequeuedMessages
    let completedMessages = testApi.ReadAllMessages()
    Expect.isTrue completed "completed"

    Expect.isTrue
      (completedMessages
       |> Seq.map (fun x -> x.Completed.Value >= now)
       |> Seq.forall ((=) true))
      "completed"

    //delete messages
    let deleted = testApi.Delete completedMessages
    Expect.isTrue deleted "deleted"
    let deletedMessages = testApi.ReadAllMessages()
    Expect.isEmpty deletedMessages "deleted"
  }
