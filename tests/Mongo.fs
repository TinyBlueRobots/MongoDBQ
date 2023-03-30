module Mongo

open MongoDB.Driver

let dropAllCollections () =
  let mongoClient = MongoClient "mongodb://localhost:27017"
  let database = mongoClient.GetDatabase "test"

  for collection in database.ListCollectionNames().ToEnumerable() do
    database.DropCollection collection
