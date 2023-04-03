namespace MongoDBQ;

using MongoDB.Driver;

/// <summary>
/// Represents a MongoDB-backed queue.
/// </summary>
/// <typeparam name="T">The type of data the queue should handle.</typeparam>
public class MongoDBQ<T>
{
  readonly IMongoCollection<Message<T>> _collection;
  readonly int _maxDeliveryCount;
  readonly TimeSpan _lockDuration;
  readonly bool _cosmosDB;
  readonly TimeSpan _expireAfter;

  /// <summary>
  /// Initializes a new instance of the <see cref="MongoDBQ{T}"/> class.
  /// </summary>
  /// <param name="collection">The MongoDB collection to use for storing messages.</param>
  /// <param name="maxDeliveryCount">The maximum number of times a message can be delivered before it is considered poisoned.</param>
  /// <param name="lockDuration">The duration for which a message should be locked after being dequeued.</param>
  /// <param name="expireAfter">The time after which a completed message should be removed from the collection.</param>
  /// <param name="cosmosDB">Enable CosmosDB specific features</param>
  public MongoDBQ(IMongoCollection<Message<T>> collection, int maxDeliveryCount, TimeSpan lockDuration, TimeSpan expireAfter, bool cosmosDB = false)
  {
    _collection = collection;
    _maxDeliveryCount = maxDeliveryCount;
    _lockDuration = lockDuration;
    _cosmosDB = cosmosDB;
    _expireAfter = expireAfter;

    bool IndexExists(string indexName) => _collection.Indexes.List().ToList().Any(index => index["name"] == indexName);

    var indexName = "completed_expiry";
    if (expireAfter > TimeSpan.Zero && !IndexExists(indexName))
    {
      var field = cosmosDB ? "_ts" : "Completed";
      var indexKeys = Builders<Message<T>>.IndexKeys.Ascending(field);
      var indexModel = new CreateIndexModel<Message<T>>(indexKeys, new CreateIndexOptions
      {
        ExpireAfter = expireAfter,
        Name = indexName
      });
      collection.Indexes.CreateOne(indexModel);
    }

    indexName = "dequeue";
    if (!IndexExists(indexName))
    {
      var dequeueIndexKeys = Builders<Message<T>>.IndexKeys
          .Ascending(m => m.DeliveryCount)
          .Ascending(m => m.LockedUntil)
          .Ascending(m => m.ScheduledEnqueueTime)
          .Ascending(m => m.Completed)
          .Ascending(m => m.Created)
          .Ascending(m => m.PartitionKey);
      var indexModel = new CreateIndexModel<Message<T>>(dequeueIndexKeys, new CreateIndexOptions
      {
        Name = indexName
      });
      collection.Indexes.CreateOne(indexModel);
    }
  }

  /// <summary>
  /// Initializes a new instance of the <see cref="MongoDBQ{T}"/> class without message expiration.
  /// </summary>
  /// <param name="collection">The MongoDB collection to use for storing messages.</param>
  /// <param name="maxDeliveryCount">The maximum number of times a message can be delivered before it is considered poisoned.</param>
  /// <param name="lockDuration">The duration for which a message should be locked after being dequeued.</param>
  /// <param name="cosmosDB">Enable CosmosDB specific features</param>
  public MongoDBQ(IMongoCollection<Message<T>> collection, int maxDeliveryCount, TimeSpan lockDuration, bool cosmosDB = false)
      : this(collection, maxDeliveryCount, lockDuration, TimeSpan.Zero, cosmosDB) { }

  /// <summary>
  /// Enqueues a message in the queue.
  /// </summary>
  /// <param name="message">The message to enqueue.</param>
  /// <returns>A <see cref="Task{TResult}"/> representing the asynchronous operation, with a boolean value indicating whether the message was enqueued successfully.</returns>
  public async Task<bool> Enqueue(Message<T> message)
  {
    try
    {
      await _collection.InsertOneAsync(message);
      return true;
    }
    catch (MongoWriteException ex) when (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
    {
      return false;
    }
    catch (Exception)
    {
      throw;
    }
  }

  /// <summary>
  /// Deletes a message from the queue.
  /// </summary>
  /// <param name="message">The message to delete.</param>
  /// <returns>A <see cref="Task{TResult}"/> representing the asynchronous operation, with a boolean value indicating whether the message was deleted successfully.</returns>
  public async Task<bool> Delete(Message<T> message)
  {
    var query = Builders<Message<T>>.Filter.Eq(m => m.Id, message.Id);
    var result = await _collection.DeleteOneAsync(query);
    return result.IsAcknowledged;
  }

  /// <summary>
  /// Dequeues a message from the queue.
  /// </summary>
  /// <returns>A <see cref="Task{TResult}"/> representing the asynchronous operation, with a dequeued message.</returns>
  public async Task<Message<T>> Dequeue(string? partitionKey = null)
  {
    var now = DateTime.UtcNow;

    var filter =
        Builders<Message<T>>.Filter.Lt(m => m.DeliveryCount, _maxDeliveryCount) &
        Builders<Message<T>>.Filter.Lte(m => m.LockedUntil, now) &
        Builders<Message<T>>.Filter.Lte(m => m.ScheduledEnqueueTime, now) &
        Builders<Message<T>>.Filter.Eq(m => m.Completed, null);

    filter = partitionKey == null ? filter : filter & Builders<Message<T>>.Filter.Eq(m => m.PartitionKey, partitionKey);
    var sort = Builders<Message<T>>.Sort.Ascending(m => m.Created);

    var options = new FindOneAndUpdateOptions<Message<T>, Message<T>>
    {
      Sort = sort,
      ReturnDocument = ReturnDocument.After
    };

    var update = Builders<Message<T>>.Update
        .Set(m => m.LockedUntil, now + _lockDuration)
        .Inc(m => m.DeliveryCount, 1);

    return await _collection.FindOneAndUpdateAsync(filter, update, options);
  }

  /// <summary>
  /// Marks a message as completed.
  /// </summary>
  /// <param name="message">The message to mark as completed.</param>
  /// <returns>A <see cref="Task{TResult}"/> representing the asynchronous operation, with a boolean value indicating whether the message was marked as completed successfully.</returns>
  public async Task<bool> Complete(Message<T> message)
  {
    var query = Builders<Message<T>>.Filter.Eq(m => m.Id, message.Id);
    var update = Builders<Message<T>>.Update.Set(m => m.Completed, DateTime.UtcNow);

    if (_cosmosDB && _expireAfter != TimeSpan.Zero)
    {
      update = update.Set("_ttl", (int)_expireAfter.TotalSeconds);
    }

    var result = await _collection.UpdateOneAsync(query, update);
    return result.IsAcknowledged;
  }


  /// <summary>
  /// Marks a message as failed.
  /// </summary>
  /// <param name="message">The message to mark as failed.</param>
  /// <returns>A <see cref="Task{TResult}"/> representing the asynchronous operation, with a boolean value indicating whether the message was marked as failed successfully.</returns>
  public async Task<bool> Fail(Message<T> message)
  {
    var query = Builders<Message<T>>.Filter.Eq(m => m.Id, message.Id);
    var update = Builders<Message<T>>.Update.Set(m => m.LockedUntil, DateTime.MinValue).Set(m => m.ScheduledEnqueueTime, message.ScheduledEnqueueTime);
    var result = await _collection.UpdateOneAsync(query, update);
    return result.IsAcknowledged;
  }
}
