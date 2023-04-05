namespace MongoDBQ;

/// <summary>
/// Represents a generic message with a body of type T and various properties for message handling.
/// </summary>
/// <typeparam name="T">The type of the message body.</typeparam>
public class Message<T>
{
  /// <summary>
  /// Initializes a new instance of the <see cref="Message{T}"/> class with the specified message body.
  /// </summary>
  /// <param name="body">The body of the message.</param>
  public Message(T body) => Body = body;

  /// <summary>
  /// Gets or sets the body of the message.
  /// </summary>
  public T Body { get; set; }

  /// <summary>
  /// Gets or sets the completion timestamp of the message. A value of null indicates the message has not been completed.
  /// </summary>
  public DateTime? Completed { get; set; } = null;

  /// <summary>
  /// Gets or sets the creation timestamp of the message.
  /// </summary>
  public DateTime Created { get; set; } = DateTime.UtcNow;

  /// <summary>
  /// Gets or sets the number of times the message has been delivered.
  /// </summary>
  public int DeliveryCount { get; set; } = 0;

  /// <summary>
  /// Gets or sets the unique identifier of the message.
  /// </summary>
  public Guid Id { get; set; } = Guid.NewGuid();

  /// <summary>
  /// Gets or sets the timestamp until which the message is locked.
  /// </summary>
  public DateTime LockedUntil { get; set; } = DateTime.MinValue;

  /// <summary>
  /// Gets or sets the partition key for the message.
  /// </summary>
  public string? PartitionKey { get; set; }

  /// <summary>
  /// Gets or sets the enqueue time for processing the message.
  /// </summary>
  public DateTime ScheduledEnqueueTime { get; set; } = DateTime.MinValue;

  /// <summary>
  /// Gets or sets the time to live for the message for CosmosDB.
  /// </summary>
  public int _ttl { get; private set; } = -1;
}