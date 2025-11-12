
using System;
using Confluent.Kafka;
using Ksql.Linq.SerDes;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;

namespace Ksql.Linq.SerDes;


internal sealed class TombstoneSafeSerDes<T> : ISerDes<T> where T : class
{
    private readonly ISerDes<T> _inner;
    private readonly ILogger _logger;

    public TombstoneSafeSerDes(ISerDes<T> inner, ILogger logger)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }


    public TombstoneSafeSerDes(ISerDes<T> inner)
        : this(inner, NullLogger<TombstoneSafeSerDes<T>>.Instance)
    {
    }

    public void Initialize(SerDesContext context)
    {
        _inner.Initialize(context);
    }

    public T Deserialize(byte[] data, SerializationContext context)
    {
        if (data == null || data.Length == 0)
        {
            _logger.LogWarning("Tombstone-safe SerDes detected null payload for {Topic}; returning null.", context.Topic ?? string.Empty);
            return null!;
        }

        try
        {
            return _inner.Deserialize(data, context);
        }
        catch (Exception ex) when (IsTombstoneException(ex))
        {
            _logger.LogWarning(ex, "Tombstone-safe SerDes returning null after deserialization failure for {Topic}", context.Topic ?? string.Empty);
            return null!;
        }
    }

    public object DeserializeObject(byte[] data, SerializationContext context)
    {
        if (data == null || data.Length == 0)
        {
            _logger.LogWarning("Tombstone-safe SerDes detected null payload for {Topic}; returning null object.", context.Topic ?? string.Empty);
            return null!;
        }

        try
        {
            if (_inner is ISerDes innerNonGeneric)
            {
                return innerNonGeneric.DeserializeObject(data, context);
            }

            return _inner.Deserialize(data, context);
        }
        catch (Exception ex) when (IsTombstoneException(ex))
        {
            _logger.LogWarning(ex, "Tombstone-safe SerDes returning null object after deserialization failure for {Topic}", context.Topic ?? string.Empty);
            return null!;
        }
    }

    public byte[] Serialize(T data, SerializationContext context)
    {
        if (data == null)
        {
            return Array.Empty<byte>();
        }

        return _inner.Serialize(data, context);
    }

    public byte[] SerializeObject(object data, SerializationContext context)
    {
        if (data == null)
        {
            return Array.Empty<byte>();
        }

        if (_inner is ISerDes innerNonGeneric)
        {
            return innerNonGeneric.SerializeObject(data, context);
        }

        return _inner.Serialize((T)data, context);
    }

    private static bool IsTombstoneException(Exception ex)
    {
        if (ex is DeserializationException || ex is Avro.AvroException)
        {
            return true;
        }

        return ex.InnerException != null && IsTombstoneException(ex.InnerException);
    }
}
