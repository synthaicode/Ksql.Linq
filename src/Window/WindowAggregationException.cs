using System;

namespace Ksql.Linq.Window;

public sealed class WindowAggregationException : Exception
{
    public WindowAggregationException(string message, object? key = null, DateTime? bucketStartUtc = null, Exception? innerException = null)
        : base(message, innerException)
    {
        Key = key;
        BucketStartUtc = bucketStartUtc;
    }

    public object? Key { get; }

    public DateTime? BucketStartUtc { get; }
}