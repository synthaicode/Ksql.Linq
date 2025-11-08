using System.Threading;

namespace Ksql.Linq.Window;

public sealed class WindowAggregatorMetrics
{
    private long _emitted;
    private long _emitFailures;
    private long _lateDrops;
    private long _duplicateDrops;

    public long Emitted => Interlocked.Read(ref _emitted);
    public long EmitFailures => Interlocked.Read(ref _emitFailures);
    public long LateDrops => Interlocked.Read(ref _lateDrops);
    public long DuplicateDrops => Interlocked.Read(ref _duplicateDrops);

    internal void RecordEmitSuccess() => Interlocked.Increment(ref _emitted);

    internal void RecordEmitFailure() => Interlocked.Increment(ref _emitFailures);

    internal void RecordLateDrop() => Interlocked.Increment(ref _lateDrops);

    internal void RecordDuplicateDrop() => Interlocked.Increment(ref _duplicateDrops);
}