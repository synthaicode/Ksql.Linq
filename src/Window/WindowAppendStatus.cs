namespace Ksql.Linq.Window;

internal enum WindowAppendStatus
{
    Appended,
    LateDrop,
    Duplicate
}