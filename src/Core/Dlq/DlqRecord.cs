using System.Collections.Generic;

namespace Ksql.Linq.Core.Dlq;

public sealed record DlqRecord(
    string MessageId,
    string Topic,
    int Partition,
    long Offset,
    string TimestampUtc,
    byte[] RawBytes,
    string? RawText,
    string PayloadFormat,
    string? SchemaId,
    string? SchemaSubject,
    int? SchemaVersion,
    IReadOnlyDictionary<string, string> Headers,
    string? ErrorType,
    string? ErrorMessage,
    string? StackTrace
)
{
    public string MessageId { get; init; } = MessageId;
    public string Topic { get; init; } = Topic;
    public int Partition { get; init; } = Partition;
    public long Offset { get; init; } = Offset;
    public string TimestampUtc { get; init; } = TimestampUtc;
    public byte[] RawBytes { get; init; } = RawBytes;
    public string? RawText { get; init; } = RawText;
    public string PayloadFormat { get; init; } = PayloadFormat;
    public string? SchemaId { get; init; } = SchemaId;
    public string? SchemaSubject { get; init; } = SchemaSubject;
    public int? SchemaVersion { get; init; } = SchemaVersion;
    public IReadOnlyDictionary<string, string> Headers { get; init; } = Headers;
    public string? ErrorType { get; init; } = ErrorType;
    public string? ErrorMessage { get; init; } = ErrorMessage;
    public string? StackTrace { get; init; } = StackTrace;

}