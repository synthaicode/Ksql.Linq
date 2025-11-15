namespace Ksql.Linq;

public record KsqlDbResponse(bool IsSuccess, string Message, int? ErrorCode = null, string? ErrorDetail = null)
{
    public bool IsError { get; init; } = !IsSuccess;
    public string Message { get; init; } = Message;
    public int? ErrorCode { get; init; } = ErrorCode;
    public string? ErrorDetail { get; init; } = ErrorDetail;
}
