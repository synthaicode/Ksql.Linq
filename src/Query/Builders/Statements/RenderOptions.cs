namespace Ksql.Linq.Query.Builders.Statements;

internal enum KeyPathStyle
{
    None,
    Dot,
    Arrow
}

/// <summary>
/// Internal hook for tests and backward compatibility. Regular DSL usage relies on auto-detection
/// which selects Arrow for tables and None for streams. Dot style is available only via explicit override.
/// </summary>
internal class RenderOptions
{
    public KeyPathStyle KeyPathStyle { get; set; } = KeyPathStyle.None;
    public System.Type? ResultType { get; set; }
}