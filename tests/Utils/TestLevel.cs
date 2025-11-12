namespace Ksql.Linq.Tests.Utils;

/// <summary>
/// Centralized test level tags for filtering in CI and local runs.
/// </summary>
public static class TestLevel
{
    public const string L1 = "L1"; // Syntax/grammar unit tests
    public const string L2 = "L2"; // Feature/component unit tests
    public const string L3 = "L3"; // Contract tests (public API / generated KSQL)
    public const string L4 = "L4"; // Golden/canonical snapshot tests
    public const string L5 = "L5"; // Pseudo-E2E tests (fast, mocked externals)
}

