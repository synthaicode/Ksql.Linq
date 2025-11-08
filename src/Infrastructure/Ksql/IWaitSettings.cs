using System;

namespace Ksql.Linq.Infrastructure.Ksql;

public interface IWaitSettings
{
    int RequiredConsecutive { get; }
    TimeSpan StabilityWindow { get; }
    TimeSpan QueryRunningTimeout { get; }
}

internal sealed class EnvWaitSettings : IWaitSettings
{
    public int RequiredConsecutive
        => int.TryParse(Environment.GetEnvironmentVariable("KSQL_QUERY_RUNNING_CONSECUTIVE"), out var n) && n > 0 ? n : 5;

    public TimeSpan StabilityWindow
    {
        get
        {
            if (int.TryParse(Environment.GetEnvironmentVariable("KSQL_QUERY_RUNNING_STABILITY_WINDOW_SECONDS"), out var s) && s >= 0)
                return TimeSpan.FromSeconds(s);
            return TimeSpan.FromSeconds(15);
        }
    }

    public TimeSpan QueryRunningTimeout
    {
        get
        {
            if (int.TryParse(Environment.GetEnvironmentVariable("KSQL_QUERY_RUNNING_TIMEOUT_SECONDS"), out var s) && s > 0)
                return TimeSpan.FromSeconds(s);
            return TimeSpan.FromSeconds(180);
        }
    }
}
