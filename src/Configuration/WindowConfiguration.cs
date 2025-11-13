using Microsoft.Extensions.Configuration;
using System;

namespace Ksql.Linq.Configuration;

/// <summary>
/// Defines tumbling window behaviour for application-side aggregation.
/// </summary>
public sealed class WindowConfiguration
{
    private static readonly TimeSpan DefaultGracePeriod = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan DefaultSweepInterval = TimeSpan.FromSeconds(1);

    public TimeSpan WindowSize { get; init; }
    public TimeSpan GracePeriod { get; init; } = DefaultGracePeriod;
    public TimeSpan SweepInterval { get; init; } = DefaultSweepInterval;
    public TimeSpan IdleEviction { get; init; } = TimeSpan.FromMinutes(5);

    public static WindowConfiguration FromMinutes(int minutes, int graceSeconds = 3)
    {
        if (minutes <= 0)
        {
            throw new InvalidOperationException("Window size must be greater than zero.");
        }

        return new WindowConfiguration
        {
            WindowSize = TimeSpan.FromMinutes(minutes),
            GracePeriod = TimeSpan.FromSeconds(graceSeconds <= 0 ? DefaultGracePeriod.TotalSeconds : graceSeconds)
        };
    }

    public static WindowConfiguration FromConfiguration(IConfiguration section)
    {
        if (section == null)
        {
            throw new ArgumentNullException(nameof(section));
        }

        var windowSize = section.GetValue<TimeSpan>("WindowSize", TimeSpan.Zero);
        var grace = section.GetValue<TimeSpan>("GracePeriod", DefaultGracePeriod);
        var sweep = section.GetValue<TimeSpan>("SweepInterval", DefaultSweepInterval);
        var idle = section.GetValue<TimeSpan>("IdleEviction", TimeSpan.FromMinutes(5));

        var configuration = new WindowConfiguration
        {
            WindowSize = windowSize,
            GracePeriod = grace,
            SweepInterval = sweep,
            IdleEviction = idle
        };

        configuration.Validate();
        return configuration;
    }

    public void Validate()
    {
        if (WindowSize <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("Window size must be greater than zero.");
        }

        if (GracePeriod < TimeSpan.Zero)
        {
            throw new InvalidOperationException("Grace period must not be negative.");
        }

        if (GracePeriod >= WindowSize)
        {
            throw new InvalidOperationException("Grace period must be shorter than the window size.");
        }

        if (SweepInterval <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("Sweep interval must be greater than zero.");
        }

        if (IdleEviction < SweepInterval)
        {
            throw new InvalidOperationException("Idle eviction threshold must exceed sweep interval.");
        }
    }
}


