using System;

namespace Ksql.Linq.Window;

internal static class WindowingMath
{
    public static DateTime FloorToWindow(DateTime utcInstant, TimeSpan windowSize)
    {
        if (utcInstant.Kind != DateTimeKind.Utc)
        {
            throw new ArgumentException("Timestamp must be UTC.", nameof(utcInstant));
        }

        var ticks = utcInstant.Ticks - (utcInstant.Ticks % windowSize.Ticks);
        return new DateTime(ticks, DateTimeKind.Utc);
    }
}