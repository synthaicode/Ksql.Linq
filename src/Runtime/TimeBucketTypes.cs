using Ksql.Linq.Core.Attributes;
using System;
using System.Collections.Concurrent;

namespace Ksql.Linq.Runtime;

internal static class TimeBucketTypes
{
    // Key by base topic alias rather than Type to respect [KsqlTopic("alias")]
    private static readonly ConcurrentDictionary<(string BaseTopic, string Period, string Mode), Type> Map = new();

    public static void RegisterRead(Type baseType, Period period, Type concrete)
    {
        var topic = GetBaseTopic(baseType);
        Map[(topic, period.ToString(), "read")] = concrete;
    }

    public static void RegisterWrite(Type baseType, Period period, Type concrete)
    {
        var topic = GetBaseTopic(baseType);
        Map[(topic, period.ToString(), "write")] = concrete;
    }

    public static Type? ResolveRead(Type baseType, Period period)
    {
        var topic = GetBaseTopic(baseType);
        return Map.TryGetValue((topic, period.ToString(), "read"), out var t) ? t : baseType;
    }

    public static Type? ResolveWrite(Type baseType, Period period)
    {
        var topic = GetBaseTopic(baseType);
        return Map.TryGetValue((topic, period.ToString(), "write"), out var t) ? t : baseType;
    }

    // Resolve base topic respecting [KsqlTopic("alias")]
    public static string GetBaseTopic(Type baseType)
    {
        var attr = (KsqlTopicAttribute?)Attribute.GetCustomAttribute(baseType, typeof(KsqlTopicAttribute));
        return (attr?.Name ?? baseType.Name).ToLowerInvariant();
    }

    public static string GetLiveTopicName(Type baseType, Period period)
    {
        var topic = GetBaseTopic(baseType);
        if (period.Unit == PeriodUnit.Seconds && period.Value == 1)
            return $"{topic}_1s_rows";
        return $"{topic}_{period}_live";
    }

    // ========================================
    // Hopping Windows Support (MVP)
    // ========================================

    public static void RegisterHoppingRead(Type baseType, Period period, TimeSpan hopInterval, Type concrete)
    {
        var topic = GetBaseTopic(baseType);
        var hopStr = FormatHopInterval(hopInterval);
        Map[(topic, $"{period}:hop{hopStr}", "read")] = concrete;
    }

    public static Type? ResolveHoppingRead(Type baseType, Period period, TimeSpan hopInterval)
    {
        var topic = GetBaseTopic(baseType);
        var hopStr = FormatHopInterval(hopInterval);
        return Map.TryGetValue((topic, $"{period}:hop{hopStr}", "read"), out var t) ? t : baseType;
    }

    public static string GetHoppingLiveTopicName(Type baseType, Period period, TimeSpan hopInterval)
    {
        var topic = GetBaseTopic(baseType);
        var hopStr = FormatHopInterval(hopInterval);
        return $"{topic}_{period}_hop{hopStr}_live";
    }

    private static string FormatHopInterval(TimeSpan hop)
    {
        if (hop.TotalMinutes < 60 && hop.TotalMinutes == (int)hop.TotalMinutes)
            return $"{(int)hop.TotalMinutes}m";
        if (hop.TotalHours < 24 && hop.TotalHours == (int)hop.TotalHours)
            return $"{(int)hop.TotalHours}h";
        if (hop.TotalDays == (int)hop.TotalDays)
            return $"{(int)hop.TotalDays}d";
        return $"{(int)hop.TotalSeconds}s";
    }
}
