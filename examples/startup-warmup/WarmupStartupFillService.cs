using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Ksql.Linq;
using Ksql.Linq.Runtime.Fill;

namespace Examples.StartupWarmup;

/// <summary>
/// Read-only warmup at application startup.
/// - Checks ksqlDB reachability
/// - Warms pull-query paths for tables
/// - Warms push-query paths for streams (LIMIT 1)
/// Does not emit any synthetic records.
/// </summary>
public sealed class WarmupStartupFillService : IStartupFillService
{
    private readonly IReadOnlyCollection<string> _tablesToProbe;
    private readonly IReadOnlyCollection<string> _streamsToProbe;

    public WarmupStartupFillService(IEnumerable<string> tablesToProbe, IEnumerable<string>? streamsToProbe = null)
    {
        _tablesToProbe = new List<string>(tablesToProbe);
        _streamsToProbe = new List<string>(streamsToProbe ?? Array.Empty<string>());
    }

    public async Task RunAsync(KsqlContext context, CancellationToken ct)
    {
        var log = context.Logger;

        try
        {
            var show = await context.ExecuteStatementAsync("SHOW STREAMS;").ConfigureAwait(false);
            if (!show.IsSuccess) log?.LogWarning("Startup warmup: SHOW STREAMS failed: {Msg}", show.Message);
            else log?.LogInformation("Startup warmup: ksqlDB reachable.");
        }
        catch (Exception ex)
        {
            log?.LogWarning(ex, "Startup warmup: ksqlDB reachability check failed (continuing)");
        }

        foreach (var t in _tablesToProbe)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                var count = await context.PullCountAsync(t, limit: null, timeout: TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                log?.LogDebug("Warmup pull-count on table {Table}: {Count}", t, count);
            }
            catch (Exception ex)
            {
                log?.LogDebug(ex, "Warmup pull-count on table {Table} failed (continuing)", t);
            }
        }

        foreach (var s in _streamsToProbe)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                var sql = $"SELECT * FROM {s} EMIT CHANGES LIMIT 1;";
                var count = await context.QueryStreamCountAsync(sql, timeout: TimeSpan.FromSeconds(10)).ConfigureAwait(false);
                log?.LogDebug("Warmup stream-count on stream {Stream}: {Count}", s, count);
            }
            catch (Exception ex)
            {
                log?.LogDebug(ex, "Warmup stream-count on stream {Stream} failed (continuing)", s);
            }
        }

        log?.LogInformation("Startup warmup finished (read-only).");
    }
}

