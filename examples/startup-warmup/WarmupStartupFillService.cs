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
        // Use console output for example logging to avoid relying on internal context logger
        Action<string> info = s => Console.WriteLine($"[warmup] {s}");
        Action<string> warn = s => Console.WriteLine($"[warmup][warn] {s}");
        Action<Exception,string> warnEx = (ex, s) => Console.WriteLine($"[warmup][warn] {s}: {ex.Message}");
        Action<string> debug = s => Console.WriteLine($"[warmup][debug] {s}");

        try
        {
            var show = await context.ExecuteStatementAsync("SHOW STREAMS;").ConfigureAwait(false);
            if (!show.IsSuccess) warn($"SHOW STREAMS failed: {show.Message}");
            else info("ksqlDB reachable.");
        }
        catch (Exception ex)
        {
            warnEx(ex, "ksqlDB reachability check failed (continuing)");
        }

        foreach (var t in _tablesToProbe)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                var count = await context.PullCountAsync(t, where: null, timeout: TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                debug($"Pull-count on table {t}: {count}");
            }
            catch (Exception ex)
            {
                warnEx(ex, $"Pull-count on table {t} failed (continuing)");
            }
        }

        foreach (var s in _streamsToProbe)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                var sql = $"SELECT * FROM {s} EMIT CHANGES LIMIT 1;";
                var count = await context.QueryStreamCountAsync(sql, timeout: TimeSpan.FromSeconds(10)).ConfigureAwait(false);
                debug($"Stream-count on stream {s}: {count}");
            }
            catch (Exception ex)
            {
                warnEx(ex, $"Stream-count on stream {s} failed (continuing)");
            }
        }

        info("Startup warmup finished (read-only).");
    }
}
