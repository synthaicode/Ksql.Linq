using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Retry;
using System;
using System.Threading.Tasks;

namespace Ksql.Linq.Infrastructure.Ksql;

/// <summary>
/// Thin async helpers that call ksqlDB control-plane statements and delegate
/// parsing/matching to KsqlWaitService. Accepts an executor delegate to avoid
/// tight coupling to KsqlContext.
/// </summary>
internal static class KsqlWaitClient
{
    public static async Task<string?> TryGetQueryIdFromShowQueriesAsync(
        Func<string, Task<KsqlDbResponse>> execute,
        string targetTopic,
        string? statement,
        int attempts = 5,
        int delayMs = 1000)
    {
        var policy = new RetryPolicy
        {
            MaxAttempts = Math.Max(1, attempts),
            InitialDelay = TimeSpan.FromMilliseconds(Math.Max(0, delayMs)),
            Strategy = BackoffStrategy.Fixed,
            IsRetryable = _ => true
        };

        try
        {
            return await policy.ExecuteAsync(async () =>
            {
                var response = await execute("SHOW QUERIES;").ConfigureAwait(false);
                if (response.IsSuccess && !string.IsNullOrWhiteSpace(response.Message))
                {
                    var queryId = KsqlWaitService.FindQueryIdInShowQueries(response.Message, targetTopic, statement);
                    if (!string.IsNullOrEmpty(queryId))
                        return queryId;
                }
                throw new InvalidOperationException("Query ID not found yet");
            }).ConfigureAwait(false);
        }
        catch
        {
            return null;
        }
    }

    public static async Task<bool> ConfirmQueryViaShowQueriesAsync(
        Func<string, Task<KsqlDbResponse>> execute,
        string queryId,
        string targetTopic)
    {
        var response = await execute("SHOW QUERIES;").ConfigureAwait(false);
        if (!response.IsSuccess || string.IsNullOrWhiteSpace(response.Message))
            return false;
        return KsqlWaitService.ShowQueriesContainsQuery(response.Message, queryId, targetTopic);
    }

    public static async Task<bool> ConfirmQueryViaDescribeAsync(
        Func<string, Task<KsqlDbResponse>> execute,
        string queryId,
        string targetTopic)
    {
        if (string.IsNullOrWhiteSpace(queryId) || string.IsNullOrWhiteSpace(targetTopic))
            return false;
        var normalized = KsqlWaitService.NormalizeIdentifier(targetTopic);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;
        var sql = $"DESCRIBE {normalized} EXTENDED;";
        var response = await execute(sql).ConfigureAwait(false);
        if (!response.IsSuccess || string.IsNullOrWhiteSpace(response.Message))
            return false;
        return KsqlWaitService.DescribeExtendedContainsQuery(response.Message, queryId);
    }

    public static async Task<bool> ConfirmEntityExistsAsync(
        Func<string, Task<KsqlDbResponse>> execute,
        string entityName)
    {
        var normalized = KsqlWaitService.NormalizeIdentifier(entityName);
        if (string.IsNullOrEmpty(normalized)) return false;
        var tables = await execute("SHOW TABLES;").ConfigureAwait(false);
        if (tables.IsSuccess && KsqlWaitService.ShowListingContainsEntity(tables.Message, normalized))
            return true;
        var streams = await execute("SHOW STREAMS;").ConfigureAwait(false);
        if (streams.IsSuccess && KsqlWaitService.ShowListingContainsEntity(streams.Message, normalized))
            return true;
        return false;
    }

    public static async Task WaitForQueryRunningAsync(
        Func<string, Task<KsqlDbResponse>> execute,
        string targetEntityName,
        string? queryId,
        TimeSpan timeout,
        KsqlDslOptions? options = null)
    {
        var required = ResolveRequiredConsecutiveSuccess(options);
        var stability = ResolveStabilityWindow(options);
        var interval = ResolvePollInterval(options);
        await WaitForQueryRunningAsync(execute, targetEntityName, queryId, timeout, required, interval, stability).ConfigureAwait(false);
    }

    public static async Task WaitForQueryRunningAsync(
        Func<string, Task<KsqlDbResponse>> execute,
        string targetEntityName,
        string? queryId,
        TimeSpan timeout,
        int requiredConsecutive,
        TimeSpan pollInterval,
        TimeSpan stability,
        KsqlDslOptions? options = null)
    {
        var deadline = DateTime.UtcNow + timeout;
        var targetUpper = KsqlWaitService.NormalizeIdentifier(targetEntityName);
        var qidNorm = KsqlWaitService.NormalizeIdentifier(queryId);
        var consecutive = 0;
        var required = requiredConsecutive > 0 ? requiredConsecutive : ResolveRequiredConsecutiveSuccess(options);
        while (DateTime.UtcNow < deadline)
        {
            var resp = await execute("SHOW QUERIES;").ConfigureAwait(false);
            if (resp.IsSuccess && !string.IsNullOrWhiteSpace(resp.Message))
            {
                var running = KsqlWaitService.TryGetQueryStateFromJson(resp.Message, targetUpper, qidNorm, out var state)
                    ? string.Equals(state, "RUNNING", StringComparison.OrdinalIgnoreCase)
                    : KsqlWaitService.CheckQueryRunningInText(resp.Message.Split('\n', StringSplitOptions.RemoveEmptyEntries), targetUpper, qidNorm);
                if (running)
                {
                    consecutive++;
                    if (consecutive >= required)
                    {
                        if (stability > TimeSpan.Zero)
                        {
                            await Task.Delay(stability).ConfigureAwait(false);
                            var confirm = await execute("SHOW QUERIES;").ConfigureAwait(false);
                            if (confirm.IsSuccess && !string.IsNullOrWhiteSpace(confirm.Message))
                            {
                                var ok = KsqlWaitService.TryGetQueryStateFromJson(confirm.Message, targetUpper, qidNorm, out var st)
                                    ? string.Equals(st, "RUNNING", StringComparison.OrdinalIgnoreCase)
                                    : KsqlWaitService.CheckQueryRunningInText(confirm.Message.Split('\n', StringSplitOptions.RemoveEmptyEntries), targetUpper, qidNorm);
                                if (ok) return;
                                consecutive = 0;
                            }
                        }
                        else return;
                    }
                }
                else
                {
                    consecutive = 0;
                }
            }
            var delay = pollInterval > TimeSpan.Zero ? pollInterval : ResolvePollInterval(options);
            await Task.Delay(delay).ConfigureAwait(false);
        }
        throw new TimeoutException($"CTAS/CSAS query for {targetEntityName} did not reach RUNNING within {timeout.TotalSeconds}s");
    }

    public static async Task WaitForPersistentQueryAsync(
        Func<string, Task<KsqlDbResponse>> execute,
        string targetEntityName,
        string statement,
        TimeSpan timeout,
        KsqlDslOptions? options = null)
    {
        var deadline = DateTime.UtcNow + timeout;
        string? resolvedQueryId = null;
        var showConfirmed = false;
        var describeConfirmed = false;
        while (DateTime.UtcNow < deadline)
        {
            if (string.IsNullOrWhiteSpace(resolvedQueryId))
                resolvedQueryId = await TryGetQueryIdFromShowQueriesAsync(execute, targetEntityName, statement).ConfigureAwait(false);

            if (!string.IsNullOrWhiteSpace(resolvedQueryId))
            {
                if (!showConfirmed)
                    showConfirmed = await ConfirmQueryViaShowQueriesAsync(execute, resolvedQueryId, targetEntityName).ConfigureAwait(false);
                if (!describeConfirmed)
                    describeConfirmed = await ConfirmQueryViaDescribeAsync(execute, resolvedQueryId, targetEntityName).ConfigureAwait(false);

                if (showConfirmed && describeConfirmed)
                {
                    await WaitForQueryRunningAsync(execute, targetEntityName, resolvedQueryId, TimeSpan.FromSeconds(60), options: options).ConfigureAwait(false);
                    return;
                }
                if (describeConfirmed && !showConfirmed)
                {
                    return; // consider stabilized by DESCRIBE
                }
            }

            if (!showConfirmed || !describeConfirmed || string.IsNullOrWhiteSpace(resolvedQueryId))
            {
                var exists = await ConfirmEntityExistsAsync(execute, targetEntityName).ConfigureAwait(false);
                if (exists) return;
            }
            await Task.Delay(1000).ConfigureAwait(false);
        }

        if (!string.IsNullOrWhiteSpace(resolvedQueryId))
            return; // trust command response despite timeout

        // last resort: entity exists
        if (await ConfirmEntityExistsAsync(execute, targetEntityName).ConfigureAwait(false))
            return;

        throw new TimeoutException($"Persistent query for {targetEntityName} did not register within the expected window.");
    }

    private static int ResolveRequiredConsecutiveSuccess(KsqlDslOptions? options)
        => options?.KsqlQueryRunningConsecutiveCount > 0
            ? options.KsqlQueryRunningConsecutiveCount
            : 5;

    private static TimeSpan ResolveStabilityWindow(KsqlDslOptions? options)
    {
        var seconds = options?.KsqlQueryRunningStabilityWindowSeconds ?? 15;
        if (seconds < 0) seconds = 0;
        return TimeSpan.FromSeconds(seconds);
    }

    private static TimeSpan ResolvePollInterval(KsqlDslOptions? options)
    {
        var ms = options?.KsqlQueryRunningPollIntervalMs ?? 2000;
        if (ms <= 0) ms = 2000;
        return TimeSpan.FromMilliseconds(ms);
    }
}
