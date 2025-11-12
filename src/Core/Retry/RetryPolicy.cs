using System;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Core.Retry;

public enum BackoffStrategy
{
    Fixed,
    Linear,
    Exponential
}

public class RetryPolicy
{
    public int MaxAttempts { get; init; } = 3;
    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(8);
    public BackoffStrategy Strategy { get; init; } = BackoffStrategy.Exponential;
    public Func<Exception, bool>? IsRetryable { get; init; }

    public static RetryPolicy Default => new()
    {
        MaxAttempts = 3,
        InitialDelay = TimeSpan.FromSeconds(1),
        MaxDelay = TimeSpan.FromSeconds(8),
        Strategy = BackoffStrategy.Exponential
    };

    public async Task ExecuteAsync(Func<Task> action, CancellationToken cancellationToken, Action<int, Exception>? onRetry)
    {
        if (action == null) throw new ArgumentNullException(nameof(action));

        var attempt = 0;
        var delay = InitialDelay;
        while (true)
        {
            try
            {
                attempt++;
                await action().ConfigureAwait(false);
                return;
            }
            catch (Exception ex)
            {
                if (!ShouldRetry(ex, attempt))
                    throw;

                onRetry?.Invoke(attempt, ex);
                await DelayAsync(delay, cancellationToken).ConfigureAwait(false);
                delay = NextDelay(delay);
            }
        }
    }

    public async Task<T> ExecuteAsync<T>(Func<Task<T>> action, CancellationToken cancellationToken, Action<int, Exception>? onRetry)
    {
        if (action == null) throw new ArgumentNullException(nameof(action));

        var attempt = 0;
        var delay = InitialDelay;
        while (true)
        {
            try
            {
                attempt++;
                return await action().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (!ShouldRetry(ex, attempt))
                    throw;

                onRetry?.Invoke(attempt, ex);
                await DelayAsync(delay, cancellationToken).ConfigureAwait(false);
                delay = NextDelay(delay);
            }
        }
    }

    // Convenience overloads without optional parameters
    public Task ExecuteAsync(Func<Task> action) => ExecuteAsync(action, CancellationToken.None, null);
    public Task ExecuteAsync(Func<Task> action, CancellationToken cancellationToken) => ExecuteAsync(action, cancellationToken, null);
    public Task<T> ExecuteAsync<T>(Func<Task<T>> action) => ExecuteAsync(action, CancellationToken.None, null);
    public Task<T> ExecuteAsync<T>(Func<Task<T>> action, CancellationToken cancellationToken) => ExecuteAsync(action, cancellationToken, null);

    private bool ShouldRetry(Exception ex, int attempt)
    {
        if (attempt >= MaxAttempts)
            return false;
        if (IsRetryable == null)
            return true;
        try { return IsRetryable(ex); } catch { return false; }
    }

    private Task DelayAsync(TimeSpan delay, CancellationToken ct)
    {
        try { return Task.Delay(delay, ct); } catch { return Task.CompletedTask; }
    }

    private TimeSpan NextDelay(TimeSpan current)
    {
        return Strategy switch
        {
            BackoffStrategy.Fixed => current,
            BackoffStrategy.Linear => Cap(current + InitialDelay),
            BackoffStrategy.Exponential => Cap(TimeSpan.FromMilliseconds(Math.Min(current.TotalMilliseconds * 2, MaxDelay.TotalMilliseconds))),
            _ => current
        };
    }

    private TimeSpan Cap(TimeSpan d) => d > MaxDelay ? MaxDelay : d;
}
