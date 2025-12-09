using Ksql.Linq;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace HoppingWindowExample;

/// <summary>
/// KsqlContext for consuming hopping window aggregation results
/// </summary>
public sealed class HoppingWindowConsumerContext : KsqlContext
{
    public HoppingWindowConsumerContext(IConfiguration cfg, ILoggerFactory? lf = null)
        : base(cfg, lf) { }
}

class ConsumerProgram
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("=== Hopping Window Consumer ===\n");
        Console.WriteLine("This consumer reads aggregated transaction statistics from the hopping window table.");
        Console.WriteLine("Press Ctrl+C to stop.\n");

        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
            Console.WriteLine("\nShutting down gracefully...");
        };

        try
        {
            var cfg = new ConfigurationBuilder()
                .AddJsonFile("appsettings-consumer.json", optional: false, reloadOnChange: false)
                .Build();

            using var lf = LoggerFactory.Create(b => b
                .SetMinimumLevel(LogLevel.Information)
                .AddConsole());

            await using var ctx = new HoppingWindowConsumerContext(cfg, lf);

            Console.WriteLine("Waiting for entity to be ready...");
            try
            {
                await ctx.WaitForEntityReadyAsync<UserTransactionStatsConsumer>(
                    TimeSpan.FromSeconds(30),
                    cts.Token);
                Console.WriteLine("Entity is ready. Starting to consume...\n");
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Cancelled during entity ready check.");
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Could not verify entity readiness: {ex.Message}");
                Console.WriteLine("Proceeding with consumption anyway...\n");
            }

            // Demonstrate pull queries first
            await DemonstratePullQueries(ctx, cts.Token);

            // Start consuming from the hopping window table
            Console.WriteLine("\n--- Starting real-time consumption ---\n");
            try
            {
                await ctx.Set<UserTransactionStatsConsumer>()
                    .ForEachAsync((stats, headers, meta) =>
                    {
                        Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] {stats}");
                        return Task.CompletedTask;
                    },
                    cancellationToken: cts.Token);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("\nConsumer stopped by user.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\nError during consumption: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\nFatal error: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
            Environment.ExitCode = 1;
        }
        finally
        {
            cts.Dispose();
            Console.WriteLine("\n=== Consumer finished ===");
        }
    }

    static async Task DemonstratePullQueries(HoppingWindowConsumerContext ctx, CancellationToken ct)
    {
        if (ct.IsCancellationRequested)
        {
            return;
        }

        Console.WriteLine("\n--- Pull Query Examples ---");

        try
        {
            var table = "USER_TRANSACTION_STATS";

            // Example 1: Get the latest window for a specific user
            var userId = Environment.GetEnvironmentVariable("DEMO_USER_ID") ?? "user_A";
            var latestSql = $@"
                SELECT
                    user_id,
                    WINDOWSTART,
                    WINDOWEND,
                    transaction_count,
                    total_amount
                FROM {table}
                WHERE user_id = '{userId.Replace("'", "''")}'
                ORDER BY WINDOWSTART DESC
                LIMIT 1;";

            Console.WriteLine($"\nQuerying latest window for user '{userId}'...");
            var latest = await ctx.QueryRowsAsync(latestSql, TimeSpan.FromSeconds(5), ct);

            if (latest.Count > 0)
            {
                var row = latest[0];
                Console.WriteLine($"  User: {row[0]}");
                Console.WriteLine($"  Window: {row[1]} - {row[2]}");
                Console.WriteLine($"  Transaction Count: {row[3]}");
                Console.WriteLine($"  Total Amount: ${row[4]}");
            }
            else
            {
                Console.WriteLine($"  No data found for user '{userId}'");
            }

            // Example 2: Get top users by transaction count in the most recent window
            var topUsersSql = $@"
                SELECT
                    user_id,
                    transaction_count,
                    total_amount
                FROM {table}
                ORDER BY WINDOWSTART DESC, transaction_count DESC
                LIMIT 5;";

            Console.WriteLine($"\nQuerying top users by transaction count...");
            var topUsers = await ctx.QueryRowsAsync(topUsersSql, TimeSpan.FromSeconds(5), ct);

            if (topUsers.Count > 0)
            {
                Console.WriteLine("  Top Users:");
                foreach (var row in topUsers)
                {
                    Console.WriteLine($"    - {row[0]}: {row[1]} transactions, ${row[2]} total");
                }
            }
            else
            {
                Console.WriteLine("  No data available");
            }

            // Example 3: Count total distinct users in the table
            var countSql = $"SELECT COUNT(DISTINCT user_id) FROM {table};";
            var count = await ctx.QueryCountAsync(countSql, TimeSpan.FromSeconds(5), ct);
            Console.WriteLine($"\nTotal distinct users in hopping window table: {count}");
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("\nPull query examples cancelled.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\nPull query examples failed: {ex.Message}");
            Console.WriteLine("This is expected if the table doesn't exist yet or has no data.");
        }

        Console.WriteLine("--- End of Pull Query Examples ---\n");
    }
}
