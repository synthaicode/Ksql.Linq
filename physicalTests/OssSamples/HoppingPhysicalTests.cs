using System;
using System.Linq;
using System.Threading.Tasks;
using Ksql.Linq;
using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Modeling;
using Microsoft.Extensions.Logging;
using PhysicalTestEnv;
using Xunit;
using Ksql.Linq.Runtime;

namespace Ksql.Linq.Tests.Integration;

public sealed class Transaction
{
    [KsqlKey(1)] public string TransactionId { get; set; } = string.Empty;
    public string UserId { get; set; } = string.Empty;
    public double Amount { get; set; }
    public string Currency { get; set; } = string.Empty;
    [KsqlTimestamp] public DateTime TransactionTime { get; set; }
}

[KsqlTopic("hopping_usertransactionstat")]
public sealed class UserTransactionStat : IWindowedRecord
{
    [KsqlKey(1)] public string UserId { get; set; } = string.Empty;
    [KsqlIgnore] public DateTime WindowStart { get; set; }
    [KsqlIgnore] public DateTime WindowEnd { get; set; }
    public DateTime TransactionTime { get; set; }
    public long TransactionCount { get; set; }
    public double TotalAmount { get; set; }
    public double MaxAmount { get; set; }
    DateTime IWindowedRecord.WindowStart => WindowStart;
    DateTime IWindowedRecord.WindowEnd => WindowEnd;
}

// Composite-key variant (for hopping with multiple keys)
public sealed class CompositeTransaction
{
    [KsqlKey(1)] public string Region { get; set; } = string.Empty;
    [KsqlKey(2)] public string UserId { get; set; } = string.Empty;
    public double Amount { get; set; }
    public string Currency { get; set; } = string.Empty;
    [KsqlTimestamp] public DateTime TransactionTime { get; set; }
}

[KsqlTopic("hopping_composite_userstat")]
public sealed class CompositeUserStat : IWindowedRecord
{
    [KsqlKey(1)] public string Region { get; set; } = string.Empty;
    [KsqlKey(2)] public string UserId { get; set; } = string.Empty;
    [KsqlIgnore] public DateTime WindowStart { get; set; }
    [KsqlIgnore] public DateTime WindowEnd { get; set; }
    public DateTime TransactionTime { get; set; }
    public long TransactionCount { get; set; }
    public double TotalAmount { get; set; }
    public double MaxAmount { get; set; }
    DateTime IWindowedRecord.WindowStart => WindowStart;
    DateTime IWindowedRecord.WindowEnd => WindowEnd;
}

internal sealed class TestContext : KsqlContext, IDesignTimeKsqlContextFactory
{
    private static bool _designTimeForce;
    private static readonly ILoggerFactory _lf = LoggerFactory.Create(b =>
    {
        b.AddConsole();
        b.SetMinimumLevel(LogLevel.Information);
    });

    private bool _designTime;
    private readonly bool _includeComposite;

    public TestContext() : this(true, true)
    {
    }

    public TestContext(bool designTime = true, bool includeComposite = true)
        : base(BuildOptions(designTime), _lf)
    {
        _designTime = designTime;
        _includeComposite = includeComposite;
    }

    public EventSet<Transaction> Transactions { get; set; } = null!;
    public EventSet<CompositeTransaction> CompositeTransactions { get; set; } = null!;

    protected override bool SkipSchemaRegistration => (_designTime || _designTimeForce);
    protected override bool IsDesignTime => (_designTime || _designTimeForce);

    protected override void OnModelCreating(IModelBuilder modelBuilder)
    {
        // Align Transaction key/value schema names with SR registration
        var statBuilder = modelBuilder.Entity<UserTransactionStat>();

        statBuilder.ToQuery(q => q.From<Transaction>()
                .Hopping(t => t.TransactionTime, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1))
                .GroupBy(t => new { t.UserId })
                .Select(g => new UserTransactionStat
                {
                    UserId = g.Key.UserId,
                    TransactionTime = g.Max(x => x.TransactionTime),
                    TransactionCount = g.Count(),
                    TotalAmount = g.Sum(x => x.Amount),
                    MaxAmount = g.Max(x => x.Amount)
                }));

        if (_includeComposite)
        {
            modelBuilder.Entity<CompositeUserStat>()
                .ToQuery(q => q.From<CompositeTransaction>()
                    .Hopping(t => t.TransactionTime, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1))
                    .GroupBy(t => new { t.Region, t.UserId })
                    .Select(g => new CompositeUserStat
                    {
                        Region = g.Key.Region,
                        UserId = g.Key.UserId,
                        TransactionTime = g.Max(x => x.TransactionTime),
                        TransactionCount = g.Count(),
                        TotalAmount = g.Sum(x => x.Amount),
                        MaxAmount = g.Max(x => x.Amount)
                    }));
        }
    }

    // IDesignTimeKsqlContextFactory implementation for CLI script generation
    public KsqlContext CreateDesignTimeContext() => new TestContext(designTime: true);

    private static KsqlDslOptions BuildOptions(bool designTime)
    {
        // Ensure IsDesignTime is true during base constructor execution
        _designTimeForce = designTime;
        return new KsqlDslOptions
        {
            Common = new CommonSection { BootstrapServers = "127.0.0.1:39092" },
            SchemaRegistry = new Ksql.Linq.Core.Configuration.SchemaRegistrySection { Url = "http://127.0.0.1:18081" },
            KsqlDbUrl = "http://127.0.0.1:18088"
        };
    }
}

[Collection("KsqlExclusive")]
public class HoppingPhysicalTests
{
    [Fact]
    public async Task Hopping_Ctas_StartsAndRegisters()
    {
        await KsqlHelpers.TerminateAllAsync("http://127.0.0.1:18088");
        await KsqlHelpers.DropArtifactsAsync("http://127.0.0.1:18088", new[]
        {
            "HOPPING_USERTRANSACTIONSTAT",
            "TRANSACTION"
        });
        await EnsureKafkaTopicAsync("transactions", partitions: 1, replicationFactor: 1);

        await using var ctx = new TestContext(designTime: false, includeComposite: false);
        await ctx.StartAsync();

        var baseTime = DateTime.UtcNow;
        await ctx.Transactions.AddAsync(new Transaction
        {
            TransactionId = "txn_add_1",
            UserId = "user_add",
            Amount = 10.5,
            Currency = "USD",
            TransactionTime = baseTime
        });
        await ctx.Transactions.AddAsync(new Transaction
        {
            TransactionId = "txn_add_2",
            UserId = "user_add",
            Amount = 20.0,
            Currency = "EUR",
            TransactionTime = baseTime.AddMinutes(1)
        });

        var rows = new System.Collections.Generic.List<UserTransactionStat>();
        for (int i = 0; i < 10; i++)
        {
            rows = (await ctx.ReadHoppingAsync<UserTransactionStat>(
                key: new { UserId = "user_add" },
                from: baseTime.AddMinutes(-1),
                to: baseTime.AddMinutes(10),
                limit: 10,
                timeout: TimeSpan.FromSeconds(5))).ToList();
            if (rows.Count > 0) break;
            await Task.Delay(TimeSpan.FromSeconds(3));
        }
        foreach (var r in rows)
        {
            Console.WriteLine(
                $"row: UserId={r.UserId}, WindowStart={r.WindowStart:o}, WindowEnd={r.WindowEnd:o}, " +
                $"Count={r.TransactionCount}, Total={r.TotalAmount}, Max={r.MaxAmount}");
        }
        Assert.NotEmpty(rows);
    }

    [Fact]
    public async Task Hopping_CompositeKey_StartsAndRegisters()
    {
        await KsqlHelpers.TerminateAllAsync("http://127.0.0.1:18088");
        await KsqlHelpers.DropArtifactsAsync("http://127.0.0.1:18088", new[]
        {
            // ベースの単一キー/テーブルも毎回クリーンアップ（OnModelCreatingで両方作成するため）
            "HOPPING_USERTRANSACTIONSTAT",
            "TRANSACTION",
            "HOPPING_COMPOSITE_USERSTAT",
            "COMPOSITETRANSACTION"
        });
        await EnsureKafkaTopicAsync("compositetransaction", partitions: 1, replicationFactor: 1);

        await using var ctx = new TestContext(designTime: false);
        await ctx.StartAsync();

        var baseTime = DateTime.UtcNow;
        await ctx.CompositeTransactions.AddAsync(new CompositeTransaction
        {
            Region = "APAC",
            UserId = "user_join",
            Amount = 30.0,
            Currency = "USD",
            TransactionTime = baseTime
        });
        await ctx.CompositeTransactions.AddAsync(new CompositeTransaction
        {
            Region = "APAC",
            UserId = "user_join",
            Amount = 40.0,
            Currency = "USD",
            TransactionTime = baseTime.AddMinutes(1)
        });
    }

    private static async Task EnsureKafkaTopicAsync(string topicName, int partitions, short replicationFactor)
    {
        using var admin = new Confluent.Kafka.AdminClientBuilder(new Confluent.Kafka.AdminClientConfig { BootstrapServers = "127.0.0.1:39092" }).Build();
        try
        {
            var metadata = admin.GetMetadata(topicName, TimeSpan.FromSeconds(2));
            if (metadata?.Topics != null && metadata.Topics.Count > 0 && metadata.Topics[0].Error.Code == Confluent.Kafka.ErrorCode.NoError)
                return;
        }
        catch { }

        try
        {
            await admin.CreateTopicsAsync(new[]
            {
                new Confluent.Kafka.Admin.TopicSpecification { Name = topicName, NumPartitions = partitions, ReplicationFactor = replicationFactor }
            }).ConfigureAwait(false);
        }
        catch (Confluent.Kafka.Admin.CreateTopicsException ex)
        {
            if (ex.Results.TrueForAll(r => r.Error.Code == Confluent.Kafka.ErrorCode.TopicAlreadyExists))
                return;
            throw;
        }
    }
}
