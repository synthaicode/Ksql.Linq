using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Infrastructure.Ksql;
using Ksql.Linq.Query.Metadata;
using Xunit;

namespace Ksql.Linq.Tests.Infrastructure.Ksql;

public class KsqlRowsLastOrchestratorTests
{
    private class RowsRecord
    {
        public string Broker { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public string BrokerHead { get; set; } = string.Empty;
        public double RoundAvg1 { get; set; }
    }

    [Fact]
    public async Task EnsureAsync_UsesTimestampColumnWhenPresent()
    {
        var rowsModel = new EntityModel
        {
            EntityType = typeof(RowsRecord),
            TopicName = "bar_1s_rows",
            KeyProperties = new[]
            {
                typeof(RowsRecord).GetProperty(nameof(RowsRecord.Broker))!,
                typeof(RowsRecord).GetProperty(nameof(RowsRecord.Symbol))!
            },
            AllProperties = typeof(RowsRecord).GetProperties()
        };

        var metadata = new QueryMetadata
        {
            Identifier = "bar_1s_rows",
            Role = "Final1sStream",
            TimeframeRaw = "1s",
            TimestampColumn = "Timestamp",
            Keys = new QueryKeyShape(
                new[] { "BROKER", "SYMBOL" },
                new[] { typeof(string), typeof(string) },
                new[] { false, false }),
            Projection = new QueryProjectionShape(
                new[] { "TIMESTAMP", "BROKERHEAD", "ROUNDAVG1" },
                new[] { typeof(DateTime), typeof(string), typeof(double) },
                new[] { false, false, false })
        };

        QueryMetadataWriter.Apply(rowsModel, metadata);

        var executed = new List<string>();
        Task<KsqlDbResponse> ExecuteAsync(string sql)
        {
            executed.Add(sql);
            if (sql.StartsWith("CREATE TABLE IF NOT EXISTS BAR_1S_ROWS_LAST", StringComparison.OrdinalIgnoreCase))
                return Task.FromResult(new KsqlDbResponse(false, "forced failure"));
            return Task.FromResult(new KsqlDbResponse(true, string.Empty));
        }

        var response = await KsqlRowsLastOrchestrator.EnsureAsync(
            ExecuteAsync,
            () => Task.FromResult(new HashSet<string>(StringComparer.OrdinalIgnoreCase)),
            () => Task.FromResult(new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "bar_1s_rows" }),
            rowsModel,
            ddlRetryCount: 0,
            ddlRetryInitialDelayMs: 0,
            publishEvent: null);

        Assert.False(response.IsSuccess);
        var ddl = executed.FindLast(sql => sql.StartsWith("CREATE TABLE IF NOT EXISTS BAR_1S_ROWS_LAST", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(ddl);
        Assert.Contains("LATEST_BY_OFFSET(TIMESTAMP)", ddl!, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("LATEST_BY_OFFSET(BUCKETSTART", ddl!, StringComparison.OrdinalIgnoreCase);
    }
}
