using System;
using System.Collections.Generic;
using System.Linq;
using Ksql.Linq.Infrastructure.Ksql;
using Xunit;

namespace Ksql.Linq.Tests.Core;

public class ShowQueriesParserTests
{
    [Fact]
    public void FindQueryIdInShowQueries_ByTopic_ReturnsId()
    {
        var output = @"
+------------------------+----------------+-----------------------------------------+--------+
| Query ID               | Kafka Topic    | Query String                            | Status |
+------------------------+----------------+-----------------------------------------+--------+
| CTAS_BAR_1S_123        | BAR_1S_FINAL   | CREATE TABLE BAR_1S_FINAL AS SELECT *;  | RUNNING|
+------------------------+----------------+-----------------------------------------+--------+";

        var sampleLine = "| CTAS_BAR_1S_123        | BAR_1S_FINAL   | CREATE TABLE BAR_1S_FINAL AS SELECT *;  | RUNNING|";
        var columns = sampleLine.Split('|', StringSplitOptions.RemoveEmptyEntries)
            .Select(c => c.Trim())
            .Where(c => !string.IsNullOrEmpty(c))
            .ToArray();
        Assert.Equal("CTAS_BAR_1S_123", columns[0]);

        var normalizedTopic = KsqlWaitService.NormalizeIdentifier("BAR_1S_FINAL");
        Assert.Contains(normalizedTopic, sampleLine.ToUpperInvariant());

        var parsed = sampleLine.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
        Assert.Equal("CTAS_BAR_1S_123", parsed[0]);
        Assert.True(parsed.Count > 1);
        var topicsNormalized = parsed[1].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(t => KsqlWaitService.NormalizeIdentifier(t.Replace("\"", string.Empty)))
            .ToList();
        Assert.Contains(normalizedTopic, topicsNormalized);
        var lineUpper = sampleLine.Trim().ToUpperInvariant();
        Assert.True(topicsNormalized.Any(t => t.Equals(normalizedTopic, System.StringComparison.OrdinalIgnoreCase)) || lineUpper.Contains(normalizedTopic));

        string? manualId = null;
        foreach (var rawLine in output.Split('\n', System.StringSplitOptions.RemoveEmptyEntries))
        {
            var line = rawLine.Trim();
            if (string.IsNullOrEmpty(line) || !line.Contains('|') || line.StartsWith("+") || line.StartsWith("-"))
                continue;

            var cols = line.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
            if (cols.Count == 0)
                continue;
            var qid = cols[0];
            if (string.IsNullOrEmpty(qid) || qid.Equals("Query ID", System.StringComparison.OrdinalIgnoreCase))
                continue;

            var lineUpperManual = line.ToUpperInvariant();
            var topicHit = cols.Count > 1 && cols[1].Split(',', System.StringSplitOptions.RemoveEmptyEntries | System.StringSplitOptions.TrimEntries)
                .Select(t => KsqlWaitService.NormalizeIdentifier(t.Replace("\"", string.Empty)))
                .Any(t => t.Equals(normalizedTopic, System.StringComparison.OrdinalIgnoreCase));
            if (!topicHit && lineUpperManual.Contains(normalizedTopic))
            {
                topicHit = true;
            }

            if (topicHit)
            {
                manualId = qid;
                break;
            }
        }

        Assert.Equal("CTAS_BAR_1S_123", manualId);

        var result = KsqlWaitService.FindQueryIdInShowQueries(output, "BAR_1S_FINAL", null);

        Assert.Equal("CTAS_BAR_1S_123", result);
    }

    [Fact]
    public void FindQueryIdInShowQueries_ByStatement_ReturnsId()
    {
        var output = @"
+------------------------+----------------+--------------------------------------------------+--------+
| Query ID               | Kafka Topic    | Query String                                     | Status |
+------------------------+----------------+--------------------------------------------------+--------+
| CTAS_ANOTHER_456       |                | CREATE STREAM FOO AS SELECT * FROM BAR EMIT CHANGES; | RUNNING|
+------------------------+----------------+--------------------------------------------------+--------+";

        var result = KsqlWaitService.FindQueryIdInShowQueries(output, "FOO", "CREATE STREAM FOO AS SELECT * FROM BAR EMIT CHANGES;");

        Assert.Equal("CTAS_ANOTHER_456", result);
    }

    [Fact]
    public void FindQueryIdInShowQueries_NoMatch_ReturnsNull()
    {
        var output = @"
+-----------+-------------+------------------+--------+
| Query ID  | Kafka Topic | Query String     | Status |
+-----------+-------------+------------------+--------+
| Q1        | TOPIC_A     | SELECT * FROM A; | RUNNING|
+-----------+-------------+------------------+--------+";

        var result = KsqlWaitService.FindQueryIdInShowQueries(output, "TOPIC_B", "SELECT * FROM B;");

        Assert.Null(result);
    }
}
