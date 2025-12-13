using System;
using System.Linq.Expressions;
using Ksql.Linq;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Tests.Utils;
using Xunit;
using System.Linq;

namespace Ksql.Linq.Tests.Query.Builders;

[Trait("Level", TestLevel.L3)]
public class HoppingSampleDdlTests
{
    private class Transaction
    {
        [KsqlKey(1)] public string TransactionId { get; set; } = string.Empty;
        public string UserId { get; set; } = string.Empty;
        public double Amount { get; set; }
        public string Currency { get; set; } = string.Empty;
        public DateTime TransactionTime { get; set; }
    }

    [Fact]
    public void BuildHopping_DdlMatchesSampleShape()
    {
        var model = new KsqlQueryRoot()
            .From<Transaction>()
            .Hopping(t => t.TransactionTime, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1))
            .GroupBy(t => new { t.UserId })
            .Select(g => new
            {
                g.Key.UserId,
                WindowStart = g.WindowStart(),
                Cnt = g.Count(),
                Total = g.Sum(x => x.Amount)
            })
            .Build();

        var ddl = KsqlCreateWindowedStatementBuilder.BuildHopping("USER_TRANSACTION_STATS", model);

        SqlAssert.StartsWithNormalized(ddl, "CREATE TABLE IF NOT EXISTS USER_TRANSACTION_STATS");
        SqlAssert.ContainsNormalized(ddl, "WINDOW HOPPING ( SIZE 5 MINUTES , ADVANCE BY 1 MINUTES )");
        SqlAssert.ContainsNormalized(ddl, "GROUP BY USERID");
        // WindowStart is omitted from value schema to keep payload lean; window bounds come from windowed key.
        SqlAssert.DoesNotContainNormalized(ddl, "WINDOWSTART AS");
    }
}
