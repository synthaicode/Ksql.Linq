using Ksql.Linq.Core.Abstractions;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Query.Dsl;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Ksql.Linq.Tests.Utils;
using Xunit;

namespace Ksql.Linq.Tests.Query.Dsl;

[Trait("Level", TestLevel.L3)]
public class HoppingJoinValidationTests
{
    private class Transaction
    {
        [KsqlKey(1)] public string UserId { get; set; } = string.Empty;
        public DateTime TransactionTime { get; set; }
        public double Amount { get; set; }
    }

    [KsqlTable]
    private class UserMaster
    {
        [KsqlKey(1)] public string UserId { get; set; } = string.Empty;
        public string Tier { get; set; } = string.Empty;
    }

    private class UserTransactionStat : IWindowedRecord
    {
        [KsqlKey(1)] public string UserId { get; set; } = string.Empty;
        public string Tier { get; set; } = string.Empty;
        public long TransactionCount { get; set; }
        DateTime IWindowedRecord.WindowStart => DateTime.MinValue;
        DateTime IWindowedRecord.WindowEnd => DateTime.MinValue;
    }

    [Fact]
    public void StreamTableJoinWithHopping_ValidOrder_Passes()
    {
        var model = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(Transaction), typeof(UserMaster) },
            JoinCondition = (Expression<Func<Transaction, UserMaster, bool>>)((t, u) => t.UserId == u.UserId),
            GroupByExpression = (Expression<Func<Transaction, object>>)(t => new { t.UserId }),
            SelectProjection = (Expression<Func<Transaction, UserMaster, UserTransactionStat>>)((t, u) => new UserTransactionStat
            {
                UserId = t.UserId,
                Tier = u.Tier,
                TransactionCount = 1
            }),
            Hopping = new HoppingWindowSpec
            {
                Size = TimeSpan.FromMinutes(5),
                Advance = TimeSpan.FromMinutes(1)
            }
        };
        model.OperationSequence.AddRange(new[] { "From", "Join", "Hopping", "GroupBy", "Select" });

        ToQueryValidator.ValidateHoppingPipeline(typeof(UserTransactionStat), model);
    }

    [Fact]
    public void HoppingBeforeJoin_IsRejected()
    {
        var model = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(Transaction), typeof(UserMaster) },
            JoinCondition = (Expression<Func<Transaction, UserMaster, bool>>)((t, u) => t.UserId == u.UserId),
            GroupByExpression = (Expression<Func<Transaction, object>>)(t => new { t.UserId }),
            SelectProjection = (Expression<Func<Transaction, UserMaster, UserTransactionStat>>)((t, u) => new UserTransactionStat
            {
                UserId = t.UserId,
                Tier = u.Tier,
                TransactionCount = 1
            }),
            Hopping = new HoppingWindowSpec
            {
                Size = TimeSpan.FromMinutes(5),
                Advance = TimeSpan.FromMinutes(1)
            }
        };
        model.OperationSequence.AddRange(new[] { "From", "Hopping", "Join", "GroupBy", "Select" });

        Assert.Throws<InvalidOperationException>(() => ToQueryValidator.ValidateHoppingPipeline(typeof(UserTransactionStat), model));
    }

    private class NonTableUser
    {
        [KsqlKey(1)] public string UserId { get; set; } = string.Empty;
    }

    [Fact]
    public void StreamToStreamJoin_IsRejected()
    {
        var model = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(Transaction), typeof(NonTableUser) },
            JoinCondition = (Expression<Func<Transaction, NonTableUser, bool>>)((t, u) => t.UserId == u.UserId),
            GroupByExpression = (Expression<Func<Transaction, object>>)(t => new { t.UserId }),
            SelectProjection = (Expression<Func<Transaction, NonTableUser, UserTransactionStat>>)((t, u) => new UserTransactionStat
            {
                UserId = t.UserId,
                Tier = string.Empty,
                TransactionCount = 1
            }),
            Hopping = new HoppingWindowSpec
            {
                Size = TimeSpan.FromMinutes(5),
                Advance = TimeSpan.FromMinutes(1)
            }
        };
        model.OperationSequence.AddRange(new[] { "From", "Join", "Hopping", "GroupBy", "Select" });

        Assert.Throws<NotSupportedException>(() => ToQueryValidator.ValidateHoppingPipeline(typeof(UserTransactionStat), model));
    }
}
