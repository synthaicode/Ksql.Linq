using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Modeling;
using Ksql.Linq.Query.Pipeline;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using Xunit;
namespace Ksql.Linq.Tests.Query.Pipeline;

public class DMLQueryGeneratorTests
{
    private static T ExecuteInScope<T>(Func<T> func)
    {
        using (ModelCreatingScope.Enter())
        {
            return func();
        }
    }
    [Fact]
    public void GenerateSelectAll_WithPushQuery_AppendsEmitChanges()
    {
        var generator = new DMLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateSelectAll("s1", isPullQuery: false));
        Ksql.Linq.Tests.Utils.SqlAssert.StartsWithNormalized(query, "SELECT * FROM s1");
        File.AppendAllText("generated_queries.txt", query + Environment.NewLine);

    }

    [Fact]
    public void GenerateSelectWithCondition_Basic()
    {
        Expression<Func<TestEntity, bool>> expr = e => e.Id == 1;
        var generator = new DMLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateSelectWithCondition("s1", expr.Body, false));
        Ksql.Linq.Tests.Utils.SqlAssert.StartsWithNormalized(query, "SELECT * FROM s1 WHERE (Id = 1)");
        File.AppendAllText("generated_queries.txt", query + Environment.NewLine);
    }

    [Fact]
    public void GenerateSelectAll_TableQuery_NoEmitChanges()
    {
        var generator = new DMLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateSelectAll("t_orders", true, true));
        Ksql.Linq.Tests.Utils.SqlAssert.EqualNormalized("SELECT * FROM t_orders;", query);
        File.AppendAllText("generated_queries.txt", query + Environment.NewLine);
    }

    [Fact]
    public void GenerateSelectWithCondition_TableQuery_NoEmitChanges()
    {
        Expression<Func<TestEntity, bool>> expr = e => e.Id == 1;
        var generator = new DMLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateSelectWithCondition("t_orders", expr.Body, true, true));
        Ksql.Linq.Tests.Utils.SqlAssert.EqualNormalized("SELECT * FROM t_orders WHERE (Id = 1);", query);
        File.AppendAllText("generated_queries.txt", query + Environment.NewLine);
    }

    [Fact]
    public void GenerateCountQuery_ReturnsExpected()
    {
        var generator = new DMLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateCountQuery("t1"));
        Ksql.Linq.Tests.Utils.SqlAssert.EqualNormalized("SELECT COUNT(*) FROM t1;", query);
        File.AppendAllText("generated_queries.txt", query + Environment.NewLine);
    }

    [Fact]
    public void GenerateAggregateQuery_Basic()
    {
        Expression<Func<TestEntity, object>> expr = e => new { Sum = e.Id };
        var generator = new DMLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateAggregateQuery("t1", expr.Body));
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "FROM t1");
        Ksql.Linq.Tests.Utils.SqlAssert.StartsWithNormalized(query, "SELECT");
        File.AppendAllText("generated_queries.txt", query + Environment.NewLine);
    }

    [Fact]
    public void GenerateAggregateQuery_LatestByOffset()
    {
        Expression<Func<IGrouping<int, TestEntity>, object>> expr = g => new { Last = g.LatestByOffset(x => x.Id) };
        var generator = new DMLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateAggregateQuery("t1", expr.Body));
        Ksql.Linq.Tests.Utils.SqlAssert.EqualNormalized("SELECT LATEST_BY_OFFSET(Id) AS Last FROM t1;", query);
        File.AppendAllText("generated_queries.txt", query + Environment.NewLine);
    }

    [Fact]
    public void GenerateAggregateQuery_EarliestByOffset()
    {
        Expression<Func<IGrouping<int, TestEntity>, object>> expr = g => new { First = g.EarliestByOffset(x => x.Id) };
        var generator = new DMLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateAggregateQuery("t1", expr.Body));
        Ksql.Linq.Tests.Utils.SqlAssert.EqualNormalized("SELECT EARLIEST_BY_OFFSET(Id) AS First FROM t1;", query);
        File.AppendAllText("generated_queries.txt", query + Environment.NewLine);
    }

    [Fact]
    public void GenerateAggregateQuery_WindowStart()
    {
        Expression<Func<IGrouping<int, TestEntity>, object>> expr = g => new { Start = g.WindowStart() };
        var generator = new DMLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateAggregateQuery("t1", expr.Body));
        Ksql.Linq.Tests.Utils.SqlAssert.EqualNormalized("SELECT WINDOWSTART AS Start FROM t1;", query);
        File.AppendAllText("generated_queries.txt", query + Environment.NewLine);
    }

    private class DedupRate
    {
        [KsqlKey(order: 0)] public string Broker { get; set; } = string.Empty;
        [KsqlKey(order: 1)] public string Symbol { get; set; } = string.Empty;
    }

    [Fact]
    public void GenerateLinqQuery_GroupByKeys_UsesEntityPrefix()
    {
        IQueryable<DedupRate> src = new List<DedupRate>().AsQueryable();
        var expr = src
            .GroupBy(r => new { r.Broker, r.Symbol })
            .Select(g => new { g.Key.Broker, g.Key.Symbol, Count = g.Count() });

        var generator = new DMLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateLinqQuery("deduprate", expr.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "GROUP BY Broker, Symbol");
    }

    [Fact]
    public void GenerateLinqQuery_FullClauseCombination()
    {
        IQueryable<TestEntity> src = new List<TestEntity>().AsQueryable();
        var expr = src
            .Where(e => e.IsActive)
            .GroupBy(e => e.Type)
            .Having(g => g.Count() > 1)
            .Select(g => new { g.Key, Count = g.Count() })
            .OrderBy(x => x.Key);

        var generator = new DMLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateLinqQuery("s1", expr.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "SELECT Type");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "COUNT(*) AS Count");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "FROM s1");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "WHERE (IsActive = true)");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "GROUP BY Type");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "HAVING (COUNT(*) > 1)");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "ORDER BY");
        Ksql.Linq.Tests.Utils.SqlAssert.EndsWithSemicolon(query);
        Ksql.Linq.Tests.Utils.SqlAssert.AssertOrderNormalized(
            query,
            "from s1",
            "where",
            "group by",
            "having",
            "order by");
        File.AppendAllText("generated_queries.txt", query + Environment.NewLine);
    }

    private class Order
    {
        public int CustomerId { get; set; }
        public string Region { get; set; } = string.Empty;
        public double Amount { get; set; }
        public bool IsHighPriority { get; set; }
    }

    private class Customer
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class OrderWithCount
    {
        public int CustomerId { get; set; }
        public double Amount { get; set; }
        public int Count { get; set; }
    }

    private class WindowEntity
    {
        public int Id { get; set; }
        public DateTime Timestamp { get; set; }
    }

    [Fact]
    public void GenerateLinqQuery_GroupBySelectHaving_ComplexCondition()
    {
        IQueryable<Order> src = new List<Order>().AsQueryable();

        var expr = src
            .GroupBy(o => o.CustomerId)
            .Having(g => g.Count() > 10 && g.Sum(x => x.Amount) < 5000)
            .Select(g => new { g.Key, OrderCount = g.Count(), TotalAmount = g.Sum(x => x.Amount) });

        var generator = new DMLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateLinqQuery("Orders", expr.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "SELECT CustomerId");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "COUNT(*) AS OrderCount");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "SUM(Amount) AS TotalAmount");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "FROM Orders");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "GROUP BY CustomerId");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "HAVING ((COUNT(*) > 10) AND (SUM(Amount) < 5000))");
        Ksql.Linq.Tests.Utils.SqlAssert.EndsWithSemicolon(query);
        Ksql.Linq.Tests.Utils.SqlAssert.AssertOrderNormalized(
            query,
            "from orders",
            "group by",
            "having");
        File.AppendAllText("generated_queries.txt", query + Environment.NewLine);
    }

    [Fact]
    public void GenerateLINQQuery_JoinGroupByHavingCondition_ReturnsExpectedQuery()
    {
        IQueryable<Order> orders = new List<Order>().AsQueryable();
        IQueryable<Customer> customers = new List<Customer>().AsQueryable();

        var expr = orders
            .Join(customers, o => o.CustomerId, c => c.Id, (o, c) => new { o, c })
            .GroupBy(x => x.o.CustomerId)
            .Having(g => g.Count() > 2 && g.Sum(x => x.o.Amount) < 10000)
            .Select(g => new
            {
                g.Key,
                OrderCount = g.Count(),
                TotalAmount = g.Sum(x => x.o.Amount)
            });

        var generator = new DMLQueryGenerator();
        var query = ExecuteInScope(() => generator.GenerateLinqQuery("Orders", expr.Expression, false));
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "JOIN");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "GROUP BY CustomerId");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "HAVING ((COUNT(*) > 2) AND (SUM(Amount) < 10000))");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "COUNT(*) AS OrderCount");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(query, "SUM(o.Amount) AS TotalAmount");
        Ksql.Linq.Tests.Utils.SqlAssert.EndsWithSemicolon(query);
        Ksql.Linq.Tests.Utils.SqlAssert.AssertOrderNormalized(
            query,
            "join",
            "group by",
            "having");
        File.AppendAllText("generated_queries.txt", query + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_JoinGroupByHavingCondition_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();
        var customers = new List<Customer>().AsQueryable();

        var query =
            (from o in orders
             join c in customers on o.CustomerId equals c.Id
             group o by o.CustomerId into g
             select g)
            .Having(g => g.Count() > 2 && g.Sum(x => x.Amount) < 10000)
            .Select(g => new
            {
                g.Key,
                OrderCount = g.Count(),
                TotalAmount = g.Sum(x => x.Amount)
            });

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("joined", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "JOIN");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "GROUP BY");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "HAVING");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "COUNT(*)");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "SUM(");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "HAVING ((COUNT(*) > 2) AND (SUM(Amount) < 10000))");
        Ksql.Linq.Tests.Utils.SqlAssert.EndsWithSemicolon(result);
        Ksql.Linq.Tests.Utils.SqlAssert.AssertOrderNormalized(
            result,
            "join",
            "group by",
            "having");
        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_GroupByHavingWithMultipleAggregates_ReturnsExpectedQuery()
    {
        var src = new List<Order>().AsQueryable();

        var query = src
            .GroupBy(o => o.CustomerId)
            .Having(g => g.Average(x => x.Amount) > 100 && g.Sum(x => x.Amount) < 1000)
            .Select(g => new
            {
                g.Key,
                OrderCount = g.Count(),
                TotalAmount = g.Sum(x => x.Amount),
                AvgAmount = g.Average(x => x.Amount),
                TotalSmall = g.Sum(x => x.Amount)
            });

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("multiagg", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "GROUP BY");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "HAVING");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "AVG(");
        Assert.DoesNotContain("MAX(", result);
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "COUNT(*)");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "SUM(");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "HAVING ((AVG(Amount) > 100) AND (SUM(Amount) < 1000))");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "TotalSmall");
        Ksql.Linq.Tests.Utils.SqlAssert.EndsWithSemicolon(result);
        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_JoinGroupByHavingCombination_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();
        var customers = new List<Customer>().AsQueryable();

        var query = orders
            .Join(
                customers,
                o => o.CustomerId,
                c => c.Id,
                (o, c) => new { o, c }
            )
            .GroupBy(x => x.c.Name)
            .Having(g => g.Count() > 5 && g.Sum(x => x.o.Amount) > 1000)
            .Select(g => new
            {
                g.Key,
                OrderCount = g.Count(),
                TotalAmount = g.Sum(x => x.o.Amount)
            });

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("joined", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "JOIN");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "GROUP BY");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "HAVING");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "COUNT(");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "SUM(");
        Ksql.Linq.Tests.Utils.SqlAssert.EndsWithSemicolon(result);
        Ksql.Linq.Tests.Utils.SqlAssert.AssertOrderNormalized(
            result,
            "join",
            "group by",
            "having");
        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_MultiKeyGroupByWithHaving_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => new { o.CustomerId, o.Region })
            .Having(g => g.Sum(x => x.Amount) > 1000)
            .Select(g => new
            {
                g.Key.CustomerId,
                g.Key.Region,
                Total = g.Sum(x => x.Amount)
            });

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "GROUP BY");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "HAVING");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "CustomerId");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "Region");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "SUM");
        Ksql.Linq.Tests.Utils.SqlAssert.EndsWithSemicolon(result);
        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_GroupByWithConditionalSum_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => o.CustomerId)
            .Select(g => new
            {
                g.Key,
                Total = g.Sum(o => o.Amount),
                HighPriorityTotal = g.Sum(o => o.IsHighPriority ? o.Amount : 0)
            });

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "GROUP BY");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "CASE WHEN");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "SUM");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "HighPriorityTotal");        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_GroupByWithAvgSum_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => o.CustomerId)
            .Select(g => new
            {
                g.Key,
                AverageAmount = g.Average(o => o.Amount),
                TotalAmount = g.Sum(o => o.Amount)
            });

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "GROUP BY");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "AVG");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "SUM");        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_GroupByAnonymousKeyWithKeyProjection_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => new { o.CustomerId, o.Region })
            .Select(g => new
            {
                g.Key,
                Total = g.Sum(o => o.Amount)
            });

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "GROUP BY");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "CustomerId");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "Region");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "SUM");        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_GroupBySelectOrderBy_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => o.CustomerId)
            .Select(g => new
            {
                g.Key,
                Total = g.Sum(o => o.Amount)
            })
            .OrderBy(x => x.Total);

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "GROUP BY");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "SUM");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "ORDER BY");        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_GroupBySelectOrderByDescending_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => o.CustomerId)
            .Select(g => new
            {
                g.Key,
                Total = g.Sum(o => o.Amount)
            })
            .OrderByDescending(x => x.Total); // descending sort

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "GROUP BY");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "SUM");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "ORDER BY");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "DESC");        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_OrderByThenByDescending_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => new { o.CustomerId, o.Region })
            .Select(g => new
            {
                g.Key.CustomerId,
                g.Key.Region,
                Total = g.Sum(o => o.Amount)
            })
            .OrderBy(x => x.CustomerId)            // ascending
            .ThenByDescending(x => x.Total);       // descending

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "GROUP BY");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "ORDER BY");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "CustomerId");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "Total");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "DESC");        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_MultiKeyGroupByMultipleAggregates_HavingComplexConditions_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => new { o.CustomerId, o.Region })
            .Having(g => (g.Sum(x => x.Amount) > 1000 && g.Count() > 10) || g.Average(x => x.Amount) > 150)
            .Select(g => new
            {
                g.Key.CustomerId,
                g.Key.Region,
                TotalAmount = g.Sum(x => x.Amount),
                OrderCount = g.Count(),
                AverageAmount = g.Average(x => x.Amount)
            });

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "GROUP BY");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "HAVING");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "SUM");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "COUNT");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "AVG");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "AND");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "OR");
        Ksql.Linq.Tests.Utils.SqlAssert.EndsWithSemicolon(result);
        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_GroupByWithCaseWhen_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => o.CustomerId)
            .Select(g => new
            {
                g.Key,
                Total = g.Sum(o => o.Amount),
                Status = g.Sum(o => o.Amount) > 1000 ? "VIP" : "Regular"
            });

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "GROUP BY");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "SUM");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "CASE");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "WHEN");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "THEN");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "ELSE");        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_GroupByWithComplexHavingConditions_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => o.CustomerId)
            .Where(g =>
                (g.Sum(o => o.Amount) > 1000 && g.Count() > 5) ||
                g.Average(o => o.Amount) > 500)
            .Select(g => new
            {
                g.Key,
                Total = g.Sum(o => o.Amount),
                Count = g.Count(),
                Avg = g.Average(o => o.Amount)
            });

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "GROUP BY");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "HAVING");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "AND");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "OR");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "(");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, ")");        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_GroupByWithComplexOrHavingCondition_ReturnsExpectedQuery()
    {
        var orders = new List<OrderWithCount>().AsQueryable();

        var query = orders
            .GroupBy(o => o.CustomerId)
            .Where(g => g.Sum(x => x.Amount) > 1000 || g.Sum(x => x.Count) > 5)
            .Select(g => new
            {
                g.Key,
                TotalAmount = g.Sum(x => x.Amount),
                TotalCount = g.Sum(x => x.Count)
            });

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "GROUP BY CustomerId");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "HAVING");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "SUM");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, " OR ");        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_WhereNotInClause_ReturnsExpectedQuery()
    {
        var excludedRegions = new[] { "CN", "RU" };
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .Where(o => !excludedRegions.Contains(o.Region))
            .Select(o => new
            {
                o.CustomerId,
                o.Region,
                o.Amount
            });

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "WHERE");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "NOT IN");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "'CN'");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "'RU'");        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    private class NullableOrder
    {
        public int? CustomerId { get; set; }
        public string Region { get; set; } = string.Empty;
        public double Amount { get; set; }
    }

    private class NullableKeyOrder
    {
        public int? CustomerId { get; set; }
        public double Amount { get; set; }
    }

    [Fact]
    public void GenerateLinqQuery_WhereIsNullClause_ReturnsExpectedQuery()
    {
        var orders = new List<NullableOrder>().AsQueryable();

        var query = orders
            .Where(o => o.CustomerId == null)
            .Select(o => new
            {
                o.Region,
                o.Amount
            });

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "WHERE");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "IS NULL");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "CustomerId");        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_WhereIsNotNullClause_ReturnsExpectedQuery()
    {
        var orders = new List<NullableOrder>().AsQueryable();

        var query = orders
            .Where(o => o.CustomerId != null)
            .Select(o => new
            {
                o.Region,
                o.Amount
            });

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "WHERE");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "IS NOT NULL");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "CustomerId");        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_GroupByNullableKey_WithWhereNotNull_ProducesCorrectQuery()
    {
        var orders = new List<NullableKeyOrder>().AsQueryable();

        var query = orders
            .Where(o => o.CustomerId != null)
            .GroupBy(o => o.CustomerId)
            .Select(g => new
            {
                CustomerId = g.Key,
                Total = g.Sum(x => x.Amount)
            });

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "WHERE CustomerId IS NOT NULL");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "GROUP BY CustomerId");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "SUM");        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_GroupByWithExpressionKey_ReturnsExpectedQuery()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => o.Region.ToUpper())
            .Where(g => g.Sum(x => x.Amount) > 500)
            .Select(g => new
            {
                RegionUpper = g.Key,
                TotalAmount = g.Sum(x => x.Amount)
            });

        var generator = new DMLQueryGenerator();
        var result = ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, false));

        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "GROUP BY");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "UPPER");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "HAVING");
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(result, "SUM");        File.AppendAllText("generated_queries.txt", result + Environment.NewLine);
    }

    [Fact]
    public void GenerateLinqQuery_NestedAggregate_ThrowsNotSupportedException()
    {
        var orders = new List<Order>().AsQueryable();

        var query = orders
            .GroupBy(o => o.CustomerId)
            .Select(g => new
            {
                CustomerId = g.Key,
                AvgTotal = g.Average(x => g.Sum(y => y.Amount))
            });

        var generator = new DMLQueryGenerator();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, false)));

        Assert.Contains("Nested aggregate functions are not supported", ex.Message);
    }

    [Fact]
    public void GenerateSelectAll_OutsideScope_Throws()
    {
        var generator = new DMLQueryGenerator();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            generator.GenerateSelectAll("s1"));

        Assert.Contains("Where/GroupBy/Select", ex.Message);
    }

    [Fact]
    public void GenerateLinqQuery_GroupByPullQuery_Throws()
    {
        var src = new List<Order>().AsQueryable();
        var query = src
            .GroupBy(o => o.CustomerId)
            .Select(g => new { g.Key, Count = g.Count() });

        var generator = new DMLQueryGenerator();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, true)));

        Assert.Contains("GROUP BY is not supported in pull or table queries", ex.Message);
    }

    [Fact]
    public void GenerateLinqQuery_GroupByTableQuery_Throws()
    {
        var src = new List<Order>().AsQueryable();
        var query = src
            .GroupBy(o => o.CustomerId)
            .Select(g => new { g.Key, Count = g.Count() });

        var generator = new DMLQueryGenerator();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            ExecuteInScope(() => generator.GenerateLinqQuery("orders", query.Expression, true, true)));

        Assert.Contains("GROUP BY is not supported in pull or table queries", ex.Message);
    }

    [Fact]
    public void GenerateLinqQuery_MultipleWindowStart_Throws()
    {
        IQueryable<WindowEntity> src = new List<WindowEntity>().AsQueryable();

        var expr = src
            .Tumbling(e => e.Timestamp, new Windows { Minutes = new[] { 1 } })
            .GroupBy(e => e.Id)
            .Select(g => new { Start1 = g.WindowStart(), Start2 = g.WindowStart() });

        var generator = new DMLQueryGenerator();
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ExecuteInScope(() => generator.GenerateLinqQuery("win", expr.Expression, false)));
        Ksql.Linq.Tests.Utils.SqlAssert.ContainsNormalized(ex.Message, "Windowed query requires exactly one WindowStart() in projection.");
    }
}