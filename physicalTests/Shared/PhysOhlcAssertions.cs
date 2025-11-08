using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Ksql.Linq.Tests.Integration
{
    public static class PhysOhlcAssertions
    {
        // Reusable assertion: exact rows for a given bucket across multiple keys, with decimal OHLC equality
        public static async Task AssertBucketExactAsync(
            Func<string, TimeSpan, Task<List<object?[]>>> query,
            string table,
            long bucketStartMs,
            (string broker, string symbol, decimal Open, decimal High, decimal Low, decimal Close)[] expected,
            TimeSpan timeout)
        {
            var sql = $"SELECT BROKER, SYMBOL, BUCKETSTART, OPEN, HIGH, LOW, KSQLTIMEFRAMECLOSE FROM {table} WHERE BUCKETSTART={bucketStartMs};";
            var rows = await query(sql, timeout);
            Assert.Equal(expected.Length, rows.Count);

            foreach (var e in expected)
            {
                var r = rows.Single(x => (string)x[0]! == e.broker && (string)x[1]! == e.symbol);
                Assert.Equal(bucketStartMs, Convert.ToInt64(r[2]!));
                Assert.Equal(e.Open,  Convert.ToDecimal(r[3]!));
                Assert.Equal(e.High,  Convert.ToDecimal(r[4]!));
                Assert.Equal(e.Low,   Convert.ToDecimal(r[5]!));
                Assert.Equal(e.Close, Convert.ToDecimal(r[6]!));
            }
        }
    }
}
