using System;
using Ksql.Linq.Query.Builders;
using Ksql.Linq.Query.Builders.Statements;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Tests.Utils;
using Xunit;

namespace Ksql.Linq.Tests.Query.Builders;

public class StringFunctionTranslationTests
{
    private class Rec
    {
        public string Text { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
    }

    private class RecOut
    {
        public int Len { get; set; }
    }

    [Fact]
    public void Builder_Translates_String_Length_To_LEN()
    {
        var model = new KsqlQueryRoot()
            .From<Rec>()
            .Select(r => new RecOut { Len = r.Text.Length })
            .Build();

        var sql = KsqlCreateStatementBuilder.Build("len_stream", model);
        Console.WriteLine("SQL=>\n" + sql);

        SqlAssert.ContainsNormalized(sql, "LEN(");
        SqlAssert.ContainsNormalized(sql, "CREATE STREAM IF NOT EXISTS len_stream");
        SqlAssert.EndsWithSemicolon(sql);
    }
}
