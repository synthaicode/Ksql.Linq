using System.Linq.Expressions;

namespace Ksql.Linq.Query.Builders.Common;

internal static class ExpressionUtils
{
    public static bool IsConvert(UnaryExpression unary)
        => unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked;

    public static Expression UnwrapConvert(Expression expr)
    {
        while (expr is UnaryExpression u && IsConvert(u))
            expr = u.Operand;
        return expr;
    }

    public static string ExtractConstantValue(Expression expression, string nullFallback = "NULL", string defaultFallback = "0")
    {
        var e = UnwrapConvert(expression);
        return e switch
        {
            ConstantExpression c => c.Value?.ToString() ?? nullFallback,
            _ => defaultFallback
        };
    }
}

