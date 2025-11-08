using System;
using System.Linq.Expressions;
using Ksql.Linq.Query.Dsl;

namespace Ksql.Linq.Query.Dsl;

internal static class PrimarySourceAliasDecider
{
    public static bool Determine(KsqlQueryModel model)
    {
        if (model == null)
            throw new ArgumentNullException(nameof(model));

        // Joins always require aliases on the primary source.
        if (model.SourceTypes.Length > 1)
            return true;

        // Callers can force behaviour via Extras flag.
        if (model.Extras.TryGetValue("select/requireAlias", out var flag) && flag is bool forced)
            return forced;

        // Hub projections (metadata propagated) expect "o." for the primary source.
        if (model.SelectProjectionMetadata?.IsHubInput == true)
            return true;

        var projection = model.SelectProjection;
        if (projection == null)
            return false;

        return RequiresAlias(projection.Body);
    }

    private static bool RequiresAlias(Expression expression)
    {
        var requiresAlias = false;
        void Walk(Expression? node)
        {
            if (requiresAlias || node == null)
                return;

            switch (node)
            {
                case MemberExpression member:
                    if (member.Expression is ParameterExpression)
                    {
                        // Direct member access is fine; no alias needed.
                        break;
                    }

                    // Nested access (e.g., Key.Property) can live without alias, but when
                    // the chain starts from a capturing parameter (anonymous type) we prefer
                    // aliasing to avoid ambiguity in generated SQL.
                    if (member.Expression is MemberExpression inner && inner.Expression is ParameterExpression)
                    {
                        requiresAlias = true;
                        return;
                    }

                    Walk(member.Expression);
                    break;

                case MethodCallExpression call:
                    foreach (var arg in call.Arguments)
                        Walk(arg);
                    Walk(call.Object);
                    break;

                case UnaryExpression unary:
                    Walk(unary.Operand);
                    break;

                case BinaryExpression binary:
                    Walk(binary.Left);
                    Walk(binary.Right);
                    break;

                case ConditionalExpression conditional:
                    Walk(conditional.Test);
                    Walk(conditional.IfTrue);
                    Walk(conditional.IfFalse);
                    break;

                case NewExpression @new:
                    foreach (var arg in @new.Arguments)
                        Walk(arg);
                    break;

                case MemberInitExpression init:
                    Walk(init.NewExpression);
                    foreach (var binding in init.Bindings)
                        if (binding is MemberAssignment assignment)
                            Walk(assignment.Expression);
                    break;
            }
        }

        Walk(expression);
        return requiresAlias;
    }
}
