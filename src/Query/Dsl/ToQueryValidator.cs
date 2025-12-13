using Ksql.Linq.Configuration;
using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Ksql.Linq.Query.Dsl;

internal static class ToQueryValidator
{
    public static void ValidateSelectMatchesPoco(Type resultType, KsqlQueryModel model)
    {
        if (resultType == null) throw new ArgumentNullException(nameof(resultType));
        if (model == null) throw new ArgumentNullException(nameof(model));

        ValidateHoppingPipeline(resultType, model);

        var isWindowed = typeof(IWindowedRecord).IsAssignableFrom(resultType);

        bool ShouldInclude(PropertyInfo p)
        {
            if (Attribute.IsDefined(p, typeof(KsqlIgnoreAttribute), true))
                return false;
            if (isWindowed && (string.Equals(p.Name, nameof(IWindowedRecord.WindowStart), StringComparison.OrdinalIgnoreCase)
                || string.Equals(p.Name, nameof(IWindowedRecord.WindowEnd), StringComparison.OrdinalIgnoreCase)))
                return false;
            return true;
        }

        var entityProps = resultType
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .OrderBy(p => p.MetadataToken)
            .Where(ShouldInclude)
            .ToArray();

        var entityPropMap = entityProps.ToDictionary(p => p.Name);

        var projectionProps = ExtractProjectionProperties(model.SelectProjection, resultType)
            .Where(p => entityPropMap.ContainsKey(p.Prop.Name))
            .ToArray();

        if (entityProps.Length != projectionProps.Length)
            throw new InvalidOperationException("Select projection does not match POCO properties.");

        for (int i = 0; i < entityProps.Length; i++)
        {
            var proj = projectionProps[i];
            if (entityProps[i].Name != proj.Prop.Name)
                throw new InvalidOperationException("Select projection does not match POCO property order.");
            if (entityProps[i].PropertyType != proj.Type)
                throw new InvalidOperationException("Select projection property types do not match POCO.");
            if ((entityProps[i].PropertyType == typeof(decimal) || entityProps[i].PropertyType == typeof(decimal?))
                && (proj.Type == typeof(decimal) || proj.Type == typeof(decimal?)) && proj.Source != null)
            {
                var ea = entityProps[i].GetCustomAttribute<KsqlDecimalAttribute>(true);
                var sa = proj.Source.GetCustomAttribute<KsqlDecimalAttribute>(true);
                var ep = DecimalPrecisionConfig.ResolvePrecision(ea?.Precision, entityProps[i]);
                var es = DecimalPrecisionConfig.ResolveScale(ea?.Scale, entityProps[i]);
                var sp = DecimalPrecisionConfig.ResolvePrecision(sa?.Precision, proj.Source);
                var ss = DecimalPrecisionConfig.ResolveScale(sa?.Scale, proj.Source);
                if (ep != sp || es != ss)
                    throw new InvalidOperationException("Select projection decimal precision does not match POCO.");
            }
        }

        var entityKeys = entityProps
            .Select(p => (Prop: p, Attr: p.GetCustomAttribute<KsqlKeyAttribute>(true)))
            .Where(x => x.Attr != null)
            .OrderBy(x => x.Attr!.Order)
            .Select(x => x.Prop.Name)
            .ToArray();

        var projectionKeys = projectionProps
            .Select(p => (Name: p.Prop.Name, Attr: entityPropMap.TryGetValue(p.Prop.Name, out var ep)
                ? ep.GetCustomAttribute<KsqlKeyAttribute>(true)
                : null))
            .Where(x => x.Attr != null)
            .OrderBy(x => x.Attr!.Order)
            .Select(x => x.Name)
            .ToArray();

        if (!entityKeys.SequenceEqual(projectionKeys))
            throw new InvalidOperationException("Select projection key order does not match POCO.");
    }

    private static List<(PropertyInfo Prop, PropertyInfo? Source, Type Type)> ExtractProjectionProperties(LambdaExpression? projection, Type resultType)
    {
        if (projection == null)
            return resultType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .OrderBy(p => p.MetadataToken)
                .Select(p => (Prop: p, Source: (PropertyInfo?)p, Type: p.PropertyType))
                .ToList();

        var props = new List<(PropertyInfo, PropertyInfo?, Type)>();
        switch (projection.Body)
        {
            case NewExpression newExpr when newExpr.Members != null:
                for (int i = 0; i < newExpr.Members.Count; i++)
                {
                    if (newExpr.Members[i] is PropertyInfo mem && resultType.GetProperty(mem.Name) != null)
                    {
                        PropertyInfo? src = (newExpr.Arguments[i] as MemberExpression)?.Member as PropertyInfo;
                        props.Add((mem, src, newExpr.Arguments[i].Type));
                    }
                }
                break;
            case MemberInitExpression initExpr:
                foreach (var binding in initExpr.Bindings.OfType<MemberAssignment>())
                {
                    if (binding.Member is PropertyInfo mem && resultType.GetProperty(mem.Name) != null)
                    {
                        PropertyInfo? src = (binding.Expression as MemberExpression)?.Member as PropertyInfo;
                        props.Add((mem, src, binding.Expression.Type));
                    }
                }
                break;
            case ParameterExpression:
                props.AddRange(resultType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .OrderBy(p => p.MetadataToken)
                    .Select(p => (p, (PropertyInfo?)p, p.PropertyType)));
                break;
            case MemberExpression me when me.Member is PropertyInfo pi:
                if (resultType.GetProperty(pi.Name) != null)
                    props.Add((pi, me.Member as PropertyInfo, me.Type));
                break;
        }
        return props;
    }

    public static void ValidateHoppingPipeline(Type resultType, KsqlQueryModel model)
    {
        if (resultType == null) throw new ArgumentNullException(nameof(resultType));
        if (model == null) throw new ArgumentNullException(nameof(model));
        if (!model.HasHopping())
            return;

        var ops = model.OperationSequence ?? new List<string>();
        bool HasOp(string op, [NotNullWhen(true)] out int index)
        {
            index = ops.IndexOf(op);
            return index >= 0;
        }

        var hasJoin = model.JoinCondition != null || model.SourceTypes.Length > 1;
        if (hasJoin)
        {
            if (!HasOp("Join", out var joinIdx))
                throw new InvalidOperationException("Hopping with join requires Join() before defining the window.");
            if (!HasOp("Hopping", out var hopIdx))
                throw new InvalidOperationException("Hopping window is missing for the join query.");
            if (!HasOp("GroupBy", out var groupIdx) || !HasOp("Select", out var selectIdx))
                throw new InvalidOperationException("Hopping with join requires GroupBy() followed by Select().");

            if (!(joinIdx < hopIdx && hopIdx < groupIdx && groupIdx < selectIdx))
                throw new InvalidOperationException("Allowed order: From -> Join -> Hopping -> GroupBy -> Select.");

            if (ops.Count(op => string.Equals(op, "Hopping", StringComparison.OrdinalIgnoreCase)) > 1)
                throw new InvalidOperationException("Multiple hopping windows are not supported.");

            if (ops.Skip(hopIdx + 1).Any(op => string.Equals(op, "Join", StringComparison.OrdinalIgnoreCase)))
                throw new InvalidOperationException("Join after hopping is not supported.");

            if (model.HasTumbling())
                throw new NotSupportedException("Window-to-window join is not supported.");

            if (model.SourceTypes.Length != 2)
                throw new NotSupportedException("Only stream-to-table join is supported for hopping.");

            var leftIsTable = IsTable(model.SourceTypes[0]);
            var rightIsTable = IsTable(model.SourceTypes[1]);
            if (leftIsTable || !rightIsTable)
                throw new NotSupportedException("Only Stream -> Table join is supported for hopping.");
        }
        else
        {
            // Non-join hopping: still require GroupBy -> Select ordering if provided.
            if (HasOp("Hopping", out var hopIdx) && HasOp("GroupBy", out var groupIdx) && HasOp("Select", out var selectIdx))
            {
                if (!(hopIdx < groupIdx && groupIdx < selectIdx))
                    throw new InvalidOperationException("Hopping requires GroupBy then Select in order.");
            }
        }
    }

    private static bool IsTable(Type type) => Attribute.IsDefined(type, typeof(KsqlTableAttribute), inherit: true);
}
