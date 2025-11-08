using Ksql.Linq.Core.Attributes;
using Ksql.Linq.Query.Dsl;
using Ksql.Linq.Query.Abstractions;
using Ksql.Linq.Query.Builders.Clauses;
using Ksql.Linq.Query.Builders.Common;
using Ksql.Linq.Query.Builders.Utilities;
using Ksql.Linq.Query.Hub.Analysis;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Text.RegularExpressions;
using System.Reflection;

namespace Ksql.Linq.Query.Builders.Statements;

internal static class KsqlCreateStatementBuilder
{
    public static string Build(string streamName, KsqlQueryModel model, string? keySchemaFullName = null, string? valueSchemaFullName = null, string? partitionBy = null, RenderOptions? options = null)
    {
        return Build(streamName, model, keySchemaFullName, valueSchemaFullName, ResolveSourceName, partitionBy, options);
    }

    /// <summary>
    /// Build a CREATE statement with an optional source name resolver for FROM/JOIN tables.
    /// </summary>
    public static string Build(string streamName, KsqlQueryModel model, string? keySchemaFullName, string? valueSchemaFullName, Func<Type, string> sourceNameResolver, string? partitionBy = null, RenderOptions? options = null)
    {
        if (string.IsNullOrWhiteSpace(streamName))
            throw new ArgumentException("Stream name is required", nameof(streamName));
        if (model == null)
            throw new ArgumentNullException(nameof(model));

        var groupByClause = BuildGroupByClause(model.GroupByExpression, model.SourceTypes, model.PrimarySourceRequiresAlias);
        string? partitionClause = null;

        string selectClause;
        var forcePreserveSelectAlias = false;
        if (model.SelectProjection == null)
        {
            selectClause = "*";
        }
        else
        {
            var map = new System.Collections.Generic.Dictionary<string, string>(StringComparer.Ordinal);
            var parameters = model.SelectProjection.Parameters;
            var sourceCount = model.SourceTypes?.Length ?? 0;
            var aliasPrimarySource = model.PrimarySourceRequiresAlias;
            for (int i = 0; i < parameters.Count && i < sourceCount; i++)
            {
                var pname = parameters[i].Name ?? string.Empty;
                string? alias = null;
                if (i == 0 && aliasPrimarySource)
                    alias = "o";
                else if (i > 0)
                    alias = i == 1 ? "i" : $"s{i}";

                if (!string.IsNullOrEmpty(alias))
                    map[pname] = alias;
            }
            // Default builder; may be overridden by ResultType or hub overrides
            SelectClauseBuilder builder = new SelectClauseBuilder(map);
            if (options?.ResultType != null)
            {
                // Build alias type hints from the result type (target DTO)
                var hints = new System.Collections.Generic.Dictionary<string, (System.Type Type, int? Precision, int? Scale)>(StringComparer.OrdinalIgnoreCase);
                var props = options.ResultType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
                foreach (var p in props)
                {
                    var decAttr = p.GetCustomAttribute<KsqlDecimalAttribute>(true);
                    hints[p.Name] = (p.PropertyType, decAttr?.Precision, decAttr?.Scale);
                }
                builder = new SelectClauseBuilder(map, hints);
            }
            else
            {
                // Prefer hub-input overrides when either:
                //  - ProjectionMetadata explicitly flags IsHubInput, or
                //  - Overrides were attached via Extras (fallback when metadata propagation is missing)
                var meta = model.SelectProjectionMetadata;
                var hasMetaHub = meta?.IsHubInput == true;
                System.Collections.Generic.Dictionary<string, HubProjectionOverride>? overrides = null;
                System.Collections.Generic.HashSet<string>? exclude = null;
                if (model.Extras.TryGetValue("select/overrides", out var ov) && ov is System.Collections.Generic.Dictionary<string, HubProjectionOverride> d)
                    overrides = d;
                if (model.Extras.TryGetValue("select/exclude", out var ex) && ex is System.Collections.Generic.HashSet<string> h)
                    exclude = h;

                if (hasMetaHub && (overrides == null || exclude == null))
                {
                    System.Collections.Generic.ISet<string>? availableColumns = null;
                    if (model.Extras.TryGetValue("hub/availableColumns", out var colsObj) && colsObj is System.Collections.Generic.ISet<string> set)
                        availableColumns = set;
                    HubSelectPolicy.BuildOverridesAndExcludes(
                        meta!,
                        out var derivedOverrides,
                        out var derivedExclude,
                        availableColumns);
                    if (overrides == null)
                    {
                        overrides = derivedOverrides;
                        model.Extras["select/overrides"] = overrides;
                    }
                    else
                    {
                        foreach (var kv in derivedOverrides)
                            overrides[kv.Key] = kv.Value;
                    }
                    if (exclude == null)
                    {
                        exclude = derivedExclude;
                        model.Extras["select/exclude"] = exclude;
                    }
                    else
                    {
                        foreach (var alias in derivedExclude)
                            exclude.Add(alias);
                    }
                }

                if (hasMetaHub || overrides != null || exclude != null)
                {
                    var hasPrimaryAlias = map.Values.Contains("o");
                    var srcAlias = hasPrimaryAlias ? "o" : string.Empty;
                    if (overrides != null && exclude != null && exclude.Count > 0)
                    {
                        builder = new SelectClauseBuilder(map, overrides, srcAlias, exclude);
                        if (hasPrimaryAlias)
                            forcePreserveSelectAlias = true;
                    }
                    else if (overrides != null)
                    {
                        builder = new SelectClauseBuilder(map, overrides, srcAlias);
                        if (hasPrimaryAlias)
                            forcePreserveSelectAlias = true;
                    }
                    else if (exclude != null && exclude.Count > 0)
                        builder = new SelectClauseBuilder(map, exclude);
                }
            }
            selectClause = builder.Build(model.SelectProjection.Body);
        }

        var fromClause = BuildFromClauseCore(model, sourceNameResolver, out var aliasToSource);
        var whereClause = BuildWhereClause(model.WhereCondition, model);
        var havingClause = BuildHavingClause(model.HavingCondition);

        var hasGroupBy = model.HasGroupBy();
        var hasWindow = model.HasTumbling();
        var hasEmitFinal = HasEmitFinal(model);
        var sourceTypes = model.SourceTypes ?? Array.Empty<Type>();
        var sourceIsStream = sourceTypes.Length > 0 && Array.TrueForAll(sourceTypes, t => !IsTableType(t));
        var partitionMergedIntoGroupBy = false;

        if (!string.IsNullOrWhiteSpace(partitionBy))
        {
            var normalizedPartition = NormalizePartitionClause(partitionBy);
            var partitionColumns = ExtractPartitionColumnKeys(normalizedPartition);
            if (partitionColumns.Count > 0)
            {
                var primaryType = sourceTypes.Length > 0 ? sourceTypes[0] : null;
                var primaryKeys = primaryType != null
                    ? ExtractKeyNames(primaryType)
                    : new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var keysKnown = primaryKeys.Count > 0;
                var partitionMatchesKey = keysKnown
                    && partitionColumns.Count == primaryKeys.Count
                    && partitionColumns.All(primaryKeys.Contains);

                var singleSourceStream = sourceIsStream && sourceTypes.Length == 1;
                if (singleSourceStream
                    && !hasGroupBy
                    && !hasWindow
                    && !hasEmitFinal
                    && (!partitionMatchesKey || !keysKnown))
                {
                    partitionClause = normalizedPartition;
                }
            }
        }

        var aliasMetadata = BuildAliasMetadata(aliasToSource, sourceTypes);

        var keyMap = BuildKeyAliasMap(model, options?.KeyPathStyle ?? KeyPathStyle.None);
        var mapForFrom = keyMap.Where(kv => kv.Value.Style != KeyPathStyle.None)
            .ToDictionary(kv => kv.Key, kv => kv.Value);
        fromClause = ApplyKeyStyle(fromClause, mapForFrom);
        selectClause = ApplyKeyStyle(selectClause, keyMap);
        groupByClause = ApplyKeyStyle(groupByClause, keyMap);
        if (!string.IsNullOrWhiteSpace(partitionClause))
            partitionClause = ApplyKeyStyle(partitionClause, keyMap);
        whereClause = ApplyKeyStyle(whereClause, keyMap);
        havingClause = ApplyKeyStyle(havingClause, keyMap);

        var preserveSelectAlias = model.PrimarySourceRequiresAlias || forcePreserveSelectAlias;
        DealiasClauses(aliasMetadata, preserveSelectAlias, ref selectClause, ref groupByClause, ref partitionClause, ref whereClause, ref havingClause);

        if (!string.IsNullOrWhiteSpace(partitionClause))
        {
            partitionClause = DeduplicatePartitionColumns(partitionClause);
        }

        if (!string.IsNullOrWhiteSpace(partitionClause))
        {
            groupByClause = MergeGroupByAndPartition(groupByClause, partitionClause!, out partitionMergedIntoGroupBy);
            partitionClause = null;
        }

        var isTable = model.DetermineType() == StreamTableType.Table || partitionMergedIntoGroupBy;
        var createType = isTable ? "CREATE TABLE IF NOT EXISTS" : "CREATE STREAM IF NOT EXISTS";

        var sb = new StringBuilder();
        sb.Append($"{createType} {streamName} ");
        var hasKey = AnySourceHasKeys(model);
        if (string.IsNullOrWhiteSpace(valueSchemaFullName)
            && model.Extras != null
            && model.Extras.TryGetValue("valueSchemaFullName", out var __vsf)
            && __vsf is string __vsfStr
            && !string.IsNullOrWhiteSpace(__vsfStr))
        {
            valueSchemaFullName = __vsfStr;
        }
        int? partitionsValue = null;
        short? replicasValue = null;
        long? retentionCandidate = null;
        if (model.Extras != null)
        {
            if (model.Extras.TryGetValue("sink/partitions", out var __sp) && __sp is int __spi && __spi > 0)
                partitionsValue = __spi;
            if (model.Extras.TryGetValue("sink/replicas", out var __sr) && __sr is int __sri && __sri > 0)
                replicasValue = Convert.ToInt16(__sri);
        }
        var streamTableType = model.DetermineType();
        var allowRetentionMs = streamTableType != StreamTableType.Table || model.HasTumbling();
        if (model.Extras != null &&
            model.Extras.TryGetValue("sink/retentionMs", out var retentionObj) &&
            WithClauseBuilder.TryConvertRetention(retentionObj, out var convertedRetention))
        {
            retentionCandidate = convertedRetention;
        }

        var withClause = WithClauseBuilder.BuildClause(
            kafkaTopic: streamName,
            hasKey: hasKey,
            valueSchemaFullName: valueSchemaFullName,
            timestampColumn: null,
            partitions: partitionsValue,
            replicas: replicasValue,
            retentionMs: retentionCandidate,
            allowRetentionMs: allowRetentionMs,
            model: model);
        sb.Append(' ').Append(withClause);
        sb.AppendLine(" AS");
        sb.AppendLine($"SELECT {selectClause}");
        sb.Append(fromClause);
        if (!string.IsNullOrEmpty(whereClause))
        {
            sb.AppendLine();
            sb.Append(whereClause);
        }
        if (!string.IsNullOrEmpty(groupByClause))
        {
            sb.AppendLine();
            sb.Append(groupByClause);
        }
        if (!string.IsNullOrEmpty(havingClause))
        {
            sb.AppendLine();
            sb.Append(havingClause);
        }
        sb.AppendLine();
        sb.Append("EMIT CHANGES;");
        return sb.ToString();
    }

    private static bool AnySourceHasKeys(KsqlQueryModel model)
    {
        var types = model.SourceTypes ?? Array.Empty<Type>();
        foreach (var t in types)
        {
            var keys = ExtractKeyNames(t);
            if (keys != null && keys.Count > 0) return true;
        }
        return false;
    }

    private static string BuildFromClauseCore(KsqlQueryModel model, Func<Type, string>? sourceNameResolver, out Dictionary<string, string> aliasToSource)
    {
        var types = model.SourceTypes;
        if (types == null || types.Length == 0)
            throw new InvalidOperationException("Source types are required");

        if (types.Length > 2)
            throw new NotSupportedException("Only up to 2 tables are supported in JOIN");

        var result = new StringBuilder();
        aliasToSource = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var left = sourceNameResolver?.Invoke(types[0]) ?? ResolveSourceName(types[0]);
        var usePrimaryAlias = model.PrimarySourceRequiresAlias;
        string? leftAlias = null;
        if (usePrimaryAlias)
        {
            leftAlias = "o";
            aliasToSource[leftAlias] = left;
            result.Append($"FROM {left} {leftAlias}");
        }
        else
        {
            aliasToSource["o"] = left;
            result.Append($"FROM {left}");
        }

        if (types.Length > 1)
        {
            var right = sourceNameResolver?.Invoke(types[1]) ?? ResolveSourceName(types[1]);
            var rAlias = "i"; // explicit alias for right source
            aliasToSource[rAlias] = right;
            result.Append($" JOIN {right} {rAlias}");
            if (model.JoinCondition == null)
                throw new InvalidOperationException("Join condition required for two table join");

            // Enforce WITHIN for stream-stream joins: allow default 300s unless forbidden
            int withinSeconds;
            if (model.WithinSeconds.HasValue && model.WithinSeconds.Value > 0)
            {
                withinSeconds = model.WithinSeconds.Value;
            }
            else if (!model.ForbidDefaultWithin)
            {
                withinSeconds = 300; // default
            }
            else
            {
                throw new InvalidOperationException("Stream-Stream JOIN requires explicit Within(...) when default is disabled.");
            }
            result.Append($" WITHIN {withinSeconds} SECONDS");

            // Build a qualified join condition using aliases to avoid ambiguity
            var leftAliasForJoin = leftAlias ?? throw new InvalidOperationException("Primary source alias missing for join condition.");
            var condition = BuildQualifiedJoinCondition(model.JoinCondition, leftAliasForJoin, rAlias);
            result.Append($" ON {condition}");
        }

        return result.ToString();
    }

    private static string BuildQualifiedJoinCondition(LambdaExpression joinExpr, string leftAlias, string rightAlias)
    {
        string Build(Expression expr)
        {
            switch (expr)
            {
                case BinaryExpression be when be.NodeType == ExpressionType.Equal:
                    return $"({Build(be.Left)} = {Build(be.Right)})";
                case MemberExpression me:
                    {
                        var param = GetRootParameter(me);
                        if (param != null)
                        {
                            if (joinExpr.Parameters.Count > 0 && param == joinExpr.Parameters[0])
                                return $"{leftAlias}.{me.Member.Name}";
                            if (joinExpr.Parameters.Count > 1 && param == joinExpr.Parameters[1])
                                return $"{rightAlias}.{me.Member.Name}";
                        }
                        throw new InvalidOperationException("Unqualified column access in JOIN condition is not allowed.");
                    }
                case UnaryExpression ue:
                    return Build(ue.Operand);
                case ConstantExpression ce:
                    return Builders.Common.BuilderValidation.SafeToString(ce.Value);
                default:
                    return expr.ToString();
            }
        }

        static ParameterExpression? GetRootParameter(MemberExpression me)
        {
            Expression? e = me.Expression;
            while (e is MemberExpression m)
                e = m.Expression;
            return e as ParameterExpression;
        }

        return Build(joinExpr.Body);
    }

    private static string ResolveSourceName(Type type)
    {
        // If the entity type has [KsqlTopic("name")], use that (uppercased for KSQL identifiers)
        var attr = type.GetCustomAttributes(true).OfType<Ksql.Linq.Core.Attributes.KsqlTopicAttribute>().FirstOrDefault();
        if (attr != null && !string.IsNullOrWhiteSpace(attr.Name))
            return attr.Name.ToUpperInvariant();
        return type.Name;
    }

    private static string BuildWhereClause(LambdaExpression? where, KsqlQueryModel model)
    {
        if (where == null) return string.Empty;
        // Build parameter-to-alias map: first param -> o, second -> i
        System.Collections.Generic.IDictionary<string, string>? map = null;
        if (where.Parameters != null && where.Parameters.Count > 0)
        {
            var temp = new System.Collections.Generic.Dictionary<string, string>(StringComparer.Ordinal);
            if (where.Parameters.Count > 0 && model.PrimarySourceRequiresAlias)
                temp[where.Parameters[0].Name ?? string.Empty] = "o";
            if (where.Parameters.Count > 1)
                temp[where.Parameters[1].Name ?? string.Empty] = "i";
            if (temp.Count > 0)
                map = temp;
        }
        var builder = map == null ? new WhereClauseBuilder() : new WhereClauseBuilder(map);
        var condition = builder.Build(where.Body);
        return $"WHERE {condition}";
    }

    private static string BuildGroupByClause(LambdaExpression? groupBy, Type[]? sourceTypes, bool aliasPrimary)
    {
        if (groupBy == null) return string.Empty;

        var map = new System.Collections.Generic.Dictionary<string, string>(StringComparer.Ordinal);
        var parameters = groupBy.Parameters;
        for (int i = 0; i < parameters.Count && i < (sourceTypes?.Length ?? 0); i++)
        {
            var pname = parameters[i].Name ?? string.Empty;
            string? alias = null;
            if (i == 0 && aliasPrimary)
                alias = "o";
            else if (i > 0)
                alias = i == 1 ? "i" : $"s{i}";

            if (!string.IsNullOrEmpty(alias))
                map[pname] = alias;
        }
        var builder = map.Count == 0 ? new GroupByClauseBuilder() : new GroupByClauseBuilder(map);
        var keys = builder.Build(groupBy.Body);
        return $"GROUP BY {keys}";
    }

    private static string BuildHavingClause(LambdaExpression? having)
    {
        if (having == null) return string.Empty;
        var builder = new HavingClauseBuilder();
        var condition = builder.Build(having.Body);
        return $"HAVING {condition}";
    }

    private static System.Collections.Generic.Dictionary<string, (System.Collections.Generic.HashSet<string> Keys, KeyPathStyle Style)> BuildKeyAliasMap(KsqlQueryModel model, KeyPathStyle overrideStyle)
    {
        var map = new System.Collections.Generic.Dictionary<string, (System.Collections.Generic.HashSet<string>, KeyPathStyle)>(StringComparer.OrdinalIgnoreCase);
        var types = model.SourceTypes ?? Array.Empty<Type>();
        if (types.Length > 0)
        {
            var alias = model.PrimarySourceRequiresAlias ? "o" : string.Empty;
            map[alias] = (ExtractKeyNames(types[0]), DetermineStyle(types[0], overrideStyle));
        }
        if (types.Length > 1)
            map["i"] = (ExtractKeyNames(types[1]), DetermineStyle(types[1], overrideStyle));
        // TODO: future model-provided aliases should feed this map instead of fixed o/i.
        return map;
    }

    private static KeyPathStyle DetermineStyle(Type type, KeyPathStyle overrideStyle)
    {
        if (overrideStyle != KeyPathStyle.None)
            return overrideStyle;
        // Auto-detection only yields Arrow for tables; Dot is reserved for explicit overrides.
        return type.GetCustomAttributes(true).OfType<KsqlTableAttribute>().Any()
            ? KeyPathStyle.Arrow
            : KeyPathStyle.None;
    }

    private static System.Collections.Generic.HashSet<string> ExtractKeyNames(Type type)
    {
        return type
            .GetProperties()
            .Where(p => p.GetCustomAttributes(true).OfType<KsqlKeyAttribute>().Any())
            .Select(p => p.Name.ToUpperInvariant())
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    private static string ApplyKeyStyle(string clause, System.Collections.Generic.Dictionary<string, (System.Collections.Generic.HashSet<string> Keys, KeyPathStyle Style)> map)
    {
        if (string.IsNullOrEmpty(clause)) return clause;
        foreach (var kv in map)
        {
            var alias = kv.Key;
            var keys = kv.Value.Keys;
            var style = kv.Value.Style;
            if (style == KeyPathStyle.None)
                continue; // keep original alias-qualified key paths for streams

            if (keys.Count == 1)
            {
                var lone = keys.First();
                var standAlonePattern = @"\bKEY\b";
                var standAloneReplacement = style switch
                {
                    KeyPathStyle.Dot => $"key.{lone}",
                    KeyPathStyle.Arrow => $"KEY->{lone}",
                    _ => lone
                };
                clause = System.Text.RegularExpressions.Regex.Replace(
                    clause,
                    standAlonePattern,
                    standAloneReplacement,
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.CultureInvariant);
            }

            foreach (var key in keys)
            {
                var replacement = style switch
                {
                    KeyPathStyle.Dot => $"key.{key}",
                    KeyPathStyle.Arrow => $"KEY->{key}",
                    _ => key
                };

                if (string.IsNullOrEmpty(alias))
                {
                    clause = System.Text.RegularExpressions.Regex.Replace(
                        clause,
                        $@"(?<!KEY->)(?<!key\.)(?<![`'""])\b{key}\b(?![`'""])",
                        match =>
                        {
                            return ShouldReplace(match.Index) ? replacement : match.Value;
                        },
                        System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.CultureInvariant);

                    bool ShouldReplace(int index)
                    {
                        var i = index - 1;
                        while (i >= 0 && char.IsWhiteSpace(clause[i])) i--;
                        if (i < 1) return true;
                        var end = i;
                        while (i >= 0 && char.IsLetter(clause[i])) i--;
                        var token = clause[(i + 1)..(end + 1)];
                        return !token.Equals("AS", StringComparison.OrdinalIgnoreCase);
                    }
                }
                else
                {
                    // Skip tokens already prefixed (KEY-> or key.) and any inside quotes/backticks.
                    var pattern = $@"(?<!KEY->)(?<!key\.)(?<![`'""])\b{alias}\.{key}\b(?![`'""])";
                    clause = System.Text.RegularExpressions.Regex.Replace(
                        clause,
                        pattern,
                        replacement,
                        System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.CultureInvariant);
                }
            }
        }
        return clause;
    }

    private static bool HasEmitFinal(KsqlQueryModel model)
    {
        if (model.Extras != null && model.Extras.TryGetValue("emit", out var emitValue))
        {
            if (emitValue is string emitString && emitString.IndexOf("FINAL", StringComparison.OrdinalIgnoreCase) >= 0)
                return true;
        }
        return false;
    }

    private static bool IsTableType(Type type)
    {
        return type.GetCustomAttributes(typeof(KsqlTableAttribute), inherit: true).Length > 0;
    }

    private static string NormalizePartitionClause(string partitionClause)
    {
        if (string.IsNullOrWhiteSpace(partitionClause))
            return partitionClause;

        var parts = partitionClause.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return string.Join(", ", parts);
    }

    private static List<string> ExtractPartitionColumnKeys(string partitionClause)
    {
        var keys = new List<string>();
        if (string.IsNullOrWhiteSpace(partitionClause))
            return keys;

        var parts = partitionClause.Split(',', StringSplitOptions.RemoveEmptyEntries);
        foreach (var part in parts)
        {
            var trimmed = part.Trim();
            if (trimmed.Length == 0)
                continue;

            var unqualified = ExtractUnqualifiedIdentifier(trimmed);
            var normalized = NormalizeIdentifierForComparison(unqualified);
            if (!string.IsNullOrEmpty(normalized))
                keys.Add(normalized);
        }

        return keys;
    }

    private static string DeduplicatePartitionColumns(string partitionClause)
    {
        if (string.IsNullOrWhiteSpace(partitionClause))
            return partitionClause;

        var parts = partitionClause.Split(',', StringSplitOptions.RemoveEmptyEntries);
        var tokens = new List<(string Original, string Unqualified, string Normalized, string Qualifier, int Index)>();
        for (int i = 0; i < parts.Length; i++)
        {
            var trimmed = parts[i].Trim();
            if (trimmed.Length == 0)
                continue;

            var unqualified = ExtractUnqualifiedIdentifier(trimmed);
            if (string.IsNullOrEmpty(unqualified))
                continue;

            var normalized = NormalizeIdentifierForComparison(unqualified);
            if (string.IsNullOrEmpty(normalized))
                continue;

            var qualifier = ExtractQualifier(trimmed);

            tokens.Add((trimmed, unqualified, normalized, qualifier, i));
        }

        if (tokens.Count == 0)
            return string.Empty;

        var ordered = tokens
            .OrderBy(t => t.Normalized, StringComparer.OrdinalIgnoreCase)
            .ThenBy(t => t.Qualifier, StringComparer.OrdinalIgnoreCase)
            .ThenBy(t => t.Index)
            .ToList();

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var result = new List<string>();
        foreach (var token in ordered)
        {
            var key = BuildDedupKey(token.Normalized, token.Qualifier);
            if (seen.Add(key))
            {
                result.Add(string.IsNullOrEmpty(token.Qualifier) ? token.Unqualified : token.Original);
            }
        }

        return string.Join(", ", result);
    }

    private static string MergeGroupByAndPartition(string groupByClause, string partitionColumns, out bool merged)
    {
        merged = false;
        if (string.IsNullOrWhiteSpace(partitionColumns))
            return groupByClause;

        var groupColumns = ExtractGroupByColumns(groupByClause);
        var known = new HashSet<string>(groupColumns.Select(c => NormalizeGroupByKey(c)), StringComparer.OrdinalIgnoreCase);

        var partitionList = ExtractColumns(partitionColumns);
        foreach (var column in partitionList)
        {
            var key = NormalizeGroupByKey(column);
            if (known.Add(key))
                groupColumns.Add(column.Trim());
        }

        merged = partitionList.Count > 0;
        if (groupColumns.Count == 0)
            return string.Empty;

        return "GROUP BY " + string.Join(", ", groupColumns);
    }

    private static void DealiasClauses(Dictionary<string, SourceAliasMetadata> aliasMetadata, bool preserveSelectAlias, ref string selectClause, ref string groupByClause, ref string? partitionClause, ref string whereClause, ref string havingClause)
    {
        if (aliasMetadata == null || aliasMetadata.Count == 0)
            return;

        var ambiguousColumns = DetermineAmbiguousColumns(aliasMetadata.Values);

        if (!preserveSelectAlias && aliasMetadata.Count == 1 && ambiguousColumns.Count == 0)
        {
            var metadata = aliasMetadata.Values.First();
            selectClause = RemoveAliasFromSelectClause(selectClause, metadata);
        }

        // Preserve SELECT clause source aliases to maintain deterministic projection strings.
        groupByClause = ApplyAliasPreferences(groupByClause, aliasMetadata, ambiguousColumns);
        if (!string.IsNullOrWhiteSpace(partitionClause))
            partitionClause = ApplyAliasPreferences(partitionClause!, aliasMetadata, ambiguousColumns);
        whereClause = ApplyAliasPreferences(whereClause, aliasMetadata, ambiguousColumns);
        havingClause = ApplyAliasPreferences(havingClause, aliasMetadata, ambiguousColumns);
    }

    private static string ApplyAliasPreferences(string clause, Dictionary<string, SourceAliasMetadata> aliasMetadata, HashSet<string> ambiguousColumns)
    {
        if (string.IsNullOrWhiteSpace(clause))
            return clause;

        foreach (var metadata in aliasMetadata.Values)
        {
            clause = ReplaceAliasWithPreferredScope(clause, metadata, ambiguousColumns);
        }

        return clause;
    }

    private static string ReplaceAliasWithPreferredScope(string clause, SourceAliasMetadata metadata, HashSet<string> ambiguousColumns)
    {
        if (string.IsNullOrWhiteSpace(clause))
            return clause;

        var aliasPattern = Regex.Escape(metadata.Alias);

        clause = ReplaceAliasPattern(clause, $@"\b{aliasPattern}\.\`(?<column>[A-Za-z0-9_]+)\`", metadata, ambiguousColumns, '`');
        clause = ReplaceAliasPattern(clause, $@"\b{aliasPattern}\.""(?<column>[A-Za-z0-9_]+)""", metadata, ambiguousColumns, '"');
        clause = ReplaceAliasPattern(clause, $@"\b{aliasPattern}\.(?<column>[A-Za-z0-9_]+)", metadata, ambiguousColumns, null);

        return clause;
    }

    private static string RemoveAliasFromSelectClause(string selectClause, SourceAliasMetadata metadata)
    {
        if (string.IsNullOrWhiteSpace(selectClause))
            return selectClause;

        var alias = Regex.Escape(metadata.Alias);
        string Pattern(char quote)
            => $@"\b{alias}\.(?<quote>{quote})(?<column>[^{quote}]+){quote}";

        selectClause = Regex.Replace(
            selectClause,
            Pattern('`'),
            m => $"`{m.Groups["column"].Value}`",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        selectClause = Regex.Replace(
            selectClause,
            Pattern('"'),
            m => $"\"{m.Groups["column"].Value}\"",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        selectClause = Regex.Replace(
            selectClause,
            $@"\b{alias}\.(?<column>[A-Za-z_][A-Za-z0-9_]*)",
            m => m.Groups["column"].Value,
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        return selectClause;
    }

    private static string ReplaceAliasPattern(string clause, string pattern, SourceAliasMetadata metadata, HashSet<string> ambiguousColumns, char? quote)
    {
        return Regex.Replace(clause, pattern, match =>
        {
            var column = match.Groups["column"].Value;
            var normalized = NormalizeIdentifierForComparison(column);
            var columnWithQuote = quote switch
            {
                '`' => $"`{column}`",
                '"' => $"\"{column}\"",
                _ => column
            };

            if (ambiguousColumns.Contains(normalized))
                return $"{metadata.SourceName}.{columnWithQuote}";

            return columnWithQuote;
        }, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    }

    private static HashSet<string> DetermineAmbiguousColumns(IEnumerable<SourceAliasMetadata> aliasMetadata)
    {
        var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var metadata in aliasMetadata)
        {
            foreach (var column in metadata.ColumnIdentifiers)
            {
                if (string.IsNullOrEmpty(column))
                    continue;

                counts[column] = counts.TryGetValue(column, out var existing) ? existing + 1 : 1;
            }
        }

        return counts
            .Where(kv => kv.Value > 1)
            .Select(kv => kv.Key)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    private static string ExtractUnqualifiedIdentifier(string expression)
    {
        if (string.IsNullOrWhiteSpace(expression))
            return string.Empty;

        var value = expression.Trim();
        var arrowIndex = value.LastIndexOf("->");
        if (arrowIndex >= 0)
            value = value[(arrowIndex + 2)..];

        var dotIndex = value.LastIndexOf('.');
        if (dotIndex >= 0)
            value = value[(dotIndex + 1)..];

        return value.Trim();
    }

    private static string NormalizeIdentifierForComparison(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            return string.Empty;

        var value = identifier.Trim();
        if (value.EndsWith("()"))
            value = value[..^2];

        if (value.Length >= 2 && ((value[0] == '`' && value[^1] == '`') || (value[0] == '"' && value[^1] == '"')))
            value = value[1..^1];

        return value.ToUpperInvariant();
    }

    private static List<string> ExtractGroupByColumns(string groupByClause)
    {
        if (string.IsNullOrWhiteSpace(groupByClause))
            return new List<string>();

        var value = groupByClause.Trim();
        if (value.StartsWith("GROUP BY", StringComparison.OrdinalIgnoreCase))
            value = value.Substring("GROUP BY".Length).Trim();

        return ExtractColumns(value);
    }

    private static List<string> ExtractColumns(string columns)
    {
        if (string.IsNullOrWhiteSpace(columns))
            return new List<string>();

        return columns
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(c => c.Trim())
            .Where(c => c.Length > 0)
            .ToList();
    }

    private static string NormalizeGroupByKey(string column)
    {
        return NormalizeIdentifierForComparison(ExtractUnqualifiedIdentifier(column));
    }

    private static string ExtractQualifier(string expression)
    {
        if (string.IsNullOrWhiteSpace(expression))
            return string.Empty;

        var value = expression.Trim();
        var arrowIndex = value.LastIndexOf("->");
        if (arrowIndex >= 0)
        {
            var qualifier = value[..arrowIndex];
            return qualifier.Trim();
        }

        var dotIndex = value.LastIndexOf('.');
        if (dotIndex >= 0)
        {
            var qualifier = value[..dotIndex];
            return qualifier.Trim();
        }

        return string.Empty;
    }

    private static string BuildDedupKey(string normalized, string qualifier)
    {
        if (string.IsNullOrEmpty(qualifier))
            return normalized;

        return qualifier.ToUpperInvariant() + "::" + normalized;
    }

    private static Dictionary<string, SourceAliasMetadata> BuildAliasMetadata(Dictionary<string, string> aliasToSource, Type[] sourceTypes)
    {
        var map = new Dictionary<string, SourceAliasMetadata>(StringComparer.OrdinalIgnoreCase);
        if (aliasToSource == null || aliasToSource.Count == 0)
            return map;

        foreach (var kv in aliasToSource)
        {
            var alias = kv.Key;
            var sourceName = kv.Value;
            var sourceType = alias switch
            {
                "o" => sourceTypes.Length > 0 ? sourceTypes[0] : null,
                "i" => sourceTypes.Length > 1 ? sourceTypes[1] : null,
                _ => null
            };

            map[alias] = new SourceAliasMetadata(alias, sourceName, sourceType);
        }

        return map;
    }

    private static HashSet<string> ExtractColumnIdentifiers(Type? type)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (type == null)
            return set;

        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var sanitized = KsqlNameUtils.Sanitize(property.Name);
            var normalized = NormalizeIdentifierForComparison(sanitized);
            if (!string.IsNullOrEmpty(normalized))
                set.Add(normalized);
        }

        return set;
    }

    private sealed class SourceAliasMetadata
    {
        public SourceAliasMetadata(string alias, string sourceName, Type? sourceType)
        {
            Alias = alias;
            SourceName = sourceName;
            SourceType = sourceType;
            ColumnIdentifiers = ExtractColumnIdentifiers(sourceType);
        }

        public string Alias { get; }
        public string SourceName { get; }
        public Type? SourceType { get; }
        public HashSet<string> ColumnIdentifiers { get; }
    }

    private static string FormatTimeSpan(TimeSpan timeSpan)
    {
        if (timeSpan.TotalDays >= 1 && timeSpan.TotalDays == Math.Floor(timeSpan.TotalDays))
            return $"{(int)timeSpan.TotalDays} DAYS";
        if (timeSpan.TotalHours >= 1 && timeSpan.TotalHours == Math.Floor(timeSpan.TotalHours))
            return $"{(int)timeSpan.TotalHours} HOURS";
        if (timeSpan.TotalMinutes >= 1 && timeSpan.TotalMinutes == Math.Floor(timeSpan.TotalMinutes))
            return $"{(int)timeSpan.TotalMinutes} MINUTES";
        if (timeSpan.TotalSeconds >= 1 && timeSpan.TotalSeconds == Math.Floor(timeSpan.TotalSeconds))
            return $"{(int)timeSpan.TotalSeconds} SECONDS";
        if (timeSpan.TotalMilliseconds >= 1)
            return $"{(int)timeSpan.TotalMilliseconds} MILLISECONDS";
        return "0 SECONDS";
    }
}