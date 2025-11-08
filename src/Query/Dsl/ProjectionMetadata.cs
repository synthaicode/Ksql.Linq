using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Ksql.Linq.Query.Dsl;

public sealed record ProjectionMetadata(
    IReadOnlyList<ProjectionMember> Members,
    bool IsHubInput);

public sealed record ProjectionMember(
    string Alias,
    Expression Expression,
    string ExpressionText,
    ProjectionMemberKind Kind,
    string? ResolvedColumnName,
    string? AggregateFunctionName,
    string? SourceMemberPath,
    Type ResultType,
    bool IsNullable);

public enum ProjectionMemberKind
{
    Aggregate,
    Key,
    Value,
    Computed
}
