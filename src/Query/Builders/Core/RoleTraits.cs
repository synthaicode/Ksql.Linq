using Ksql.Linq.Query.Analysis;

namespace Ksql.Linq.Query.Builders.Core;

/// <summary>
/// Describes window and emit behavior for each query role.
/// Final roles never compose intermediate sources; they operate on physical or view tables and require windowing with <c>EMIT FINAL</c>.
/// </summary>
internal static class RoleTraits
{
    public static OperationSpec For(Role role)
    {
        return role switch
        {
            // 1s hub/stream: no EMIT clause
            Role.Final1sStream => new(false, null),
            // Live windows use EMIT CHANGES
            Role.Live => new(true, "CHANGES"),
            _ => new(false, null)
        };
    }
}
