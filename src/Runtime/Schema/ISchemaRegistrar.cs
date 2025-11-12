using System.Threading;
using System.Threading.Tasks;
using Ksql.Linq.Core.Abstractions;

namespace Ksql.Linq.Runtime.Schema;

/// <summary>
/// Registers schemas and ensures DDL materialization for entities.
/// </summary>
internal interface ISchemaRegistrar
{
    /// <summary>
    /// Registers schemas, creates topics/streams/tables as needed, and performs warmup/validation.
    /// </summary>
    Task RegisterAndMaterializeAsync(CancellationToken ct = default);
}
