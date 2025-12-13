using System;

namespace Ksql.Linq.Core.Abstractions;

/// <summary>
/// Base contract for windowed query results exposing window bounds.
/// </summary>
public interface IWindowedRecord
{
    DateTime WindowStart { get; }
    DateTime WindowEnd { get; }
}
