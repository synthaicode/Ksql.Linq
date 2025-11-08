# EntitySets

Core implementation of `EventSet<T>` for Kafka stream operations.

## ⚠️ IMPORTANT: Namespace

All classes in this directory use `namespace Ksql.Linq;` (NOT `Ksql.Linq.EntitySets`).

**Reason**: Users should only need `using Ksql.Linq;` to access all public APIs.

```csharp
// src/EntitySets/EventSet.cs
namespace Ksql.Linq;  // ← NOT Ksql.Linq.EntitySets!

public abstract class EventSet<T> : IEntitySet<T> where T : class
{
    // ... implementation
}
```

## Files

- **EventSet.cs**: Base abstract class implementing `IEntitySet<T>`
  - Namespace: `Ksql.Linq`
  - Provides `ForEachAsync()`, `ToListAsync()`, `AddAsync()`
  - Handles error propagation and DLQ integration

- **EventSetWithServices.cs**: Concrete implementation
  - Namespace: `Ksql.Linq`
  - Integrates with `KsqlContext` services (producer/consumer managers)
  - Instantiated via `KsqlContext.Set<T>()`

## Design Notes

`EventSet<T>` is the core abstraction for bidirectional Kafka operations:
- **Producer**: `AddAsync()` sends messages to topics
- **Consumer**: `ForEachAsync()` subscribes to streaming data
- **Snapshot**: `ToListAsync()` reads current table state

Related extension methods live under `src/Extensions/` (also `namespace Ksql.Linq;`).
