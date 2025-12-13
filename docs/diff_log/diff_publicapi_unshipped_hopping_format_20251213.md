# diff: PublicAPI baseline (Hopping) format fix (20251213)

## Summary
Fix `src/PublicAPI.Unshipped.txt` entries for newly added Hopping-related public APIs so that PublicApiAnalyzers strict mode (`RS0016/RS0017`) passes.

## Motivation
- `dotnet build -p:StrictPublicApi=true -warnaserror:RS0016,RS0017` failed due to mismatched baseline formatting.
- Without this, CI workflows that run strict public API checks can fail, and it becomes unclear whether `PublicAPI.Shipped.txt` has omissions or the baseline is simply malformed.

## Changes
- Align unshipped signatures with existing baseline conventions:
  - Use `Expression<Func<...>!>!` (note the `!` after `Func`), consistent with existing `Tumbling` entries.
  - Use `List<string!>!` for string lists (inner nullability marker).
  - Prefix static methods with `static` (e.g., `HoppingWindow.Get<T>`, `HoppingExtensions.ReadHoppingAsync<T>`).
  - Use `T!` in return types where appropriate (e.g., `HoppingWindow<T!>` and `IReadOnlyList<T!>`).

## Verification
- Local strict build passes:
  - `dotnet build src/Ksql.Linq.csproj -c Release -p:StrictPublicApi=true -warnaserror:RS0016,RS0017`

