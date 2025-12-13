# diff: Promote PublicAPI.Unshipped to Shipped (v1.1.0) (20251213)

## Summary
Promote the v1.1.0 public API surface from `src/PublicAPI.Unshipped.txt` into `src/PublicAPI.Shipped.txt`, and reset `src/PublicAPI.Unshipped.txt` back to header-only.

## Motivation
- For stable releases, newly introduced public APIs should be declared in `PublicAPI.Shipped.txt`.
- Keeping release APIs in `Unshipped` makes it harder to detect real omissions and confuses release readiness checks.

## Promoted APIs (examples)
- Hopping window API surface:
  - `KsqlQueryable<T>.Hopping(...)`
  - `KsqlQueryable2<T1,T2>.Hopping(...)`
  - `KsqlQueryModel.HasHopping()` / `Hopping` / `OperationSequence`
  - `HoppingWindowSpec`
- Runtime helpers:
  - `HoppingWindow.Get<T>(...)`
  - `HoppingExtensions.ReadHoppingAsync<T>(...)`
- Model additions:
  - `EntityModel.TimeKey` getter/setter
  - `IWindowedRecord` interface

## Verification
- Strict public API build passes:
  - `dotnet build src/Ksql.Linq.csproj -c Release -p:StrictPublicApi=true -warnaserror:RS0016,RS0017`

