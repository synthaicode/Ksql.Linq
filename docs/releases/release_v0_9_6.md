# Release Notes v0.9.6

## Summary
- Runtime tuning knobs externalized to appsettings: query stabilization timeout, warmup seconds (simple/query entities), DDL visibility timeout, ksqlDB HTTP timeout.
- SchemaRegistrar / KsqlContext / KsqlWaitClient / KsqlDbClient updated to honor the new configurable timeouts and wait/retry behavior.
- Release automation improvements: comment/label-driven stable tagging, manual dispatch workflow, and refreshed release guide.

## Verification
- Build: `dotnet build src/Ksql.Linq.csproj -c Release -p:StrictPublicApi=true -warnaserror:RS0016,RS0017`
- Tests (Integration excluded): `dotnet test tests/Ksql.Linq.Tests.csproj -c Release --filter "TestCategory!=Integration"`

## RC/Release
- RC: GitHub Packages `0.9.6-ci.79` validated.
- Stable: `v0.9.6` tag on origin/main; `nuget-publish.yml` succeeded and pushed to nuget.org.
