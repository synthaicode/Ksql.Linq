# Release Notes v0.9.7

## Breaking Changes
- `KsqlDslOptions.DeserializationErrorPolicy` and `ReadFromFinalTopicByDefault` are now `{ get; init; }`. If you were setting them after construction, move the assignment to option initialization.
- KSQL wait settings now rely on `KsqlDslOptions` first; environment variable fallbacks were removed. If you used `KSQL_QUERY_RUNNING_*`, set the equivalent keys in appsettings instead.
- Public API baseline: all previous Unshipped entries were moved to Shipped; Unshipped is reset. Future API changes should be added to Unshipped.

## New Features / Changes
- Wait settings unified: RUNNING checks now use `KsqlDslOptions` values; defaults apply when unset (Consecutive=5, Interval=2000ms, Stability=15s, Timeout=180s).
- Defaults managed in one place: removed `DefaultValue` attributes from `KsqlDslOptions`; rely on initializers only.

### Key KsqlDslOptions (defaults)
- `KsqlQueryRunningConsecutiveCount`: 5  
- `KsqlQueryRunningPollIntervalMs`: 2000  
- `KsqlQueryRunningStabilityWindowSeconds`: 15  
- `KsqlQueryRunningTimeoutSeconds`: 180  
- `KsqlWarmupDelayMs`: 3000  
- `KsqlDdlRetryCount`: 5  
- `KsqlDdlRetryInitialDelayMs`: 1000  
- `AdjustReplicationFactorToBrokerCount`: true  
- `KsqlSimpleEntityWarmupSeconds`: 15 / `KsqlQueryEntityWarmupSeconds`: 10 / `KsqlEntityDdlVisibilityTimeoutSeconds`: 12  
- `DeserializationErrorPolicy`: Skip  
- `ReadFromFinalTopicByDefault`: false  
- `DecimalPrecision`: 18 / `DecimalScale`: 2  

## Migration Guide
- If you relied on `KSQL_QUERY_RUNNING_*`, configure the corresponding `KsqlDsl` settings in appsettings (defaults above apply if omitted).
- If you set `DeserializationErrorPolicy` / `ReadFromFinalTopicByDefault` after construction, move the assignment to option initialization.
- Track future API changes in `PublicAPI.Unshipped.txt` (baseline moved to Shipped).

## Known Issues
- None new. Note that changing wait settings may alter stabilization timing; adjust values as needed for your environment.
