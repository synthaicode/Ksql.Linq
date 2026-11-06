# Invariant numeric SQL and explicit empty-string preservation

## Approved scope

Fix reviewed K-C02 and K-R07 on source f9a31349ffc76baedc5cbfebb1c43def7c15d447.
The user authorized reproduction, regression tests and commit/push after checks.
Other review findings, policy decisions, dependency/version changes, main merge
and publication are outside this change.

## Behavior differences

- Numeric literals in the shared SQL formatter and GROUP BY constant path use
  invariant formatting. The same `Price > 1.5m` expression now yields
  `(Price > 1.5)` under both en-US and fr-FR, rather than fr-FR yielding `1,5`.
  The helper handles built-in numeric types only. Existing string, bool, null,
  date and enum rendering is preserved; SQL safety/escaping is not redesigned.
- GenericRecord normalization preserves explicitly present empty strings instead
  of replacing them with null in nullable unions. Explicit null, nonempty values
  and existing missing-field handling are retained. Both ordinary send and
  schema-alignment retry still call the same normalization boundary.

Implementation/test design: [feature instruction](../../features/culture-empty-preservation/instruction.md).

## Verification

- Added 29 L3 regression cases: 22 numeric/culture/formatting controls and seven
  string/null/missing-field controls. Before product changes: 8 expected failures,
  21 passes, no skips. After the fixes: all 29 pass, no skips. Tests were not
  weakened between the red and green runs.
- The same component reproduction, referencing the rebuilt product project,
  changed numeric invariance and explicit-value preservation from false/false
  to true/true. This is a product-boundary component observation, not a Kafka
  delivery or profiler-injection test.
- Release solution build: net8.0/net10.0, zero warnings/errors.
- Required L3 gate: 123 passes, two skips. Required L4 golden: two passes,
  no skips, no approval/update mode and no golden baseline changes.

- Full main unit suite: 691 passes, 27 skips (net8.0). CLI: four passes;
  cache: seven passes. These are 702 passing cases across distinct suites;
  the targeted/L3/L4 runs overlap and are not added to that total. Existing
  skips are retained, not counted as passes.
- No Kafka physical integration or profiler intervention was run for these
  local formatting/normalization fixes. net10.0 was compiled; the unit and
  reproduction executions above target net8.0.
The original library-wide review remains needs-review for its other findings.
- Independent review re-disposed K-C02 and K-R07 as pass-after-fix after reading
  the actual before/after, regression and gate evidence. This acceptance is
  limited to these two corrections, not the remaining library review findings.
