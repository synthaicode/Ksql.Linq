# Ksql.Linq overview

This file is a quick map of the repository so you can find things fast.

## Key directories

- `src/`: library implementation (LINQ → KSQL translation, runtime, etc.)
- `tests/`: unit tests / golden tests and test runner scripts
- `docs/`: design & operational documentation
  - `docs/diff_log/`: design/behavior change logs (`diff_{feature}_{YYYYMMDD}.md`)
- `examples/`: usage samples

## Common test commands

- UT (guard: L3): `pwsh -NoLogo -File tests/run-ut.ps1 -Level gate -Configuration Release`
- Golden (guard: L4): `pwsh -NoLogo -File tests/run-golden.ps1`
- Impact guard (auto): `pwsh -NoLogo -File tests/run-guard.ps1 -Mode auto`
