# Structure Graph Profile — Ksql.Linq

This `.xrefkit/` directory persists the **requirement-independent** parts of the
structure-graph backstop for this repository. It is the relation/classification
layer that XDDP's *Where* step (spec-out → traceability matrix) traverses.

Design reference: XRefKit `knowledge/source_analysis/160_structure_graph_tm_backstop.md`
(xid `163AD9936979`).

## What is persisted here (and why)

These are facts/decisions that do **not** depend on any single change request, so
they live in the repo and are reviewed/versioned:

| File | Holds |
|------|-------|
| `graph-profile.json` | GraphProfile: identity, summary, hub classification, name-coupling classification, external-boundary definition |
| `graph-rules.yml` | traversal / pruning rules (which edges, which name categories, what to prune as transit) |
| `graph-baseline.json` | accepted baseline — the frozen snapshot future runs diff against for drift |
| `graph-notes.md` | this file |

## What is NOT persisted here (per-change artifacts)

The "cut" is computed per change requirement and must not be stored as a fixed
mark (see design principle 1). The following are **artifacts**, produced per run
and kept outside this profile (e.g. attached to the PR / change record):

- the change-requirement traceability matrix (TM)
- the impacted-boundary list
- per-PR traversal results
- transient seeds

## Why this repo needs name coupling (it is unusual)

Most .NET projects are type-coupled and the call+type graph is sufficient.
Ksql.Linq is the contrasting case: it is a KSQL DSL whose impact propagates
through **string names** (topic / window column / config key) that the call
graph cannot see. The top fan-in symbol is `GetTopicName` — a name-resolution
funnel — and shared names like `BucketStart` (14 members), `_1s_rows` (10),
`retention.ms` (7) couple members that have no call relationship. Hence
`name_coupling.required_for_tm = true`.

## Key scheme

Nodes are keyed by Roslyn documentation-comment id (DocID, e.g.
`M:Ns.Type.Method(System.String)`). DocID is deterministic, stable across
body-only edits, and collapses multi-target (net8.0/net10.0) duplication
automatically.

## Known limitations of this profile

- name-edge extraction is a generic identifier-like-literal heuristic with a
  keyword stoplist; residual generic/method-name noise remains (e.g. `name`,
  `Value`, `ToUpper`).
- the Avro/reflection codegen boundary (`SpecificRecordGenerator`,
  `PopulateAvroKeyValue`) is reflection-driven and is **not** mechanically
  traceable — it stays spec-out territory.
- type-level attribute-declared names (e.g. on entity classes) are not yet
  captured; only method-body literals are.
