# map — docs/guide/

## Purpose

**User-facing** documentation: how to *use* repark, written for a data engineer adopting it as a
PySpark replacement. Plain Markdown, no site tooling.

This directory is the one place in the repo aimed at a **user** rather than a contributor. The
contributor spine ([../../AGENTS.md](../../AGENTS.md), [../../ARCHITECTURE.md](../../ARCHITECTURE.md),
[../../DEVELOPMENT.md](../../DEVELOPMENT.md), [../testing.md](../testing.md)) is not restated here,
and nothing here is authoritative: a guide **describes** behavior the engine already has and
**links** the document that owns each fact —
[../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) for every difference from Apache
Spark, [../../STATUS.md](../../STATUS.md) for release and delivery state, the ADRs for why a
boundary exists. A guide that restates one of those is wrong by construction.

**Truth rule for this directory:** every behavioral claim is verified against the tree, and every
code snippet shown was executed against a built module before it landed — outputs are real, never
illustrative. A claim with no verified basis does not go in.

## Contents

- [getting-started.md](getting-started.md) — install from PyPI (`pip install repark`, Python ≥ 3.12,
  abi3 wheel, the optional extras), the one-line import swap, the first session, `createDataFrame`,
  parquet / CSV / JSON round trips, a first `dynamicFlatten`, and the pointer to the tour notebook.
- [session-and-conf.md](session-and-conf.md) — the `ReparkSession` builder; `getOrCreate` reuse
  semantics; how `conf.get` / `conf.set` behave (unset keys raise; three tiers of key: build-time
  engine knob / live `datafusion.*` / facade-local); where the defaults live (`_SQLCONF_DEFAULTS`);
  and the keys users actually set — `spark.sql.pyspark.inferNestedDictAsStruct.enabled` (FA-4),
  `spark.sql.session.timeZone` (TZ-2 / TZ-3), `spark.sql.ansi.enabled`, target partitions, batch
  size, the one-truth memory pool, `repark.display.style`.
- [dataframe-guide.md](dataframe-guide.md) — the lazy model and what is schema-only; select /
  filter / groupBy / joins (incl. the semi family and the conditionless refusal, G4-3) / window
  functions; the action table (`collect` / `to_arrow` / `to_arrow_batches` / `toPandas` /
  `to_polars` / `toLocalIterator`) with peak-memory cost; ingestion shapes and map-vs-struct dict
  inference under the FA-4 default; struct-field addressing; `dynamicFlatten` flags and
  mixed-case `explode`; and the limits worth knowing (FA-1, ID-1, ID-3, G10-1, TY-4/TY-5, FA-3).
- [sql-doors.md](sql-doors.md) — the two SQL surfaces honestly: the Spark-facade door
  (`spark.sql`, Spark dialect, your session) and the native door (`repark.sql`, stock
  DataFusion/ANSI, its own process-wide session); the no-blended-parser rule (ADR-0002); why two
  sessions; wrong-door sniffing at a high level and what the Python callable does *today*;
  identifier case (ID-1); how to choose.
- `map.md` — this file.

## I want to...

| ...do this | go to |
|---|---|
| Install repark and run something | [getting-started.md](getting-started.md) |
| Find out which conf key does what, and when it takes effect | [session-and-conf.md](session-and-conf.md) |
| Understand why a `conf.set` appeared to do nothing | [session-and-conf.md](session-and-conf.md) "How `conf.get` / `conf.set` behave" |
| Learn the DataFrame API / flatten nested data | [dataframe-guide.md](dataframe-guide.md) |
| Work out which `sql()` to call | [sql-doors.md](sql-doors.md) |
| Find out how repark differs from Apache Spark, and why | [../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) (authoritative) |
| Check release / delivery state | [../../STATUS.md](../../STATUS.md) (authoritative) |
| Run a worked example end to end | [../../examples/notebooks/datasets_tour.ipynb](../../examples/notebooks/datasets_tour.ipynb) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../../examples/map.md](../../examples/map.md) — runnable examples; the guides link the
  tour notebook rather than duplicating it. [../../README.md](../../README.md) points here.

## Constraints

- No credentials, no real hosts (`example.com` only), no absolute user paths in any snippet.
- Snippets are executed before they land; an output block that was not produced by a real run does
  not belong here.
- Do not restate STATUS.md, the divergence registry, or the contributor spine — link them.

## Debug

| Symptom | First check |
|---|---|
| A guide and the divergence registry disagree | The registry wins — it is the authoritative home and carries the pin. Fix the guide |
| A guide describes a surface that no longer exists | The guide was not updated with the change; the pin that moved names the new behavior |
| A snippet's output does not reproduce | Rebuild the module (`make develop`) — a guide's outputs are recorded against a built wheel, not a source tree |
