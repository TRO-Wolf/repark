# PROJECT.md — RePark (project intent & north star)

> **Purpose of this file.** Orient any agent or contributor to *what RePark is, why it exists, and
> the decisions that must not be undone*. The single authoritative day-to-day contract is
> [AGENTS.md](AGENTS.md) (which holds the precedence chain; [CLAUDE.md](CLAUDE.md) is a thin
> tool adapter), with a `map.md` in every directory. **This file states INTENT; the contract states
> MECHANICS** — if they conflict, reconcile, never silently discard intent.

## What RePark is

A pure-Rust, **no-JVM** single-node data engine with the deepest Apache Iceberg support of any
single-node engine. Positioning: **"Trino's SQL, DuckDB's deployment model, deepest Iceberg
support"** — a combination no current engine occupies. One `pip install`, no cluster, no daemon.
Built **on** Apache DataFusion + Arrow + our **owned iceberg-rust fork** (all pure-Rust and
JVM-free) with native PyO3 bindings. Compute runs in Rust; data crosses the Python boundary as
Apache Arrow, zero-copy.

Two user-facing **doors**, no blended parser:

- **Native door** — a lazy DataFrame API with an inspectable optimizer, plus `repark.sql()`
  speaking an **ANSI, Trino-style** dialect (catalog-determines-format CTAS, `WITH (…)` table
  properties, `FOR VERSION AS OF` time travel, maintenance as callable ops).
- **Spark facade door** — a near-drop-in PySpark facade whose `.sql()` keeps the **Spark dialect**
  unchanged, so existing PySpark pipelines migrate with only the import line changed.

The Iceberg machinery (commit semantics, MERGE, snapshots, evolution) is shared beneath both doors;
the dialect layers are thin translators.

## Goals

- Full Iceberg integration against AWS **Glue (primary)** + **S3 Tables (secondary)**, tracking the
  latest Iceberg features (V3: deletion vectors, row lineage, variant type) over time.
- Single binary / single `pip install` — no JVM, no cluster, no daemon; cold start under a second.
- Predictable memory via spill-to-disk by default. (*The "never OOM on data larger than RAM" claim
  is pending a spill-coverage spike — DataFusion's operator spill coverage is partial; the honest
  goal today is "spills where the engine can, documented where it cannot".*)
- Zero-copy interop — anything that speaks Arrow is a first-class citizen.
- Reproducibility: same query + same snapshot = same bytes, every time.
- **TA serving performance at parity with `polars_talib`** on the shapes that matter
  (many-symbols windows, wide multi-indicator SELECT statements, last-row serving) — reached
  **golden-safe**: bit-exact numerics are non-negotiable, so the wins come from
  allocation/copy/dispatch/plan-shape work, never math reordering; every optimization is
  gated on a recorded benchmark baseline (measure first, then implement). `unsafe` stays
  workspace-forbidden; a per-module exception is a last rung reached only on flamegraph
  evidence that safe restructuring cannot match, and carries Miri + fuzz gates of its own.
- Long-term ease of maintenance: strict testing contract, mechanical gates, one owned fork.

## Differentiators

The reasons someone picks this over DuckDB or Polars; everything else is table stakes.

- Deepest Iceberg support of any single-node engine, including V3 features.
- A native TA (technical-analysis) function library, bit-exact and fast, built in Rust.
- Aggressive, intelligent schema inference and a CSV reader that handles real-world mess.
- First-class Excel read/write.
- ML that trains directly off Iceberg tables out-of-core, no extraction step.
- The PySpark facade: migrate existing pipelines without rewrites.

## Non-negotiable invariants (do not undo)

- **iceberg-rust is forked & owned; DataFusion is built ON (not forked).** The
  `TRO-Wolf/iceberg-rust` fork is a **sibling sub-project we own** (1:1 Java `iceberg-core`
  parity), a **separate repo, never vendored**; the engine-agnostic table-format work lives in the
  fork. `[patch.crates-io]` + rev-pin is the wiring (from phase 1). See
  [docs/adr/0001-own-iceberg-fork.md](docs/adr/0001-own-iceberg-fork.md).
- **Two honest SQL doors, no blended parser** —
  [docs/adr/0002-two-sql-doors.md](docs/adr/0002-two-sql-doors.md).
- **Server-prep disciplines from day one** (everything-through-Session, bindings-as-thin-adapter) —
  [docs/adr/0004-server-prep-disciplines.md](docs/adr/0004-server-prep-disciplines.md).
- **No PyIceberg in any form**; **no Sail / pysail**. Own-the-stack.
- **`unsafe_code = "forbid"`** workspace-wide EXCEPT `crates/repark-python` (landed phase-3 PR-3;
  the crate sets a local `unsafe_code = "allow"` because PyO3 macros expand to `unsafe`).
- **Tests land with the code** (same commit); `cargo test --workspace` is the test command.
- **`map.md` in every directory, in lockstep** with code changes.
- **Pin one DataFusion family** across all DF-touching crates; `Cargo.toml` is the SSOT;
  `Cargo.lock` checked in.
- **Distribution is deferred** behind the `ExecutionBackend` seam.
- **[AGENTS.md](AGENTS.md) is the single authoritative contract** (it holds the precedence chain);
  tool adapters ([CLAUDE.md](CLAUDE.md), [.agents/](.agents/map.md)) carry no authoritative facts and
  cannot drift.

## Target architecture (crate skeleton — the port's destination)

```
crates/
  repark-core        lazy-frame IR, planning, optimizer hooks, Session
  repark-exec        execution config, spill, out-of-core (thin over DataFusion early on)
  repark-io          smart CSV, Excel, JSON — the inference readers
  repark-iceberg     catalogs (Glue, S3 Tables) + DML + maintenance; adapter over the owned fork
  repark-connect     Postgres, MSSQL connectivity
  repark-sql         ANSI SQL front end (native dialect)
  repark-spark       Spark semantics: function shims, Spark SQL dialect, the parity surface
  repark-ml          Arrow→DMatrix handoff, out-of-core training
  repark-python      PyO3: thin adapter over the internal engine API
python/repark        native lazy API + repark.sql()
python/repark/spark  near-drop-in PySpark facade
dbt-repark           dbt adapter, separate Python package (modeled on dbt-duckdb)
```

v1 crates re-home rather than rewrite. DataFusion remains the engine under everything.

## Distributed posture

**Fleet-parallel → server mode → distributed-if-needed.** Fleet-parallel (many embedded engines
against one Iceberg catalog; the catalog's commit protocol is the coordinator) covers
backtest/parameter-sweep scale-out with zero engine work. Server mode (an Arrow Flight SQL
endpoint) is later an adapter, not a rewrite, because of the server-prep disciplines. Distributed
single-query execution comes only if a query outgrows one box, behind the `ExecutionBackend` seam.

## Year one

Priorities serve **the operator's production data-engineering workloads (Airflow + Iceberg +
dbt)**. Load-bearing surfaces, in order: (1) the SQL engine with Iceberg DML, (2) **dbt-repark**
(engine embedded in the dbt process — dbt-duckdb precedent), (3) the lazy DataFrame API for Airflow
tasks. The PySpark facade's year-one job: migrate existing pipelines without rewrites.

## Validation & documentation roadmap (owner-set, 2026-08-16)

Four workstreams the owner has named as roadmap commitments. Each ships twice: as repo
artifacts (docs / datasets / test suites) AND as runnable Jupyter notebooks.

1. **Full examples documentation.** A worked example for every public function and
   transformation — every `F.*` function, every DataFrame method, every TA kernel, every
   reader/writer — kept executable so drift fails loudly rather than rotting in prose.

2. **Torture-test dataset suite** (each dataset ≥1M rows, generators checked in so the data
   is reproducible, never committed as blobs):
   - *Nested reading + `dynamicFlatten`*: deep struct/list nesting with mixed element types,
     lists of structs, capitalized field names, and null-typed lists.
   - *Schema-inference conflicts*: columns whose observed type shifts mid-file (int32 until
     row 500k then int64; string-vs-float halves; bool-looking ints; date-looking strings;
     as many conflict classes as we can enumerate) — the inference torture battery.
   - *Extreme types*: high-precision decimals (decimal128-scale values such as
     `102.102334252345232345233`), UUID columns, paragraph-length strings, and columns with
     embedded HTML fragments.
   - *Secrets-flagging fixture*: columns named like credentials (`apiKey`, `api_key`,
     `api_token`, `access_token`, …) carrying fake plaintext secrets — the fixture for an
     **opt-in secrets-flagging mechanism** (disabled by default; a bool conf enables
     read-time flagging/refusal). The mechanism itself is a roadmap feature this fixture
     exists to test.
   - *smartCsv expansion*: grow the messy-CSV battery (header normalization, blank cells,
     currency/decimal widths, bool spellings) far beyond the current three-row example.

3. **Full Iceberg statement coverage.** Every DML and DDL statement, and every system
   operation (`rewrite data files`-class maintenance, snapshot rollback/expiry, branch/tag
   operations — the complete procedure surface), each validated in **full comparison against
   PySpark** on the same tables; gaps land in the divergence registry, never silently.

4. **Cross-engine function validation.** Systematic comparison-and-validation batteries for
   the full `pyspark.sql.functions` surface (extending the existing parity harness), plus
   equivalent comparisons against polars and DuckDB function behavior where surfaces
   overlap — three oracles, one function matrix.

## Current state

**Status is tracked in [STATUS.md](STATUS.md)** — the single source of truth for release state,
delivered capabilities, active workstreams, and deferred work. PROJECT.md states intent only; it
does not restate current state. The port that stood this repository up is recorded in
[docs/port/PLAN.md](docs/port/PLAN.md) and
[docs/adr/0003-copy-then-rehome-port.md](docs/adr/0003-copy-then-rehome-port.md).

## Conventions (summary; see [AGENTS.md](AGENTS.md) + [DEVELOPMENT.md](DEVELOPMENT.md) for the full contract)

Rust: rustfmt `max_width=100`, edition 2024, clippy `all`+`pedantic` `-D warnings`, 91-`=` banner
doc blocks on section functions, `thiserror`/`anyhow`, no panics in prod (no `unwrap`/`expect`).
Python: type hints, Pydantic v2, `pathlib`, `logging`, Ruff line-100. `make ci` is the gate;
`make verify` and `make preflight` extend it. Every directory carries a `map.md`
(Purpose / Contents / I want to… / Pointers / Debug).
