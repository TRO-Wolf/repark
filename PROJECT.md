# PROJECT.md — RePark (project intent & north star)

> **Purpose of this file.** Orient any agent or contributor to *what RePark is, why it exists, and
> the decisions that must not be undone*. The day-to-day execution contract is
> [CLAUDE.md](CLAUDE.md) + [AGENTS.md](AGENTS.md) (authoritative, must stay in sync), with a
> `map.md` in every directory. **This file states INTENT; those state MECHANICS** — if they
> conflict, reconcile, never silently discard intent.

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
- **`unsafe_code = "forbid"`** workspace-wide EXCEPT the future `crates/repark-python`.
- **Tests land with the code** (same commit); `cargo test --workspace` is the test command.
- **`map.md` in every directory, in lockstep** with code changes.
- **Pin one DataFusion family** across all DF-touching crates; `Cargo.toml` is the SSOT;
  `Cargo.lock` checked in.
- **Distribution is deferred** behind the `ExecutionBackend` seam.
- **[CLAUDE.md](CLAUDE.md) and [AGENTS.md](AGENTS.md) are the authoritative contracts and must not
  drift from each other.**

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

## Current state (2026-08-06)

**Phase 1 of the port — engine core (in flight).** Phase 0 (bootstrap) is complete: governance,
the testing contract, mechanical gates, map.md discipline, SEPMO, and tier-1 CI are in place and
green. Phase-1 PR-A armed the Cargo workspace: `crates/repark-common` (the error seed) is the
first member, with the crate-DAG and lib.rs guards live. `repark-iceberg` (PR-B) and
`repark-core` (PR-C) follow. Code arrives by porting the private v1 repository copy-then-re-home in
four phases (0 bootstrap → 1 engine core → 2 the two SQL doors → 3 Python facade + parity =
milestone one); v1 freezes to bugfix-only at milestone one. **Public ≠ released:** the API-forever
clock starts at the first tagged PyPI release, held until milestone one. See
[docs/port/PLAN.md](docs/port/PLAN.md) and
[docs/adr/0003-copy-then-rehome-port.md](docs/adr/0003-copy-then-rehome-port.md).

See [task/todo.md](task/todo.md) + [task/lessons.md](task/lessons.md) for live state.

## Conventions (summary; see [CLAUDE.md](CLAUDE.md) / [AGENTS.md](AGENTS.md) for the full contract)

Rust: rustfmt `max_width=100`, edition 2024, clippy `all`+`pedantic` `-D warnings`, 91-`=` banner
doc blocks on section functions, `thiserror`/`anyhow`, no panics in prod (no `unwrap`/`expect`).
Python: type hints, Pydantic v2, `pathlib`, `logging`, Ruff line-100. `make ci` is the gate;
`make verify` and `make preflight` extend it. Every directory carries a `map.md`
(Purpose / Contents / I want to… / Pointers / Debug).
