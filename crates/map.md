# map — crates/

## Purpose

The Cargo workspace: the repark engine, decomposed into single-responsibility crates layered on
Apache DataFusion + iceberg-rust + Arrow. One-directional dependency DAG.

## Contents

| Crate | Responsibility |
|---|---|
| [repark-common](repark-common/map.md) | Shared error-seed types (`Error` / `ErrorClass` / `Result`). Bottom of the DAG. |
| [repark-iceberg](repark-iceberg/map.md) | Iceberg surface (tier 1): Glue + S3 Tables catalog wiring for DataFusion (`catalog/`) + the Spark-semantics write adapter — MERGE INTO, append, overwrite, ALTER — over the owned fork (`write/`). Carries the `[patch.crates-io]` fork pin's consumers. |
| [repark-core](repark-core/map.md) | The Session-centric engine API (tier 2): `ReparkSession` over a DataFusion `SessionContext` + the `ExecutionBackend` / `SqlDialect` / `SessionExtension` seams (phase-1 PR-C, landing commit-by-commit). |
| [repark-functions](repark-functions/map.md) | Spark-compatible function registry (tier 3): `datafusion-spark` registration + the Spark-semantics date shim + analyzer rules. DataFusion-native — no `repark-core` dep. |
| [repark-spark](repark-spark/map.md) | The Spark SQL door (tier 3): v1 `repark-sql` ported — statement router + `SparkDialect`/`SparkExtension` over the phase-1 seams. PR-2 ships the spine; DDL/DML handlers land PR-3a/PR-3b. |

DAG: `repark-core → {repark-iceberg, repark-common}`, `repark-iceberg → repark-common`;
`repark-functions` is a tier-3 leaf with no internal deps (speaks `datafusion::error::Result`);
`repark-spark → {repark-core, repark-iceberg, repark-functions}` (tier-3 door; same-tier edge to
repark-functions is legal).

> **The layering SSOT is [`../scripts/check_crate_dag.py`](../scripts/check_crate_dag.py)** —
> it holds the tier map and is enforced by `make check-crate-dag`, and crate-root thinness by
> `make check-lib-rs`, both in the `make ci` chain and the pre-commit hook. The rule: no
> `repark-*` crate depends on a **strictly higher** tier; same-tier edges are allowed. The prose
> above is **orientation only** — read the script for the truth, and when adding a crate or an
> edge, change the script, not this file.

> **Internal deps are declared once, in the root `[workspace.dependencies]`**, each as
> `{ path = …, version = "0.0.0", default-features = false }`; members write
> `repark-x.workspace = true` and add only their own `features = [...]`.
> `default-features = false` is deliberate and load-bearing: **a new internal `default` feature
> will NOT reach consumers automatically** — make it opt-in at the consuming manifest. Adding it
> later would be a workspace-wide breaking change (rust-lang/cargo#11329), so it is set now
> while the internal feature surface is empty. The `version` matches `[workspace.package]` and
> is forward-compat only — the workspace is `publish = false`.

## I want to...

| ...do this | go to |
|---|---|
| Add an error variant / shared seed type | [repark-common/map.md](repark-common/map.md) |
| Catalog wiring / MERGE / append / overwrite / ALTER | [repark-iceberg/map.md](repark-iceberg/map.md) |
| Add a `ReparkSession` method / session knob / reader | [repark-core/map.md](repark-core/map.md) |
| Add/fix a Spark function or date-shim UDF | [repark-functions/map.md](repark-functions/map.md) |
| Spark-SQL statement routing / dialect / extension | [repark-spark/map.md](repark-spark/map.md) |
| See where the next crates land | `../docs/design/session-api.md` |

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| `unsafe` lint error | Only `repark-python` (phase 3) may use `unsafe`; move FFI there |
| Version-resolution conflict | Pin one DataFusion across `datafusion`/`datafusion-spark`/`iceberg*` (see AGENTS.md) |
| `crate-dag: layering inversion` | A new dep points UP a tier — see [../scripts/map.md#debug](../scripts/map.md) |

First checks: `cargo clippy --workspace --all-targets -- -D warnings`. Escalate to:
[../map.md#debug](../map.md).
