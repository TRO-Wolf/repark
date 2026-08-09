# map — crates/

## Purpose

The Cargo workspace: the repark engine, decomposed into single-responsibility crates layered on
Apache DataFusion + iceberg-rust + Arrow. One-directional dependency DAG.

## Contents

| Crate | Responsibility |
|---|---|
| [repark-common](repark-common/map.md) | Shared error-seed types (`Error` / `ErrorClass` / `Result`) + the dialect-neutral SQL surface registry (`surfaces`) both doors' `matrix.rs` audits against. Bottom of the DAG. |
| [repark-iceberg](repark-iceberg/map.md) | Iceberg surface (tier 1): Glue + S3 Tables catalog wiring for DataFusion (`catalog/`) + the Spark-semantics write adapter — MERGE INTO, append, overwrite, ALTER — over the owned fork (`write/`). Carries the `[patch.crates-io]` fork pin's consumers. |
| [repark-core](repark-core/map.md) | The Session-centric engine API (tier 2): `ReparkSession` over a DataFusion `SessionContext` + the `ExecutionBackend` / `SqlDialect` / `SessionExtension` seams (delivered; phase-1 PR-C). |
| [repark-functions](repark-functions/map.md) | Spark-compatible function registry (tier 3): `datafusion-spark` registration + the Spark-semantics date shim + analyzer rules. DataFusion-native — no `repark-core` dep. |
| [repark-ta](repark-ta/map.md) | Technical-analysis kernels (tier 3): bit-exact TA-Lib 0.4.0 hand-ports, golden-gated, plus the optional `datafusion` feature's window-UDF layer and the door-neutral `TaExtension`. |
| [repark-spark](repark-spark/map.md) | The Spark SQL door (tier 3): v1 `repark-sql` ported — statement router + `SparkDialect`/`SparkExtension` over the phase-1 seams. Delivered across PR-2 (spine) + PR-3a/PR-3b (DDL/DML handlers). |
| [repark-sql](repark-sql/map.md) | The **ANSI/Trino-flavoured SQL door** (tier 3): NEW code — `AnsiDialect` over the frozen seam, delegating to DataFusion and intercepting only the Iceberg catalog DDL it cannot express. No `SessionExtension` (native semantics ARE stock DataFusion), no `sqlparser` or `datafusion-spark` dep. Delivered across PR-5 (M1: guards, wrong-door sniff, CREATE TABLE family + `WITH (…)`, schema DDL) + PR-6 (ALTER/MERGE/time travel). |
| [repark-ml](repark-ml/map.md) | Native ML estimator kernels (tier 3): hand-rolled Cholesky + streaming fit accumulators (OLS, IRLS logistic, Lloyd k-means). A capability leaf with **no internal deps** and one third-party dep (`thiserror`); models hold params only. Ported verbatim at the phase-3 port pin (phase-3 PR-2). |
| [repark-python](repark-python/map.md) | The PyO3 `cdylib` (**tier 4 "bindings"** — the only tier-4 crate, and nothing may ever depend on it): `repark._native`, a thin adapter exposing `PyReparkSession` / `PyDataFrame` / `PyColumn`, the PySpark exception taxonomy, and the M3 ML fit binder. Data crosses as Arrow via the PyCapsule interface, zero-copy. **The only crate allowed `unsafe`**, and the only one that opts out of `[lints] workspace = true`. Ported at the phase-3 port pin under design §3's edit classes EC-1/2/3/5/6/10 (phase-3 PR-3). |

DAG: `repark-core → {repark-iceberg, repark-common}`, `repark-iceberg → repark-common`;
`repark-functions` is a tier-3 leaf with no internal deps (speaks `datafusion::error::Result`);
`repark-ta → repark-core` **only under the `datafusion` feature** (the `TaExtension` wrapper — the
kernel core stays dependency-light); `repark-spark → {repark-core, repark-iceberg,
repark-functions, repark-ta}` (tier-3 door; same-tier edges to repark-functions and repark-ta are
legal); `repark-sql → {repark-core, repark-iceberg, repark-common}` (the other tier-3 door);
`repark-ml` is a tier-3 leaf with no internal deps at all (pure math + accumulators — the PyO3
binding is what streams rows into it); `repark-python → {repark-core, repark-functions, repark-ta
(feature `datafusion`), repark-spark, repark-ml}` — five edges, tier 4 reaching down, plus the two
**deliberate non-edges** review enforces (design §2.2): no `repark-sql` (zero ANSI surface from
Python) and no `repark-iceberg` (the binding reaches Iceberg only through `ReparkSession` and SQL
text). Its `repark-common` edge is **dev-only** (the EC-1 type-identity guard) — declared in the
policy as a `dev` edge, so it is visible and reasoned about, but exempt from the layering rule
(a test-only edge is not a layering statement).
**There is no door→door edge, ever** (design §1): the two doors share machinery only through
tiers 0–1, which is what keeps each free to have its own grammar. The one crossing —
`repark-sql → repark-spark`, the cross-door test protocol — is declared `dev`; the same edge as
`normal` is exactly what the policy forbids.

> **The dependency-policy SSOT is
> [`../scripts/check_crate_dag.py`](../scripts/check_crate_dag.py)** — it holds the tier map,
> the crate **roles**, and the **explicit allowed-edge table** (every internal edge with the
> dependency KINDS it may take: `normal` / `optional` / `dev` / `build`). Enforced by
> `make check-crate-dag`, and crate-root thinness by `make check-lib-rs`, both in the `make ci`
> chain and the pre-commit hook. The rules: every internal edge must be **declared** with its
> kind and a reason (a new same-tier edge reds until it is); no door → door edge outside `dev`;
> nothing depends on the bindings crate; `repark-common` depends on nothing internal; a
> capability crate never depends on a door; and no PRODUCT edge points at a **strictly higher**
> tier (same-tier edges are allowed). The prose above is **orientation only** — read the script
> for the truth, and when adding a crate or an edge, change the script, not this file. Each
> crate's path / layer / delivery status is additionally declared in
> [`../repo-manifest.toml`](../repo-manifest.toml), whose `layer` values `make check-manifest`
> cross-checks against this same script.

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
| Add / fix a TA indicator, or its SQL window UDF | [repark-ta/map.md](repark-ta/map.md) |
| Spark-SQL statement routing / dialect / extension | [repark-spark/map.md](repark-spark/map.md) |
| Change an ML solver / estimator kernel | [repark-ml/map.md](repark-ml/map.md) |
| Add/change a Python-visible method, class or exception | [repark-python/map.md](repark-python/map.md) |
| See where the next crates land | `../docs/design/session-api.md` |

## Pointers

- Up: [../map.md](../map.md)
- Architecture (crate DAG + the three runtime flows): [../ARCHITECTURE.md](../ARCHITECTURE.md).
- Each crate-root `map.md` carries a standardized `## Component contract` section (Owns /
  Does-not-own / inputs / outputs / lifecycle / allowed deps / failure model / extension points /
  test strategy / limitations) — the per-crate contract detail ARCHITECTURE.md indexes.

## Debug

| Symptom | First check |
|---|---|
| `unsafe` lint error | Only `repark-python` may use `unsafe`; move FFI there |
| `cargo test --all-features` fails to link | Never use `--all-features`: it turns on `repark-python`'s `extension-module`, which tells PyO3 not to link libpython — see [repark-python/map.md#debug](repark-python/map.md) |
| Version-resolution conflict | Pin one DataFusion across `datafusion`/`datafusion-spark`/`iceberg*` (see AGENTS.md) |
| `crate-dag: layering inversion` | A new dep points UP a tier — see [../scripts/map.md#debug](../scripts/map.md) |

First checks: `cargo clippy --workspace --all-targets -- -D warnings`. Escalate to:
[../map.md#debug](../map.md).
