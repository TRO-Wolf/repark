# map — crates/

## Purpose

The Cargo workspace: the repark engine, decomposed into single-responsibility crates layered on
Apache DataFusion + iceberg-rust + Arrow. One-directional dependency DAG.

## Contents

| Crate | Responsibility |
|---|---|
| [repark-common](repark-common/map.md) | Shared error-seed types (`Error` / `ErrorClass` / `Result`). Bottom of the DAG. |

Phase-1 crates still to land (see `docs/design/session-api.md`): `repark-iceberg` (catalog +
write over the owned iceberg-rust fork, tier 1) and `repark-core` (the `ReparkSession` engine
API, tier 2). DAG target: `repark-core → {repark-iceberg, repark-common}`,
`repark-iceberg → repark-common`.

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
