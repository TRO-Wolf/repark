# map — repark-spark/src/tests

## Purpose

Lib-root unit battery for the Spark SQL door (the former `src/tests.rs` monolith, split in
G-4 as a **declared-rename** unit under `docs/testing.md` "Relocation discipline"). Production
code is not here — only tests, shared fixtures, and the module manifest.

## Contents

- `mod.rs` — pure module manifest (`mod common;` + one `mod` per leaf).
- `common.rs` — shared fixtures (`setup`, `rows`, `run`, `register_source`, `table_rows`, …)
  and the cross-cutting helpers that more than one leaf needs (`time_travel_id_multiset`,
  `execute_without_collecting`, unsafe-cast walk helpers). Bodies moved byte-identically from
  the monolith; `pub(super)` visibility + type re-exports for leaf `use super::common::*;`.
- **Production-aligned leaves** (flat tests gained one path segment `tests::<leaf>::…`):
  `ctas`, `create_table`, `namespace_ddl`, `catalog_ops`, `describe_show`, `alter`, `dml`
  (DELETE/UPDATE + BUG-001 valve; no production `delete`/`update` module), `insert_overwrite`,
  `merge`, `call`, `ref_ddl`, `time_travel`, `metadata_tables`, `normalize`, `local_fs_ddl`,
  `router` (multi-statement, F-BR-2 eager DML, TRUNCATE refuse).
- **Path-preserving sibling lifts** (former nested `mod`s; cargo paths unchanged):
  `partitioned_ctas`, `partitioned_merge`, `transform_overwrite` (still nests
  `provider_partition_correctness`), `service_managed_ctas`.

## Mapping rule

1. Production-module alignment by name / primary assertion.
2. Two-module tests follow the primary assertion; genuine cross-cutting residue gets a small
   shared home (`dml`, `common`).
3. Nested mods lift to sibling files of the same name (path-preserving).
4. Helpers move byte-identically; only new lines are `mod` decls and `use` adjustments.

Authoritative membership + margin rulings: unit ledger `task/g4-tests-split-ledger.md` and
planning cut map `G4-CUT-MAP.md`. Generated name map: `task/g4-artifacts/name-map.md`.

## Pointers

- Up: [../map.md](../map.md)
- Registry pins: [../../../../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md)
- Surface matrix (also `#[cfg(test)]`): [../matrix.rs](../matrix.rs)

## Debug

| Symptom | First check |
|---|---|
| Identity / rename doubt | `cargo test -p repark-spark --lib -- --list` vs `task/g4-artifacts/`; leaf multiset must match; full paths follow the name map |
| Matrix pin string red / stale | `matrix.rs` strings are `--list` names; update with renames only — never re-point a surface |
| Registry pin path stale | filesystem form `crates/repark-spark/src/tests/<file>.rs::leaf` |
| Missing `TempDir` / Arrow types in a leaf | `use super::common::*;` (shared re-exports live in `common.rs`) |
| Cross-leaf helper not found | should be `pub(super)` in `common.rs` (or wrongly left private in one leaf) |
| Nested partition pin fails | `partitioned_ctas` / `partitioned_merge` / `transform_overwrite` — manifest-level `DataFile.partition` oracles |

First checks: `cargo test -p repark-spark tests::<module>::`. Escalate to: [../map.md#debug](../map.md).
