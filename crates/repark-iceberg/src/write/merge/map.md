# map — repark-iceberg/src/write/merge/

## Purpose

Spark `MERGE INTO` adapter (copy-on-write + merge-on-read). The former `merge.rs` monolith
lives as this module directory (move-only; pub surface frozen).

## Contents

- `mod.rs` — types, `execute_merge`, plan/SQL helpers, write/commit path.
  `commit_overwrite` / `commit_row_delta_kind` are `pub(super)` so identity DML
  (`../predicate_dml.rs`) reuses the COW/MoR commit arms without calling
  `execute_merge`. MERGE SQL still goes through `commit` / `commit_row_delta`
  (serializable MERGE recipe; tests stay identity-diff).
- `tests.rs` — primary unit battery
- `occ_tests.rs` — OCC / commit conflict pins
- `streaming_tests.rs` — stream write interleaving pins
- `parallel_write_tests.rs` — concurrent file write pins
- `streaming_scan_tests.rs` — streaming target-scan pins

## I want to…

| Task | Go to |
|---|---|
| Change MERGE execute / MoR-CoW arms | `mod.rs` |
| Add a unit pin for SQL shape | `tests.rs` |
| Touch OCC commit behavior | `occ_tests.rs` |

## Pointers

Up: [../map.md](../map.md). Fork contract: `docs/ENGINE_CONTRACT.md` (owned fork).

## Debug

- `--list` paths must stay `write::merge::<battery>::<test>` — identity gate for the
  declared-rename census.
- Pub `write_data_files*` re-exported from the write module root (`../mod.rs`) and the crate
  root (`lib.rs`).
