# Unit ledger — ICE-COMMIT-UNKNOWN-1 · an ambiguous commit gets its own exception class (G-6)

**Date:** 2026-09-14 · **Branch:** `feat/ice-commit-unknown-1` · **Base:** `origin/main`
**Model:** swe-2-high · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) · **Path:** STANDARD.
**Card:** [../../../briefs/](../../../briefs/) ICE-COMMIT-UNKNOWN-1 — assessment row K3, fork
GAP_MATRIX R157.

**Gap.** `crates/repark-core/src/error_map.rs` `classify_iceberg_error` folds
`ErrorKind::CommitStateUnknown` into the generic `Error::Iceberg`, so the caller sees a base
`PySparkException` and cannot tell an ambiguous commit (the catalog may have applied it) from a
definite failure. Airflow can neither alert on the ambiguous case nor restrict its retry to
MERGE and CTAS IF NOT EXISTS (inventory §8 ruling 8); a retried append duplicates rows.

**Decisions (from the card).** D-1: a new native `CommitStateUnknownException(PySparkException)`
in `crates/repark-python/src/exceptions.rs`, registered on `repark._native`, re-exported from
`repark.errors` (`__all__` included); every `except PySparkException` / `except RuntimeError`
still catches it. D-2: `classify_iceberg_error` maps `ErrorKind::CommitStateUnknown` to a new
`Error::CommitStateUnknown { message, operation_id }` variant and a new
`ErrorClass::CommitStateUnknown`; `to_py_err` raises the new class and sets an `operation_id`
attribute on the instance (`None` when unknown); the message text stays byte-identical. D-3: at
each RePark commit site that mints `engine.operation-id`, the minted id is attached to a
`CommitStateUnknown` failure on the way out through a small wrapper error in repark-iceberg that
`error_map` also downcasts. D-4: the class name `CommitStateUnknownException` and the attribute
name `operation_id` are fixed by the brief; nothing else public changes.

**Mechanism chosen for D-3.** `crates/repark-iceberg/src/write/commit_error.rs` holds
`CommitStateUnknownError { inner: iceberg::Error, operation_id: String }` (an `std::error::Error`
whose `Display` delegates to the inner error, keeping the message byte-identical and `source()`
the live chain) plus `commit_err(error, operation_id)`, which boxes that wrapper only when
`error.kind() == ErrorKind::CommitStateUnknown` and otherwise returns the same
`DataFusionError::External(Box<iceberg::Error>)` the local `iceberg_err` helpers return today.
A second helper `operation_id_and_summary()` mints the `Uuid` and the stamped summary map in one
place so the id attached to the error cannot drift from the id the commit stamps. This is the
smallest mechanism that keeps `error_map`'s downcast working: `External` first downcasts to
`CommitStateUnknownError` (stamped RePark commit), then to `iceberg::Error` (every other path —
fork-provider DML, DDL commits, non-minting sites — which carries `operation_id: None`).

## PROPOSITION LEDGER — ICE-COMMIT-UNKNOWN-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `rust_classification`: `ErrorKind::CommitStateUnknown` classifies to `Error::CommitStateUnknown { message, operation_id }` → `ErrorClass::CommitStateUnknown` through BOTH the `DataFusionError::External` peel and the direct `iceberg_err` fold, with the rendered message byte-identical to today's `Error::Iceberg` text. | The repark-common routing pin and the repark-core classification pins green; red-first output pasted. | PROVEN | `cargo test -p repark-common`: 13/13 green incl. `exception_class_routes_every_variant`; `cargo test -p repark-core commit_unknown`: 3/3 green (`commit_state_unknown_classifies_to_dedicated_variant_on_both_routes` covers the External-peel and direct-fold routes and asserts the message text). Red output in §Red first. |
| C-002 | `python_class`: `CommitStateUnknownException` subclasses `PySparkException` (hence `RuntimeError`), is registered on `repark._native`, is re-exported from `repark.errors` by identity and listed in `__all__`, and `to_py_err` sets an `operation_id` attribute on the instance (`str` when stamped, `None` when not). | The repark-python `to_py_err` pin green; the facade hierarchy/identity pins green after `make develop`. | PROVEN | `cargo test -p repark-python --lib`: 75/75 green incl. `to_py_err_commit_state_unknown_is_typed_and_carries_operation_id` (instance of the new class, of `PySparkException`, of `PyRuntimeError`; `operation_id` = stamped str / `None`). Facade: `test_errors.py` hierarchy + `is _native.` identity pins green under the `-k "error or exception or commit"` run (337 passed). No facade-level catalog-injection hook exists, so the end-to-end Python pin is the `to_py_err` unit pin plus the facade taxonomy pins — per the card's fallback. |
| C-003 | `operation_id_carried`: a MERGE commit whose `update_table` answers `CommitStateUnknown` surfaces the new class carrying the `engine.operation-id` the commit attempted to stamp (captured from the `TableCommit` the catalog wrapper received), through the repark-iceberg merge commit path. | The delegating-catalog pin in `merge/tests/commit_unknown.rs` green; red-first output pasted. | PROVEN | `cargo test -p repark-iceberg commit_unknown`: 2/2 green — `merge_overwrite_commit_unknown_surfaces_the_stamped_operation_id` and `merge_row_delta_commit_unknown_surfaces_the_stamped_operation_id` both capture `engine.operation-id` from the `TableCommit`'s `AddSnapshot` update, return `CommitStateUnknown` from `update_table`, and assert the surfaced `CommitStateUnknownError::operation_id()` equals the captured id while `update_table` attempts == 1. Red output in §Red first. |
| C-004 | `definite_failures_unchanged`: a definite `CatalogCommitConflicts` still classifies to `Error::Iceberg` → `ErrorClass::Base` → `PySparkException` — on the plain `iceberg::Error` route AND on the stamping commit route (`commit_err` does not tag a definite failure); `PreconditionFailed` / `Unexpected` / `DataInvalid` stay `Error::Iceberg`. | The existing partition pins stay green plus the `commit_err` conflict leg in the repark-core pin. | PROVEN | `commit_err_leaves_definite_kinds_in_the_base_bucket` (repark-core) green: `commit_err` returns the plain `DataFusionError::External` for `CatalogCommitConflicts`, and `engine_err` still maps it to `Error::Iceberg`; the surviving `base_kinds` battery in `session/tests/session.rs` (PreconditionFailed/Unexpected/DataInvalid/CatalogCommitConflicts, minus the moved CommitStateUnknown leg) passes inside the 379-test repark-core run. |
| C-005 | `statement_shapes`: the ledger and the registry row list which statement shapes carry an operation id (MERGE both arms, repark predicate DML, `INSERT OVERWRITE` all three forms, `TRUNCATE`, CTAS/append commits through `commit_append`) and which do not (fork-provider `INSERT INTO` / `DELETE` / `UPDATE`, non-data DDL commits — ALTER, TBLPROPERTIES, BRANCH/TAG). | The table in §Shapes matches the mint sites grep-verified; the parity row lists the same. | PROVEN | §Shapes table filled from the caller grep: `commit_append` callers are the two CTAS doors + the CTAS write node + `append()`; `snapshot_commit.rs` serves MERGE and predicate DML; `overwrite_commit.rs` serves `INSERT OVERWRITE` + `TRUNCATE`; `partition_overwrite.rs` serves both PARTITION forms. `INSERT INTO` routes `passthrough_after_p11` to the fork provider (no stamp); non-allowlisted DELETE/UPDATE delegate likewise. The parity row's Statement-shapes bullet mirrors the table. |
| C-006 | `registry_and_inventory`: `docs/spark-sql-iceberg-parity.md` carries an `ICE-COMMIT-UNKNOWN-1` row near the commit/concurrency rows (what raises, the `operation_id` attribute, which shapes carry an id, the Airflow guidance) and `docs/cutover/inventory.md` §8 ruling 8's follow-through cell names the class and attribute. | Both doc edits land; `make check-docs-links` green. | PROVEN | `#### ICE-COMMIT-UNKNOWN-1` row added after DML-5 in §2.3 (what raises, the attribute, the statement-shapes bullet, the Airflow alert/retry guidance); inventory §8 ruling 8's follow-through cell now names `CommitStateUnknownException` and `exc.operation_id`. check-docs-links result in §Gates. |
| C-007 | `mutation_proof`: routing `CommitStateUnknown` back to `Error::Iceberg` in a scratch edit turns the new pins red; restoring turns them green. | The red run output pasted. | PROVEN | Scratch edit restored the whole old pipeline — `engine_err`'s stamped arm and `classify_iceberg_error`'s kind arm both folded to `Error::Iceberg`, and `exception_class` routed the variant to `ErrorClass::Base`. Reds: repark-core 2/3 (`commit_state_unknown_classifies_to_dedicated_variant_on_both_routes`: "expected Error::CommitStateUnknown, got Iceberg(…)"; `stamped_commit_unknown_carries_the_minted_operation_id`: same; the definite-kinds pin stayed green — it pins the negative), repark-common `exception_class_routes_every_variant` ("left: Base, right: CommitStateUnknown"), repark-python `to_py_err_commit_state_unknown_is_typed_and_carries_operation_id` ("is_instance_of::<CommitStateUnknownException>" failed). The repark-iceberg wrapper pin does not red under this mutant by design — it pins the stamping layer below error_map (a mutant that skips `commit_err` reds it instead). Restored; all pins green again. |
| C-008 | `gates_green`: the card's gate list passes on this branch. | Counts pasted into §Gates. | PROVEN | §Gates: all named cargo filters green (13/13, 3/3+14, 2/2, 75/75), `make develop` + facade `-k` run 337/7-skipped, `make verify` green end to end, docs-links 5431 clean, ledger-grammar + check-ledgers clean, parity suite 757/2-skipped/12-xfailed, size ceilings ratcheted down only, fence shows attribute tokens only. |

VERDICT: 8 clauses, 8 PROVEN, 0 OPEN, 0 REJECTED.

## Shapes — which commits carry `engine.operation-id`

Verified by caller grep (2026-09-14): the six RePark commit sites that mint the stamp, and the
fork-owned commits that mint none.

| Statement shape | Commit site | `operation_id` |
|---|---|---|
| CTAS (both doors: `create_table.rs`, `ctas.rs`, the `partition_write.rs` CTAS node) and the repark `append()` bulk path | `commit_append` | minted |
| `MERGE` — `commit_overwrite` and `commit_row_delta_kind` arms; repark identity DELETE/UPDATE via `execute_predicate_dml` (commits through the same MERGE arms) | `snapshot_commit.rs` | minted |
| `INSERT OVERWRITE` whole-table; `TRUNCATE` (`commit_truncate_to` delegates) | `commit_overwrite_replace_all_to` | minted |
| `INSERT OVERWRITE … PARTITION` static and dynamic | `partition_overwrite.rs` (two sites) | minted |
| `INSERT INTO` (DataFusion passthrough → fork `IcebergTableProvider::insert_into`) | fork | `None` |
| Non-allowlisted `DELETE`/`UPDATE` (fork `iceberg-datafusion` TableProvider) | fork | `None` |
| Non-data DDL commits (CREATE/ALTER/BRANCH/TAG, TBLPROPERTIES) | catalog calls | `None` |

## Red first

Pins written before any implementation; every one failed on the base tree (the new names do not
exist yet — the failure IS the classification gap the card names).

- `cargo test -p repark-common` → `error[E0599]: no variant named CommitStateUnknown found for
  enum Error` (tests.rs:35) and `no variant, associated function, or constant named
  CommitStateUnknown found for enum ErrorClass` (tests.rs:40).
- `cargo test -p repark-core commit_unknown` → `error[E0432]: unresolved import
  repark_iceberg::write::commit_err — no commit_err in write`; `error[E0599]: no variant named
  CommitStateUnknown found for enum repark_common::Error` (commit_unknown.rs:26, :53);
  `error[E0599]: no variant ... named CommitStateUnknown found for enum
  error_map::EngineErrorKind<'a>` (commit_unknown.rs:49).
- `cargo test -p repark-iceberg commit_unknown` → `error[E0432]: unresolved import
  crate::write::CommitStateUnknownError — no CommitStateUnknownError in write`
  (merge/tests/commit_unknown.rs:19).
- `cargo test -p repark-python --lib` → `error[E0425]: cannot find type
  CommitStateUnknownException in this scope` (tests.rs:78, :98); `error[E0599]: no variant
  named CommitStateUnknown found for enum repark_core::Error` (tests.rs:74, :94).
- `.venv/bin/python -m pytest python/repark/tests/test_errors.py -q -k "hierarchy or
  reexported"` → `ImportError: cannot import name 'CommitStateUnknownException' from
  'repark.errors'` at collection.

## Gates

- `cargo test -p repark-common` — 13/13 green.
- `cargo test -p repark-core commit_unknown` — 3/3 green; `cargo test -p repark-core error` —
  14/14 green under the card's filter.
- `cargo test -p repark-iceberg commit_unknown` — 2/2 green (both MERGE commit arms).
- `cargo test -p repark-python --lib` — 75/75 green.
- `make develop` — maturin editable rebuild clean; `.venv/bin/python -m pytest
  python/repark/tests -q -k "error or exception or commit"` — 337 passed, 7 skipped.
- `make verify` — green end to end: fmt, workspace clippy (`-D warnings`), panic-ban clippy,
  crate-dag, lib-rs, rust-file-size (503 files, 36 exceptions — `session/tests/session.rs`
  ratcheted 1412 → 1407), lib-py (685 files), python-conventions, docstring-presence,
  example-coverage, manifest, check-ledgers, check-ledger-grammar, docs-compaction,
  docs-links (5431 links), owner-ruling, parity-live dual-wire, matrix-test-liveness,
  cargo check, ruff, taplo, typos, `cargo test --workspace`.
- `make check-docs-links` — 850 files, 5431 links clean.
- `make check-ledger-grammar` — 132 live ledgers clean.
- `make check-ledgers` — 350 ledgers, 963 links resolve.
- Parity suite `pytest python/repark-parity/tests -q -p no:cacheprovider` — 757 passed, 2
  skipped, 12 xfailed. The first run caught the intended additive move
  (`test_api_freeze`: `CommitStateUnknownException` joins `errors.py`'s `__all__`; D-4 fixes
  the name public); `docs/design/v1-0-api-freeze.json` was regenerated (`frozen_names`
  888 → 889) with the pin's expected count in the same change.
- Comment fence `git diff --cached -- '*.rs' '*.py' '*.toml' '*.sh' '*.yml' | grep -P
  '^\+\s*(//|#(?! noqa))'` — nine hits, all `#[...]` attribute tokens (`#[test]`,
  `#[tokio::test]`, `#[derive(Debug)]`, `#[error("{message}")]`); zero comment lines added.
- Size ceilings — `session/tests/session.rs` ratchet DOWN 1412 → 1407 recorded in
  `scripts/check_rust_file_size.py` and the CAP-1 mirror in the same change; `append.rs`
  holds its exact 1882 baseline net-zero; `errors.py` grew 4 lines under its ceiling.
- Mutation (C-007) — reds pasted in the clause; all pins green again after restore.
