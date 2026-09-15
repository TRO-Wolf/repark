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
| C-003 | `operation_id_carried`: a MERGE commit whose `update_table` answers `CommitStateUnknown` surfaces the new class carrying the `engine.operation-id` the commit attempted to stamp (captured from the `TableCommit` the catalog wrapper received), through the repark-iceberg merge commit path. | The delegating-catalog pin in `merge/tests/commit_unknown.rs` green; red-first output pasted. | PROVEN | `cargo test -p repark-iceberg commit_unknown`: 2/2 green — `merge_overwrite_commit_unknown_surfaces_the_stamped_operation_id` and `merge_row_delta_commit_unknown_surfaces_the_stamped_operation_id` both capture `engine.operation-id` from the `TableCommit`'s `AddSnapshot` update, return `CommitStateUnknown` from `update_table`, and assert the surfaced `CommitStateUnknownError::operation_id()` equals the captured id while `update_table` attempts == 1. Red output in §Red first. Critic round: the service-managed CTAS pin in `service_managed_ctas.rs` proves the same stamped id reaches `Error::CommitStateUnknown` through `engine_err` end to end, and the two `*_leaves_written_files_on_disk` pins prove the abort keep-set (files stay on disk). |
| C-004 | `definite_failures_unchanged`: a definite `CatalogCommitConflicts` still classifies to `Error::Iceberg` → `ErrorClass::Base` → `PySparkException` — on the plain `iceberg::Error` route AND on the stamping commit route (`commit_err` does not tag a definite failure); `PreconditionFailed` / `Unexpected` / `DataInvalid` stay `Error::Iceberg`. | The existing partition pins stay green plus the `commit_err` conflict leg in the repark-core pin. | PROVEN | `commit_err_leaves_definite_kinds_in_the_base_bucket` (repark-core) green: `commit_err` returns the plain `DataFusionError::External` for `CatalogCommitConflicts`, and `engine_err` still maps it to `Error::Iceberg`; the surviving `base_kinds` battery in `session/tests/session.rs` (PreconditionFailed/Unexpected/DataInvalid/CatalogCommitConflicts, minus the moved CommitStateUnknown leg) passes inside the 379-test repark-core run. Critic round: `ctas_service_managed_commit_failure_drops_the_created_table` stays green — a definite `Unexpected` still abort-drops with the same `create-first abort` message on both doors' code path (the unknown guard only fires on the ambiguous kind). |
| C-005 | `statement_shapes`: the ledger and the registry row list which statement shapes carry an operation id (MERGE both arms, repark predicate DML, `INSERT OVERWRITE` all three forms, `TRUNCATE`, service-managed CTAS/append commits through `commit_append`) and which do not (staged CTAS/REPLACE through `StagedTableTransaction` — Glue and other warehouse catalogs, fork-provider `INSERT INTO` / `DELETE` / `UPDATE`, non-data DDL commits — ALTER, TBLPROPERTIES, BRANCH/TAG). | The table in §Shapes matches the mint sites grep-verified; the parity row lists the same. | PROVEN | §Shapes table filled from the caller grep and corrected by the critic round: `commit_append` callers are the service-managed CTAS arms + `append()`; `snapshot_commit.rs` serves MERGE and predicate DML; `overwrite_commit.rs` serves `INSERT OVERWRITE` + `TRUNCATE`; `partition_overwrite.rs` serves both PARTITION forms. The staged path (`StagedTableTransaction::commit` → `publish_create_table`/`publish_replace_table`) mints no id at fork `edc38c6a` — pinned `None` in `ctas_staged_commit_state_unknown_surfaces_class_without_operation_id`. `INSERT INTO` routes `passthrough_after_p11` to the fork provider (no stamp); non-allowlisted DELETE/UPDATE delegate likewise. The parity row's Statement-shapes bullet mirrors the table. |
| C-006 | `registry_and_inventory`: `docs/spark-sql-iceberg-parity.md` carries an `ICE-COMMIT-UNKNOWN-1` row near the commit/concurrency rows (what raises, the `operation_id` attribute, which shapes carry an id, the Airflow guidance) and `docs/cutover/inventory.md` §8 ruling 8's follow-through cell names the class and attribute. | Both doc edits land; `make check-docs-links` green. | PROVEN | `#### ICE-COMMIT-UNKNOWN-1` row added after DML-5 in §2.3 (what raises, the attribute, the statement-shapes bullet, the Airflow alert/retry guidance); inventory §8 ruling 8's follow-through cell now names `CommitStateUnknownException` and `exc.operation_id`. check-docs-links result in §Gates. |
| C-007 | `mutation_proof`: routing `CommitStateUnknown` back to `Error::Iceberg` in a scratch edit turns the new pins red; restoring turns them green. | The red run output pasted. | PROVEN | Scratch edit restored the whole old pipeline — `engine_err`'s stamped arm and `classify_iceberg_error`'s kind arm both folded to `Error::Iceberg`, and `exception_class` routed the variant to `ErrorClass::Base`. Reds: repark-core 2/3 (`commit_state_unknown_classifies_to_dedicated_variant_on_both_routes`: "expected Error::CommitStateUnknown, got Iceberg(…)"; `stamped_commit_unknown_carries_the_minted_operation_id`: same; the definite-kinds pin stayed green — it pins the negative), repark-common `exception_class_routes_every_variant` ("left: Base, right: CommitStateUnknown"), repark-python `to_py_err_commit_state_unknown_is_typed_and_carries_operation_id` ("is_instance_of::<CommitStateUnknownException>" failed). The repark-iceberg wrapper pin does not red under this mutant by design — it pins the stamping layer below error_map (a mutant that skips `commit_err` reds it instead). Restored; all pins green again. |
| C-008 | `gates_green`: the card's gate list passes on this branch. | Counts pasted into §Gates. | PROVEN | §Gates: all named cargo filters green (13/13, 3/3+14, 2/2, 75/75), `make develop` + facade `-k` run 337/7-skipped, `make verify` green end to end, docs-links 5431 clean, ledger-grammar + check-ledgers clean, parity suite 757/2-skipped/12-xfailed, size ceilings ratcheted down only, fence shows attribute tokens only. |

VERDICT: 8 clauses, 8 PROVEN, 0 OPEN, 0 REJECTED.

## Critic round (Grok 4.6 critic-logic) — findings and disposition

The orchestrator ran a critic-logic review of `b4456c94`
(`/tmp/oc-worker/i-ccrit/report.md`, verdict NEEDS_REMEDIATION: one P1, two P2, three P3).
Every finding is dispositioned below; the only fork-touching residue is named for the owner
under L-003.

| Finding | Disposition |
|---|---|
| L-001 (P1) — service-managed CTAS drops the table and swallows the class on `CommitStateUnknown` (`ctas.rs:450`, `create_table.rs:447`) | REMEDIATED. `is_commit_state_unknown(&DataFusionError)` in `commit_error.rs` detects the kind before any wrapping (stamped `CommitStateUnknownError` or bare `iceberg::Error` of that kind, through the same `Context`/`Diagnostic`/`Shared`/`Collection` peel `error_map` uses). Both doors return the original error unwrapped and skip `drop_table` on the ambiguous kind only; definite kinds keep the drop-and-explain message byte-identical. Pins: `ctas_service_managed_commit_state_unknown_keeps_table_and_surfaces_class` (Spark door: `drop_table_calls()==0`, table still exists, `engine_err` → `CommitStateUnknown` with the captured minted id) and the native-door twin `service_managed_ctas_commit_state_unknown_keeps_table_and_surfaces_class` — both red on the base tree (`drop_table_calls()==1`); the definite-kind pin `ctas_service_managed_commit_failure_drops_the_created_table` stays green. |
| L-002 (P2) — the MERGE keep-set is not mutation-proof | REMEDIATED. `merge_overwrite_commit_unknown_leaves_written_files_on_disk` and `merge_row_delta_commit_unknown_leaves_written_files_on_disk` stage REAL Parquet files through `write_data_files` (`occ_conflict` helpers promoted `pub(super)`), inject `CommitStateUnknown`, and assert `path_exists` afterwards. Mutation proof: deleting the `kind() == CommitStateUnknown` early return in `abort.rs` reds both pins ("a possibly-committed file must stay on disk"); the two id pins stay green (different layer); restore greens all four. |
| L-003 (P2) — Glue (staged) CTAS mints no `operation_id` | DOCUMENTED + PINNED; fork untouched per the card. The registry row, §Shapes, C-005 and inventory §8 now say service-managed CTAS carries an id via `commit_append` while staged CTAS/REPLACE (Glue, warehouse catalogs) carries `None`. Pin `ctas_staged_commit_state_unknown_surfaces_class_without_operation_id`: `publish_create_table` returns `CommitStateUnknown` → `Error::CommitStateUnknown { operation_id: None }`, `create_table`/`drop_table` never called. **Residue for the owner:** stamping `engine.operation-id` on the staged path needs a fork change — `StagedTableTransaction::commit` carries no snapshot properties at `edc38c6a` (`staged_table.rs` `materialize_pending`/`fast_append`). |
| L-004 (P3) — `to_py_err` setattr failure replaces the class | REMEDIATED. On `setattr("operation_id", …)` `Err` the arm now returns `raised` — always the `CommitStateUnknownException` — with the setattr failure carried by `tracing::warn!`; the class can no longer be demoted by the attribute write. |
| L-005 (P3) — unit date is 2026-09-14; several maps stamp 2026-09-15 | RESOLVED before this round: the orchestrator's docs commit `36461427` dated the unit 2026-09-14 across the maps, the registry and this ledger; a re-grep shows no remaining 2026-09-15 stamps. |

## Shapes — which commits carry `engine.operation-id`

Verified by caller grep (2026-09-14): the six RePark commit sites that mint the stamp, and the
fork-owned commits that mint none.

| Statement shape | Commit site | `operation_id` |
|---|---|---|
| Service-managed CTAS (`LocationPolicy::ServiceManagedLocation` — S3 Tables; both doors' create-first paths) and the repark `append()` bulk path | `commit_append` | minted |
| `MERGE` — `commit_overwrite` and `commit_row_delta_kind` arms; repark identity DELETE/UPDATE via `execute_predicate_dml` (commits through the same MERGE arms) | `snapshot_commit.rs` | minted |
| `INSERT OVERWRITE` whole-table; `TRUNCATE` (`commit_truncate_to` delegates) | `commit_overwrite_replace_all_to` | minted |
| `INSERT OVERWRITE … PARTITION` static and dynamic | `partition_overwrite.rs` (two sites) | minted |
| Staged CTAS/REPLACE on warehouse catalogs (Glue — `StagedTableTransaction` publishes through `publish_create_table` / `publish_replace_table`; the fork's staged `fast_append` carries no snapshot properties at `edc38c6a`) | fork staged publish | `None` |
| `INSERT INTO` (DataFusion passthrough → fork `IcebergTableProvider::insert_into`) | fork | `None` |
| Non-allowlisted `DELETE`/`UPDATE` (fork `iceberg-datafusion` TableProvider) | fork | `None` |
| Non-data DDL commits (CREATE/ALTER/BRANCH/TAG, TBLPROPERTIES) | catalog calls | `None` |

Ambiguous-commit cleanup keep-set (both observed in pins): a service-managed CTAS does NOT
`drop_table` on `CommitStateUnknown` (the create may have landed) and returns the original
error unwrapped; a MERGE leaves its written data files on disk. Definite kinds keep the old
abort behaviour exactly (drop + `Execution` wrap; best-effort file delete).

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

Round 2 (critic remediation) reds, before the `is_commit_state_unknown` guard existed:

- `cargo test -p repark-spark service_managed` →
  `ctas_service_managed_commit_state_unknown_keeps_table_and_surfaces_class` FAILED:
  `assertion left == right failed: an ambiguous commit is NOT abort-dropped — left: 1,
  right: 0` (the base tree dropped the created table and returned `DataFusionError::Execution`).
- `cargo test -p repark-sql --lib service_managed` →
  `service_managed_ctas_commit_state_unknown_keeps_table_and_surfaces_class` FAILED:
  same `left: 1, right: 0` on the native door.
- The staged pin `ctas_staged_commit_state_unknown_surfaces_class_without_operation_id`
  passed on the base tree — the staged path already carries the bare kind through
  `iceberg_err`; it is a regression pin on the L-003 correction, not a new behavior.
- The keep-set pins passed on the base tree (the `abort.rs` carve-out already exists);
  their red is the §Critic-round mutation (carve-out deleted → both red).

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

Round 2 (critic remediation) gates — see §Critic round for the reds:

- `cargo test -p repark-spark service_managed` — 9/9 green (incl. the two new CTAS pins;
  the definite-kind drop pin unchanged).
- `cargo test -p repark-sql service_managed_ctas_commit_state_unknown` — 1/1 green
  (native-door unknown pin).
- `cargo test -p repark-iceberg commit_unknown` — 4/4 green (two id pins + two keep-set
  pins; `occ_conflict.rs` held at its exact 1023 baseline — `pub(super)` re-marks only).
- `cargo test -p repark-core --lib commit_unknown` — 3/3 green; `cargo test -p
  repark-common` — 13/13; `cargo test -p repark-python --lib` — 75/75.
- `make develop` → `.venv/bin/python -m pytest python/repark/tests -q -k "error or
  exception or commit or ctas"` — 404 passed, 13 skipped.
- `make verify` — green end to end (clippy caught `#[must_use]` on the new helper —
  added; fmt/clippy/panic-ban/file-size/manifest/ledgers/grammar/links/workspace tests
  all clean).
- Parity suite — 757 passed, 2 skipped, 12 xfailed.
- Comment fence — six hits, all `#[...]` attribute tokens; the `///`/`//` lines the
  round-2 pins first carried were removed (attributes are code, comments are not).
- Mutation (L-002) — `abort.rs` unknown early-return deleted → both keep-set pins red;
  restored → 4/4 green.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-commit-unknown-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against measured behavior — classification on both routes, the stamped id end to end on both MERGE arms and the service-managed CTAS door, definite kinds unchanged, the statement-shape table grep-verified and corrected by the critic round, registry + inventory cells name the class/attribute, and L-001..L-005 are all dispositioned with pins.
      artifacts: [task/ledgers/staging/ice-commit-unknown-1-ledger.md, crates/repark-core/src/session/tests/commit_unknown.rs, crates/repark-spark/src/tests/service_managed_ctas.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary inputs exercised — stamped vs unstamped unknown (Some vs None operation_id), the wrapper vs a bare iceberg::Error, DataFusion wrapper peels (Context/Diagnostic/Shared/Collection), an empty write_result (no files -> no commit -> no unknown), and the staged publish arm which mints no id.
      artifacts: [crates/repark-core/src/session/tests/commit_unknown.rs, crates/repark-iceberg/src/write/commit_error.rs, crates/repark-spark/src/tests/service_managed_ctas.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The failure modes are the unit — a lost UpdateTable response surfaces the class with the minted id; the service-managed CTAS abort keeps the table on unknown while definite kinds still drop with the same message; MERGE abort leaves written files on disk under unknown; exactly one update_table attempt (the fork never retries this kind).
      artifacts: [crates/repark-iceberg/src/write/merge/tests/commit_unknown.rs, crates/repark-iceberg/src/write/merge/abort.rs, crates/repark-spark/src/tests/service_managed_ctas.rs]
    - id: AT-4
      status: ATTACKED
      evidence: update_table_attempts == 1 pinned on both MERGE arms — the ambiguous kind is never retried even when retryable(); one UUID minted per commit call and threaded by value, so no shared mutable state; the fork's reload-reconcile ordering is unchanged (raw iceberg::Error reaches the abort before commit_err wraps).
      artifacts: [crates/repark-iceberg/src/write/merge/tests/commit_unknown.rs, crates/repark-iceberg/src/write/commit_error.rs]
    - id: AT-5
      status: N/A
      justification: No credential, injection, deserialization or privileged-action surface — a new error variant, an exception subclass and one string attribute.
    - id: AT-6
      status: ATTACKED
      evidence: Data-integrity keep-set proven — a possibly-committed table is not dropped and possibly-committed files are not deleted on an ambiguous outcome (both mutation-proven); backward compatibility pinned: the class subclasses PySparkException so every existing except still catches it, and str(exc) is the byte-identical iceberg message.
      artifacts: [crates/repark-spark/src/tests/service_managed_ctas.rs, crates/repark-iceberg/src/write/merge/tests/commit_unknown.rs, python/repark/tests/test_errors.py]
    - id: AT-7
      status: N/A
      justification: A boxed error variant and one UUID per failed commit — no hot-path growth, no unbounded structure.
    - id: AT-8
      status: ATTACKED
      evidence: Interface contracts honored — no fork change; the staged-publish id gap is a documented fork residue (named for the owner in §Critic round L-003); the api-freeze register was regenerated for the single intended additive name; PyO3 conversion returns the dedicated class even when the attribute setattr fails.
      artifacts: [docs/design/v1-0-api-freeze.json, crates/repark-python/src/lib.rs, docs/spark-sql-iceberg-parity.md]
    - id: AT-9
      status: ATTACKED
      evidence: The class is the operability hook — Airflow alerts on CommitStateUnknownException and reads exc.operation_id; the MERGE abort warns via tracing::warn! when it skips cleanup; the setattr failure path warns rather than silently degrading.
      artifacts: [crates/repark-python/src/lib.rs, crates/repark-iceberg/src/write/merge/abort.rs, docs/cutover/inventory.md]
    - id: AT-10
      status: ATTACKED
      evidence: Two mutation proofs went red — folding the kind/class back to the old pipeline reds pins at three layers; deleting the abort.rs unknown carve-out reds both keep-set pins; the CTAS unknown pins were red-first on the base tree (drop_table_calls()==1).
      artifacts: [task/ledgers/staging/ice-commit-unknown-1-ledger.md, crates/repark-iceberg/src/write/merge/tests/commit_unknown.rs]
  complete: true
```

## S2-21 performance review (orchestrator, 2026-09-14)

Grok 4.6 read-only reviewer over `ac057eb2` (fresh clone): CLEAN, no P1 or P2. Every new branch runs on the
commit-failure or Python-raise path; `commit_result` is a no-op `map_err` on success; the extra `error_map` downcast
runs only for a failed statement's `DataFusionError::External`; `repark.errors` import grows by about 310 ns.

| Finding | Severity | Disposition |
|---|---|---|
| One extra 36-byte UUID `String` clone per successful commit in `operation_id_and_summary` (`commit_error.rs`) | P3 | Recorded; keep the `Uuid` on the stack and format it only into the summary map and on the error arm if a later unit touches the file |

