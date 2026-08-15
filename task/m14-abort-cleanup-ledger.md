# Unit ledger — M14 abort-path cleanup (rejected MERGE commit)

**Unit:** M14 · conductor-14 T5 · **Date:** 2026-08-15 ·
**Lane:** repark · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-m14` · **Branch:** `grok/m14-abort-cleanup` ·
**Base (FROZEN):** `a5d2d98a449815891016923594c8b1dcd4ae3b43`

**Charter:** conductor-14 T5 + Addendum A7/A8 (owner-ratified design A).
**SEPMO:** HIGH — octo + C4. Floor S1. Sequential hat-switch Actor → C1 → C2
→ C3 → C4.

This ledger does **not** edit `docs/spark-sql-iceberg-parity.md`,
`STATUS.md`, `predicate_dml.rs`, lockfiles, `.github/`, or `[patch]`.
BL-5 registry flip is orchestrator-side after SQM.

### Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | On `tx.commit` `Err`, best-effort `FileIO::delete` every file this attempt wrote, then re-raise the original error. | PROVEN — `commit_overwrite` + `commit_row_delta_kind` |
| C-002 | Delete set is threaded from writer results in hand (`new_files` / `data_files` / `delete_files` from a successful `write_position_deletes`). Never re-derived from the table or manifests (S1). | PROVEN — `abort::written_file_paths` before the `Vec` moves |
| C-003 | Cleanup runs ONLY on the `tx.commit` error path. A catch that can fire after a successful commit is a HALT. | PROVEN — `match` `Ok => Ok` / `Err => delete then original` |
| C-004 | Do not delete `affected` / referenced existing data files. | PROVEN — overwrite abort uses `new_file_paths` only; reject pin asserts live `test/a.parquet` |
| C-005 | Per-file delete failures `tracing::warn` naming the path; never mask the original commit error. | PROVEN — helper returns `()`; `delete_failure_does_not_mask_cow_commit_error_m14` |
| C-006 | Cleanup lives inside `commit` / `commit_overwrite` / `commit_row_delta` / `commit_row_delta_kind`. Wrappers inherit via call-through; `plan_and_commit_*` unedited (T3 fence). | PROVEN — diff names |
| C-007 | Battery I characterization pins flip: staged files GONE after OCC reject; error still `DataInvalid`. | PROVEN — renamed `_files_are_removed_m14` |
| C-008 | Success-path files-untouched pins for both modes. | PROVEN — COW overwrite + MoR row-delta |
| C-009 | Delete-failure pin: scripted LocalFs delete fail (directory path). Custom `Storage` FileIO is a documented cut (typetag + lockfile forbidden). | PROVEN — see §3 |
| C-010 | `mod.rs` ≤ 2700. Helper extracted to `abort.rs`; T5 owns `mod abort;`. `named_column` and below unedited. | PROVEN — measured after rustfmt |
| C-011 | `map.md` lockstep + this ledger linked from `task/map.md` in the same change. No xfail. No AWS. | PROVEN — listed in §2 |

---

## 0. Blast + seam

Finding M14 / BL-5: a rejected MERGE commit left the already-written Parquet
files in the warehouse. Battery I characterized that (engine frozen at OCC-2).

Design A: abort-delete writer-result paths on `tx.commit` `Err` only.

| Item | Location |
|---|---|
| Helper | `crates/repark-iceberg/src/write/merge/abort.rs` |
| COW | `commit` → `commit_overwrite` (`new_files` paths) |
| MoR | `commit_row_delta` → `commit_row_delta_kind` (`data_files` + written delete files) |
| Pins | `occ_conflict_tests.rs` battery I |
| Closed | `predicate_dml.rs`, `plan_and_commit_*`, `named_column` and below |

Altitude: engine commit path. Identity DML reuses the same `pub(super)`
commit arms and inherits the abort.

---

## 1. Implementation

- New [`abort.rs`](../crates/repark-iceberg/src/write/merge/abort.rs):
  `written_file_paths` + `delete_written_files_best_effort`.
- `mod abort;` at the top of `merge/mod.rs` (T5-owned decl). T3 adds no
  mod decls; no collision.
- `commit_overwrite`: collect `new_files` paths before `add_files`; on
  `tx.commit` `Err` delete those paths and `Err(iceberg_err(original))`.
- `commit_row_delta_kind`: collect `data_files` paths before the write;
  collect delete-file paths from a successful `write_position_deletes`;
  on `tx.commit` `Err` delete both sets. If the writer itself fails,
  there is no successful writer result — no directory walk.
- `commit` / `commit_row_delta` are thin isolation resolvers over the
  inner functions; cleanup is therefore inside all four symbols.

---

## 2. Files

| Path | Role |
|---|---|
| `crates/repark-iceberg/src/write/merge/abort.rs` | helper (new) |
| `crates/repark-iceberg/src/write/merge/mod.rs` | `mod abort;` + commit-error seams |
| `crates/repark-iceberg/src/write/merge/occ_conflict_tests.rs` | pin flips + success + delete-failure |
| `crates/repark-iceberg/src/write/merge/map.md` | abort.rs + battery I |
| `crates/repark-iceberg/src/write/map.md` | Debug row |
| `task/map.md` | link |
| this ledger | — |

CLOSED: `predicate_dml.rs`, `insert.rs`, `plan_and_commit_*`, lockfiles,
STATUS, registry, `.github/`, `[patch]`.

---

## 3. Pin table

| Test | Claim |
|---|---|
| `rejected_cow_commit_files_are_removed_m14` | OCC reject: staged data files gone; `DataInvalid`; live `affected` kept |
| `rejected_row_delta_files_are_removed_m14` | OCC reject: no new Parquet orphans; `DataInvalid` |
| `successful_cow_overwrite_commit_leaves_written_data_files_m14` | COW success: committed files still exist and are live |
| `successful_row_delta_leaves_written_delete_files_m14` | MoR success: written delete files still exist |
| `delete_failure_does_not_mask_cow_commit_error_m14` | directory-path `FileIO::delete` fails; error stays `DataInvalid` |

Former names (`rejected_*_leaves_written_*_in_the_warehouse_m14`) renamed
to honest names; `_m14` suffix kept.

### Delete-failure cut

A custom `Storage` / `FileIO` that fails only `delete` needs
`#[typetag::serde]` plus `serde`/`typetag` dev-deps and a lockfile
change — forbidden on this track. The pin scripts the real LocalFs
backend: `delete` is `remove_file`, which errors on a directory.
That is a scripted FileIO failure, not a fake FileIO.

---

## 4. Measures

| File | Lines / ceiling |
|---|---|
| `merge/mod.rs` | **2636 / 2700** |
| `occ_conflict_tests.rs` | **1085 / 1500** |
| `abort.rs` | **36** (default ceiling) |

---

## 5. Octo — sequential hat-switch

### Actor

Smallest honest diff: extract the helper so `mod.rs` stays under 2700,
thread paths from writer results, `match` only `tx.commit`. Prefer zero
edits in `plan_and_commit_*` (T3 stream-seam fence).

### C1 (safety / standing rules)

| Claim | Disposition |
|---|---|
| C1-1: a catch after successful commit would delete live data. | **Held.** `Ok(_) => Ok(())` is a passthrough. |
| C1-2: re-deriving the delete set from manifests can pick up committed files (S1). | **Held.** Paths come from the `Vec<DataFile>` already in hand. |
| C1-3: no `unwrap`/`expect` on the prod path. | **Held.** |
| C1-4: do not delete `affected`. | **Held.** |

### C2 (completeness)

| Claim | Disposition |
|---|---|
| C2-1: battery I calls `commit` / `commit_row_delta` directly, so cleanup cannot live only in `plan_and_commit_*`. | **Held.** A7. |
| C2-2: pin is valid only if reverting the cleanup turns the flipped tests red. | **Held.** Pre-fix characterization asserted files remain. |
| C2-3: success-path pins both modes. | **Held.** |
| C2-4: delete-failure does not mask. | **Held** with the LocalFs directory cut. |

### C3 (docs / maps)

| Claim | Disposition |
|---|---|
| C3-1: `occ_conflict_tests` is no longer "engine frozen". | **Fixed** in merge/map.md. |
| C3-2: `abort.rs` named; task ledger linked. | **Held.** |

### C4 (Iceberg-spec correctness)

| Claim | Disposition |
|---|---|
| C4-1: abort is warehouse hygiene, not a snapshot mutation. Failed commits never entered the manifest list. | **Held.** |
| C4-2: Java Iceberg also best-effort-deletes uncommitted files on a failed commit; we match that posture, not a table-scan orphan sweep. | **Held.** |
| C4-3: `FileIO::delete` on a missing path is a no-op in LocalFs (`if path.exists()`). Double-delete is harmless. | **Held.** |

**Engine label:** OCTO-CONVERGED (C1–C4 ran; no open critic block).

---

## 6. Registry / STATUS

None. BL-5 registry flip is orchestrator after SQM. Paste-true if wanted:

> M14 / BL-5 FIXED (design A): a rejected MERGE `tx.commit` best-effort
> `FileIO::delete`s the writer-result files this attempt produced, then
> re-raises the original error. Delete set is never re-derived from the
> table. Success-path files stay. A failed delete is warned, not returned.

---

## Close-out (filled after gates)

- `make verify` **EC=0** (2026-08-15).
- `make preflight` constituents **EC=0**: verify; facade `3214 passed, 71
  skipped`; `make audit`; `make workflows-lint`. A single wrapped
  `make preflight` timed out after pytest; the same steps were re-run
  to completion (same posture as M16).
- Hook surface: worktree uses the shared `pre-commit` hook (map.md,
  crate-dag, lib-rs, rust-file-size, lib-py, manifest, cargo fmt,
  taplo, typos).

## Orchestrator amendment at re-pass (pre-merge)

The catch scope as shipped deleted on EVERY `tx.commit` error. The fork distinguishes
`CatalogCommitConflicts` (commit definitively failed) from `CommitStateUnknown` (the catalog
may have persisted — Java rethrows CommitStateUnknownException AHEAD of its cleanup catch).
Deleting under unknown state can corrupt a successful commit — the exact S1 the charter's
HALT clause names. Amendment: `delete_written_files_best_effort` now takes the commit error
and SKIPS deletion on `CommitStateUnknown` (warn + leave for orphan maintenance). Honest cut:
no test injects `CommitStateUnknown` (the battery's real-catalog conflicts produce
`CatalogCommitConflicts`, which exercises the cleanup path); injection needs scripted-catalog
machinery at the Rust layer — named residual, not xfail.
