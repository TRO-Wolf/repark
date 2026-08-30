# Unit ledger — OCC-2 conflict batteries (M19/M20 + M14/M15 pins)

**Unit:** OCC-2 · conductor-13 T3 · **Date:** 2026-08-15 ·
**Lane:** repark · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-occ` · **Branch:** `grok/occ2-conflict-batteries` ·
**Base (FROZEN):** `cd0db4f459e62994b45f8aadd1d5b58f040d90a5`

**Charter:** `planning/grok/BRIEF-occ-hardening-13.md` OCC-2 + conductor-13
Addendum A9/A10. **SEPMO:** octo + C4. Sequential hat-switch Actor → C1 → C2
→ C3 → C4. Floor S1. Risk: high (concurrency tests; wrong tests are worse
than none).

**Engine FROZEN.** No production edits in `merge/mod.rs` commit region (only a
`#[cfg(test)]` sibling-module wire). No production edits in
`predicate_dml.rs`. Skip A (OCC-1 #117 owns it) and D (schema-evolution race;
optional, not cheap). Independent of OCC-1 — `HEAD^` is the freeze.

xfail forbidden. No AWS.

---

## Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | Battery B: `RowDeltaKind::Delete` + snapshot tolerates concurrent DELETE-op file removal (`validate_deleted_files` not armed). | PROVEN — green on freeze |
| C-002 | Battery B: Delete-kind + serializable still tolerates that removal (kind, not isolation, arms the exist-check widen). | PROVEN |
| C-003 | Battery B: Merge-kind + snapshot rejects the same DELETE-op removal. | PROVEN |
| C-004 | Battery C: MERGE↔MERGE COW rewrite, both orders; loser rejects; winner is the only live file. | PROVEN — needle `conflict`/`missing data files` |
| C-005 | Battery C: MERGE↔MERGE MoR via `commit_row_delta`, both orders. | PROVEN |
| C-006 | Battery E: refreshed handle + original pin + benign concurrent delete succeeds; new snapshot parents S1. | PROVEN |
| C-007 | Battery E: refreshed handle + original pin + conflicting append rejects (pin precedence over tx start). | PROVEN |
| C-008 | Battery F: empty-table `snapshot_id == None`; from-root walk rejects concurrent insert-only. | PROVEN |
| C-009 | Battery G / M15: serializable MERGE + `AlwaysTrue` rejects concurrent append in a different partition. Not an xfail. | PROVEN |
| C-010 | Battery G control: concurrent delete in a different partition is allowed. | PROVEN |
| C-011 | Battery H / M20: insert-only COW=`append`, insert-only MoR=`overwrite`, mixed both=`overwrite`, delete-only MoR=`delete`. CDC mode-flip named. | PROVEN — 5 named tests |
| C-012 | Battery I / M14: rejected COW commit leaves staged data files on disk. Characterization, not xfail. | PROVEN |
| C-013 | Battery I / M14: rejected MoR row delta leaves new Parquet delete files on disk. | PROVEN |
| C-014 | DELETE isolation property (A10): no trim, lowercase, default serializable, garbage ⇒ Plan `Invalid isolation level: {name}`. | PROVEN |
| C-015 | UPDATE isolation property (A10) + serializable reject / snapshot commit through concurrent append. | PROVEN |
| C-016 | File fence: only allowed paths. `occ_tests.rs` stays 706/1500. New sibling ≤1500. No lockfiles/STATUS/registry. | PROVEN — see §2 |
| C-017 | `map.md` lockstep + ledger in the same commit. | PROVEN |

---

## 0. Blast

Test-only coverage of the pub(super) commit arms' untested policy dimensions
(M19) and the M20 operation-stamp / M14 orphan / M15 over-rejection
characterizations. Engine is the freeze recipe: MERGE `commit` /
`commit_row_delta` stay hard-wired Serializable + Merge-kind;
`commit_row_delta_kind` / `commit_overwrite` already take a policy
(identity DML's path).

---

## 1. Implementation

- New sibling [`crates/repark-iceberg/src/write/merge/occ_conflict_tests.rs`](../../../../crates/repark-iceberg/src/write/merge/tests/occ_conflict.rs)
  (split rather than grow `occ_tests.rs` past 1500).
- `#[cfg(test)] mod occ_conflict_tests;` in `merge/mod.rs` (test-module wire
  only; production commit region untouched).
- Isolation-property cases in
  [`predicate_dml_tests.rs`](../crates/repark-iceberg/src/write/predicate_dml_tests.rs)
  and
  [`predicate_dml_update_tests.rs`](../crates/repark-iceberg/src/write/predicate_dml_update_tests.rs).
- map.md: merge/, write/, task/.

---

## 2. Files

- `crates/repark-iceberg/src/write/merge/occ_conflict_tests.rs` (new)
- `crates/repark-iceberg/src/write/merge/mod.rs` (test-module decl only)
- `crates/repark-iceberg/src/write/predicate_dml_tests.rs`
- `crates/repark-iceberg/src/write/predicate_dml_update_tests.rs`
- `crates/repark-iceberg/src/write/merge/map.md`
- `crates/repark-iceberg/src/write/map.md`
- `task/map.md`
- this ledger

CLOSED: production `commit*` bodies, `predicate_dml.rs` production,
`position_delete.rs`, lockfiles, STATUS, registry, functions, ta,
`grok/occ1-merge-isolation`.

---

## 3. Pin table

| Battery | Test | Claim |
|---|---|---|
| B | `commit_row_delta_kind_delete_snapshot_tolerates_concurrent_delete_op_removal` | Delete+snapshot tolerates DELETE-op removal |
| B | `commit_row_delta_kind_delete_serializable_tolerates_concurrent_delete_op_removal` | kind, not isolation |
| B | `commit_row_delta_kind_merge_snapshot_rejects_concurrent_delete_op_removal` | Merge+snapshot rejects |
| C | `commit_cow_merge_merge_race_first_rewrite_wins` / `_second_rewrite_wins` | COW both orders |
| C | `commit_row_delta_merge_merge_race_first_wins` / `_second_wins` | MoR both orders |
| E | `commit_retry_through_benign_commit_revalidates_from_original_pin_and_succeeds` | rebase success |
| E | `commit_refreshed_handle_still_validates_from_original_pin` | pin ≻ tx start |
| F | `commit_empty_table_none_pin_from_root_walk_rejects_concurrent_insert` | from-root |
| G | `commit_serializable_merge_rejects_concurrent_append_in_a_different_partition_m15` | M15 over-reject |
| G | `commit_serializable_merge_allows_concurrent_delete_in_a_different_partition` | control |
| H | `merge_commit_operation_stamps_match_audit_m20_table` | M20 stamps + CDC |
| I | `rejected_cow_commit_leaves_written_data_files_in_the_warehouse_m14` | M14 COW orphans |
| I | `rejected_row_delta_leaves_written_delete_files_in_the_warehouse_m14` | M14 MoR orphans |
| DML | `delete_isolation_property_a10_no_trim_lowercase_default_garbage` | A10 DELETE |
| DML | `update_isolation_property_a10_no_trim_lowercase_default_garbage` | A10 UPDATE |
| DML | `update_isolation_serializable_rejects_concurrent_append` | UPDATE serializable |
| DML | `update_isolation_snapshot_commits_through_concurrent_append` | UPDATE snapshot |

Skip A (OCC-1). Skip D (not cheap).

---

## 4. Measures

| File | Lines / ceiling |
|---|---|
| `occ_tests.rs` | **706 / 1500** (untouched) |
| `occ_conflict_tests.rs` | **967 / 1500** |
| `predicate_dml_tests.rs` | **1448 / 1500** |
| `predicate_dml_update_tests.rs` | **873 / 1500** |
| `merge/mod.rs` | **2587 / 2700** (+2 `#[cfg(test)]` module wire) |

Honest cut: DELETE isolation is resolver-only (A10 parse). The file is 1448/1500;
a two-handle DELETE behavioral pair would cross the ceiling. UPDATE carries the
`commit_overwrite` serializable-vs-snapshot thread (same arm identity DELETE COW
uses). DELETE MoR kind is battery B.

Skip A (OCC-1). Skip D (schema-evolution race; not cheap).

---

## 5. Critics (sequential hat-switch)

### C1 — freeze-engine correctness

ACCEPTED after one fix round. All 19 `occ_conflict_tests` + 4 isolation pins
green against the frozen commit recipe (`commit`/`commit_row_delta` hard-wired
Serializable+Merge; `commit_row_delta_kind`/`commit_overwrite` take policy).

- B: kind axis isolated from isolation (Delete+serializable still tolerates;
  Merge+snapshot still rejects). Matches fork `validate_deleted_files` op-set
  widen.
- C: same-shape both orders, not insert-only-vs-rewrite. C1 asked for an OCC
  needle so a random apply-side `DataInvalid` cannot green the loser: added
  `conflict` / `missing data files`.
- E: the load-bearing mutation is the *refreshed handle + original pin*, not
  the stale-handle success already in `occ_tests`. Dropping
  `validate_from_snapshot` would empty the walk (tx start = S1) and C-007
  goes red.
- F: `starting_snapshot_id == None` is from-root (`files_after` docs).
- G/H/I: live truth matches the audit table (insert-only MoR is `overwrite`
  per Java 1.10.0 `BaseRowDelta`, not MAIN's later APPEND branch).

### C2 — house style / fence / map.md

ACCEPTED. Allowed paths only. `occ_tests.rs` identity-stable. Production
commit bodies and `predicate_dml.rs` untouched. The 2-line
`#[cfg(test)] mod occ_conflict_tests;` is a test-module wire required by the
sibling (brief-allowed); not a commit-region edit. Clippy
`doc_markdown` / `too_many_lines` / `items_after_statements` fixed before
verify. Maps: merge/ (sibling row), write/ (isolation pin sentence), task/.

### C3 — testing contract

ACCEPTED. No `#[ignore]`, no xfail, no `TODO: add test`. M14/M15 are named
characterization pins (future fix flips them). MemoryCatalog, no AWS. Tests
*are* the change (engine frozen). Engine commit-arm unit tests, not SQL-door
rows — the surface under test is `pub(super)` commit policy, not a user door.

### C4 — concurrency subtlety

ACCEPTED after the C needle tighten. Two-handle races are sequential (the
MERGE executor serializes under `cfg(test)`; `tokio::spawn` is banned). Doc
comments say so. E does not confuse "stale handle still has S0 as tx start"
with pin precedence — the refreshed-handle pair is the one that dies if the
from-snapshot call is dropped. G's control (different-partition delete
allowed) keeps the M15 reject from being "any concurrent commit". I walks
real `FileIO` / warehouse Parquet, not synthetic manifest-only paths.

**OCTO-CONVERGED** (C1–C4 ran; floor S1).

---

## 6. Gates

| Item | Result |
|---|---|
| `make verify` | **0** |
| `make py-test-facade` (via preflight) | **0** (3119 passed, 71 skipped) |
| `make audit` (via preflight) | **0** |
| `make workflows-lint` (via preflight) | **0** |
| `make preflight` surface | **0** |
| Identity | `64240326+TRO-Wolf@users.noreply.github.com` |
| Trailer | `Authored-By: Grok (grok-4.6) <noreply@x.ai>` |
| Base | `cd0db4f` — not stacked on OCC-1 |

---

## 7. Outcome

SHIPPED. Test-only OCC conflict batteries B/C/E/F/G/H/I + DELETE/UPDATE
isolation-property pins. Engine frozen. Independent PR off the freeze.
