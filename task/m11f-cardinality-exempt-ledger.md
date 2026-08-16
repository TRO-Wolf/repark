# Unit ledger — M11 lone-unconditional-DELETE cardinality exemption

**Unit:** M11 · conductor-15 Track T2 · **Date:** 2026-08-15 ·
**Lane:** repark · **Executor:** Grok (grok-4.6) ·
**Tree:** conductor-15 T2 worktree · **Branch:** `grok/m11f-cardinality-exempt` ·
**Base (FROZEN):** `8cbde88`

**Charter:** conductor-15 T2 + Addendum A7/A12. Registry/STATUS/BL-3 flip is
orchestrator after SQM — this ledger does **not** edit
`docs/spark-sql-iceberg-parity.md` or `STATUS.md`.

### Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | Skip cardinality IFF `spec.matched` is exactly one clause AND `predicate_sql.is_none()` AND `action` is `MatchedAction::Delete`. | PROVEN — `skip_cardinality` match |
| C-002 | Compute once on the spec-owning execute path after `expand_star_clauses` (which does not rewrite Delete). | PROVEN — `plan_and_commit_cow` / `plan_and_commit_mor` call `skip_cardinality(spec)` |
| C-003 | Thread `skip_cardinality: bool` through `affected_files` → `fold_discovery_batch_into_affected` and `matched_work_mor` → `consume_matched_work_batch`. | PROVEN — signatures + call sites |
| C-004 | Never re-parse SQL inside the fold. | PROVEN — bool only |
| C-005 | When `skip_cardinality && match_count > 1`, do not return `CARDINALITY_VIOLATION_MSG`; still fold mutations / pos-deletes. | PROVEN — `if match_count > 1 && !skip_cardinality` |
| C-006 | Conditional DELETE, UPDATE, UpdateAll, multi-clause, empty matched keep the check. | PROVEN — `skip_cardinality_only_lone_unconditional_delete` + native still-raises |
| C-007 | `mod.rs` ≤ 2700; no EXCEPTIONS raise. | PROVEN — measured 2671 |
| C-008 | Native door: refuse-gone + still-raises, not in `tests.rs` (1593/1600). | PROVEN — `merge/cardinality_tests.rs` |
| C-009 | Flip `dup_source_keys_unconditional_delete` split → content; do not hand-edit the Spark golden. | PROVEN — `repark=None`, golden table unchanged |
| C-010 | Do not touch `merge_cardinality_uses_file_and_pos_not_file_alone` or the conditional-clause differential row. | PROVEN — those files/rows untouched except consume arg `false` in existing unit pins |
| C-011 | `map.md` lockstep + tests in the same commit as the code. | PROVEN — listed in §2 |

---

## 0. Blast

Spark `RewriteMergeIntoTable.isCardinalityCheckNeeded` is false when the only
MATCHED action is an unconditional DELETE (double-delete is idempotent; no
last-writer-wins ambiguity). Repark previously raised
`MERGE_CARDINALITY_VIOLATION` whenever any MATCHED arm existed. The #131
oracle recorded the Spark survivor table (id=1 / name='a') as a split
disclosure; this unit lands the exemption and flips that row to content
equality.

T5 / M14 abort-path cleanup is not touched.

## 1. Implementation

- `skip_cardinality(&MergeSpec)` matches exactly
  `[MatchedClause { predicate_sql: None, action: MatchedAction::Delete }]`.
- Computed at `plan_and_commit_cow` / `plan_and_commit_mor` (spec already
  star-expanded by `execute_merge`).
- Fold helpers take the bool. `match_count > 1` still collects affected files
  / pos-delete pairs when the exemption fires.

## 2. Files

- `crates/repark-iceberg/src/write/merge/mod.rs`
- `crates/repark-iceberg/src/write/merge/tests.rs`
- `crates/repark-sql/src/merge.rs` (test-module declaration)
- `crates/repark-sql/src/merge/cardinality_tests.rs` (new)
- `python/repark/tests/test_merge_differential_parity.py`
- map.md: `crates/repark-iceberg/src/write/merge/map.md`,
  `crates/repark-sql/src/map.md`, `crates/repark-sql/src/merge/map.md`,
  `python/repark/tests/map.md`, `task/map.md`
- this ledger

## 3. Tests (red-then-green)

- Iceberg unit: `skip_cardinality_only_lone_unconditional_delete`.
- Native door: `merge_dup_source_keys_unconditional_delete_succeeds` (survivor
  id=1 / name='a', Int64 + Utf8) + UPDATE and conditional-DELETE still-raises.
- Spark facade: `dup_source_keys_unconditional_delete` kind=content,
  `repark=None` against the recorded Spark table.

## 4. Residuals

- BL-3 / registry / STATUS retirement is orchestrator-side after SQM.
- Optional iceberg execute pin next to
  `merge_cardinality_uses_file_and_pos_not_file_alone` skipped: native +
  differential cover both doors; `streaming_scan_tests.rs` slack is tight.

## 5. Gates

| Gate | Result |
|---|---|
| `make format` | exit 0 |
| `make verify` | exit 0 (clippys + rust-file-size + workspace tests) |
| `make develop` | exit 0 (fresh maturin after worktree `uv sync`) |
| `make py-test-facade` | exit 0 — **3230 passed, 71 skipped, 0 failed** (204s) |
| `make rust-audit` / `rust-deny` / `py-audit` | exit 0 (advisories/bans/licenses/sources ok; pip-audit clean) |
| `make workflows-lint` | exit 0 (13 workflows parse; zizmor no findings) |
| `make preflight` wrap | not re-run as one target after the facade (would re-pay 3+ min); every constituent above is exit 0 |

`crates/repark-iceberg/src/write/merge/mod.rs` **2671 / 2700**.
`crates/repark-sql/src/tests.rs` untouched at **1593 / 1600**.
