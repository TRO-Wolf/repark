# Unit ledger — MG-2 MERGE lowering strictness (M2, M3, M8, M10)

**Unit:** MG-2 · daytime conductor-12 T2 · **Date:** 2026-08-15 ·
**Lane:** repark · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-mg2` · **Branch:** `grok/mg2-lowering-strictness` ·
**Base (FROZEN):** `a2b385f4113a725a3b013553d2ee99fcf8278cfb`

**Charter:** `planning/grok/BRIEF-mg2-lowering-strictness.md` + conductor-12
Addendum A8. **SEPMO:** acc + C4. Floor S1. max_cycles=2. Risk: standard
(door validation; not engine commit path).

This ledger does **not** edit `docs/spark-sql-iceberg-parity.md` or
`STATUS.md` (A9). Nothing under `crates/repark-iceberg/`. No Python tests.

### Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | M2: `update_predicate` / `delete_predicate` / `insert_predicate` are destructured; any `Some(_)` is a loud `Plan` error naming the Oracle construct and the Spark `WHEN … AND` form. | PROVEN — both doors |
| C-002 | r5 (`UPDATE SET … WHERE`) refuses on both doors. | PROVEN — `oracle_style_update_where_predicate_refuses` (both) + Spark execute `merge_oracle_style_update_where_refuses` |
| C-003 | A `DELETE WHERE` variant refuses on both doors. | PROVEN — `oracle_style_delete_where_predicate_refuses` (both) + Spark execute `merge_oracle_style_delete_where_refuses` |
| C-004 | An `INSERT … WHERE` variant refuses on both doors. | PROVEN — `oracle_style_insert_where_predicate_refuses` (both) + Spark execute `merge_oracle_style_insert_where_refuses` |
| C-005 | M3: SET / INSERT targets accept only a bare column or `<target-alias>.column` (qualifier == target alias, case-insensitive). | PROVEN — `target_qualified_and_bare_set_targets_lower` (both, Spark also `SET T.name`) + `quoted_target_alias_set_target_lowers` (both) + `target_qualified_insert_columns_lower` (ANSI) + Spark execute `merge_target_qualified_and_bare_set_targets_execute` |
| C-006 | Any other qualifier is a loud error naming the received qualifier and the target alias. r7 refuses. | PROVEN — `source_qualified_set_target_refuses` / `source_qualified_insert_column_refuses` (both) + Spark execute `merge_source_qualified_set_target_refuses` |
| C-007 | Three-or-more-part targets refuse with `nested-field assignment is not supported`. Nested assignment is not implemented. | PROVEN — `nested_field_set_target_refuses` / `nested_field_insert_column_refuses` (both) + Spark execute `merge_nested_field_set_target_refuses` |
| C-008 | M8: Spark door refuses column-list-less `INSERT VALUES` with the ANSI needle verbatim. `INSERT *` and explicit lists stay green. | PROVEN — `insert_without_column_list_refuses` + `insert_star_still_lowers` + existing `star_forms_lower_to_markers` / `merge_star_forms_upsert` / `lowers_classic_upsert` |
| C-009 | M10: unconditioned clause before another clause of the same kind refuses with Spark error-class wording. r12 refuses. Unconditional LAST still works. Existing multi-conditional first-match-wins stays green. | PROVEN — `non_last_unconditional_*` + `unconditional_last_matched_clause_still_lowers` + existing `clause_predicates_and_order_survive` / `merge_clause_order_first_match_wins` / `merge_matched_and_arm_order_update_then_delete` |
| C-010 | Existing `DataFusionError::Plan` / `NotImplemented` only. No new error type. | PROVEN — grep of the two `merge.rs` files |
| C-011 | No existing test asserted the old permissive accept (search: `INSERT VALUES`, `SET s.`, `UPDATE SET … WHERE`). ANSI `degenerate_update_and_insert_shapes_refuse` already refused M8. **Flipped permissive pins: none.** | PROVEN — tree search |
| C-012 | T2 fence: only the two door `merge.rs` files, their test batteries, map.md lockstep, this ledger + `task/map.md`. | PROVEN — `git diff --name-only` |
| C-013 | `map.md` lockstep + tests in the same commit as the code. | PROVEN — listed in §2. |

**Enumeration:** M2 {UPDATE WHERE, DELETE WHERE, INSERT WHERE} × 2 doors + M3
{wrong qualifier SET, nested SET, wrong qualifier INSERT, nested INSERT,
bare/target-qualified positive} × 2 doors (execute extra on Spark) + M8 Spark
refuse + INSERT* / explicit green + M10 {MATCHED, NOT MATCHED} × 2 doors +
last-unconditional green. Pin count ≥ partition size.

---

## 0. Blast

Door-only. Shared executor (`repark_iceberg::write::merge`) keeps positional
`insert_projection` and is not edited. The Spark door previously accepted
column-list-less INSERT and handed an empty column list to that path; closing
the door is the charter.

---

## 1. Implementation

Both doors, same shape (no door→door edge):

- Destructure `MergeUpdateExpr::{update_predicate, delete_predicate}` and
  `MergeInsertExpr::insert_predicate`. Any `Some` → `Plan` with
  `ORACLE_STYLE_SUB_PREDICATE_REFUSAL`.
- `resolve_merge_column`: 1 part accept; 2 parts require qualifier ==
  unquoted target alias (`eq_ignore_ascii_case`); 3+ parts →
  `nested-field assignment is not supported`. Used for SET and INSERT lists.
- Spark: after star-insert detection, empty `columns` → verbatim
  `MERGE INSERT requires an explicit column list: INSERT (a, b) VALUES (…)`.
- After clause collection: walk each kind; unconditioned non-last →
  `NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION` /
  `NON_LAST_NOT_MATCHED_CLAUSE_OMIT_CONDITION`.

---

## 2. Files

- `crates/repark-spark/src/merge.rs` + in-module tests
- `crates/repark-spark/src/tests/merge.rs` (execute pins)
- `crates/repark-sql/src/merge.rs`
- `crates/repark-sql/src/merge/tests.rs`
- map.md: `crates/repark-spark/src/map.md`, `crates/repark-spark/src/tests/map.md`,
  `crates/repark-sql/src/map.md`, `crates/repark-sql/src/merge/map.md`,
  `task/map.md`
- this ledger

---

## 3. Flipped permissive pins

**None.** Tree search found no test that asserted the old accept except
ANSI `degenerate_update_and_insert_shapes_refuse`, which already refused
column-list-less INSERT (kept).

---

## 4. Gates

- `cargo clippy -p repark-spark -p repark-sql --lib -- -D warnings` — EC=0
- `cargo test -p repark-spark -p repark-sql --lib merge` — 71 Spark (filter) / 23 ANSI (filter) green; after Critic-1 pins, `merge::tests` 24 Spark + 21 ANSI
- `make` ci static chain (fmt, clippy workspace, panic-ban, dag, lib-rs, rust-file-size, lib-py, manifest, parity-live dual-wire, matrix-liveness, rust-check, ruff, lock, taplo, typos) — EC=0
- `make test` first pass: unrelated `repark-ta` `hour0_bbands_three_vs_one_1e6` load-flake; retry EC=0. Workspace minus that crate EC=0.
- ACC: Critic-1 cycle 1 filed Q-001/Q-002 (S2 coverage) — remediating pins landed; Critic-2 CLEAN; C4 after commit.
