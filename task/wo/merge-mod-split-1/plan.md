# MERGE-MOD-SPLIT-1 — plan

Owner, 2026-10-12. This file closes when the split merges; it is the unit's record, not a live
document. Base: `572130dd` (main).

## Why

`crates/repark-iceberg/src/write/merge/mod.rs` is 1,569 lines, exactly its exception ceiling, so
no fix can add a line to it. ICE-NTZ-NESTED-WALL-1 must pass a source type through
`store_assignment_then_sql` and its callers. The exception row names the seam: plan preparation
on one side, copy-on-write and merge-on-read execution on the other.

## The boundary

`mod.rs` keeps plan preparation: `execute_merge`, mode resolution, the guards, star expansion,
the scratch target, `MergeTarget`, `plan_and_commit` (the mode dispatch), the helpers both arms
share (`stream_sql`, `skip_cardinality`, `require_non_null_i64`, `strict_cast`, `named_column`),
the test instruments, and the batch-sink family. Three new files take the rest:

| New file | Items | Visibility after the move |
|---|---|---|
| `merge_sql.rs` | `MergeSql` (struct and its whole `impl`, 20 methods), `NOT_MATCHED_POS_SENTINEL` | struct and its four fields `pub(super)`; a method is `pub(super)` only where `mod.rs`, a sibling or a test calls it; the constant stays private |
| `copy_on_write.rs` | `plan_and_commit_cow`, `resolve_affected_data_files`, `affected_files`, `fold_discovery_batch_into_affected` | `plan_and_commit_cow` `pub(super)`; `resolve_affected_data_files` `pub(crate)` because `write::predicate_dml::cow_commit` already calls it through `merge::`; the other two private |
| `merge_on_read.rs` | `plan_and_commit_mor`, `matched_work_mor`, `consume_matched_work_batch` | `plan_and_commit_mor` `pub(super)`; `consume_matched_work_batch` `pub(super)` for its two unit pins; `matched_work_mor` private |

The SQL builders get their own file instead of staying beside plan preparation: with them
`mod.rs` would end near 1,085 lines and keep an exception row. `merge_sql.rs` then holds
`rewrite_column_with_maps` and the `store_assignment_then_sql` call site at roughly 310 lines.

## What `mod.rs` gains

- `mod copy_on_write; mod merge_on_read; mod merge_sql;`
- `use merge_sql::MergeSql;`, so `super::MergeSql` still resolves for `insert.rs`,
  `not_matched_by_source.rs`, `row_lineage.rs` and the tests, which are not edited.
- `pub(super) use copy_on_write::resolve_affected_data_files;` for the predicate-DML caller.
- `#[cfg(test)]` imports for the names the test batteries reach through `super::super::*`
  (`consume_matched_work_batch`, and any `std` or Arrow name `mod.rs` itself stops using).
- The helpers the new files call widen from private to `pub(super)` only if the compiler asks;
  a child module reads its parent's private items, so none should.

## Rules held

Bodies move byte for byte. A moved block drops every comment it carried; code that stays keeps
its comments. No signature, name, order or generic changes. No `#[path]`. No test logic changes;
no test file is edited. `not_matched_by_source.rs` is not edited.

## Sizes

Expected: `mod.rs` about 750, `merge_sql.rs` about 310, `copy_on_write.rs` about 240,
`merge_on_read.rs` about 250. `mod.rs` falls under 1,000, so its row leaves
`scripts/check_rust_file_size.py` and `test_cap_1_source_file_line_cap.py` (33 Rust rows become
32). Measured counts are in the proof section.

## Proof of a pure move

`prove_move.py` in this directory compares each moved item between `origin/main:mod.rs` and the
file that holds it at HEAD, after it removes comments, visibility keywords and all whitespace. It
also counts the items in the old file and across the four files. Its output is committed as
`proof.txt` with the move.
