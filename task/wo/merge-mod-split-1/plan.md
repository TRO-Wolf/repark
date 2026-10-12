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

Recorded 2026-10-12, against `origin/main` at `572130dd`.

`prove_move.py` compares each item of `origin/main:mod.rs` with the same item in the file that
holds it now. Before it compares, it removes comments, visibility keywords, whitespace outside
string literals, and the comma rustfmt adds before a closing delimiter when a widened signature
wraps (two methods: `insert_sql` and `prior_clauses_do_not_apply_legacy`). String literals are
compared byte for byte. `use` and `mod` declarations are not items; an `impl` counts once, plus
each of its methods. Its output is [proof.txt](proof.txt):

- result `PURE MOVE`: all 76 items compare the same, 45 that stay and 31 that moved;
- 76 items in the base file, 76 across the four files (45 + 24 + 4 + 3); none missing, none new.

Measured line counts:

| File | Lines | Under the 1,000 default by |
|---|---|---|
| `mod.rs` | 752 (was 1,569) | 248 |
| `merge_sql.rs` | 336 | 664 |
| `copy_on_write.rs` | 245 | 755 |
| `merge_on_read.rs` | 265 | 735 |

`merge_sql.rs` holds `rewrite_column_with_maps` and its `store_assignment_then_sql` call, so
the waiting fix has 664 lines of headroom there. `not_matched_by_source.rs` is byte-identical
(279 lines). The `mod.rs` exception row is removed from both size tables.

What differs from the plan above: `mod.rs` keeps `use insert::store_assignment_then_sql;`,
because `not_matched_by_source.rs` imports that name through `super::` and is not edited. The
`#[cfg(test)]` imports `mod.rs` gained are `HashMap`, `StringArray`, `ManifestContentType` and
`consume_matched_work_batch`; the test batteries reach them through `super::super::*`. No helper
in `mod.rs` needed wider visibility.
