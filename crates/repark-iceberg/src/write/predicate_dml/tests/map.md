# map — repark-iceberg/src/write/predicate_dml/tests

ICE-MIXED-CASE-1 (2026-09-17): predicate-DML pins covering the case-insensitive scope delegation. pins: ice-mixed-case-1/C-003

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

Identity DELETE/UPDATE tests. `predicate_dml.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index.
- `plain.rs` — **RP-9 r2:** `try_allowed_plain_identity` accepts `DELETE … WHERE id = 0` on a
  three-part name and refuses a subquery `WHERE`, a literal `IN` list, an `UPDATE`, and a
  four-part branch selector (those stay on the IN/EXISTS allow-list or the fork delete exec).
  pins: rp-9-repin-f23/C-005
  **ICE-LIST-NULL-2 (2026-09-19):** the gate pins — a compound predicate over a list, a
  map and a struct column each need the fork, a bare nested `IS NULL` is still not a
  plain-identity claim at all, a primitive-only compound and an unknown column stay off
  the fork, and a target-qualified nested column still needs it.
  pins: ice-list-null-2/C-001
  **ICE-SESSION-WRITE-CONF-1 round 2 (2026-09-19):** the four-part row above is now the
  opposite claim. Round 1's branch-write step made `split_branch_parts` peel a
  `branch_<name>` selector, so `DELETE FROM ice.sales.t.branch_b WHERE id = 0` IS a plain
  identity and carries `branch: Some("b")` — that is the whole point of the step, and the
  owned route is the only one that can stamp the session conf on a branch head. The Spark
  door still decides whether the ref-qualified name survives to get here
  (`repark-spark` `write_to_branch.rs` keeps it only when the session write conf is set).
  A `tag_<name>` selector and an empty `branch_` both stay four parts and decline, so a
  tag is never written.
  pins: ice-session-write-conf-1/C-043
- `predicate_dml.rs` — DELETE: `IN` / `NOT IN (SELECT …)` including the NULL 3VL trap,
  `[NOT] EXISTS` with and without correlation, correlated `IN`, isolation-level pins
  (M19 / A10).
- `update.rs` — ICE-OCC-SCOPED-1: the two isolation pins hand `commit_overwrite` a
  `&CommitScope::unscoped(isolation)` (the commit now takes a scope, not a bare level).
- `update.rs` — identity `UPDATE … SET <scalar> WHERE col IN`. Unknown
  `write.delete.granularity` refuses before any parquet write (MW-9). **V3-9:**
  `identity_pairs_share_one_arc_per_data_file_path` counts `Arc` identities over 600,003 pairs
  on two paths and requires exactly two allocations.
  pins: v3-9-mor-predicate-dml-dv/C-009
  **ICE-SESSION-WRITE-CONF-1 (2026-09-19):** the isolation batteries call
  `merge::commit_overwrite` directly, keeping their spellings across the
  `cow_commit` split.

## Pointers

- Up: [../map.md](../map.md)
- **ICE-SESSION-WRITE-CONF-1 round 1 (2026-09-19):** the spec builders here name the new
  `branch: None` field (a `None` branch is the current-ref write these batteries pin).
