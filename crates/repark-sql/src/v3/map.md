# map — repark-sql/src/v3

## Purpose

ANSI-door format-v3 test modules. `lib.rs` declares `#[cfg(test)] mod v3;`.

## Contents

- `mod.rs` — thin index.
- `create.rs` — **V3-2:** ANSI CREATE/CTAS `format_version = 3` opt-in pins
  (`Model: Grok 4.6 xHigh` on the module's functions). **V3-6 C-003:** opt-in CREATE
  `timestamp_ns` / `timestamptz_ns` stores the Iceberg primitives; `timestamp_ns`
  SELECT round-trips ns values and Arrow types (pins: v3-6-v3-types/C-003).
- `cow.rs` — **V3-COW-1 (RP-6 lift):** adopted-v3 UPDATE / MERGE matched-update keep
  `_row_id`; sequential COW DELETE keeps the survivor id at next-row-id 6; subquery
  WHERE DML commits; padded MoR UPDATE keeps `_row_id`; plain-`WHERE` DELETE including
  a second MOR DELETE that merges into the live vector; v2 control; Hadoop `vN` write
  (pins: rp-6-fork-repin/C-002, C-003; rp-3-fork-repin/C-008). Hash-pinned by
  `v3_lineage.rs::cow_keep_refusal_files_are_byte_untouched`.
- `types.rs` — `GEOMETRY` / `GEOGRAPHY` / `VARIANT` refuse at CREATE (`V3-GEO-1`);
  reuses `cow.rs`'s `Door`. **V3-6 C-004:** the `UNKNOWN` column refuses naming the
  type, no table left (pins: v3-6-v3-types/C-004).
- `branch_tag_time_travel.rs` — ANSI branch/tag + `FOR VERSION AS OF` over the partitioned
  v3 DV fixture; RP-3 shared-Puffin DELETE keeps the untouched sibling
  (pins: rp-3-fork-repin/C-004).
- `partitioned_equality_deletes.rs` — ANSI live-row twins of the Spark-written partitioned
  DV and equality-delete + DV fixtures, plus `$delete_files` content 1/2, cross-partition DV
  DELETE, live-DV UPDATE commits (`Model: Grok 4.6 xHigh`; rp-6-fork-repin/C-003),
  and C-007 ANSI CALL / fork no-op of `rewrite_position_delete_files`
  (pins: rp-3-fork-repin/C-007). **V3-4:** ANSI `_row_id` / `_last_updated_sequence_number`
  on both fixtures, `SELECT *, _row_id` expands user columns only, qualified/aliased forms,
  unquoted case-fold, JOIN/CTE/subquery/`FOR VERSION AS OF` refuse `V3-ROWID-2`, v2 unresolved
  as `No field named _row_id`.
  pins: v3-4-serve-lineage-columns/C-003, C-005, C-007, C-008, C-011, C-012, C-013, C-014,
  C-015, C-016, C-018, C-020

## Pointers

- Up: [../map.md](../map.md)
