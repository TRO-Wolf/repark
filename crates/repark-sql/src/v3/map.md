# map — repark-sql/src/v3

## Purpose

ANSI-door format-v3 test modules. `lib.rs` declares `#[cfg(test)] mod v3;`.

## Contents

- `mod.rs` — thin index.
- `create.rs` — **V3-2:** ANSI CREATE/CTAS `format_version = 3` opt-in pins
  (`Model: Grok 4.6 xHigh` on the module's functions). **V3-6 C-003:** opt-in CREATE
  `timestamp_ns` / `timestamptz_ns` stores the Iceberg primitives; `timestamp_ns`
  SELECT round-trips ns values and Arrow types (pins: v3-6-v3-types/C-003).
  **V3-9:** the opt-in refusal must not claim merge-on-read is unserved
  (pins: v3-9-mor-predicate-dml-dv/C-006).
  **V3-10:** `alter_set_properties_*` pin the ANSI door's in-place upgrade — `SET PROPERTIES
  (format_version = 3)` with the session opt-in installed as the real `ReparkSqlConfig`, its
  without-opt-in twin, pre-upgrade rows reading NULL lineage, the same-version request writing
  no new metadata file, the `extra_properties` map spelling driving the same resolver, and the
  downgrade / `'1'` / `'-1'` / `'4'` / `'x'` / `'3.0'` refusals; the `extra_properties` map
  spelling of the reserved key keeps steering to the curated `format_version`
  (pins: v3-10-upgrade-v2-to-v3/C-003, C-004).
- `cow.rs` — **V3-COW-1 (V3-7 MERGE lift):** adopted and created v3 UPDATE and MERGE
  keep `_row_id`; The module doc no longer carries a pins line; citations live here.
  sequential COW DELETE keeps the survivor id at next-row-id 6; **V3-8:** subquery-`WHERE`
  `UPDATE … IN` / `DELETE … IN` keep `_row_id` at next-row-id 6 / 5 and the outside-the-hole
  `NOT IN` UPDATE still refuses `G3-E8` without `V3-COW-1`; padded MoR UPDATE keeps `_row_id`; MoR MERGE matched-update
  is Spark-equal; plain-`WHERE` DELETE including a second MOR DELETE that merges into
  the live vector; v2 control; Hadoop `vN` write; **V3-9:** the MoR subquery-`WHERE` twin —
  `DELETE … IN` at next-row-id 3 / added 0 and `UPDATE … IN` at next-row-id 4 / added 1, each
  with one live `Puffin` delete file
  (pins: v3-9-mor-predicate-dml-dv/C-003; v3-8-subquery-where-lineage/C-002;
  v3-7-merge-lineage/C-002; rp-6-fork-repin/C-002, C-003; rp-3-fork-repin/C-008).
  Hash-pinned by `v3_lineage.rs::cow_keep_refusal_files_are_byte_untouched`.
- `types.rs` — `GEOMETRY` / `GEOGRAPHY` / `VARIANT` refuse at CREATE (`V3-GEO-1`);
  reuses `cow.rs`'s `Door`. **V3-6 C-004:** the `UNKNOWN` column refuses naming the
  type, no table left (pins: v3-6-v3-types/C-004).
- `branch_tag_time_travel.rs` — ANSI branch/tag + `FOR VERSION AS OF` over the partitioned
  v3 DV fixture; RP-3 shared-Puffin DELETE keeps the untouched sibling
  (pins: rp-3-fork-repin/C-004).
- `partitioned_equality_deletes.rs` — ANSI live-row twins of the Spark-written partitioned
  DV and equality-delete + DV fixtures, plus `$delete_files` content 1/2, cross-partition DV
  DELETE, live-DV UPDATE Spark-equal lineage (`Model: Grok 4.6 xHigh`; rp-6-fork-repin/C-003),
  and C-007 ANSI CALL / fork no-op of `rewrite_position_delete_files`
  (pins: rp-3-fork-repin/C-007). **V3-4:** ANSI `_row_id` / `_last_updated_sequence_number`
  on both fixtures, `SELECT *, _row_id` expands user columns only, qualified/aliased forms,
  unquoted case-fold, JOIN/CTE/subquery/`FOR VERSION AS OF` refuse `V3-ROWID-2`, v2 unresolved
  as `No field named _row_id`.
  pins: v3-4-serve-lineage-columns/C-003, C-005, C-007, C-008, C-011, C-012, C-013, C-014,
  C-015, C-016, C-018, C-020

## Pointers

- Up: [../map.md](../map.md)
