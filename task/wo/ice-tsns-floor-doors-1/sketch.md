# Sketch ICE-TSNS-FLOOR-DOORS-1 (U-3, R-009): one kernel body narrows, the batch casts stop narrowing

**Date:** 2026-10-13. **Author:** Claude (claude-opus-5-5), lane B. **Status:** sketch, no product
code; the build waits for the planner head (U-1). **Order:** [../ice-tsns-floor-doors-1.md](../ice-tsns-floor-doors-1.md).
**Step 0:** [evidence/seams.md](evidence/seams.md), [evidence/main-cells.md](evidence/main-cells.md).

## 1. The traced doors

Method: main's release module (product tree of `018d0efd`, version 1.5.5) run under breakpoints on
`timestamp_ns_cast::narrow`, `timestamp_ltz_ntz::ntz_single` and `arrow_cast::cast::cast_with_options`,
session zone America/New_York, format version 3. Each door ran with a `timestamp_ns` source, a
`timestamptz_ns` source and a microsecond control; a function is named only where the nanosecond
run adds hits over the control, and each Arrow cast is attributed to its nearest `repark_` caller.
"Stored" is the tick for `-1 ns` (LTZ: naive, instant; NTZ: naive, instant). The rule gives
`17999999999, -1` and `-1, -18000000001`.

| Door | Target | Function that makes the tick | Site | Stored |
|---|---|---|---|---|
| INSERT (select, VALUES, column list, static `PARTITION`), REPLACE WHERE, `overwrite(cond)`, `writeTo().append()`, `insertInto` append, `saveAsTable` append | LTZ | `narrow` | `repark-functions/src/timestamp_ns_cast.rs:478` | `17999999999, -1` |
| MERGE update arm (all spellings, both modes) | LTZ | `narrow`, through the text `CAST((x) AS TIMESTAMP)` | same; text at `write/ntz_store.rs:89` | `17999999999, -1` |
| UPDATE from a sibling column (both modes) | LTZ | `narrow`; the cast that routes to it is not pinned | same | `17999999999, -1` |
| INSERT OVERWRITE (plain, column list, BY NAME), `overwritePartitions`, `insertInto(overwrite=True)` | LTZ; NTZ naive | `positional_map_overwrite_batch` | `write/overwrite.rs:102`, casts at 171 and 228 | `0, 0`; `0` |
| MERGE insert arm (all spellings, both modes, partitioned) | LTZ | `cast_one_batch_to_write_schema` | `write/merge/mod.rs:554`, cast at `catalog/uuid_presentation.rs:134` | `0, 0` |
| INSERT … BY NAME | LTZ; NTZ naive | `conform_batch_scoped` | `write/conform.rs:142`, cast at 174 | `0, 0`; `0` |
| INSERT OVERWRITE … PARTITION (k = 0) | LTZ; NTZ naive | `store_assign_source_column` | `write/partition_overwrite.rs:91`, cast at 101 | `0, 0`; `0` |
| every other door and source | NTZ | `ntz_single` | `repark-functions/src/timestamp_ltz_ntz.rs:388`, casts at 436 and 439 | `0, -18000000000` |

Paths under `write/` and `catalog/` are in `crates/repark-iceberg/src/`. Three corrections to step 0:
INSERT OVERWRITE does not run the plan EXPLAIN prints. `insert_overwrite_iceberg_stage_then_swap`
(`crates/repark-spark/src/insert_overwrite.rs:411`) executes the bare source query and stages it, so
no `__repark_narrow_timestamp_ns__` ever runs. The MERGE insert arm narrows in the merge staging
cast, not in the store-assignment cast. `store_cast.rs:270` casts float and decimal to integer only
(`is_overflow_store_pair`, line 49); a DataFusion `Cast` node narrows a nanosecond source on no door.

The three NTZ "other" cells: `ntz_single` line 436 truncates the instant toward zero and only then
shifts it to the wall. In Asia/Kolkata `-1 ns` becomes `0`, then `19800000000`; the rule floors to
`19799999999`. The wall is positive there, so the recorder's truncated-wall reference equals the
floor and the label falls to "other". Same defect as New York's "truncated" trio.

## 2. The seam

**Single chokepoint: yes, one function decides the tick.** Today's `narrow(array, zone)` takes the
target type:

    fn store_micros(array: &ArrayRef, target: &DataType, zone: Tz) -> Result<ArrayRef>

in `crates/repark-functions/src/timestamp_ns_cast.rs`. It decides by source type, target type and
session zone, never by a value: a nanosecond source floors (`div_euclid`, as line 488 does); then a
naive source into an instant target is localised in the session zone, an instant source into a
naive target is shifted to the session wall, and the same domain passes through. A source that is
not nanoseconds keeps today's exact Arrow cast, so no microsecond-source cell moves. Two callers:
the LTZ kernel `NarrowTimestampNs::invoke_with_args`, and the `Timestamp` arm of `ntz_single`
(lines 434 to 443), whose two Arrow casts are replaced by the call.

Every door reaches it by type through routers that exist. INSERT-class doors: the analyzer
(`rewrite_cast`, `instant_ts.rs:607` to 616 for LTZ; `retarget_dml_store_casts` for NTZ). Staged
doors: `zone_store_frame` (`write/ntz_store.rs:189`: every INSERT OVERWRITE spelling, static
partition, BY NAME, both DataFrame overwrite doors) and `zone_wrapping_stream_sql`
(`write/merge/insert.rs:105`: MERGE insert). Both ask two predicates, which gain one arm each:
`needs_ltz_instant_cast` (`ntz_store.rs:94`) accepts `Timestamp(Nanosecond, _)`, so the door emits
the same cast to the column type that INSERT plans and the analyzer turns it into the kernel;
`wall_kernel_reads` (`ntz_store.rs:60`) accepts a naive nanosecond source for a microsecond target.

**It is not the conform the type planner touches.** U-1 edits `rewrite_cast` and
`conform_values_timestamp_columns` (line 510), which only widens VALUES rows into nanosecond
columns and narrows nothing. The seam is in the same file and U-1 leaves `narrow` standing. What
U-1 deletes is the `Timestamp(Nanosecond, None)` half of the arm at `instant_ts.rs:607`, and that
half is how MERGE update and the MERGE insert route above reach the kernel: the text
`CAST((x) AS TIMESTAMP)` types as naive nanoseconds on main and as session-zone microseconds under
the planner. Build order: U-1 merges; this unit re-measures on that head; the surviving half of the
arm must rewrite `Cast(Timestamp(ns, _) -> Timestamp(us, Some(_)))` for any zone label.

## 3. What is deleted

- The two Arrow casts in `ntz_single` (436, 439).
- The power of the five batch casts to coarsen a timestamp: `conform.rs:174`, `overwrite.rs:171` and
  228, `partition_overwrite.rs:101`, `merge/mod.rs:561`. One helper beside `strict_cast` in
  `conform.rs`, `strict_store_cast(column: &ArrayRef, target: &DataType) -> Result<ArrayRef>`, returns
  an internal error naming the column when a top-level timestamp's unit would get coarser, and is
  today's strict cast otherwise. A door that misses the routers then fails; it cannot store a second
  answer. Nested leaves stay as on main (R-011, R-015).

Files the build may edit: `timestamp_ns_cast.rs`, `timestamp_ltz_ntz.rs`, `write/ntz_store.rs`,
`write/conform.rs`, `write/overwrite.rs`, `write/partition_overwrite.rs`, `write/merge/mod.rs`; the
fixture, the recorder and its JSON, the registry row. `instant_ts.rs` only for the arm above, and
only after the halt H1 is reported.

## 4. The fixture

`python/repark/tests/timestamps/test_ice_tsns_floor_doors_1.py`, red-first. The step 0 matrix (216
combinations, 2376 ticks) with `rule_expected` of the recorder as the only source of expectations
(floor by `//`, zone by `zoneinfo`). Red on main: 712 ticks (304 LTZ, 408 NTZ); 132 are the v2
UPDATE refusal and stay. Added doors, same derivation, plain and partitioned, copy-on-write and
merge-on-read: INSERT BY NAME, INSERT OVERWRITE BY NAME and with a column list, INSERT OVERWRITE
with a static `PARTITION`, REPLACE WHERE, `overwrite(cond)`, `saveAsTable` append, the star and
literal MERGE spellings. A microsecond source on every door, expected byte-equal to main. The 72
control cells of `ice_tsns_merge_wall_1_main.json` re-recorded on the planner head, then moved to
the floor in step 3. Neighbours green unchanged: `test_ice_tstzns_wall_1.py`,
`test_ice_tsns_merge_wall_1.py`, `test_ice_tsns_frame_door_1.py`.

## 5. Halts and mutants

- H1: on the planner head, before any edit, a door that is 66 of 66 on main differs from the rule.
- H2: any fixture cell raises the `strict_store_cast` error once the routers are extended.
- H3: a microsecond-source cell moves, or INSERT's stored value into `TIMESTAMP` moves.
- H4: main answers a statement the head refuses; cost above 1.05; a file outside section 3.

Mutants, each must die: `div_euclid` becomes `/`; the naive-to-instant localisation is dropped; the
instant-to-wall shift is dropped; `ntz_single` gets its Arrow cast back; `Nanosecond` leaves
`needs_ltz_instant_cast`; the naive arm leaves `wall_kernel_reads`; the last two again with
`strict_store_cast` reduced to the plain cast (must die by value, not by error);
`strict_store_cast` refuses a widening pair.

## 6. Provisional, with its revert test

The refusal in `strict_store_cast` is the one provisional choice; it ships as its own last commit.
Reverting that commit alone on `probe/floor-doors-guard-revert` must leave the fixture and the
three neighbours green, since the routers make the guard unreachable; the count is pasted.

## 7. Risks, each measured on main

1. The NTZ kernel serves reads too. `SELECT CAST(ns AS TIMESTAMP_NTZ)` of `-1 ns` answers `0`
   through `ntz_single`, where `CAST(ns AS TIMESTAMP)` answers the floor. The seam moves it to `-1`.
2. More doors differ than the registry's six: INSERT BY NAME and INSERT OVERWRITE with a static
   `PARTITION` store `0` for `-1 ns` and read the wall as UTC.
3. The wall change is not confined to old values. On the overwrite doors `2026-03-08
   02:30:00.000000001` stores `1772937000000000` in New York where INSERT stores
   `1772955000000000`; every naive nanosecond value in a non-UTC session moves.
4. U-1 removes the arm MERGE update reaches `narrow` through today (section 2); H1 exists for it.

Not traced: which cast routes UPDATE to `narrow` (its assignment text carries none,
`write/predicate_dml/lineage.rs:95`); the ANSI door's own overwrite staging (`crates/repark-sql/src/insert_overwrite.rs`,
which never calls `zone_stores`), streaming sinks, the create door, nested leaves, format version 2
and zones other than New York under breakpoints (step 0 has their values, equal by version).
