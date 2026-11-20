# ICE-HELD-FRAME-SNAPSHOT-1 unit 1a — the bar

**Builds.** Main: `S/ddu-work/pkg-main-9a48932f` (`BUILT_FROM` `9a48932f`), label `main`. Head:
`S/ddu-work/pkg-ice1a-58c7e307` (`BUILT_FROM` `58c7e307`), label `head`. Spark 4.1.2 with the Iceberg 1.11.0 runtime.
The first head `330f6aba` ran the same bar before the same-state fix (`140b260b`, then `58c7e307`); its counts are not
in this record. `S` is the lane scratchpad and `N` the session scratchpad; neither is in the repository (see
[evidence/](evidence/map.md)).

**Verdicts** ([probes/barcmp.py](evidence/probes/barcmp.py)). EQUAL when both engines give the same answer, or both raise at the
top level. RAISE when only the build raises. Any other difference is WRONG, judged on the whole cell answer.

## Deciding cell and new schema-change cells

Spark 4.1.2; both ANSI modes give the same answers ([raw](evidence/map.md)).

- Drop `c`, re-add `c` as STRING, insert: Spark answers `(1, None)`, `(2, None)`, `(9, 'z')`. The check and the
  mapping are by name.
- Widen `INT` to `BIGINT`, and relax `NOT NULL`: `COLUMNS_MISMATCH`. A reorder (`c` first) answers in the frame's
  order. `TRUNCATE`: 0 rows. `SET TBLPROPERTIES`: the same rows (3 after an insert). `RENAME TO`: hadoop refuses the
  rename on both engines.
- Derived writes under `PROHIBIT_CHANGES` pass a reorder and a re-add, by name. A bare `writeTo(copy)` after a reorder
  writes Spark's new order `(c, n)`: a 1b re-plan cell, silent on 1a (column order), a named gap. See the D cells below.
- `collect`, `INSERT OVERWRITE`, `expire_snapshots`, `collect`: Spark's own-plan `collect` raises (the file is gone),
  and `count` answers 1. RePark raises too, with a different error (below).
- `localCheckpoint` after `collect` keeps the first binding (memo shared), as Spark, for the rows. The final count does
  not match: Spark 3, RePark 2 on both builds (below).

## Bar

Counts per ANSI mode (each figure is the same in both modes), from `cmp.txt`:

| group | cells per mode | main EQUAL / WRONG / RAISE | head EQUAL / WRONG / RAISE |
|---|---|---|---|
| `B` (units 1a and 1b) | 490 | 39 / 445 / 6 | 424 / 0 / 66 |
| `C` (cached) | 4 | 0 / 4 / 0 | 2 / 2 / 0 |
| `D` (decide probe) | 16 | 2 / 14 / 0 | 11 / 5 / 0 |
| `HA` | 90 | 20 / 70 / 0 | 80 / 10 / 0 |
| `HW` | 180 | 36 / 144 / 0 | 163 / 17 / 0 |
| `M` | 19 | 5 / 13 / 1 | 17 / 2 / 0 |
| `P` (pinned, refs and metadata) | 84 | 68 / 16 / 0 | 68 / 16 / 0 |
| `Q` (Parquet inverse) | 2 | 0 / 2 / 0 | 0 / 2 / 0 |
| `S` | 15 | 6 / 9 / 0 | 15 / 0 / 0 |
| `WA` | 90 | 8 / 82 / 0 | 85 / 0 / 5 |
| **all** | **990** | **184 / 799 / 7** | **865 / 54 / 71** |

Units (both modes together), from `cmp.txt`. `first` is the comparison's label for the cells outside 1a, 1b and D:

| unit | cells | main EQUAL / WRONG / RAISE | head EQUAL / WRONG / RAISE |
|---|---|---|---|
| 1a | 750 | 74 / 676 / 0 | 750 / 0 / 0 |
| 1b | 230 | 4 / 214 / 12 | 98 / 0 / 132 |
| D | 32 | 4 / 28 / 0 | 22 / 10 / 0 |
| first | 968 | 286 / 680 / 2 | 860 / 98 / 10 |
| **all** | **1980** | **368 / 1598 / 14** | **1730 / 108 / 142** |

- main `9a48932f`: 1598 WRONG, 14 RAISE, 368 EQUAL, 1014 stored (507 per mode).
- head `58c7e307`: 1730 EQUAL, 142 RAISE, 108 WRONG, 8 stored (4 per mode).
- Moved, main to head: 1360 WRONG to EQUAL, 2 RAISE to EQUAL, 130 WRONG to RAISE, 0 EQUAL lost.
- Unit 1a: 750 of 750 EQUAL on the head.
- Gap 1b: 98 EQUAL, 132 RAISE, 0 WRONG on the head (so no silent cell in 1b). Main: 4 EQUAL, 214 WRONG, 12 RAISE.
- The 68 pinned cells (group `P` EQUAL) stay EQUAL on the head.
- Stored rows: step 0: 785 of 974 cells per build and mode WRONG, all silent, 509 stored; this build's main run stores
  the old rows in 507 per mode.

## Zero-inner-load proof

`repark-spark held_iceberg::tests::a_held_action_makes_no_catalog_load_with_or_without_a_write`: a counting catalog
under the decorator sees 0 `load_table` calls for a held `collect` with no write and after a same-session `INSERT`.
The wrapper never reaches a stored plan (frame or DataFrame view), asserted.

## Frozen epoch signature

`repark_iceberg::catalog::session_tables::table_epoch(&SessionConfig, catalog: &str, &TableIdent) -> Option<u64>` and
`SessionTables::epoch(&self, &str, &TableIdent) -> u64`; the epoch moves on commit, drop, create, rename, refresh and a
new load.

## Still WRONG on the head

On the head, 250 cells are not EQUAL: 132 RAISE in gap 1b, 10 RAISE and 98 WRONG in `first`, and 10 WRONG in D. Per
ANSI mode that is 125: 54 WRONG and 71 RAISE. The head stores 8 cells (4 per mode).

| family | verdict | per mode | where |
|---|---|---|---|
| path read, `HA` and `HW` (`path`) | WRONG | 27 | Q4; its own card |
| refs and metadata tables, `P` | WRONG | 16 | Q5, shed: `branch_id` 2, `branch_opt` 2, `tag_id` 2, `files` 2, `history` 4, `snapshots` 4 |
| Parquet inverse, `Q` | WRONG | 2 | `parquet_acted` 1, `parquet_never` 1 |
| cached cells, `C` | WRONG | 2 | `cache_append` 1, `cache_count` 1 (4 in all; unit 2) |
| hadoop REFRESH after an external commit, `M` | WRONG | 1 | `refresh_sql_external` |
| cast, `M` | WRONG | 1 | `held_cast`, not named in the hand-back: ANSI on, Spark `CAST_INVALID_INPUT`, head an Arrow cast error; ANSI off, Spark returns the rows, head raises an Arrow cast error |
| D cells | WRONG | 5 | the table below |
| gap 1b | RAISE | 66 | 132 in all, 0 WRONG |
| table copies, `WA` | RAISE | 5 | `WA` table after `alter_add`, `alter_drop`, `alter_rename`, `recreate` and `wt_create` (copy); `first`, not named in the hand-back |

### D cells (both ANSI modes give the same answers)

| cell | Spark | main | head | class |
|---|---|---|---|---|
| `widen_int_bigint` | held and derived: `COLUMNS_MISMATCH`; copy: `(1,'a') (2,'b') (9,'z')` | held and derived: count 2 and two rows; copy: `(1,'a') (2,'b')` | held and derived: `COLUMNS_MISMATCH`; copy: `COLUMNS_MISMATCH` | bare `writeTo` re-plan (1b), on the copy; the head raises where Spark writes |
| `relax_not_null` | held and derived: `COLUMNS_MISMATCH`; copy: `(1,'a') (2,'b') (None,'z')` | held and derived: count 2 and two rows; copy: `(1,'a') (2,'b')` | held and derived: `COLUMNS_MISMATCH`; copy: `COLUMNS_MISMATCH` | bare `writeTo` re-plan (1b), on the copy; the head raises where Spark writes |
| `reorder_c_first` | held and derived: count 3 and `(1,'a') (2,'b') (9,'z')`; copy: `('a',1) ('b',2) ('z',9)` | held and derived: count 2 and two rows; copy: `(1,'a') (2,'b')` | held and derived: count 3 and three rows; copy: `(1,'a') (2,'b') (9,'z')` | bare `writeTo` re-plan (1b), on the copy; SILENT (column order `(n, c)` where Spark writes `(c, n)`); tagged `unit D` |
| `local_checkpoint_after_collect` | `[2 rows, count 2, 2 rows, count 3]` | `[2 rows, count 2, 2 rows, count 2]` | `[2 rows, count 2, 2 rows, count 2]` | not a re-plan; SILENT (final count 2, Spark 3, on both builds); tagged `unit D`; not assigned to 1a, 1b or unit 2 |
| `own_collect_ow_expire_collect` | `[2 rows, SparkException (job aborted), count 1]` | `[2 rows, DataInvalid (failed to open file), count 2]` | `[2 rows, DataInvalid (failed to open file), count 1]` | not a re-plan; both raise on the second `collect`, with different errors (Spark's SparkException, RePark's DataInvalid); tagged `unit D`; not assigned to any unit |

The bare-`writeTo` re-plans are the `copy` answer of the first three rows. Held and derived match Spark in each, so only
the copy differs.

**The silent cell.** `D|table|reorder_c_first` (its copy answer, both modes) is WRONG on both builds with no error:
RePark writes `(1, 'a')`, `(2, 'b')`, `(9, 'z')` where Spark writes `('a', 1)`, `('b', 2)`, `('z', 9)`. The verdict file
tags it `unit D`, so the gap's "0 silent" holds only with D excluded. `local_checkpoint_after_collect` is silent in the
same way (count 2 against Spark's 3, on both builds) and is tagged D too.

## The cells round 0 left open (fix round 1, F3)

Each class is taken from the verdict files and checked against a fresh frame on main and on the head.

| cell (per ANSI mode) | Spark | main | head | class |
|---|---|---|---|---|
| `M\|held_cast` (1) | ANSI on `CAST_INVALID_INPUT`; ANSI off the rows, `'zz'` as `NULL` | the old two rows | `Cannot cast string 'zz' to value of Int32 type` | **not 1a's: an existing card.** A fresh frame raises the same Arrow cast error on main and on the head, in both modes (and so does an in-memory frame), so the held frame now answers as a fresh read does. The cast is [STRING-CAST-LENIENT-1](../../../roadmap/mid-term/string-cast-lenient-1-card-2026-11-16.md) (junk text to the INT family refuses loudly). |
| `D\|table\|local_checkpoint_after_collect` (1) | final `held.count()` 3 | 2 | 2 | **not 1a's: an existing card.** RePark's `localCheckpoint` returns its receiver, materialised, so the later `held.count()` reads the checkpoint; Spark returns a new Dataset and the original re-binds. [LOCAL-CHECKPOINT-NEW-FRAME-1](../../../roadmap/mid-term/local-checkpoint-new-frame-1-card-2026-10-03.md). The checkpoint's own rows and count match Spark. |
| `D\|table\|own_collect_ow_expire_collect` (1) | `SparkException` (job aborted, the file is gone), then count 1 | `DataInvalid` (failed to open file), then count 2 | `DataInvalid`, then count 1 | **its own residual**, carded in [cards.md](cards.md) (ICE-EXPIRED-FILE-READ-ERROR-1): both engines raise on the deleted file, with a different error; the binding agrees. |
| `WA\|table\|{alter_add, alter_drop, alter_rename, recreate, wt_create}\|copy` (5) | the new table's rows | the old rows | `COLUMNS_MISMATCH` or `TABLE_ID_MISMATCH` | **1b's.** The source is a bare `s.table(t)` and the action is `writeTo(copy).create()`: Spark re-plans the bare frame by name, which is 1b's re-plan; 1a raises loud where 1b will answer, as in group `B`'s 66 gap cells. |

None of the four classes is fixed in this round: none is 1a's.

## The 8 stored cells (fix round 1, F4)

The head `58c7e307` stores old rows in 8 cells (4 per mode). **None of them is a bare-`writeTo` re-plan, so none is
1b's: all 8 are the path read** (Q4 (b), [ICE-HELD-PATH-READ-1](../../../roadmap/mid-term/cards-2026-11-19/ice-held-path-read-1-card-2026-11-20.md)),
a frame held from `spark.read.format("iceberg").load(<path>)`.

| cell (both ANSI modes) | Spark stores | the head stores |
|---|---|---|
| `HA\|path\|delete\|copy` | `(2, 'b')` | `(1, 'a')`, `(2, 'b')` |
| `HA\|path\|delete\|insel` | `(2, 'b')` | `(1, 'a')`, `(2, 'b')` |
| `HA\|path\|insert\|copy` | `(1, 'a')`, `(2, 'b')`, `(9, 'z')` | `(1, 'a')`, `(2, 'b')` |
| `HA\|path\|insert\|insel` | `(1, 'a')`, `(2, 'b')`, `(9, 'z')` | `(1, 'a')`, `(2, 'b')` |

`copy` is `writeTo(<new>).create()` and `insel` is `INSERT INTO <new> SELECT * FROM` a view of the frame.

## Gates

The fix round's gates are in [cost.md](cost.md)'s sibling section below and in the hand-back; the PR carries them.

Up: [map.md](map.md).
