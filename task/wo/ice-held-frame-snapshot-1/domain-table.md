# ICE-HELD-FRAME-SNAPSHOT-1 — the domain table (Spark 4.1.2, main, main before unit 1; both ANSI modes)

Every cell: an Iceberg v2 table `lc.db.tN (n INT, c STRING)` on a local `type=hadoop` catalog, seeded with
`(1, 'a'), (2, 'b')` in one commit (`PARTITIONED BY (c)` only for the two dynamic-overwrite writes); a frame or view
**held** (built before the write); one **write** to the same table in the same session; then one **action** on the
held object. Probes: [evidence/probes/cells.py](evidence/probes/cells.py). Raw cells:
[evidence/raw/](evidence/raw/map.md). Verdicts: [evidence/raw/verdicts.jsonl.gz](evidence/raw/verdicts.jsonl.gz),
made by [evidence/probes/cmp.py](evidence/probes/cmp.py).

**Marks (main against Spark):** `E` EQUAL (both raising is EQUAL by class); `W` WRONG, RePark answers other rows,
silently; `w` WRONG where Spark raises and RePark silently answers the old rows; `S` WRONG and **stored**: the held
frame's old rows are written into an Iceberg table; `R` RAISE where Spark answers (none occur). **Base equals main in
every cell, and no cell's verdict moves with the ANSI mode**, so each mark stands for four cells (main and base, ANSI
on and off).

## 0. Totals

| | cells per build and mode | main | base |
|---|---|---|---|
| verdict cells (groups `HW`, `WA`, `HA`, `P`, `S`, `C`, `Q`) | 465 | 327 WRONG, 138 EQUAL, 0 RAISE | identical |
| of the WRONG: stored into an Iceberg table (`S`) | | 62 | 62 |
| of the WRONG: Spark raises, RePark answers (`w`, and 2 of the `S`) | | 44 | 44 |
| external-writer cells (`X`, described in §6, no verdict) | 6 | | |

Over both builds and both modes: 1,860 verdict cells, **1,308 WRONG, all silent**, 552 EQUAL, 0 RAISE; 248 stored.
The WRONG set by the seat that would move it is in §7.

## 1. Holds × writes (`HW`), action `collect` (sorted rows)

Holds: `spark.table(t)`, `spark.sql("SELECT * FROM t")`, `read.table(t)`, the path read
`read.format("iceberg").load(<table location>)`, a `filter`, a `groupBy("c").count()`, a DataFrame view
(`createOrReplaceTempView`, then SQL over it after the write), a SQL `CREATE TEMPORARY VIEW … AS SELECT`, and each side
of a join with a fresh `spark.table(t)` on `n, c`. Writes: none (the control); `INSERT`; `DELETE … WHERE n = 1`;
`UPDATE … SET n = n + 10`; `MERGE` (update `n = 2`, insert `n = 9`); static `INSERT OVERWRITE`; dynamic `INSERT
OVERWRITE` (`partitionOverwriteMode=dynamic`); `writeTo` `append`, `overwrite(n > 0)`, `overwritePartitions`,
`replace`, `createOrReplace`, and `create` after a `DROP`; RTAS (`CREATE OR REPLACE TABLE … AS SELECT`); `ALTER`
add, drop, rename a column, then an `INSERT`; drop and recreate, then an `INSERT`. Main answers `(1,a) (2,b)` in every
non-view cell.

| write | Spark (`table`) | `table` | `sql` | `read` | `path` | `filter` | `agg` | `dfview` | `sqlview` | `join_l` | `join_r` |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `none` | (1,a) (2,b) | E | E | E | E | E | E | E | E | E | E |
| `insert` | (1,a) (2,b) (9,z) | W | W | W | W | W | W | W | E | W | W |
| `delete` | (2,b) | W | W | W | W | W | W | W | E | E | E |
| `update` | (11,a) (12,b) | W | W | W | W | W | E | W | E | W | W |
| `merge` | (1,a) (2,y) (9,z) | W | W | W | W | W | W | W | E | W | W |
| `ins_ow` | (7,o) | W | W | W | W | W | W | W | E | W | W |
| `ins_ow_dyn` | (2,b) (5,a) | W | W | W | W | W | E | W | E | W | W |
| `wt_append` | (1,a) (2,b) (9,z) | W | W | W | W | W | W | W | E | W | W |
| `wt_overwrite` | (7,o) | W | W | W | W | W | W | W | E | W | W |
| `wt_ow_parts` | (2,b) (5,a) | W | W | W | W | W | E | W | E | W | W |
| `wt_replace` | (7,o) | W | W | W | W | W | W | W | E | W | W |
| `wt_cor` | (7,o) | W | W | W | W | W | W | W | E | W | W |
| `wt_create` | `…TABLE_ID_MISMATCH` | w | w | w | w | w | w | W | E | w | w |
| `rtas` | (7,o) | W | W | W | W | W | W | W | E | W | W |
| `alter_add` | (1,a) (2,b) (9,z) | W | W | W | W | W | W | W | E | W | W |
| `alter_drop` | `…COLUMNS_MISMATCH` | w | w | w | w | w | w | w | E | E | E |
| `alter_rename` | `…COLUMNS_MISMATCH` | w | w | w | w | w | w | w | E | E | E |
| `recreate` | `…TABLE_ID_MISMATCH` | w | w | w | w | w | w | W | E | w | w |

144 WRONG and 36 EQUAL per build and mode. `…` is `INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.`. Notes:

- **The path read is refreshed by Spark** like a catalog table (IcebergSource hands Spark a catalog and an
  identifier), including its `TABLE_ID_MISMATCH`; RePark's path read is a static provider (inventory F4).
- **A DataFrame view** raises `INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION` on Spark after a dropped
  or renamed column, and answers the **new** table after a drop and recreate (`(5,r)`, `(7,o)`): the view is
  re-resolved by name, without the identity check.
- **The SQL view is EQUAL everywhere** (it re-plans its text, inventory V2), including the two raises.
- The `E` cells of `agg` (`update`, `ins_ow_dyn`, `wt_ow_parts`: the per-`c` counts do not change) and of the joins
  (`delete`, and the two column changes, where both engines fail the `USING` join, `UNRESOLVED_USING_COLUMN_FOR_JOIN`
  on Spark) are EQUAL by coincidence; the fix keeps them EQUAL.
- After `ALTER … ADD COLUMN`, Spark's held frame keeps its two columns and reads all three rows; the join with a fresh
  read answers `(1,a,None) (2,b,None) (9,z,5)` on Spark, `(1,a,None) (2,b,None)` on main.

## 2. Writes × actions (`WA`), hold `spark.table(t)`

Actions: `count`, `show` (the printed rows), `held.writeTo(t_copy).create()` then read the copy (`copy`), the held
frame registered as a view **after** the write, `CREATE TABLE u AS SELECT * FROM v WHERE 1 = 0` and `INSERT INTO u
SELECT * FROM v`, then read `u` (`insel`), and a self-join `held.alias("x").join(held.alias("y"), "n")` (`selfjoin`).

| write | `copy` | `count` | `insel` | `selfjoin` | `show` |
|---|---|---|---|---|---|
| `none` | E | E | E | E | E |
| `insert`, `delete`, `merge`, `ins_ow`, `wt_append`, `wt_overwrite`, `wt_replace`, `wt_cor`, `rtas`, `alter_add` | S | W | S | W | W |
| `update`, `ins_ow_dyn`, `wt_ow_parts` | S | E | S | W | W |
| `alter_drop`, `alter_rename` | S | w | S (Spark raises) | w | w |
| `wt_create`, `recreate` | S | w | S | w | w |

82 WRONG and 8 EQUAL per build and mode; 34 stored. The three `count` cells marked `E` keep two rows on both engines.

**A write of the held frame re-resolves the table by name on Spark.** After a schema or identity change, where
`count`, `show` and the self-join raise, `held.writeTo(copy).create()` answers the **current** table in its current
shape: `(1,a,None) (2,b,None) (9,z,5)` after `ADD COLUMN`, `(1,) (2,) (9,)` after `DROP COLUMN`, `(1,a) (2,b) (9,z)`
after `RENAME COLUMN`, `(5,r)` after a recreate, `(7,o)` after `DROP` then `writeTo().create()`. The view of the held
frame made after the write answers the current rows (`insel`), raises
`INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION` after a dropped or renamed column, and keeps the frame's
two columns after an added one. Main stores the two seeded rows in all of these. (Q6.)

## 3. Holds × actions (`HA`), writes `INSERT` and `DELETE`

| hold | `delete`: `copy` `count` `insel` `selfjoin` `show` | `insert`: `copy` `count` `insel` `selfjoin` `show` |
|---|---|---|
| `sql`, `read`, `path`, `filter`, `agg`, `dfview` | S W S W W | S W S W W |
| `join_l`, `join_r` | E E E E E | S W S W W |
| `sqlview` | E E E E E | E E E E E |

70 WRONG and 20 EQUAL per build and mode; 28 stored. The joins after a `DELETE` are EQUAL because the inner join with
a fresh read drops the deleted row on both engines. **A deleted row is returned** by `count`, `show`, `collect`,
the self-join, and is **stored** by `copy` and `insel`, for every hold but the SQL view.

## 4. Frames that must stay pinned, refs and metadata tables (`P`)

Each table also has a branch `b` and a tag `g` at the seed snapshot. Writes: `INSERT` and `DELETE` on `main`,
`INSERT INTO t.branch_b` (`b_insert`), `INSERT` then `ALTER TABLE … REPLACE TAG g` (`tag_move`). Actions: `count`
and `collect` (the row count of `collect` for the metadata tables).

| hold | `b_insert` | `delete` | `insert` | `tag_move` | Spark's rule |
|---|---|---|---|---|---|
| `VERSION AS OF <id>`, `read.option("versionAsOf")`, `TIMESTAMP AS OF <commit time>`, `TIMESTAMP AS OF <a day later>` | E E | E E | E E | E E | pinned (`timeTravelSpec`) |
| `VERSION AS OF 'b'`, `VERSION AS OF 'g'` | E E | E E | E E | E E | pinned |
| `t.branch_b`, `read.option("branch", "b")` | **W W** | E E | E E | E E | refreshed: reads the branch head |
| `t.tag_g` | E E | E E | E E | **W W** | refreshed: reads the tag's current snapshot |
| `t.snapshots`, `t.history` | — | **W W** | **W W** | — | refreshed |
| `t.files` | — | E E | **W W** | — | refreshed (after the `DELETE` both list one file) |

16 WRONG and 68 EQUAL per build and mode. **Every time-travel cell is EQUAL and must stay so.** The branch and tag
identifier forms, the reader's `branch` option and the metadata tables are WRONG: Spark re-reads them at the action,
RePark binds them at resolution (inventory T3, T4). Fable's §13 listed metadata tables and branch and tag reads among
the frames that must stay pinned; Spark pins only the `VERSION AS OF` / `TIMESTAMP AS OF` forms.

## 5. Action sequences (`S`), the cached cells (`C`) and the Parquet inverse (`Q`)

A held `spark.table(t)` and the listed steps; the answers in order.

| cell | Spark | main | |
|---|---|---|---|
| `collect`, `INSERT`, `collect` | [(1,a) (2,b)], [(1,a) (2,b)] | the same | E |
| `collect`, `INSERT`, `toLocalIterator` | [(1,a) (2,b)], [(1,a) (2,b)] | the same | E |
| `collect`, `INSERT`, `tail(10)` | [(1,a) (2,b)], [(1,a) (2,b)] | the same | E |
| `toLocalIterator`, `INSERT`, `toLocalIterator` | [(1,a) (2,b)], [(1,a) (2,b)] | the same | E |
| `explain`, `INSERT`, `collect` | [(1,a) (2,b)] | the same | E |
| `collect`, `DELETE`, `collect` | [(1,a) (2,b)], [(1,a) (2,b)] | the same | E |
| `collect`, `INSERT`, `count` | [(1,a) (2,b)], 3 | …, 2 | W |
| `collect`, `INSERT`, `take(10)` | …, [(1,a) (2,b) (9,z)] | …, [(1,a) (2,b)] | W |
| `collect`, `INSERT`, `show` | …, 1 a, 2 b, 9 z | …, 1 a, 2 b | W |
| `collect`, `INSERT`, a child `filter("n > 0").collect` | …, [(1,a) (2,b) (9,z)] | …, [(1,a) (2,b)] | W |
| `count`, `INSERT`, `collect` | 2, [(1,a) (2,b) (9,z)] | 2, [(1,a) (2,b)] | W |
| `columns`, `INSERT`, `collect` | [n c], [(1,a) (2,b) (9,z)] | [n c], [(1,a) (2,b)] | W |
| `INSERT`, `collect`, `INSERT`, `collect`, `count` | [3 rows], [3 rows], 4 | [2 rows], [2 rows], 2 | W |
| `count`, `INSERT`, `count`, `INSERT`, `count`, fresh `count` | 2, 3, 4, 4 | 2, 2, 2, 4 | W |
| empty table held; `INSERT`; `count`, `collect` | 1, [(9,z)] | 0, [] | W |
| `cache()`, `count`, `INSERT`, `count`, fresh `count` | 2, 3, 3 | 2, 2, 3 | W |
| `cache()` (lazy), `INSERT`, `count`, fresh `count` | 3, 3 | 2, 3 | W |
| `CACHE TABLE t`, `INSERT`, held `count`, fresh `count` | 3, 3 | 2, 3 | W |
| `cache()`, `count`, `writeTo(t).append()`, `count`, fresh `count` | 2, 3, 3 | 2, 2, 3 | W |
| Parquet: held `read.parquet`, `count`, append, `count`, fresh `count` | 1, 2 | 2, 2 | W |
| Parquet: the same, never acted before the append | 1, 2 | 2, 2 | W |

**The first six rows are EQUAL only because main is stale and Spark pins a Dataset's own plan** (sketch §1 point 3):
a fix that re-resolves every action would turn these 12 cells per build WRONG, the deleted-row one included. They
are the must-hold set for the own-plan memo. The four cached rows need A1's lazy cache table to drop its fill on a
same-session write (sketch §5). The Parquet rows are Fable's inverse cell: RePark is fresher than Spark (S3).

## 6. The external-writer control (`X`)

The held frame is acted on once (`acted`) or not (`never`); another session or process inserts `(9, 'z')`; then the
held frame's `count` and rows and a fresh `spark.table(t).count()`; then `catalog.refreshTable(t)` and both counts
again; in `own`, a same-session `INSERT` after the external one.

| writer | Spark: held, fresh → after `refreshTable` | main and base: held, fresh → after `refreshTable` |
|---|---|---|
| a second session in the process (`newSession()`) | 2, 2 → 3, 3; `own`: 4, 4 → 4, 4 | the second session cannot see the table (`table … not found`): a `type=hadoop` catalog is per session |
| a second process, the other engine | the RePark writer cannot see Spark's table (the same reason); not measured | Spark writes: 2, 2 → 2, 2; `own`: `CatalogCommitConflicts` (the in-process pointer is behind the external `vN`, and the commit refuses; nothing clobbered) |

- **Spark does not over-refresh:** after an external commit its held frame and a fresh read both answer the cached
  table (2) until `refreshTable`; after the refresh both answer 3. A same-session commit after the external one
  refreshes the base (4, 4).
- **RePark on `type=hadoop` never sees an external commit,** held or fresh, refreshed or not: the catalog's pointer
  lives in the session (inventory R6). The held frame and the fresh read agree, as on Spark before its refresh. After
  `refreshTable` they stay 2 where Spark answers 3: a pre-existing divergence of the local catalog, not this card's.
- **Not measurable here:** an external writer RePark can see. On Glue and S3 Tables every planning `load_table` reads
  the catalog's pointer (inventory R7, R8), so a fresh RePark read sees an external commit at once where Spark waits for
  a refresh; a held frame keeps its resolution snapshot today. The seats' answers there are in sketch §4 (a read, not
  a measurement).

## 7. The WRONG set by what moves it (per build and mode)

| family | WRONG (all silent) | stored | Spark raises | moved by |
|---|---|---|---|---|
| catalog-table frames in `HW`, `WA`, `HA` (`table`, `sql`, `read`, `filter`, `agg`, the joins) | 242 | 54 | 38 | unit 1 |
| DataFrame views in `HW`, `HA` | 27 | 4 | 2 | unit 1 (the same rebinding point; the view's own rules, Q6) |
| the path read in `HW`, `HA` | 27 | 4 | 4 | unit 1 only under Q4 (a); else a separate card |
| refs and metadata tables, `P` | 16 | 0 | 0 | unit 1 |
| action sequences, `S` | 9 | 0 | 0 | unit 1 (the memo keeps §5's six `E` rows) |
| cached, `C` | 4 | 0 | 0 | unit 2, after A1 |
| Parquet inverse, `Q` | 2 | 0 | 0 | none: a named divergence (S3) |
| **total** | **327** | **62** | **44** | |

The EQUAL cells a fix must keep (per build and mode): the 68 pinned and ref `P` cells, the 28 SQL-view cells, the
six own-plan `S` rows, the 14 `none` controls outside the SQL view, and the 22 coincidental `E` cells of §1 to §3
(68 + 28 + 6 + 14 + 22 = 138).

Up: [map.md](map.md).
