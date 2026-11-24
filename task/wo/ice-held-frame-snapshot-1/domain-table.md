# ICE-HELD-FRAME-SNAPSHOT-1 — the domain table (Spark 4.1.2, main, main before unit 1; both ANSI modes)

Every cell: an Iceberg v2 table `lc.db.tN (n INT, c STRING)` on a local `type=hadoop` catalog, seeded with
`(1, 'a'), (2, 'b')` in one commit (`PARTITIONED BY (c)` only for the two dynamic-overwrite writes); a frame or view
**held** (built before the write); one **write** to the same table in the same session; then one **action** on the
held object. Probes: [evidence/probes/cells.py](evidence/probes/cells.py). Raw cells:
[evidence/raw/](evidence/raw/). Verdicts: [evidence/raw/verdicts.jsonl.gz](evidence/raw/verdicts.jsonl.gz),
made by [evidence/probes/cmp.py](evidence/probes/cmp.py).

**Marks (main against Spark):** `E` EQUAL (both raising is EQUAL by class); `W` WRONG, RePark answers other rows,
silently; `w` WRONG where Spark raises and RePark silently answers the old rows; `S` WRONG and **stored**: the held
frame's old rows are written into an Iceberg table; `R` RAISE where Spark answers (none occur). **Base equals main in
every cell, and no cell's verdict moves with the ANSI mode**, so each mark stands for four cells (main and base, ANSI
on and off).

## 0. Totals (corrected by the revision round, §8)

| | cells per build and mode | WRONG (silent) | stored | Spark raises, RePark answers | RAISE | EQUAL |
|---|---|---|---|---|---|---|
| first pass (`HW`, `WA`, `HA`, `P`, `S`, `C`, `Q`) | 465 | 327 | 62 | 44 | 0 | 138 |
| revision: writes of a held frame (`B`, §8.1) | 490 | 445 | 445 | 207 | 6 | 39 |
| revision: the missed shapes (`M`, §8.2) | 19 | 13 | 2 | 1 | 1 | 5 |
| **total** | **974** | **785** | **509** | **252** | **7** | **182** |
| external-writer cells (`X`, §6, no verdict) | 6 | | | | | |

Base equals main in every cell, and no verdict moves with the ANSI mode. Over both builds and both modes: 3,896
verdict cells, **3,140 WRONG, all silent**, 2,036 of them stored into an Iceberg table, 28 RAISE where Spark answers
(the first version's "0 RAISE" was wrong), 728 EQUAL. Eight stored shapes (§8). The WRONG set by the unit that would move
it is in §7.

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

**For this bare hold, `writeTo` re-plans the frame by name on Spark** (revised: §8.1 shows that it holds only for
`writeTo` of a frame built from a name or SQL text; a derived frame's `writeTo` and every `insertInto` /
`saveAsTable` raise `INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS`). After a schema or identity change, where
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
a fix that re-resolves every action would turn these 12 cells per build WRONG (22 with §8.2's five), the deleted-row one included. They
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

## 7. The WRONG and RAISE set by what moves it (per build and mode, revised)

| family | WRONG or RAISE | stored | moved by |
|---|---|---|---|
| first pass: catalog-table frames, DataFrame views, refs and metadata tables, sequences | 294 | 58 | unit 1a, except the 5 `WA` `copy` cells after a schema or identity change (unit 1b) |
| first pass: the path read | 27 | 4 | unit 1a only under Q4 (a); else a separate card |
| first pass: cached, `C` | 4 | 0 | unit 2, after A1 |
| first pass: Parquet inverse, `Q` | 2 | 0 | none: a named divergence (S3) |
| `B`: write paths after `INSERT` or `DELETE`, and after a schema or identity change where Spark reads by name in a view or raises a query check | 338 | 338 | unit 1a |
| `B`: a name- or text-built frame's `writeTo` after a schema or identity change, and an added column under a write | 113 (6 of them RAISE) | 107 | unit 1b (Q6 (a)) |
| `M`: the `CALL`s, spec and sort changes, the other-namespace recreate, the `MERGE` source, two held frames, the held join, ten rounds, the cast, the self-append, `first` | 13 | 2 | unit 1a (the three `CALL`s under Q11) |
| `M`: `REFRESH TABLE` after an external commit | 1 | 0 | none on `type=hadoop` (the catalog never sees the commit); the AWS leg's cell (Q10) |
| **total** | **792** | **509** | unit 1a 640, unit 1b 118, unit 2 4, Q4 27, none 3 |

The EQUAL cells a fix must keep (per build and mode, 182): the 68 pinned and ref `P` cells, the 28 SQL-view cells,
the 11 own-plan rows (six in §5, five in §8.2), the 14 `none` controls outside the SQL view, the 22 coincidental `E`
cells of §1 to §3, and the 39 `B` cells where both engines refuse the same write (the join side's `USING` failure,
`agg`'s self-append and the like).

## 8. The revision round (the critic's required items; [evidence/probes/rev.py](evidence/probes/rev.py))

The same builds, interpreter, Spark, catalog and modes. Spark ran with pandas linked into its path (from the RePark
interpreter's environment, both Python 3.12.3) for the `toPandas` cells. Raw: `evidence/raw/rev-{spark,main,base}.jsonl.gz`.
Verdicts: [evidence/raw/rev-verdicts.jsonl.gz](evidence/raw/rev-verdicts.jsonl.gz). Base equals main in every cell
and no verdict moves with ANSI.

### 8.1 Writes of a held frame (`B`): 10 holds × 7 writes × 7 write paths, 490 cells per build and mode

Holds: the bare `spark.table`, `read.table`, `spark.sql("SELECT * FROM t")`, and `spark.sql("SELECT n, c FROM t
WHERE n > 0")` (`sqlf`, built from SQL text but not bare); the derived `filter`, `select("n")`, `select("n", "c")`
(`sel_all`), `groupBy("c").count()`, a join side; a DataFrame view made before the write. Writes: `INSERT`, `DELETE`,
`ALTER` add, drop, rename, drop and recreate, `DROP` then `writeTo().create()`. Write paths (each reads its target
back, columns and rows): `writeTo(copy).create()` (`copy`), `writeTo(new).createOrReplace()` (`cor`),
`writeTo(other).append()` into a table made before the write (`app`), `write.insertInto(other)` (`ins_into`),
`write.saveAsTable(new)` (`sat`), a view of the frame made after the write and `INSERT INTO other SELECT * FROM view`
(`insel`), and `writeTo(t).append()` into its own table (`self`).

**Spark's answer after a schema or identity change** (ANSI on; off is identical). `new` is the current table read by
name (the shape it gives), `COL` / `ID` are `INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH` /
`.TABLE_ID_MISMATCH`, `VIEW` is `INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION`, `tgt` a target-side
arity or type refusal, `txt` the SQL text failing to resolve a dropped or renamed column (`UNRESOLVED_COLUMN`), `join`
the `USING` join failing on both engines.

| hold | write | `copy` | `cor` | `app` | `ins_into` | `sat` | `insel` | `self` |
|---|---|---|---|---|---|---|---|---|
| `table`, `read`, `sql` | add | new (n,c,d) | new (n,c,d) | tgt | COL | COL | new (n,c) | new (n,c,d) |
| | drop | new (n) | new (n) | new (n, c NULL) | COL | COL | VIEW | new (n) |
| | rename | new (n,e) | new (n,e) | tgt | COL | COL | VIEW | new (n,e) |
| | recreate, `wt_create` | new | new | new | ID | ID | new | new |
| `sqlf` | add | new (n,c) | new (n,c) | new (n,c) | COL | COL | new (n,c) | new (n,c,d) |
| | drop, rename | txt | txt | txt | COL | COL | VIEW | txt |
| | recreate, `wt_create` | new | new | new | ID | ID | new | new |
| `filter`, `select`, `sel_all`, `agg` | add, drop, rename | COL | COL | COL | COL | COL | new (frame's columns) for add, VIEW for drop and rename | COL or tgt |
| | recreate, `wt_create` | ID | ID | ID | ID | ID | new | ID (`agg`: tgt) |
| join side | add | COL | COL | tgt | tgt | COL | tgt | COL |
| | drop, rename | join | join | join | join | join | join | join |
| | recreate, `wt_create` | ID | ID | ID | ID | ID | new | ID |
| DataFrame view | add, recreate, `wt_create` | new (n,c) | new (n,c) | new (n,c) | new (n,c) | new (n,c) | new (n,c) | new (n,c,d) for add, new (n,c) otherwise |
| | drop, rename | VIEW | VIEW | VIEW | VIEW | VIEW | VIEW | VIEW |

**Spark's rule, read from these cells.** (1) `writeTo(…)` (`create`, `createOrReplace`, `append`, the self-append)
of a frame **built from a name or SQL text** re-plans it from that name or text: the current table, in the shape the
text gives (`SELECT *` gives the new columns; an explicit list keeps its columns; a text naming a dropped column fails
to resolve). (2) `writeTo` of a **derived** frame (any DataFrame-API operation, `select("n", "c")` included), and
`write.insertInto` / `write.saveAsTable` of **any** frame, are checked under `PROHIBIT_CHANGES`: `COL` for any column
change, an added one included, `ID` for a new table. (3) A **view** (made before or after the change) re-resolves its
tables by name with no identity check: the current rows in the view's columns, `VIEW` for a dropped or renamed column.

**Main** answers or stores the seeded rows (`(1,a) (2,b)`), silently, in every cell where the write succeeds; where
Spark answers `new` after a drop or rename, main's self-append is refused (6 RAISE: `INSERT_COLUMN_ARITY_MISMATCH`,
`INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS`). After `INSERT` and `DELETE` every write path of every hold stores the
seeded rows on main (the deleted row included) where Spark stores the current ones, except `agg`'s self-append (both
engines refuse the shape); the join side after a `DELETE` and after a dropped or renamed column is EQUAL (the inner join drops the row; both engines fail the `USING` join).

| per build and mode | WRONG | of which stored | Spark raises, main stores | RAISE | EQUAL |
|---|---|---|---|---|---|
| `B` | 445 | 445 | 207 | 6 | 39 |

### 8.2 The missed shapes (`M`), one cell each per build and mode

| shape | Spark | main | |
|---|---|---|---|
| `INSERT`; hold; `CALL rollback_to_snapshot(<seed>)`; held count and rows, fresh count | 2, (1,a) (2,b), 2 | 3, with (9,z), 2 | W |
| `INSERT`; hold; `CALL rewrite_data_files`; `DELETE n = 1`; count, rows | 2, (2,b) (9,z) | 3, the deleted row included | W |
| hold; `INSERT OVERWRITE`; `CALL expire_snapshots(retain_last => 1)`; count, rows | 1, (7,o) | **raises** `Failed to load manifest list` | R |
| hold; `ALTER TABLE … ADD PARTITION FIELD c`; `INSERT`; count, rows | 3 | 2 | W |
| hold; `ALTER TABLE … WRITE ORDERED BY n`; `INSERT`; count, rows | 3 | 2 | W |
| hold `db.t`; `DROP`; create and fill `db2.t` (the same name, another namespace) | `TABLE_OR_VIEW_NOT_FOUND` | the old rows | w |
| a DataFrame view as the `MERGE` source; `INSERT` into the source; `MERGE … WHEN NOT MATCHED THEN INSERT *` into an empty table | (1,a) (2,b) (9,z) | (1,a) (2,b): stored | S |
| two held frames; `h1.filter("n = 1").writeTo(t).append()`; both counts, `h2`'s rows | 3, 3, (1,a) (1,a) (2,b) | 2, 2, (1,a) (2,b) | W |
| a held join; `writeTo(t).overwrite(n > 0)`; a child `select`, the join's `count` | (1,o); 1 | the old rows; 2 | W |
| ten rounds of `count` then `INSERT` on one held frame | 2, 3, …, 12 | 2 in every round | W |
| held `cast(c AS INT)` over `'1'`, `'2'`; `INSERT (9, 'zz')`; `collect`, `count` | ANSI on: `CAST_INVALID_INPUT`, 3; off: (1,1) (2,2) (None,9), 3 | (1,1) (2,2), 2 in both modes: the ANSI error is hidden | W |
| hold; `INSERT`; `held.writeTo(t).append()` (Fable §13 row 9) | each of the three rows twice | (1,a) (2,b) twice, (9,z) once: stored | S |
| hold; `DELETE n = 1`; `first`, `head(5)` | (2,b); [(2,b)] | (1,a); [(1,a) (2,b)]: the deleted row | W |
| own-plan, an `INSERT` between: `collect` then `toPandas`; `toPandas` twice; `collect` then `toArrow`; `toArrow` twice; `toArrow` then `collect` | the first binding kept in all five | the same | E (5) |
| another session (Spark) or a Spark process (RePark) inserts; held and fresh count; SQL `REFRESH TABLE`; again | 2, 2 then 3, 3 | 2, 2 then 2, 2 | W |

`toPandas` and `toArrow` keep their first binding on Spark, as `collect` does: Q3's own-plan set is now measured for
all six actions. `refresh_sql_external` is the SQL `REFRESH TABLE` form of §6's `refreshTable`; `type=hadoop` RePark
never sees the external commit, so it stays WRONG under either seat on this catalog (Q10).

Up: [map.md](map.md).
