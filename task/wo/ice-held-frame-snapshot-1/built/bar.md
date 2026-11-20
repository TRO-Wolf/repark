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

## ENC-1 cells (fix round 1, F2)

RePark only: Spark 4.1.2 has no table encryption gate and writes plaintext (ES-3, a dated divergence), so there is no
Spark answer to record. The cells are in the pin file's `_ice_held_frame_snapshot_1_enc_1_cells.py`, run by
`test_ice_held_frame_snapshot_1.py` on one `type=hadoop` session; each keyed table holds `(1, 'a')`, `(2, 'b')` and
then `encryption.key-id`. Measured on the fix-round package `eced2285` and on main `9a48932f`.

**Condition 2, a refused write records nothing** (4 cells): a frame held on a keyed table is read, a write is
refused, and the held frame is read again. On the head and on main the refusal is RePark's ENC-1 sentence
(`UnsupportedOperationException: Table db.eN carries property 'encryption.key-id': ...`), and the held frame, before
and after, and a fresh read after, all answer the two seed rows.

| cell | the refused write |
|---|---|
| `refused_sql_insert` | `INSERT INTO t VALUES (9, 'z')` |
| `refused_sql_delete` | `DELETE FROM t WHERE n = 1` |
| `refused_held_append` | `held.writeTo(t).append()` |
| `refused_frame_insert_into` | `createDataFrame(...).write.insertInto(t)` |

**Condition 3, a map-served table behaves as a fresh guarded load** (9 pairs): each pair runs the action once through
the map-served frame and once through a fresh frame, on the same table where the action is refused or a read, and on
twin tables where it writes. Every pair is identical on the head, refusal text included.

| pair | map-served side | fresh side | both answer | main |
|---|---|---|---|---|
| `keyed_read` | frame held, then a property commit | `s.table(t)` | the seed rows | EQUAL |
| `keyed_view_read` | DataFrame view, then a property commit | `s.table(t)` | the seed rows | EQUAL |
| `keyed_append` | `held.writeTo(t).append()` | the same from a fresh frame | the ENC-1 refusal | EQUAL |
| `keyed_insert_into` | `held.filter(...).write.insertInto(t)` | the same from a fresh frame | the ENC-1 refusal | EQUAL |
| `keyed_sql_from_view` | `INSERT INTO t SELECT * FROM <view>` | `INSERT INTO t SELECT * FROM t` | the ENC-1 refusal | EQUAL |
| `key_added_read` | held, then `INSERT (9, 'z')` and the key added | `s.table(t)` | three rows | **red** (held: two rows) |
| `key_added_append` | held, then the key added, then `writeTo(t).append()` | the same from a fresh frame | the ENC-1 refusal | EQUAL |
| `key_removed_read` | held keyed, the key removed, then `INSERT (9, 'z')` | `s.table(t)` | three rows | **red** (held: two rows) |
| `key_removed_append` | held keyed, the key removed, then `writeTo(t).append()` | the same from a fresh frame (twin table) | `OK`, four rows | EQUAL |

The Rust side of the three conditions is in the module's map
([session_tables](../../../../crates/repark-iceberg/src/catalog/session_tables/map.md)); the Spark-door test
`a_refused_write_moves_no_epoch_and_a_held_frame_answers_as_before` holds the epoch across a refused `INSERT`, `INSERT
OVERWRITE`, `DELETE`, `UPDATE` and `MERGE`.

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

## Fix round 1: the bar re-run

The fix-round package `eced2285` against main `9a48932f`, the same 1,980 cells (both ANSI modes, D included): 1,730
EQUAL, 142 RAISE, 108 WRONG, 8 stored; moved from main 1,360 WRONG to EQUAL, 2 RAISE to EQUAL, 130 WRONG to RAISE;
**0 EQUAL lost**. Unit 1a 750 of 750 EQUAL; gap 1b 98 EQUAL and 132 RAISE. Against the round-0 head no verdict moved;
36 answers differ only in a generated scratch-view name inside an error both engines raise. The pins: the 46 Spark
cells and the 13 ENC-1 cells pass on the head (main: 38 of the Spark cells red, and the 2 ENC-1 reads that follow a
same-session commit), with #1373's 95.

## Gates

On the fix-round head `85675c24` and package `eced2285`, each exit 0: `cargo fmt --check`, `make rust-clippy`, `make
rust-panic-ban`, `cargo test --locked -p` for repark-iceberg (1,081), repark-core (1,645) and repark-spark (2,992),
`make py-test-facade FACADE_WORKERS=6`, the package facade at `-n 6` (41,147 passed), `make py-test` (1,133), the file
size, lib-py, map-size, map-sync and docs-links checks, `typos`, `ruff check`, `ruff format --check`, and the comment
gate against the merge base `a4d8857f` (0).

## Delta 1: `explain` after an incompatible change (A1), and the refused-write cells (C1)

The delta-1 package `pkg-ice1a-afb78fb1` (`BUILT_FROM` `afb78fb1`) changes two Python files of the 12; its native
library is byte-identical to `eced2285`'s. The probes are in `N/ice-1a-out/delta1-probe/` (Spark 4.1.2 with Iceberg
1.11.0, the fix-round package and the delta-1 code, both ANSI modes; every answer is the same in both modes).

**A1, the cause.** After a dropped, renamed or retyped column, a recreate or a `DROP TABLE`, a held frame's re-bind
refusal is raised inside the first optimizer rule, `repark_reader_parse` (`held_iceberg::rebind`). `explain` runs
`EXPLAIN SELECT * FROM <scratch view>`, and DataFusion 54.1's EXPLAIN does not raise a failed rule: the optimizer wraps
the error as `Optimizer rule 'repark_reader_parse' failed`, `SessionState::optimize` turns it into a `logical_plan
after Optimizer rule 'repark_reader_parse' failed` row and marks the logical optimization failed, and the physical
planner then lists every stored step instead of a plan. `_render_explain_sections` keeps only the `logical_plan` and
`physical_plan` rows, so `explain()` and `explain(True)` printed nothing, and `explain("formatted")` printed the whole
listing. Main printed its stale plan. `explain("cost")` runs `EXPLAIN ANALYZE`, which executes, so it already raised.

**The fix** (`surface_a._explain_rows`, called from `core.py`'s `_explain_text` in place of one line, so `core.py`
keeps its exact size): when the rows carry `initial_logical_plan`, which DataFusion lists only when the logical
optimization failed (the facade never asks for `VERBOSE`), the frame is planned once more outside EXPLAIN, under its
own parse memo, through the plan-only `input_files` path (`create_physical_plan`, nothing executes). That raises the
error the frame's next action raises, the same exception with the same text. The modes follow Spark, as measured:

| mode | Spark 4.1.2 | fix-round head | delta 1 |
|---|---|---|---|
| `explain()`, `explain("simple")`, a child's `explain()` | raises | prints nothing | raises |
| `explain("formatted")` | raises | prints the step listing | raises |
| `explain(True)`, `explain(mode="extended")` | prints its sections, each holding the `AnalysisException` | prints nothing | prints both sections, each holding the error |
| `explain("codegen")` | prints the `AnalysisException` | prints the codegen note only | prints the physical section holding the error, and the note |
| `explain("cost")` | prints the `AnalysisException` | raises | raises (unchanged; a residual below) |

The conditions are Spark's in every cell: `INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH` after a dropped,
renamed or retyped column, `...TABLE_ID_MISMATCH` after a recreate, and `TABLE_OR_VIEW_NOT_FOUND` after a drop. A
`SELECT * FROM <DataFrame view of the held frame>` frame's `explain()` raises Spark's
`INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION` (Spark raises it when the frame is built). After an
insert, an added column or a recreate under a view, `explain` prints a plan on every build, as Spark does.

**SQL `EXPLAIN` over a DataFrame view of a held frame.** Spark does not raise: `EXPLAIN SELECT * FROM <view>`
returns the rows `Error occurred during query planning:` and the error's lines, naming the view's condition
(`INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION` after a dropped, renamed or retyped column,
`TABLE_OR_VIEW_NOT_FOUND` after a drop). The head does not raise either: it returns DataFusion's listing for a failed
optimization (54 rows and 3,601 characters for one table), whose last row, `logical_plan after Optimizer rule
'repark_reader_parse' failed`, carries the same condition. Main returned its stale two-row plan. RePark's SQL
`EXPLAIN` answers DataFusion's `(plan_type, plan)` rows for every query, against Spark's one `plan` column, on main as
on the head; that shape is not 1a's and is unchanged. The pin compares the condition the rows name.

**The text.** The error is the frame's own: `Optimizer rule 'repark_reader_parse' failed caused by Error during
planning: [<condition>] ...`, as `count()` raises it on the fix-round head. Its sentence differs from Spark's in two
places that predate this delta: the DataFusion prefix, and the column list (`` `c` has been removed. `` and `` `n`
type has changed from Int32 to Int64 ``, where Spark writes `` - `c` STRING has been removed `` and `` - `n` type
has changed from INT to BIGINT ``). The cells, Spark's and the verifier's, compare the condition.

**The pins** (`test_ice_held_frame_snapshot_1.py`, both ANSI modes, Spark's recorded answers; re-recording left every
earlier answer unchanged):

| cell | Spark (both modes) | main | fix-round head | delta 1 |
|---|---|---|---|---|
| `insert_explain` | prints, no condition | equal | equal | equal |
| `drop_column_explain` | `COLUMNS_MISMATCH` | red | red | equal |
| `drop_column_filter_explain` | `COLUMNS_MISMATCH` | red | red | equal |
| `drop_column_explain_extended` | prints `COLUMNS_MISMATCH` | red | red | equal |
| `recreate_explain` | `TABLE_ID_MISMATCH` | red | red | equal |
| `dropped_table_explain` | `TABLE_OR_VIEW_NOT_FOUND` | red | red | equal |
| `dfview_drop_column_sql_explain` | rows naming `INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION` | red | equal | equal |

**C1, the refused-write cells.** The 4 cells now read the held frame by `count` and by a child (`select("n", "c")`)
before and after the refusal, and the fresh frame by `count` and `collect`; `collect` on the held frame keeps its
first binding (`own_collect_twice`), so it could not show a moved map entry. **The mutant** (in a scratch clone,
`N/ice-1a-out/delta1-mutant/mutant.diff`, never in the lane clone): when `refuse_encrypted_table` refuses, the
decorator's map records a table the refused write never committed (the same metadata with the `main` ref removed,
under a new metadata location), so the held frame's binding moves while the catalog does not. On the mutant's package,
the 4 cells in their fix-round form all pass (the held `collect` answers the seed rows before and after), and in
their delta-1 form all 4 fail: the held frame counts 0 and its child reads no rows, while a fresh read answers the
seed rows. The committed pin file on the mutant: 4 failed (exactly these), 69 passed. On the head package: 73 passed.

**The verifier's cells** (`V/mycells.sh` and `V/probes/`, copied to `N/ice-1a-out/delta1-mine/`, run on Spark, the
delta-1 package and main, 129 cells per mode): 248 of 258 equal Spark on the head, against 238 in the verifier's run
of the fix-round head. The 10 that moved are exactly A1's (`A|tab|{dropc,ren,widen}|explain` and
`O|tab|{recreate,drop}|explain`, both modes); every other head answer is byte-identical to the verifier's. The 10 left
are the named residuals: `K|cache_held` (2, unit 2), `N|*` (6, `newSession` and a hadoop table) and `V|global|ins`
(2, R-DF-BATCH2). Spark's answers equal the verifier's run; main's differ in one cell, `X|ext|count` with ANSI off,
which the verifier's overlapping clean-up broke in its run and which equals Spark here.

## Delta 1: the bar re-run

`BAROUT=N/ice-1a-out/delta1-bar`, the turnstile copy `run_v.sh` on `pkg-ice1a-afb78fb1`, compared by the committed
[barcmp.py](evidence/probes/barcmp.py): 1,730 EQUAL, 142 RAISE, 108 WRONG, 8 stored, and **0 EQUAL lost** from main
(1,360 WRONG to EQUAL, 2 RAISE to EQUAL, 130 WRONG to RAISE). Unit 1a 750 of 750; gap 1b 98 EQUAL and 132 RAISE.
**Against the fix-round run (`verdicts-fix1.jsonl`) no verdict moved**: the bar holds no cell where an `explain` meets
an incompatible change (its one `explain` cell, `S|s_explain_collect`, is EQUAL on every build). 36 answers differ,
each only in a generated `_repark_jl_` or `_repark_jr_` view name inside an error both engines raise. The pin file on
the delta-1 package: 73 passed (60 Spark cells, 13 ENC-1); main: 52 failed (the 38 earlier Spark cells, 12 of the new
explain cells, and `key_added_read` and `key_removed_read`).

**Delta 1 gates** (`N/ice-1a-out/delta1-gates`, head `afb78fb1`, package `afb78fb1`), each exit 0: `cargo fmt --check`,
the file size, lib-py, map-size, map-sync and docs-links checks, `typos`, `ruff check`, `ruff format --check`, the
comment gate against `a4d8857f` (0), `make rust-clippy`, `make rust-panic-ban`, `cargo test --locked -p` for
repark-iceberg (1,081), repark-core (1,692) and repark-spark (3,262), `make py-test-facade FACADE_WORKERS=6`
(41,156 passed), the package facade at `-n 6` (41,161 passed) and `make py-test` (1,133 passed).

Up: [map.md](map.md).
