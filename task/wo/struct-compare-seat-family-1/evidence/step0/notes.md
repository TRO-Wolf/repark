# Notes: family step-0 table (STRUCT-COMPARE-SEAT-FAMILY-1)

## Result shape

- 2274 cells per leg: 2088 query cells and 186 DML cells. Inside the 1500 to 3000 target, under the 4000 cap.
- Files: `probe_family.py` (`--leg`, `--render`, `--plan`), `run_all.sh`, `raw/<leg>.jsonl` (one run per leg), `raw/<leg>.setup.json`, `raw/<leg>.identity.json`, `table.md`, `notes.md`, `provenance.md`, and the copied Fable probes.

## Gap: v3 DML is unmeasured on head and base

- Head and base refuse `format-version` 3 at table creation (`repark.sql.allowCreateFormatVersion3` is not set, and the default create stays v2). All 62 v3 DML cells per leg fail at setup, so the v3 rows in `table.md` are setup refusals, not write answers.
- Spark ran the v3 cells normally.
- Not re-run with the flag: it is a config switch that changes the measured surface, and that choice is for the owner or the successor. Successor item: re-run the 62 v3 DML cells on head and base with `repark.sql.allowCreateFormatVersion3=true` (DML only, same run script).

## Judgement calls (for the hand-back)

1. **DERIVED oracle scope.** Applied only where Spark raised `Cannot create expression literal`, and only to predicate DML. This happened for every literal DELETE. Spark's own literal UPDATE and MERGE did not raise, so for those Spark's post-state is the oracle (labels `WRONG-WRITE`, no `DERIVED-` prefix). Spark's DELETE error is consistent with the comparison-unit note.
2. **Labels added for DML.** `EQUAL` (the brief lists no equal label for DML). `DERIVED-` prefix on all derived labels. `SNAPSHOT-DIFF` only when both legs report integer snapshot counts and they differ.
3. **INT-TWIN.** Each struct cell with a `star` or `key` projection is compared to its `INT`-key twin (same API, join type and projection). `INT-TWIN-SAME` means the struct cell's head-vs-Spark label equals the twin's label. `INT-TWIN-DIFFERS` otherwise. This is one reading of "same shown-key behaviour".
4. **List kinds.** Spark has no LargeList, FixedSizeList(2) or ListView, so the Spark leg uses plain arrays with the same elements. The Spark label on kind cells is therefore not kind-aware. `INVARIANT` and `VARIANT-DIFFERS` compare the head kind cell with the head plain-List twin of the same API, join type and order, not with Spark.
5. **`EQUAL` with schema difference.** Kept as `EQUAL`; the schema match is recorded in a separate column. No new label was invented for it.
6. **PREPARE cells.** 23 cells, all reordered (`rev`) SQL seats, chosen from the generated SQL seats. Spark runs the plain statement; head and base run `PREPARE` then `EXECUTE`, and the PREPARE result is recorded per cell.
7. **Literal bounds.** `B` (reordered), `A` (same order) and `C` (name-only) against the `lk` left side, with no right table.
8. **Duplicate-field and map shapes** use their own left tables (`ldup`, `lmap`), so the left side changes with the shape. This is a choice, not something the brief stated.

## Observations (measured, not interpreted)

- Fable's 120 probe cells reproduce on all three legs: 360 memo values compared, 0 differ. Fable's `dml-results.txt` reproduces on all three legs.
- `EXECUTE` on head pairs by position for the prepared `BETWEEN` cell, while direct SQL on head pairs by name. This matches the memo's section 7 note.
- Head refuses reordered `INTERSECT` and `EXCEPT` (`BOTH-REFUSED`, the unit C control holds). Same-order controls are `EQUAL`.
- Head `WRONG-ANSWER` counts: key-list join 280, nullif 104, simple CASE 53, BETWEEN 36, list kinds 23, PREPARE/EXECUTE 18, NOT BETWEEN 12. Total 526.
- Head `ACCEPTED-WHERE-SPARK-REFUSED` counts: key-list join 168, nullif 32, BETWEEN 12, simple CASE 15, lateral 10, NOT BETWEEN 4, PREPARE 1. Total 242.
- ON control on head: no `WRONG-ANSWER`. Some `REFUSED-WHERE-SPARK-ANSWERED` (24) and `BOTH-REFUSED` (54).
- DML on head (v2 copy-on-write and merge-on-read): 17 predicate `WRONG-WRITE` per format (reordered and literal BETWEEN, nullif and CASE across DELETE, UPDATE and MERGE), 2 insert and 2 merge `WRONG-WRITE` per format. One `DERIVED-WRONG-WRITE` per format (literal BETWEEN DELETE).
- Same-order DML controls: 58 `EQUAL`, 6 derived-equal, 29 refused (these 29 are the v3 setup refusals).
- Base differs from head outside the ON control in 16 cells. The examples read are typed `Column.between` cells (group F-BETWEEN). The other 13 were not checked in detail. Full list in table.md.

## Successor items

1. Re-run the 62 v3 DML cells on head and base with the format-version-3 flag (see the gap above).
2. Check the 16 base-vs-head cells outside the ON control; only three were read.
3. The derived rule for literal UPDATE and MERGE is currently Spark's own answer. If the owner wants the derived oracle for those too, the rule needs a ruling.

## Orchestrator additions (2026-11-10)

**v3 DML re-run.** The clerk's v3 cells failed at setup on the RePark legs, because `repark.sql.allowCreateFormatVersion3` was unset.
- Ruling: set the opt-in. Earlier units' verifier probes set it too, and v3 writes on RePark are opt-in.
- `rerun_v3.py` re-ran the 62 v3 DML cells on head and base with the flag set, through `run_v3.sh`, as a `repark.slice` unit. Both exits were 0.
- The results replaced the v3 records in `raw/head.jsonl` and `raw/base.jsonl`. The pre-merge copies are `raw/*-pre-v3.jsonl` and `table-pre-v3.md`. `table.md` was re-rendered.
- v3 now matches v2 per format on head: 17 predicate, 2 `INSERT … SELECT` and 2 `MERGE` WRONG-WRITE cells, plus 1 DERIVED-WRONG-WRITE.

**Ruling on clerk question 2.** Literal UPDATE and MERGE keep Spark's own answer as the oracle. The derived oracle (owner, 2026-11-09) covers only statements Spark cannot run.

**Ruling on clerk question 3.** I read the 16 base-differs-from-head cells outside the ON control. Every one is a #1292 move to EQUAL or BOTH-REFUSED:
- the typed `Column.between` (the DataFrame door builds it as `>=` and `<=`, which #1292's rule already sees): 12 cells;
- `EXECUTE` of a prepared statement: 4 cells.

The typed BETWEEN door is therefore already fixed on head. The family's BETWEEN cells are the SQL and string doors (`Expr::Between`).

## Completeness additions (build item 1, 2026-11-10)

The order's completeness check lists four "add" rows. `probe_family_add.py` measures them on the `spark` and `head`
legs: 148 query cells (`X0001`–`X0148`) and 6 DML cells (`Y0001`–`Y0006`). `table.md` renders them after the clerk's
table, under "Completeness additions (build item 1)".

**`NATURAL` semi and anti (`X0001`–`X0060`, 60 cells).**
- Spark parses both forms and refuses them in analysis: `requirement failed: Unsupported natural join type
  LeftSemi` (and `LeftAnti`), an `IllegalArgumentException`.
- Head answers 42 of the 60 cells. The other 18 refuse on both engines: the `arity`, `unrel`, `dup` and `case`
  shapes.
- The acceptance does not depend on structs. The four `INT` cells answer too, and every star cell is
  `INT-TWIN-SAME`.
- No cell is a wrong answer. Proposed card `NATURAL-EXISTENCE-JOIN-ACCEPT-1` (S3).

**`NOT BETWEEN` on the string door (`X0061`–`X0111`, 51 cells).**
- These are `filter`, `selectExpr` and `F.expr` over the 14 table shapes and the three literal bounds.
- Head is WRONG-ANSWER on 18 cells: the `rev`, `name`, `nest` and `arr` shapes and `lit-B` and `lit-C`, on all three
  string doors. Also: 15 EQUAL, 6 ACCEPTED-WHERE-SPARK-REFUSED, 9 REFUSED-WHERE-SPARK-ANSWERED, 3 BOTH-REFUSED.
- This is the same profile as the SQL `NOT BETWEEN` group. Both reach DataFusion as `Expr::Between { negated: true }`.

**A `MAP` key column in `USING` (`X0112`–`X0139`, 28 cells, plus the clerk's `map` shape).**
- The fixtures are `lmk`/`rmk` (`MAP<STRING, INT>`, no struct) and `lms`/`rms` (`MAP<STRUCT<a,b>, INT>` against
  `MAP<STRUCT<b,a>, INT>`).
- Spark refuses every `USING`, `NATURAL` and DataFrame key-name cell with `DATATYPE_MISMATCH.INVALID_ORDERING_TYPE`,
  on both fixtures and all six join types. Head answers every one: 26 ACCEPTED-WHERE-SPARK-REFUSED.
- The `ON` controls (`X0125`, `X0139`) refuse on both engines. There, `map_ordering::refuse_in_expr` already sees
  the `=`.
- The clerk's `map` shape (`MAP<STRING, STRUCT<a,b>>` against `MAP<STRING, STRUCT<b,a>>`) is the same: 62 key-list
  cells, all ACCEPTED-WHERE-SPARK-REFUSED (for example `Q1117`–`Q1134`), with the `ON` controls BOTH-REFUSED.
- So Spark refuses a map key whatever the value type, and the order's test ("Spark's answer decides whether
  `map_ordering::refuse_in_expr` applies to keys") answers yes.

**`EXECUTE` cells for the seats that had none (`X0140`–`X0148`, `Y0001`–`Y0006`).**
- `NATURAL` semi and anti: PREPARE ok, and EXECUTE answers where Spark refuses, as the direct statements do.
- A map key: ACCEPTED, as the direct statements are.
- List kinds: `X0144` and `X0145` are WRONG-ANSWER by name, as the direct `K-KIND` cells are.
- Set operations: `INTERSECT` and `EXCEPT` refuse with unit C's `INCOMPATIBLE_COLUMN_TYPE` at PREPARE. `UNION ALL`
  refuses at PREPARE as well (unit C; `STRUCT-SETOP-BY-POSITION-1`).
- DML: the Spark door refuses `PREPARE` of a write ("its statement writes to a table, and a prepared write would
  run later, at EXECUTE") for `DELETE` and `INSERT … SELECT`, and `PREPARE … AS MERGE` does not parse. So `EXECUTE`
  does not reach the DML seats. All six are REFUSED-WHERE-SPARK-ANSWERED, because Spark runs the plain statement.
  This matches the clerk's design (judgement call 6).
