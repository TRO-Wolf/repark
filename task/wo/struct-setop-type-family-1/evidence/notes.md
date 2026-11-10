# Variant table: executed under the orchestrator's prune ruling

## Status

The ruled plan (P1 to P14, with the additions below) ran to completion on all three legs. Nothing was committed, no clone was edited or built, and the live repository was not touched. Raw results are in `raw/spark.jsonl`, `raw/base.jsonl` and `raw/head.jsonl`; the rendered table is `table.md`. `plan_count.py` (the first clerk's plan) is kept unchanged; it counts 1,846 cells for P1 to P12, and this run's P1 to P12 differ only where listed below.

## Cell count

2,177 cells in total: spark 790 records (31 distinct Spark executions, 759 reused by shape), base 805, head 582. The ruling said "about 2,170". The difference is the ruled P1 to P14 total of 2,170 with these changes: P12 has 10 cells, not 12 (see deviations); P8b adds 3 (recursive CTE on each leg); P15 adds 2 (facade ingest of dictionary); P10dml adds 4 (seat C DML row). 2,170 - 2 + 3 + 2 + 4 = 2,177.

## Spellings

- Kind labels: `list`, `large` (LargeList), `fsl1`/`fsl2` (FixedSizeList of 1 or 2), `view` (ListView), `largeview` (LargeListView). Encodings: `plain`, `ree` (RunEndEncoded), `dict` (Dictionary). Each side is `kind/enc/order/names`: order `AB` is field order (a,b) and `BA` is (b,a); names `std` or `ren` apply to map entries.
- Values: left element `{a:1,b:2}`, right element `{a:4,b:3}`, written in the side's field order. Memo values were `{b:3,a:4}`; these are the same shape.
- Labels: the Spark label compares a cell with its Spark twin on the same logical shape. `SELF` marks Spark rows. The kind-invariance label compares a cell with its List twin: same leg, door, source, encodings, payload, depth, operation and orientation. Both refused counts as `INVARIANT` whatever the refusal class.
- "Hidden" on head: answered, and either rows differ from Spark or Spark refused.

## Prunes and deviations (all judgement calls, for review)

1. P1 base source is pandas Arrow dtypes, not polars. Polars cannot realise `LargeList` vs `List` as requested (it always yields LargeList), so the kind axis would not be measured. Polars is covered by P7 (`polars_df` and `polars_array`).
2. P7 Iceberg rows: the Iceberg side is always `List` whatever kind was requested, so the requested left kind is ignored for those rows. Polars sides arrive as LargeList (or FixedSizeList for `polars_array`). P7 measures the source, not the requested kind.
3. P10 (seat C) is run as P10 defines it, with no orientation crossed. The brief asked for both orientations; I kept the ruled count (432) and did not add them. Seat C right values equal the left values (`{a:1,b:2}`), so `=`, `IN` and join can match; with these values Spark compares struct fields by position and returns false or no rows.
4. P12 (F-K): Spark has one door, so the Spark leg runs 2 cells, not 4. Total 10, not 12.
5. Spark twin per logical shape (as the brief says): each distinct Spark shape is executed once and its result reused for every cell that maps to it. Spark records marked `reuse: true` in the raw files are reuses.
6. Dictionary encodings: DataFusion refuses `arrow_cast` to `Dictionary(Int32, List(...))` ("Unsupported output type for dictionary packing") and the facade refuses pandas dictionary dtype. No dictionary-encoded list cell was constructed on either repark leg; all 108 P2 dictionary cells and 18 P14 dictionary cells are refusals at construction, and P15 records the facade ingest refusal.
7. The facade refuses `intersectAll` and `exceptAll` on purpose (`octo C1-L-005`, `octo C1-L-006`). The 6 `intersect_all` and 6 `except_all` cells per repark leg record that refusal, not the engine's set semantics. No SQL-door INTERSECT ALL or EXCEPT ALL cell is in the plan.
8. Struct literals in DML, path and insert statements are cast to BIGINT (`CAST(1 AS BIGINT)`). Without the cast the first attempt failed on INT vs BIGINT, which is a width mismatch and hides the field-order question. The reordered predicate is the only difference between a reordered and a same-order control.
9. Iceberg tables: I did not set `format-version` or `write.*.mode` explicitly on any table (the facade smoke showed the properties parse, but the cells do not pass them). Iceberg 1.11's defaults are format v2 and copy-on-write, but I did not verify that in table metadata.
10. Path tables (P11): fresh `USING parquet LOCATION` table per statement, as the brief says. Spark refuses UPDATE, DELETE and MERGE on parquet tables (`UNSUPPORTED_FEATURE.TABLE_OPERATION`), so every Spark path answer is a refusal and the path cells carry no oracle answer.
11. Seat C DML row (P10dml): Spark raised `IllegalArgumentException: Cannot create expression literal from GenericArrayData` on the reordered predicate, for both payloads. This is an internal Spark error, recorded as a Spark refusal. It is not a semantic answer, so the base comparison is not a clean oracle.
12. The base leg's polars and pandas: installed polars 2.1.0 first, then pinned polars 1.43.2 and pandas 3.0.5 to match head, so base and head differ only in repark. Recorded in provenance.md.
13. Spark leg: one shared session, one JVM under `spark-jvm.lock`. The Spark warehouse was cleared before the final run (a smoke run had left tables behind, which caused false `TABLE_OR_VIEW_ALREADY_EXISTS` refusals).
14. Added cells beyond the ruling: P8b (recursive CTE on the native door, 3 cells, one per leg), P15 (facade ingest of the dictionary column, 2 cells), P10dml (seat C DML, 4 cells).

## Findings the orchestrator should read first

- Spark's own answers: UNION accepts reordered struct names and matches fields by position. EXCEPT and INTERSECT refuse them (`INCOMPATIBLE_COLUMN_TYPE`), on all 72 P13 cells. So on the Spark oracle every EXCEPT over reordered structs is a refusal, and every repark answer there is a hidden acceptance.
- Head, union: 104 answered-by-name cells differ from Spark (`RESULT-ROWS-DIFFER`, union_all). Head also refuses same-kind reordered unions (list/list, list/large, large/large, view/view), which Spark answers: 321 `REFUSED-WHERE-SPARK-ACCEPTED` on union_all, mostly that and encodings.
- Head, EXCEPT: 40 cells answered where Spark refuses (`ACCEPTED-WHERE-SPARK-REFUSED`), plus 3 INTERSECT. Both orientations hidden alike (11 each for P13).
- Base (1.5.6): the same-kind list/list union answers by name (`{a:4,b:3}`; Spark `{a:3,b:4}`). The memo measured only head, so this is new: 1.5.6 already gives a wrong answer for list/list on union.
- The memo's section 1.2 answered/refused categories for union and subtract match head for all 11 measured rows, but the memo's sentence "the reverse orientation refuses correctly for union" does not hold on head: reversed (large, list), (list, fsl1) and (large, fsl1) answer by name.
- F-K (memo Q6): the memo's claim that the typed-NULL-first answer equals Spark and base is contradicted. Spark gives `{a:2,b:1}` (positional), base gives `{a:1,b:2}` (by name), and head refuses with `INCOMPATIBLE_COLUMN_TYPE`. Null-last matches Spark on base and head.
- Insert (P9, list/list): Spark persists `{a:3,b:4}` (positional), base persists `{a:4,b:3}` (by name), head refuses. Snapshot changed on 36 Spark, 16 base and 8 head answered inserts.
- Path tables (memo Q5.2): on fresh tables, base 1.5.6 deletes or updates the correct row (id 1) for UPDATE, DELETE and MERGE with the reordered literal, and the same-order controls agree. The memo's "MERGE deleted every row" did not reproduce on fresh tables. The memo's defect came from a shared table changed by earlier statements, which this run did not reproduce.
- Seat C on base: name pairing returns `true` or rows where Spark's positional comparison returns `false` or no rows (48 `RESULT-ROWS-DIFFER`, 24 `ROWS-DIFFER`), and 108 `REFUSED-WHERE-SPARK-ACCEPTED` cells, mostly view and fixed-size kinds.
- Head, facade polars and Iceberg sources (P7): 22 refusals with `INCOMPATIBLE_COLUMN_TYPE` where base answered by name, consistent with the same-kind refusal rule.

## Questions for the orchestrator

1. Seat C DML: Spark's internal `IllegalArgumentException` on the reordered array-literal predicate means the DML row has no clean oracle. Should a same-order Spark control be added?
2. Path tables: Spark cannot run DML on parquet at all, so the memo's path controls have no Spark answer. Should the shared-table sequence (UPDATE, DELETE, MERGE on one table, as the memo ran it) be run on base and head, to test whether the memo's defect exists under that sequence?
3. Dictionary: DataFusion refuses every dictionary-of-list cast. Should a dictionary of struct (no list) be measured, or should dictionary stay an unmeasured row in the table?
4. INTERSECT ALL and EXCEPT ALL on the SQL door are not in the plan, and the DataFrame door refuses them on purpose. Add SQL-door rows?
5. Seat C orientation was not crossed (P10 as ruled). Add the orientation rows, or accept?
6. The F-K acceptance in memo Q6 needs revisiting (see findings).

## Item-2 note (2026-11-10)

P4-00927 and P4-00933 are twin artifact: element count, not a defect: both sides
carry the same field names (AB), the head's rows are exactly the constructed input,
and fixed_size_list[2] builds two copies of the row while the Spark twin per logical
shape has one element. b8ad7a9c answers identically. Excluded from the acceptance
count.
