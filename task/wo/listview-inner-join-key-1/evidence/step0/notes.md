# LISTVIEW-INNER-JOIN-KEY-1 step-0 notes

Model: claude-haiku-5-5 (clerk). Measurement only. Nothing committed, no clone edited, no build run in any clone. Paths below use placeholders; see provenance.md for the mapping.

Placeholders: `<OUT>` = the output directory, `<SCRATCH>` = the session scratchpad, `<FAM_MAIN_WT>` = the main-68f70b0f worktree, `<SPARK_PY>` = the c26-oracle interpreter, `<LOCK>` = the spark-jvm flock file.

## Result in one paragraph

On main 68f70b0f, the defect is confined to name-keyed inner joins. `df.join(other, "k")` and `df.join(other, ["k"])` return the full cross product (left rows × right rows) for every ListView and LargeListView key: 28 cells, 7 kinds × 3 datasets for `df_str` plus 7 `df_list`. The same key in an explicit ON (`l.k = r.k`, `(l.k = r.k) & (l.id > 0)`, SQL `ON`) is correct. Every outer and semi/anti join is correct for every key type. The second-key form `join(other, ["k","j"])` returns a wrong answer (24 rows where Spark returns 5) for the 7 variant kinds; plain List controls are correct on it. The Iceberg DML path reproduces the cross product: `writeTo().append()` persists 56 rows where Spark persists 9. `INSERT ... SELECT` over an ON join is correct.

## Judgement calls

1. **Build route.** `createDataFrame` on main rejects a raw `pa.Table` (PySparkTypeError). The pandas route with `pd.ArrowDtype` mistypes the list-view column as `StringType` and the int column as `LongType`, so it would mislabel the reached type. I used the probe's SQL `arrow_cast` route for all main views. Polars was not used; no polars route was needed to reach the same Arrow types, and the brief lists it only "where it can produce them".
2. **Reached type.** repark's Python `DataFrame.schema` reports every ListView and LargeListView key as `string` (raw field `*_schema`), so it cannot be used for the reached type. I recorded `toArrow().schema` instead, which shows `list_view<item: int64>` and `large_list_view<...>`. This is itself a finding for the build: the Python schema is wrong for list views.
3. **Spark twin.** Spark has one array type, so ListView, LargeListView, List, LargeList and FixedSizeList(2) all reach Spark as plain arrays, per the brief.
4. **FixedSizeList(2) data.** A FixedSizeList(2) cannot hold an empty list or a length-1 list, so for that kind the empty key `C` became `[3,3]` and the length-1 key `G` became `[2,2]`. The NULL key was kept. The FSL2 control is therefore not the same data as the List controls, and only its own Spark reference is compared.
5. **Nested kinds.** "A ListView inside a struct key" and "a struct holding a ListView" describe the same shape here, so I measured one shape: `STRUCT{a: ListView(Int64)}`, with twin `STRUCT{a: List(Int64)}`. Its NULL key is a struct whose field `a` is NULL, not a NULL struct. Likewise a NULL element inside a struct list is a struct with NULL fields, not a NULL struct element.
6. **Native door.** `repark.sql()` cannot see the Spark temp views (`table 'datafusion.public.lv_l' not found`) for all 13 kinds, controls included. The 26 native cells are therefore REFUSED for a reason unrelated to the defect. They do not measure the native door and need a catalog-backed table to test. The brief said "if it can see the frames", and it cannot.
7. **Native reference.** Native forms are labelled against the Spark `sql_using` and `sql_on` cells of the same kind.
8. **Scope of explain().** `explain()` was captured only for `df_str` and `sql_on` inner on the base dataset (26 cells). It was not captured for `df_list`, `df_two`, or the one and big datasets. That is a gap against the brief's "each failing form"; the failing `df_str` plan is recorded, the others are not.
9. **Cell count.** 326 main records and 300 Spark records (both include DML and aggregate rows). Brief range is 300 to 800, so no prune was needed. Join cells: main 299, Spark 273. DML: 14 per leg, since NEST_LV counts as a seventh variant. Aggregate: 13 per leg.
10. **Dataset sizes.** `big` is 1000 × 1000 rows with keys cycling over seven values, so a correct inner join returns 122,408 rows on Spark and the cross product is 1,000,000. Dataset `one` is 1 × 1 with non-matching keys, so the cross product is 1 row against Spark's 0.

## Oracle observations (not judged here)

- Spark matches a list-with-NULL-element key to an identical key: `[1, NULL]` joins `[1, NULL]`. The base control count (9) only reconciles that way. Main agrees with Spark on this for the controls, so the defect table does not depend on it. Recorded as observed, not as a rule.
- The 24-row `df_two` result on main equals the number of pairs that match on `j` alone, counted in pure Python from the base data (5×3 + 3×3 = 24). That is an inference from a count, not from a plan. The plan for `df_two` was not captured (see judgement 8).
- The failing `df_str` plan is a `CrossJoinExec` whose scans project only `lid` and `rid`, so the key column never reaches the plan. The passing `sql_on` plan is a `NestedLoopJoinExec` with `filter=k@0 = k@1`, which is correct but is a nested loop, not a hash join.

## Run history (for the successor)

- Run 1 (smoke, then full): the normalizer treated repark `Row` as opaque and stringified it, so main and Spark hashes could not be compared. Fixed by normalizing through `asDict()`.
- Run 2 (full, both legs): main DML failed with `CatalogCommitConflicts` because `warehouse-main` still held Iceberg metadata from earlier runs. Fixed by clearing that warehouse.
- Run 3 (final main leg, fresh warehouse): clean. The final Spark leg is run 2 (after the normalizer fix). Only the final main and Spark raw files are in `raw/`.
- Spark's `spark.sql.ansi.enabled` read back as `None` (the conf read did not report a value), and repark's also read `None`. ANSI mode was not measured on either leg.

## Not done

- No `df_list` or `df_two` `explain()`.
- No native door measurement against a catalog-backed table.
- No polars or pandas build route.
- No ANSI-on or ANSI-off comparison.
- The smoke leftovers in `<OUT>` (`typecheck_main.py`, `smoke_main.log`, `smoke_spark.log`) are scratch and were kept, not deleted.
