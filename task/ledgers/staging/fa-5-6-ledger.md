# Unit ledger — FA-5 and FA-6 · duplicate display names in csv writes and temp views

**Date:** 2026-10-08 · **Branch:** `fix/fa-5-6-duplicate-names` · **Base:** `d3f4fc16` (`origin/main`)
· **Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Order:** the orchestrator's brief "FA-5 and FA-6: duplicate display names in csv writes and
temp views", closing the two 2026-10-06 rows of the
[v1.5.3 card](../../roadmap/mid-term/v1-5-3-card-2026-10-04.md) ("csv duplicate-header rename",
"Duplicate-name temp-view schemas"), which fold SM-2 of the
[ATTR-ID-1 ledger](attr-id-1-ledger.md) (C-061 csv, C-064 temp views) left as ruled refusals.

**Retires:** this ledger moves to `../completed/` in the unit's last commit.

**Fold 1 (2026-10-08), read this first.** The verifier failed round 1 at `a85f0d7b`
(`/tmp/oc-worker/direct/wo/fa-5-6/verify/verdict.json`: five S1, six S2, one S3). The
orchestrator's ruling, "never worse than main": **FA-6 is withdrawn** and the four view doors
refuse exactly as on main; **FA-5 ships**, reworked so that no display name is ever inferred
from a column's spelling. The step-0 cells and the design note below are kept as written.
Where fold 1 overrides them, a dated note says so; the sections "Fold 1" and "Why FA-6 was
withdrawn" near the end are the current record, and the clause table is rewritten to match.

**Why.** A frame whose display names hold exact duplicates (a qualified self-join, a `USING`
join with shared non-key columns) refuses a csv path write and every temp-view door with
`[COLUMN_ALREADY_EXISTS]`. Spark 4.1.2 writes the duplicate header and registers the view.
Both refusals exist because the engine cannot plan over a relation that carries one name
twice. This unit keeps the engine names unique and carries the display names beside them.

## Step 0 — measured cells (2026-10-08)

Oracle: live Spark 4.1.2, `local[1]`, session zone UTC, probe `/tmp/fa56/spark_probe.py`,
verbatim output `/tmp/fa56/spark.json`. Main: `d3f4fc16`, probe `/tmp/fa56/repark_probe.py`,
verbatim output `/tmp/fa56/repark-main.json`. Frames: `L = [(1,'a',10),(2,'b',20)]` as
`id,s,v`; `R = [(1,'x'),(3,'y')]` as `id,t`; `J` is `L.alias('l').join(L.alias('r'), l.id == r.id)`
(display `id,s,v,id,s,v`), `JR` joins `L` with `R` the same way (`id,s,v,id,t`), `JU` is
`L.alias('l').join(L.alias('r'), 'id')` (`id,s,v,s,v`). File cells show the bytes of each
part file under its partition directory. Row lists are sorted by the probe, so `DESCRIBE`
and `SHOW COLUMNS` rows appear sorted here; Spark emits them in schema order.

Engine names on main: `J` carries six unique twin names (`__repark_l_<hash>_0_id`, …), `JR`
carries twin names only on the two `id` columns (`s`, `v`, `t` keep their names), and `JU`
carries the duplicates themselves (`id, s, v, s, v`, told apart by plan qualifier only).

The avro door is not measurable on this Spark install (`Failed to find data source: avro`,
the external module is absent); repark has no avro path writer either, so the cell is
unsupported on both sides and stays out of this unit.

### FA-5 cells

| Cell | Spark 4.1.2 | main `d3f4fc16` |
|---|---|---|
| `csv_noheader` | {".": ["1,a,10,1,a,10\n2,b,20,2,b,20\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_header` | {".": ["id,s,v,id,s,v\n1,a,10,1,a,10\n2,b,20,2,b,20\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_header_readback` | cols `id0,s1,v2,id3,s4,v5` rows [["1", "a", "10", "1", "a", "10"], ["2", "b", "20", "2", "b", "20"]] | ERR PySparkException: Arrow error: Csv error: incorrect number of fields for line 1, expected 3 got more than 3 |
| `csv_header_readback_infer` | cols `id0,s1,v2,id3,s4,v5` rows [[1, "a", 10, 1, "a", 10], [2, "b", 20, 2, "b", 20]] | ERR AnalysisException: Schema error: Schema contains duplicate qualified field name "?table?".id |
| `csv_header_readback_noheader` | cols `_c0,_c1,_c2,_c3,_c4,_c5` rows [["1", "a", "10", "1", "a", "10"], ["2", "b", "20", "2", "b", "20"], ["id", "s", "v", "id", "s", "v"]] | ERR PySparkException: repark internal error in PyDataFrame.__arrow_c_stream__: index out of bounds: the len is … |
| `csv_header_option` | {".": ["id,s,v,id,s,v\n1,a,10,1,a,10\n2,b,20,2,b,20\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_mixed_header` | {".": ["id,s,v,id,t\n1,a,10,1,x\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_using_header` | {".": ["id,s,v,s,v\n1,a,10,a,10\n2,b,20,b,20\n"]} | ERR COLUMN_ALREADY_EXISTS `s` |
| `csv_sep_header` | {".": ["id\|s\|v\|id\|s\|v\n1\|a\|10\|1\|a\|10\n2\|b\|20\|2\|b\|20\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_quoteall_header` | {".": ["\"id\",\"s\",\"v\",\"id\",\"s\",\"v\"\n\"1\",\"a\",\"10\",\"1\",\"a\",\"10\"\n\"2\",\"b\",\"20\",\"2\",\"b\",\"20\"\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_part_dup_s` | ERR AMBIGUOUS_REFERENCE `s` [`l`.`s`, `r`.`s`] | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_part_nondup_t` | {"t=x": ["id,s,v,id\n1,a,10,1\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_part_dup_id_mixed` | ERR AMBIGUOUS_REFERENCE `id` [`l`.`id`, `r`.`id`] | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_mode_overwrite_fresh` | {".": ["id,s,v,id,s,v\n1,a,10,1,a,10\n2,b,20,2,b,20\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_mode_append_fresh` | {".": ["id,s,v,id,s,v\n1,a,10,1,a,10\n2,b,20,2,b,20\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_mode_ignore_fresh` | {".": ["id,s,v,id,s,v\n1,a,10,1,a,10\n2,b,20,2,b,20\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_mode_error_fresh` | {".": ["id,s,v,id,s,v\n1,a,10,1,a,10\n2,b,20,2,b,20\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_mode_errorifexists_fresh` | {".": ["id,s,v,id,s,v\n1,a,10,1,a,10\n2,b,20,2,b,20\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_mode_overwrite_existing` | {".": ["id,s,v,id,s,v\n1,a,10,1,a,10\n2,b,20,2,b,20\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_mode_append_existing` | {".": ["id,s,v,id,s,v\n1,a,10,1,a,10\n2,b,20,2,b,20\n", "id,s,v,id,s,v\n1,a,10,1,a,10\n2,b,20,2,b,20\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_mode_ignore_existing` | {".": ["id,s,v,id,s,v\n1,a,10,1,a,10\n2,b,20,2,b,20\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_mode_error_existing` | ERR PATH_ALREADY_EXISTS: [PATH_ALREADY_EXISTS] Path file:/tmp/fa56/spark-wh/csv_mode_error_existing already … | ERR COLUMN_ALREADY_EXISTS `id` |
| `json` | ERR COLUMN_ALREADY_EXISTS `id` | ERR COLUMN_ALREADY_EXISTS `id` |
| `text` | ERR COLUMN_ALREADY_EXISTS `id` | ERR UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE: [UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE] The Text datasource doesn't support the column … |
| `text_one_col` | {".": ["a\nb\n"]} | {".": ["a\nb\n"]} |
| `text_two_s` | ERR COLUMN_ALREADY_EXISTS `s` | ERR _LEGACY_ERROR_TEMP_1290: Text data source supports only a single column, and you have 2 columns. |
| `parquet` | ERR COLUMN_ALREADY_EXISTS `id` | ERR COLUMN_ALREADY_EXISTS `id` |
| `orc` | ERR COLUMN_ALREADY_EXISTS `id` | ERR NOT_IMPLEMENTED: [NOT_IMPLEMENTED] orc is not implemented. |
| `avro` | ERR _LEGACY_ERROR_TEMP_1139: Failed to find data source: avro. Avro is built-in but external data source module since … | ERR AnalysisException: DATA_SOURCE_NOT_FOUND: Failed to find the data source: 'avro'. repark path writes support … |
| `xml` | ERR COLUMN_ALREADY_EXISTS `id` | ERR NOT_IMPLEMENTED: [NOT_IMPLEMENTED] xml is not implemented. |
| `csv_compress` | {".": ["id,s,v,id,s,v\n1,a,10,1,a,10\n2,b,20,2,b,20\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_header_sensitive` | {".": ["id,s,v,id,s,v\n1,a,10,1,a,10\n2,b,20,2,b,20\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_temporal_header` | {".": ["id,ts,d,id,ts,d\n1,2024-01-02T03:04:05.000Z,2024-01-02,1,2024-01-02T03:04:05.000Z,2024-01-02\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_temporal_fmt_header` | {".": ["id,ts,d,id,ts,d\n1,2024/01/02 03,2024-01-02,1,2024/01/02 03,2024-01-02\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_temporal_part` | ERR AMBIGUOUS_REFERENCE `d` [`l`.`d`, `r`.`d`] | ERR COLUMN_ALREADY_EXISTS `id` |
| `csv_empty_header` | {".": ["id,s,v,id,s,v\n"]} | ERR COLUMN_ALREADY_EXISTS `id` |

### FA-6 cells

| Cell | Spark 4.1.2 | main `d3f4fc16` |
|---|---|---|
| `v_crotv` | null | ERR COLUMN_ALREADY_EXISTS `id` |
| `v_ctv` | null | ERR COLUMN_ALREADY_EXISTS `id` |
| `v_ctv_again` | ERR TEMP_TABLE_OR_VIEW_ALREADY_EXISTS: [TEMP_TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create the temporary view `v2` because it … | ERR COLUMN_ALREADY_EXISTS `id` |
| `v_cgtv` | null | ERR COLUMN_ALREADY_EXISTS `id` |
| `v_cgtv_again` | ERR TEMP_TABLE_OR_VIEW_ALREADY_EXISTS: [TEMP_TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create the temporary view `g1` because it … | ERR COLUMN_ALREADY_EXISTS `id` |
| `v_crogtv` | null | ERR COLUMN_ALREADY_EXISTS `id` |
| `v_mixed` | null | ERR COLUMN_ALREADY_EXISTS `id` |
| `v_using` | null | ERR COLUMN_ALREADY_EXISTS `s` |
| `sql_star` | cols `id,s,v,id,s,v` rows [[1, "a", 10, 1, "a", 10], [2, "b", 20, 2, "b", 20]] | ERR view not found |
| `sql_star_order` | cols `id,s,v,id,s,v` rows [[1, "a", 10, 1, "a", 10], [2, "b", 20, 2, "b", 20]] | ERR view not found |
| `sql_dup` | ERR AMBIGUOUS_REFERENCE `id` [`v1`.`id`, `v1`.`id`] | ERR view not found |
| `sql_dup_qual` | ERR AMBIGUOUS_REFERENCE `v1`.`id` [`v1`.`id`, `v1`.`id`] | ERR view not found |
| `sql_dup_s` | ERR AMBIGUOUS_REFERENCE `s` [`v1`.`s`, `v1`.`s`] | ERR view not found |
| `sql_dup_where` | ERR AMBIGUOUS_REFERENCE `id` [`v1`.`id`, `v1`.`id`] | ERR view not found |
| `sql_dup_order` | ERR UNRESOLVED_COLUMN.WITH_SUGGESTION [`id`, `id`, `s`, `s`, `v`] | ERR view not found |
| `sql_dup_group` | ERR AMBIGUOUS_REFERENCE `id` [`v1`.`id`, `v1`.`id`] | ERR view not found |
| `sql_count` | cols `c` rows [[2]] | ERR view not found |
| `sql_describe` | cols `col_name,data_type,comment` rows [["id", "bigint", null], ["id", "bigint", null], ["s", "string", null], ["s", "string", null], ["v", "bigint", null], ["v", "bigint", null]] | ERR view not found |
| `sql_describe_table` | cols `col_name,data_type,comment` rows [["id", "bigint", null], ["id", "bigint", null], ["s", "string", null], ["s", "string", null], ["v", "bigint", null], ["v", "bigint", null]] | ERR view not found |
| `sql_show_columns` | cols `col_name` rows [["id"], ["id"], ["s"], ["s"], ["v"], ["v"]] | ERR view not found |
| `sql_star_except_id` | ERR AMBIGUOUS_REFERENCE `id` [`v1`.`id`, `v1`.`id`] | ERR view not found |
| `sql_star_except_idsv` | ERR AMBIGUOUS_REFERENCE `id` [`v1`.`id`, `v1`.`id`] | ERR view not found |
| `sql_alias_star` | cols `id,s,v,id,s,v` rows [[1, "a", 10, 1, "a", 10], [2, "b", 20, 2, "b", 20]] | ERR view not found |
| `sql_alias_dup` | ERR AMBIGUOUS_REFERENCE `x`.`id` [`x`.`id`, `x`.`id`] | ERR view not found |
| `sql_subq_star` | cols `id,s,v,id,s,v` rows [[1, "a", 10, 1, "a", 10], [2, "b", 20, 2, "b", 20]] | ERR view not found |
| `sql_subq_dup` | ERR AMBIGUOUS_REFERENCE `id` [`q`.`id`, `q`.`id`] | ERR view not found |
| `sql_join_view` | ERR AMBIGUOUS_REFERENCE `v1`.`v` [`v1`.`v`, `v1`.`v`] | ERR view not found |
| `sql_join_view_t` | ERR AMBIGUOUS_REFERENCE `v1`.`v` [`v1`.`v`, `v1`.`v`] | ERR view not found |
| `sql_join_view_dup` | ERR AMBIGUOUS_REFERENCE `v1`.`id` [`v1`.`id`, `v1`.`id`] | ERR view not found |
| `sql_union_star` | cols `id,s,v,id,s,v` rows [[1, "a", 10, 1, "a", 10], [1, "a", 10, 1, "a", 10], [2, "b", 20, 2, "b", 20], [2, "b", 20, 2, "b", 20]] | ERR view not found |
| `sql_mixed_star` | cols `id,s,v,id,t` rows [[1, "a", 10, 1, "x"]] | ERR view not found |
| `sql_mixed_t` | cols `t` rows [["x"]] | ERR view not found |
| `sql_mixed_t_v` | cols `t,v,s` rows [["x", 10, "a"]] | ERR view not found |
| `sql_mixed_id` | ERR AMBIGUOUS_REFERENCE `id` [`vm`.`id`, `vm`.`id`] | ERR view not found |
| `sql_using_star` | cols `id,s,v,s,v` rows [[1, "a", 10, "a", 10], [2, "b", 20, "b", 20]] | ERR view not found |
| `sql_using_id` | cols `id` rows [[1], [2]] | ERR view not found |
| `sql_using_s` | ERR AMBIGUOUS_REFERENCE `s` [`vu`.`s`, `vu`.`s`] | ERR view not found |
| `sql_global_star` | cols `id,s,v,id,s,v` rows [[1, "a", 10, 1, "a", 10], [2, "b", 20, 2, "b", 20]] | ERR view not found |
| `sql_global_dup` | ERR AMBIGUOUS_REFERENCE `id` [`global_temp`.`g1`.`id`, `global_temp`.`g1`.`id`] | ERR view not found |
| `sql_ctas` | ERR COLUMN_ALREADY_EXISTS `id` | ERR view not found |
| `sql_ctas_mixed_t` | cols `` rows [] | ERR view not found |
| `sql_create_view_over` | ERR COLUMN_ALREADY_EXISTS `id` | ERR view not found |
| `sql_view_over_star` | ERR view not found | ERR view not found |
| `sql_insert_select` | cols `count(1)` rows [[1]] | ERR view not found |
| `sql_cache_table` | cols `` rows [] | ERR view not found |
| `sql_cached_star` | cols `id,s,v,id,s,v` rows [[1, "a", 10, 1, "a", 10], [2, "b", 20, 2, "b", 20]] | ERR view not found |
| `sql_cached_dup` | ERR AMBIGUOUS_REFERENCE `id` [`v1`.`id`, `v1`.`id`] | ERR view not found |
| `sql_uncache` | cols `` rows [] | ERR view not found |
| `sql_show_tables` | cols `namespace,tableName,isTemporary` rows [["", "v1", true], ["", "v2", true], ["", "vm", true], ["", "vu", true], ["default", "ctas_m", false]] | cols `namespace,tableName,isTemporary` rows [] |
| `sql_pos_order` | cols `id,s,v,id,s,v` rows [[1, "a", 10, 1, "a", 10], [2, "b", 20, 2, "b", 20]] | ERR view not found |
| `sql_star_where_v` | ERR AMBIGUOUS_REFERENCE `v` [`v1`.`v`, `v1`.`v`] | ERR view not found |
| `table_columns` | ["id", "s", "v", "id", "s", "v"] | ERR view not found |
| `table_schema` | "struct<id:bigint,s:string,v:bigint,id:bigint,s:string,v:bigint>" | ERR view not found |
| `table_collect` | cols `id,s,v,id,s,v` rows [[1, "a", 10, 1, "a", 10], [2, "b", 20, 2, "b", 20]] | ERR view not found |
| `table_select_id` | ERR AMBIGUOUS_REFERENCE `id` [`v1`.`id`, `v1`.`id`] | ERR view not found |
| `table_getitem_id` | ERR AMBIGUOUS_REFERENCE `id` [`v1`.`id`, `v1`.`id`] | ERR view not found |
| `table_select_v_mixed` | cols `t,v` rows [["x", 10]] | ERR view not found |
| `table_drop_id` | cols `s,v,s,v` rows [["a", 10, "a", 10], ["b", 20, "b", 20]] | ERR view not found |
| `table_join` | ERR AMBIGUOUS_REFERENCE `a`.`v` [`a`.`v`, `a`.`v`] | ERR view not found |
| `table_exists` | true | false |
| `list_columns` | ["id", "s", "v", "id", "s", "v"] | ERR view not found |
| `df_columns` | ["id", "s", "v", "id", "s", "v"] | ["id", "s", "v", "id", "s", "v"] |
| `cached_frame_view` | cols `i,s,v,i,s,v` rows [[1, "a", 10, 1, "a", 10], [2, "b", 20, 2, "b", 20]] | ERR COLUMN_ALREADY_EXISTS `id` |
| `cached_frame_view_dup` | ERR AMBIGUOUS_REFERENCE `id` [`vc`.`id`, `vc`.`id`] | ERR view not found |
| `catalog_cache_table` | null | ERR view not found |
| `catalog_cached_star` | cols `id,s,v,id,s,v` rows [[1, "a", 10, 1, "a", 10], [2, "b", 20, 2, "b", 20]] | ERR view not found |
| `drop_view` | true | false |
| `v_sensitive` | null | ERR COLUMN_ALREADY_EXISTS `id` |
| `sql_sensitive_star` | cols `id,s,v,id,s,v` rows [[1, "a", 10, 1, "a", 10], [2, "b", 20, 2, "b", 20]] | ERR view not found |
| `sql_sensitive_dup` | ERR AMBIGUOUS_REFERENCE `id` [`vs`.`id`, `vs`.`id`] | ERR view not found |

### What the cells say

- **csv is the only file door Spark answers.** json, text, parquet, orc and xml refuse
  `COLUMN_ALREADY_EXISTS` naming the first duplicate; csv writes with and without `header`,
  under every `mode`, both case flags, every separator and quoting option measured, and for
  an empty frame (header line only). The header line is the display names joined by the
  separator.
- **A partition column that names a duplicate refuses `AMBIGUOUS_REFERENCE`** with the plan
  qualifiers as candidates; a partition column with a unique name writes, and the header
  drops it.
- **All four view doors register.** `SELECT *`, `spark.table`, `DESCRIBE`, `SHOW COLUMNS`
  and `listColumns` answer the display names. Every reference to a duplicate name refuses
  `AMBIGUOUS_REFERENCE` with the relation as written (`v1`, the alias `x`, the derived
  table `q`) on both candidates; `ORDER BY` of a duplicate over a star refuses
  `UNRESOLVED_COLUMN.WITH_SUGGESTION` instead. A unique name on the same view answers.
- **Durable SQL over the view refuses.** `CREATE TABLE … AS SELECT *` and
  `CREATE TEMP VIEW … AS SELECT *` refuse `COLUMN_ALREADY_EXISTS`.
- **Cached views answer the same.**

## Step 1 — design note (2026-10-08, before any product code)

### One shared rule: duplicate-tolerant engine names

> **Fold 1 (2026-10-08) overrides the last two bullets.** `display_name(engine)` is gone. It
> read the display back from the generated prefix, so a user's own column named
> `__repark_dup_0_id` was renamed on every door (verifier S1). The registration copy now
> records each display name in field metadata, and only that record is read. See "Fold 1".

Both units need the same thing: a relation whose display names repeat while its engine
names do not. DataFusion 54.1.0 cannot hold the repeat itself. A view or a `COPY` source is
scanned under one relation name, and `DFSchema::check_names` (called from
`DFSchema::new_with_metadata` and `try_from_qualified_schema`, reached through
`TableScan::try_new` and `SubqueryAlias::try_new`) refuses two fields with one
`(qualifier, name)` pair: `Schema error: Schema contains duplicate qualified field name`.
A projection that aliases two expressions to one name refuses the same way
(`Projections require unique expression names`). So neither design asks DataFusion to hold
a duplicate. The engine sees unique names; the display names ride beside them.

The rule lives in one new core module,
`crates/repark-core/src/session/df_guards/duplicate_names.rs`:

- `duplicate_tolerant_names(displays)` answers `None` when no display name repeats exactly.
  Otherwise each position whose display repeats becomes `__repark_dup_<position>_<display>`
  and every other position keeps its display. The names are unique because the position is.
- `display_name(engine)` reads the display back out of such a name.
- `rename_duplicate_tolerant(frame, displays)` is the positional rename projection
  (`rename_output_fields` with those names).

The prefix starts with `__repark_`, which `is_scratch_relation` already treats as internal,
so unresolved-column suggestions never list these names. Exact duplicates only: case twins
(`id`, `ID`) keep their own names, as fold SM-2b measured Spark and the engine both hold them.

**Attribute ids.** The user's frame is never rebuilt. Each door renames a registration copy
(a projection over the frame the door already registers), so the ids stamped on the user's
frame, its display overlay and its plan qualifiers are untouched. A frame read back from a
view gets fresh ids when the facade stamps it, as every SQL-door frame does today.

### FA-5 — csv

**Where the refusal lives.** `python/repark/src/repark/spark/dataframe/writer_readwriter.py`
`DataFrameWriter._apply_path_write`: `_refuse_duplicate_output_columns(frame,
exact_only=stored_as == "CSV")`, ahead of both the local and the s3a route.

**What carries the names past it.** The write stays one native
`COPY (SELECT * FROM <view>) TO … STORED AS …`:

1. The csv door registers its scratch view over `rename_duplicate_tolerant(frame, displays)`
   when the display names repeat, so the `COPY` source has unique names. Every other door
   keeps today's registration frame: the rename is opt-in per call.
2. `ReparkSession::text_write_copy_parts` takes the display names. When they repeat and the
   format is csv it resolves `STORED AS repark_text_csv` (the existing temporal sink) and adds
   `'repark.text.display_header' 'true'` to the spec options, whether or not the frame holds
   temporal columns.
3. `ReparkTextSerializer` renames the batch schema through `display_name` before it hands
   the batch to DataFusion's `CsvSerializer`, which writes the header from the batch schema.
   Arrow record batches allow duplicate field names, so this is the physical-only rename:
   the logical plan and the demux never see a duplicate.

The rename is one schema rebuild per batch, only when the flag is set. A frame without
duplicate display names takes the same code path as today, byte for byte: plain
`STORED AS CSV` without temporal columns, the temporal sink with the flag off otherwise.
The per-row path gains nothing, and nothing re-plans.

**Partitioned writes.** Unique names keep their display name under the rename, so
`PARTITIONED BY (t)` still matches and the demux strips `t` before the serializer runs.
A partition column that names a duplicate refuses in the facade with Spark's
`AMBIGUOUS_REFERENCE` text before any view is registered.

**Files.** `crates/repark-core/src/session/df_guards/duplicate_names.rs` (new),
`df_guards.rs` and the `frame_names` export, `text_write_format/{select,spec,serializer,sink}.rs`
and its `map.md`; `crates/repark-python/src/{session_write_options,dataframe_names}.rs`;
facade call sites `writer_readwriter.py` (`_apply_path_write`, `_run_through_temp_view`),
`writer_layout.py` (`run_through_temp_view`, `text_write_copy_parts`), `writer_s3.py`.

**Blast radius.** `text_write_copy_sql` builds the `COPY` for local and s3a csv and json
writes; json never receives display names, so its parts are unchanged. `ReparkTextSerializer`
serves every temporal csv and json write; with the flag off it does what it does today.
`run_through_temp_view` serves `insertInto`, `saveAsTable`, `writeTo` and the path writers;
only the csv path write passes the rename flag. `_refuse_duplicate_output_columns` keeps
refusing parquet, json, orc, `saveAsTable` and `writeTo`.

### FA-6 — temp views

> **Withdrawn in fold 1 (2026-10-08).** This design was built, verified and removed. It is
> kept as the record of what failed and why; see "Why FA-6 was withdrawn". Nothing below
> describes the product.

**Where the refusal lives.** `python/repark/src/repark/spark/dataframe/core.py`
`create_or_replace_temp_view` (shared by `createTempView`) and `create_global_temp_view`
(shared by `createOrReplaceGlobalTempView`): `_refuse_duplicate_output_columns(self,
exact_only=True)`.

**What carries the names past it.**

1. **Registration.** `_register_temp_view` registers
   `rename_duplicate_tolerant(frame, displays)` when the display names repeat. The view's
   engine schema is unique; each duplicated column is named `__repark_dup_<position>_<display>`.
2. **Answers.** A frame the SQL door returns takes a display overlay when any of its engine
   fields carries such a name: display `id`, engine `__repark_dup_0_id`. The overlay is the
   one ATTR-ID-1 already gives a condition join, so `columns`, `schema`, `collect`,
   `toArrow`, `toPandas`, `show`, `drop`, `select` and the writers already read it. Measured
   on main with the overlay set by hand: `S.table('v1').columns` is `id,s,v,id,s,v`,
   `select('id')` refuses `AMBIGUOUS_REFERENCE`, `drop('id')` drops both, `write.parquet`
   refuses `COLUMN_ALREADY_EXISTS`. The facade asks one native function,
   `duplicate_display_names(frame)`, which answers `None` on the first pass over the field
   names when no name carries the prefix. No statement re-plans.
3. **References.** `SELECT id FROM v1` reaches DataFusion as a missing field whose valid
   fields include two names that display as `id`. `stamp_unresolved_column`, the one place
   every unresolved name ends, answers `AMBIGUOUS_REFERENCE` with the written reference and
   one candidate per such field under the relation as written. A miss that matches no
   duplicate keeps `UNRESOLVED_COLUMN`, with the duplicated columns suggested by display name.
4. **Metadata doors.** `DESCRIBE` of a temp view maps names through `display_name`;
   `listColumns` and `spark.table` read the overlay.
5. **Durable SQL.** `CREATE TABLE … AS SELECT` and `CREATE [TEMP] VIEW … AS SELECT` refuse
   `COLUMN_ALREADY_EXISTS` when the query's output carries two fields with one display name,
   as Spark does, so an internal name never reaches a table or a stored view definition.
6. **Global doors.** `createGlobalTempView` and `createOrReplaceGlobalTempView` are
   unsupported for every frame (no `global_temp` catalog). The duplicate refusal ahead of
   that error goes away, so a duplicate-name frame gets the same unsupported error as any
   other frame. Spark registers; that gap is the existing global-temp gap, not FA-6.

**Files.** `duplicate_names.rs` (shared with FA-5);
`crates/repark-core/src/column_resolution.rs` plus a new `column_resolution/duplicate_views.rs`;
`crates/repark-spark/src/ctas.rs`, `view_ddl/` (the temp and catalog `CREATE VIEW` routes) and
the temp-view `DESCRIBE` rows; `crates/repark-python/src/dataframe_names.rs`; facade call sites
`catalog_surface.py` (`_register_temp_view`, `session_table`), `session/sql_run.py`,
`dataframe/core.py` (the two refusals removed; the file-size row ratchets down).

**Blast radius.** Every changed answer sits behind a view registration that refuses on main,
or behind a SQL frame that reads such a view. `stamp_unresolved_column` serves every SQL
statement, but the new branch needs a valid field with the `__repark_dup_` prefix, which only
such a view supplies. `_register_temp_view` also serves the `selectExpr`/`filter` scratch
lowerings through `register_view_without_fill(rename_fields=False)`; that flag keeps today's
frame. `EXPLAIN` keeps printing the true engine plan, as the v1.5.3 notes already state.

### HALT check

- **Outside the allowed crates:** no. Core, spark, python binding, and facade call sites
  that pass one more argument or remove a refusal.
- **Answers outside the measured cells:** no. Every changed answer is a step-0 cell or sits
  behind a door that refuses on main.
- **DataFusion holding duplicate names:** not needed. It refuses at `DFSchema::check_names`
  as described above, and both designs keep the engine names unique.

The unit continues.

## FA-5 — what landed in round 1 (2026-10-08)

> **Fold 1 (2026-10-08) corrects one claim here.** "Every csv write cell is equal to Spark"
> was true of the 31 cells round 1 measured and false in general. The verifier's wider grid
> found byte differences on duplicate-name frames that also occur on unique-name frames:
> they are the csv door's, not this unit's, and they are listed under "The csv door's own
> differences" in "Fold 1". The partition residue below is now a clean, pinned refusal.

**Head against Spark, cell by cell** (`/tmp/fa56/repark-head5.json`, the step-0 probe rerun on
the FA-5 build). Every csv write cell is equal to Spark on bytes or on the refusal, with two
exceptions that are not this unit's:

- `csv_noheader`: repark writes the header when the `header` option is absent, for every
  frame, on main too (`L.write.csv(p)` over a plain frame writes `id,s,v`). Spark's default
  is off. The duplicate-name frame follows the door's existing default; with
  `header=false` the bytes equal Spark's, and the pin sets the option.
- `csv_mode_error_existing`: the same `PATH_ALREADY_EXISTS` condition; main's text has no
  `file:` prefix and no SQLSTATE, as for a plain frame.

Head against main: the probe differs only on the csv write cells. Every json, text, parquet,
orc, avro and xml cell, and every FA-6 cell, is byte-identical to main on this build.

**Cells this unit did not change, recorded because the brief asked for them.**

- Reading the duplicate-header file back (`csv_header_readback*`): the reader fails on main
  and on head, including one caught Rust panic with `header` off (`index out of bounds` in
  `__arrow_c_stream__`). Spark answers `id0,s1,v2,id3,s4,v5`. A reader defect, outside a
  write unit.
- `text`: Spark refuses `COLUMN_ALREADY_EXISTS`; main and head refuse with
  `UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE` and name the twin engine field in the message.
- `orc`, `xml`, `avro`: no path writer on either side of this unit.

**A residue inside FA-5.** `L.select('id', 'id', 's').write.partitionBy('id').csv(…)`: Spark
writes `id=1/` with header `s` (both same-origin columns are partition columns). On head the
engine fails the `PARTITIONED BY` lookup loudly. Main refused the frame outright, so nothing
that answered before answers differently.

**Design note, one correction.** The note said `text_write_copy_parts` takes the display
names. It does not need them: the csv door hands it the renamed frame, and the builder reads
the duplicate-tolerant names from the schema. One argument fewer crosses the binding, and
the s3a route gets the same behaviour from the frame alone.

**Mutations, facade half** (run on the FA-5 build, each reverted, tree clean after):

| Mutation | Change | Red pins |
|---|---|---|
| M3 (a duplicate reaches a door Spark refuses) | the parquet/json refusal in `_apply_path_write` never fires | 5: both `test_other_file_doors_keep_refusing_duplicate_names` cells and three C-061 pins |
| M6 (own) | the partition ambiguity refusal is skipped | 4: every `AMBIGUOUS_REFERENCE` partition pin |
| M7 (own) | the csv door registers without the duplicate-tolerant rename | 25: every csv write pin |

The Rust half ran in the unit's cargo batch and is recorded under "Mutations, Rust and FA-6"
below.

## FA-6 — round 1 as built (2026-10-08), withdrawn in fold 1

> **Withdrawn.** None of the product behaviour in this section exists after fold 1: the view
> doors refuse as on main. The measured Spark cells in the first table stay valid and feed the
> acceptance grid of the FA-6 card.

**Extra cells measured before the code** (live Spark 4.1.2, `/tmp/fa56/spark_probe2.py` and
`spark_probe3.py`, verbatim `/tmp/fa56/spark2.json` and `spark3.json`), because the design
depended on them:

| Cell | Spark 4.1.2 |
|---|---|
| `DESCRIBE v1`, rows in emitted order | `id, s, v, id, s, v` (schema order) |
| `SELECT ID FROM v1` | `AMBIGUOUS_REFERENCE` `` `ID` `` ``[`v1`.`ID`, `v1`.`ID`]`` (the written spelling) |
| `SELECT max(v) FROM v1`, `SELECT id + 1 FROM v1` | `AMBIGUOUS_REFERENCE` |
| `SELECT count(*) AS c FROM v1 HAVING max(id) > 0` | `UNRESOLVED_COLUMN.WITH_SUGGESTION` ``[`c`]`` |
| `SELECT nope FROM v1` / `FROM vm` | `UNRESOLVED_COLUMN.WITH_SUGGESTION` ``[`id`, `id`, `s`, `s`, `v`]`` / ``[`id`, `id`, `s`, `t`, `v`]`` |
| `SELECT a, d FROM v1 AS x(a, b, c, d, e, f)` | rows `[[1, 1], [2, 2]]` |
| `WITH c AS (SELECT * FROM v1) SELECT * FROM c` / `SELECT id FROM c` | display columns / `AMBIGUOUS_REFERENCE` ``[`c`.`id`, `c`.`id`]`` |
| `INSERT INTO tgt SELECT * FROM v1` | writes positionally |
| `CREATE VIEW pv AS SELECT * FROM v1` | `INVALID_TEMP_OBJ_REFERENCE` |
| `CREATE OR REPLACE TEMP VIEW va (a, …, f) AS SELECT * FROM v1` | registers; `SELECT a, d FROM va` answers |
| `SELECT id FROM v1 CROSS JOIN plain` (`plain` has `id`) | `AMBIGUOUS_REFERENCE` ``[`plain`.`id`, `v1`.`id`, `v1`.`id`]`` |
| `SELECT plain.id, w FROM v1 CROSS JOIN plain` | answers |
| `SELECT plain.id AS id FROM v1 CROSS JOIN plain ORDER BY id` | answers |
| `S.table('v1')`: `toDF`, `withColumn`, `filter`, `orderBy`, `union`, `distinct`, `limit`, `select('*')` | display columns kept |
| `S.table('v1').write.parquet` | `COLUMN_ALREADY_EXISTS` |
| `S.table('v1').write.csv(header=True)` | `id,s,v,id,s,v` header |
| `S.table('v1').createOrReplaceTempView('v9')` | registers; columns `id, s, v, id, s, v` |
| `L.select('id','id','s').write.partitionBy('id').csv` | writes `id=1/` with header `s` |
| `JU.write.partitionBy('id').csv` / `partitionBy('s')` | writes `s,v,s,v` / `AMBIGUOUS_REFERENCE` |
| `J.write.partitionBy('S').csv` | `AMBIGUOUS_REFERENCE` `` `S` `` ``[`l`.`S`, `r`.`S`]`` |

**Head against Spark, cell by cell** (`/tmp/fa56/repark-head6.json`, the step-0 probe on the
FA-6 build). Equal on columns and rows, or on condition and text up to the SQLSTATE, for
every view cell except these:

| Cell | Head | Class |
|---|---|---|
| `v_cgtv`, `v_crogtv`, `v_cgtv_again`, `sql_global_*` | unsupported (`no global_temp catalog`) | the global-temp gap, older than this unit; the misleading `COLUMN_ALREADY_EXISTS` ahead of it is gone |
| `v_ctv_again` | `createTempView` replaces an existing view, for every frame | older than this unit |
| `sql_show_columns`, `sql_show_tables` | `SHOW COLUMNS` does not find a temp view; `SHOW TABLES` lists none | older than this unit, the same for a plain view |
| `sql_dup_order` (`SELECT * FROM v1 ORDER BY id`) | `AMBIGUOUS_REFERENCE` | Spark says `UNRESOLVED_COLUMN.WITH_SUGGESTION`; both loud, pinned as a divergence |
| `sql_star_except_idsv` (`* EXCEPT (id, s)`) | `AMBIGUOUS_REFERENCE` naming `id` or `s`, varying by run (DataFusion walks the excluded names unordered) | Spark names `id`, the first written; pinned as a divergence that accepts either name |
| `table_select_id`, `table_getitem_id` | `AMBIGUOUS_REFERENCE` with bare candidates ``[`id`, `id`]`` | the DataFrame door's text for a frame without plan qualifiers; Spark writes ``[`v1`.`id`, `v1`.`id`]``; pinned as a divergence |
| `sql_ctas_mixed_t`, `sql_view_over_star`, `sql_insert_select`, `cached_frame_view.is_cached` | result-shape and text differences of the statements themselves | older than this unit, the same for a plain view |

Three more loud divergences, measured after the design and pinned
(`test_loud_refusals_whose_text_differs_from_spark_divergence`): `HAVING max(id)` refuses
`AMBIGUOUS_REFERENCE` where Spark says `UNRESOLVED_COLUMN`; `SELECT ID FROM v1` reports the
reference lowered under the default case rule, where Spark keeps `ID`; and
`SELECT plain.id AS id FROM v1 CROSS JOIN plain ORDER BY id` refuses where Spark answers.

**The design note, three corrections.**

1. *A silent wrong answer the note did not see.* DataFusion resolves a bare `id` in
   `SELECT id FROM v1 CROSS JOIN plain` to `plain.id`, because `v1` carries only hidden twins
   of that name. Spark refuses three candidates. `refuse_shadowed_duplicates` closes it: after
   a statement plans, a node that reads a plain column whose name a hidden field in the
   node's inputs displays refuses `AMBIGUOUS_REFERENCE` when the statement wrote that name
   bare. The audit cannot tell a bare select alias from a bare reference, so the third
   divergence above is its price: a loud refusal in place of a silent wrong answer. It runs on
   every planned statement on both case paths, reads schemas only, and returns per node on
   the first field check when nothing is hidden. The case-sensitive path now collects the
   statement's written references before planning (one visitor pass, no clone, no re-plan).
2. *The suggestions.* Every valid field of such a view starts with `__repark_`, so the stamp
   returned DataFusion's raw `No field named …` text listing the internal names. A missing
   name now lists the display names.
3. *Catalog `CREATE VIEW`.* Spark refuses a persistent view over a temp view
   (`INVALID_TEMP_OBJ_REFERENCE`); repark creates one. With a duplicate-name view below it the
   create refuses `COLUMN_ALREADY_EXISTS`, so no stored definition carries an internal name.

**Residues inside FA-6**, all loud or cosmetic, none silent:

- `SELECT struct(*)`-style nesting is not user-reachable on this door (`struct(*)` does not
  parse); a user who writes the internal name in backticks (`` SELECT `__repark_dup_0_id` ``)
  gets that column, shown as `id`.
- Two views joined and wrapped in one derived table (`SELECT * FROM (SELECT * FROM v1 CROSS
  JOIN v1) q`) fail DataFusion's duplicate-name check, as any two relations with one column
  name do on this door.
- `EXPLAIN` prints the engine plan, internal names included, as the v1.5.3 notes state for
  the twin names.
- The UDF-rewrite path of `spark.sql` (`_sql_with_registered_udfs`) and the UDTF path return
  their own frames and do not pass through the overlay.

**Out of scope, observed.** A SQL temp view defined over a frame-registered view
(`CREATE TEMP VIEW va AS SELECT * FROM pl`) fails `TABLE_OR_VIEW_NOT_FOUND` on a
`__repark_cdf_` scratch table once the Python frame that fed `createDataFrame` is garbage
collected. It reproduces on unique names and on main; the FA-6 pin keeps its source frame
alive. `DESCRIBE <temp view> <column>` is a parse error on main and head.

**No next-release notes draft exists** under `task/roadmap/mid-term/` (the newest notes are
the shipped v1.5.3 file, left untouched), so no line was added.

## Round 1 mutations, Rust and FA-6 (2026-10-08)

> Round 1's record at `a85f0d7b`. The FA-6 rows test code that fold 1 removed; fold 1's own
> mutation table is under "Fold 1".

Each mutation was applied to the final tree, run, and reverted; `git status` shows only the
unit's own changes afterwards. Core mutations run `cargo test -p repark-core --lib -- duplicate`
(35 tests); the two Spark-crate mutations rebuild the native module and run the FA-5 and FA-6
pytest files (90 tests); the facade mutations run the two view files (68 tests).

| Mutation | Change | Red pins |
|---|---|---|
| R-M1, FA-5 (remove the rename) | the csv sink serializer never renames the batch schema | 4: `csv_writes_the_duplicate_display_header`, the separator, partition and temporal sink pins |
| R-M2, both units (the display name reaches the engine) | `duplicate_tolerant_names` returns the display names unchanged | 14: every sink pin, the two name pins, the rename pin, and all five `duplicate_views_tests` (the view cannot register) |
| R-M4, FA-5 (own) | a duplicate csv keeps the plain `STORED AS CSV` | 5: `parts_route_duplicate_csv_to_the_sink_and_leave_the_rest` and four sink pins |
| R-M5, FA-5 (own) | the empty-frame header keeps the engine names | 1: `csv_empty_duplicate_frame_writes_the_display_header` |
| F-M1, FA-6 (remove the rename) | `_register_temp_view` registers the frame as it is | 51 of 68 |
| R-M8, FA-6 (a duplicate reaches a door Spark refuses) | the CTAS refusal is dropped | 1: `test_create_table_as_select_star_refuses_and_creates_nothing` |
| R-M10, FA-6 (own) | a repeated name is never ambiguous on the miss path | 2: `a_repeated_name_is_ambiguous_under_the_relation_as_written`, `the_case_rule_decides_a_respelled_repeat` |
| R-M11, FA-6 (own) | the shadow audit never runs | 1: `a_bare_name_shared_with_a_plain_relation_is_ambiguous` |
| R-M12, FA-6 (own) | the case-sensitive path folds case | 1: `the_case_rule_decides_a_respelled_repeat` |
| R-M9, FA-6 (own) | temp-view `DESCRIBE` shows engine names | 1: `test_describe_and_list_columns_answer_display_names_in_schema_order` |
| F-M2, FA-6 (own) | a SQL-door frame gets no display overlay | 25 of 68 |
| F-M3, FA-6 (own) | `spark.table` skips the overlay | 9 of 68 |

R-M9's run also showed one more red cell, the `* EXCEPT (id, s)` divergence pin. That was not
the mutation: the pin was order-dependent (the engine names either excepted column). It now
accepts both names and was rerun three times green.

## Round 1 no-regression record (2026-10-08)

> Round 1's record at `a85f0d7b`; fold 1 reran every row, see "Fold 1 gates".

| Check | Result |
|---|---|
| Sort grid (`attr-id-1/stack-to-main/reverify2/sort4/sort4.py`, 394 cells) | 394/394 equal on head, on main's Python sources over the same native module, and to the recorded `s4_orch.json` |
| Replay corpus (`attr-id-1/s4/replay_timed.py`, 24 families, 43,989 cells), head against main's Python sources over the same native module | 0 deterministic differences; 2 cells differ, both on the harness's nondeterministic list (`r3.F_cp_bare_alU_join_parent`, `r3.F_cp_sel_str_join_parent`). The corpus holds no duplicate-name csv write or temp-view cell, so no measured cell appears in it |
| `cargo test -p repark-core -p repark-spark -p repark-python --lib` | core 1,309 passed, 1 ignored; spark 2,630 passed, 5 ignored; python 151 passed |
| `cargo fmt --check`, `make rust-clippy`, `make rust-panic-ban` | exit 0 each |
| `pytest python/repark/tests -k "csv or view or write or attr or duplicate" -n 8` | 4,137 passed, 59 skipped, 9 xfailed (4,080 on the FA-5 build, plus FA-6's 57 pins) |
| attr-id and sort suites (27 files, `-n 8`) | 869 passed, 9 skipped, 3 xfailed on head and on main's sources |
| Every suite touching `fillna` / `.fill(` (14 files) | 498 passed, 65 skipped, 3 xfailed on head and on main's sources |
| Self-join suites (every file naming `_self_join`, `self-join` or `_LEGACY_ERROR_TEMP_1182`) | main's 21 files: 835 passed, 3 xfailed; head's 23 files: 925 passed, 3 xfailed. The difference is this unit's two new files (33 and 57 tests) |
| `python/repark/tests/test_dfcore_1_exports.py` | 10 passed; `dataframe/core.py` gains no module-level name and ratchets 3462 → 3456 |
| Parity-harness suite (`python/repark-parity/tests`, the CAP-1 and REG-1 mirrors included) | 788 passed, 32 skipped, 12 xfailed; the CAP-1 mirror row for `core.py` follows the ratchet |

The main baseline for the grid and the corpus is main's Python tree (`origin/main`, a detached
worktree) run over head's native module. That isolates the unit soundly: every Rust change
is inert unless a field carries the `__repark_dup_` prefix or the csv spec carries the
display-header option, and only head's facade produces either.

**Perf guard.** No statement re-plans. A csv write of a frame without repeated display names
builds the same `COPY` as before (pinned by `parts_route_duplicate_csv_to_the_sink_and_leave_the_rest`
and the unchanged text-write suites); the serializer's rename is one schema rebuild per batch
behind a flag only a duplicate-name write sets. `spark.sql` and `spark.table` make one extra
binding call that scans the result's field names. Every planned SQL statement takes one
schema walk (`refuse_shadowed_duplicates`), and the case-sensitive path one more AST visit.

## Why FA-6 was withdrawn (2026-10-08)

Round 1 registered a duplicate-name frame as a temp view under unique engine names, laid the
display names over the frames the SQL door returned, and audited planned statements to
re-create Spark's ambiguity. The verifier ran 850 view cells against live Spark 4.1.2
(`verify/c_views.py`, `d.py`; views `vj` self-join, `vm` mixed join, `vu` `USING`, `vs`
`select('id','id','s')`, `vn` a union of same-origin repeats, `v3` three repeats, `vt`
`id, v AS id, s AS ID`; beside `plain(id, w)` and `ps(id, s)`). Its findings on FA-6:

**Silent wrong answers (S1).** Main cannot produce any of these: it does not register the view.

1. **`NATURAL JOIN` joins on the wrong keys.** The common-column set is computed over engine
   names, so a repeated display name is never a key. `SELECT * FROM vm NATURAL JOIN plain`
   returned 3 rows and 7 columns, a cross join, where Spark returns 1 row
   `[1,a,10,1,x,100]` with columns `id,s,v,id,t,w`; `plain NATURAL JOIN vj` returned 6 rows
   for Spark's 2; `ps NATURAL JOIN vu` joined on `id` alone. 28 of 32 `NATURAL JOIN` cells
   differed in rows and columns, with no error.
2. **A bare repeated name inside a subquery binds to the outer relation.**
   `SELECT * FROM plain WHERE EXISTS (SELECT 1 FROM vj WHERE id = 7)` returned `[[7,700]]`
   although `vj` holds no id 7; `LATERAL (SELECT id AS x FROM vj)` returned `plain.id` as
   `x`. The inner miss fell through to the outer scope as an outer-reference column, which
   the audit did not match. Spark refuses `AMBIGUOUS_REFERENCE`.
3. **A qualified reference answers the case twin.** Under the default case rule
   `SELECT vt.id FROM vt` returned the `ID` column's strings. Spark refuses with three
   candidates.

**Undeclared gaps (S2).**

1. **Same-origin repeats refuse where Spark answers.** On `vs`, `vn` and `v3` every
   reference (`SELECT id`, `WHERE`, `GROUP BY`, `ORDER BY`, a window, `* EXCEPT`, a join
   condition, through a derived table, a CTE and `CACHE TABLE`, about 60 cells) refused
   `AMBIGUOUS_REFERENCE`. Spark answers: the candidates are one attribute.
2. **`JOIN … USING` on the repeated name refused** where Spark answers (it joins on the first
   `id`); `SELECT plain.id FROM vj CROSS JOIN plain ORDER BY id` refused where Spark answers
   (a second shape of the alias false refusal round 1 had declared); `ORDER BY` of a repeated
   name outside the select list said `AMBIGUOUS_REFERENCE` for Spark's
   `UNRESOLVED_COLUMN.WITH_SUGGESTION`.
3. **Engine names in error text** (the view half): under `caseSensitive=true`,
   `SELECT Id FROM vt` suggested ``[`__repark_dup_0_id`, `__repark_dup_1_id`, `ID`]`` for
   Spark's ``[`ID`, `id`, `id`]``.
4. **`explain()` printed engine names** for a frame read from such a view.

Round 1 had itself found and closed one such scope (a bare name resolving to a plain
relation's column beside the view). That was the pattern, not the exception: the engine held
unique names and an audit re-created the ambiguity afterwards, so **every scope the audit did
not model was a silent wrong answer.** A refusal at registration is safe. A registered view
that answers wrongly is not. The design is withdrawn, the shadow audit with it, and the ask is
filed as [its own card](../../roadmap/mid-term/fa-6-duplicate-view-schemas-card-2026-10-08.md)
with these shapes as its acceptance grid.

**What the tree holds now.** `git diff origin/main...HEAD` touches no view, catalog-surface,
column-resolution, CTAS or `DESCRIBE` path: `column_resolution.rs`, `ctas.rs`, `view_ddl/`,
`catalog_surface.py`, `session/sql_run.py`, `dataframe/core.py`, `check_lib_py.py` and the
CAP-1 mirror are byte-identical to main, and no statement pays for an audit. The four view
doors refuse a duplicate-name frame with main's `COLUMN_ALREADY_EXISTS`; the ATTR-ID-1 pins
(`test_attr_id_1_sm2_dupviews.py`, 11 tests) are main's again, and
`test_a_duplicate_name_view_stays_refused_so_no_bare_name_binds_beside_it` pins that the
round-1 silent answer cannot occur: the view does not exist to stand beside a plain relation.

## Fold 1 — FA-5 as it ships (2026-10-08)

### The prefix finding (S1) and the fix

Round 1 recovered a display name from the generated engine name's prefix. A user's own unique
column named `__repark_dup_0_id` therefore lost its name: csv header `id,s,v`, and with FA-6
in place `SELECT *`, `spark.table`, `listColumns` and `DESCRIBE` answered `id`;
`SELECT 1 AS __repark_dup_3_x` answered a column `x`; `__repark_dup_7_` became an empty name.

The rename is now carried as data:

- `rename_duplicate_tolerant` records each display name in the output field's metadata under
  `repark.display`, the way ATTR-ID-1 carries attribute ids. The generated engine name is only
  a way to be unique, and it steps around any display name of the same spelling.
- `recorded_display_names(schema)` is the only reader. `build_text_write_copy_parts` hands
  the list to the sink as `repark.text.display_header_hex`; the format pairs it with the
  writer plan's input schema by position and the serializer renames the batch through that
  map. The empty-part header of the s3a route reads the same record.
- No code path matches on the `__repark_dup_` spelling. With FA-6 withdrawn no SQL, view or
  catalog path knows it at all.

Pinned: a unique `__repark_dup_0_id` and `__repark_dup_7_` keep their names in the csv header
(Rust and Python), beside a repeat too; `SELECT 1 AS __repark_dup_3_x`, and a view over such a
column through `SELECT *`, `DESCRIBE`, `spark.table` and `listColumns`, answer the user's
name. The verifier's 171 unique-name cells (`b_unique.py`) rerun on the fold: 166 equal to
its run of main's sources, and the other 5 are the cells where its run still had round 1's
native module renaming the column; on the fold they carry the user's name.

### The csv door's own differences (S1, re-graded out of scope)

The verifier's grid (`a_csv.py`, 195 cells against live Spark 4.1.2) found byte differences on
duplicate-name frames. Each one reproduces on a unique-name frame (`b_unique.py`), so it is
the csv door's existing behaviour and applies to a duplicate-name frame as to any frame. This
unit does not change it and no longer claims otherwise. Measured:

| Where | repark | Spark 4.1.2 |
|---|---|---|
| `header` option absent | header written | no header |
| Header or body value with leading or trailing space | written as is | trimmed (`lead,lead,v`) |
| Empty column name; empty string value | written as nothing | `""` |
| A name starting `#` | unquoted | `"#c"` in the first cell |
| A quote in a name or value | escaped as `""` | escaped as `\"` |
| A backslash under `quoteAll` | not doubled | doubled |
| Empty frame (`limit(0)`) | header never quoted and `quoteAll` ignored, so a name holding the separator or a newline splits | header quoted as for a non-empty frame |
| Binary column | hex (`61`) | `[61]` |
| A union frame after `coalesce(1)` | two part files | one |
| Empty partitioned write | a header-only file at the root | no file |

What the duplicate-name change adds, and all it claims: the door writes instead of refusing,
and the header carries the display names. Rerun on the fold, 193 of the 195 cells are
byte-identical to round 1's head; the other two are the partition refusals below.

### Refusal text (S2)

No csv refusal names the scratch view any more. All three shapes refuse in the facade before a
view is registered:

| Shape | Fold 1 | Spark 4.1.2 (`/tmp/fa56/spark4.json`) |
|---|---|---|
| `partitionBy('s')` over two attributes, any case flag | `AMBIGUOUS_REFERENCE` ``[`l`.`s`, `r`.`s`]`` | the same |
| The same with aliases `z`, `a` | ``[`a`.`s`, `z`.`s`]`` (sorted; pinned, VM6 red) | the same |
| `caseSensitive=true`, `partitionBy('S')` where only `s` exists, twice | `Partition column `S` not found in schema struct<…>.`, condition `_LEGACY_ERROR_TEMP_1155` | the same text and condition |
| `select('id','id','s').write.partitionBy('id')` | a declared repark sentence naming `id` (pinned `_divergence`) | writes `id=1/`, `id=2/` with header `s` |

The last row stays a refusal: writing it needs both copies treated as the partition column,
which is not a small change to the `COPY`.

### Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | (Rewritten in fold 1.) `duplicate_tolerant_names` answers `None` without an exact duplicate and otherwise unique engine names that step around every display name; `rename_duplicate_tolerant` renames positionally and records each display name in field metadata; `recorded_display_names` reads only that record, so a column's spelling never decides its display name. | Unit pins on exact, case-twin and collision cases, the record, and unrecorded prefix-shaped names. | PROVEN | `crates/repark-core/src/session/tests/duplicate_names.rs` (`names_without_an_exact_duplicate_answer_none`, `repeated_names_take_their_position_and_unique_names_stay`, `generated_names_step_around_a_user_column_of_the_same_shape`, `display_names_are_read_from_the_record_never_from_a_name`, `rename_gives_unique_engine_names_and_keeps_a_plain_frame`). |
| C-002 | (Narrowed in fold 1.) A csv path write of a frame with exact-duplicate display names writes instead of refusing, and its header carries the display names, for the self, mixed and `USING` joins, every measured option and mode, temporal columns and the empty frame, on the local and the s3a route. A user column named like a generated name keeps its name. Bytes other than the header names are the csv door's own. | One pin per step-0 csv cell with `header` set explicitly; the prefix cells; the s3a route. | PROVEN | `python/repark/tests/test_fa_5_duplicate_csv.py`; `crates/repark-core/src/session/tests/duplicate_names.rs` (the `csv_*`, routing, option round-trip and batch-rename pins); `test_attr_id_1_sm2_dupwrites.py::test_csv_write_of_duplicate_display_names_writes_the_display_header`. |
| C-003 | A csv `partitionBy` over a duplicate display name refuses before any file is created, with text that names only display columns: Spark's `AMBIGUOUS_REFERENCE` (candidates sorted) for two attributes, Spark's not-found text for a case-sensitive spelling miss, and a declared sentence for a same-origin repeat. A unique partition column writes and the header drops it. | One pin per measured cell. | PROVEN | `python/repark/tests/test_fa_5_duplicate_csv.py` (`test_csv_partition_*`, nine tests). |
| C-004 | parquet, json and orc keep refusing a duplicate-display-name frame, and a frame without duplicates writes the same csv bytes as on main. | Control pins per door; unique-name csv bytes compared with a main native module. | PROVEN | `test_fa_5_duplicate_csv.py::test_other_file_doors_keep_refusing_duplicate_names`, `::test_csv_of_unique_names_is_unchanged`; the C-061 pins in `test_attr_id_1_sm2_dupwrites.py`; `parts_route_recorded_csv_to_the_sink_and_leave_the_rest`; the main-native comparison under "Fold 1 gates". |
| C-005 | (Rewritten in fold 1.) The four temp-view doors refuse a frame with exact-duplicate display names exactly as on main, so no statement can read such a view. | Main's pins unchanged, plus a pin that a bare name beside the refused view finds no view. | PROVEN | `python/repark/tests/test_attr_id_1_sm2_dupviews.py` (main's 11 tests, byte-identical); `test_fa_5_duplicate_csv.py::test_a_duplicate_name_view_stays_refused_so_no_bare_name_binds_beside_it`. |
| C-006 | (Round 1.) A reference to a duplicated name on a registered duplicate-name view refuses `AMBIGUOUS_REFERENCE` on both doors. | n/a | REJECTED (withdrawn in fold 1: the verifier found scopes that answered wrongly without an error; see "Why FA-6 was withdrawn") | The verdict's S1 1 to 3. |
| C-007 | (Round 1.) Durable SQL over a registered duplicate-name view refuses `COLUMN_ALREADY_EXISTS`. | n/a | REJECTED (withdrawn in fold 1 with the view registration it guarded) | The fold-1 ruling. |
| C-008 | No regression: the attr-id, sort, fill and self-join suites, the 394-cell sort grid and the replay corpus answer as on main, and a unique-name csv write costs what it cost. | The suites and both harnesses on head and on a main native module; the timing bar. | PROVEN | "Fold 1 gates" below. |

## Fold 1 gates (2026-10-08)

| Check | Result |
|---|---|
| `git diff origin/main...HEAD`, code files | `crates/repark-core/src/session/`: `df_guards.rs`, `df_guards/case_bind.rs` (one re-export), `df_guards/duplicate_names.rs` (new), `path_write.rs`, `text_write_format/{select,spec,serializer,sink,file_format}.rs`, `tests/{mod,duplicate_names}.rs`; `crates/repark-python/src/dataframe_names.rs` (one binding); `python/repark/src/repark/spark/dataframe/{writer_layout,writer_readwriter,writer_s3}.py`, `spark/qualified_names.py`; two test files. No view, catalog-surface, column-resolution, CTAS or `DESCRIBE` file |
| Main native module (`origin/main` worktree, second target dir, `maturin build --profile dev`, under the build rule) against head, the verifier's `b_unique.py`: 171 unique-name cells, 64 of them csv writes, 43 statements with `EXPLAIN` | 171 of 171 identical in bytes, columns, plans and error text |
| Time of a unique-name csv write, head against the main native module, three interleaved processes each, median of per-process medians | 2-row frame, 300 writes per process: 15.86 ms against 15.89 ms (0.9985); 300,000-row frame, 12 writes per process: 363.7 ms against 359.6 ms (1.0115). Both inside the 3% bar; debug builds on a shared box |
| The verifier's `a_csv.py` (195 duplicate-name csv cells) on the fold against its round-1 head | 193 identical; the 2 that changed are the partition refusals, now clean |
| `pytest python/repark/tests -k "csv or view or write or attr or duplicate" -n 8` | 4,089 passed, 59 skipped, 9 xfailed |
| attr-id and sort suites (27 files) | 869 passed, 9 skipped, 3 xfailed, as on main |
| fill suites (14 files) | 498 passed, 65 skipped, 3 xfailed, as on main |
| self-join suites | 22 files, 877 passed, 3 xfailed: main's 21 files and 835 tests plus this unit's file of 42 |
| `python/repark/tests/test_dfcore_1_exports.py` | 10 passed; `core.py` is main's file again, ceiling 3462 |
| Parity-harness suite (`python/repark-parity/tests`) | 788 passed, 32 skipped, 12 xfailed |
| pyarrow in the clone's venv | 25.0.1 |

The cargo gates and the Rust mutations run on this commit and are recorded in the commit that
follows it (CARGO_PENDING).

**Fold 1 mutations**, each applied, run and reverted on the fold's tree. Facade mutations run
the FA-5 file and the two ATTR-ID-1 files (67 tests).

| Mutation | Change | Red pins |
|---|---|---|
| VM1 (verifier; survived round 1) | the s3a csv route registers without the duplicate-tolerant rename | 1: `test_s3_csv_route_hands_the_engine_recorded_display_names` |
| VM6 (verifier; survived round 1) | partition candidates left unsorted | 1: `test_csv_partition_candidates_are_sorted_as_spark_sorts_them` |
| VM2 (verifier) | the partition match no longer folds case | 1 |
| VM3 (verifier) | the frame handed to the `COPY` builder is not renamed | 26 |
| VM4 (verifier) | the parquet door drops out of the duplicate refusal | 3 |
| M6 | the partition refusal is skipped | 7 |
| M7 | the csv door registers without the rename | 2: the `USING` cells, whose engine names repeat. Every other cell stays green, because the header now comes from the recorded list paired by position, not from the registered names |
| F1 (own) | the same-origin partition refusal is dropped | 1 |
| F2 (own) | the case-sensitive not-found refusal is dropped | 1 |
| F3 (own, the withdrawal) | a view door stops refusing a duplicate-name frame | 5: the bare-name pin and four of main's ATTR-ID-1 refusal pins |


```
COVERAGE_ATTESTATION:
  pr_unit: fa-5-6
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each fold-1 item maps to a section or a clause. FA-6 is withdrawn (C-005 rewritten to main's refusal, C-006 and C-007 rejected, the withdrawal section, the card, the reopened registry and card rows). The prefix fix is C-001. The csv claim is narrowed in C-002 and the door's own differences are listed. The refusal texts and the candidate sort are C-003. The s3a pin is in C-002. The main-native comparison and the timing bar are C-004 and C-008.
      artifacts: [task/ledgers/staging/fa-5-6-ledger.md, docs/spark-sql-iceberg-parity.md, task/roadmap/mid-term/fa-6-duplicate-view-schemas-card-2026-10-08.md, task/roadmap/mid-term/v1-5-3-card-2026-10-04.md]
    - id: AT-2
      status: ATTACKED
      evidence: A user column spelled like a generated name, alone and beside a repeat; a generated name colliding with a display name; empty, comma, quote and non-ASCII names through the hex option; case twins; same-origin repeats; a USING join whose engine names repeat; an empty frame; temporal twins; both case flags; every save mode.
      artifacts: [crates/repark-core/src/session/tests/duplicate_names.rs, python/repark/tests/test_fa_5_duplicate_csv.py]
    - id: AT-3
      status: ATTACKED
      evidence: Every partition refusal, the parquet and json controls and the view refusal assert that nothing was created and that the text names no scratch view. A display list whose length does not match the writer plan's input is a loud plan error; bad hex in the option refuses.
      artifacts: [python/repark/tests/test_fa_5_duplicate_csv.py, crates/repark-core/src/session/tests/duplicate_names.rs]
    - id: AT-4
      status: N/A
      justification: No shared state. The record is field metadata on a scratch copy of the frame and an option on one COPY statement; the serializer's map is immutable per write.
    - id: AT-5
      status: N/A
      justification: No credential, path or permission surface changes.
    - id: AT-6
      status: ATTACKED
      evidence: The verifier's three silent wrong answers on views cannot occur, because the view is refused as on main and no view, catalog-surface or column-resolution file differs from main. The prefix rename of a user's column is gone and pinned on five doors. Unique-name csv bytes are identical to a main native module on 171 cells.
      artifacts: [python/repark/tests/test_fa_5_duplicate_csv.py, python/repark/tests/test_attr_id_1_sm2_dupviews.py, task/ledgers/staging/fa-5-6-ledger.md]
    - id: AT-7
      status: ATTACKED
      evidence: A unique-name csv write costs 0.9985 and 1.0115 of main's on a real main native module, inside the 3 percent bar. No statement pays for an audit, since the audit is removed. The batch rename runs only when a display list is present.
      artifacts: [task/ledgers/staging/fa-5-6-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: Refusals assert Spark's condition, SQLSTATE and first line where Spark refuses, and the one declared refusal where Spark writes is a named _divergence pin and a line on registry row FA-5. Registry row FA-6 is main's wording plus one dated line.
      artifacts: [python/repark/tests/test_fa_5_duplicate_csv.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-9
      status: ATTACKED
      evidence: No csv refusal or written byte carries a scratch view name or a generated name, each asserted; the registry row no longer overstates the bytes and lists the door's own differences from Spark.
      artifacts: [python/repark/tests/test_fa_5_duplicate_csv.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: The verifier's two surviving mutations are red, its three others stay red, and the fold adds its own on both tiers, including one that reopens the view door and turns main's refusal pins red.
      artifacts: [task/ledgers/staging/fa-5-6-ledger.md]
  complete: true
```

