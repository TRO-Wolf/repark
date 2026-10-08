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

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `duplicate_tolerant_names` answers `None` without an exact duplicate and unique `__repark_dup_<position>_<display>` names for the repeated positions otherwise; `display_name` inverts it; `rename_duplicate_tolerant` renames positionally. | Unit pins on exact, case-twin, and digit-bearing names, plus the round trip. | OPEN | Closes with the FA-5 Rust commit. |
| C-002 | A csv path write of a frame with exact-duplicate display names writes Spark's bytes: the display header (when `header` is set) and the rows, for the self, mixed and `USING` joins, every measured option and mode, temporal columns, and the empty frame. | One pin per step-0 csv cell, byte-equal to the recorded Spark file. | OPEN | Closes with the FA-5 commits. |
| C-003 | A csv `partitionBy` over a duplicate display name refuses `AMBIGUOUS_REFERENCE` with Spark's text before any file is created; a unique partition column writes and the header drops it. | One pin per measured cell. | OPEN | Closes with the FA-5 commits. |
| C-004 | parquet, json, orc and text keep refusing a duplicate-display-name frame, and a frame without duplicates writes the same csv and json bytes as on main. | Control pins per door; the temporal and plain csv suites unchanged. | OPEN | Closes with the FA-5 commits. |
| C-005 | `createOrReplaceTempView` and `createTempView` register a frame with exact-duplicate display names; `SELECT *`, `spark.table`, `DESCRIBE` and `listColumns` answer the display names and Spark's rows, cached or not. | One pin per step-0 cell. | OPEN | Closes with the FA-6 commits. |
| C-006 | A reference to a duplicated name on such a view refuses `AMBIGUOUS_REFERENCE` with Spark's reference and candidates on the SQL door and the DataFrame door; a unique name answers. | One pin per measured reference shape. | OPEN | Closes with the FA-6 commits. |
| C-007 | `CREATE TABLE … AS SELECT *` and `CREATE VIEW … AS SELECT *` over such a view refuse `COLUMN_ALREADY_EXISTS`, and no durable surface carries a `__repark_dup_` name. | Refusal pins plus a no-internal-name scan. | OPEN | Closes with the FA-6 commits. |
| C-008 | No regression: the attr-id, sort, fill and self-join suites and the 394-cell sort grid answer as on main. | The named suites green on the head build; the grid diffed against main. | OPEN | Closes with the last commit. |
