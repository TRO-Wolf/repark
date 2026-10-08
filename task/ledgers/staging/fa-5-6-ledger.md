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

## FA-5 — what landed (2026-10-08)

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

## FA-6 — what landed (2026-10-08)

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

## Mutations, Rust and FA-6 (2026-10-08)

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

## No regression (2026-10-08)

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

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `duplicate_tolerant_names` answers `None` without an exact duplicate and unique `__repark_dup_<position>_<display>` names for the repeated positions otherwise; `display_name` inverts it; `rename_duplicate_tolerant` renames positionally. | Unit pins on exact, case-twin, and digit-bearing names, plus the round trip. | PROVEN | `crates/repark-core/src/session/tests/duplicate_names.rs` (`names_without_an_exact_duplicate_answer_none`, `repeated_names_take_their_position_and_unique_names_stay`, `display_name_reads_the_display_back`, `rename_gives_unique_engine_names_and_keeps_a_plain_frame`). |
| C-002 | A csv path write of a frame with exact-duplicate display names writes Spark's bytes: the display header (when `header` is set) and the rows, for the self, mixed and `USING` joins, every measured option and mode, temporal columns, and the empty frame. | One pin per step-0 csv cell, byte-equal to the recorded Spark file. | PROVEN | `python/repark/tests/test_fa_5_duplicate_csv.py` (the header, option, mode, case-flag, temporal, empty and byte-equal pins); `crates/repark-core/src/session/tests/duplicate_names.rs` (the `csv_*` sink pins and the routing and batch-rename pins); `test_attr_id_1_sm2_dupwrites.py::test_csv_write_of_duplicate_display_names_writes_the_display_header`; head probe `/tmp/fa56/repark-head5.json` against `/tmp/fa56/spark.json`. |
| C-003 | A csv `partitionBy` over a duplicate display name refuses `AMBIGUOUS_REFERENCE` with Spark's text before any file is created; a unique partition column writes and the header drops it. | One pin per measured cell. | PROVEN | `python/repark/tests/test_fa_5_duplicate_csv.py` (`test_csv_partition_by_*`, five tests, six cells). |
| C-004 | parquet, json, orc and text keep refusing a duplicate-display-name frame, and a frame without duplicates writes the same csv and json bytes as on main. | Control pins per door; the temporal and plain csv suites unchanged. | PROVEN | `python/repark/tests/test_fa_5_duplicate_csv.py::test_other_file_doors_keep_refusing_duplicate_names`, `::test_csv_of_unique_names_is_unchanged`; the unchanged C-061 pins in `test_attr_id_1_sm2_dupwrites.py` (parquet, json, orc, `saveAsTable`, `writeTo`); `parts_route_duplicate_csv_to_the_sink_and_leave_the_rest` in the Rust file; the brief's pytest selection green on the FA-5 build (4080 passed, 59 skipped, 9 xfailed). |
| C-005 | `createOrReplaceTempView` and `createTempView` register a frame with exact-duplicate display names; `SELECT *`, `spark.table`, `DESCRIBE` and `listColumns` answer the display names and Spark's rows, cached or not. | One pin per step-0 cell. | PROVEN | `python/repark/tests/test_fa_6_duplicate_views.py` (the door, star, seventeen-shape SQL, describe, `spark.table`, re-register, cache, replace and control pins); `test_attr_id_1_sm2_dupviews.py` (rewritten: the two local doors register, the global doors are unsupported like any frame); head probe `/tmp/fa56/repark-head6.json` against `/tmp/fa56/spark.json`. |
| C-006 | A reference to a duplicated name on such a view refuses `AMBIGUOUS_REFERENCE` with Spark's reference and candidates on the SQL door and the DataFrame door; a unique name answers. | One pin per measured reference shape. | PROVEN | `python/repark/tests/test_fa_6_duplicate_views.py::test_a_reference_to_a_repeated_name_is_ambiguous` (sixteen shapes), `::test_table_frame_refuses_a_repeated_name_on_the_dataframe_door`, `::test_case_sensitive_session_answers_the_same_cells`, `::test_a_missing_name_suggests_display_names_only`, and the seven `_divergence` cells; `crates/repark-core/src/column_resolution/duplicate_views_tests.rs` (five tests, both case paths). |
| C-007 | `CREATE TABLE … AS SELECT *` and `CREATE VIEW … AS SELECT *` over such a view refuse `COLUMN_ALREADY_EXISTS`, and no durable surface carries a `__repark_dup_` name. | Refusal pins plus a no-internal-name scan. | PROVEN | `python/repark/tests/test_fa_6_duplicate_views.py::test_create_table_as_select_star_refuses_and_creates_nothing`, `::test_create_temp_view_as_select_star_refuses_unless_renamed`, `::test_more_sql_shapes_over_the_view_answer_as_spark` (catalog `CREATE VIEW` and `saveAsTable` refuse), `::test_durable_writes_of_the_view_frame_follow_the_file_doors`, `::test_replace_and_drop_leave_no_display_state_behind`; `test_attr_id_1_sm2_dupviews.py::test_registered_duplicate_view_shows_no_internal_names`. |
| C-008 | No regression: the attr-id, sort, fill and self-join suites and the 394-cell sort grid answer as on main. | The named suites green on the head build; the grid diffed against main. | PROVEN | The "No regression" table above: sort grid 394/394, replay corpus 0 deterministic differences in 43,989 cells, the attr-id, sort, fill and self-join suites with the same counts on head and on main apart from this unit's own pins. Cited from `python/repark/tests/map.md` (the FA-6 entry). |

## Every-round gates (2026-10-08, on the unit's last code commit)

The cargo gates ran in one lock hold on the final Rust tree (no `.rs` file changed after it):
`cargo fmt --check`, `make rust-clippy`, `make rust-panic-ban` and
`cargo test -p repark-core -p repark-spark -p repark-python --lib`, exit 0 each, then
`make develop`. On the same tree, exit 0 each: `python3 scripts/check_rust_file_size.py`,
`./scripts/check_lib_rs.sh`, `./scripts/check_lib_py.sh`, `./scripts/check_crate_dag.sh`,
`python3 scripts/sync_map_md.py --check`, `bash scripts/check_map_md.sh --base origin/main`,
`python3 scripts/check_docs_links.py`, `python3 scripts/check_ledger_grammar.py`, and
`comment_ban.py /tmp/xattr origin/main HEAD` with 0 hits. The pre-commit hook fired on every
commit.

**Disk.** `/` had 226 GB free at the start; the unit built incrementally in the clone's
existing `target/` and created one detached worktree of `origin/main` (Python sources only,
for the regression baseline), removed at the close. Probe outputs stay under `/tmp/fa56/`.

```
COVERAGE_ATTESTATION:
  pr_unit: fa-5-6
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each brief item maps to a clause. Step 0 is the two cell tables; the design note and its HALT check precede the code; FA-5 is C-001 to C-004 and FA-6 is C-005 to C-007; no regression is C-008; the card rows and registry rows FA-5 and FA-6 are rewritten to what is true; no next-release notes draft exists, which the FA-6 section records.
      artifacts: [task/ledgers/staging/fa-5-6-ledger.md, docs/spark-sql-iceberg-parity.md, task/roadmap/mid-term/v1-5-3-card-2026-10-04.md]
    - id: AT-2
      status: ATTACKED
      evidence: Names that hold digits and underscores round-trip; case twins are not repeats; a repeat beside a case twin, a same-origin repeat, a USING join whose engine names already repeat, an empty frame, temporal twins, both case flags and every save mode are pinned. On the view side, sixteen reference shapes, a bare name shared with a plain relation, an upper-case spelling under both rules and a missing name are pinned.
      artifacts: [crates/repark-core/src/session/tests/duplicate_names.rs, crates/repark-core/src/column_resolution/duplicate_views_tests.rs, python/repark/tests/test_fa_5_duplicate_csv.py, python/repark/tests/test_fa_6_duplicate_views.py]
    - id: AT-3
      status: ATTACKED
      evidence: Refusals are half the unit. The partition ambiguity, the parquet and json controls, CTAS, both CREATE VIEW doors and saveAsTable each assert that nothing was created; the csv error mode leaves the existing path; the scratch view of a csv write is dropped in the existing finally block, which the unit does not touch.
      artifacts: [python/repark/tests/test_fa_5_duplicate_csv.py, python/repark/tests/test_fa_6_duplicate_views.py]
    - id: AT-4
      status: N/A
      justification: No shared state is added. The display names live in the engine field names of the view itself, so replace and drop need no bookkeeping, which test_replace_and_drop_leave_no_display_state_behind pins; the serializer's flag is immutable per write.
    - id: AT-5
      status: N/A
      justification: No credential, path or permission surface changes; error texts gain display names only and lose internal ones.
    - id: AT-6
      status: ATTACKED
      evidence: A silent wrong answer was found and closed. A bare name resolved to a plain relation's column beside a duplicate-name view where Spark refuses; refuse_shadowed_duplicates refuses it and mutation R-M11 turns its pin red. csv bytes are compared with the recorded Spark files, including a byte-equal single-part cell; view rows and Arrow types are compared, never only show.
      artifacts: [crates/repark-core/src/column_resolution/duplicate_views.rs, crates/repark-core/src/column_resolution/duplicate_views_tests.rs, python/repark/tests/test_fa_6_duplicate_views.py]
    - id: AT-7
      status: ATTACKED
      evidence: The perf guard paragraph. No statement re-plans; a csv write without repeated names builds the same COPY, pinned; the batch rename is behind a per-write flag; the SQL door adds one field-name scan per returned frame and one schema walk per planned statement. The replay corpus and sort grid answer identically.
      artifacts: [crates/repark-core/src/session/tests/duplicate_names.rs, task/ledgers/staging/fa-5-6-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: Every refusal asserts Spark's condition, SQLSTATE and first line. Where the text or condition differs from Spark the cell is a named _divergence pin and a line in registry row FA-6; the six ATTR-ID-1 refusal pins the registry said would go red were rewritten in the same commit.
      artifacts: [python/repark/tests/test_fa_6_duplicate_views.py, python/repark/tests/test_attr_id_1_sm2_dupviews.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-9
      status: ATTACKED
      evidence: No error text, DESCRIBE row, listColumns entry, catalog name or written csv byte carries an internal name, each asserted. A missing column on such a view now lists display names where it listed internal ones. EXPLAIN keeps the engine plan, stated in the registry row.
      artifacts: [python/repark/tests/test_fa_6_duplicate_views.py, python/repark/tests/test_fa_5_duplicate_csv.py]
    - id: AT-10
      status: ATTACKED
      evidence: Fifteen mutations, each red and reverted, covering the three the brief names per unit and two or more of the unit's own. One pin proved order-dependent under mutation R-M9 and was corrected to accept both names. The old refusal pins fail on the new code and the new pins fail on main, where every door refuses.
      artifacts: [task/ledgers/staging/fa-5-6-ledger.md]
  complete: true
```
