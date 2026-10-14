# ICE-STRUCT-PAIRING-1 step 0: measure, no product code

Base `420dae93a2192d33a0d5b36235d5088fe3219dfb` (branch `fix/ice-struct-pairing-1`).
No file under `crates/*/src` or `python/repark/src` changes on this branch in this round.
Product tree is unchanged since the sketch read at `018d0efd`
(`git log --oneline 018d0efd..HEAD -- crates/repark-iceberg/src crates/repark-spark/src python/repark/src`
prints nothing), so sketch §3 line numbers still hold; lines re-verified at HEAD are marked `ok`.

## 0. Halt table

| Halt | Verdict | Evidence |
|---|---|---|
| H1 unlisted struct-pairing place | NOT MET | eight unclear files cleared by reading, §1b |
| H2 mode via state snapshot | NOT MET | probe hands by name, then by position, §2 |
| H3 DataFusion raises before the rule | NOT MET | stored pairs never raise early; recased raises at execution, §2 |
| H4 kernel visible to user SQL | MET | both names resolve in user SQL; Q2 open, §2 |
| H5 source_by_name moves route or cost | NOT MET | two readers; flag true vs false identical rows, §2 |
| H6 cost above 1.05 or EXPLAIN differs | pending cost bar | see §4 |
| H7 route to S1/S2/S3 past neither call site | MET | static-partition route stages with no wrap call, §2 |

Seams: more than one (H4, H7).

## 1a. Route table (order item a)

Two call sites of sketch §4: site 1 is `conform_one_store_expr`
(`crates/repark-iceberg/src/write/store_overflow.rs:286`, flags at `:280`, both `ok`),
reached as 1a through `conform_store_exprs` (`:230`, `ok`) from `transform_store_dml`
(`:83`, `ok`) under the `StoreOverflowCast` analyzer rule (`:34-38`, `ok`,
registered at `crates/repark-spark/src/extension.rs:132`), or as 1b through
`wrap_store_outputs` (`store_overflow.rs:603`, `ok`); site 2 is
`store_assignment_cast_sql` (`crates/repark-iceberg/src/write/update_cast.rs:12`, `ok`)
reached from the MERGE and UPDATE rewrite text. Batch casts: S1 `conform_batch_scoped`
(`crates/repark-iceberg/src/write/conform.rs:142`, cast at `:176`, both `ok`);
S2 `positional_map_all_columns` (`crates/repark-iceberg/src/write/overwrite.rs:144`,
cast at `:171`, `ok`) and `positional_map_column_list` (`:183`, cast at `:228`, `ok`);
S3 the static-partition cast (`crates/repark-iceberg/src/write/partition_overwrite.rs:91`,
cast at `:101`).

| Door | Entry | Call site | Batch cast |
|---|---|---|---|
| `insert` (INSERT..SELECT) | `router/insert_positional.rs:28` void gate, then `spark_ast.rs:144` `execute_passthrough_inner` (`ok`) | 1a | S1 staged append (`partitioned_files.rs:72` calls `conform_batch_scoped`) |
| `insert_values` (INSERT..VALUES) | same as `insert`; `transform_store_dml` takes the `LogicalPlan::Values` arm | 1a | S1 |
| `insert_overwrite` whole-table | `insert_overwrite.rs:39` `execute_insert_overwrite`, then `insert_overwrite/store.rs:5` `conform_types` (`ok`) | 1b (`store.rs:33`, `by_name=false`, gate `INSERT OVERWRITE`, `ok`) | S2 `positional_map_all_columns` |
| `replace_where` | `router/insert_positional/replace_where.rs` (void gate at `:244`); source built by `prepare_source`, planned by passthrough | 1a | S2 (row-filter overwrite stages through `overwrite.rs:71`) |
| `df_insert_into` append | `writer_layout.py:95` `run_through_temp_view` (`ok`) into `session_sql_with_write_options` as INSERT | 1a | S1 |
| `df_insert_into_overwrite` | same facade path as INSERT OVERWRITE | 1b via `conform_types` | S2 |
| `insert_by_name` | `insert_by_name.rs:48` `execute_insert_by_name` (void gate `:79`), `append_by_name_projection` (`:207`) | 1b (`:225`, `by_name=true`, gate `append`, `ok`) | S1 staged append |
| `merge_insert` | `write/merge/insert.rs` `insert_stream_checked`; rewrite text via `merge/nested_assign.rs:391,393` | 2 and 1b (`:184`, `:194`, `by_name=true`, `ok`) | none of S1/S2/S3 (checked stream) |
| `merge_insert_star` | same as `merge_insert` plus star expansion `merge/nested_assign/expand.rs:25,121,205` | 2 and 1b | none of S1/S2/S3 |
| `merge_update` | `write/merge/insert.rs` `update_stream_checked`, wrap at `:256` (`ok`) | 2 and 1b (`:256`, `by_name=true`) | none of S1/S2/S3 |
| `merge_update_star` | same as `merge_update` plus star expansion | 2 and 1b | none of S1/S2/S3 |
| `update` (UPDATE..WHERE) | `write/predicate_dml.rs:412` (`by_name=true`, `ok`); assignment text via site 2 | 2 and 1b (`:412`) | none of S1/S2/S3 (CoW/MoR rewrite) |
| `update_no_where` | same as `update` | 2 and 1b | none of S1/S2/S3 |
| `df_append` | facade `writer_readwriter.py:848` `append` through `run_through_temp_view` as INSERT (`source_by_name=False` today) | 1a | S1 |
| `df_overwrite_partitions` | `writer_readwriter.py:860` `overwritePartitions` through `run_overwrite_partitions` (`writer_layout.py:124`, `dynamic_overwrite=True`, `ok`) | 1b via `conform_types` | S2 (`positional_map_column_list` when a column list rides along, else `all_columns`) |
| `df_save_append` | facade saveAsTable append through `run_through_temp_view` as INSERT | 1a | S1 |
| `overwrite(condition)` | `writer_readwriter.py:868` `overwrite` through `run_overwrite_condition` (`writer_layout.py:131`, `source_by_name=True`, `ok`) into `replace_where.rs` (`by_name_source` at `:294`) | 1a on the planned source | S2 (row-filter overwrite) |

S3 serves the static-partition spelling only: `insert_overwrite.rs:189`
`execute_partition_overwrite` stages through `stage_partition_overwrite_files` and the
partition cast at `partition_overwrite.rs:91/101`. None of the 16 matrix doors takes it;
it is the H7 candidate (see §3).

## 1b. The eight unclear files (order item b)

The sketch counted 21 cast-holding files across the two write trees; the count no longer
reproduces with a bare `cast` grep (82 non-test files at HEAD), so step 0 clears every
struct-capable candidate the §3 tables do not name. Each file below was read at HEAD;
none pairs a struct value on a write, so H1 is NOT MET.

| File | What the read found | Verdict |
|---|---|---|
| `crates/repark-iceberg/src/write/store_cast.rs` | `cast_store_value` (`:224`) refuses unless `is_overflow_store_pair` (`:238`); `store_issue` (`store_overflow.rs:65`) returns `None` for every non-overflow pair, so a struct never reaches `cast_with_options` here | cleared |
| `crates/repark-iceberg/src/write/store_fold.rs` | only wraps overflow issues through `cast_store_value` (`:14-19`, `:72`); same gate as above | cleared |
| `crates/repark-iceberg/src/write/append.rs` | every `cast` hit is `downcast_ref` (Arrow downcast) or test text; no value cast | cleared |
| `crates/repark-iceberg/src/write/nested_column.rs` | only `spark_can_up_cast` over `PrimitiveType` (`:427`, `:544`); structs outside its domain | cleared |
| `crates/repark-iceberg/src/write/ntz_store.rs` | timestamp wall/instant kernels only (`wall_cast_sql` `:42`, `store_kernel_sql` `:55`); no struct arm | cleared |
| `crates/repark-iceberg/src/write/insert_defaults.rs` | only `null_stripped` over `Cast`/`TryCast` exprs (`:528`); default fills, no pairing | cleared |
| `crates/repark-spark/src/update_cast.rs` | delegates to the iceberg `incompatible_*_message` gate (`:12`, `:164`); a check, not a cast | cleared |
| `crates/repark-spark/src/insert_overwrite/assignment_types.rs` | predicate `cast_may_fail_at_runtime` (`:4`); judges, never casts a value | cleared |

Also read and cleared: `cast_gate.rs` (void/uuid rewrites only), `void_type/source_leaves.rs`
(literal names only), `distribution.rs`, `merge/copy_on_write.rs`, `merge/merge_on_read.rs`,
`merge/mod.rs`, `merge/row_lineage.rs`, `static_value.rs`,
`router/insert_positional/partition_append.rs` (the seven the sketch had already cleared;
unchanged since `018d0efd`), and `scan_prune.rs` (read path, no store).

## 1c. L1 check (order item e)

`refuse_insert_source_types` (`crates/repark-spark/src/void_type/insert_source_types.rs:14`,
fronted by `void_type.rs:32`) returns before comparing any types unless the table has a
column that `refuses_double` and the planned source has a `Null` column (`:45-62`). L1 is
a list or map struct field-count mismatch and a VALUES arity mismatch: struct, list and
map types with no Double/Null requirement. The gate never holds both L1 types, so it
cannot render L1 refusals and L1 stands as named.

## 2. Probes of H2, H3, H4, H5, H7

### H2: NOT MET (the snapshot carries the mode; no leak)

Temporary product change on the throwaway local branch `probe/struct-pairing-h2`
(commits `77117a5a` and `17ae50f0`, never pushed, never merged): a `ProbePairingMode`
extension on `ConfigOptions`; `execute_passthrough_inner`
(`crates/repark-spark/src/spark_ast.rs:152`) sets by name on the statement's own
state snapshot when the SQL carries `probe_by_name`; `StoreOverflowCast::analyze`
threads its `config` into `transform_store_dml` and `conform_store_exprs`
(`crates/repark-iceberg/src/write/store_overflow.rs`), which refuses
`probe handed={mode}` on a struct target. `cargo check -p repark-iceberg
-p repark-spark` green; release module built into a second venv
(`CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 uvx maturin@1.14.1 develop --release`).

Probe, one session, table `(id INT, st STRUCT<a: INT, b: INT>)`, printed results:

```text
marked REFUSED: repark_store_overflow_cast
caused by
Error during planning: probe handed=by_name for column `st`
unmarked REFUSED: repark_store_overflow_cast
caused by
Error during planning: probe handed=by_position for column `st`
```

The marked `INSERT ... SELECT` of a reordered struct is handed by name; the second,
unmarked statement on the same session is handed by position. The mode reaches
`conform_store_exprs` through the statement snapshot and does not leak.

One lesson for the unit from the first probe run: with a source-mismatch condition
the probe never fired, because the plan reaching `conform_store_exprs` already
carries DataFusion's own struct cast to the target type. The pairing at this call
site must look through the planner cast (`unwrap_cast` exists for this); H2 itself
only asks about the mode, which arrives.

### H3: NOT MET

On main's module (no probe change needed):

- Reordered `INSERT ... SELECT` stores `[[2,2,1]]`: no raise anywhere, so the rule
  would get its chance. The SELECT source EXPLAINs clean.
- Recased `INSERT ... SELECT` refuses `UnsupportedOperationException: This feature
  is not implemented: Unsupported CAST from Struct("A": Int32, "B": Int32) to
  Struct("a": Int32, "b": Int32)` at execution; the source EXPLAINs clean, so the
  raise sits at or after physical planning, after `StoreOverflowCast` runs at
  analysis.
- Surplus-field top-level `INSERT ... SELECT` stores `[[2,1,2]]`, dropping `c`:
  no raise at all.

Only pairs both engines refuse (L1, logical planning) raise before any rule runs.

### H4: MET (names are user-visible; needs the Q2 ruling)

Reading: the MERGE and UPDATE rewrites are planned with the same function registry
as user SQL (`ctx.sql`), and registry row R-013 already records that the shared
sites render the wall kernel by name, forcing registration, which publishes it.
One probe each on main's module, printed results:

- `SELECT arrow_cast('x', 'Utf8')` resolves and answers `x`.
- `SELECT __repark_cast_timestamp_ns__('2024-01-01 00:00:00')` resolves and answers
  the timestamp.

Answer: no. A function registered by name cannot be called from the rewrite's SQL
text but hidden from user SQL. Q2 for the owner: publish `StorePairKernel` the same
way and extend R-013's row (the sketch lean), or carry the mode in a planning state
private to the rewrite.

### H5: NOT MET

Readers of `StatementWriteOptions.source_by_name` at HEAD: exactly two,
`crates/repark-spark/src/router/nested_ns.rs:28` (nested-supply gate strictness)
and `crates/repark-spark/src/router/insert_positional/replace_where.rs:294`
(`by_name_source`, REPLACE WHERE statements only). No reader sits on the
plain-INSERT append, `overwritePartitions` or `saveAsTable` executor route.
Behavior on main, same reordered struct on fresh tables, printed rows:

- Plain INSERT through `_native.session_sql_with_write_options` with the flag true
  vs false: identical `[[1,10,20],[2,2,1]]`.
- REPLACE WHERE with the flag true vs the SQL spelling: identical `[[2,2,1]]`.

No route move and no answer change observed. Caveat: with the flag true on a
REPLACE WHERE statement, `by_name_source` adds a by-name projection to the plan;
answers match the SQL route, timings not compared here.

### H7: MET

Static-partition overwrite (`INSERT OVERWRITE t PARTITION (p='x') SELECT ...`):
`execute_partition_overwrite` (`crates/repark-spark/src/insert_overwrite.rs:189`)
plans the filled source through passthrough (site 1a runs struct-blind), then
`stage_partition_overwrite_files` (`:275`) stages with no `conform_types` or wrap
call — contrast the dynamic path, which calls `store::conform_types` at `:433` —
and the S3 batch cast (`crates/repark-iceberg/src/write/partition_overwrite.rs:91`,
cast at `:101`) pairs. Behavioral probe on main: a reordered struct stores
`{p: x, id: 2, a: 2, b: 1}`, i.e. Arrow's by-name rule, past neither pairing call
site. Second seam; the unit must give this route a call or a guard.

Halt verdicts: H1 NOT MET; H2 NOT MET; H3 NOT MET; H4 MET; H5 NOT MET; H7 MET.
Seams: more than one (H4, H7).

## 3. Added cells, both engines (order item d)

Live Spark 4.1.2 with Iceberg 1.11 (`sparkenv`, `JAVA_HOME` Zulu 17, Hadoop
catalog; banner `4.1.2 f0bb2e6a47d0ebda424ffd633fcea8644a597954`, zone UTC) and
main's module (built from `293896ce`; product-identical to this base). Table
`(id INT, st STRUCT<a: INT, b: INT>)`, format v2, seed `(10,20)`, written `(1,2)`;
readback `(id, st.a, st.b)`. Machine JSON beside this file:
[ice_struct_pairing_1_step0.json](ice_struct_pairing_1_step0.json), assembled by a
recorder script from the two engines' printed outputs.

| Cell | Spark 4.1.2 | Main | Same? |
|---|---|---|---|
| `overwrite(condition)` reordered | `[[2,2,1]]`, by name; seed gone | `[[2,2,1]]` | yes |
| `overwrite(condition)` renamed | `CFD` for `` `st`.`b` `` | `CFD` for `` `st`.`b` `` | yes |
| write with `mergeSchema`, surplus struct field | `ESF`, extra `c` refused | stores `[[1,10,20],[2,1,2]]`, drops `c` | NO, new divergence |
| BY NAME, two source fields differ only in case | `AMBIGUOUS_COLUMN_NAME` at `` `st`.`a` `` | stores `[[1,10,20],[2,1,2]]` | NO, new divergence |
| BY NAME, recased fields | stores `[[1,10,20],[2,1,2]]` | stores `[[1,10,20],[2,1,2]]` | yes |
| required struct field, NULL supplied | seed stores; NULL refused `NOT_NULL_ASSERT_VIOLATION` (`SparkRuntimeException`) | seed stores; NULL refused Arrow non-nullable (`PySparkException`) | both refuse, texts differ |

Notes: `overwrite(condition)` pairs by name on Spark, matching the facade, which
already binds that door's source by name (`writer_layout.py:131`). The case-twin
refusal confirms hint H-2's text shape (`AMBIGUOUS_COLUMN_NAME` with the dotted
path). The `mergeSchema` cell used `writeTo(t).option("merge-schema","true")` on
both engines; Spark still refuses the surplus struct field.

## 4. Cost bar (order item f)

Pending; write-cost medians of main's module on the write set under `docs/perf/`,
five interleaved sets (D4 commit).
