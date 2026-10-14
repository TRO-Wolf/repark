# ICE-STRUCT-PAIRING-1 step 0: measure, no product code

Base `420dae93a2192d33a0d5b36235d5088fe3219dfb` (branch `fix/ice-struct-pairing-1`).
No file under `crates/*/src` or `python/repark/src` changes on this branch in this round.
Product tree is unchanged since the sketch read at `018d0efd`
(`git log --oneline 018d0efd..HEAD -- crates/repark-iceberg/src crates/repark-spark/src python/repark/src`
prints nothing), so sketch §3 line numbers still hold; lines re-verified at HEAD are marked `ok`.

## 0. Halt table (provisional; H2-H7 complete in the probes section)

| Halt | Verdict | Evidence |
|---|---|---|
| H1 unlisted struct-pairing place | NOT MET (reading; §1b) | eight unclear files cleared by reading, §1b |
| H2 mode via state snapshot | pending probe | see §3 |
| H3 DataFusion raises before the rule | pending probe | see §3 |
| H4 kernel visible to user SQL | pending probe | see §3 |
| H5 source_by_name moves route or cost | reading done, probe pending | readers `router/nested_ns.rs:28`, `router/insert_positional/replace_where.rs:294`, see §3 |
| H6 cost above 1.05 or EXPLAIN differs | pending cost bar | see §5 |
| H7 route to S1/S2/S3 past neither call site | reading done, probe pending | static-partition spelling is the candidate, see §1a/§3 |

Seams: one so far (H1 NOT MET, H2-H7 pending).

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

Pending; runs on the probe branch `probe/struct-pairing-h2` (D2 commit).

## 3. Added cells, both engines (order item d)

Pending; recorder plus `ice_struct_pairing_1_step0.json` beside this file (D3 commit).

## 4. Cost bar (order item f)

Pending; write-cost medians of main's module on the write set under `docs/perf/`,
five interleaved sets (D4 commit).
