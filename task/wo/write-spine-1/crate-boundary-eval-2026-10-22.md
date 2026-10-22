# CRATE-BOUNDARY-EVAL-1 — a read-only evaluation of the write-path crate boundary, against Sail (2026-10-22)

Filed by the orchestrator on the owner's word ("do the read only evaluation with Opus … evaluate how sail organizes their iceberg to spark domain boundary"). Written by an Opus 5.5 agent on RePark main `a556289d` and Sail upstream main `d219a985`; nothing built or edited in either tree. Owner page: `docs/artifacts/crate-boundary-eval-2026-10-22.html`. Paths are repo-relative, prefixed `repark:` or `sail:`; the Sail boundary index lives in the research workspace beside the Sail clone, outside this repository.


**Evaluator:** Claude, model id `claude-opus-5-5` (read from my own context). Read-only.
**RePark:** clone at main `a556289d`, branch `eval/crate-boundary`. The ZONE-SEAM-1 branch
(`feat/zone-seam-1`, head `58321e94`) was fetched into the clone only to read records that are not
on main yet. Its code, including `__repark_zone_seam__`, is **not on main**
(`git merge-base --is-ancestor` fails).
**Sail:** `lakehq/sail` main `d219a985` (committed 2026-10-22 10:45 UTC), read-only. `git status`
was clean before and after.
**Commands:** `cargo metadata --locked --offline --format-version 1` on both trees (no `cargo tree`
was needed), `wc -l`, `grep`, `git show` and `git log`. No build, no test, no edit in either tree.
**Path prefixes:** `repark:` is the RePark repo root and `sail:` is the Sail repo root. I ran
`test -e` on every path cited, and `git cat-file -e` on the branch-only ones, which are marked
*(seam branch)*.

How to read the evidence labels:
- **measured** means a count or line number I took from the tree in this session.
- **by reading** means I traced the code path but did not execute it.
- **record** means the number comes from a ledger or card that someone else measured. The source is
  named each time.

---

## 1. What each RePark crate owns

All edges are product (`normal`) edges from `cargo metadata` unless marked. "Lines" is every `.rs`
line under the crate. "OOL tests" counts files named `tests.rs` or `*_tests.rs` and files under any
`tests/` directory. "Rest" is lines minus OOL tests, so inline `#[cfg(test)]` tails are still in it.
"pub items" counts lines matching `pub (async )?(fn|struct|enum|trait|const|type)` anywhere in
`src/`, which over-counts the true export surface. All of it is measured.

| crate | internal edges | lines / OOL tests / rest | pub items | public surface in one line | the one concept it should own |
|---|---|---|---|---|---|
| repark-common | none | 6,370 / 1,604 / 4,766 | 114 | `Error`/`ErrorClass`/`Result` plus 9 `pub mod`s: `java_case`, `java_double`, `names`, `redaction`, `source`, `spark_error`, `spark_timestamp_text`, `surfaces`, `zone_horizon` | Spark-visible vocabulary that needs no engine: errors, names, and scalar zone and number rendering |
| repark-connect | common | 19,736 / 12,039 / 7,697 | 260 | Postgres provider, settings, type maps, read and write, and the `WallClockLocaliser` trait | the external-database table service |
| repark-core | common, connect, iceberg | 87,301 / 36,993 / 50,308 | 625 | `ReparkSession` and the `SqlDialect` / `SessionExtension` / `ExecutionBackend` seams, `local_fs_policy`, `microbatch`, path and Postgres writes, `session_time_zone` | the session: build, catalogs, execution and policy |
| repark-functions | common (also the fork's `iceberg`, an external crate) | 77,493 / 4,322 / 73,171 | 641 | 74 `pub mod`s: the Spark function registry, `analyzer_rules()`, the session-zone and timestamp kernels | Spark expression semantics: functions, casts and analyzer rules |
| repark-iceberg | common | 76,964 / 35,279 / 41,685 | 651 | `catalog`, `write`, `microbatch`, `view`, with about 80 names re-exported from `lib.rs` | the Iceberg table service: catalogs, staging and commit |
| repark-spark | common, core, functions, iceberg, ta | 165,482 / 105,137 / 60,345 | 102 | `SparkDialect`, `SparkExtension` and the statement router | the Spark SQL door |
| repark-sql | common, core, functions, iceberg (dev: spark, ta) | 27,746 / 15,250 / 12,496 | 2 | `AnsiDialect` | the ANSI door |
| repark-ta | core (optional) | 10,805 / 2,684 / 8,121 | 78 | TA-Lib kernels, window UDFs and `TaExtension` | TA kernels |
| repark-ml | none | 1,562 / 0 / 1,562 | 41 | Cholesky, OLS, IRLS and KMeans | ML kernels |
| repark-distributed | core | 5,054 / 2,599 / 2,455 | 38 | `DistributedExecutor` and the local executor | the distributed runtime |
| repark-python | core, functions, ml, spark, ta (dev: common) | 21,174 / 4,611 / 16,563 | 200 | `_native` | the bindings |

**Where the docs and the code disagree:**
1. **Crate count.** `repark:ARCHITECTURE.md` says "ten crates" and lists `repark-connect` as
   pre-declared with "no directory until their first unit". The workspace actually has 11 members,
   `repark-connect` is built, and `repark-core → repark-connect` is a live edge.
   `repark:crates/map.md` still says "C-2 adds `repark-core → repark-connect`".
2. **`repark-functions` edges.** `repark:crates/map.md` calls `repark-functions` "a tier-3 leaf
   with no internal deps". In the code it depends on `repark-common` (it uses `zone_horizon`,
   `spark_timestamp_text` and `java_double`) and on the fork's `iceberg` crate
   (`repark:crates/repark-functions/src/iceberg_system.rs:11`).
3. **"Thin adapter".** `repark:ARCHITECTURE.md` calls `repark-iceberg` a "thin adapter", and
   `repark:crates/repark-iceberg/src/write/map.md` says "DELETE/UPDATE/INSERT need no adapter".
   In the code:
   - `write/` is 37,324 lines including tests.
   - Twelve files hold about 4,457 product lines of Spark store-assignment semantics:
     `update_cast`, `ntz_store`, `store_overflow`, `store_cast`, `store_assign`, `struct_pairing`,
     `negated_null_store`, `void_store`, `nested_ns_gate`, `insert_gate`, `nested_type_sql` and
     `store_fold`. That count excludes inline tests, measured from each file's
     `#[cfg(test)]` line.
   - The crate also has two `AnalyzerRule`s (`InsertStoreAssignment`, `StoreOverflowCast`), seven
     store UDFs, and the CoW/MoR UPDATE and DELETE executors (`predicate_dml`).
4. **The native door and INSERT OVERWRITE.** `repark:ARCHITECTURE.md` says the native door refuses
   INSERT OVERWRITE. `repark:crates/repark-sql/src/router.rs:159-162` executes the PARTITION forms
   of INSERT OVERWRITE. Only CALL is refused, at line 173.
5. **`repark-common`'s description is too narrow.** The docs describe it as "error seed plus
   surfaces". It also holds `zone_horizon` (the scalar zone math that every zone kernel bottoms out
   in), Java number rendering and redaction.

The docs and code agree on every other edge.

---

## 2. Rules that straddle plan time and store time

**Count: 3 of the 7 rules straddle** (zone, floor, pairing). The other 4 are plan-time only.
**1 of the 3** (pairing) already shares one function between plan time and store time.

The site lists are measured. "Doors" refers to the door ids of §3.

### 2a. Session-zone conversion of a zoned or naive timestamp (straddles; cannot share a function across the crate line today)

| kind | site | crate | doors |
|---|---|---|---|
| leaf, plan time | `SparkLtzTimestampCast` (`repark:crates/repark-functions/src/instant_ts.rs:526`, `rewrite_cast` at :578) → `retarget_dml_store_casts` (`timestamp_ntz_cast.rs:78`, called at `instant_ts.rs:542`) | functions | D1, D2, D5 (the DataFusion DML plan) |
| leaf kernels | `store_micros` (`timestamp_ns_cast.rs:523`, 40 lines), `unlocalize_to_walls` (`timestamp_ltz_ntz.rs:364`, 19), `localize_wall_micros_in_zone` (`datetime/session_wall.rs:24`, 4), `parse_session_zone` (`timestamp_cast.rs:689`, 7), `session_time_zone_from_options` (`session_time_zone.rs:202`, 6) | functions, all `pub(crate)` except the last | reached through UDFs |
| leaf, store time, by UDF name | `zone_stores` / `zone_store_frame` (`repark:crates/repark-iceberg/src/write/ntz_store.rs:126`, `:196`; `ctx.udf(name).ok()` at `:221`). Called from `repark:crates/repark-spark/src/insert_overwrite.rs:226`, `:433` and `insert_by_name.rs:218` | iceberg (called by spark) | D3, D4 (top-level columns only: it matches `Type::Primitive`) |
| leaf, assignment SQL text | `paired_assignment_cast_sql` → `store_kernel_sql` (`repark:crates/repark-iceberg/src/write/update_cast.rs:26`), `wall_cast_sql` (`:45`), `ltz_instant_cast_sql` (`:50`) | iceberg | D8, D9, D10 |
| leaf, MERGE insert | `zone_wrapping_stream_sql` (`repark:crates/repark-iceberg/src/write/merge/insert.rs:114`, called at `:192`) | iceberg | D7 |
| **nested store, site A** | `entry_store_cast` (`repark:crates/repark-iceberg/src/write/conform.rs:269`), using Arrow `cast_with_options` at `:278`. No session zone is in scope. | iceberg | D2–D6, D15–D17, D21 at the sink (it converts only on D3, D4 and D21; the others arrive already cast) |
| **nested store, site B** | `nested_leaf_cast_sql` (`update_cast.rs:44`), which emits `arrow_cast((expr), '<type>')` text at `:55` | iceberg | D8, D10 (whole-struct assignment) |
| **nested store, site C** | `insert_stream_checked` (`merge/insert.rs:170`) → `wrap_store_outputs` (`repark:crates/repark-iceberg/src/write/store_overflow.rs:833`) | iceberg | D7 |
| container cast, plan time | not on main. The seam `rewrite_zone_seam` is in `crates/repark-functions/src/zone_seam.rs` *(seam branch, 676 lines)* | functions | D1, D2, D5 once the seam merges |
| scalar math | `repark:crates/repark-common/src/zone_horizon.rs`: `wall_to_micros_earlier` (:246), `wall_at_instant` (:199), `offsets_at_wall` (:212) | common | everything |
| Postgres door copy | `SessionZoneLocaliser` (`repark:crates/repark-core/src/session/zone_localiser.rs:129`), which implements `repark-connect`'s `WallClockLocaliser` (`repark:crates/repark-connect/src/provider/scan.rs:29`). It **refuses** a DST gap or overlap where the Spark leaf cast resolves it, so it is a second zone rule, not a copy of the first. | core | the Postgres write door (out of scope) |
| zone-id parsing | `repark:crates/repark-core/src/session_time_zone.rs` (383 lines), `repark:crates/repark-functions/src/session_time_zone.rs` (279 lines), and a UTC-alias test in `repark:crates/repark-spark/src/router/nested_ns.rs:69` / `:73` | core, functions, spark | three homes |

**Can plan time and store time share one function today?**
- **Leaf conversion: yes, but only by string.** `repark-iceberg` names the
  `repark-functions` kernels through the session's function registry. Three timestamp UDF names
  are each defined twice: `ntz_store.rs:15-17` and `timestamp_ntz_cast.rs:20` /
  `timestamp_ns_cast.rs:34`. Two more store names (`store_overflow.rs:28-29`) also appear in
  `repark-functions` (`ansi.rs`, `analyzer.rs`, `decimal_spark.rs`). That is 5 measured string
  contracts across the crate line.
- **The lookup fails open.** `ntz_store.rs:221` calls `.ok()`, so an unregistered kernel means no
  conversion and a UTC store.
- **Nested conversion: no.** Site A is a batch function with no session. Site B emits a bare
  `arrow_cast`. Site C wraps without any zone-aware call.

### 2b. Nanosecond floor (R-009, R-017) (straddles; plan time and store time run different functions)

| kind | site | doors |
|---|---|---|
| kernel | `store_micros` floors with `div_euclid` (`timestamp_ns_cast.rs:523`). It is the `NarrowTimestampNs` UDF's `invoke_with_args`, which reads the zone from `ScalarFunctionArgs.config_options`. | through UDFs |
| plan time, top-level | `zone_stores` via `wall_kernel_reads` / `store_kernel_udf_name` (`ntz_store.rs:60`, `:47`) | D3, D4 |
| plan time, DML plan | `insert_timestamp_ns::before_analysis` / `after_analysis` (`repark:crates/repark-spark/src/insert_timestamp_ns.rs:16`, `:68`, called at `spark_ast.rs:261`, `:290`) | D1, D2, D5 |
| plan time, typing (R-017) | the door `TypePlanner` plus `SparkTimestampUnitWiden`, installed by three session builders: `repark-spark` extension, `repark-sql` `dialect.rs`, `repark-python` `session_runtime.rs` (record: the R-017 rows of those crates' `map.md`) | all doors |
| store-time guard | `refuse_coarser_timestamp` (`conform.rs:282`), called at `conform.rs:277` and `repark:crates/repark-iceberg/src/write/merge/mod.rs:564`. It **refuses** with an internal error and never floors. | every repark-owned sink |

Plan time and store time do not share a function. A door that reaches the sink without the
plan-time kernel gets a refusal instead of the floor. By reading, the streaming sink D21 is such a
door: `repark:crates/repark-core/src/microbatch/run.rs:479`.

### 2c. Struct field pairing (R-016) (straddles; shares one kernel today)

- **Kernel.** `pair_store_value` / `pair_store_value_at`
  (`repark:crates/repark-iceberg/src/write/struct_pairing.rs:567`, `:578`).
- **Uses of that kernel:**
  - the plan-time UDF `__repark_store_pair__` (`StorePairKernel`, `:779` / `:850`);
  - SQL text through `store_pair_sql` (`:679`), called from `merge/insert.rs:91` and
    `update_cast.rs:24`, `:39`;
  - an `Expr` builder, `store_pair_expr` (`:700`);
  - the case-sensitivity carrier `statement_pairing` (`:890`), installed at
    `repark:crates/repark-spark/src/spark_ast.rs:103`;
  - the sink-side refusal `refuse_unpaired_struct` (`:655`), called at `conform.rs:276`.
- **Gap at the MERGE/UPDATE staging sink.** That sink
  (`repark:crates/repark-iceberg/src/write/merge/session_staging.rs:26`) runs `conform_batch`
  only on its partitioned, non-lineage branch (`:72`). Its unpartitioned branch and its lineage
  branch skip the sink pairing refusal.

### 2d. Rules that do not straddle (plan time only)

| rule | sites | crates | can one function serve? |
|---|---|---|---|
| NTZ nested gates R-011 / R-015 | `refuse_nested_supply` (`repark:crates/repark-spark/src/router/nested_ns.rs:78`, called at `router.rs:143` for every Spark statement), `refuse_zoned_micros` (`:298`), `casts_a_zoned_value_to_naive` (`:703`), `shifts_in_an_update` (`:719`); `casts_zoned_to_naive` and `refuse_nested_ns_supply` (`repark:crates/repark-iceberg/src/write/nested_ns_gate.rs:156`, `:282`). The ZS-9 refusal `refuse_naive_into_instant` is at `nested_ns.rs:335` *(seam branch)*. | spark, iceberg | one gate already serves the Spark door. The native door (D14–D20) and streaming (D21) never reach it. |
| local-file policy PA-02..04 (PA-01 is the dbt profile, not a write door) | `refuse_local_filesystem_plan` / `refuse_local_filesystem_writes` / `location_under_any_warehouse` / `resolve_physical_destination` (`repark:crates/repark-core/src/local_fs_policy.rs:140`, `:152`, `:249`, `:272`), and the wrapping dialect `session_dialect` (`:100`, used at `session.rs:318`). Product call sites are `repark:crates/repark-spark/src/spark_ast.rs:266` and `repark:crates/repark-sql/src/router.rs:343`. The knob is parsed in `repark:crates/repark-functions/src/cardinality.rs:121` and read back by key string at `local_fs_policy.rs:119`. `path_escape_kind` is at `repark:crates/repark-iceberg/src/write/idents.rs:19`, with 3 callers. | core, functions, iceberg | yes, already one function |
| negated-null store | `refuse_negated_null_writes` (`repark:crates/repark-iceberg/src/write/negated_null_store.rs:49`). Product callers: `repark:crates/repark-spark/src/update_cast.rs:166`, `void_type/insert_source_types.rs:82`, `:151`, and `merge/insert.rs:184`, `:319`. Plus the `SparkNegateNullDecimal` analyzer rule (`repark:crates/repark-functions/src/registration.rs:21`). | iceberg, spark, functions | one function with 5 door-local calls |
| nested type SQL | `rewrite_nested_type_tokens` / `rewrite_create_column_types` / `struct_field_required` (`repark:crates/repark-iceberg/src/write/nested_type_sql.rs:62`, `:158`, `:202`). Used by `repark:crates/repark-spark/src/replace_columns.rs:29`, `normalize.rs:17`, `create_table.rs:25` and `repark:crates/repark-sql/src/alter/nested.rs:56`, `create_table/nested_type.rs:12`. | iceberg (a SQL-token rewrite in the table crate) | one function. It is DDL grammar, so it does not belong in a write spine. |

---

## 3. The write doors, enumerated from the code

**Door count: 18 value-writing Iceberg doors** (by reading; definition below). 10 are on the Spark
door, 7 on the native door and 1 is the streaming sink. The DataFrame writers add no doors of their
own, and the Rust append API has one product caller (D18).

Three more Iceberg doors write no new values: DELETE, CALL and TRUNCATE. Postgres, path and text
writes are not Iceberg doors.

**Definition:** a door is a distinct code path from a user entry to the function where its rows
become the sink's stream. Doors are split where the conversion of the rows differs.

### Settling 28 against 37

- **28 is a list of statement spellings, not code paths.** It is the U-8 probe's door list
  (`repark:task/wo/ice-ntz-nested-refuse-1/evidence/probe.py:44-72`): 22 spellings plus 6
  field-assignment spellings, all on the Spark door. They map onto 8 of the 10 Spark code paths
  below. Per path: D1 = 5, D3 = 1, D4 = 4, D5 = 2, D7 = 3, D8 = 3, D9 = 2, D10 = 8, which sums to
  28. The probe has no CTAS family (D6), no options path (D2), no native door and no streaming
  sink.
- **37 is not a door count.** In the ZONE-SEAM-1 ledger *(seam branch,
  `task/ledgers/staging/zone-seam-1-ledger.md`, third halt)* it is the number of `ltz_nested`
  cells per zone that stored Spark's instant on main: 19 doors with a literal source plus 18 with
  a column source.

### How every DataFrame writer reduces to a Spark SQL spelling

- **The mechanism.** Every DataFrame writer registers a temporary view and calls
  `session_sql_with_write_options`
  (`repark:python/repark/src/repark/spark/dataframe/writer_layout.py:95-121` →
  `repark:crates/repark-python/src/session_write_options.rs:16`) with a SQL string.
- **`insertInto`** becomes `INSERT INTO|OVERWRITE t SELECT * FROM v` (`writer_readwriter.py:229`).
- **`writeTo().append()` and an append-mode `saveAsTable`** become a column-list `INSERT` with
  `source_by_name`, or `INSERT … BY NAME` when the frame has extra columns
  (`writer_schema.py:66`, `:69`).
- **`overwritePartitions()`** becomes `INSERT OVERWRITE` (`writer_readwriter.py:861`).
- **`overwrite(cond)`** becomes `INSERT INTO … REPLACE WHERE` (`writer_schema.py:102`).
- **`create`, `replace` and `createOrReplace`** become CTAS or RTAS (`writer_readwriter.py:920`,
  `writer_save.py:37`).
- **`mergeInto`** becomes `MERGE` (`repark:python/repark/src/repark/spark/session/session_core.py:476`).
- **Write options do not count `source_by_name`.** `StatementWriteOptions::is_empty`
  (`repark:crates/repark-spark/src/write_options.rs:36`) checks only the raw options. So
  `writeTo().append()` with no options takes D1, not D2.

### The door table

Every Spark statement enters through `router.rs:88`, then `:133`, then the R-011/R-015 gate at
`:143`, then `execute_inner` at `:284`.

Column key:
- **zone leaf / zone nested** say where the session-zone rule is applied.
- **floor** is where the nanosecond rule runs. G is the sink guard.
- **pair** is where struct pairing runs.
- **gate** is whether the R-011/R-015 gate is reached.
- **void / neg-null** is whether the void and negated-null refusals run.
- UTC means the wall is read as UTC.

| id | entry → conform point → sink | zone leaf | zone nested | floor | pair | gate | void / neg-null | probe spellings |
|---|---|---|---|---|---|---|---|---|
| D1 | INSERT (no options): `router.rs:414` → `catalog_ops.rs:85` / `spark_ast.rs:37` → DataFusion `Dml(Insert)` with the session analyzer → fork `IcebergTableProvider::insert_into` (no RePark sink conform) | analyzer (`instant_ts.rs:542`) | plan `Expr::Cast`: UTC on main, session zone once the seam merges | `insert_timestamp_ns`, plus the kernel through the analyzer | `StoreOverflowCast` analyzer rule | yes | `refuse_insert_source_types` (`router/insert_positional.rs:28`) | insert_values, insert_select, df_insert_into, df_append, df_save_as_table |
| D2 | INSERT with options, WAP, session write conf or owned append: `append_with_options.rs:10` → `spark_ast.rs:63` (the same DML plan, `insert_input` at `:360`) → `append_staged_with_options` / `stage_overwrite_files_with` / `append_with_statement_options` → site A (identity) | analyzer | plan cast | as D1, plus G | analyzer plus sink refusal | yes | as D1 | none |
| D3 | INSERT … BY NAME: `insert_by_name.rs:49` → SELECT passthrough (`:217`) → `zone_stores_by_name` (`:218`) → `wrap_store_outputs` (`:226`) → `stage_*` / `write_*_from_stream` → **site A** | `zone_stores` | **site A (UTC)** | `zone_stores` kernel, plus G | `wrap_store_outputs` plus sink | yes | `:80` | insert_by_name |
| D4 | INSERT OVERWRITE (dynamic, static, PARTITION, VALUES): `insert_overwrite.rs:40` → SELECT passthrough → `zone_stores` (`:226` / `:433`) → `insert_overwrite/store.rs:35` → overwrite stagers → `overwrite.rs:164` / `:223` or `partition_overwrite.rs:101` → **site A** | `zone_stores` | **site A (UTC)** | kernel, plus G | as D3 | yes | `void_type.rs:51` (`refuse_partition_overwrite_sources`) | insert_overwrite, insert_overwrite_values, df_insert_into_overwrite, df_overwrite_partitions |
| D5 | INSERT … REPLACE WHERE: `router/insert_positional/replace_where.rs:186` → `spark_ast.rs:63` (DML plan) → overwrite stagers → site A (identity) | analyzer | plan cast | as D1, plus G | analyzer plus sink | yes | `:245` | insert_replace_where, df_overwrite_condition |
| D6 | CTAS / RTAS / CREATE OR REPLACE: `ctas.rs:158` → passthrough plus `analyze_eagerly` (`:208`) → `stage_*` (`:358`, `:366`) or `write_data_files_from_plan` (`:425`, then `IcebergPartitionWriteExec` and `file_sink`) → site A. The target types are the source types, so no store conversion happens. | n/a (types copied) | n/a (the value carries whatever the plan computed) | G | sink | yes | no | none (CONTAINER-CAST-ZONE-1 names its 6 writers) |
| D7 | MERGE insert arm: `merge.rs:46` → `repark:crates/repark-iceberg/src/write/merge/mod.rs:102` → `merge/insert.rs:170` → `session_staging.rs:26` | `zone_wrapping_stream_sql` | **site C (UTC)** | as zone, plus G (`merge/mod.rs:564`) | `store_pair_sql` (`:91`) plus `wrap_store_outputs` | yes | `merge/insert.rs:183-190` | merge_insert, merge_insert_star, merge_insert_literal |
| D8 | MERGE update, whole struct or `SET *`: `repark:crates/repark-spark/src/merge/nested_assign.rs:398` / `:400` → `paired_*_cast_sql` → merge executor, CoW (`session_staging.rs:26`) or MoR (`merge/merge_on_read.rs:84`) | `store_kernel_sql` | **site B (UTC)** | `store_kernel_sql`, plus G | `store_pair_sql` | yes | `merge/insert.rs:318-325` (`gate_update_probe`) | merge_update_whole, merge_update_whole_literal, merge_update_star |
| D9 | MERGE update, one field: same entry; the leaf goes through `store_kernel_sql` | kernel (session zone) | n/a (leaf) | kernel | n/a | yes | as D8 | merge_update_field, merge_update_field_literal |
| D10 | UPDATE (predicate DML, CoW/MoR): `router/delete_update.rs:60` → `repark:crates/repark-spark/src/update_cast.rs:47` → `spark_ast.rs:431` → `repark:crates/repark-iceberg/src/write/predicate_dml.rs:203` → `predicate_dml/lineage.rs:146` (`store_assignment_cast_sql`) / `predicate_dml.rs:402` (`wrap_store_outputs`) → `session_staging.rs:26` | `store_kernel_sql` | **site B (UTC)** for a whole struct; kernel for a field | as zone, plus G | `store_pair_sql` | yes | `update_cast.rs:164-168` | update_whole_where, update_whole_nowhere, update_col_where, update_col_nowhere, update_field_where, update_field_nowhere, update_field_col_where, update_field_col_nowhere |
| D14 | native plain INSERT: `repark:crates/repark-sql/src/router.rs:163` → `delegate` → fork provider | none (DataFusion with the ANSI planner, ruling ZS-5) | none | type planner | none | **no** | no | none |
| D15 | native INSERT with session write conf: `repark:crates/repark-sql/src/session_insert.rs:8` → `stage_overwrite_files_with` → site A | none | **site A** | G | sink | **no** | no | none |
| D16 | native INSERT OVERWRITE PARTITION: `repark:crates/repark-sql/src/insert_overwrite.rs:23` → stagers → site A | none | **site A** | G | sink | **no** | no | none |
| D17 | native CREATE TABLE AS: `repark:crates/repark-sql/src/create_table.rs:67` → `write_data_files_from_plan` (`:624`) / `stage_*` (`:635`, `:643`) | n/a | n/a | G | sink | **no** | no | none |
| D18 | native v3 create through the Rust append API: `repark:crates/repark-sql/src/v3/create.rs:335` → `repark_iceberg::append` (`repark:crates/repark-iceberg/src/write/append.rs:33`, `conform_batches` at `:43`) → site A. This is the API's only product caller. | none | site A | G | sink | **no** | no | none |
| D19 | native MERGE: `repark:crates/repark-sql/src/merge.rs:39` → shared `execute_merge` (`merge/mod.rs:102`) | as D7/D8 machinery | site C / B | G | yes | **no** | as D7 | none |
| D20 | native UPDATE/DELETE predicate DML: `repark-sql` `router.rs:300` → shared `execute_predicate_dml` | as D10 | site B | G | yes | **no** | no | none |
| D21 | streaming micro-batch sink: `repark:crates/repark-core/src/microbatch/run.rs:465` (`append`) → `stage_overwrite_files_with` (`:479`) → site A. It bypasses the router entirely. | **none** | **site A (UTC)** | **G only** (by reading, a nanosecond source refuses rather than floors) | sink refusal only | **no** | no | none |

**Rows with no store conversion of their own:**
- DELETE (`router/delete_update.rs:11`) copies rows unchanged.
- CALL (`repark:crates/repark-spark/src/call.rs:107`) rewrites data files.
- TRUNCATE.
- UPDATE delegated to the fork provider: I did not trace which UPDATE shapes fall through to it.

**What the table shows:**
- The three nested store sites are confirmed on this main by reading. They match ZS-8's
  instrumented build (record: the WRITE-NTZ-TO-LTZ-NESTED-1 card, *seam branch*).
- **The doors that already convert in the plan (D1, D2, D5) have no site of their own.** They let
  DataFusion's `insert_to_plan` inject an `Expr::Cast`, which the session analyzer rewrites. That
  covers 9 of the 28 probe spellings.
- The ZONE-SEAM-1 ledger records that the seam made 54 NTZ-source cells on these doors right
  (record, *seam branch*, step 6).
- **The defects live on the doors that build their own store projection or SQL text instead:** D3,
  D4, D7, D8, D10 and D21.

**Doors I did not trace, and why:**
- The fork-provider UPDATE delegation, because its trigger set was not visible in the budget.
- `foreach_batch` in streaming (`run.rs:442`), because the user's body does the write.
- The Postgres and path write doors, which are outside Iceberg.
- `execute_unparsable_fallthrough` (`router.rs:943`), because it only returns a DataFusion frame.

---

## 4. How Sail draws the Iceberg-to-Spark boundary (standalone)

Everything here was measured at `d219a985` unless marked "by reading".

**Who owns what:**
- **Spark semantics** live in `sail-plan`. `PlanConfig` (`sail:crates/sail-plan/src/config.rs`)
  carries `session_timezone`, `ansi_mode`, `store_assignment_policy` and `case_sensitive`.
  `sail:crates/sail-spark-connect/src/config.rs:234` fills it from Spark conf. The function kernels,
  including `ConvertTz` (`sail:crates/sail-function/src/scalar/datetime/convert_tz.rs:26`), live
  in `sail-function`.
- **The Iceberg format** lives in `sail-iceberg` (49,711 lines). Its internal edges are `catalog`,
  `common`, `common-datafusion`, `data-source`, `logical-plan`, `physical-plan` and
  `sql-analyzer`. It depends on **neither `sail-plan` nor `sail-function`**, and has no session-zone
  reference (a grep for `time_zone` / `timezone` finds only UTC metadata columns and literal
  parsing).
- `sail-catalog-iceberg` is a REST catalog client (4,648 lines) built on `sail-iceberg`'s spec
  types.

**Where the two meet: one pair of traits in `sail-common-datafusion`.**
- `DataSource::create_writer(&self, ctx, info: SinkInfo) -> LogicalPlan`
  (`sail:crates/sail-common-datafusion/src/datasource.rs:486`), where `SinkInfo { input:
  LogicalPlan, mode, partition_by, … }` (`:252`).
- `LakeSource::plan_row_level_operation(&self, ctx, RowLevelOperation) -> LogicalPlan`
  (`sail:crates/sail-common-datafusion/src/lakesource.rs:198`), where
  `RowLevelOperation = Delete | Update | Merge` (`DeleteInfo` / `UpdateInfo` / `MergeInfo`).
- **What crosses is a DataFusion `LogicalPlan` whose output is already in the table's types.**
  Iceberg's `create_writer` (`sail:crates/sail-iceberg/src/lake_source.rs:116`) wraps `input` in
  an `IcebergWriteNode` without touching its expressions.

**Write entry points share two stages before the sink:**
- **Append and overwrite stage.** Seven calls to `resolve_write_with_builder` reach one
  `create_writer` call (`sail:crates/sail-plan/src/resolver/command/write.rs:559`):
  - `insert.rs:59`, `:124`, `:196` (INSERT INTO, INSERT OVERWRITE, insert into a temporary view);
  - `write_v1.rs:164` (DataFrameWriter);
  - `write_v2.rs:104` (`writeTo`);
  - `write_stream.rs:71` (streaming);
  - `catalog/table.rs:208` (CTAS).
- **The conform inside that stage.** For an existing table, `rewrite_write_input` (`write.rs:847`,
  called at `:355`) builds **one projection of `cast_to(<table column type>)`** by position or by
  name (`write.rs:899`, `:919`). Generated, default and identity columns take the slower path at
  `:979`.
- **One bypass of that conform.** With `mergeSchema` on a Delta or Iceberg target, the conform is
  skipped (`:353`).
- **Row-level stage.** Three entries (`delete.rs:57`, `update.rs:241`, `merge.rs:210`) call
  `plan_row_level_operation`.
- **Totals:** 10 user write commands and 2 shared stages. Procedures are separate
  (`procedure.rs:33`).

**Session-zone and timestamp-unit conversion on a write (by reading; Sail was not run):**
- **The zone lives in the Arrow type.** Sail carries the session zone inside the Arrow type: a
  Spark `TIMESTAMP` resolves to `Timestamp(µs, Some(session_timezone))` (`resolve_timezone`,
  `sail:crates/sail-plan/src/resolver/data_type.rs:427-441`).
- **Expression casts apply the zone once.** An NTZ-to-LTZ cast in an expression goes through
  `ConvertTz` with the session zone (`sail:crates/sail-plan/src/resolver/expression/cast.rs:150-170`).
  The other `ConvertTz` call sites in `sail-plan` are `function/scalar/array.rs:318` and
  `datetime.rs:758`, `:853`.
- **The write stage applies the zone zero times.** `rewrite_write_input` uses plain DataFusion
  `cast_to`. The table columns come from the format schema, which maps `timestamptz` to
  `Timestamp(µs, "UTC")` (`sail:crates/sail-iceberg/src/datasource/type_converter.rs:381-382`).
  So a zoned value is relabelled without a change of instant. A naive value written into a
  `TIMESTAMP` column is read as a UTC wall: the same Spark divergence RePark refuses under ZS-9.
- **No dedicated unit conversion.** I found no nanosecond floor kernel in the write path.
- **The format crate only checks.** `schema_evolution.rs:318` compares time-zone compatibility,
  with a UTC-alias list.

**How Delta and Iceberg share row-level code after `1a449ec4` (#2418, 15 files, +556/−398):**
- The trait methods `create_deleter` and `create_merger` became one `plan_row_level_operation`
  taking a `RowLevelOperation` enum (UPDATE was added).
- `sail:crates/sail-logical-plan/src/row_level.rs` (1,205 lines now) holds the shared
  `RowLevelWriteNode` (`:291`, with `new_merge` / `new_delete` / `new_update`),
  `RowLevelEffectPlans`, and the one struct-pairing function `align_row_level_value` (`:42`).
  Pairing is by name with case sensitivity, and leaves are cast with a plain `cast`.
- `align_row_level_value` is used by `sail-plan` (`update.rs:201`, `merge.rs:853`) and by
  `sail-logical-plan/src/merge.rs` (`:349`, `:1426`).
- Both formats implement the trait identically: `sail:crates/sail-iceberg/src/lake_source.rs:289-297`
  and `sail:crates/sail-delta-lake/src/lake_source.rs:464-472` dispatch to their own
  `expand_{delete,update,merge}_node`, each building a `RowLevelWriteNode`.
- Each format has its own physical planner: `sail:crates/sail-iceberg/src/physical/row_level_planner.rs`
  and `sail:crates/sail-delta-lake/src/physical_plan/planner/row_level.rs`. Both are registered in
  `sail-session/src/planner.rs:104-112`.
- 20 later commits touch that path up to `d219a985`, among them #2419 (Delta UPDATE, unified
  CoW/DV), #2579 (Iceberg CoW), #2655 (Iceberg v3 MoR), #2770, #2784 and #2827. None of them adds a
  second row-level entry point.

**What Sail does not do that RePark does (by reading):**
- It applies no session-zone conversion at the write stage.
- It runs ANSI store assignment only on MERGE and UPDATE. `ansi_store_assignment_compatible`
  (`sail:crates/sail-plan/src/resolver/command/merge.rs:1057`) is used at `:1006`, and the
  `Legacy` check appears at `update.rs:75` and `merge.rs:58`. On the INSERT path `cast_to` runs
  unconditionally.
- It has no nanosecond floor.
- It has no local-file DDL policy. A grep for `local_filesystem` / `allowLocalFile` finds only
  `ObjectStoreUrl::local_filesystem()`.
- It has no R-011/R-015-style refusals.
- It has one dialect, served over Spark Connect, so there is no second door.

**Bottom line for RePark:**
- **Sail has the same crate gap.** Its format crate cannot call its zone kernel either.
- **It does not need to.** Every conversion is a logical-plan projection built by one function in
  the Spark-semantics crate, and the format crate receives a plan already in table types.
- **The zone rides in the type.** Sail carries the session zone in the Arrow type. RePark labels
  `TIMESTAMP` as UTC and corrects the value site by site.

---

## 5. The smallest graph change (standalone)

**Facts from sections 1 to 4 that the options rest on:**
- **The plan-time stage already exists.** `repark-iceberg` already runs at plan time: 2 analyzer
  rules, store UDFs, and SQL text for assignments.
- **The crate gap is already crossed by name.** `repark-iceberg` already reaches `repark-functions`
  kernels through the function registry (5 string contracts; `ntz_store.rs:221`).
- **The 3 nested store sites are door-local.** They sit on 6 doors that build their own projection
  or SQL text: D3, D4, D7, D8, D10 and D21. They are not on the doors that let the DML plan cast.
- **The seam is the missing piece.** Its UDF `__repark_zone_seam__` takes the target type as a
  literal argument, as `arrow_cast` does *(seam branch, `zone_seam.rs:21`, `:40`)*. Any SQL text
  or `Expr` can name it without a crate edge.

### Option A: a leaf crate for conversions under `repark-common`, plus one conform stage in `repark-iceberg`

- **Crates and edges:**
  - New tier-0 crate (for example `repark-temporal`) depending on `arrow` (with `chrono-tz`) and
    `repark-common`.
  - +2 normal edges (functions → temporal, iceberg → temporal). Optionally +1 (core → temporal) to
    retire `zone_localiser`'s copy.
  - Rows added to `repark:scripts/check_crate_dag.py` and `repark:repo-manifest.toml`.
  - Putting the code in `repark-common` instead would give the foundation crate an Arrow
    dependency it does not have today (`chrono` and `thiserror` only).
- **What moves:**
  - The 5 kernels of §2a (76 measured lines, plus private helpers I did not size).
  - The seam's Arrow walker, part of the 676-line `zone_seam.rs` *(seam branch)*.
- **Signature changes:**
  - The `conform_batch*` family gains a zone parameter. It has 11 product call sites outside
    `conform.rs`, in `append.rs`, `partitioned_files.rs`, `write_options.rs` (5),
    `merge/session_staging.rs` and `merge/file_sink.rs`.
  - Or `WriterStagingOverrides` (`repark:crates/repark-iceberg/src/write/write_options.rs:37`)
    gains a `session_zone` field. It has 2 product struct literals:
    `repark:crates/repark-spark/src/write_options.rs:91` and
    `repark:crates/repark-iceberg/src/write/session_write_conf.rs:136`. The other uses go through
    `none()` / `Default`.
- **Rules that become one site:** nested zone at site A only. Sites B and C are SQL text or `Expr`
  and still need a UDF by name. The floor guard could floor instead of refuse.
- **Re-pins:**
  - `crates/repark-spark/tests/ntz_into_ltz_nested.rs` *(seam branch)*.
  - The U-8 `ltz_nested` cells.
  - `repark:crates/repark-iceberg/src/write/conform.rs` unit pins.
- **Risk to the v2 label (2026-11-10):**
  - *Before GATE-RETIRE-1:* the new crate and edge gates in a 19-day window, and the files touched
    are disjoint from GATE-RETIRE-1's.
  - *After:* the same risk, delayed.
- **Verdict:** it fixes the literal "cannot call" but not the door pattern. It costs a crate and
  fixes one of the three sites.

### Option B: the conform stage in `repark-core`, with `repark-iceberg` handing it the schema

- **Crates and edges:**
  - `repark-core` does not depend on `repark-functions`. A `core → functions` edge points from
    tier 2 to tier 3, which `check_crate_dag.py` forbids. So B needs A's leaf crate as well.
  - Every door would then reach the stagers through `repark-core`. Today `repark-spark` and
    `repark-sql` call `repark_iceberg::write::stage_*` / `commit_*` directly, about 45 call lines
    by grep (the §3 entry rows).
- **Signature changes:** new public stage functions on `ReparkSession` or in a `repark-core`
  module. This accretes onto the session that ADR-0005 defers decomposing.
- **Verdict:** no. It needs A anyway, adds a hop, and grows the crate ADR-0005 flags.

### Option C: a Sail-shaped split

- **The change:** move the 12 store-semantics files (about 4,457 product lines, §1 point 3) out
  of `repark-iceberg` into the Spark side, either `repark-spark` or a new tier-3 capability crate
  that both doors may use. `repark-iceberg` would keep catalogs, staging and commit, and receive a
  conformed stream, as `sail-iceberg` does.
- **Crates and edges:**
  - A new crate with edges from `repark-spark`, `repark-sql` and `repark-core` (microbatch).
  - `repark-sql`'s uses of `store_cast` (`repark:crates/repark-sql/src/dialect.rs:35`) and
    `nested_type_sql` move with it.
- **Signature changes:** dozens of `repark_iceberg::write::*` re-exports change. Every `pins:`
  citation and file-size exception keyed by path moves.
- **Verdict:**
  - The evidence supports the direction, not the cost before v2.
  - It reopens the 1.8 layout ruling, which placed only the dialect below `repark-spark`.
  - Sail's lesson is the single plan-time stage, not the crate cut.

### Option D (recommended): a Sail-shaped stage inside the existing graph

- **Crates and edges:** no new crate and no new edge.
- **Step 1, no behaviour change:**
  - Move the 5 cross-crate UDF names into a `repark-common` module (about 10 lines) so both crates
    compile against one constant.
  - Make `ntz_store.rs:221` fail closed: a missing kernel refuses instead of storing the UTC wall.
- **Step 2, the spine, all at plan time with `ctx` in hand:**
  - Turn `zone_stores` plus `wrap_store_outputs` into one function, conforming a frame to target
    types. This touches `ntz_store.rs:126-251` and `store_overflow.rs:833`.
  - Extend it to nested targets by wrapping seam pairs in `__repark_zone_seam__` by name.
  - Route D3 (`insert_by_name.rs:218-226`), D4 (`insert_overwrite.rs:226`, `:433`,
    `insert_overwrite/store.rs:35`), D7 (`merge/insert.rs:192-215`) and D21 (`run.rs:479`, which
    has `batch: &SessionContext` and `frame`) through it.
  - Change site B (`update_cast.rs:44-55`) to emit the seam UDF name in place of a bare
    `arrow_cast` for a nested zone pair.
  - Site A then becomes the identity on every door except D18. D18 has no session, uses
    native-door semantics, and stays as a control.
- **Step 3, the mechanical door matrix:** generate it from `router.rs` `execute_inner` arms plus
  `StatementWriteOptions` flags.
- **Step 4, retire the refusals:** retire ZS-9 and R-011 arm A1 with dated rows.
- **Lines that move:** about 125 (`zone_store_frame`) and about 12 (`nested_leaf_cast_sql`) are
  rewritten in place. Nothing moves crates.
- **Rules that become one site:**
  - nested and leaf zone store: one plan-time function plus the seam UDF;
  - floor: one kernel, reached by every door including D21;
  - pairing: already one.
- **Public signature changes:**
  - `zone_stores` / `zone_stores_by_name` (`pub`) are merged or renamed.
  - `wrap_store_outputs` (`pub`) gains `ctx`, or its callers pass the UDF.
  - The `update_cast` text changes.
  - No trait or crate API visible outside the workspace changes.
- **Re-pins:**
  - `ntz_into_ltz_nested.rs` *(seam branch)*: the refusal becomes the stored session wall.
  - The U-8 `ltz_nested` cells: record 134 refused per zone in New York and in Kolkata, of which
    37 stored Spark's value on main.
  - The R-011 A1 pins in `nested_ns.rs`.
- **Risk to the v2 label:**
  - D depends on ZONE-SEAM-1 (#1139) merging.
  - *Steps 1 and 3 before GATE-RETIRE-1:* no behaviour change, and they make GATE-RETIRE-1's
    verification mechanical. Its files are disjoint from GATE-RETIRE-1's
    (`router/nested_ns.rs`, `nested_ns_gate.rs`).
  - *Steps 2 and 4 after GATE-RETIRE-1:* these move stored values. ZS-9's over-refusal is honest
    and labelled, so v2 does not need them. Landing them after the 2026-11-10 label and before v3
    (2026-11-17) keeps value changes out of the label's freeze.
- **Verdict:** it adopts Sail's one-stage, plan-time shape and settles the "nowhere to put it"
  question without a crate.

### Recommendation

I recommend option D.

- **Before GATE-RETIRE-1:** land step 1 (shared UDF names in `repark-common`, a fail-closed
  lookup) and step 3 (a door matrix generated from the router arms). Neither changes a stored
  value.
- **After the seam merges and after GATE-RETIRE-1:** land step 2, routing D3, D4, D7, D8/D10's
  whole-struct text and D21 through one plan-time conform function that names the seam UDF. Then
  retire ZS-9, preferably after the v2 label.

Options A, B and C each cost a crate or an edge and fix fewer sites. The evidence does not show the
layering is the cause, so a redesign of the crate set is out of scope.

**Is the layering itself the cause?** No. Sail has the same format-crate-cannot-call-the-zone-kernel
gap and still has one conform stage. RePark's three sites come from six doors that each build their
own store projection, while the doors that let the DataFusion DML plan cast already get the rule in
one place.

---

## What I could not measure, and why

- **Door behaviour.** I ran nothing in either tree (read-only brief). Every door and path in §3 is
  by reading, except the A/B/C door assignment, which is ZS-8's instrumented build (record).
- **Sail's write-time zone behaviour** (a naive value read as a UTC wall, and nanosecond handling)
  is by reading only. Sail was not run.
- **The `task/lessons.md` entry dated 2026-10-14** does not exist on this main. Entries jump from
  2026-10-13 to 2026-10-17. I read the 2026-10-22 entry only.
- **The ZONE-SEAM-1 sketch §10b, the WRITE-NTZ-TO-LTZ-NESTED-1 card and the seam ledger** exist
  only on `feat/zone-seam-1`. I read them via `git show FETCH_HEAD:…`.
- **The UPDATE delegation to the fork provider:** I did not trace which shapes fall through to it.
- **Option A's kernel size** includes only the 5 named functions (76 lines). Their private helpers
  and the share of `zone_seam.rs` that would move were not sized.
- **The 45 door-to-stager call lines in option B** are a grep count of `repark_iceberg::write::`
  stage, commit and write calls in `repark-spark` and `repark-sql`, not a reading of each one.
- **`cargo tree`** was not run. `cargo metadata` gave every internal edge.
- **Sail workspace note:** `LocalEngines/AGENTS.md` still lists Sail at `1a449ec4`. I did not edit
  it because the brief allows only the locator file.
