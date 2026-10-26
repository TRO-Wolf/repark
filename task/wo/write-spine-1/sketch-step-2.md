# WRITE-SPINE-1 step (2): the step-0 sketch of one plan-time conform stage (2026-10-25)

**Status:** sketch for [the order](order-step-2.md) §4 step 0, written 2026-10-25 by Claude
(claude-opus-5-5), lane A, on `origin/main` `cf6641a4`. No product code, no test, no map and no
ledger row changed in this round. The build waits for the orchestrator's rulings on §g. This file
closes when step (2) merges; it then moves to `docs/history/` with the unit's other records.

Counts in §d and §e are read from the two committed census fixtures
(`write_spine_1_main_7a789fc5.json`, `write_spine_1_spark_oracle.json`) with
`_write_spine_1_matrix.load_cells`, classified with `_record_write_spine_1.classify` and the census
families of `test_write_spine_1_doors._census_family`. The recount reproduces the fixture: 7,135
EQUAL, 1,731 DIFFERENT, 2,380 REFUSED, 946 ERROR, and 0 cells whose recorded status differs from
`classify`. Nothing was replayed.

## Corrections to the order and the evaluation (lines, not files)

1. `store_overflow.rs` (939) and `store_cast.rs` (994) are not `EXCEPTIONS` rows of
   `scripts/check_rust_file_size.py`. They sit under the default ceiling 1000, with 61 and 6 lines
   of headroom. A shrink needs no ceiling edit.
2. `__repark_zone_seam__` is not registered by name in any session. `repark-functions`
   `registration.rs:16` registers only the two UTC shifts (`zone_seam.rs:58`). The ANSI dialect
   (`repark-sql/src/dialect.rs:29-46`) does not register it either. The seam reaches a plan only
   as an `Arc` from `rewrite_zone_seam` (`instant_ts.rs:587`). So a fail-closed lookup of
   `ZONE_SEAM_NAME` refuses on every session, and SQL text that names it does not plan (Q1).
3. WS2-3's "`store_micros` through `TIMESTAMP_NS_CAST_NAME`": `TIMESTAMP_NS_CAST_NAME` is the cast
   into a nanosecond column (`SparkTimestampNsCast`). `store_micros` (`timestamp_ns_cast.rs:522`)
   is reached in two ways. Into a microsecond wall it runs through `TIMESTAMP_NTZ_CAST_NAME`
   (`timestamp_ltz_ntz.rs:435`). Into a microsecond instant it runs through `NarrowTimestampNs`
   (`__repark_narrow_timestamp_ns__`, `timestamp_ns_cast.rs:463`). Only the analyzer installs that
   kernel (`instant_ts.rs:621`), and it has no registered name and no `udf_names` entry (Q2).
4. The R-011/R-015/ZS-9 gate call is at `router.rs:145`, not `:143`.
5. Site A has a twin. On the MERGE/UPDATE staging sink, the unpartitioned branch and the lineage
   branch (`merge/session_staging.rs:43-67`) run `cast_one_batch_to_write_schema`
   (`merge/mod.rs:555`). That calls `convert_uuid_column`, whose fallback is the same UTC
   `cast_with_options` (`catalog/uuid_presentation.rs:134`), with the coarser guard but without
   `refuse_unpaired_struct`. This is WS2-5's gap: it is a missing guard, not a missing stage.
6. D10 has two sub-paths. An `UPDATE` whose `WHERE` `plain.rs` accepts runs RePark's identity DML
   (`predicate_dml.rs:203`). An `UPDATE` with no `WHERE` (`plain.rs:221` answers `None`), or one
   that `plain_identity_needs_fork` accepts, runs on the fork provider and records D10 through
   `record_planned_update` (`spark_ast.rs:255`). All census `update_*_nowhere` spellings take the
   fork sub-path.
7. D4 has two stage points. The PARTITION path (`insert_overwrite.rs:226`) runs `zone_stores` but
   no `wrap_store_outputs`, so it pairs only at the sink. The overwrite path (`:433`, then
   `insert_overwrite/store.rs:35` at `:444`) runs both.
8. `write/mod.rs` must change: it declares the new module and re-exports `zone_stores` at `:130`.
9. "The 1,512 store-assignment marker cells": `Cannot write incompatible data for the table`
   matches 2,488 main answers. The 976 ZS-9 answers carry the same text, and the C-016 pin assigns
   them to the ZS-9 row first. 1,512 is the remainder.
10. WS2-4 is not a census cell. No spelling of the corpus calls `add_months`.
11. There are fail-open returns beyond C-007 and C-008. `wrap_store_outputs` returns the plan
    unchanged when its top node is not a `Projection` (`store_overflow.rs:842`), or when a
    positional width differs (`:845`). `insert_overwrite/store.rs:29-31` returns the frame
    unconverted when a listed column is missing. The stage closes all of them (§b).
12. The native ANSI session registers the NTZ wall kernel and both ns casts
    (`dialect.rs:36-46`). So D19 and D20 already apply the Spark leaf zone rule through the shared
    executors (`zone_wrapping_stream_sql`, the assignment text).

## (a) Every door, where its rows become the sink's batch, and the three rules today

"Sink" is the function that turns the door's stream into data files. A is `entry_store_cast` (a
UTC Arrow cast with the pairing refusal and the coarser guard). A′ is the twin of correction 5.
G is `refuse_coarser_timestamp`. "Analyzer" means the session's analyzer rules on the DataFusion
DML plan (`SparkLtzTimestampCast` with `rewrite_zone_seam`, `insert_timestamp_ns`,
`StoreOverflowCast`).

| door | rows become the sink's batch at | zone, leaf | zone, nested | floor | pairing |
|---|---|---|---|---|---|
| D1 | the fork's `IcebergTableProvider::insert_into` on the analyzed DML plan (`router.rs:480`, `spark_ast.rs:37`) | analyzer | analyzer (seam) | analyzer (`NarrowTimestampNs`, wall kernel) | `StoreOverflowCast` |
| D2 | `append_with_options.rs:10` → RePark stagers on the analyzed DML plan → A | analyzer | analyzer (seam) | analyzer, plus G | analyzer, plus the A refusal |
| D3 | `insert_by_name.rs:236` stream → `write_*_from_stream*` / `stage_*_with_overrides` → A | `zone_stores_by_name` (`:218`) | **A, UTC** | `zone_stores` kernel, plus G | `wrap_store_outputs` (`:226`), plus A |
| D4 overwrite | `insert_overwrite.rs:445` stream → overwrite stagers → A | `zone_stores` (`:433`) | **A, UTC** | kernel, plus G | `conform_types` → `wrap_store_outputs` (`store.rs:35`), plus A |
| D4 PARTITION | `insert_overwrite.rs:229` `stage_partition_overwrite_files` → A | `zone_stores` (`:226`) | **A, UTC** | kernel, plus G | **A only** |
| D5 | `replace_where.rs:186` → the DML plan → overwrite stagers → A | analyzer | analyzer (seam) | analyzer, plus G | analyzer, plus A |
| D6 | `ctas.rs:358`, `:366`, `:425` → A or `file_sink` | none: target types are the source types | none | G | A |
| D7 | `merge/insert.rs:218` stream → staging sink (`session_staging.rs:26`): A on the partitioned branch, **A′** on the others | `zone_wrapping_stream_sql` text (`:114`, `:192`) | **A or A′, UTC** (site C) | wall kernel text, plus G (`merge/mod.rs:564`) | `store_pair_sql` text (`:91`), `wrap_store_outputs` (`:193`, `:205`), plus A on the partitioned branch only |
| D8 | `update_stream_checked` (`merge/insert.rs:261`) → staging sink | `store_kernel_sql` text in the assignment (`nested_assign.rs:402`) | **site B** `arrow_cast` text, UTC | kernel text, plus G | `store_pair_sql` text, `wrap_store_outputs` (`:270`), plus A or nothing |
| D9 | as D8 | kernel text (`paired_assignment_cast_sql`) | n/a (leaf), or site B on a struct field path | kernel text, plus G | as D8 |
| D10, `WHERE` | `collect_identity_update_rows` (`predicate_dml.rs:386`) → CoW / MoR commit → staging sink | `store_kernel_sql` text and the instant re-plan (`lineage.rs:85-153`) | **site B** (whole-struct text from `nested_assign.rs:400`) | kernel text, plus G | `wrap_store_outputs` (`:402`), plus A or nothing |
| D10, fork | the fork provider's UPDATE on the DML plan | analyzer | analyzer | analyzer | analyzer |
| D14 | the fork provider (bare `repark.sql()` session, `repark-sql` `router.rs:163`) | none (ANSI planner, ZS-5) | none | type planner | none |
| D15 | `repark-sql/src/session_insert.rs:8` → `stage_overwrite_files_with` → A | none | A, UTC | G | A |
| D16 | `repark-sql/src/insert_overwrite.rs:146` → stagers → A | none | A, UTC | G | A |
| D17 | `repark-sql/src/create_table.rs:624`, `:635`, `:643` | none: types copied | none | G | A |
| D18 | `repark_iceberg::append` → `conform_batches` (`append.rs:43`) → A | none | A | G | A |
| D19 | shared `execute_merge` → as D7, D8, D9 | as D7, D8, D9 (the wall kernel is registered, correction 12) | site C, site B | as D7 | as D7 |
| D20 | shared `execute_predicate_dml` → as D10 | as D10 | site B | as D10 | as D10 |
| D21 | `microbatch/run.rs:475` stream → `stage_overwrite_files_with` (`:479`) → A | **none** | **A, UTC** | **G only**: a nanosecond source refuses with an internal error | A only |

The doors that build their own store projection or text are D3, D4, D7, D8, D9, D10 (`WHERE`),
D19, D20 and D21. They are the routed doors of this step. D1, D2, D5 and D10's fork sub-path take
the rules from the analyzer on their DML plan. D6 and D17 copy source types. D14 to D18 are native
paths whose only conversion is A.

## (b) The stage, its signature, and where each door calls it

New file `crates/repark-iceberg/src/write/conform_stage.rs`. It has no comments, and its tests go
in `conform_stage/tests.rs`.

```rust
pub enum StageSite { InsertByName, InsertOverwrite, PartitionOverwrite, MergeInsert, MergeUpdate, PredicateUpdate, MicroBatchAppend }

pub struct StageRequest<'a> {
    pub site: StageSite,
    pub targets: &'a [(String, DataType)],
    pub by_name: bool,
    pub fold: bool,
    pub gate_op: Option<&'a str>,
    pub pairing: StructPairing,
    pub table: &'a str,
}

pub fn conform_stage(ctx: &SessionContext, plan: LogicalPlan, request: &StageRequest<'_>) -> Result<LogicalPlan>;
pub async fn conform_frame(ctx: &SessionContext, frame: DataFrame, request: &StageRequest<'_>) -> Result<DataFrame>;
pub fn table_targets(table: &Table, listed: &[String], reserved: &[String]) -> Result<Vec<(String, DataType)>>;
```

The single public function is `conform_stage`. `conform_frame` runs it and executes the plan.
`table_targets` replaces the target lists of `zone_stores` and `conform_types`. The stage does
this, in order, for each output column of the plan:

1. It analyzes the plan once (`execute_and_check`) and propagates the error. This closes C-007:
   the error is the one execution would raise.
2. It puts a column `Projection` on a plan whose top node is not one. This closes
   `store_overflow.rs:842`.
3. It resolves each output to its target, by name or by position. When the positional widths
   differ, it resolves by name. When an output whose type needs a rule has no target, it refuses
   with a `Plan` error that names the column. This closes C-008, `:845` and `store.rs:29-31`.
4. Gate: `judge_wrap_store_assignable` when `gate_op` is set (unchanged).
5. Pairing and overflow: `conform_one_store_expr` (unchanged). The pair kernel only re-lays the
   children, so after it the leaves keep their source types in the target layout.
6. Leaf zone rule and floor, for a top-level timestamp target. The rule is `zone_store_frame`'s
   rule: the wall kernel or an ns kernel by name through `registered_store_kernel`, and the instant
   cast for a wall, date or ns source into an instant. The ns into µs instant case names the
   narrowing kernel (Q2). A kernel that is not registered refuses. This closes the step-1
   fail-open for every rule.
7. Nested zone rule, for a target that holds a timestamp leaf. The stage computes a partial type
   over the paired source type. It replaces only two kinds of leaf with the target leaf: a wall or
   a date into an instant, and an instant into a wall. A text leaf, an integer leaf and every other
   leaf keep their source type. When the partial type differs from the source, the stage wraps
   `__repark_zone_seam__(value, <typed null of the partial type>)`, named fail-closed (Q1). The
   seam converts each changed leaf in the session zone and returns every other leaf unchanged
   (`zone_seam.rs:381`).
8. Site B, typed (Q3, lean B2). In each output expression, the stage rewrites every
   `arrow_cast(x, '<nested T>')` whose `x` has a step-7 partial type into
   `arrow_cast(__repark_zone_seam__(x, <partial>), '<T>')`. This includes the row-merge `CASE` of
   D8 and D9 (`merge_sql.rs:274`).
9. It records `request.site` in the stage record (§c).

Step 7 excludes text leaves for a measured reason. On the `array_*` targets of D3, D4 and D10,
102 cells (F6) store the string read as UTC in every zone while Spark refuses. A seam on a text
leaf would move them to another DIFFERENT value, which is H3.

The nanosecond-leaf refusal (R-015) is not in the stage. It stays at `router.rs:145` for the
Spark doors (WS2-3).

**Call sites.** Each sits at or after the door's record point (WS2-2). D3 is recorded at
`router.rs:440`, D4 at `:454`, D7, D8 and D9 in `record_merge_doors` (`nested_assign.rs:746`),
and D10 at `spark_ast.rs:433`. ZS-9 stays at `router.rs:145`, ahead of every door, so **the 976
ZS-9 cells remain REFUSED on this head**.

| door | call | replaces |
|---|---|---|
| D3 | `insert_by_name.rs:217-235`: one `conform_frame` (`InsertByName`, by name, fold, gate `append`) | `:218` `zone_stores_by_name` and `:226` `wrap_store_outputs` |
| D4 overwrite | `insert_overwrite.rs:433-444`: one `conform_frame` (`InsertOverwrite`, by position, fold, gate `INSERT OVERWRITE`) | `:433` `zone_stores` and `store::conform_types` (`store.rs` shrinks to nothing; its target loop becomes `table_targets`) |
| D4 PARTITION | `insert_overwrite.rs:226`: `conform_frame` (`PartitionOverwrite`, same flags as `:444`, Q9) | `:226` `zone_stores` |
| D7, D19 insert | `merge/insert.rs:192-217`: one `conform_stage` on `raw` (`MergeInsert`, by name, fold) | `zone_wrapping_stream_sql` (`:114-167`) and the second plan (`:203-217`) |
| D8, D9, D19 update | `merge/insert.rs:269-278` (`MergeUpdate`, no fold) | `:270` `wrap_store_outputs` |
| D10, D20 | `predicate_dml.rs:397-410` (`PredicateUpdate`, no fold) | `:402` `wrap_store_outputs` |
| D21 | `microbatch/run.rs:475`: `conform_frame` with `batch` and the sink's targets (`MicroBatchAppend`, by name, fold, gate `append`, Spark's default pairing) | nothing (D21 had no stage) |

**What stays outside the plan pass, by design (Q13).** On D8 and D9 the assignment value sits
under the row-merge `CASE`, so its leaf kernel text (`paired_assignment_cast_sql` →
`store_kernel_sql`) stays. On D10 the identity projection has no `CASE`, but its leaf text
(`lineage.rs:85-153`) stays too. That keeps its EQUAL leaf cells on the code they run today. The
plan pass sees those leaves already at their target type and leaves them alone.

**Site A** keeps its code. On a routed door every zone and unit leaf now arrives at its target
type, so `entry_store_cast` and A′ have no zone or unit left to convert. G becomes unreachable on
routed doors (pin 3). D18 keeps A as its only conversion. **WS2-5:** the guard pair of the
partitioned branch runs on all three branches. That is one `refuse_unpaired_struct` call beside
`refuse_coarser_timestamp` in `cast_one_batch_to_write_schema` (`merge/mod.rs:564`), or in the
closure at `session_staging.rs:43` (Q12).

## (c) The WS2-6 mechanism: a stage record beside `last_write_doors`

`conform_stage.rs` holds a session-config extension `StageRecord` (a `Mutex<Vec<StageSite>>`,
deduplicated like `door_tag::record`). It also holds `install_stage_record(ctx)`, which writes the
extension into the live state, `clear_stage_record(ctx)` and `last_stage_sites(ctx)`. No product
session installs it. Production records nothing and pays one extension lookup per stage call. The
D21 batch session is a snapshot of its starting state (`run.rs:407`), so it shares the `Arc` and
records into the parent's record.

**Pin 1** lives in `crates/repark-sql/tests/write_door_native.rs`. `repark-sql` already reaches
`repark-spark` (dev), `repark-core` and `repark-iceberg`, so no crate edge changes. It walks
`WriteDoor::ALL` less D18. Per door it builds a session, installs and clears the record, runs one
write, and reads both records. A routed door must show its door in `last_write_doors` (Spark
doors) and its `StageSite` in `last_stage_sites`. Every other door goes through an exhaustive
`match` with no wildcard that names its reason (Q4). A new `WriteDoor` variant then fails to
compile until someone classifies it. Mutants: remove each of the seven call sites, and the doors
of that site red.

## (d) The 1,512 store-assignment marker cells

All 1,512 have a string source. **1,497 are EQUAL**: Spark raises
`INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` too. **0 are REFUSED**: Spark never stores one
of them. **15 are DIFFERENT**: on D1 `insert_values` with a string column source, Spark raises
`SCALAR_SUBQUERY_IN_VALUES`.

| door | cells | EQUAL | REFUSED | DIFFERENT |
|---|---|---|---|---|
| D1 | 126 | 111 | 0 | 15 |
| D2 | 24 | 24 | 0 | 0 |
| D3 | 36 | 36 | 0 | 0 |
| D4 | 126 | 126 | 0 | 0 |
| D5 | 48 | 48 | 0 | 0 |
| D7 | 252 | 252 | 0 | 0 |
| D8 | 252 | 252 | 0 | 0 |
| D9 | 72 | 72 | 0 | 0 |
| D10 | 576 | 576 | 0 | 0 |
| **all** | **1,512** | **1,497** | **0** | **15** |

The D10 share is exactly C-016's `D10-STORE-CAST-AHEAD-OF-DOOR` row (576), and all of it is EQUAL.
The store-assignment refusal ahead of D10 matches Spark on every cell, so keeping it where it is
(WS2-2) costs no cell.

## (e) The cells expected to move under WS2-8

**Expected moves: 328 cells, all DIFFERENT → EQUAL, all family F5.** F5 is a date into a nested
`TIMESTAMP` outside UTC. Main stores UTC midnight (`1767312000000000`). Spark stores session
midnight (`1767330000000000` in New York, `1767292200000000` in Kolkata). Step 7's seam takes the
date leaf (`Leaf::Instant`) in the session zone, as D1, D2 and D5 already do (their F5 cells are
EQUAL).

| door | spelling (mode) | cells |
|---|---|---|
| D3 | `insert_by_name` | 16 |
| D4 | `insert_overwrite`, `df_insert_into_overwrite`, `df_overwrite_partitions` (16 each), `insert_overwrite_values` (8) | 56 |
| D7 | `merge_insert`, `merge_insert_literal`, `merge_insert_star` (cow, mor; 16 each) | 96 |
| D8 | `merge_update_whole`, `merge_update_whole_literal`, `merge_update_star` (cow, mor; 16 each) | 96 |
| D10 | `update_whole_where`, `update_col_where` (cow, mor; 16 each) | 64 |
| **all** | struct, array, map and array_struct targets × a date literal and a date column × two zones | **328** |

**Expected unchanged:** every other cell. These are named because a wrong stage would move them:

- **F6, 102 cells on D3 (12), D4 (42) and D10 (48)**: a string into `array_*`. Spark refuses and
  main stores the UTC reading. They stay unchanged only because step 7 excludes text leaves.
- **F8, 432 cells on D10**: the `update_*_nowhere` spellings on the fork sub-path. CoW raises
  `arguments need to have the same data type` and MoR raises `column types must match schema
  types`. The stage does not reach them (correction 6, Q7).
- **F8, 4 cells on D4** (`insert_overwrite_values`, map targets, ERROR). The sink's map entry
  names differ (`keys`/`values` against `key`/`value`). One of them,
  `insert_overwrite_values.append.map_ltz.wall.utc`, carries a zone leaf in UTC. The seam would
  re-type its map before the same sink error, so its error text may change (Q10).
- **F1, F2, F7**: 216, 228 and 318 cells. They are a subquery or a string; no rule of the stage
  applies.
- **The 976 ZS-9 cells**: they stay REFUSED (WS2-2).
- **The 2,400 `native_sql` cells**: they run D14 through `repark.sql()` (C-009), which no stage
  call reaches.

**WS2-9 expectation, for step 4.** The mutant run turns ZS-9 off. Of the 976 cells, the 952
REFUSED ones should end EQUAL. F3 is an instant into a nested `TIMESTAMP_NTZ`: D1 72, D2 16, D3 16,
D4 56, D5 32, D7 96, D8 96, D9 16, D10 160. F4 is a wall into a nested `TIMESTAMP`: D3 16, D4 56,
D7 96, D8 96, D10 128. On D1, D2 and D5 the analyzer's seam does it; on the routed doors steps 7
and 8 do it. The 24 DIFFERENT ones (F1: `insert_values` on D1, 8, and `insert_overwrite_values` on
D4, 16, both with a column source) cannot end EQUAL. Spark raises `SCALAR_SUBQUERY_IN_VALUES`
there, so they are a finding by design, not a stage defect.

**WS2-4** is outside the census (correction 10). Pin 4 alone proves it, and only once Q8 is ruled.

## (f) Lines deleted

| file | lines | what |
|---|---|---|
| `ntz_store.rs` (410) | 110-125, 127-181, 183-196, 198-248, 250-268 | `analyzed_types`, `zone_stores`, `zone_stores_by_name`, `zone_store_frame`, `registered_store_kernel` move into the stage, rewritten (155 lines plus their blank separators). The type helpers (21-108), `refuse_ntz_writes` (270-304) and the tests stay. |
| `store_overflow.rs` (939) | 832-903, 905-936 | `wrap_store_outputs` and `judge_wrap_store_assignable` move into the stage (105 lines with their separators; the file goes to about 834). Five helpers become `pub(crate)` in place, with no line added: `store_output_name` :190, `rewrap_aliases` :200, `PairSite` :449, `ConformStoreFlags` :455, `conform_one_store_expr` :506 (Q11). |
| `update_cast.rs` (478) | none under B2; 44-56 rewritten under B1 | Under B2, site B's text stays and the stage seams it typed. Under B1, `nested_leaf_cast_sql` takes the source and emits the seam for a nested zone pair (Q3). |
| `merge/insert.rs` (503) | 114-167, 192-217 | `zone_wrapping_stream_sql` and the double plan; one stage call in their place |
| `insert_overwrite/store.rs` (45) | 6-45 | `conform_types`; its target loop becomes `table_targets` |
| `write/mod.rs` | 130 | the re-export of `zone_stores` / `zone_stores_by_name`, replaced by the stage's |

## (g) Questions (premise, then lean)

- **Q1. Seam registration.** *Premise:* `__repark_zone_seam__` is registered by name in no
  session (correction 2), so a fail-closed lookup refuses everywhere. *Lean:* register
  `zone_seam_udf(false)` by name in `repark-functions` `registration.rs` and in the ANSI dialect's
  `on_session_built`, one line each, in files outside §2.
- **Q2. The floor into a µs instant.** *Premise:* that floor is `NarrowTimestampNs`, installed
  only by the analyzer, with no name in `udf_names` (correction 3). *Lean:* move
  `NARROW_TIMESTAMP_NS_NAME` into `repark_common::udf_names`, register the UDF beside the wall
  kernel on both session builders, and have the stage name it fail-closed. The µs wall floor stays
  on `TIMESTAMP_NTZ_CAST_NAME`.
- **Q3. The site B mechanism.** *Premise:* `nested_leaf_cast_sql(expr, target)` has no source
  type. Its caller with an unknown source (`nested_assign.rs:382`, when `probe_type` answers
  `None`) cannot tell a zone pair from a text pair, and seaming a text pair is H3 (§e, F6).
  *Lean:* B2. The site B text stays, and the stage's plan pass seams each nested `arrow_cast` with
  types from the analyzed plan, inside the row-merge `CASE` too. Pin 6 keeps its name and asserts
  that the planned projection names `__repark_zone_seam__` and that no nested `arrow_cast` converts
  a zone leaf, with other pairs unchanged. *Alternative:* B1 (the order's text). It threads the
  source through `nested_assign.rs:400`/`:402` and refuses when the source is unknown; that is a
  new refusal on a path that stores today.
- **Q4. The doors pin 1 enumerates.** *Premise:* the stage sits in a RePark write function for D3,
  D4, D7, D8, D9, D10 (`WHERE`), D19, D20 and D21 only (§a). §2 names call sites for those only.
  *Lean:* the routed doors must show a stage record. The exhaustive `match` names the others:
  `PlanConformed` (D1, D2, D5), `TypesCopied` (D6, D17), `ForkProvider` (D14), `NativeStager`
  (D15, D16). D14 to D17 then go into `doors_not_touched`.
- **Q5. Native semantics on D19 and D20.** *Premise:* they already run the Spark leaf rule (the
  wall kernel is registered, correction 12). With Q1 they also run the nested rule through the
  shared executors. D14, D15 and D16 keep the UTC reading (census N6). No census cell measures
  D15 to D20. *Lean:* D19 and D20 take the stage like D7 to D10, with no native special case in
  the shared executors. A native-door zone ruling is its own card.
- **Q6. D21 on a session without the kernels.** *Premise:* the batch session is a snapshot of the
  starter's session. A bare `repark-core` session registers no Spark kernel. Today a converting
  write there stores the UTC reading at A. *Lean:* it refuses (WS2-1) with a `Plan` error that
  names the missing kernel and the dialect that registers it. Pin 2's D21 case is that session,
  and pin 3's D21 case runs on an ANSI-dialect session in `write_door_native.rs`.
- **Q7. D10's fork sub-path.** *Premise:* the stage cannot reach it without the fork (correction
  6). Its 432 F8 cells stay ERROR. *Lean:* classify it with D1 (`PlanConformed`, a fork sink); it
  is not an H2 halt. Pin 1's D10 write uses a `WHERE`.
- **Q8. WS2-4's statement.** *Premise:* Spark's `1769922000000000` is New York midnight as an
  instant, so the stored column is a `TIMESTAMP`, or the `TIMESTAMP_NTZ` value is the argument.
  No statement is recorded. With no `WHERE` it runs on the fork sub-path (Q7), where the stage
  cannot reach. *Lean:* before slice 3, measure `UPDATE … SET ts = add_months(ntz, 1) WHERE
  id >= 0` and the no-`WHERE` form on the head and on live Spark 4.1.2. Pin the spelling that
  RePark's path runs. If only the fork form is wrong, hand back for a ruling.
- **Q9. D4's PARTITION path.** *Premise:* today it pairs only at the sink and has no overflow
  guard (correction 7). *Lean:* the stage applies its full rule set there, with the flags of
  `:444`. One Rust pin covers an integer-overflow PARTITION write, because the census has none.
- **Q10. What counts as a move under WS2-8.** *Premise:* C-005 compares whole answers. A cell can
  keep its status and change its value or error text (§e, the D4 map cell). *Lean:* a move is any
  change in the recorded answer: status, type, values, error class or first line. Any move that
  does not end EQUAL is H3, and the hand-back tables it.
- **Q11. Where `wrap_store_outputs` lives.** *Lean:* it moves into the stage, as in §f. No wrapper
  stays behind, and its five external call sites call the stage. *Alternative:* it stays in
  `store_overflow.rs` as `pub(crate)` and the stage calls it. That deletes nothing there.
- **Q12. Files outside §2 that the design needs.** *Premise:* these need edits:
  `write/mod.rs`; `insert_overwrite/store.rs`; `merge/mod.rs:564` (WS2-5, or the
  `session_staging.rs` closure); `repark-functions` `registration.rs`, `timestamp_ns_cast.rs:34`
  and `repark-sql/src/dialect.rs` (Q1, Q2); `repark-common/src/udf_names.rs` (Q2).
  *Lean:* allow these line edits. No new file is needed beyond the order's.
- **Q13. The leaf text of D8, D9 and D10.** *Premise:* D8 and D9 values sit under the row-merge
  `CASE`, so their leaf kernel must be in the assignment text. D10's has no `CASE`. *Lean:* keep
  all three leaf texts unchanged as the stage's text face, the same kernels by name. The plan pass
  leaves an already-typed leaf alone. No rule gains a site, so this is not H5.

## Doors I did not touch, and why

None were touched, because this round builds nothing. The build plan leaves out: D18 (the
order's control); D1, D2, D5 and D10's fork sub-path (the analyzer conforms the DML plan, Q4,
Q7); D6 and D17 (types copied); D14 (fork provider on the bare session); D15 and D16 (native
stagers, Q5).
