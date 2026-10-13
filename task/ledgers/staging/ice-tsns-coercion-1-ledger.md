# Unit ledger — ICE-TSNS-COERCION-1 · slice 0 measures the twin oracle and the Spark lattice

**Scope (2026-10-12):** measurement only, on main `572130dda35f739795578134348f99fd7b530cfd`.
No product file changes. The twin is the same statement with `__repark_cast_timestamp_ns__`
or `__repark_cast_timestamptz_ns__` written by hand. Spark 4.1.2 is the second oracle:
`timestamp_ns` stands where `TIMESTAMP_NTZ` stands, and `timestamptz_ns` where `TIMESTAMP` stands.

**Date:** 2026-10-12 · **Branch:** `test/ice-tsns-coercion-1-slice-0` · **Base:** `572130dd` ·
**Model:** recorders drafted by Grok 4.7, completed and recorded by Muse Spark
(muse-spark-1.3-contributor) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Order:** the owner's ruling of 2026-10-10 on parity row R-017, card
[ice-tsns-coercion-1-card-2026-10-10.md](../../roadmap/mid-term/ice-tsns-coercion-1-card-2026-10-10.md).
**This ledger closes when the unit's pull request merges.** Slice 0 does not merge that change.

## PROPOSITION LEDGER — ICE-TSNS-COERCION-1 — 2026-10-12

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The twin of every card spelling is recorded from main 572130dd on every door, five zones. | The twin fixture and its recorder. | **PROVEN** | `python/repark/tests/ice_tsns_coercion_1_twins.json`: 107,520 twin, 107,520 statement and 1,920 plain stored cells, 1,120 reads, 1,680 range cells, 5 zones; recorder `_record_ice_tsns_coercion_1_twins.py`; summary `task/wo/ice-tsns-coercion-1/evidence/twins-summary.md`. |
| C-002 | Spark 4.1.2's lattice for the temporal set is recorded in both ANSI modes. | The lattice JSON and its recorder. | **PROVEN** | `python/repark/tests/ice_tsns_coercion_1_spark_lattice.json`: 1,540 cells, Spark 4.1.2 read from the live session, both ANSI modes, 5 zones, 0 errors; recorder `_record_ice_tsns_coercion_1_spark_lattice.py`. |
| C-003 | Every twin's kind and zone placement matches Spark's analogue. | The comparison table. | **PROVEN** | Comparison table in `twins-summary.md`: kind 560/560, placement 560/560 over `ns` and aligned `tzns` reads; fixture `python/repark/tests/ice_tsns_coercion_1_twins.json`. The 2026-10-12 re-record aligned the `tzns` seed to Spark's `src_ltz` instant per zone and kept the zone-fixed seed as `tzns_utc`. |
| C-004 | The gate-open rate, the cost baseline and the four censuses are recorded. | Section 9 of the slice brief. | **PROVEN** | `task/wo/ice-tsns-coercion-1/evidence/`: `gate.md` (rate uncountable from public `EXPLAIN`; hook named), `cost-baseline.json` + `cost-baseline.md`, `pins.md`, `casts.json` + `written-casts.md`, `try-repark.json` + `try-spark.json` + `try-cast.md`, `lambda.json` + `lambda.md`. |
| C-005 | Before the first type_coercion, every card site that holds a nanosecond value is wrapped in the existing cast kernels. | Slice 1. | **OPEN** | Not this slice. |
| C-006 | `nvl` and `ifnull` over the temporal set become `coalesce`, then those kernels apply. | Slice 3. | **OPEN** | Not this slice. SQL `nvl` on main still has no timestamp signature. |
| C-007 | A written `CAST`, `::timestamp`, `to_timestamp` or `TIMESTAMP` literal is unchanged by the card. | Slice 2. | **OPEN** | Not this slice. |

## 1. What step A records (C-001, C-002)

Sources are `timestamp_ns` and `timestamptz_ns`, each holding `2026-01-02 03:04:05.123456789`
and `1969-12-31 23:59:59.999999999`. Targets are `timestamp_ns`, `timestamptz_ns`, and a
microsecond `TIMESTAMP` control. Spellings are `coalesce`, `array(...)[0]`, `CASE`, `if`,
`nvl`, `ifnull`, and `UNION ALL`. Siblings are an untyped NULL, `CAST(NULL AS TIMESTAMP)`,
`CAST(NULL AS TIMESTAMP_NTZ)`, a microsecond column and literal of each kind, and `DATE`.
Doors are the 64 R-017 doors, including MERGE, UPDATE, table-creating doors and the cached
frame. Zones are UTC, America/New_York, Asia/Kolkata, Australia/Lord_Howe and `-03:30`.

Stored cells are read from the Parquet files as int64 ticks. Reads, including field metadata,
are `toArrow()`. The `UNION ALL` write doors use `max(v) GROUP BY id` because a
`ScalarSubquery` does not plan. `INSERT VALUES` of a column sibling is skipped: `VALUES`
has no column scope. `nvl` and `ifnull` twins are `coalesce` with the kernels; the statement
role keeps SQL `nvl` and `ifnull`.

The Spark lattice is five zones, both ANSI modes, the same spellings, and the three range
siblings. The summary `twins-summary.md` lands in step A, written by
`_summarize_ice_tsns_coercion_1.py`.

## 2. Kind and placement (C-003)

Step A recorded 2026-10-12 by Muse Spark (muse-spark-1.3-contributor). Kind
matches Spark on all 560 card-spelling twin reads. Placement matches on 336:
every naive read and every `ns`-source zoned read (the kernel places the naive
wall in the session zone exactly as Spark places `TIMESTAMP_NTZ`). The 224
`tzns` non-UTC reads differ by the zone offset: the seeds differ by
construction and the twin passes `tzns` through, so those cells carry no
coercion signal either way. Two comparison fixes landed with the summary: the
unit match reads `[ns`/`[us` prefixes so zoned types compare, and epoch
microseconds floor like `unix_micros`.

The 35 untyped-NULL stored-value mismatches on nanosecond targets are 15
`union` cells through MERGE doors where statement and twin error identically
(oracle consistent) and 20 `array` cells on the DataFrame-functions door where
the twin text fails inside `functions.expr` while the same text plans through
`spark.sql` and `selectExpr`. Slice 1 wraps programmatically with the kernel's
declared return type, so neither class moves the design. The sketch's 96 hold:
192 cells over `coalesce`/`array`/`CASE`/`if`, both sources, all three targets,
`insert_select`/`insert_values`/`cached_frame_append`/`ctas`, UTC and New York,
zero differ.

## 3. Product clauses left open (C-005, C-006, C-007)

Nothing in `crates/*/src/` or `python/repark/src/` changes in this slice. The gate, the
lattice rewrite, and the temporal `nvl` rewrite are later slices.

## 4. Step B (C-004)

Recorded 2026-10-12 by Muse Spark. Item 1: the rate is uncountable — no
`EXPLAIN` text, plain or `VERBOSE`, prints leaf field types — so `gate.md`
names the missing hook (analyzed leaf schemas, or one analyzer counter on
`Timestamp(Nanosecond, _)`) and counts 19 pandas `datetime64[ns]` source
lines. Item 2: the baseline ran with the cargo lock free and no build up
(load decaying 5.7 to 5.6); set medians agree to <2% except `merge_mor`.
Items 3 to 6 re-ran byte-identical except `lambda.json`, whose physical plan
carries the machine's `RoundRobinBatch` fanout (26 here, 8 before); the
logical plan and the markdown match.

Ruling of 2026-10-12 on Q1 (C-004): the rate is not counted in slice 0.
Slice 1's first product change is one analyzer counter in the session
metrics facade, incremented when a leaf relation has a
`Timestamp(Nanosecond, _)` field at any depth; the rate is measured with it
before any site is wrapped.

## 5. Q2 re-record (C-003)

Ruling of 2026-10-12 on Q2: Spark as the second oracle stands, so the seeds
were aligned instead of ruled out. The `tzns` source now holds the wall
interpreted in the session zone — the same instant as Spark's `src_ltz` in
each zone — and the zone-fixed wall+00:00 seed stays as `tzns_utc` behind
the untouched stored matrix. Only the reads were re-recorded: 560 new
`tzns` reads and 420 range cells merged beside the renamed evidence. The
fresh `tzns_utc` and `ns` reads reproduce the renamed cells exactly except
the `PARQUET:field_id` shift from the added column, which the retained
evidence keeps as recorded. Kind 560/560, placement 560/560; no twin was
adjusted. C-003 PROVEN.

## 6. Slice 1, round 1 (2026-10-12)
Built: the Spark door's first-`type_coercion` wrapper seat, the gate walk and the two counters; nothing is rewritten.
Pins: `closed_gate_hands_the_inner_rule_the_plan_it_was_given`, `analyzer_configuration_keeps_the_rule_names_and_count_through_the_wrapper`, `configured_first_coercion_counts_open_analyses_and_ignores_closed_ones`, the 10-case gate truth table, `test_pandas_datetime64_ns_frame_opens_the_gate`, `test_twelve_statements_equal_the_recorded_statement_role`, … (`timestamp_ns_coercion/tests.rs`, `extension/tests.rs`, `test_ice_tsns_coercion_1_slice1.py`).
Gate-open rate: 12670 opens over 16657 items (460 items opened, 2.8%); 3 opens from one pandas-only item.
Cost: interleaved head/baseline 0.9776 insert, 0.9848 merge, 1.0080 merge_mor; the first run's over-cap ratios were box noise.
Next: round 2, untyped NULL sites.

## 7. Slice 1, round 2 (2026-10-12)
Built: the all-or-nothing fallback check, the untyped-NULL site walk and the kernel rewrite on the open-gate path; the fallback sink logs at the seat via `tracing::debug!`; the fallback cast check matches a top-level `Timestamp` target only.
Pins: 43 Rust pins (coalesce/array/case/if/union ns+tzns output types, idempotence, fallback triggers, leave-alone sites, never-sites, sink lines, container-cast) plus the seat sink pin; facade: 8,967 remaining cells equal the recorded twin, 258 DML-store-cast cells stay main's with the fallback moved, 220 equal the corrected-twin oracle (all oracle 1, recorded on the baseline module), 155 equal main with the stale `tzns` text replaced, 45 reads equal the twin, the union `tzns_utc` read pins field id 4, 40 plain ns-target cells never fall back.
Split: 8967 + 258 + 220 + 155 = 9,600; the union read pins field id 4 (live main reads 4, the recorded twin's 3 is stale); the DML store cast stays main's until slice 2 tells it from a written cast.
Cost: interleaved head/baseline 0.9928 insert, 0.9823 merge, 0.9939 merge_mor; largest pair ratio 1.0061.
Registry: `docs/spark-sql-iceberg-parity.md` row `ICE-TSNS-SQL-1-R-017` still states the UPDATE-no-WHERE refusal; on this head all eight spellings store their twin's walls, so the row is stale until the close-out slice rewords it.
Next: slice 2, honest typing.

## 8. Slice 1, fold 1 (2026-10-12)
Built: the per-node `TypeCoercionRewriter` ask before any NULL wrap (union columns ask
`coerce_union`), and the retry on the original plan with one fallback when the inner
rule rejects a rewrite. No allow-list or deny-list of function names.
Pins: 11 new Rust pins (each twelve-shape class matches main's rule alone, the retry,
four pins aimed at the surviving mutants, of which only the union pin killed its mutant;
corrected 2026-10-13, see section 9) with the 43 round-2 pins unmoved; 152 facade cells (15 SQL
plus 4 DataFrame shapes, ns/tzns, UTC/America-New-York, ANSI on/off) equal the
baseline-recorded oracle.
What was wrong: every function argument counted as a site operand, so a NULL in a
non-timestamp position was wrapped whenever another argument was one nanosecond type;
twelve shapes errored or changed answer. Fixed by construction, not a function list.
Corrections: the union read's field id is 4 on live main in all five zones (the
recorded twin's 3 is stale); 70 of the 258 carve-out cells target `tz_ns`, 188 `ltz`;
written `TIMESTAMP_NTZ` and nanosecond casts reach the wrapper as kernel calls and
never trip the fallback, which sees only surviving `Cast`/`TryCast` nodes.
Open: the fallback counter over-counts analyses the wrapper runs twice (verdict
finding 3; due before slice 2); whether Spark can read a table created from a
rewritten read (verdict Q2; the owner is deciding).
Cost: interleaved head/baseline 1.0067 insert, 1.0063 merge, 0.9929 merge_mor;
largest pair ratio 1.0231.
Next: slice 2, honest typing.

## 9. Slice 1, fold 2 (2026-10-13)
Finding: the second verifier's G-1 — a rewritten site keeps nanoseconds where main's
coercion narrowed it to microseconds, and 117 probe cells main answers were refused at
execution (95 `sequence`, 22 pandas/Arrow door), past the wrapper's retry.
Rulings (orchestrator, provisional, 2026-10-13): (1) it parks as written and the fix is at
the consumers, with no function list, no consumer sniffing and no un-wrapping in the
wrapper; (2) `sequence` builds its list in the unit and zone of its input; (3) the Python
output doors floor a same-kind nanosecond column to the declared microseconds at their one
shared check; (4) `createDataFrame(pandas, schema)` is left to ICE-TSNS-FRAME-DOOR-1;
(5) m4, m5, m7 are re-pinned and the aggregate pin gets a nanosecond first argument;
(6) nested-site and NULL-branch-UNION cost is accepted for this fold, measured, not gated;
(7) the fallback counter's over-count stays open.
Built: `spark_sequence.rs` brings both bounds to the declared element's unit and
`rows::timestamp_ticks` walks per unit, so `TIMESTAMP_NTZ`, nanosecond, millisecond and
second inputs answer; `udf_schema._validate_map_in_arrow_batch` returns the batch and
floors a nanosecond timestamp (any list depth, same zone label) to the declared
microseconds (first with `floor_temporal` and a safe cast; replaced in follow-up 1 below
by an integer floor of the ticks); `applyInPandas` routes its one lossy
`timestamp[ns] → timestamp[us…]` conversion to that check instead of refusing first.
Stored for a sub-microsecond start: `sequence` keeps the digits, every element is
`start + i × step` (`…05.123456789` by one second gives `…06.123456789`), the step stays
microsecond-exact, and a stop that is short of an element by one nanosecond drops it.
Pins, Rust (`spark_sequence/timestamp_tests.rs`):
`microsecond_forms_keep_their_zone_and_match_spark`,
`nanosecond_forms_equal_the_microsecond_twin`,
`nanosecond_start_keeps_its_sub_microsecond_digits`,
`nanosecond_gap_under_a_microsecond_keeps_the_step_sign_rule`,
`mixed_units_follow_the_first_argument`, `second_and_millisecond_inputs_keep_their_unit`,
`scalar_nanosecond_bounds_build_a_nanosecond_list`,
`null_bound_answers_a_null_list_in_the_input_unit`; the six older `sequence` pins and the
18 facade `sequence` tests keep their answers.
Pins, facade (`test_ice_tsns_coercion_1_slice1_fold2.py`):
`test_cells_main_answered_still_answer`, `test_nanosecond_cells_equal_the_microsecond_twin`,
`test_ticks_differ_from_main_only_where_main_narrowed_the_site`,
`test_microsecond_sequence_forms_equal_live_spark` (48 cells, Spark 4.1.2, NTZ and LTZ,
UTC and America/New_York, all equal),
`test_map_in_arrow_floors_nanoseconds_to_the_declared_microseconds`,
`test_map_in_arrow_floors_nanoseconds_inside_lists`,
`test_map_in_arrow_refuses_a_nanosecond_column_of_another_kind`,
`test_pandas_doors_floor_nanoseconds_to_the_declared_microseconds`,
`test_apply_in_pandas_refuses_a_naive_nanosecond_column_under_a_zoned_type`.
Pins, mutants (`timestamp_ns_coercion/tests.rs`): m4, m5, m7 and the ask-skipped mutant
each red on the mutant and green after the restore. Section 8's "four surviving-mutant
pins" was wrong: of those four only the union pin (m8) killed its mutant. The ruled m4
form `coalesce(ns, µs, NULL)` cannot kill m4 — DataFusion's common type of `ns` and `µs`
is `µs`, so the ask never answers the site target — and the pin adds a call with a fixed
two-nanosecond-timestamp signature, which does.
Cells moved: 113 of 117 (95 of 95 `sequence`, 18 of 22 door), re-measured with the
verifier's probes on head and main; none that fold 1 answered is refused; 43 cells main
refuses answer now (plain nanosecond and `TIMESTAMP_NTZ` `sequence` forms, `mapInArrow`
over a plain nanosecond column with its own schema).
Value oracle: main's ticks are not it. Of the 68 facade cells 65 answer; all 68 equal the
microsecond twin once floored; 39 equal main's ticks and 26 do not, exactly where main's
own narrowing set the value (15: the naive `ns` site read as a wall in America/New_York;
11: `-1 ns` cut toward zero to `0` in UTC on main's DataFrame-door site and Python doors).
Still refused, 4 cells: 2 `createDataFrame(pandas, schema)` over `ns` — closed by
ICE-TSNS-FRAME-DOOR-1; re-measure after it merges; and the 2 cells under the next heading.
Changed text: a naive nanosecond pandas column with sub-microsecond digits under a declared
zoned `timestamp` in `applyInPandas` was refused with `failed converting pandas output …
would lose data` and is now refused with the shared `mapInArrow schema mismatch` text.
Cost: eight interleaved sets, head over main, same build profile: 0.9946 insert, 1.0005
merge, 1.0089 merge_mor (median pair ratio; one merge_mor pair of eight read 1.0752, one
0.9649). Ungated, ruling 6: 1.85 on an eight-deep nested `coalesce(…, NULL)` and 1.13 on a
200-branch NULL-branch UNION; accepted for this fold, no optimisation.
Open: the fallback counter's over-count on re-analysis (due before slice 2); the owner's
answer on the two cells of section 9a; whether Spark can read a table created
from a rewritten read (the owner is deciding); calendar steps of a zoned `sequence` run on
the UTC wall, as before this fold (equal to Spark in the 48 recorded cells, none of which
crosses a DST change).
Evidence: [cells](../../wo/ice-tsns-coercion-1/evidence/cells-fold2.md),
[mutants](../../wo/ice-tsns-coercion-1/evidence/mutants-fold2.md),
[cost](../../wo/ice-tsns-coercion-1/evidence/cost-slice1-fold2.md).
Next: slice 2, honest typing.

### 9a. Main answered, head refuses, owner to accept or reject (2 cells)
A named change, left refused by ruling (orchestrator, provisional, 2026-10-13, on Q2).
Statement (DataFrame door, source `ns`, the verifier's `m_udf` case
`udf_map_in_arrow_ddl_ltz`, configurations America/New_York ANSI on and UTC ANSI off):
`T.select('id', F.coalesce(F.col('ns'), F.lit(None)).alias('v')).mapInArrow(lambda it: it, 'id int, v timestamp')`
with `T = spark.table('ice.ns.src')`.
Main's answer, the same in both zones: `struct<id:int,v:timestamp>`, Arrow
`timestamp[us, tz=UTC]`, ticks `1767323045123456` and `0` — the wall `2026-01-02
03:04:05.123456` stored as that UTC instant whatever the session zone, and `-1 ns` cut
toward zero.
Head's text: `mapInArrow schema mismatch on field 'v': expected type timestamp[us, tz=UTC]
(timestamp), got timestamp[ns]`.
Why: main typed the naive site as a zoned microsecond column, a wall as an instant. On
head the site is a naive nanosecond column and the declared type is zoned; the door floors
a unit, never a kind. The explicit twin on main (the kernel written in place of the NULL)
refuses with the same text as head, and a naive microsecond column under a declared
`timestamp` is refused the same way on both. The `tzns` half of the same case answers.
Pinned: `KIND_REFUSED` in `test_cells_main_answered_still_answer` and the first row of
`test_map_in_arrow_refuses_a_nanosecond_column_of_another_kind`.

### 9b. Follow-up 1 (2026-10-13)
Rulings (orchestrator, provisional, 2026-10-13): Q1 accepted — the facade file's relation
to main is (a) answered where main answers, (b) equal to the microsecond twin, (c) the
26-cell split by one rule per group, as stated under "Value oracle" above; Q2 leave
refused as the named change of 9a; Q3 keep the `applyInPandas` route, side effect recorded
under "Changed text" above; Q4 accepted (the fixed-signature m4 form).
Correction (orchestrator): ruling 3 named `floor_temporal`, and that was wrong. On a zoned
column the kernel goes through local time: it raises `Local time is ambiguous` for an
instant in a fall-back hour, fails on the lowest ticks of the int64 range, and is slow.
Measured here on the fold's first form: the door's declared `timestamp` carries the label
`UTC`, so no door statement reached an ambiguous hour, but the lowest int64 tick did fail
through the door (`would lose data: 9223372036854775616` — the kernel had wrapped), and the
shared check called with an America/New_York label raised the ambiguity error.
Built: the floor in `udf_schema._floor_to_declared_microseconds` is integer arithmetic on
the int64 ticks — truncating divide by 1000, minus one where the remainder is negative —
viewed as the declared type with its zone label; whole-column kernels, nulls stay null,
any list depth as before.
Pins: `test_shared_check_floors_integer_ticks_and_keeps_the_zone_label` (naive, UTC and
America/New_York labels; fall-back `2025-11-02 06:10:00.123456789Z`, spring-forward
`2025-03-09 07:00:00.000000789Z`, lowest and highest int64 ticks, a list layer; ticks and
returned type) and `test_doors_floor_daylight_saving_and_range_edge_instants` (the same
instants through `mapInArrow`, `mapInPandas`, `applyInPandas` in an America/New_York
session, naive and zoned; the lowest tick through `mapInArrow` only, because pandas reads
that tick as `NaT`). Both red on the first form, green now; `-1 ns → -1 µs` stays pinned.
Mutants: truncating divide in place of the floor fails 7 of the file's 11 tests; zone label
dropped fails 5; restored, 11 pass.
Cost of the floor per million rows (`_floor_to_declared_microseconds`, median of 15, one
June 2025 month of random ticks): naive 15.2 ms before and 15.3 after; America/New_York
111.1 ms before and 15.3 after. No gate.
Re-measure after the change (the verifier's probes, head against main): unchanged — 113
of 117 answer, the same 4 refused, none newly refused, 43 that main refuses answer.
One helper later: the frame door (ICE-TSNS-FRAME-DOOR-1, PR #1045, unmerged) holds the
same floor in `create_dataframe_arrow.py`; the two become one helper in the first unit that
follows both merges. Nothing was merged or copied from that branch.

## 10. Slice 1, narrow fold (2026-10-13)
Rule: the rewrite stays only where the value's single reader is a nanosecond store
column; every read-only statement is byte-identical to main except the one named class
below. Commits, in order: `7c77237` item 1 revert (fold-2 product files back to main's
bytes, gate `git diff --quiet origin/main -- spark_sequence* python/repark/src` holds at
head); `57a114a` item 2 red pins (14 Rust `feed_tests.rs`, 6 facade narrow pins, shown red
then green); `ca5b5fe3` item 3a anchor (b) `Dml` feeds, the wrapper's new order, the UNION /
`VALUES` / aggregate / window deletions; `e79cb70e` item 3b anchor (a) kernel feeds plus the
written-cast read exception; `be63305` item 4 re-home of the 54 `tests.rs` pins through
`dml_feeds.rs` (`tests.rs` 972 lines after the split, no ceiling raised); `93cf2d3` item 5
census and sweep; `4fb3e2c` item 5 replay scripts and reports; `fae8367f` item 5
feed-anchored mutant killers and two stale-pin re-homes.
Built: `timestamp_ns_coercion_feeds.rs` (anchor (b): `Dml(Insert|Update)` nanosecond-target
positions, peel `Alias` plus one store-kernel, raw planner `Cast` never an anchor; `descend`
crosses only `SubqueryAlias`, bare-column read-once `Projection`, non-reading
`Filter`/`Sort`/`Limit`; root-only scalar/`CASE` rewrite; a subtree site refuses once) and
anchor (a) (a kernel's single argument starts the same walk from its host
`Projection`/`Filter`; other hosts unvisited, so `UNION` stays main's). Wrapper order: gate
check, feeds, then `Unchanged` hands the inner plan over untouched with no counters,
`Refused` counts gate plus fallback, `Rewritten` counts gate and falls back only on a
surviving trigger or a rejected inner analysis.
Census: 9,600 cells, 8,320 main / 1,280 kept / 0 neither, frozen in
`python/repark/tests/ice_tsns_coercion_1_narrow_census.json` written by
`_record_ice_tsns_coercion_1_narrow_census.py` on this head; ambiguous main==twin cells
count main-first. Kept cells are the `Dml`-anchor feeds (`insert_select`
coalesce/`CASE`/`if` into nanosecond columns) and the kernel-anchor feeds (MERGE and
`UPDATE … WHERE` doors); frames, `OVERWRITE`, `BY NAME`, creating doors, `array`, `UNION`,
joins and aggregates are main's.
Sweep: `_record_ice_tsns_coercion_1_narrow_sweep.py` runs every catalog function over an
ns site on two modules into `ice_tsns_coercion_1_narrow_sweep.json`: 872 cells, nothing
refuses where main answers.
Replay: `_replay_ice_tsns_coercion_1_narrow_reads.py` replays the 117 probe cells where
fold 1 errored and main answered against main's recorded outs: 117 ran, 0 differ, fallback
untouched on every read, `m_udf` setup errors identical to main's (steps 2–5, the
`CREATE FUNCTION` parser refusal predates the fold). `_replay_…_narrow_cdoors.py` replays
the 46 consumer-door attribution cells in UTC and America/New_York: 92 ran, 0 differ; rows
compare as multisets because the door returns the two struct rows in either order on the
same head (shown over three runs; the comparison still rejects a mutated row).
Mutants, each red then restored (diffs reproduced from the recorded forms where they
exist): n1 every projection item a feed kills 16 `tests.rs` pins; n2 crossing `Aggregate`
kills `every_other_plan_node_stops_the_walk`; n3 dropped read-once count kills
`column_read_twice_stops_the_walk`; n4 dropped Filter/Sort read checks kills
`filter_or_sort_that_reads_the_column_stops_the_walk`; n5 dropped step 4 kills
`site_left_inside_a_rewritten_root_restores_the_feed`; n6 microsecond target kills
`microsecond_target_position_is_not_an_anchor`; n7 member descent kills the step-4 pin;
n8 any call an anchor kills `dml_position_feed_is_rewritten` among others; n9 fallback with
no refused feed kills the two counter pins. m7 in recorded form kills
`null_case_when_condition_is_not_a_site` (8 pins red). m4 and m5 in recorded form do NOT
die on their re-homed bare-read pins: a bare projection never reaches operand
classification in the narrow fold (no `Dml` anchor, no kernel), so the pins are structurally
insensitive — the E2 "re-run" premise is false for those two. Two feed-anchored killer
pins added in `feed_tests.rs` (`microsecond_sibling_calls_at_a_feed_are_left_to_main`,
`mixed_ns_and_tzns_siblings_at_a_feed_are_left_to_main`): m4 dies on the first, m5 on the
second, each exactly one failure, 70 green after restore. m3 and m6 are named in sketch E2
but defined in no record (probe mutant table, fold-2 evidence, this ledger, all briefs name
only n1, n2, n3, m4, m5, m7, m8): open question, see §Q.
Stale unit-own pins re-homed to ruled semantics: the fold-1 extension gate-count pin
expected gate+1 on an open scan, contradicting the ruled order and the E1 pin — renamed to
`configured_first_coercion_counts_nothing_without_a_rewritten_or_refused_feed` with 0/0;
the nested-shapes no-`WHERE` update pin expected walls for `array({c}, NULL)[0]`, but main
refuses that statement with the narrowed-store text (verified byte-identical on the main
module for both `c` and `z`) — the spelling left the loop for a dedicated refusal pin
`an_update_with_an_array_index_site_refuses_with_mains_text`, array-index stays main's per
R5.
Item 3b revert proof: `git revert --no-commit e79cb70e` applies clean at head (maps
auto-merge); `timestamp_ns_coercion_feeds.rs` is byte-identical to 3a; `cargo test -p
repark-functions --lib timestamp_ns_coercion` gives 69 pass with only
`kernel_argument_feed_is_rewritten` red; `cargo test -p repark-spark --lib` gives 2705
pass, 0 fail; `git revert --abort` restores the head. The owner can switch anchor (a) off
with one revert.
Cost: `task/wo/ice-tsns-coercion-1/interleaved_cost.py`, five sets of 15 repeats,
alternating head-first/main-first, same release profile both sides, build lock checked free
before each set. Median pair ratios: 1.0022 insert, 0.9952 merge, 0.9729 merge_mor, 0.9882
nested-eight read, 0.9999 200-branch `UNION` read; gate max 1.0022, at or under 1.05 on all
five. Gate counters stayed [0, 0] across every set. Evidence:
`task/wo/ice-tsns-coercion-1/evidence/interleaved-sets-narrow.json` (10 raw sets) and
`interleaved-cost-narrow.json` (summary).
Named read exception (H1 on): a site that is the direct operand of a WRITTEN cast to a
nanosecond type (`SELECT CAST(coalesce(ns, NULL) AS timestamp_ns)`) keeps its digits and
equals the twin, type unchanged — pinned in `test_ice_tsns_coercion_1_narrow.py`, revertible
with item 3b alone per the proof above.
Disclosure: E3 row under `ICE-TSNS-SQL-1-R-017` in `docs/spark-sql-iceberg-parity.md`.
Open: the walk's growth list (R5/G: `UNION` NULL branch and crossable node, `VALUES` rows,
`Join`, pass-through `Aggregate`, first-value `Window`, member descent replacing the step-4
restore, the reader on feed operands, the feed-root lattice, `array(…)[0]`); the m3/m6
ruling; slice 2 honest typing. Next: the single verification.

## Q. Questions

No ruling is requested by this ledger until the summary names a kind mismatch or an
untyped-NULL stored-value mismatch on a nanosecond target.

Update (2026-10-12, step A): the tripwire fired — the summary names zero kind
mismatches and 35 untyped-NULL stored-value mismatches — and measurement
disposes it without a ruling; see §2. No ruling requested. C-003 stays OPEN
with its seed question above.

Update (2026-10-12, Q2 re-record): C-003 PROVEN, see §5. No ruling requested.

Update (2026-10-13, narrow fold §10): one ruling requested — sketch E2 names mutants m3
and m6 of the earlier rounds, but no record defines them (the probe mutant table,
`mutants-fold2.md`, this ledger and all briefs name only n1, n2, n3, m4, m5, m7 and m8).
Lean: the defined sets all die on a Rust target (see §10), so treat m3/m6 as a sketch typo
unless ruled otherwise.
