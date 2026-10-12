# Unit ledger — ICE-TSNS-NARROW-REFUSE-1 · a narrowing the analyzer inserted refuses by name

**Date:** 2026-10-10 · **Branch:** `fix/ice-tsns-narrow-refuse-1` · **Base:** `8d1c4f49`
(the head of ICE-TSTZNS-WALL-1, itself on main `9b230aed`) ·
**Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** Target: v1.5.5.

**Order:** the owner's ruling of 2026-10-10 on parity row
[ICE-TSNS-SQL-1-R-017](../../../docs/spark-sql-iceberg-parity.md), filed as card
[ice-tsns-narrow-refuse-1-card-2026-10-10.md](../../roadmap/mid-term/ice-tsns-narrow-refuse-1-card-2026-10-10.md).
**This ledger closes when the unit's pull request merges.**

**The owner rule (verbatim, binding).**

> narrowing the user wrote stores, narrowing the analyzer inserted refuses. A CAST, TRY_CAST or
> date_trunc written in the statement is deliberate and stores. A microsecond type that only exists
> because type coercion widened a NULL branch (coalesce, nvl, array, CASE ELSE NULL, if) is
> accidental and refuses by name.

**Fold 1 (2026-10-10).** The owner ruled on the six questions of this ledger on 2026-10-10.
Section 11 records each ruling with its date, what it changed and the measurements of the
changed source; clauses C-014 to C-018 carry the rulings and C-019 to C-027 are the fold's.
Sections 2 to 10 describe the unit before the rulings (head `91f4e2f4`); where section 11
differs, section 11 holds.

**Fold 2 (2026-10-11).** The verifier failed head `45260e44` (9 S1, 5 S2, 3 S3). Section 12 is
the redesign of the guard's rule, written before its code, and the fold's measurements; where
it differs from sections 2 to 11, section 12 holds.

**Fold 3 (2026-10-11 and 12).** The verifier's second verdict failed head `9cee945a` (6 S1,
4 S2): nothing narrowed by the analyzer was found stored, but statements main stored right
refused, reads changed and CI was red. Section 13 is the last fold: it removes that, takes
"what main does" wherever a finding could also be closed by a smarter rule, and lists in
§13.9 every shape left as on main. Clauses C-039 to C-051 are the fold's; where section 13
differs from sections 2 to 12, section 13 holds.

## PROPOSITION LEDGER — ICE-TSNS-NARROW-REFUSE-1 — 2026-10-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The base `8d1c4f49` is measured before any product change, five zones, values read from the Parquet files, and the 650-cell core of parity row R-017 reproduces: 360 cut, 120 cut with the wall moved, 40 refused, 130 errors. | §1, the recorder and the committed fixture; `test_the_base_fixture_holds_the_counts_the_parity_row_records`. | **PROVEN** | §1: recorded at commit `e5950aad`, extended on the same build of the base (§1.2). |
| C-002 | Where the written and the inserted narrowing can be told apart is decided, with its alternatives, before the product code. | §2, committed as `5ec09dc0` before the first product commit. | **PROVEN** | §2. |
| C-003 | An untyped NULL beside a nanosecond branch of a function call, a `CASE`, a lambda body or a `UNION` is tagged before the first coercion; the session rule turns the tag into the mark only when a sibling is still a nanosecond value, and always removes the tag; a NULL the statement typed and a branch the statement narrowed are not marked. | The unit pins of `null_narrowing/tests.rs`; the kept and written spellings on every door (§5). | **PROVEN** | §3.1; mutants M1 to M9. |
| C-004 | Every measured write door refuses, by name, a value narrowed beside an untyped NULL into a top-level `timestamp_ns` or `timestamptz_ns` column, and stores nothing: 64 doors, five zones, both sources, fifteen spellings. The refusal is raised by its own statement and no Parquet file under the table holds a value. | The Rust door file (39 doors, five zones, files counted and the table read back); the facade file (64 doors, held against the base fixture); the matrix (§4). | **PROVEN** | §4: 16,320 family cells refuse that stored a cut value or raised another error on the base; 0 refused cells leave a value on disk. |
| C-005 | Never worse on a nanosecond target: no cell that stores what INSERT stores for the plain column moves; no cell of a written `CAST`, `TRY_CAST`, `date_trunc`, a typed NULL or a kept type moves, but a nanosecond value in a `UNION` with an untyped NULL branch, which is the unit's purpose. | The matrix, head against base, by class (§5); `test_a_written_narrowing_and_a_kept_type_answer_what_the_base_answers`. | **PROVEN** | §5: 27,084 right-value cells, 0 moved; 12,800 written cells, 40 moved (a `TRY_CAST`, which keeps nanoseconds, in a `UNION` with an untyped NULL); 8,960 kept cells, 280 moved (the same `UNION`). |
| C-006 | A `TIMESTAMP` or `TIMESTAMP_NTZ` target does not move: stored values and error text. | The matrix (§5); `test_a_microsecond_target_does_not_move`; `a_microsecond_target_takes_a_narrowed_value_as_before`. | **PROVEN** | §5: 42,000 cells of the 60 doors that write the target, 0 moved. |
| C-007 | The refusal carries the text the unfiltered `UPDATE` carried, with the target's type in it, and the class and SQLSTATE Spark 4.1.2 raises for an unsafe store cast. | The exact-text unit pin of `narrowed_store/tests.rs`; the Spark measurement (§6). | **PROVEN** | §6. |
| C-008 | The store guard is one function over a paired supply, called where a door already pairs its supply with its target columns; each call site is named with its doors and has a mutant that reds a door pin. The lineage walk follows a value-carrying position only and refuses only when the narrowing cast sits beside the marked NULL. | §3.2 and §3.3; the unit pins of `narrowed_store/tests.rs`; mutants M10 to M31. | **PROVEN** | §3.2, §8. |
| C-009 | The card's recorded disagreement is resolved under the owner rule: the three written casts on the unfiltered `UPDATE` store what they stored on the base, on every door. | The measured cells (§7); the written spellings of the facade and Rust pins. | **PROVEN** | §7; question Q1. |
| C-010 | Each pin is shown red by a hand mutant of the rule it pins. | §8. | **PROVEN** | §8: 31 mutants, each killed by a named pin. |
| C-011 | The check adds no plan pass to a statement that touches no nanosecond column, and INSERT and MERGE over such a table take the time they took on the base. | The code (§3.1: two hooks in passes that already run, one store walk gated on a nanosecond target); the release-build timing, interleaved, five runs (§9). | **PROVEN** | §9: medians within the spread of one build's own runs: INSERT 27.62 ms on the base and 28.06 on the head, MERGE 83.37 and 77.79, planning of a fifty-branch union 353.60 and 351.71. |
| C-012 | The unit's gates are green with real exit codes. | §10. | **PROVEN** | §10. |
| C-013 | A nested `timestamp_ns` leaf answers as before: the R-015 refusal precedes this guard, and the nested pins are unchanged and green. | `timestamp_ns_nested_shapes` (6 tests) and the R-007 facade file on the head. | **PROVEN** | §10. |
| C-014 | A nanosecond value beside a NULL the statement typed as a microsecond type (`CAST(NULL AS TIMESTAMP)`, `CAST(NULL AS TIMESTAMP_NTZ)`) is refused on every door, and the text names the value and the written cast that would make it deliberate. | Owner ruling on Q2, 2026-10-10 (§11.1). The Rust door pins of the typed NULL (40 doors); the facade rows; the matrix (§11.5). | **PROVEN** | §11.4, §11.5: 2,225 cells refuse beside a typed NULL, 2,135 of them not refused on the base. |
| C-015 | `nvl(ns, NULL)` and `ifnull(ns, NULL)` are refused by this text. | Owner ruling on Q3, 2026-10-10: not a refusal in this unit; card ICE-TSNS-COERCION-1 owns the typing and registry row R-019 names the `UNION` cell. | **REJECTED** (owner ruling, 2026-10-10) | §11.1; the `UNION` cell is C-026. |
| C-016 | A frame materialised by `cache()` or `persist()` from a narrowed value is refused when it is written. | Owner ruling on Q4, 2026-10-10: not a refusal; card ICE-TSNS-CACHE-1 keeps the nanoseconds through the cache. The site is named and the cells counted. | **REJECTED** (owner ruling, 2026-10-10) | §11.6: 290 of the 300 cells are the cache alone. |
| C-017 | A nanosecond value narrowed beside a microsecond column or literal that is not NULL is refused on every door, by a text that names the value and two ways out. | Owner ruling on Q5, 2026-10-10 (§11.1). The Rust door pins of the microsecond value (40 doors); the facade rows; the matrix (§11.5). | **PROVEN** | §11.4, §11.5: 4,830 cells refuse, 4,590 of them not refused on the base. |
| C-018 | The unfiltered `UPDATE` stores the microsecond value for a written `CAST(c AS TIMESTAMP)`, as every other door does. | Owner ruling on Q6, 2026-10-10: a separate unit after the verify; card ICE-TSNS-UPDATE-CAST-1 holds the cells. | **REJECTED** (owner ruling, 2026-10-10) | §11.7: 15 cells per target for each source type. |
| C-019 | The mark is on the narrowed value: a cast coercion left from a nanosecond timestamp to a coarser one is marked as a branch of a `CASE`, as an argument beside a microsecond sibling and at the root of a plan expression; a cast the statement wrote, through SQL or the DataFrame, and a cast a function's own signature asked for are not. | The unit pins of `null_narrowing/tests.rs`; the written and kept spellings on every door. | **PROVEN** | §11.2; mutants F1 to F11. |
| C-020 | A written call over a narrowed value stores where it stores what the same call stores over the nanosecond value, with a pre-epoch value one nanosecond below a second boundary among the moments, and refuses where it differs; each refusing arm has its reason recorded. | The base measurement, arm by arm (§11.3); `a_written_call_over_a_narrowed_value_stores_what_it_stores_over_the_nanosecond_value`; the `WRITTEN_OVER` facade rows. | **PROVEN** | §11.3; mutants F6, W1 to W5. |
| C-021 | The guard reads an `INSERT` plan through the planner's own cast of a selected column and an `UPDATE` plan as written; in a `UNION` it names the branch narrowed beside an untyped NULL first. | The unit pins of `narrowed_store/tests.rs`; the `EXPLAIN ANALYZE` and `PREPARE` doors of the Rust door file. | **PROVEN** | §11.2; mutants W6, W9, W10. |
| C-022 | Never worse after the rulings: every cell that moved moved into the refusal; no cell that stores nine digits on the base moved; no cell of a microsecond target moved; no refused cell left a value on disk. | The matrix on the final product source, 104,960 cells, head against base (§11.5); the 228 facade rows. | **PROVEN** | §11.5: 23,525 moved into the refusal, 81,215 byte-identical, 220 refused on both. |
| C-023 | Each new rule has a hand mutant that a named pin kills. | §11.8. | **PROVEN** | §11.8. |
| C-024 | The fold adds no analyzer rule; planning a statement that touches no nanosecond column takes the time it took on the base; INSERT and MERGE are timed against the base, release builds, interleaved, five runs, and the figures recorded as measured. | §11.9. | **PROVEN** | §11.9: the plan's median moves by 0.3%; the writes' medians are 3% to 7% higher on the head, inside one build's own spread of 9% to 14%, which this sample does not separate from the machine. |
| C-025 | The gates are green on the fold's final source with real exit codes. | §11.10. | **PROVEN** | §11.10. |
| C-026 | The `UNION` cell of `nvl` and `ifnull` (registry row R-019) keeps storing its cut value, as the ruling on Q3 reads. | Not built: measured, the cell is a nanosecond branch narrowed beside a microsecond one and the ruling on Q5 refuses it (§11.4). | **OPEN** | Q7. Orchestrator ruling of 2026-10-11, provisional until the owner rules: Q7 stands, the cell refuses. |
| C-027 | The 80 cells that refused by name before the rulings and raise the base's own error after them refuse by name again. | Not built: the guard cannot tell the planner's cast of an `UPDATE`'s `SET` value from a written `CAST(… AS TIMESTAMP)`, which Q1 says stores (§11.5). | **OPEN** | Q8. Orchestrator ruling of 2026-10-11, provisional until the owner rules: Q8 stands. Measured after fold 2: 65 of the 80 refuse by name again, because the walk follows every argument; 15 keep the base's error (§12.9). |
| C-028 | Never worse, the two S1s of the verdict: `coalesce(CAST(c AS TIMESTAMP), ts)` and `array(CAST(c AS TIMESTAMP), ts)[0]` store what `CAST(c AS TIMESTAMP)` stores on every door, and a plain nine-digit column stores when a narrowing sits in a `WHERE`, a sibling aggregate or a sibling column under `ROLLUP`, `CUBE` and `LATERAL`. | §12.1 "Written or inserted" and S1; the door pins `a_written_cast_beside_a_microsecond_value_stores_on_every_door` and `a_plain_column_stores_beside_a_narrowing_under_rollup_cube_and_lateral`, red on `45260e44`; the facade pins of the frame writers. | **PROVEN** | §12.8; mutants R1, R11. |
| C-029 | The mark is placed wherever the analyzer narrows a nanosecond value and nowhere the statement does: in a container cast, in a function that unifies in its kernel, on a zoned value cast to the wire type beside a `DATE`, and with no nanosecond column in the node's inputs (an outer reference, a lambda variable). | §12.1 M1, M2 and the skip test; the unit pins of `null_narrowing/tests.rs`; the door pins of the container, DATE and outer-reference families. | **PROVEN** | §12.8; mutants R13 to R17. |
| C-030 | The store follows the stored column alone, through every plan node and every argument whatever its type, and a mark on that lineage refuses: a value that leaves through STRING, BIGINT or JSON text refuses (ruling V2), and an `UPDATE` whose `SET` column or value carries a qualifier refuses in both row-level modes. | §12.1 S1 to S3; the unit pins of `narrowed_store/tests.rs`; `a_narrowed_value_that_leaves_the_timestamp_type_and_returns_refuses`, `an_update_through_a_qualified_set_column_refuses_in_both_modes`, `a_narrowed_aggregate_under_grouping_sets_refuses_and_its_plain_sibling_stores`. | **PROVEN** | §12.8; mutants R2 to R6, R9, R10, R23. |
| C-031 | A written call forgives a narrowed value only where the stored value equals the same expression over the nanosecond value: the innermost call decides, a naive value cut beside a `TIMESTAMP_NTZ` sibling is not forgiven by a cast to `TIMESTAMP` or `DATE`, a call above an arm that refuses does not forgive it, and `to_date(x)` and `date(x)` answer as `CAST(x AS DATE)`. | §12.2 (the verifier's measurement on main); `the_innermost_written_call_over_a_narrowed_value_decides`, `a_written_call_does_not_forgive_a_cut_it_does_not_equal`, `to_date_and_date_answer_as_a_date_cast_does`; the verifier's equality suite on the final build. | **PROVEN** | §12.8, §12.9; mutants R7, R8, R22, R27, W1 to W5. |
| C-032 | A written `CAST(v AS TIMESTAMP)` over a column of a subquery, a temporary view, a common table expression or a frame that holds the analyzer's narrowing stores what the inline cast stores (ruling V1). | §12.1 "Written or inserted", last sentence; `a_written_cast_over_a_column_that_holds_the_narrowing_stores_as_the_inline_cast_does`; the facade pin of the frame doors. | **PROVEN** | §12.8; mutants R20, R21. |
| C-033 | The refusal names what the statement wrote: the value in SQL spelling with no mark, no Arrow type and no interval debug text, a lambda variable with the array it ranges over, and the table's name on every door, MERGE included. | §12.4; `the_refusal_names_what_the_statement_wrote`; every refusing pin of `timestamp_ns_narrow_rule.rs` asserts the text holds none of `__repark`, `Timestamp(`, `Interval` and the empty table name. | **PROVEN** | §12.8; mutants R24, R26, W8. |
| C-034 | A read shows nothing of the unit (ruling V3): no tag in a schema, the nanosecond column's `PARQUET:field_id` kept through a `UNION`, no mark in any form of `EXPLAIN`, and no mark in a join condition, a filter or a sort key. | §12.5; `a_read_shows_no_mark_no_tag_and_keeps_its_field_metadata`; the unit pins of the union schema, the mark's field and the unmarked filter; `explain_marks` unit pin; the verifier's `reads.py` and `pq.py` on the final build. | **PROVEN** | §12.8, §12.9; mutants R16, R18, R19, R25. |
| C-035 | Never worse after the redesign: on the 104,960-cell matrix and on the verifier's suites (shapes, doors, stmts, equality, the int64 limits) every cell that moved moved into the refusal from a cut value, an error or a partial store; no cell that stores nine digits on main moved; no cell of a microsecond target moved; no refused cell left a value on disk; no cell stores a value that differs from the same written call over the nanosecond value. | The matrix and the suites on the final build, main against head (§12.9). | **PROVEN** | §12.9: 104,960 cells, 81,140 identical, 23,600 into the refusal, 220 refused on both; 67,960 suite cells. |
| C-036 | Each rule of the fold has a hand mutant that a named pin kills; a survivor is pinned or explained. | §12.11. | **PROVEN** | §12.11. |
| C-037 | The fold costs a write on a table with no nanosecond column, and a read that mixes nanosecond and microsecond branches, at most 1.05 of main. | §12.12: release builds, interleaved, six sets of fifteen per build. | **PROVEN** | §12.12: at most 1.017. |
| C-038 | The gates are green on the fold's final source with real exit codes. | §12.13. | **PROVEN** | §12.13. |
| C-039 | An `EXPLAIN` of any statement is the lazy statement frame main returns, and the CI checks that were red on `9cee945a` pass (F8). | §13.2; `an_explain_frame_is_a_statement_and_prints_no_hidden_function`; main's two pins. | **PROVEN** | §13.3, §13.8. |
| C-040 | A statement that writes nothing answers as on main: schema with field metadata, values and plan text (F4, F6; addendum E i). | §13.2; `a_written_cast_changes_no_read`; the facade fixture recorded on main, `test_ice_tsns_narrow_refuse_1_reads.py`. | **PROVEN** | §13.5. |
| C-041 | No hidden name is printed by any `EXPLAIN` form, and the `EXPLAIN`, `EXPLAIN VERBOSE` and `EXPLAIN ANALYZE` text of an `INSERT`, `MERGE`, `UPDATE` or CTAS is main's (addendum D). | §13.2 "The planner's cast is signed too"; the sign taken off before physical planning; `a_cast_from_nanoseconds_to_an_instant_before_coercion_is_the_statements`. | **PROVEN** | §13.5: 0 of 205 outputs differ from main. |
| C-042 | A written `TRY_CAST` is a written cast on every door (F1). | §13.2; `a_written_try_cast_beside_a_microsecond_value_stores_on_every_door`. | **PROVEN** | §13.4. |
| C-043 | The walk follows only what the stored digits pass through, and that is one table of roles (F2, addendum B). | §13.2 "The role table"; the store pins of `narrowed_store/lineage_tests.rs`; `a_plain_column_stores_beside_a_narrowing_it_does_not_pass_through`. | **PROVEN** | §13.4; mutants T1, T4, X6, X7 (§13.6). |
| C-044 | A written call reaches the narrowed value through what hands it on unchanged, only while the zone kind is kept (F7, addendum C). | §13.2; `a_written_call_stores_through_a_choice_an_element_a_lambda_and_a_subquery`. | **PROVEN** | §13.4: 0 stored values differ from the twin but the 10 of F3. |
| C-045 | A refusal names a plain column or the target alone and the table the statement wrote; no engine text reaches it (F5). | §13.2 "The text"; `a_refusal_names_a_plain_column_and_otherwise_only_the_target`. | **PROVEN** | §13.3. |
| C-046 | Never worse: no cell main stored fully is refused, errors or stores differently; no microsecond-target cell moves; no refusal leaves a value (addendum E ii). | §13.4. | **PROVEN** | §13.4. |
| C-047 | The verdict's surviving mutants L4 and P3 are pinned, and each rule of the fold has a mutant a named pin kills. | §13.6. | **PROVEN** | §13.6. |
| C-048 | The fold costs at most 1.05 of main on the nine timed statements. | §13.7. | **PROVEN** | §13.7: at most 1.026 on the final build. |
| C-049 | The gates are green on the fold's final source with real exit codes. | §13.8. | **PROVEN** | §13.8. |
| C-050 | F3 (a date call over a naive value cut to an instant, in a zone that skipped a day) is left as on main. | Ruling F3: the smaller change. | **PROVEN** | §13.9 item 6. |
| C-051 | The five card spellings refuse on every write door in five zones (addendum A). | §13.11. | **OPEN** | §13.11: four spellings refuse on 56 of 64 doors; a cached frame stores cut as on main (owner ruling 4), a door that creates its table types it microseconds, and `nvl` is typed STRING (owner ruling 3). Q9. |

## 1. The base, measured before any change (C-001)

### 1.1 Step 0

Build of `8d1c4f49` (debug, `make develop`), recorded by
`python/repark/tests/_record_ice_tsns_narrow_refuse_1.py 8d1c4f49` into
`ice_tsns_narrow_refuse_1_base.json` and committed as `e5950aad` before any product file
changed. 59,400 cells: five session zones (UTC, America/New_York, Asia/Kolkata,
Asia/Kathmandu, Australia/Lord_Howe), four target column types (`timestamp_ns`,
`timestamptz_ns`, `TIMESTAMP`, `TIMESTAMP_NTZ`), two source types (`timestamp_ns`,
`timestamptz_ns`), 27 spellings, 55 doors. What a door stored is read from every Parquet
file under the table directory with `pyarrow`.

**The 650-cell core** (the five spellings of the row, the thirteen doors of the R-007
verify's `gq.py trunc`, both sources, the `timestamp_ns` target, five zones) reproduces the
row's count:

| Class | Cells |
|---|---|
| the value cut to microseconds | 360 |
| cut and the wall moved | 120 |
| the named refusal (`UPDATE` with no `WHERE`) | 40 |
| an error that is not the refusal (`nvl` is typed a string and fails store assignment) | 130 |

### 1.2 The cells added while hunting

Eight spellings and nine doors were added after the first build of the head (a lambda body,
`greatest`, `nullif`, a struct field, a three-branch `CASE`, a narrowing beside a literal, a
NULL beside the value in a struct, a plain lambda; MERGE `*`, an `UPDATE` from a scalar
subquery, reordered columns, a sort with a limit, a `UNION` with an untyped NULL branch in
SQL and through `unionByName`, a higher-order function before the value in the same
projection and below it in a frame). Their 30,200 base cells were measured with the
recorder's `--extend` on the same build of the base, whose native module was kept aside
before the first product build; the 59,400 cells of step 0 are byte-identical in the
extended fixture. The fixture holds 89,600 cells: 35 spellings, 64 doors.

**Classes.** A cell is named against what `INSERT … SELECT` of the plain column stores in
the same zone, target and source (R-007 and R-008 hold every door to that value): `full` is
that value, `cut` is it floored to microseconds, `cut+wall` is a nanosecond-scale value with
no digit below the microsecond at another wall, `refused` is the named refusal, `error` any
other error, `other` anything else (a microsecond column a `CREATE TABLE AS` door made, a
partial store). The first run's classifier named a refusal by a substring that a long table
name pushes past the 240 characters the fixture keeps, and named `cut+wall` without the
scale test; both were corrected before any count below was taken, and the core's count did
not change.

## 2. Where the two narrowings can be told apart (C-002)

**Measured first** (base build, `EXPLAIN VERBOSE` and the DataFrame API, America/New_York):

- `coalesce(c, NULL)` over a `timestamp_ns` column leaves the first type coercion as
  `coalesce(c, CAST(NULL AS Timestamp(ns)))`. The session's timestamp rule then reads that cast
  as the SQL `TIMESTAMP` and wraps it to `Timestamp(µs, "UTC")`. The second type coercion of
  the same analysis narrows the column to match: `CAST(c AS Timestamp(µs, "UTC"))`, a plain
  Arrow cast that reads a naive wall as UTC. A second analysis of the same plan turns that
  cast into `__repark_narrow_timestamp_ns__(c)`, which localises the wall in the session
  zone. The two readings are the "cut" and the "cut and the wall moved" classes.
- A written `CAST(c AS TIMESTAMP)` arrives as `CAST(c AS Timestamp(ns))` and becomes the same
  `__repark_narrow_timestamp_ns__(c)`. The DataFrame `col("c").cast("timestamp")` arrives as
  `CAST(c AS Timestamp(µs, "UTC"))`, the shape the coercion inserts. After analysis nothing
  in the plan separates a written narrowing from an inserted one: not the function, not the
  cast, not its target type. That is the card's evidence, and it holds for both front ends.
- A written `CAST(NULL AS TIMESTAMP)` is `CAST(NULL AS Timestamp(ns))` before the first
  coercion; the untyped `NULL` becomes the same expression after it. Whether the NULL was
  typed by the statement is therefore visible only before the first coercion.
- Whether the sibling was narrowed by the statement or by the coercion is visible only
  between the session's timestamp rule and the second coercion: a written cast is already a
  microsecond value there, a nanosecond sibling is still a nanosecond value.

**The decision.** The distinction is recorded while it exists, in two existing analyzer
passes, and carried in the plan as a marker:

1. *Before the first coercion* (a pass that already visits every expression before
   `type_coercion`; first `HigherOrderPreparation`, in the end `SparkFloatStringify`, see
   below): an untyped `NULL` literal that is a direct branch of a function call or a `CASE`,
   beside a branch whose Arrow type is a nanosecond timestamp, takes a tag in its literal
   metadata (`null_narrowing::tag_untyped_nulls`). The tree keeps its shape, so every rule
   between the two passes treats the literal as before.
2. *In the session's timestamp rule* (`spark_ltz_timestamp_cast`, after each node's branches
   are rewritten): a node that holds a tagged NULL always loses the tag. If the tagged NULL
   is now a microsecond timestamp and a sibling is still a nanosecond timestamp, the coercion
   that follows will narrow that sibling, and the node is wrapped in
   `__repark_narrowed_beside_null__` (`null_narrowing::settle_tagged_nulls`). The wrapper
   returns its argument, has its argument's type and nullability, and its `simplify` removes
   it, so the optimized plan and every evaluated value are the base's.
3. *At the store* (`repark_iceberg::write::narrowed_store`): for a top-level nanosecond
   target column, the lineage of the stored value is walked through the analyzed plan
   (projections, subqueries, unions, joins, aggregates, windows, scalar subqueries, inlined
   and catalog views); a wrapper on a value-carrying path refuses with the text the
   unfiltered `UPDATE` carries today. A branch whose type holds no timestamp is not followed,
   so a narrowed value that only feeds a condition does not refuse.

No plan pass is added. A statement with no untyped NULL beside a nanosecond branch is not
changed by pass 1, pass 2 finds no tag, and the store walk starts only for a nanosecond
target column.

**Why the marker and not the alternatives.**

| Alternative | Why not |
|---|---|
| A test on the lineage of the stored value (any cast from nanoseconds to a coarser timestamp) | The card's evidence: it refuses a written `CAST(c AS TIMESTAMP)` and `date_trunc`. The unfiltered `UPDATE` keeps that test (it answers nothing there on main), no other door takes it. |
| Read the cast's target type at the narrowing site (`Timestamp(µs, "UTC")` is inserted, `Timestamp(ns)` is written) | Measured false: the DataFrame `cast("timestamp")` writes `Timestamp(µs, "UTC")`. |
| Compare the plan before analysis with the analyzed plan at the write door | A view, a temp view of a frame and a frame from `spark.sql` reach the door already analyzed; the plan before coercion is gone. The marker has to be placed during analysis to survive them. |
| A new analyzer rule before `type_coercion` | It is a plan pass on every statement; the acceptance forbids it. The existing pass costs one enum match per expression node. |
| Decide everything in the session's timestamp rule, from the shape `CAST(CAST(NULL AS Timestamp(ns)) AS Timestamp(µs, "UTC"))` | That shape is also a written `CAST(NULL AS TIMESTAMP)`; the rule would refuse a NULL the statement typed. Question Q2. |
| Carry the mark in the result type (a second spelling of the UTC zone) | It changes the Arrow type every query returns for these spellings, and an array element cast back to nanoseconds loses it. |
| Keep the tag as literal metadata in the analyzed plan | `Cast` copies its source field's metadata, so the tag would reach result schemas. The tag lives only between the two passes. |
| Replace DataFusion's coercion, or keep the nanoseconds through it | The real fix, card ICE-TSNS-COERCION-1, out of this unit. |

The marker is not inside DataFusion's coercion and needs no new seam between crates: both
passes are in `repark-functions`, the store walk is in `repark-iceberg` beside the guard it
mirrors (`negated_null_store`, which walks the same lineage for another refusal), and the
two crates share only two function names, pinned equal as the store kernels' names are.

**Added while building** (the decision above stands; these are its edges, each found by a
probe or a pin and measured before it was built):

- *The pass that tags.* The decision first named `HigherOrderPreparation`. Measured against
  it: `transform(a, x -> coalesce(x, NULL))` stored the cut value, and so did
  `SELECT size(transform(…)), coalesce(ns, NULL)` and a frame whose plan holds a
  higher-order function below a `coalesce(ns, NULL)`. That pass returns "stop" at a
  higher-order function, which ends its walk of the node's remaining expressions and, when
  nothing changed, of the plan above. The tag now rides `SparkFloatStringify`, the next
  pass before `type_coercion`, which walks every expression of every plan node bottom-up,
  lambda bodies included, and has no early stop. `HigherOrderPreparation` is as it was. The
  store walk enters a lambda body.
- *A `UNION` branch.* `SELECT id, ns … UNION ALL SELECT id, NULL …` stored the cut value on
  the base (the fixture's `union_null_insert` cells) by the same reading: the union's
  coercion casts the NULL column to `Timestamp(ns)`, the session rule reads it as
  microseconds, the second coercion narrows the other branch. A NULL column there has the
  Arrow type `Null`, which only an untyped NULL has, so the tag needs no literal: the branch's
  column is replaced by a tagged NULL literal of the same name (`tag_union_nulls`), in the
  branch's projection or in one added above a branch that is not a projection, and the
  session rule wraps it when another branch is still nanoseconds (`settle_union_nulls`).
  `unionByName` builds its branch projections with the union's schema, so a branch is typed
  by its expressions, not by its declared schema.
- *The narrowing must sit beside the mark.* The session rule decides before the second
  coercion, from the types of the siblings. A sibling whose own coercion is still pending
  (`CASE WHEN … THEN ns ELSE TIMESTAMP '…' END`, a nanosecond value beside a written
  microsecond literal) reads as nanoseconds there and is narrowed inside itself, not beside
  the NULL. The store therefore refuses only when a direct sibling of the marked NULL is a
  cast from a nanosecond timestamp to a coarser one, or `__repark_narrow_timestamp_ns__` of
  one, which is the cast the second coercion inserts; for a `UNION` the cast is the other
  branch's column. A mark without it is walked through like any other expression.

## 3. What was built (C-003, C-008)

### 3.1 The mark (`repark-functions`)

`null_narrowing.rs` (new): `tag_untyped_nulls`, `tag_union_nulls`, `settle_tagged_nulls`,
`settle_union_nulls` and the function `__repark_narrowed_beside_null__`. Hooks: in
`java_double.rs` `rewrite_float_plan`, `tag_untyped_nulls` after `rewrite_float_expr` for
each expression node and `tag_union_nulls` for each plan node; in `instant_ts.rs`
`rewrite_plan`, `settle_tagged_nulls` after `rewrite_cast` for each node and
`settle_union_nulls` for each plan node; and the function's registration. No rule was
added to the analyzer and none was reordered.

What a statement without a nanosecond value pays: `tag_untyped_nulls` returns at the first
of three tests (is the node a function call or a `CASE`; has it an untyped NULL branch; has
it a nanosecond branch), `tag_union_nulls` and `settle_union_nulls` return unless the node
is a `UNION`, and `settle_tagged_nulls` returns unless a branch is a tagged NULL. A node
that has an untyped NULL branch asks its siblings' types once; that is the only work added
to a statement that then turns out to touch no nanosecond value.

### 3.2 The store guard (`repark-iceberg`, called from `repark-spark`)

`narrowed_store.rs` (new): `refuse_narrowed_ns_writes(ctx, table, plan, targets)` analyzes
the supply, pairs its output columns with the target columns by position and, for each
target that is a top-level nanosecond timestamp, walks the column's lineage
(`column_narrowed`, `expr_narrowed`). It returns before any work when no target is a
nanosecond timestamp.

The DataFrame writers reach the store through SQL: `append`, `insertInto`, `saveAsTable`,
`overwrite`, `overwritePartitions`, `createOrReplace` and `mergeInto` register the frame as a
temp view and run an `INSERT`, an `INSERT OVERWRITE`, a `REPLACE WHERE`, a `CREATE TABLE AS`
or a `MERGE` over it (`python/repark/src/repark/spark/dataframe/writer_readwriter.py`), so
the SQL router is their entry too. There is no streaming engine on the Python door
(`readStream` refuses, `session-surface-1/C-004`); the Rust micro-batch sink appends record
batches, which are data and not an expression (the same as Q4).

The guard is not at the router's nested gate (`router::execute_calibrated`), where R-007's
refusal sits. That gate decides from the statement's text and the target's schema; this
refusal needs the analyzed supply paired with the target columns, and before the router's
rewrites a supply that names a branch, a snapshot or a metadata table does not plan. The
guard is called at the five places a door already holds that pairing:

| Call site | Doors it holds |
|---|---|
| `insert_timestamp_ns::refuse_narrowed_stores`, called by `spark_ast` on the analyzed plan: the DataFusion `Dml` of an `INSERT` or an `UPDATE`, at the top, under `EXPLAIN ANALYZE` or inside `PREPARE` | `INSERT … VALUES`, `INSERT … SELECT` in every spelling, `REPLACE WHERE`, the DataFrame `append`, `insertInto`, `saveAsTable` (append) and `overwrite(condition)`, a branch and a staged snapshot, `EXPLAIN ANALYZE INSERT`, `PREPARE`, `UPDATE` with no `WHERE` |
| `ntz_store::zone_stores` | `INSERT OVERWRITE` (whole table, dynamic and static partition), `INSERT … BY NAME`, the DataFrame `overwritePartitions` and `insertInto(overwrite=True)` |
| `ntz_store::refuse_ntz_writes`, the timestamp store guard the MERGE and `UPDATE` probes already call | MERGE matched update, not-matched insert and not-matched-by-source, in both row-level modes, on a branch, through `mergeInto` |
| `update_cast::refuse_incompatible_update_cast`, the `UPDATE` assignment probe, with the table's name | `UPDATE` with and without a `WHERE`, both modes, from a scalar subquery, under `EXPLAIN ANALYZE` |
| `insert_timestamp_ns::refuse_narrowed_columns`, called by `ctas::execute_ctas` | `CREATE TABLE AS`, `CREATE OR REPLACE TABLE AS`, the DataFrame `createOrReplace` and `saveAsTable` (overwrite), when the statement would create a nanosecond column |

A prepared statement is refused when it is prepared, so `EXECUTE` finds none and nothing is
stored. A plain `EXPLAIN` writes nothing and answers as before.

The unfiltered `UPDATE` keeps its older lineage test (`Narrowed::refuse`, any cast from
nanoseconds to a coarser timestamp) beside the new guard. It refuses more there than the
mark does (a typed `TIMESTAMP` NULL, `date_trunc` of a zoned source), and removing it would
turn those refusals into stores of a cut value.

### 3.3 The table's name in the text

The text names the table as the door that raises it names a table in its other refusals:
`` `ice.ns.t` `` on the `INSERT` and `UPDATE` doors (the unfiltered `UPDATE`'s text is
byte-identical to the base's), an empty name on the MERGE doors (the MERGE store refusals
all carry it), `` `ns`.`t` `` on the overwrite doors, `` `ice`.`ns`.`t` `` on
`CREATE TABLE AS`, and the pinned relation's name for a write to a branch or a staged
snapshot (as the unfiltered `UPDATE` of a branch did on the base).

## 4. The matrix, head against base (C-004)

The recorder on the head (`--output`, then `--compare` against the fixture), 89,600 cells.
**16,800 cells moved, every one into the named refusal, and no refused cell left a value in
any Parquet file under its table. The other 72,800 cells are the base's cells:** the stored
values, and the error text where there is one.

| Class on the base | Refused now | Unchanged |
|---|---|---|
| the value INSERT stores for the plain column (`full`) | 0 | 27,084 |
| cut to microseconds | 11,749 | 4,484 |
| cut and the wall moved | 3,706 | 5,126 |
| an error that is not the refusal | 1,145 | 10,140 |
| anything else (a microsecond column a `CREATE TABLE AS` door made, a partial store) | 200 | 22,376 |
| the named refusal | 1,110 keep it, with the same text | |
| nothing written (`EXPLAIN`), no such spelling on the door | 0 | 2,480 |

**The 650-cell core:** the 360 cut cells and the 120 cut cells with a moved wall refuse; the
40 refusals of the unfiltered `UPDATE` are unchanged, text included; the 130 `nvl` errors
are unchanged (Q3).

**By door**, for the fifteen spellings that narrow beside an untyped NULL (the row's four
that are typed timestamps, and `CASE` with the NULL first, `coalesce(NULL, c)`,
`element_at`, a lambda body, `greatest`, `nullif`, a struct field, a three-branch `CASE`,
and `date_trunc` of, a cast back to nanoseconds of, and a `CASE` around a narrowed value),
both nanosecond targets, both sources, five zones, 19,200 cells:

| Door | Cells | Cut on the base, refused now | Error or partial on the base, refused now | Refused on both | Cut on both | Other, unchanged |
|---|---|---|---|---|---|---|
| `INSERT … VALUES` | 300 | 280 | 20 | 0 | 0 | 0 |
| `INSERT … SELECT`, by position, reordered, `BY NAME`, after a higher-order function | 1500 | 1500 | 0 | 0 | 0 | 0 |
| `INSERT OVERWRITE`, static and dynamic partition | 900 | 900 | 0 | 0 | 0 | 0 |
| `REPLACE WHERE` | 300 | 300 | 0 | 0 | 0 | 0 |
| MERGE matched update (expression, subquery, `*`), both modes | 1500 | 1500 | 0 | 0 | 0 | 0 |
| MERGE not-matched insert (expression, subquery, `*`), both modes | 1200 | 1200 | 0 | 0 | 0 | 0 |
| MERGE not-matched-by-source, both modes | 600 | 600 | 0 | 0 | 0 | 0 |
| `UPDATE` with a `WHERE`, both modes, and from a scalar subquery | 900 | 520 | 300 | 0 | 0 | 80 |
| `UPDATE` with no `WHERE`, both modes | 600 | 0 | 0 | 600 | 0 | 0 |
| DataFrame `append`, `insertInto`, `saveAsTable` append, `selectExpr` (also above a higher-order function), functions API | 1800 | 1540 | 0 | 0 | 0 | 260 |
| DataFrame `overwrite(condition)`, `overwritePartitions`, `insertInto(overwrite=True)` | 900 | 900 | 0 | 0 | 0 | 0 |
| DataFrame `mergeInto` | 300 | 300 | 0 | 0 | 0 | 0 |
| `createOrReplace`, `saveAsTable` overwrite, `CREATE TABLE AS`, `CREATE OR REPLACE TABLE AS` | 1200 | 160 | 0 | 0 | 0 | 1040 |
| a branch (INSERT, MERGE, UPDATE) and a staged snapshot | 1200 | 900 | 0 | 300 | 0 | 0 |
| `EXPLAIN ANALYZE` (INSERT, UPDATE), `PREPARE` with `EXECUTE` | 900 | 75 | 825 | 0 | 0 | 0 |
| plain `EXPLAIN` | 300 | 0 | 0 | 0 | 0 | 300 |
| a common table expression, a subquery, a union, a join, an aggregate, a window, `DISTINCT`, a scalar subquery, a sort with a limit | 2700 | 2660 | 40 | 0 | 0 | 0 |
| a view (of a frame, temporary, catalog), MERGE from a frame view | 1200 | 1200 | 0 | 0 | 0 | 0 |
| a `UNION` with an untyped NULL branch (SQL, `unionByName`) | 600 | 600 | 0 | 0 | 0 | 0 |
| a frame materialised by `cache()` | 300 | 0 | 0 | 0 | 300 | 0 |

Reading the columns. *Error or partial on the base, refused now* (1,185 cells): the base
raised DataFusion's internal error for these spellings through `INSERT … VALUES` (20),
an internal, raw Arrow or schema error through `EXPLAIN ANALYZE` (INSERT and UPDATE) and
`PREPARE` (825), another error through an `UPDATE` from a scalar subquery (300), and
stored one branch of a `UNION` cut and the other whole (40); all carry the named refusal
now. *Other, unchanged*: the two
spellings that cast to a nanosecond type keep main's `Unsupported SQL type` through
`UPDATE … WHERE` (80, parity row R-010); the functions API has no spelling for ten of the
fifteen (260); a `CREATE TABLE AS` door makes a microsecond column for the spellings typed
microseconds, which is no nanosecond target (1,040; it refuses the 160 cells whose column
it would create as nanoseconds: `array(c, NULL)[0]` and the cast back); a plain `EXPLAIN`
writes nothing (300). *Cut on both*: a frame materialised by `cache()` (300, Q4).

The pins hold the same doors: the Rust door file by its own statements (40 doors, five
zones, 13 tests), the facade file by the fixture (64 doors, 172 rows, 173 tests).

## 5. Never worse (C-005, C-006)

**No cell that stores the right value moved.** 27,084 cells store on the base what INSERT
stores for the plain column (8,492 of them on a nanosecond target); all 27,084 are
byte-identical on the head.

### 5.1 Nanosecond targets

- **A written narrowing stores what it stored.** Ten spellings (`CAST(c AS TIMESTAMP)`,
  `TRY_CAST`, `CAST(c AS TIMESTAMP_NTZ)`, `date_trunc`, a `CASE`, a `coalesce` and an `if`
  whose branch is a written narrowing, a NULL typed `TIMESTAMP`, a NULL typed
  `TIMESTAMP_NTZ`, a narrowing beside a `TIMESTAMP` literal), 12,800 cells: 12,760
  unchanged, the 210 refusals of the unfiltered `UPDATE` among them. The 40 that moved are
  `TRY_CAST(c AS TIMESTAMP)` in a `UNION` with an untyped NULL branch: `TRY_CAST` of a
  nanosecond value keeps the nanosecond type (it is not lowered to the narrowing kernel), so
  the cut was the union's coercion beside the NULL, the unit's purpose.
- **A kept type stores what it stored.** Seven spellings (the plain column, a typed NULL in
  `coalesce` and in `CASE`, a `CASE` with no `ELSE`, a narrowed value in the condition only,
  a NULL beside the value in a struct, a plain lambda), 8,960 cells: 8,680 unchanged. The
  280 that moved are those spellings in a `UNION` with an untyped NULL branch, which stored
  a cut value on the base.
- **The family.** 23,040 cells (eighteen spellings): 16,320 moved into the refusal; 900
  refusals of the unfiltered `UPDATE` unchanged; 80 cells that store nine digits on the base
  (`nvl` and `ifnull` through `INSERT … VALUES` and an `UPDATE` of a branch) still store
  them; 340 still store a cut value (300 through a cached frame, Q4; 40 `nvl` and `ifnull`
  in a `UNION`, Q3); the rest are the base's errors and its microsecond `CREATE TABLE AS`
  columns.

### 5.2 Microsecond targets (C-006)

44,800 cells of `TIMESTAMP` and `TIMESTAMP_NTZ` targets. The 42,000 cells of the 60 doors
that write the target are byte-identical. The four
doors that create the table from the query (`createOrReplace`, `saveAsTable` overwrite,
`CREATE TABLE AS`, `CREATE OR REPLACE TABLE AS`) never see the row's target type: 160 of
their 2,800 cells create a nanosecond column from `array(c, NULL)[0]` or a cast back to
nanoseconds and refuse, exactly as in the nanosecond rows; the other 2,640 are unchanged.

### 5.3 What stores a cut value on the head, as on the base

| What | Cells | Why it is not refused |
|---|---|---|
| a nanosecond value beside a NULL typed `TIMESTAMP` or `TIMESTAMP_NTZ` | the `typed_ts_null` and `typed_ntz_null` rows of §5.1 | the statement wrote the type (Q2) |
| `nvl(ns, NULL)`, `ifnull(ns, NULL)` in a `UNION ALL` with a nanosecond branch | 40 | typed `STRING`; they raise that door's store-assignment error on 56 of 64 doors (Q3) |
| a frame materialised by `cache()` | 300 | the write sees data, not an expression (Q4) |
| a nanosecond value beside a microsecond literal or column that is not NULL | the `beside_literal` row of §5.1 | the narrowing is not beside a NULL (Q5) |

`nvl2(i, c, NULL)` raises on every door on both builds (`nvl2(UserDefined)`, a coercion
failure of the function itself).

## 6. The text and Spark's class (C-007)

```text
[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the table
`ice.ns.t`: Cannot safely cast `v` "TIMESTAMP" to "TIMESTAMP_NS". The value was narrowed from
nanoseconds to microseconds before the store; give the NULL beside it the type timestamp_ns.
SQLSTATE: KD000
```

It is the text the unfiltered `UPDATE` carried since R-007 (`"TIMESTAMPTZ_NS"` and
`timestamptz_ns` for a zoned column, since R-008). Spark 4.1.2 was measured before keeping
it (2026-10-10, local session, a parquet table): `INSERT INTO t SELECT 1, TIMESTAMP '…'` into
an `INT` column, and the same through `coalesce(TIMESTAMP '…', NULL)`, raise
`[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the
table `spark_catalog`.`default`.`t`: Cannot safely cast `v` "TIMESTAMP" to "INT". SQLSTATE:
KD000`, error class `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`, state `KD000`. The
class, the sentence and the state are Spark's; the second sentence is repark's, because
Spark has no nanosecond type and no such narrowing. Spark types the pairs the unit reads
the same way the rule does: `coalesce(TIMESTAMP_NTZ '…', NULL)` is `timestamp_ntz` (the NULL
takes its sibling's type), `coalesce(TIMESTAMP_NTZ '…', CAST(NULL AS TIMESTAMP))` is
`timestamp` (a typed NULL widens the pair).

## 7. The recorded disagreement (C-009)

The card records three written casts on the unfiltered `UPDATE` that disagree. Measured on
the base and on the head, America/New_York, `timestamp_ns` source and target, the four
moments (the second is a wall inside New York's gap):

| `SET v =` | The unfiltered `UPDATE` | `INSERT … SELECT` and every other door |
|---|---|---|
| `CAST(c AS TIMESTAMP)` | nine digits: `1767323045123456789`, `1772937000000000001` | the microsecond instant's wall: `1767323045123456000`, `1772940600000000000` (03:30) |
| `CAST(c AS TIMESTAMP_NTZ)` | `1767323045123456000`, `1772937000000000000` | the same |
| `CASE WHEN id > 0 THEN CAST(c AS TIMESTAMP) ELSE NULL END` | `1767323045123456000`, `1772940600000000000` | the same |

**Verdict.** All three are narrowings the statement wrote, so under the owner rule all
three store, and none moved on any door (§5). The disagreement is not between a written
and an inserted narrowing. It is one door's reading of one spelling: the unfiltered
`UPDATE` peels a top-level cast to the planner's `Timestamp(ns)` (it cannot tell the
statement's `CAST(c AS TIMESTAMP)` from the cast the planner adds to reach the column's
type), so it keeps nine digits where the 51 other doors that store into the column store the value the
cast was written to produce. The card's lean (a written `CAST` stores the value it was
written to produce) describes the other doors and the second and third rows; the first row
on that one door stores more than was written. It is left as it is, because the unit moves
no written narrowing (question Q6).

**The third row is not a cut the analyzer makes.** The brief reads the lean as refusing it
(a written cast inside the branch beside the NULL). Measured: the branch is already a
microsecond value when the NULL is read, because the statement's cast made it one; the NULL
then takes the branch's microsecond type, and no coercion narrows anything. The microsecond
type does not exist "only because type coercion widened a NULL branch"; it exists because
the statement wrote `CAST(c AS TIMESTAMP)`. The rule decides the row and it stores, with
the value the cast was written to produce, the gap wall at 03:30 included (question Q1).

## 8. Mutants (C-010)

Each applied by hand to the committed tree, the named suites run, the file restored
(`git status` clean of the product files after each pass). "Door" is
`every_door_refuses_a_value_narrowed_beside_an_untyped_null_in_utc` unless another test is
named; a mutant that names a unit pin and a door pin was run against both and reds both.

| Mutant | Killed by |
|---|---|
| M1 an untyped NULL beside a nanosecond branch is not tagged | three unit pins of `null_narrowing`; the door pin |
| M2 a settled node is never wrapped | `a_widened_null_beside_a_nanosecond_branch_is_marked_and_the_tag_is_gone`; the door pin |
| M3 a node is wrapped without a sibling that is still nanoseconds | `a_branch_narrowed_before_the_null_is_read_is_not_marked`; `a_narrowing_the_statement_writes_and_a_kept_type_store_on_every_door` (the written cast beside the NULL refuses) |
| M4 a NULL the statement typed is tagged too | `an_untyped_null_beside_a_nanosecond_branch_is_tagged_and_no_other_null`; `a_narrowing_the_statement_writes_…` (the typed `TIMESTAMP` NULL refuses) |
| M5 the tag is left in the settled node | five unit pins of `null_narrowing` |
| M6 the session rule does not settle a tag | the door pin |
| M7 a NULL column of a `UNION` branch is not tagged | three unit pins; `a_union_with_an_untyped_null_branch_refuses_and_a_typed_one_stores` |
| M8 a `UNION` branch is settled without the wrapper | `an_untyped_null_branch_of_a_union_beside_nanoseconds_is_tagged_then_marked`; the union door pin |
| M9 the optimizer keeps the wrapper | `the_marker_is_its_argument_and_the_optimizer_drops_it` |
| M10 the DML plan is not guarded | the door pin (`INSERT … VALUES`) |
| M11 `EXPLAIN ANALYZE` is not looked into | the door pin |
| M12 `PREPARE` is not looked into | the door pin |
| M13 the overwrite doors are not guarded | the door pin |
| M14 the shared MERGE and `UPDATE` guard does not ask | the door pin |
| M15 the `UPDATE` probe does not name the table | the door pin (the table's name in the text) |
| M16 `CREATE TABLE AS` is not guarded | `create_table_as_refuses_a_narrowed_value_it_would_type_as_nanoseconds` |
| M17 a `UNION` is followed only when every branch carries the mark | two unit pins of `narrowed_store`; the door pin |
| M18 the right side of a join is not followed | the door pin |
| M19 an aggregate is not followed | `a_marked_value_is_followed_through_the_plan_nodes_between_it_and_the_store`; the door pin |
| M20 a window expression is not followed | the door pin |
| M21 a scalar subquery is not followed | the door pin |
| M22 a lambda body is not followed | `every_door_refuses_the_other_spellings_of_the_family` |
| M23 a branch is followed whatever its type | `a_marked_value_is_followed_through_every_carrying_position_and_no_other` |
| M24 a `CASE` is followed through its conditions | the same unit pin; `a_narrowing_the_statement_writes_…` (the narrowed value in the condition only refuses) |
| M25 a view held by a table scan is not followed | `a_marked_value_is_followed_into_a_view_the_scan_holds` |
| M26 a filter is not followed | `a_marked_value_is_followed_through_the_plan_nodes_between_it_and_the_store` |
| M27 a microsecond target is walked too | `only_a_nanosecond_target_refuses_and_the_text_names_the_column`; `a_microsecond_target_takes_a_narrowed_value_as_before` |
| M28 the store refuses on the mark alone | `a_marked_value_is_followed_through_every_carrying_position_and_no_other` |
| M29 a marked NULL branch of a `UNION` refuses beside any branch | `a_marked_null_branch_of_a_union_refuses_only_beside_a_narrowed_branch` |
| M30 the store looks for another function name | `the_store_reads_the_mark_the_analyzer_places`; `a_written_call_over_a_narrowed_value_does_not_make_it_store` |
| M31 a plain `EXPLAIN` is looked into | `a_query_that_stores_nothing_answers_as_before` |

Two runs were lost and repeated: the first pass of M1 to M20 ran while a trait import was
missing from an uncommitted edit, so nothing compiled and nothing was counted; M6's first
text did not compile. The four other zone tests and the facade pins hold the same rules
over more cells and were not run under the mutants; they were green on the unmutated build
(§10).

## 9. Cost (C-011)

**The code.** No rule was added to the analyzer and none reordered (§3.1). The tag test
rides the float-stringify pass and the settle test rides the session's timestamp rule; both
return at their first test for a node that has no untyped NULL branch. The store walk is
behind `nanosecond_target`: `refuse_narrowed_ns_writes` returns before it analyzes anything
when no target column is a nanosecond timestamp, and the DML guard reads the target's
schema it already holds.

**The timing.** Release builds of the base `8d1c4f49` and of the head `544cfbe2`
(`cargo build --release -p repark-python --features extension-module,allocator-mimalloc`,
each tree in its own target directory), `python/repark/tests/_time_ice_tsns_narrow_refuse_1.py`:
a 2,000-row table with no nanosecond column, every expression list holding untyped NULLs
beside microsecond timestamps (`coalesce(t, NULL)`, `CASE … ELSE NULL END`, `if(…, n, NULL)`,
`array(t, NULL)[0]`), which is the worst case for the tag test. Each figure is the median of
fifteen statements, in milliseconds; five runs, the two builds interleaved, four cores
outside the build set on a shared machine (load about 8).

| Run | INSERT base | INSERT head | MERGE base | MERGE head | MERGE merge-on-read base | head | `EXPLAIN`, fifty-branch union, base | head |
|---|---|---|---|---|---|---|---|---|
| 1 | 27.62 | 28.06 | 83.37 | 76.52 | 106.29 | 109.55 | 355.65 | 360.85 |
| 2 | 26.26 | 27.20 | 74.82 | 77.79 | 112.15 | 104.95 | 350.69 | 348.84 |
| 3 | 24.27 | 25.97 | 71.79 | 72.22 | 96.59 | 96.60 | 345.67 | 347.16 |
| 4 | 40.25 | 29.41 | 87.29 | 86.19 | 118.76 | 113.68 | 356.55 | 353.66 |
| 5 | 29.44 | 29.29 | 85.95 | 79.99 | 112.38 | 116.54 | 353.60 | 351.71 |
| median | 27.62 | 28.06 | 83.37 | 77.79 | 112.15 | 109.55 | 353.60 | 351.71 |

The medians differ by +1.6%, −6.7%, −2.3% and −0.5%; one build's own runs differ by 20% to
60% on the writes and by 3% on the plan, so no difference is measured. The `EXPLAIN` row is
planning alone and the steadiest.

**What the first pass found.** Before commit `544cfbe2` the same script, on a quieter
machine, measured the fifty-branch plan 1.1% slower on the head (345.02 against 348.96 ms,
five runs each within 2.5%): `tag_union_nulls` typed every projection expression of every
`UNION` branch to look for a NULL-typed column. It now reads a NULL literal, a column whose
input field is NULL-typed or a cast to the NULL type directly, and types the other branches
only for such a column. The table above is after that change.

## 10. Gates (C-012, C-013)

Run 2026-10-10 on the head `544cfbe2` (the code), one cargo command at a time under the
build lock on cores 32-47; the document gates on the tree of the records commit.

| Command | Exit | Result |
|---|---|---|
| `cargo fmt --all -- --check` | 0 | |
| `make rust-clippy` | 0 | |
| `make rust-panic-ban` | 0 | |
| `cargo test --locked -p repark-functions --lib` | 0 | 919 passed, 1 ignored |
| `cargo test --locked -p repark-iceberg --lib` | 0 | 1005 passed |
| `cargo test --locked -p repark-spark --lib` | 0 | 2692 passed, 5 ignored |
| `cargo test --locked -p repark-sql --lib` | 0 | 394 passed |
| `cargo test --locked -p repark-core --lib` | 0 | 1420 passed, 1 ignored |
| `cargo test --locked -p repark-spark --test timestamp_ns_narrow_refuse` | 0 | 13 passed |
| `cargo test --locked -p repark-spark --test timestamp_ns_nested_shapes --test timestamp_ns_wall_doors --test timestamptz_ns_wall_doors` | 0 | 6 + 7 + 4 passed |
| `make develop` | 0 | |
| `pytest python/repark/tests/test_ice_tsns_narrow_refuse_1.py python/repark/tests/test_ice_tstzns_wall_1.py python/repark/tests/test_ice_tsns_merge_wall_1.py python/repark/tests/test_ice_tsns_sql_1.py -q -n 8` | 0 | 703 passed |
| `pytest python/repark/tests -q -n 12 -k "iceberg or v3 or merge or timestamp or nested or struct or union or lambda or transform or coalesce or null or higher or float or double"` | 0 | 5323 passed, 214 skipped, 16 xfailed |
| the recorder on the head, `--compare` against the fixture | 0 | §4; run twice on the final code, 0 of 89,600 cells differ between the two runs |
| `python3 comment_ban.py <clone> origin/main HEAD` | 0 | hits=0 |
| `ruff check .` and `ruff format --check .` (0.15.22) | 0 | |
| `make check-rust-file-size`, `make check-lib-rs`, `make check-crate-dag`, `make check-manifest` | 0 | |
| the map, ledger, link, spelling and compaction gates | 0 | |

The nested pins are untouched and green (C-013): `timestamp_ns_nested_shapes` (6 tests, the
unfiltered `UPDATE`'s first refusal among them) and the R-007 facade file.

**Not green outside the unit.** `cargo clippy -p repark-spark` alone (without the
workspace's feature unification) fails in `repark-core/src/named_sources.rs` (`unused_self`,
`unused_async` when the `postgres` feature is off), on the base as well; `make rust-clippy`
builds the workspace and is green.

## 11. Fold 1: the owner's rulings of 2026-10-10 on Q1 to Q6

Sections 2 to 9 describe the unit as it stood before these rulings (head `91f4e2f4`). This
section records each ruling, what it changed, and the measurements of the changed source.
Where the two differ, this section holds.

### 11.1 The rulings

| Question | Owner ruling, 2026-10-10 | Effect in this unit |
|---|---|---|
| Q1 | A written cast inside a NULL branch stores: ratified. The `date_trunc` case is overturned to store: a written cast or `date_trunc` over a narrowed value is the outermost narrowing and the user wrote it. The pin must show the stored value equals the same expression over the nanosecond value, including a pre-epoch value one nanosecond below a second boundary. An arm whose value differs refuses, and the ledger says why. | Built, arm by arm (§11.3). |
| Q2 | `coalesce(c, CAST(NULL AS TIMESTAMP))` refuses; the remedy names `c` and the written cast that would make it deliberate. A NULL typed `TIMESTAMP` or `TIMESTAMP_NTZ` beside a nanosecond value refuses on every door. | Built (§11.2, §11.4). |
| Q3 | `nvl` and `ifnull`: the coercion card owns them. The one `UNION` shape that stores a cut value gets a dated registry row naming the cell, linked from the card. Not a refusal in this unit. | Registry row R-018 in the parity document; the coercion card links it. Nothing built. |
| Q4 | A cached or persisted frame is not a refusal. Its own card, v1.5.5; the fix is to keep nanoseconds through cache and persist. Measure the site and count the cells that are the cache alone. | Not fixed. Site and count in §11.6; card ICE-TSNS-CACHE-1 filed. |
| Q5 | `coalesce(ns, ts)` refuses, the same family; build it here if the existing site carries it. | Built: the site carries it (§11.2, §11.4). |
| Q6 | The unfiltered `UPDATE` keeping nine digits under a written `CAST` is a separate unit, v1.5.5, after the verify. | Not built. Card ICE-TSNS-UPDATE-CAST-1 filed with the cells (§11.7). |

### 11.2 What changed in the source

The mark moved from the NULL to the value. Before the rulings the analyzer marked the untyped
NULL and the store looked for a narrowing cast beside it. Now two identity functions,
`__repark_narrowed_beside_null__` and `__repark_narrowed_beside_value__`, wrap the nanosecond
value that coercion is about to narrow, and the store refuses when a cast to a coarser
timestamp sits directly on a marked value. The name of the function chooses the text.

- **Which casts are marked** (`repark-functions`, `null_narrowing.rs`). After the first type
  coercion, a `Cast` from a nanosecond timestamp to a coarser one is the analyzer's: a cast
  the statement wrote is a different node by then (SQL `CAST(c AS TIMESTAMP)` is a cast to
  the nanosecond wire type that the session rule rewrites; a DataFrame `.cast("timestamp")`
  is turned into the written-narrowing function by the pass that runs before coercion,
  `before_coercion`). `mark_coerced_branches` marks such a cast when it is a branch of a
  `CASE`, or an argument of a function that also holds a microsecond timestamp argument
  which is not itself such a cast. `mark_coerced_narrowing` marks it at the root of a plan
  expression, which is where a `UNION` puts it. A cast that only fits a function's
  signature (`date_trunc('second', ns)`, `from_utc_timestamp(ns, 'UTC')`) has no microsecond
  sibling and is not marked: the statement wrote that call.
- **The untyped NULL** keeps its tag before coercion; `settle_branches` marks the nanosecond
  sibling with the beside-a-NULL function and removes the tag. The tag now only chooses the
  text.
- **The pass before coercion** is its own rule, `FloatStringifyBeforeCoercion`, registered
  where the first float-stringify pass was. The second float-stringify pass runs after
  coercion and must not read a coerced cast as a written one.
- **A written `CAST(… AS TIMESTAMP)` over a marked subtree** is kept as the written-narrowing
  function by `keep_written_cast`; the session rule would otherwise drop it as a cast between
  equal types, and the store could not see that the statement wrote it.
- **The store guard** (`repark-iceberg`, `write/narrowed_store.rs`) returns the narrowing it
  found (`Narrowing`: the value's text, which mark, zoned or not) and carries what written
  call it has passed on the way down (`Written`). §11.3 lists what each written call
  forgives. In a `UNION` it prefers the branch narrowed beside an untyped NULL, so the text
  names the NULL when one is there.
- **`INSERT` under `EXPLAIN ANALYZE` and `PREPARE`** (`refuse_narrowed_ns_inserts`). The
  planner's own cast of each selected column to the target type is the same node as a
  written `CAST(v AS TIMESTAMP)` once the session rule has read it. On an `INSERT` the guard
  looks through that cast when it sits directly on a column of the query below. An `UPDATE`
  is not looked through: its top expression is the statement's `SET` value.

The two texts, after the shared head
`[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the table <t>: Cannot safely cast `<column>` "TIMESTAMP" to "<TYPE>".`:

- beside an untyped NULL, unchanged: `The value was narrowed from nanoseconds to microseconds
  before the store; give the NULL beside it the type <type>. SQLSTATE: KD000`
- beside a typed NULL or a microsecond value (Q2, Q5): `The value <c> was narrowed from
  nanoseconds to microseconds before the store, to match the microsecond value beside it;
  write CAST(<c> AS TIMESTAMP) if microseconds are intended, or give the value beside it a
  nanosecond type. SQLSTATE: KD000`

`<c>` is the narrowed expression as the plan names it (`ice.ns.src.ns`, `c`, `i.ns`).

### 11.3 Q1, arm by arm

Measured on the base through `INSERT … SELECT`, five zones, both targets, with the five
moments of the door file, which include `1969-12-31 23:59:58.999999999` (one nanosecond
below a second boundary, before the epoch) and `1969-12-31 23:59:59.000000001`. Each arm was
written over `coalesce(c, NULL)` and over `c`, and the two stored columns compared.

| Written call over the narrowed value | Zoned source (`timestamptz_ns`) | Naive source (`timestamp_ns`) |
|---|---|---|
| `CAST(… AS TIMESTAMP)` | equal: stores | equal: stores |
| `date_trunc(unit, …)`, ten units from `microsecond` to `year` | equal: stores | differs: refuses |
| `CAST(… AS DATE)` | differs: refuses | equal: stores |
| `CAST(… AS TIMESTAMP_NTZ)`, `TRY_CAST(… AS TIMESTAMP_NTZ)` | differs: refuses | differs: refuses |
| `CAST(… AS timestamp_ns)`, `CAST(… AS timestamptz_ns)` | differs: refuses | differs: refuses |
| `TRY_CAST(… AS TIMESTAMP)` | differs: refuses | differs: refuses |

*Corrected 2026-10-11 (fold 2, §12.2): the cut is not the same for every sibling. Beside an
untyped NULL, a `TIMESTAMP` NULL or a `TIMESTAMP` value it floors; beside a `TIMESTAMP_NTZ`
sibling it truncates toward zero, and there `CAST(… AS TIMESTAMP)` and `CAST(… AS DATE)`
over a naive value differ from the call over the nanosecond value and refuse. The table
above and the reasons below were measured with an untyped NULL, a `TIMESTAMP` NULL and a
`TIMESTAMP` literal only.*

Why each refusing arm differs:

- **`date_trunc` over a naive value.** The analyzer's cut reads the naive value as a wall in
  the session zone and makes an instant of it; `date_trunc` then floors that instant in the
  session zone. `date_trunc` over the nanosecond value itself truncates the naive value
  toward zero, so before the epoch it lands one unit later, and a wall inside a
  daylight-saving gap is moved by the cut and not by the call. `week` was equal on the
  measured moments and refuses with the other nine units: one rule per function, and no
  measurement shows it equal on every moment.
- **`CAST(… AS DATE)` over a zoned value.** The cut makes a microsecond instant by dividing
  toward zero; the date of a pre-epoch instant one nanosecond below midnight UTC differs from
  the date of the nanosecond value.
- **`CAST(… AS TIMESTAMP_NTZ)`.** The call over the nanosecond value keeps nine digits; over
  the narrowed value it stores six.
- **`CAST(… AS timestamp_ns)` and `CAST(… AS timestamptz_ns)`.** The call widens back and
  stores the cut value with three zeros; over the nanosecond value it stores nine digits.
- **`TRY_CAST(… AS TIMESTAMP)`.** Over a nanosecond value it keeps the nanosecond type and
  nine digits; over the narrowed value it stores six.

The guard encodes the table: a cast to an instant type over a value that is not itself a mark
stops the walk (`CAST(… AS TIMESTAMP)`); `date_trunc` forgives a zoned mark below it;
`__repark_timestamp_to_date__` and a cast to `Date32` forgive a naive mark. Every other call
is walked through.

Pin: `a_written_call_over_a_narrowed_value_stores_what_it_stores_over_the_nanosecond_value`
(`crates/repark-spark/tests/timestamp_ns_narrow_refuse.rs`), five zones, both sources, both
targets, fifteen arms over three narrowed spellings (an untyped NULL, a typed NULL, a
microsecond literal): an equal arm must store the column the same call stores over the
nanosecond value; a differing arm must refuse by name and leave no data file.

### 11.4 Q2 and Q5: what refuses now

Both are the same site: coercion left a `Cast` from the nanosecond value to the microsecond
type of its sibling, and that cast is marked (§11.2). A NULL typed `TIMESTAMP` or
`TIMESTAMP_NTZ` is a microsecond sibling like any other. Measured spellings, each on the 64
doors, five zones, both sources, both nanosecond targets:

| Spelling | Arm |
|---|---|
| `coalesce(c, CAST(NULL AS TIMESTAMP))`, `coalesce(c, CAST(NULL AS TIMESTAMP_NTZ))` | Q2 |
| `CASE WHEN id = 1 THEN c ELSE TIMESTAMP '…' END`, `coalesce(c, CAST(c AS TIMESTAMP))`, `greatest(c, TIMESTAMP '…')`, `array(c, TIMESTAMP '…')[0]` | Q5 |
| `SELECT c … UNION ALL SELECT <a microsecond value> …` (the door `union_insert` under every spelling the statement types as microseconds) | Q5, the plain nanosecond branch is the narrowed one |

Pins: `every_door_refuses_a_value_narrowed_beside_a_typed_null_or_a_microsecond_value` and
`every_door_refuses_a_value_narrowed_beside_a_microsecond_literal` (40 doors, both sources,
both targets, the files counted and the table read back after each refusal);
`a_union_with_an_untyped_null_branch_refuses_and_a_typed_one_stores`; the facade rows of the
six `BESIDE_VALUE` spellings.

**One consequence that touches the Q3 ruling, for a ruling (question Q7).** The `UNION`
cell the Q3 ruling names (`SELECT c … UNION ALL SELECT nvl(c, NULL) …`, and `ifnull`)
refuses on this head: 40 cells (two spellings, two sources, two targets, five zones). The
measurement corrected the premise of Q3: the cut there is not the string's six digits. The
`UNION` is typed as a microsecond timestamp, and coercion narrows the plain nanosecond
branch to match, which is the Q5 shape. The union rule refuses it without a line that names
`nvl`. The registry row the ruling asks for is filed (R-019) and says so. Keeping the cell
storing its cut value, as the letter of the ruling reads, needs an exception for a `UNION`
branch whose sibling was a string; it is not built.

### 11.5 The matrix, head against base

Base `8d1c4f49`, head `60687f7e`. 64 doors, five zones, two sources, four targets, 41
spellings: 104,960 cells (the 89,600 of §4 and six spellings added for the rulings, recorded
on the same build of the base: `python/repark/tests/ice_tsns_narrow_refuse_1_base.json`).
"The 59,400" is the Step 0 matrix of §1.1; "added" is every cell recorded since. Every value
is read from the Parquet files.

| | The 59,400 | Added | All |
|---|---|---|---|
| Cells | 59,400 | 45,560 | 104,960 |
| Byte-identical to the base | 48,615 | 32,600 | 81,215 |
| Moved into the refusal | 10,725 | 12,800 | 23,525 |
| Refused on the base and on the head, text changed | 60 | 160 | 220 |
| Moved any other way | 0 | 0 | 0 |
| Right-value cells (nine digits on the base) that moved | 0 | 0 | 0 |
| Cells of a microsecond target that moved (60 doors that write the target) | 0 | 0 | 0 |
| Refused cells that left a value on disk | 0 | 0 | 0 |
| Cells that store the nanosecond value and did not on the base | 0 | 0 | 0 |

Refused on the head, by arm (in brackets: of those, not refused on the base):

| Arm | The 59,400 | Added | All |
|---|---|---|---|
| Beside an untyped NULL | 6,945 (6,465) | 7,295 (6,995) | 14,240 (13,460) |
| Beside a typed NULL (Q2) | 1,865 (1,775) | 360 (360) | 2,225 (2,135) |
| Beside a microsecond value that is not NULL (Q5) | 160 (160) | 4,670 (4,430) | 4,830 (4,590) |
| A written call over a narrowed value whose result differs (Q1) | 2,505 (2,325) | 1,135 (1,015) | 3,640 (3,340) |
| Total | 11,475 (10,725) | 13,460 (12,800) | 24,935 (23,525) |

The Q5 row holds 170 cells of the door `union_insert`: 130 where the plain nanosecond branch
sits beside a microsecond value the statement wrote, and the 40 `nvl` and `ifnull` cells of
§11.4. The 220 cells refused on both sides are the unfiltered `UPDATE`, whose text now names
the narrowed value where the mark is the beside-a-value one.

Still cut on the head (a value floored to microseconds in a column of a nanosecond row):

| Owner | The 59,400 | Added | All |
|---|---|---|---|
| A narrowing the statement wrote (`CAST`, `date_trunc`; stores by the owner rule) | 5,060 | 1,990 | 7,050 |
| The same, through a cached frame | 130 | 50 | 180 |
| A written call over a narrowed value, equal arm (stores by Q1) | 410 | 1,550 | 1,960 |
| Q4: a cached frame whose uncached write refuses | 230 | 190 | 420 |
| Q3 | 0 | 0 | 0 |
| Q5 | 0 | 0 | 0 |
| Q6 (keeps nine digits, not cut: listed in §11.7) | 60 | 0 | 60 |
| Not owned by Q3 to Q6: the four doors that create the table (`ctas`, `rtas`, `df_create_or_replace`, `df_save_overwrite`) type the new column as microseconds, so no nanosecond column is written | 1,632 | 944 | 2,576 |

The last row is the typing of ICE-TSNS-COERCION-1: the created table has a `TIMESTAMP`
column. Where the created column would be a nanosecond one (`array(c, NULL)[0]` and the like)
the door refuses (`create_table_as_refuses_a_narrowed_value_it_would_type_as_nanoseconds`).

**Against the head before the rulings.** 80 cells that refused by name at `91f4e2f4` raise
the base's own error again, and store nothing on either: `EXPLAIN ANALYZE UPDATE` of a
zoned value into a `timestamp_ns` column (70 cells, fourteen spellings), `EXPLAIN ANALYZE
UPDATE` of `date_trunc('second', coalesce(ns, NULL))` (5 cells) and an `UPDATE` from a
scalar subquery over `CAST(coalesce(tzns, NULL) AS timestamptz_ns)` (5 cells). The
planner's cast of the `SET` value to the target is the node a written `CAST(… AS TIMESTAMP)`
is, and on an `UPDATE` the guard does not look through it (§11.2). The base raises an
internal error for the first (`cannot convert text Timestamp(µs, "UTC") into byte
Timestamp(ns)`, from the table provider) and a schema error for the other two. The 280
cells of `date_trunc('second', coalesce(tzns, NULL))` that refused there follow Q1: 245
store again and 35 raise the error the base raises.

### 11.6 Q4: the cache site and the count

`cache()` and `persist()` reach `materialize_dataframe_as_cache_view`, which calls
`register_collected_memtable` (`crates/repark-core/src/session/temp_views.rs`): the frame is
collected and the batches registered as a `MemTable`. The cast coercion inserted is part of
the frame's plan and is evaluated by that collect, so the cached column is a microsecond
instant with no expression behind it. A plain `timestamp_ns` column survives the cache with
nine digits; the cache itself cuts nothing.

Of the 300 cells of §5.3 (`cached_frame_append`, fifteen spellings, both targets, both
sources, five zones), **290 are the cache alone**: the same frame appended without the cache
refuses on this head. The other 10 are `date_trunc('second', coalesce(tzns, NULL))`, which
stores uncached too since Q1. Over the whole matrix the cache alone accounts for 420 cells:
those 290, 120 of the six Q2 and Q5 spellings, and 10 of
`CAST(coalesce(tzns, NULL) AS DATE)`. Nothing is fixed here; card ICE-TSNS-CACHE-1.

### 11.7 Q6: the cells

`UPDATE … SET v = CAST(c AS TIMESTAMP)` with no `WHERE` keeps nine digits on three doors
(`update_nowhere`, `update_nowhere_mor`, `branch_update`) in five zones: 15 cells per target
for each source type, 60 in all, on the base and on the head. Card ICE-TSNS-UPDATE-CAST-1.

### 11.8 Mutants of the new rules

Each mutant is one edit of the source at `60687f7e`, run against the named pins; the
source is restored after each.

| Mutant | File | Killed by |
|---|---|---|
| F1 a cast coercion left over a nanosecond value is not marked | `null_narrowing.rs` | `null_narrowing/tests.rs`: `a_cast_coercion_left_over_a_nanosecond_value_is_marked` |
| F2 a coerced branch is marked as beside an untyped NULL | `null_narrowing.rs` | `null_narrowing/tests.rs`: `a_cast_coercion_left_over_a_nanosecond_value_is_marked` |
| F3 a cast a function's signature asked for is marked too | `null_narrowing.rs` | `null_narrowing/tests.rs`: `a_cast_coercion_left_over_a_nanosecond_value_is_marked` |
| F4 a CASE branch coercion narrowed is not marked | `null_narrowing.rs` | `null_narrowing/tests.rs`: `a_cast_coercion_left_over_a_nanosecond_value_is_marked` |
| F5 a function argument coercion narrowed beside a microsecond value is not marked | `null_narrowing.rs` | `null_narrowing/tests.rs`: `a_cast_coercion_left_over_a_nanosecond_value_is_marked` |
| F6 a written CAST AS TIMESTAMP over a narrowed value is not kept | `null_narrowing.rs` | the Rust door file: `a_written_call_over_a_narrowed_value_stores_what_it_stores_over_the_nanosecond_value` |
| F7 a cast written through the DataFrame is not read as the statement's | `null_narrowing.rs` | `null_narrowing/tests.rs`: `a_cast_from_nanoseconds_to_an_instant_before_coercion_is_the_statements` |
| F8 the pass after coercion reads a coerced cast as written | `java_double.rs` | the Rust door file: `every_door_refuses_a_value_narrowed_beside_a_typed_null_or_a_microsecond_value` |
| F9 the root of a branch of a UNION is not marked | `instant_ts.rs` | `null_narrowing/tests.rs`: `the_session_rule_marks_a_cast_coercion_left_at_the_root_of_a_projection` |
| F10 a nanosecond branch beside an untyped NULL takes the other mark | `null_narrowing.rs` | `null_narrowing/tests.rs`: `the_nanosecond_branch_beside_a_widened_untyped_null_is_marked_as_beside_a_null` |
| F11 a UNION branch beside a microsecond branch is not marked | `null_narrowing.rs` | `null_narrowing/tests.rs`: `the_nanosecond_branch_of_a_union_beside_a_microsecond_branch_is_marked`, `the_nanosecond_branch_of_a_union_beside_an_untyped_null_branch_is_marked` |
| W1 date_trunc forgives a naive value too | `narrowed_store.rs` | `narrowed_store/tests.rs`: `a_written_call_stores_only_where_it_equals_the_call_over_the_nanosecond_value` |
| W2 a DATE cast forgives a zoned value too | `narrowed_store.rs` | `narrowed_store/tests.rs`: `a_written_call_stores_only_where_it_equals_the_call_over_the_nanosecond_value` |
| W3 no written call forgives | `narrowed_store.rs` | `narrowed_store/tests.rs`: `a_written_call_stores_only_where_it_equals_the_call_over_the_nanosecond_value` |
| W4 a written cast to an instant is walked through | `narrowed_store.rs` | `narrowed_store/tests.rs`: `a_written_call_stores_only_where_it_equals_the_call_over_the_nanosecond_value` |
| W5 a written CAST AS TIMESTAMP is walked through | `narrowed_store.rs` | `narrowed_store/tests.rs`: `a_written_call_stores_only_where_it_equals_the_call_over_the_nanosecond_value`, `only_a_nanosecond_target_refuses_and_the_text_names_the_column_and_the_value` |
| W6 a UNION names the first narrowed branch | `narrowed_store.rs` | `narrowed_store/tests.rs`: `a_narrowed_value_is_followed_into_a_view_the_scan_holds_and_a_union_branch` |
| W7 the mark beside a value is read as beside an untyped NULL | `narrowed_store.rs` | `narrowed_store/tests.rs`: `a_narrowed_value_is_followed_into_a_view_the_scan_holds_and_a_union_branch`, `only_a_nanosecond_target_refuses_and_the_text_names_the_column_and_the_value` |
| W8 the text does not name the narrowed value | `narrowed_store.rs` | `narrowed_store/tests.rs`: `a_cast_over_a_marked_value_is_followed_through_every_carrying_position_and_no_other`, `only_a_nanosecond_target_refuses_and_the_text_names_the_column_and_the_value` |
| W9 an INSERT plan is read without looking through the conforming cast | `insert_timestamp_ns.rs` | the Rust door file: `every_door_refuses_a_value_narrowed_beside_an_untyped_null_in_utc` |
| W10 the conforming cast is never looked through | `narrowed_store.rs` | `narrowed_store/tests.rs`: `only_a_nanosecond_target_refuses_and_the_text_names_the_column_and_the_value` |

21 mutants, 21 killed. Two needed a second run. F9 survived the whole Rust door file: no door
statement reaches the root mark alone, because `settle_union_branches` marks the branches of
a `UNION` as well. The rule is pinned where it lives, by a unit pin of the session rule
added for it (commit `6a4f6f5a`), which kills the mutant. W10 was first written as an edit that did
not compile and was rewritten.

### 11.9 Cost

**The code.** No rule is added to the analyzer. The rule before coercion replaces the
first float-stringify instance and runs the same walk. In the session rule the walk of each
expression goes down and up once where it went up once: on the way down a node that is a
`CASE` or a function call is tested for a cast to a coarser timestamp among its branches,
and types a branch only when it finds one. `holds_nanoseconds` reads the field types of the
input schema once per plan node. The store walk is still behind `nanosecond_target`. One
avoidable cost is left in: `mark_coerced_narrowing` clones a root `Cast` to test it.

**The timing.** Release builds of the base `8d1c4f49` and of the head `60687f7e`, the script
and the method of §9 (a 2,000-row table with no nanosecond column; medians of fifteen
statements in milliseconds; five runs, the builds interleaved, four cores outside the build
set), on a shared machine with a build of another lane and one of this unit running.

| Run | INSERT base | INSERT head | MERGE base | MERGE head | MERGE merge-on-read base | head | `EXPLAIN`, fifty-branch union, base | head |
|---|---|---|---|---|---|---|---|---|
| 1 | 23.11 | 23.41 | 65.81 | 65.60 | 84.90 | 84.75 | 343.15 | 338.79 |
| 2 | 23.68 | 24.55 | 65.24 | 68.29 | 84.57 | 92.22 | 342.96 | 344.93 |
| 3 | 23.76 | 24.07 | 66.97 | 71.56 | 89.47 | 95.20 | 343.86 | 347.37 |
| 4 | 26.28 | 24.54 | 70.97 | 71.89 | 91.59 | 96.03 | 346.39 | 345.45 |
| 5 | 25.78 | 25.86 | 73.40 | 72.98 | 92.46 | 92.22 | 345.93 | 342.43 |
| median | 23.76 | 24.54 | 66.97 | 71.56 | 89.47 | 92.22 | 343.86 | 344.93 |

The head's medians are higher by 3.3%, 6.9%, 3.1% and 0.3%. One build's own runs differ by
14%, 13% and 9% on the three writes and by 1% to 2.5% on the plan. The plan, which is the
analyzer alone and the steadiest figure, does not move. The writes do not show a difference
this sample can separate from the machine: runs 1 and 5 are equal, runs 2 and 3 have the
head slower on MERGE by 5% to 9%. It is not shown that the writes cost the same; it is
shown that planning does.

### 11.10 Gates on the fold

Run 2026-10-10 on the fold's final source (product code of `60687f7e`, the test of
`6a4f6f5a`), one cargo command at a time under the build lock on cores 32-47; the document
gates on the tree of the commit that records this section.

| Command | Exit | Result |
|---|---|---|
| `cargo fmt --all -- --check` | 0 | |
| `make rust-clippy` | 0 | |
| `make rust-panic-ban` | 0 | |
| `cargo test --locked -p repark-functions --lib` | 0 | 922 passed, 1 ignored |
| `cargo test --locked -p repark-iceberg --lib` | 0 | 1005 passed |
| `cargo test --locked -p repark-spark --lib` | 0 | 2692 passed, 5 ignored |
| `cargo test --locked -p repark-sql --lib` | 0 | 394 passed |
| `cargo test --locked -p repark-core --lib` | 0 | 1420 passed, 1 ignored |
| `cargo test --locked -p repark-spark --test timestamp_ns_narrow_refuse` | 0 | 16 passed |
| `cargo test --locked -p repark-spark --test timestamp_ns_nested_shapes --test timestamp_ns_wall_doors --test timestamptz_ns_wall_doors` | 0 | 6 + 7 + 4 passed |
| `make develop` | 0 | |
| `pytest tests/test_ice_tsns_narrow_refuse_1.py tests/test_ice_tstzns_wall_1.py tests/test_ice_tsns_merge_wall_1.py tests/test_ice_tsns_sql_1.py -q -n 8` | 0 | 759 passed |
| `pytest tests -q -n 12 -k "iceberg or v3 or merge or timestamp or nested or struct or union or lambda or transform or coalesce or null or higher or float or double"` | 0 | 5379 passed, 214 skipped, 16 xfailed |
| the recorder on the head, 104,960 cells, counted against the base fixture | 0 | §11.5; run once on the final product source |
| `python3 comment_ban.py <clone> origin/main HEAD` | 0 | hits=0 |
| `ruff check .` and `ruff format --check .` | 0 | |
| `make check-rust-file-size`, `make check-lib-rs`, `make check-crate-dag`, `make check-manifest` | 0 | `instant_ts.rs` 998 lines, `java_double.rs` 989, ceiling 1000 |
| `make check-map-md BASE=origin/main`, `make check-map-sync`, `make check-ledger-grammar`, `make check-ledgers`, `make check-docs-links`, `make check-docs-compaction`, `typos` | 0 | |

The first run of the two `pytest` rows wrote no log (the gate script changed directory before
it opened the log file) and was repeated; the figures are the repeat's. The nested pins are
untouched and green: `timestamp_ns_nested_shapes` (6 tests) and the R-007 facade file.

Not re-run for the fold: the 31 mutants of §8 (several of their anchors are gone with the
code the fold replaced) and the Spark measurement of §6.

## 12. Fold 2 (2026-10-11): the guard's rule, redesigned

The verifier's verdict on head `45260e44` was FAIL: 9 S1, 5 S2, 3 S3. The defect is one rule,
not nine bugs: the store refused when a cast to a coarser *timestamp* sat directly on a marked
value, and read a plan node it did not name as "the whole subtree is narrowed". This section
is the design, written before the code (step 0 of the fold). Sections 2 to 11 describe the unit
before it; where they differ, this section holds.

### 12.1 The rule

**The mark (analyzer, `repark-functions`).** A mark wraps a nanosecond value, or a list, struct
or map that holds one, at each place the analyzer narrows it and nowhere else:

- *M1, a cast coercion left.* A `Cast` on a branch of a `CASE`, or on an argument of a function
  beside a sibling that holds a coarser timestamp, when the cast takes a nanosecond leaf of the
  source type to a coarser leaf at any depth, or to the zoneless nanosecond wire type the
  session rule reads as `TIMESTAMP`.
- *M2, a narrowing about to happen.* After the session rule has rewritten a node's branches: a
  branch that holds nanoseconds, in a `CASE` or a function with a sibling that holds a coarser
  timestamp, when the node's result type after coercion holds a coarser timestamp and no
  nanosecond one. The result type is asked from the type-coercion rewriter itself, not guessed
  from the siblings, so `named_struct('f', ns, 'g', ts)` and `map(ts, ns)` are not marked and
  a function that unifies inside its kernel (`array_append(array(ns), ts)`) is.
- *M3, a `UNION` column.* The nanosecond branch beside a coarser branch or an untyped NULL
  branch, as in fold 1.

A mark is placed only in a node that produces a column (a projection, an aggregate, a window, a
`VALUES`, a `UNION` branch). A filter, a join condition and a sort key feed no stored column and
are left as main has them. The test that skips a node with no nanosecond value no longer trusts
the node's input schema alone: the expression is searched for a nanosecond producer (an outer
reference, a lambda variable, a nanosecond literal or cast).

**Written or inserted (the site where the DataFrame door already tells them apart).**
`before_coercion` runs before the first type coercion, so every cast it sees is the
statement's. It already turns the DataFrame's `cast("timestamp")` over a nanosecond value into
the written-narrowing function. It now does the same for the SQL spelling, a cast of a
nanosecond value to zoneless `Timestamp(ns)`. After it, a cast from nanoseconds to a coarser
timestamp is the analyzer's by construction, and M1 never meets a written one. A written
`CAST(v AS TIMESTAMP)` over a value that is already microseconds is a cast the session rule
drops; it is kept, as the written-narrowing function, when a mark is in its subtree or in the
plan below the node (ruling V1).

**The store (`repark-iceberg`).** For each top-level nanosecond target column the guard follows
the lineage of the stored value and nothing else:

- *S1, per stored column, through every plan node.* A projection, an aggregate with or without
  grouping sets, a window, a `UNION`, a join, a lateral subquery (an outer reference is
  followed into the plan it names), a `DISTINCT`, a `VALUES`, an unnest, a recursive query, a
  scan that holds a view. A node the walk has no rule for is followed by column name into its
  inputs. No node is read as a subtree.
- *S2, through every argument that can carry the value, whatever its type.* A string, an
  integer and a JSON text carry it (ruling V2). Not followed: the condition of a `CASE`, an
  index or key that selects (`a[i]`, `element_at(m, k)`), the filter, ordering and partitioning
  of an aggregate or a window, and a call whose result is a boolean.
- *S3, a mark on the lineage refuses.* The guard does not ask what sits on the mark.
- *S4, unless the nearest written call above it forgives it.* Walking down, the guard remembers
  the last call it passed and forgets it at any node that is not transparent (transparent: an
  alias, a column, a plan boundary, a `CASE` branch, `coalesce`, the analyzer's own cast, the
  store's widening cast). At the mark it reads how the analyzer cut the value from the node on
  the mark, and the table of §12.2 answers. A call above another call changes nothing: the
  innermost one decides, which is ruling 1's test (the same expression over the nanosecond
  value) read from the inside out.

Why there is no "directly under": S3 refuses on the mark alone, so a cast to a list, a function
that narrows in its kernel and a value that leaves through a string are all the same case.
What sits on the mark is read only to choose a forgiveness, never to decide whether there is a
narrowing. Why there is no subtree fallback: S1 gives every node a per-column answer, and the
default is by column name.

### 12.2 What forgives (ruling 1, measured by the verifier on main, five zones, 18 moments)

| Written call nearest the mark | A zoned value cut to an instant | A naive value cut to an instant | A naive value cut to a naive microsecond value (NTZ sibling) | A container cut, or no cast on the mark |
|---|---|---|---|---|
| `CAST(… AS TIMESTAMP)`, `::timestamp`, the DataFrame cast | stores | stores | **refuses** | refuses |
| `date_trunc(unit, …)` | stores | refuses | refuses | refuses |
| `CAST(… AS DATE)`, `to_date(…)`, `date(…)` | refuses | stores | **refuses** | refuses |
| anything else | refuses | refuses | refuses | refuses |

Corrections to §11.3. The analyzer's cut to an instant (beside an untyped NULL, a `TIMESTAMP`
NULL, a `TIMESTAMP` value) floors: `1969-12-31 23:59:58.999999999` becomes `…58.999999`. Only
the cut beside a `TIMESTAMP_NTZ` sibling, a plain Arrow cast, truncates toward zero and gives
`…59.000000`. §11.3 said the cut divides toward zero for every sibling and, from that, called
`CAST(… AS TIMESTAMP)` and `CAST(… AS DATE)` equal for a naive value whatever the sibling; the
two bold cells are the verifier's measurement and refuse. `date_trunc` over a naive value cut
beside an NTZ sibling measured equal; it keeps refusing (one rule per function, as `week` does).

### 12.3 Which sentence closes which finding

| # | Finding (verdict) | Closed by |
|---|---|---|
| 1 | S1 never worse: `coalesce(CAST(ns AS TIMESTAMP), ts)` refuses | "Written or inserted": the SQL cast is read before coercion, M1 does not mark over it |
| 2 | S1 never worse: a plain column refuses under ROLLUP, CUBE, LATERAL | S1: grouping sets and the lateral subquery have a per-column rule; no subtree fallback |
| 3 | S1 hole: `UPDATE … SET u.v = …` | Not the rule: the `UPDATE` probe matched the `SET` column by its whole name. It matches the last part; the guard is unchanged |
| 4 | S1 hole: container coercions | M1 "at any depth", M2 "unifies inside its kernel"; S3 |
| 5 | S1 hole: a zoned value beside a DATE | M1 "or to the zoneless nanosecond wire type": coercion casts the zoned value there and the session rule cut it unmarked |
| 6 | S1 hole: LATERAL beside a typed NULL or a literal; a lambda in `INSERT VALUES` | The skip test searches the expression (an outer reference, a lambda variable), M2 |
| 7 | S1 equality: NTZ sibling under `CAST AS TIMESTAMP`, `CAST AS DATE` | §12.2, third column |
| 8 | S1 equality: a written cast above a refusing arm | S4: the innermost call decides; a call that is not transparent forgets |
| 9 | S1 hole: STRING, BIGINT, JSON round trip | S2 "whatever its type" (V2) |
| 10 | S2: a written cast over a subquery, view or frame column | "Written or inserted", last sentence (V1) |
| 11 | S2: refusal texts | §12.4 |
| 12 | S2: the tag in a read schema, the lost `PARQUET:field_id` | §12.5 |
| 13 | S2: `to_date`, `date` | §12.2, third row |
| 14 | S2: mutant N12 | The fallback is gone; S1's grouping-set and lateral rules each take a pin that holds a plain column stored beside a narrowing |
| S3 | marker names in `EXPLAIN VERBOSE` and an internal error; planning cost; §11.3 | §12.5; §12.2 |

### 12.4 The text

The value is named as the statement spells it: the expression under the mark, printed with the
marks and the narrowing function removed, Arrow types and interval debug text replaced by their
SQL spelling, a lambda variable followed to the array it ranges over. MERGE names its table.
The remedy is the written cast over that text, or a nanosecond type for the sibling.

### 12.5 What a read can observe

A frame or a view may be written later, and the written and the inserted narrowing differ only
during analysis, so an analyzed plan has to carry the mark (§2, unchanged). It is kept out of
what a read shows:

- *Schemas.* The mark returns its argument's field (name, nullability, metadata), so a column's
  `PARQUET:field_id` survives a `UNION`. The untyped-NULL tag lives between two passes of one
  analysis; the `UNION` schema is rebuilt without it.
- *Values, the optimized plan, plain `EXPLAIN`, `explain(True)`.* The optimizer removes the mark
  (unchanged).
- *Error text.* No mark is placed in a join condition, a filter or a sort key, so an error that
  prints one prints main's text.
- *`EXPLAIN VERBOSE`.* It prints the analyzed plan. The marks are erased from the printed text
  at the `EXPLAIN` door.
- *Cost.* A node's branch types are read once and shared by M1 and M2. Measured after the build
  (§12.7); above 1.05 of main is a finding to fix.

### 12.6 What the rule deliberately does not catch

- A value materialised before the write (`cache()`, `persist()`, an Arrow round trip): Q4, card
  ICE-TSNS-CACHE-1.
- A value that reaches the column only through a boolean, a `CASE` condition or a selector.
- A function with one nanosecond argument and no microsecond sibling that narrows in its kernel
  (`date_trunc('second', ns)`, `to_timestamp(ns)`): the statement wrote that call.
- `nvl` and `ifnull`, typed `STRING` (Q3, card ICE-TSNS-COERCION-1); the `UNION` cell refuses
  (Q7 stands).
- A created table whose column the query types as microseconds (§11.5, the coercion card).
- The 80 base-error cells of §11.5 (Q8 stands).
- A nested nanosecond leaf (R-015 refuses it first).

It catches more than ruling 1 asks in one place, by choice: a written call over a narrowed
value with any other call between them (`CAST(coalesce(ns, ts) + INTERVAL '0' SECOND AS
TIMESTAMP)`) refuses although the value may be equal. S4 has no proof of equality through an
arbitrary call; the cell stored a cut value on main, so never worse holds.

### 12.7 Seams

No crate edge is added. The mark stays in `repark-functions` (where the unit's first commit
put it), the walk in `repark-iceberg`, the doors in `repark-spark`. No microsecond-target cell
is meant to move; the matrix of step 4 is the proof.

### 12.8 What the build changed in the sketch

The sketch of §12.1 to §12.7 was committed before the code (`87c38ec5`). Building it against
the matrices moved five of its sentences. Each is recorded here; where §12.1 to §12.6 differ,
this subsection holds.

- **A written cast is read before coercion only below the root of an expression.** The
  sketch read every SQL cast of a nanosecond value there. A cast at the root (also under an
  alias) is left for the session rule, as on main. Converted at the root, a
  `SELECT DISTINCT id, CAST(c AS TIMESTAMP), 0` that main fails with a DataFusion internal
  error stored the written value: 40 cells of the 104,960 moved from an error to a store
  (20 of them of a microsecond target), which the fold's acceptance does not allow. Below
  the root is the only place the cast needs to be read: there coercion wraps it while it
  still looks like nanoseconds. A `VALUES` cell's root is untouched for the same reason.
- **A `UNION` of a zoned branch and a `DATE` branch is marked before coercion.** The sketch
  marked the wire cast at the root of the branch. With root casts left as main has them, a
  root wire cast may be a written one, so `tag_union_nulls` marks the zoned branch itself
  when another branch is a date (`zoned_beside_a_date`). The mark then has no cast on it,
  which S3 refuses and S4 never forgives.
- **Two sources only look like nanoseconds, and the old "cast on the mark" test had hidden
  both.** (1) `TIMESTAMP '…'` and `CAST('…' AS TIMESTAMP)` are, until the session rule reads
  them, casts to the nanosecond wire type; beside a written cast, coercion casts them to
  microseconds and M1 marked the literal. (2) A `UNION` keeps the type it was planned with
  until the second coercion, so a union of three written casts read as nanoseconds from the
  union above it, and `settle_union_branches` marked its column. Each refused a written
  value main stores (`coalesce(CAST(c AS TIMESTAMP), TIMESTAMP '…')` in `INSERT … VALUES`;
  `… UNION ALL … UNION ALL …` of written casts). The verifier's doors suite found both on
  the fold's second build (72 cells). M1 now skips a source that is a wire cast or a
  literal, and a union node takes its branches' types before it is settled
  (`with_branch_types`). Pins: `a_union_of_written_casts_and_a_values_row_with_a_written_cast_store`
  and the unit pin `a_pending_timestamp_literal_and_a_stale_union_type_are_not_nanoseconds`.
- **The store's widening cast is not transparent** (S4). `CAST(CAST(coalesce(c, NULL) AS
  timestamp_ns) AS TIMESTAMP)` refuses: the cast back is a call like any other. The
  transparent nodes are an alias, a column, a plan boundary, a `CASE` branch, `coalesce` and
  the analyzer's own cast.
- **What a read shows (§12.5).** The mark answers its argument's name as well as its field,
  so a marked plan takes no alias. The tag changes a literal's printed name, so the pass
  before coercion aliases the expression to its own name; that alias is taken off when the
  tag is settled. The optimizer removes the written-narrowing function over a value that is
  already a microsecond instant. `EXPLAIN` erases the marks from the printed logical plans
  and writes `SAME TEXT AS ABOVE` by DataFusion's own rule. Compared byte for byte with main
  over 72 statements (36 reads and writes, plain and `VERBOSE`; §12.9): 69 are equal and the
  three that are not are the kept written cast below.
- **The mark rides the narrowing kernel where the analyzer's cut is that kernel.** The
  sketch kept one shape, a mark function under the cut. Measured on the fold's fifth build,
  that wrapper made the two mixed reads of §12.12 6% slower than main, and no line of the
  rule was the cost: every type and name lookup above a mark resolved one more function.
  The session rule now builds one node, the kernel with the mark as its alias; it prints,
  names and types as the written kernel, the optimizer turns it into the written kernel,
  and its `Debug` text is unchanged (an internal error of DataFusion prints it). The store
  reads both shapes; the wrapper remains under a plain cast, a cast to a naive type, a
  container cast and where there is no cast.
- **The text (§12.4).** A column of a relation the engine named itself (the frame behind
  `selectExpr`, `__repark_selx_…`) is printed without that name.

**V1 has one edge, by choice.** A written `CAST(v AS TIMESTAMP)` over a column of a view
whose plan the analyzer cannot read (a temporary view, a catalog view: the scan's
`TableType` is not `Base`) is kept as the written-narrowing function whether or not the view
holds a narrowing, because the analyzer cannot look. It is the identity over a microsecond
instant and the optimizer removes it, so values, plain `EXPLAIN` and the optimized plan are
main's; the analyzed-plan rows of `EXPLAIN VERBOSE` print
`__repark_narrow_timestamp_ns__(tv.ts)` where main prints `tv.ts`. The same function shows
at the root of an `EXPLAIN VERBOSE INSERT` whose supply holds a narrowing (the planner's
cast of the column, kept). No mark name is printed in either. Question V4.

**One producer the skip test does not know.** A node with no nanosecond column in its inputs
is searched for an outer reference, a lambda variable, a cast to a nanosecond type and a
function whose name holds `_ns` or `nanos`. A function of another name that returns
nanoseconds from no nanosecond argument, in such a node, beside a microsecond value that
coercion has already cast (M1), is not marked; beside an untyped NULL it is (the tag does not
depend on the test). No such function was found in the matrices.

### 12.9 The matrices on the final build

Final product source `d2717be5`, release build (`cargo build --locked --release -p
repark-python --features extension-module,allocator-mimalloc`). Main is `2c33f289`: the
verifier's release build of it (`native-main.so`) recorded the 104,960 cells again on
2026-10-11 and every cell equals the committed fixture, so the fixture is main.

**The 104,960-cell matrix** (64 doors, five zones, two sources, four targets, 41 spellings;
values read from the Parquet files), main against the final head, by destination:

| Moved | Cells |
|---|---|
| Byte-identical to main | 81,140 |
| A cut value on main, refused now (`cut` 14,808, `cut` with the wall moved 6,922) | 21,730 |
| An error or a partial store on main, refused now (`error` 1,570, `other` 300) | 1,870 |
| Refused on main and on the head, text changed (the unfiltered `UPDATE` names the value) | 220 |
| A cut value on main, the full nanosecond value now | 0 |
| Moved any other way | 0 |
| Cells of a microsecond target that moved (60 doors that write the target) | 0 |
| Refused cells that left a value on disk | 0 |

23,600 cells moved into the refusal, 75 more than fold 1's 23,525: 10 are the zoned value
beside a `DATE` branch of a `UNION` (`cast_date` on `union_insert`), 65 are cells that raised
the base's own error at fold 1 and refuse by name again (the `EXPLAIN ANALYZE UPDATE` rows of
question Q8: 65 of its 80 cells; the other 15 still raise the base's error and store
nothing). Against fold 1's head, 6,020 refusals changed their text (MERGE names its table)
and nothing else moved.

**The verifier's suites**, re-run from its directory (`verify1/probe`: `suites.py`,
`stmts.py`, `reads.py`, `pq.py`) on the final build, against its own results for main:

| Suite | Cells | Identical | Cut on main, refused | Error on main, refused | Never worse broken | Hole (cut, not refused, not written) |
|---|---|---|---|---|---|---|
| shapes (167 spellings, `INSERT … SELECT`, five zones) | 6,680 | 4,640 | 1,860 | 180 | 0 | 0 |
| doors (126 doors, 14 spellings, three zones) | 42,336 | 32,670 | 8,577 | 1,089 | 0 | 0 |
| stmts (statements and the DataFrame API, two zones) | 6,272 | 4,968 | 1,016 | 288 | 0 | 0 |
| equality (36 written arms over 8 spellings, five zones) | 5,760 | 3,150 | 2,610 | 0 | 0 | 0 |
| equality at the int64 limits (1677 and 2262, three zones) | 6,912 | 3,780 | 1,520 | 1,612 | 0 | 0 |

Reading the doors row. Its 8,577 holds 8,433 narrowed spellings, 96 cells of `array(c,
NULL)[0]` through the eight doors that create the table (the created column is a nanosecond
one whatever the row's target says, as at fold 1), 36 cells of a plain nanosecond branch of
a `UNION` narrowed beside a branch that writes a microsecond value (`cast_ts`,
`coalesce_of_cast_col` and `date_trunc_second` on `union_insert`, ruling Q5, as at fold 1)
and 12 of a `UNION` main stored in part. The verdict's 1,008 cells of
`coalesce(CAST(c AS TIMESTAMP), ts)` are identical to main, its 24 of a plain column under
`ROLLUP` and `LATERAL` are identical, the 192 `update_alias` cells refuse, and no door
stores a cut value for a narrowing spelling.

Against the verdict's table: shapes had 40 written cells refused and 300 cells stored cut
that nobody wrote (containers 120, a DATE sibling 40, the round trips 140): all 340 moved,
the 40 back to main's value and the 300 into the refusal. stmts had 96 full values refused,
80 written casts over a subquery refused and 64 errors whose text carried a mark: all 240
are identical to main now. In the equality suites no cell stores a value that differs from
the same written call over the nanosecond value: 1,870 store what the twin stores and 2,610
refuse (at the limits 990 and 3,132); the arms the verdict measured differing (the NTZ
sibling under `CAST AS TIMESTAMP` and `CAST AS DATE`, a cast above an arm that refuses)
refuse.

Across the suites the head raised 18,878 refusals: none left a value on disk and none
carries a mark, an Arrow type, an interval's debug text, a generated relation name or an
empty table name.

**Reads.** `pq.py` (490 queries written to Parquet, read through a temporary view and
collected): 0 differ from main, schemas and field ids included. `reads.py` (30 reads:
columns, schema, Arrow schema, rows, `EXPLAIN`, `printSchema`, `explain(True)`, error
texts) run on the verifier's eight cores: 0 differ once the run's own names are removed
(temporary directories, generated view names). `EXPLAIN` and `EXPLAIN VERBOSE` of 36
statements, logical-plan rows compared byte for byte with main: 69 of 72 equal; the three
that differ are the kept written cast of §12.8 (two over a temporary view's column, one at
the root of an `INSERT` whose supply holds a narrowing), in `EXPLAIN VERBOSE` only, and none
prints a mark.

**The facade.** `test_ice_tsns_narrow_refuse_1.py` (228 rows over 64 doors against the
fixture) and the fold's `test_ice_tsns_narrow_refuse_1_rule.py` (67), with the R-007 and
R-008 files: 826 passed on the final build.

### 12.10 The verdict's findings, one by one

| # | Sev | Finding | Disposition | Commit | Pin |
|---|---|---|---|---|---|
| 1 | S1 | A written SQL `CAST(c AS TIMESTAMP)` beside a microsecond value refuses | Closed: stores, on every door, as main | `03a6a5af`, `73fe665a` | `a_written_cast_beside_a_microsecond_value_stores_on_every_door`, `a_union_of_written_casts_and_a_values_row_with_a_written_cast_store`; facade `…stores_through_the_frame_writers` |
| 2 | S1 | A plain nine-digit column refuses under ROLLUP, CUBE, LATERAL | Closed: stores nine digits | `03a6a5af` | `a_plain_column_stores_beside_a_narrowing_under_rollup_cube_and_lateral`; unit `a_plain_column_beside_a_narrowing_has_its_own_lineage_under_every_plan_node` |
| 3 | S1 | `UPDATE … SET u.v = …` stores the cut value | Closed: refuses, both row-level modes | `3c05e806` | `an_update_through_a_qualified_set_column_refuses_in_both_modes` |
| 4 | S1 | A value narrowed inside a container coercion stores cut | Closed: refuses | `3c05e806` | `a_value_narrowed_inside_a_container_refuses_on_every_door` |
| 5 | S1 | A zoned value narrowed beside a DATE stores cut | Closed: refuses, the `UNION` branch included | `3c05e806`, `f3565531` | `a_value_narrowed_beside_a_date_refuses_on_every_door` |
| 6 | S1 | LATERAL beside a typed NULL or a literal, a lambda in `INSERT VALUES` | Closed: refuses | `3c05e806` | `a_value_narrowed_over_an_outer_reference_or_a_lambda_variable_refuses` |
| 7 | S1 | A naive value beside a `TIMESTAMP_NTZ` sibling under `CAST AS TIMESTAMP`, `CAST AS DATE` stores a differing value | Closed: refuses | `03a6a5af` | `a_written_call_does_not_forgive_a_cut_it_does_not_equal` |
| 8 | S1 | A written cast above a refusing arm forgives it | Closed: the innermost call decides | `03a6a5af` | the same pin; `the_innermost_written_call_over_a_narrowed_value_decides` |
| 9 | S1 | A value that passes through STRING, BIGINT or JSON text stores cut | Closed: refuses (V2) | `03a6a5af` | `a_narrowed_value_that_leaves_the_timestamp_type_and_returns_refuses` |
| 10 | S2 | A written cast over a subquery, view or frame column refuses | Closed: stores as the inline cast (V1); one edge in §12.8 | `3c05e806` | `a_written_cast_over_a_column_that_holds_the_narrowing_stores_as_the_inline_cast_does`; facade `…frame_column…stores` |
| 11 | S2 | Refusal texts carry marks, Arrow types, interval debug text, `x`, an empty table name | Closed: 0 of 18,878 refusals in the suites | `3c05e806`, `d2717be5` | `the_refusal_names_what_the_statement_wrote`; every refusing pin asserts the text holds no internal name |
| 12 | S2 | The tag in a read schema, the lost `PARQUET:field_id` | Closed: `pq.py` 0 of 490 differ | `3c05e806` | `a_read_shows_no_mark_no_tag_and_keeps_its_field_metadata`; facade `test_a_read_shows_no_mark_and_keeps_its_schema` |
| 13 | S2 | `to_date(x)`, `date(x)` refuse where `CAST(x AS DATE)` stores | Closed | `3c05e806` | `to_date_and_date_answer_as_a_date_cast_does` |
| 14 | S2 | Mutant N12 survives (the subtree fallback) | Closed: the fallback is gone; its replacements R1 to R4 are killed | `03a6a5af` | `a_narrowed_aggregate_under_grouping_sets_refuses_and_its_plain_sibling_stores` and the lineage unit pin |
| – | S3 | Mark names in `EXPLAIN VERBOSE` and an internal error | Closed: no mark printed; the join error's text is main's (stmts: 64 cells identical) | `3c89b012`, `124fd8e3`, `d2717be5` | the read pins; `explain_marks` unit pins |
| – | S3 | Planning a mixed read 7 to 11% slower | Closed: 1.002 and 1.017 (§12.12) | `09de15ce` | measured |
| – | S3 | §11.3 names the wrong cut | Corrected in §12.2 | `87c38ec5` | – |

### 12.11 Mutants

Each mutant is one edit of the product source at `09de15ce`, in a clone of the branch with a
build directory of its own, run against the named suites until one fails; the file is
restored and `git status` is clean after each. The list is fold 1's 21 (F1 to F11, W1 to
W10) re-anchored to the new code, the verifier's 24 (M and N), and 40 fresh ones in the new
rule (R1 to R40). 84 of 85 are killed; N12 has no anchor because the subtree
fallback it removed is gone (R1 to R4 are its replacements).

| Mutant | Killed by |
|---|---|
| F1 a cast coercion left over a nanosecond value is not marked | `a_cast_coercion_left_over_a_nanosecond_value_is_marked`, `a_cast_coercion_left_inside_a_container_or_to_the_wire_type_is_marked` |
| F2 a coerced branch is marked as beside an untyped NULL | `a_cast_coercion_left_over_a_nanosecond_value_is_marked`, `a_cast_coercion_left_inside_a_container_or_to_the_wire_type_is_marked` |
| F3 a cast a function's signature asked for is marked too | `a_cast_coercion_left_inside_a_container_or_to_the_wire_type_is_marked`, `a_cast_coercion_left_over_a_nanosecond_value_is_marked` |
| F4 a CASE branch coercion narrowed is not marked | `a_cast_coercion_left_over_a_nanosecond_value_is_marked` |
| F5 a function argument coercion narrowed beside a microsecond value is not marked | `a_cast_coercion_left_inside_a_container_or_to_the_wire_type_is_marked`, `a_cast_coercion_left_over_a_nanosecond_value_is_marked` |
| F6 a written CAST AS TIMESTAMP over a narrowed value is not kept | `a_written_call_over_a_narrowed_value_stores_what_it_stores_over_the_nanosecond_value` |
| F7 a cast written through the DataFrame is not read as the statement's | `a_cast_from_nanoseconds_to_an_instant_before_coercion_is_the_statements` |
| F8 the pass after coercion reads a coerced cast as written | `every_door_refuses_a_value_narrowed_beside_a_typed_null_or_a_microsecond_value` |
| F9 the root of a branch of a UNION is not marked | `the_session_rule_marks_a_cast_coercion_left_at_the_root_of_a_projection` |
| F10 a nanosecond branch beside an untyped NULL takes the other mark | `the_nanosecond_branch_beside_a_widened_untyped_null_is_marked_as_beside_a_null` |
| F11 a UNION branch beside a microsecond branch is not marked | `the_nanosecond_branch_of_a_union_beside_a_microsecond_branch_is_marked`, `the_nanosecond_branch_of_a_union_beside_an_untyped_null_branch_is_marked` |
| W1 date_trunc forgives a naive value too | `the_narrowing_kernel_that_carries_a_mark_is_the_analyzers_cut`, `a_written_call_stores_only_where_it_equals_the_call_over_the_nanosecond_value` |
| W2 a DATE cast forgives a zoned value too | `a_written_call_stores_only_where_it_equals_the_call_over_the_nanosecond_value` |
| W3 no written call forgives | `the_narrowing_kernel_that_carries_a_mark_is_the_analyzers_cut`, `a_written_call_stores_only_where_it_equals_the_call_over_the_nanosecond_value` |
| W4 a written cast to an instant is walked through | `a_written_call_stores_only_where_it_equals_the_call_over_the_nanosecond_value` |
| W5 a written CAST AS TIMESTAMP is walked through | `the_narrowing_kernel_that_carries_a_mark_is_the_analyzers_cut`, `a_written_call_stores_only_where_it_equals_the_call_over_the_nanosecond_value` |
| W6 a UNION names the first narrowed branch | `a_narrowed_value_is_followed_into_a_view_the_scan_holds_and_a_union_branch` |
| W7 the mark beside a value is read as beside an untyped NULL | `a_narrowed_value_is_followed_into_a_view_the_scan_holds_and_a_union_branch`, `only_a_nanosecond_target_refuses_and_the_text_names_the_column_and_the_value` |
| W8 the text does not name the narrowed value | `a_mark_refuses_through_every_argument_that_carries_the_value_and_no_other`, `the_narrowing_kernel_that_carries_a_mark_is_the_analyzers_cut` |
| W9 an INSERT plan is read without looking through the conforming cast | `every_door_refuses_a_value_narrowed_beside_an_untyped_null_in_utc` |
| W10 the conforming cast is never looked through | `only_a_nanosecond_target_refuses_and_the_text_names_the_column_and_the_value` |
| M1 an untyped NULL beside a nanosecond branch is not tagged | `a_settled_tag_leaves_no_alias_and_a_null_cast_is_not_read_as_written`, `an_untyped_null_beside_a_nanosecond_branch_is_tagged_and_no_other_null` |
| M5 the tag is left in the settled node | `a_branch_the_statement_narrowed_and_a_node_with_one_kind_are_not_marked`, `the_nanosecond_branch_beside_a_widened_untyped_null_is_marked_as_beside_a_null` |
| M6 the session rule does not settle a tag | `a_settled_tag_leaves_no_alias_and_a_null_cast_is_not_read_as_written`, `only_a_node_that_produces_a_column_is_marked_and_a_producer_is_found_without_a_scan` |
| M9 the optimizer keeps the wrapper | `the_marker_is_its_argument_and_the_optimizer_drops_it` |
| M10 the DML plan is not guarded | `every_door_refuses_a_value_narrowed_beside_an_untyped_null_in_utc` |
| M11 EXPLAIN ANALYZE is not looked into | `every_door_refuses_a_value_narrowed_beside_an_untyped_null_in_utc` |
| M12 PREPARE is not looked into | `every_door_refuses_a_value_narrowed_beside_an_untyped_null_in_utc` |
| M13 the overwrite doors are not guarded | `every_door_refuses_a_value_narrowed_beside_an_untyped_null_in_utc` |
| M14 the shared MERGE and UPDATE guard does not ask | `every_door_refuses_a_value_narrowed_beside_an_untyped_null_in_utc` |
| M16b CREATE TABLE AS is not guarded (the result of the guard is dropped) | `create_table_as_refuses_a_narrowed_value_it_would_type_as_nanoseconds` |
| M18 the right side of a join is not followed | `a_plain_column_beside_a_narrowing_has_its_own_lineage_under_every_plan_node` |
| M19 an aggregate is not followed | `a_plain_column_beside_a_narrowing_has_its_own_lineage_under_every_plan_node`, `a_narrowed_value_is_followed_through_the_plan_nodes_between_it_and_the_store` |
| M21 a scalar subquery is not followed | `every_door_refuses_a_value_narrowed_beside_an_untyped_null_in_utc` |
| M26 a filter is not followed | `a_plain_column_beside_a_narrowing_has_its_own_lineage_under_every_plan_node`, `a_narrowed_value_is_followed_through_the_plan_nodes_between_it_and_the_store` |
| M27 a microsecond target is walked too | `only_a_nanosecond_target_refuses_and_the_text_names_the_column_and_the_value` |
| N1 a coerced cast over a NULL literal is marked too | `a_cast_coercion_left_inside_a_container_or_to_the_wire_type_is_marked` |
| N2 the root mark does not look under an alias | `a_cast_coercion_left_over_a_nanosecond_value_is_marked`, `the_session_rule_marks_a_cast_coercion_left_at_the_root_of_a_projection` |
| N4 a sort is not followed | `a_narrowed_value_is_followed_through_the_plan_nodes_between_it_and_the_store` |
| N5 a DISTINCT is not followed | `a_narrowed_value_is_followed_through_the_plan_nodes_between_it_and_the_store` |
| N6 the ELSE branch of a CASE is not followed | `a_mark_refuses_through_every_argument_that_carries_the_value_and_no_other` |
| N7 the look-through does not read the written-narrowing function as a conforming cast | `only_a_nanosecond_target_refuses_and_the_text_names_the_column_and_the_value` |
| N8 a column behind the conforming cast is never walked | `only_a_nanosecond_target_refuses_and_the_text_names_the_column_and_the_value` |
| N11 only the first branch of a UNION is followed | `a_narrowed_value_is_followed_into_a_view_the_scan_holds_and_a_union_branch`, `a_narrowed_value_is_followed_through_the_plan_nodes_between_it_and_the_store` |
| N12 the whole-subtree fallback never refuses | no anchor: the code it changed is gone |
| R1 a grouping-set aggregate reads the grouping id as an aggregate | `a_plain_column_beside_a_narrowing_has_its_own_lineage_under_every_plan_node` |
| R2 a lateral subquery is not entered | `a_plain_column_beside_a_narrowing_has_its_own_lineage_under_every_plan_node` |
| R3 an outer reference is not followed | `a_plain_column_beside_a_narrowing_has_its_own_lineage_under_every_plan_node` |
| R4 the right side of a join has no outer scope | `a_plain_column_beside_a_narrowing_has_its_own_lineage_under_every_plan_node` |
| R5 a value that leaves through another type is not followed | `a_mark_refuses_through_every_argument_that_carries_the_value_and_no_other`, `a_written_call_stores_only_where_it_equals_the_call_over_the_nanosecond_value` |
| R6 a mark with no cast on it does not refuse | `a_mark_refuses_through_every_argument_that_carries_the_value_and_no_other` |
| R7 a naive cut is forgiven as an instant is | `a_written_call_stores_only_where_it_equals_the_call_over_the_nanosecond_value` |
| R8 a call that is not transparent keeps the call above it | `a_written_call_stores_only_where_it_equals_the_call_over_the_nanosecond_value` |
| R9 the key of a selecting call is followed | `a_mark_refuses_through_every_argument_that_carries_the_value_and_no_other` |
| R10 a call typed boolean is followed | `a_mark_refuses_through_every_argument_that_carries_the_value_and_no_other` |
| R11 the SQL cast is not read before coercion | `a_cast_to_the_sql_timestamp_type_below_the_root_is_read_before_coercion` |
| R12 a cast under an alias at the root is read before coercion too | `a_cast_to_the_sql_timestamp_type_below_the_root_is_read_before_coercion` |
| R13 the second mark does not ask for the result type | `a_branch_is_marked_only_when_the_node_loses_the_nanoseconds` |
| R14 a cast of a container is not a narrowing | `a_cast_coercion_left_inside_a_container_or_to_the_wire_type_is_marked` |
| R15 a cast of a zoned value to the wire type is not a narrowing | `a_cast_coercion_left_inside_a_container_or_to_the_wire_type_is_marked` |
| R16 a filter is marked too | `only_a_node_that_produces_a_column_is_marked_and_a_producer_is_found_without_a_scan` |
| R17 the producer search is skipped | `only_a_node_that_produces_a_column_is_marked_and_a_producer_is_found_without_a_scan` |
| R18 the union schema keeps the tag | `a_union_schema_loses_the_tag_and_keeps_its_other_metadata` |
| R19 the mark builds a field of its own | `the_mark_returns_the_field_of_its_argument` |
| R20 a kept cast stays with no mark below it | `a_written_cast_over_a_microsecond_column_is_kept_only_above_a_mark` |
| R21 a written cast over a column is not kept | `a_written_cast_over_a_microsecond_column_is_kept_only_above_a_mark` |
| R22 to_date and date are not a date cast | `to_date_and_date_answer_as_a_date_cast_does` |
| R23 the UPDATE probe skips a qualified SET column | `an_update_through_a_qualified_set_column_refuses_in_both_modes` |
| R24 MERGE does not name its table | `the_refusal_names_what_the_statement_wrote` |
| R25 EXPLAIN prints the marks | `a_read_shows_no_mark_no_tag_and_keeps_its_field_metadata` |
| R26 the analyzer's cast is printed in the text | `the_refusal_names_what_the_statement_wrote` |
| R27 the store reads no call as forgiving a zoned value under date_trunc | `a_written_call_stores_only_where_it_equals_the_call_over_the_nanosecond_value`, `the_narrowing_kernel_that_carries_a_mark_is_the_analyzers_cut` |
| R28 a zoned branch of a UNION beside a DATE branch is not marked | `a_zoned_branch_of_a_union_beside_a_date_branch_is_marked_before_coercion` |
| R29 the final plan row of EXPLAIN is collapsed too | `a_row_that_repeats_the_row_above_is_collapsed_and_the_final_plan_is_not` |
| R30 a settled tag leaves its alias | `a_settled_tag_leaves_no_alias_and_a_null_cast_is_not_read_as_written` |
| R31 the mark prints its own name in a column name | `the_mark_returns_the_field_of_its_argument`, `a_settled_tag_leaves_no_alias_and_a_null_cast_is_not_read_as_written` |
| R32 the optimizer keeps an idle narrowing | `the_mark_returns_the_field_of_its_argument` |
| R33 a cast over a pending TIMESTAMP literal is marked | `a_pending_timestamp_literal_and_a_stale_union_type_are_not_nanoseconds` |
| R34 a union keeps the type it was planned with | `a_pending_timestamp_literal_and_a_stale_union_type_are_not_nanoseconds` |
| R35 the kernel over a mark drops the mark | `the_narrowing_kernel_over_a_mark_carries_the_mark_and_prints_as_the_written_one`, `a_written_cast_over_a_microsecond_column_is_kept_only_above_a_mark` |
| R36 the store does not read the mark a kernel carries | `the_narrowing_kernel_that_carries_a_mark_is_the_analyzers_cut` |
| R37 the look-through takes a kernel that carries a mark for the planner's cast | `the_narrowing_kernel_that_carries_a_mark_is_the_analyzers_cut` |
| R38 a branch that is the kernel is typed by the schema as nanoseconds | `a_branch_is_typed_without_a_lookup_where_its_node_says_the_type`, `only_a_node_that_produces_a_column_is_marked_and_a_producer_is_found_without_a_scan` |
| R39 the optimizer keeps the kernel that carries a mark | `the_narrowing_kernel_over_a_mark_carries_the_mark_and_prints_as_the_written_one` |
| R40 a generated relation name is printed in the text | `the_narrowing_kernel_that_carries_a_mark_is_the_analyzers_cut` |

**N1 survived its first run and has a pin now.** Removing the test that a coerced cast over
a typed NULL is not a narrowing left every pin green: the fold's own exclusion of a wire
cast and of a literal covers `CAST(CAST(NULL AS Timestamp(ns)) AS …)`, the only shape the
pins held. It does not cover the NULL coercion types beside a zoned value
(`CAST(NULL AS Timestamp(ns, "UTC"))`), which the mutant marks. The unit pin
`a_cast_coercion_left_inside_a_container_or_to_the_wire_type_is_marked` holds that shape
since; the mutant was run again and is killed by it.

**Two things the mutant runs taught.** A scratch clone must not share the live tree's build
directory: cargo names a path package's artifacts by its path inside the workspace, so the
two trees overwrite each other, and one live unit run of this fold picked up a mutant that
way (the runs above use a directory of their own, and the live crates were rebuilt). The
83 mutants ran on `09de15ce`, one commit before the last product change: `d2717be5`
changed two functions of `timestamp_ns_cast.rs` and one of `narrowed_store/text.rs`. R32,
R35, the two mutants of that commit (R39, R40) and N1 were run on the final source, and
every anchor of the 85 resolves on it but N12's.

### 12.12 Cost

Release builds of main `2c33f289` (the verifier's) and of the final product source `d2717be5`, the
verifier's `probe/timing.py` (a 2,000-row table with no nanosecond column for the three
writes; planning and reading a 50-branch `UNION ALL` and a 200-arm `CASE`, over microsecond
columns and over alternating `TIMESTAMP` and `timestamptz_ns` columns). Interleaved main head
head main, three blocks, so six sets of fifteen statements per build; eight cores outside the
build set; the figure is the median of the six set medians, in milliseconds.

| Statement | Main | Head | Head over main |
|---|---|---|---|
| `INSERT`, no nanosecond column | 9.9 | 10.0 | 1.009 |
| `MERGE`, no nanosecond column | 46.0 | 46.3 | 1.006 |
| `MERGE` merge-on-read, no nanosecond column | 44.9 | 44.7 | 0.995 |
| read, 50-branch union, microseconds | 315.2 | 314.8 | 0.999 |
| read, 200-arm `CASE`, microseconds | 128.7 | 129.0 | 1.002 |
| read, 50-branch union, nanoseconds and microseconds | 66.6 | 66.8 | 1.002 |
| read, 200-arm `CASE`, nanoseconds and microseconds | 93.4 | 94.9 | 1.017 |
| `EXPLAIN`, union, microseconds | 337.9 | 286.7 | 0.849 |
| `EXPLAIN`, union, nanoseconds and microseconds | 69.2 | 60.3 | 0.872 |

Every ratio is at or below 1.05. `EXPLAIN` is faster on the head: the door now collects the
`EXPLAIN` rows itself where main returned a lazy frame. Why that saves a sixth was not
profiled.

**What was fixed on the way.** With a wrapper function on each narrowed branch (the fold's
fifth build, the source of `73fe665a` with the branch typing of `09de15ce`) the two mixed
reads measured 1.059 and 1.061, as steady as the table above. A profile showed none of the
rule's own functions above 0.1% of the time; the cost was DataFusion resolving one more
function for every type and name lookup above each mark, in every rule of both analyses.
Split by phase, the eager analysis took 0.5 and 1.6 ms more and the run after it 3.7 and
4.7 ms. `09de15ce` puts the mark on the narrowing kernel itself where the analyzer's cut is
that kernel, so those plans have main's node count.

### 12.13 Gates on the fold

Run 2026-10-11 on the final source `6c2ad4b1` (product code of `d2717be5`), one cargo command
at a time under the build lock on cores 32-47.

| Command | Exit | Result |
|---|---|---|
| `cargo fmt --all -- --check` | 0 | |
| `make rust-clippy` | 0 | |
| `make rust-panic-ban` | 0 | |
| `cargo test --locked -p repark-functions --lib` | 0 | 934 passed, 1 ignored |
| `cargo test --locked -p repark-iceberg --lib` | 0 | 1007 passed |
| `cargo test --locked -p repark-sql --lib` | 0 | 394 passed |
| `cargo test --locked -p repark-spark --lib` | 0 | 2694 passed, 5 ignored |
| `cargo test --locked -p repark-core --lib` | 101, then 0 | under the matrix and the mutant builds `microbatch::fence_tests::two_drivers_on_one_sink_land_each_epoch_exactly_once` ran past its time bound (three runs of the suite); the crate has no change in this unit. Run again when the matrix had finished: 1420 passed, 1 ignored |
| `cargo test --locked -p repark-spark --test timestamp_ns_narrow_rule` | 0 | 15 passed |
| `cargo test --locked -p repark-spark --test timestamp_ns_narrow_refuse` | 0 | 16 passed |
| `cargo test --locked -p repark-spark --test timestamp_ns_nested_shapes --test timestamp_ns_wall_doors --test timestamptz_ns_wall_doors` | 0 | 6 + 7 + 4 passed |
| `pytest tests/test_ice_tsns_narrow_refuse_1.py tests/test_ice_tsns_narrow_refuse_1_rule.py tests/test_ice_tstzns_wall_1.py tests/test_ice_tsns_merge_wall_1.py tests/test_ice_tsns_sql_1.py -q -n 8` on the release build of the final source | 0 | 826 passed |
| the recorder on the final build, 104,960 cells, against main | 0 | §12.9 |
| the verifier's `suites.py` (shapes, doors, equality, the limits), `stmts.py`, `reads.py`, `pq.py` on the final build | 0 | 23 runs, §12.9 |
| `python3 comment_ban.py <clone> origin/main HEAD` | 0 | hits=0 |
| `ruff check .` and `ruff format --check .` | 0 | |
| `make check-rust-file-size`, `make check-lib-rs`, `make check-crate-dag`, `make check-manifest` | 0 | `instant_ts.rs` 991 lines, `java_double.rs` 990, `timestamp_ns_cast.rs` 980, `null_narrowing.rs` 915, ceiling 1000 |
| `python3 scripts/sync_map_md.py --check`, `make check-map-md BASE=origin/main`, `make check-ledger-grammar`, `make check-ledgers`, `make check-docs-links`, `make check-docs-compaction`, `make spell-check` | 0 | |
| `scripts/check_lib_py.py`, `check_python_conventions.py`, `check_docstring_presence.py` | 0 | |

Not run: `make develop` (the facade ran against the release module on `PYTHONPATH`), the
wide `pytest -k` selection of §11.10, and the Spark measurement of §6. The clone has no
commit hook installed; the gates above stand in for it.


### 12.14 Disk and scratch

Free space before the first build: 356 G of 1.8 T. The fold wrote a release build directory
(seven incremental builds), seven copies of the native module, a clone for the mutants with
its own debug build directory, and the matrices' JSON under the lane's scratch directory
`fold2/`; the build directories, the module copies and the clone were removed at hand-back
and the measurements (JSON, logs, scripts) kept. The previous session's `target/r017` in
the clone was left as it was.

## 13. Fold 3 (2026-10-11): the second verdict

The verifier's second verdict on head `9cee945a` was FAIL: 6 S1, 4 S2 (two of the S2 are
surviving mutants). No analyzer-narrowed value was found stored; what failed was the other
side of the rule: statements main stored right that the head refused, reads that changed,
and CI. This fold removes that before it adds anything, and it is the last one: where a
finding could be closed by a smarter rule or by doing what main does, it does what main
does and the cell is named in §13.9. Where sections 2 to 12 differ, this section holds.

### 13.1 The rulings of the fold (orchestrator, provisional, built)

- **V2, narrowed.** The walk follows only the arguments the stored value passes through.
  A narrowed value that feeds a map key, an interval, an integer, a predicate or any other
  argument the stored nine digits do not come from does not refuse the statement.
- **V5, reversed for the transparent family.** `if`, `greatest` and `least`, `nvl2`, an
  element of an array built in place, a lambda, a scalar subquery and every spelling the
  planner lowers to the expression of `CASE` or `coalesce` hand the value on as `CASE` and
  `coalesce` do; the decision is taken on the analyzed expression, not on the spelling.
- **Reads are main's, without exception.** Whatever made a statement that writes nothing
  differ from main is removed: the `EXPLAIN` door that returned an executed frame, the
  written cast rewritten before coercion, the kept cast that showed and cost a field id.
- **`TRY_CAST` written by the user is a written cast.**
- **F3** (a date cast over a naive value cut to an instant, in a zone that skipped a day):
  the arm is not equal there; main stores the same differing value, so the smaller change
  is taken: nothing is built and the case is recorded (§13.9).

The addendum of the same day (items A to E) added: the five card spellings are the
acceptance and refuse on every write door in five zones (A, §13.11); what the value passes
through is a table of roles written as data in one place (B); a call hands the written
call on only where the type coerced inside it and the written cast are of the same zone
kind, instant or naive (C); the `EXPLAIN` text of a write is main's (D); and what parks
the unit is a statement that writes nothing and differs from main, or a statement main
stored fully that the head refuses, errors on or stores differently (E), which makes the
list of §13.9 the whole list of what is left as on main.

### 13.2 What changed in the source

**An `EXPLAIN` is main's frame again (F8).** Fold 2 ran every `EXPLAIN` at the door to
erase the marks from its text and returned the rows as an in-memory frame. That frame had
attribute ids, could be selected from, and broke two pins that pass on main
(`binding_builds_inert_empty_root_for_never_stamped_plans`,
`test_statement_frames_bind_without_an_attribute_id`); the third red check, the smoke
job, only gathers the others. The erasure moved to where the text is made: the session's
query planner is wrapped (`SessionExtension::configure_query_planner`, a fourth defaulted
hook of `repark-core`; `repark-spark` `router/explain_marks.rs` `UnmarkedExplain`), and
when the planned node is an `ExplainExec` whose rows hold a hidden name it is rebuilt with
the calls erased. The frame is the lazy `Explain` statement main returns;
`SAME TEXT AS ABOVE` is written by DataFusion from the cleaned rows.

**A read is analyzed as on main (F4, F6).** Two things fold 2 did during analysis changed
what a read answers, and both are gone:

- *The written cast rewritten before coercion.* Fold 2 turned `CAST(x AS TIMESTAMP)` below
  the root into the narrowing function before the first type coercion, so coercion typed
  the node from a microsecond branch where main types it from a nanosecond one
  (`array_except(array(tzns), array(CAST(tzns AS TIMESTAMP)))` returned another type and
  other values). The cast now stays as main has it and is *signed*: the `Cast` field takes
  the name `repark.written`. No schema, printed plan or result type reads that name
  (`Expr::to_field` uses the field's type, `Display` its type and metadata), so the plan is
  main's in every observable. The first mark skips a signed cast; that is the only reader.
- *The kept cast.* Where main's session rule drops a written cast (the wire type over a
  value that is already a `TIMESTAMP`) fold 2 kept it as the narrowing function, which
  printed in `EXPLAIN VERBOSE`, lost the column's `PARQUET:field_id` and aliased an
  aggregate. It is kept as `__repark_written_timestamp_cast__`, an identity function with
  its argument's field and name, erased from `EXPLAIN` text and removed by the optimizer;
  a root main would alias takes main's alias. Nothing is kept under
  `spark.sql.timestampType=TIMESTAMP_NTZ`, where main does not drop the cast (fold 2 kept
  it there too and returned an instant where main returns a naive value).

A written cast inside a container beside a microsecond container
(`coalesce(array(CAST(c AS TIMESTAMP)), array(ts))[0]`) is the one shape the first of these
exposed: with main's types the coercion casts a list that only looks like nanoseconds, and
the first mark sits on it. The store passes such a mark through (next paragraph).

**A mark that cuts nothing.** A mark over a value whose type in the analyzed plan holds no
nanoseconds (a `TRY_CAST` to the wire type read as `TIMESTAMP`) is passed through. The
mark was placed while the value still looked like nanoseconds; if it no longer does, the
node cuts nothing, and a narrowing inside the value has a mark of its own. This replaces
no rule of §12: an analyzer cut of a nanosecond value still carries its mark and refuses.

**`TRY_CAST` (F1).** `TRY_CAST(x AS TIMESTAMP)` is signed as `CAST` is, is not read as a
nanosecond source by the first mark, and at the store is a written call.

**What carries the value (F2).** Of a call typed a timestamp or a date that has an
argument of such a type, only those arguments are followed, and arithmetic follows its
timestamp side: `ns + (coalesce(ns, ts) - coalesce(ns, ts))` and
`ns + make_dt_interval(…, unix_micros(coalesce(ns, ts)))` store nine digits. A value
selected from a map follows the map's values, `map_keys` its keys:
`element_at(map(coalesce(ns, ts), ns), coalesce(ns, ts))` stores nine digits. A call with
no timestamp argument follows every argument, so the round trips of ruling V2 (STRING,
BIGINT, JSON, `date_format`, `concat`, DOUBLE) still refuse.

**What hands the value on (F7).** The written call nearest a mark is remembered through
`if`, `greatest`, `least`, `nvl`, `ifnull`, `nvl2` (its second and third argument), `nullif`
(its first), the element of an array built in place, a lambda body, an aggregate or a
window call typed a timestamp, and a cast to a container type. The equality these stand
on: each returns one of its arguments unchanged, or (`greatest`, `least`, `max`, `min`)
the larger or smaller of them, and the three forgiving calls of §12.2 are monotone, so
the call over the narrowed value equals the call over the nanosecond value wherever §12.2
says the plain arm does. A written cast over a scalar subquery whose plan holds a mark is
kept, so it reaches the store. `CAST(coalesce(coalesce(ns, NULL), ts) AS DATE)` refused
because the outer mark sat on a value already cut; it is a mark that cuts nothing.

**`UPDATE` reads one cast as the planner's.** DataFusion puts its conforming cast of a
scalar-subquery value at the root of the subquery, over the aggregate, where it is the
expression a written cast would be. With aggregates handing the value on, that cast
forgave `UPDATE t SET v = (SELECT max(coalesce(…)) …)`. `refuse_narrowed_ns_updates`
reads that one cast as the planner's; the statement refuses as it did at fold 2.

**The text (F5).** The narrowed value is named only when it is a column whose name is a
plain identifier, of a relation the statement named; every other value is named by the
target column alone (`The value was narrowed … write a CAST to TIMESTAMP over the
nanosecond value …`). The SQL rendering of fold 2 is deleted, so no engine text can reach
a refusal. A write to a branch or a staged snapshot names the table the statement wrote
(`ice.ns.t.branch_b1`), on the unit's refusal and on the older unfiltered `UPDATE` one.

**The role table (addendum B).** What the stored digits pass through is `ROLES` in
`repark-iceberg` `write/narrowed_store.rs`: 35 rows of a function name and a role, read by
one lookup. `coalesce`, `greatest`, `least`, `nvl`, `ifnull`, an array built in place and
the selecting aggregates and windows (`max`, `min`, `first`, `last`, `first_value`,
`last_value`, `any_value`) pass the value through every argument; `if` and `nvl2` through
the arguments after the predicate; `nullif`, `max_by`, `min_by` and `nth_value` through the
first; `lag` and `lead` through the value and the default; a selection (`element_at`,
`get_field`, an index) through the part of the container it selects, so a map by its
values and `map_keys` by its keys. The arms of `CASE`, the branches of a `UNION`, a scalar
subquery and a lambda result are plan and expression nodes and are followed by the walk
itself. A name with no row is an opaque call: it follows its timestamp and date arguments
(arithmetic by position, never the interval) and forgets the written call above it. The
table is rendered in `write/narrowed_store/map.md`. The two statements of the addendum
are pinned at the door: `coalesce(ns, ts) + INTERVAL 0 SECOND` refuses and
`ns + (coalesce(ns, ts) - coalesce(ns, ts))` stores nine digits.

**The zone kind (addendum C).** A written call is remembered through a role only while
the value keeps its zone kind. A mark that cuts nothing itself but turns an instant into a
naive value, or back, forgets the written call (`turned`): `CAST(if(c, ns, ntz) AS
TIMESTAMP)` over a zoned `ns` refuses, the same beside a `TIMESTAMP` sibling stores what
the twin stores. Probed in Pacific/Apia and Pacific/Kiritimati over 18 moments (§13.4).
`CASE WHEN c THEN ns END` has no narrowing: it stores nine digits. `nvl` and `ifnull`
beside an untyped NULL are typed STRING by the function (owner ruling 3, Q3) and are left
as on main (§13.9).

**The planner's cast is signed too (addendum D).** Fold 2 rewrote, before the first
coercion, every cast from nanoseconds to an instant type, which is also the planner's
conforming cast of an `INSERT`, `UPDATE` or `MERGE` value into a microsecond column; 17 of
205 `EXPLAIN` outputs of writes printed the narrowing function where main prints a cast.
The cast is now signed and left; the store reads the sign as it reads the function.

### 13.3 The findings one by one

| # | Sev | Finding (second verdict) | Disposition | Commit | Pin |
|---|---|---|---|---|---|
| F8 | S1 | CI red: three checks, two pins the `EXPLAIN` door broke | Closed: both pins pass; the third check gathers the others | `fb0730d5` | `an_explain_frame_is_a_statement_and_prints_no_hidden_function`; the two pins of main |
| F4 | S1 | Four container reads change type and values; a microsecond-target store moves | Closed: 36 of 36 reads equal main, the six cells store main's value | `88af0a50` | `a_written_cast_changes_no_read`; facade `test_a_read_answers_as_the_base_does` |
| F6 | S2 | A cast over a temporary view loses `PARQUET:field_id`, plain `EXPLAIN` differs | Closed: 274 `EXPLAIN` outputs and schemas equal main but the snapshot id of one `SHOW CREATE TABLE` | `88af0a50` | the same two; `a_written_cast_inside_a_container_beside_a_microsecond_container_stores` |
| F1 | S1 | A written `TRY_CAST` beside a microsecond value refuses | Closed: stores what main stores | `db9f4204` | `a_written_try_cast_beside_a_microsecond_value_stores_on_every_door` |
| F2 | S1 | A plain nine-digit value refuses beside a narrowing it does not pass through | Closed: stores nine digits | `bd6a13bb` | `a_plain_column_stores_beside_a_narrowing_it_does_not_pass_through` |
| F7 | S1 | A written call refuses over `if`, `greatest`, an array element, a lambda, a scalar subquery; 2,410 cells | Closed: 0 of the 2,410 refuse; 0 stored values differ from the twin | `4ce9ea35` | `a_written_call_stores_through_a_choice_an_element_a_lambda_and_a_subquery` |
| F3 | S1 | A date cast over a naive value cut to an instant differs in Pacific/Apia | Left as on main: the stored value is main's (§13.9) | – | – |
| F5 | S2 | Refusal texts carry generated names, engine renderings, a remedy nobody can type | Closed for (a) to (e) and (g): 0 of 36,193 texts; (f) is main's writer (§13.9) | `83d510d6` | `a_refusal_names_a_plain_column_and_otherwise_only_the_target`; `the_text_names_a_plain_column_and_no_generated_relation`; the branch doors of `timestamp_ns_narrow_refuse.rs` |
| M-L4 | S2 | Mutant L4 survives: only the first row of `VALUES` followed | Closed: killed | `d46001d2` | `every_row_of_values_is_followed`; `a_narrowing_in_a_later_values_row_refuses` |
| A | – | The five card spellings refuse on every write door in five zones | Not met on every door: the four narrowing spellings refuse in five zones on 56 of 64 doors; a cached frame stores cut as on main; §13.11, Q9 | – | §13.11; the door pins of `timestamp_ns_narrow_refuse.rs` and `timestamp_ns_narrow_rule.rs` |
| B | – | "Passes through" is one table of roles | Built | `18a7c3e2` | the eight store pins of `narrowed_store/lineage_tests.rs`; `a_plain_column_stores_beside_a_narrowing_it_does_not_pass_through`; mutants T1 to T4 (§13.6) |
| C | – | Transparency needs the same zone kind | Built | `18a7c3e2` | `a_written_call_stores_through_a_choice_an_element_a_lambda_and_a_subquery` (the naive-sibling refusals); `a_written_call_reaches_the_mark_through_what_hands_the_value_on_unchanged` |
| D | – | The `EXPLAIN` text of a write is main's | Built: 0 of 205 outputs differ from main | `df67302e` | `a_cast_from_nanoseconds_to_an_instant_before_coercion_is_the_statements`; `an_explain_frame_is_a_statement_and_prints_no_hidden_function` |
| E | – | What parks | §13.4 (no cell main stored fully moved), §13.5 (reads), §13.9 (the list) | – | – |
| M-P3 | S2 | Mutant P3 survives: a scalar subquery is not a nanosecond producer | Closed: killed | `d46001d2` | the producer unit pin; `a_scalar_subquery_of_nanoseconds_is_narrowed_where_no_column_in_scope_is` |

### 13.4 The matrices on the final build

Final source `5a982455` (the release build is of `ae3a96b8`; the commit after it renames a binding); main is `2c33f289` (the verifier's build and the
committed fixture).

**The 104,960-cell matrix**, main against the final head:

| Moved | Cells |
|---|---|
| Byte-identical to main | 81,110 |
| A cut value on main, refused now (`cut` 14,788, `cut` with the wall moved 6,902) | 21,690 |
| An error or a partial store on main, refused now (`error` 1,530, `other` 300) | 1,830 |
| Refused on main and on the head, text changed | 330 |
| A cell main stored fully, refused, erroring or stored differently now | 0 |
| Cells of a microsecond target that moved | 0 |
| Refused cells that left a value on disk | 0 |

23,520 cells moved into the refusal. Against fold 2's head 80 cells changed what they
do, all back to what main does: 40 of `TRY_CAST(c AS TIMESTAMP)` in a `UNION` with an
untyped NULL branch, through SQL and through the DataFrame union, store the cast's value
again (ruling: `TRY_CAST` is written), and 40 of `EXPLAIN ANALYZE UPDATE` (eight spellings
of the family, the naive target) raise main's own error again instead of the refusal;
nothing is stored by them on either build (Q8, §13.9). 560 more refusals changed their
text only (§13.2 "The text"). The build of `83d510d6`, before the addendum, answers every
one of the 104,960 cells as the final build does.

**The verifier's suites**, re-run from its scripts on the final build (`verify1`:
shapes, doors, stmts, equality, the limits; `verify2`: never worse, holes, forgiveness,
forgiveness at the limits and in six more zones, `VALUES`), 116,200 cells:

| Suite | Cells | Identical to main | Cut or partly cut on main, refused | Error on main, refused | Main stored fully, head does not |
|---|---|---|---|---|---|
| shapes (five zones) | 6,680 | 4,660 | 1,860 | 160 | 0 |
| doors (three zones) | 42,336 | 32,673 | 8,565 | 984 | 0 |
| stmts (two zones) | 6,272 | 4,968 | 1,016 | 288 | 0 |
| equality (five zones) | 5,760 | 3,200 | 2,560 | 0 | 0 |
| equality at the int64 limits (three zones) | 6,912 | 3,840 | 1,500 | 1,572 | 0 |
| never worse (147 statements, five zones) | 4,410 | 4,390 | 20 | 0 | 0 |
| holes (263 statements, five zones) | 7,890 | 4,190 | 3,246 | 380 | 0 |

The doors row holds the 96 cells of fold 2's note (a table-creating door labelled
microsecond that creates a nanosecond column) and 114 refusals whose text alone differs
from main's; its leak detector, which counted 480 empty table names and 6 generated ones
on this fold's third build, counts none. The never-worse row's 20 are
`nvl2(coalesce(c, ts), c, c)`, an analyzer cut main stores. The holes row also holds the
44 cells the verdict explains (the narrowing is in the table's other nanosecond column)
and its 30 microsecond-target cells (20 of that kind, 10 a reused `PREPARE` name).

*Forgiveness* (20 written arms over 22 narrowings, inline and through a subquery):

| Suite | Cells | Stores what the twin stores | Stores a value that differs from the twin | Refuses |
|---|---|---|---|---|
| five zones | 15,600 | 8,380 | 0 | 6,000 |
| at the int64 limits, three zones | 18,720 | 4,524 | 0 | 7,200 |
| Pacific/Apia and five more zones | 1,584 | 830 | 10 (F3, as on main) | 480 |

Of the 6,000 refusals in five zones 2,220 are cells main stored the twin value in. They
are the arms §12.2 refuses by rule and the verdict did not count: a date call over a zoned
value (equal in four of the five zones), `date_trunc('week', …)` over a naive value, a
`TIMESTAMP_NTZ` sibling, and the two container narrowings (`array_append`, a `coalesce` of
arrays), whose cut truncates toward zero. The verdict's 2,410 cells of F7 are not among
them: `if`, `greatest`, an array element, a lambda, a nested `coalesce` and a scalar
subquery now answer exactly as the plain `coalesce` beside a `TIMESTAMP` column does.

**Cells that moved back from the refusal** because of the fold's rulings, fold 2's head
against the final head: 40 of the 104,960-cell matrix (above), and over the verifier's
116,200 cells 4,216 store what main stores
(2,510 of the forgiveness matrix and 1,366 at the limits, 190 of the holes suite's written
casts over an aggregate, a window, a scalar subquery, an array and `greatest`, 80 of the
never-worse suite, 70 `TRY_CAST(… AS DATE)`), every one with a stored value equal to
main's; 1,771 raise main's own error again and store nothing (the limits, a map with a
duplicate key, `EXPLAIN ANALYZE UPDATE`). No cell moved into the refusal, and none stores
a value main does not store.

### 13.5 Reads

| Probe | Compared | Differ from main |
|---|---|---|
| the verifier's `reads1.sql` (a written cast beside the nanosecond value, 36 reads): schema and values | 36 | 0 |
| the verifier's `pqv.sql` (field ids in schemas and written Parquet) and `expl.sql` (`EXPLAIN` frames) | 21 | 0 |
| the verifier's `exv2.py`: `EXPLAIN`, `VERBOSE`, `EXTENDED`, `FORMATTED` of 61 statements, 9 frames, 25 results | 274 | 1 (a snapshot id in `SHOW CREATE TABLE`) |
| `pq.py` (490 queries written to Parquet and read back) | 490 | 0 |
| addendum D: `EXPLAIN`, `EXPLAIN VERBOSE` and `EXPLAIN ANALYZE` of `INSERT`, `UPDATE`, `MERGE` and CTAS over 12 values and three targets | 205 | 0 |
| `reads.py` on eight cores, as main was recorded (on the build of `83d510d6`; not repeated on the final build) | 31 | 0 |
| the fixture of this fold (`ice_tsns_narrow_refuse_1_reads_base.json`, recorded on main): schema with field metadata, rows, plain `EXPLAIN` of 73 statements | 73 | 0 |

No hidden name (`__repark_narrowed_beside_*`, `__repark_written_timestamp_cast__`,
`repark.untyped_null`, `repark.written`) is printed by any of them.

### 13.6 Mutants

Each mutant is one edit of the product source of its day in a clone of the branch with a build
directory of its own, run against the unit suite of its crate; the file is restored and its
hash compared after each. The verdict's two survivors and fourteen fresh mutants in the
lines this fold changed, and four swaps of a row of the role table: 18 of 20 killed, each
at the unit stage; the two survivors are equivalent.

| Mutant | Killed by |
|---|---|
| L4 only the first row of `VALUES` is followed (the verdict's survivor) | `every_row_of_values_is_followed` |
| P3 a scalar subquery is not a nanosecond producer (the verdict's survivor) | `only_a_node_that_produces_a_column_is_marked_and_a_producer_is_found_without_a_scan` |
| X1 a signed cast is not read as signed | `a_cast_to_the_sql_timestamp_type_below_the_root_is_read_before_coercion` |
| X2 a `TRY_CAST` to the wire type is not a written cast | the same |
| X3 the kept cast is kept when the session reads `TIMESTAMP` as `TIMESTAMP_NTZ` | `a_written_cast_over_a_microsecond_column_is_kept_only_above_a_mark` |
| X5 a mark over a value with no nanoseconds still refuses | `a_mark_over_a_value_that_holds_no_nanoseconds_cuts_nothing` |
| X16 a marked column is not followed to the expression that produces it | the same |
| X6 every argument of a timestamp call is followed | `a_plain_value_stores_beside_a_narrowing_it_does_not_pass_through` |
| X7 a value selected from a map follows the keys too | the same |
| X8 an aggregate forgets the written call above it | `a_written_call_reaches_the_mark_through_what_hands_the_value_on_unchanged`, `an_update_reads_the_cast_at_the_root_of_its_scalar_subquery_as_the_planners` |
| X9 `UPDATE` reads the cast at the root of a scalar subquery as written | `an_update_reads_the_cast_at_the_root_of_its_scalar_subquery_as_the_planners` |
| X10 a generated table name is printed | `the_text_names_a_plain_column_and_no_generated_relation` |
| X11 `EXPLAIN` prints the kept cast | `a_mark_is_erased_from_a_printed_plan_and_its_argument_kept`, `an_explain_plan_prints_no_mark_and_repeats_as_the_engine_does` |
| X12 `TRY_CAST` is not a written call at the store | `a_written_call_reaches_the_mark_through_what_hands_the_value_on_unchanged` |
| X13 a column whose name is an engine rendering is printed | `the_text_names_a_plain_column_and_no_generated_relation` |
| X14 an array built in place forgets the written call | `a_written_call_reaches_the_mark_through_what_hands_the_value_on_unchanged` |
| T1 a row of the role table is swapped: `map_values` follows the keys (addendum B) | `a_plain_value_stores_beside_a_narrowing_it_does_not_pass_through`, `a_narrowed_value_refuses_through_the_arguments_it_passes_through` |
| T4 a row of the role table is swapped: `nullif` follows both arguments | `a_plain_value_stores_beside_a_narrowing_it_does_not_pass_through` (the `nullif` row, added for it) |
| T2 `if` follows its predicate; T3 `lag` follows its offset | survive, equivalent: a call typed a timestamp follows only its timestamp and date arguments, so a predicate or an offset is not followed under either role |

X11 and X13 were edits that did not build or did not find their line on the first run and
were run again as written here. The door-level rules without a unit mutant (the alias a
kept cast takes at a root, the table named on a branch) are held by the door pins
`a_written_cast_changes_no_read` and the branch doors of `timestamp_ns_narrow_refuse.rs`;
no mutant was run against them. The 85 mutants of §12.11 were not run again.

### 13.7 Cost

Release builds of main `2c33f289` (the verifier's) and of the final source, the verifier's
`probe/timing.py`, interleaved main head head main, three blocks, six sets of fifteen
statements per build, eight cores outside the build set, the warehouse on `/dev/shm`; the
figure is the median of the six set medians, in milliseconds.

| Statement | Main | Head | Head over main |
|---|---|---|---|
| `INSERT`, no nanosecond column | 10.0 | 10.1 | 1.018 |
| `MERGE`, no nanosecond column | 45.9 | 46.6 | 1.014 |
| `MERGE` merge-on-read, no nanosecond column | 45.1 | 45.6 | 1.011 |
| read, 50-branch union, microseconds | 314.6 | 316.3 | 1.005 |
| read, 200-arm `CASE`, microseconds | 128.7 | 132.1 | 1.026 |
| read, 50-branch union, nanoseconds and microseconds | 66.4 | 66.9 | 1.007 |
| read, 200-arm `CASE`, nanoseconds and microseconds | 93.4 | 95.2 | 1.019 |
| `EXPLAIN`, union, microseconds | 337.4 | 339.8 | 1.007 |
| `EXPLAIN`, union, nanoseconds and microseconds | 69.3 | 69.8 | 1.007 |

Every ratio is at or below 1.05; the largest is 1.026. The run is on the final build;
the gate's last cargo test ran on other cores during it, and the two slowest head sets of
every statement are the two that overlapped it. `EXPLAIN` costs what it costs on main
again (fold 2 measured 0.85: its door collected the rows). The same script on the build of
`83d510d6`, before the addendum, measured at most 1.024.

### 13.8 Gates on the fold

Run 2026-10-12 on the final source `5a982455`, one cargo command at a time under the build
lock on cores 32-47. The release build measured in §13.4 to §13.7 is of `ae3a96b8`; the one
commit after it renames a binding.

| Command | Exit | Result |
|---|---|---|
| `cargo fmt --all -- --check` | 0 | |
| `make rust-clippy` | 0 | the gate run that started before the rename exited 2 on `similar_names`; run again on the final source, 0 |
| `make rust-panic-ban` | 0 | |
| `cargo test --locked -p repark-functions --lib` | 0 | 934 passed, 1 ignored |
| `cargo test --locked -p repark-iceberg --lib` | 0 | 1015 passed |
| `cargo test --locked -p repark-sql --lib` | 0 | 394 passed |
| `cargo test --locked -p repark-spark --lib` | 0 | 2694 passed, 5 ignored |
| `cargo test --locked -p repark-core --lib` | 0 | 1420 passed, 1 ignored |
| `cargo test --locked -p repark-python --no-fail-fast` | 0 | 184 passed |
| `cargo test --locked -p repark-spark --test timestamp_ns_narrow_lineage` | 0 | 9 passed |
| `cargo test --locked -p repark-spark --test timestamp_ns_narrow_rule` | 0 | 15 passed |
| `cargo test --locked -p repark-spark --test timestamp_ns_narrow_refuse` | 0 | 16 passed |
| `cargo test --locked -p repark-spark --test timestamp_ns_nested_shapes --test timestamp_ns_wall_doors --test timestamptz_ns_wall_doors` | 0 | 6 + 7 + 4 passed |
| the facade files of the unit and its neighbours (`pytest`, the release module on `PYTHONPATH`) | 0 | 842 passed |
| the recorder on the final build, 104,960 cells, against main | 0 | §13.4 |
| the verifier's suites on the final build | 0 | §13.4, §13.5 |
| `python3 comment_ban.py <clone> origin/main HEAD` | 0 | hits=0 |
| `ruff check .` and `ruff format --check .` | 0 | |
| `make check-rust-file-size`, `make check-lib-rs`, `make check-crate-dag`, `make check-manifest` | 0 | `null_narrowing/tests.rs` 988 lines, `instant_ts.rs` 993 |
| `python3 scripts/sync_map_md.py --check`, `make check-map-md BASE=origin/main`, `make check-ledger-grammar`, `make check-ledgers`, `make check-docs-links`, `make check-docs-compaction`, `make spell-check` | 0 | |
| `scripts/check_lib_py.py`, `check_python_conventions.py`, `check_docstring_presence.py` | 0 | |

Red before green: on `9cee945a` six of the nine pins of `timestamp_ns_narrow_lineage.rs`
fail (F1, F8, F4/F6, F2, F7, F5) and three pass (L4, P3 and the container pin, which hold
there at the door).

Not run: `make develop` (the facade ran against the release module on `PYTHONPATH`), the
whole workspace test, the cluster feature, the dbt adapter and the example coverage (CI
runs them, §13.10), and the Spark measurement of §6. The clone has no commit hook
installed; the gates above stand in for it.

### 13.9 What is left as on main

This is the whole list. Each entry is a shape whose stored value, error or text on the
head is main's although the rule, read to the letter, would have it otherwise. A shape
that is not here and is not byte-identical to main refuses.

**Stored cut, as on main (an analyzer narrowing the unit does not refuse).**

1. **A cached or persisted frame** (owner ruling 4, §11.6): `df.cache()` or `persist()`
   of a frame that holds the narrowing, then any write door. The cache replaces the plan
   with a scan of microseconds before the store reads it. In the matrix this is the door
   `cached_frame_append`: every one of the five card spellings that narrows stores the cut
   value there, as on main (§13.11).
2. **A door that creates its table from the query** (`CREATE TABLE … AS SELECT`,
   `REPLACE TABLE … AS SELECT`, `createOrReplace`, `saveAsTable` in overwrite mode): the
   narrowed type is the type of the column the door creates, a microsecond column, so
   nothing is cut on the way into it. Matrix class `other` on main and on the head.
3. **`nvl` and `ifnull` of a nanosecond value** (owner ruling 3, Q3; parity row R-019):
   the function types its result STRING, beside an untyped NULL and beside a microsecond
   value alike, so the value is a string and the conforming cast back is the planner's
   string cast; no timestamp coercion narrows it. Most doors raise main's error; the doors
   that conform a string store main's value (`insert_values` and `branch_update` store
   nine digits; a `UNION` with a nanosecond branch stores every row cut, R-019). A written
   call over it (`date_trunc('day', nvl(ns, NULL))`, `date_trunc('second', ifnull(ns,
   ts))`) stores main's value, which is not the twin's (16 of 816 cells of the zone-kind
   probe).
4. **A value that returns through an interval**: `TIMESTAMP '1970-01-01' + (coalesce(ns,
   ts) - TIMESTAMP '1970-01-01')` stores the cut value. Addendum B: an interval does not
   pass the value through. A return through STRING, BIGINT, JSON or DOUBLE refuses.
5. **`nvl2(coalesce(ns, ts), ns, ns)`** and every shape where the narrowed value is only
   a predicate, a map key selected past, or an integer argument: nine digits are stored,
   as on main; nothing narrowed reaches the column.
6. **F3**: `CAST(x AS DATE)`, `to_date(x)` and `date(x)` over a naive value the analyzer
   cut to an instant store, in Pacific/Apia on 2011-12-30 only, the date after the day the
   zone skipped, where the call over the nanosecond value stores the 30th. Main stores the
   same value in the same cell (10 cells of 1,584). In every other zone and moment probed
   the arm equals its twin. Not built (ruling F3: the smaller change).

**Raises main's own error where a refusal by name would be the rule.**

7. **Q8**: `EXPLAIN ANALYZE UPDATE` and `PREPARE … UPDATE` with a narrowed `SET` value.
   The planner's cast of the value reads as a written cast (§11.5); with the roles handing
   the written call on, `if`, `greatest`, `nullif`, an array element and a lambda beside
   NULL raise main's Arrow type error there and store nothing (40 cells of the matrix more
   than at fold 2). A plain `UPDATE` refuses.
8. **A narrowing at the int64 limits and a map built with a duplicate key**: where main
   raises its own error before the store is reached, the head raises the same error.

**Text, as on main.**

9. `INSERT OVERWRITE`, `INSERT … BY NAME` and `overwritePartitions` name the table
   `` `ns`.`t` `` where the other doors write `` `ice.ns.t` ``, as every refusal of those
   doors does on main.
10. `a.union(b).writeTo(t).append()` whose second frame names the timestamp column
    differently refuses with "give the NULL beside it". Main's writer registers that
    frame as a union by name and fills the unmatched column with NULL, so the text is
    accurate; that the writer does so (18 of 36 rows stored NULL on main) is a defect of
    the writer, out of scope, reported.

**Not reached by the unit.** The native ANSI door, the Postgres route and the Connect
door, as in both verdicts.

**The other direction, not "as on main" and not a finding (addendum E):** cells main
stored cut that the head refuses although the written call over the nanosecond value
would store the same (the arms of §12.2: 2,220 cells of the verifier's forgiveness
matrix): a date call over a zoned narrowed value, `date_trunc('week', …)` over a naive
one, a written call over a value narrowed beside a sibling of the other zone kind or
inside a container coercion, and a written call with an opaque call between it and the
narrowed value (`CAST(coalesce(ns, ts) + INTERVAL '0' SECOND AS TIMESTAMP)`).

### 13.10 CI jobs that run the changed code

`Rust lint (fmt + clippy + check)`, `Rust test (workspace)`, `Rust test (cluster
feature)`, `Repo guards (map.md + workflow parse + crate-DAG + lib.rs)`, `Python`, `code
changed`, `build debug wheel (profile ci)`, `facade shard 0`, `facade shard 1`, `facade
shard 2`, `dbt-adapter tests`, `example-coverage execute` and `build + import smoke (debug,
host)`; `typos` and the wheel workflow read the changed files too. The three that were red
on `9cee945a` are `Rust test (workspace)`, `facade shard 2` and the smoke job.

### 13.11 The acceptance of addendum A: the five card spellings by door

From the 104,960-cell matrix on the final build: a door counts as refusing in a zone when
every cell of the spelling refuses there (two nanosecond targets, two sources). The
figure is the number of the five zones; a class in brackets is what the cells do instead,
which in every such cell is what main does.

| Door | `coalesce(ns, NULL)` | `nvl(ns, NULL)` | `array(ns, NULL)[0]` | `CASE WHEN … THEN ns ELSE NULL END` | `if(…, ns, NULL)` |
|---|---|---|---|---|---|
| each of 56 doors (listed below) | 5 | see below | 5 | 5 | 5 |
| `df_create_or_replace` | 0 (other) | 0 (other) | 5 | 0 (other) | 0 (other) |
| `df_save_overwrite` | 0 (other) | 0 (other) | 5 | 0 (other) | 0 (other) |
| `df_functions_append` | 5 | 0 (skip) | 0 (skip) | 5 | 0 (skip) |
| `explain_insert` | 0 (nothing) | 0 (error) | 0 (nothing) | 0 (nothing) | 0 (nothing) |
| `explain_analyze_update` | 0 (error) | 0 (error) | 0 (error) | 0 (error) | 0 (error) |
| `ctas` | 0 (other) | 0 (other) | 5 | 0 (other) | 0 (other) |
| `rtas` | 0 (other) | 0 (other) | 5 | 0 (other) | 0 (other) |
| `cached_frame_append` | 0 (cut, cut+wall) | 0 (error) | 0 (cut, cut+wall) | 0 (cut, cut+wall) | 0 (cut, cut+wall) |

The doors that refuse all four narrowing spellings in five zones: `insert_values`, `insert_select`, `insert_positional`, `insert_by_name`, `insert_overwrite`, `insert_overwrite_dynamic`, `insert_overwrite_static`, `insert_replace_where`, `merge_insert`, `merge_insert_subquery`, `merge_update`, `merge_update_subquery`, `merge_by_source`, `update_where`, `update_nowhere`, `merge_insert_mor`, `merge_update_mor`, `merge_update_subquery_mor`, `merge_by_source_mor`, `update_where_mor`, `update_nowhere_mor`, `df_append`, `df_overwrite`, `df_overwrite_partitions`, `df_save_append`, `df_insert_into`, `df_insert_into_overwrite`, `df_select_expr_append`, `df_merge_into`, `branch_insert`, `branch_merge`, `branch_update`, `wap_insert`, `explain_analyze_insert`, `prepare_execute_insert`, `cte_insert`, `subquery_insert`, `union_insert`, `join_insert`, `aggregate_insert`, `window_insert`, `distinct_insert`, `scalar_subquery_insert`, `frame_view_insert`, `sql_view_insert`, `catalog_view_insert`, `frame_view_merge`, `merge_update_star`, `merge_insert_star`, `update_scalar_subquery`, `insert_reordered`, `sort_limit_insert`, `union_null_insert`, `df_union_null_append`, `lambda_first_insert`, `df_lambda_below_append`.

`nvl(ns, NULL)` refuses in five zones on 1 of 64 doors (`union_insert`); its other cells by class, head (main): error (main error): 1120; other (main other): 80; full (main full): 40; skip (main skip): 20. That spelling is typed STRING by the function (§13.9 item 3).

Read plainly: **a cached frame stores the cut value, as on main**, for every one of the
spellings that narrows; the four doors that create their table store microseconds in the
microsecond column they create; `EXPLAIN INSERT` writes nothing; `EXPLAIN ANALYZE UPDATE`
raises main's error. `skip` is a door that has no spelling of that shape (the DataFrame
functions door has no `if`, no array index and no `nvl`). Closing any of these needs an owner ruling changed (Q9).

**Cells moved back from the refusal to "stores what main stores"** by the fold's rulings
and the addendum, fold 2's head against the final head, by family: in the 104,960-cell matrix 40
(`TRY_CAST` in a `UNION` with an untyped NULL branch); in the verifier's 116,200 cells
4,216: the forgiveness matrix 2,510 (a written call over `if`, `greatest`, `least`,
`nvl2`, an array element, a lambda, a nested `coalesce` and a scalar subquery), the same
at the int64 limits 1,366, the holes suite 190 (a written cast over an aggregate, a
window, a scalar subquery, an array and `greatest`), the never-worse suite 80 (a plain
column beside a narrowing it does not pass through), `TRY_CAST(… AS DATE)` 70. Every one
stores the value main stores. 1,771 more raise main's own error again and store nothing.
No cell moved into the refusal between fold 2 and the final build.

## Q0. The questions put before the rulings (answered 2026-10-10, section 11.1)

- **Q1 (C-009, RULING).** Should `CASE WHEN … THEN CAST(c AS TIMESTAMP) ELSE NULL END` (and
  `coalesce(CAST(c AS TIMESTAMP), NULL)`, `if(…, date_trunc('second', c), NULL)`) refuse?
  *Premise:* the brief reads the card's lean as refusing a cut beside an untyped NULL "also
  when a written cast sits inside that branch". Measured, no cut is made beside the NULL in
  these statements: the written cast makes the branch a microsecond value before the NULL
  is read (§7). The owner rule names a written `CAST` as deliberate, and the brief's
  never-worse clause forbids refusing a written `CAST` that stores on the base. *Lean,
  taken:* they store, unchanged on every door. A written call *over* a narrowed value
  (`date_trunc('second', coalesce(c, NULL))`, `CAST(coalesce(c, NULL) AS timestamp_ns)`) does
  refuse: the analyzer's cut is inside it and the written call does not undo it.
- **Q2 (C-014, RULING).** Should a nanosecond value beside a NULL typed `TIMESTAMP` or
  `TIMESTAMP_NTZ` refuse? *Premise:* `coalesce(c, CAST(NULL AS TIMESTAMP))` narrows `c` by
  coercion too, and stores the cut value on every door but the unfiltered `UPDATE`, which
  refuses it since R-007 by its older test. The card's ask reads "the NULL branch carries
  no nanosecond type"; the owner rule reads "a microsecond type that only exists because
  type coercion widened a NULL branch", and here the statement wrote the type. Spark widens
  such a pair to the typed NULL's type. *Lean, taken:* not refused; the mark is placed only
  for an untyped NULL, which is what the brief asks to build. Refusing it later is one test
  in `tag_untyped_nulls`; the text's advice ("give the NULL beside it the type
  timestamp_ns") already fits it.
- **Q3 (C-015, OWNER).** `nvl(ns, NULL)` and `ifnull(ns, NULL)` are in the rule's family but
  are typed `STRING`. *Premise:* they fail store assignment by the door's own text
  (`source type Utf8 is not ANSI-store-assignable`) on 56 of 64 doors, store nine digits
  through `INSERT … VALUES`, and store a cut value through a `UNION ALL` with a nanosecond
  branch (20 cells per target, on the base and on the head). The cut there is the string's
  six digits, not a microsecond type. Marking them would refuse the `VALUES` cells that
  store the right value. *Lean:* fix the typing of `nvl` and `ifnull` over a nanosecond
  value in ICE-TSNS-COERCION-1; nothing in this unit.
- **Q4 (C-016, RULING).** Should a frame materialised by `cache()`, `persist()` or a round
  trip through Arrow refuse when it is written into a nanosecond column? *Premise:* 300
  cells of the matrix (`cached_frame_append`, fifteen spellings) store the cut value on the
  base and on the head. The cached frame is a table of microsecond instants; the statement
  that writes it holds no expression to refuse, and a `TIMESTAMP` column of any table
  stores the same way. *Lean:* not refused; the nanosecond-aware typing of
  ICE-TSNS-COERCION-1 removes the cut at its source, which is the only place a materialised
  frame can be helped.
- **Q5 (C-017, RULING).** Should a nanosecond value narrowed beside a microsecond column or
  literal that is not NULL refuse (`coalesce(ns, ts)`,
  `CASE WHEN … THEN ns ELSE TIMESTAMP '…' END`)? *Premise:* the coercion narrows the
  nanosecond value there too, and the cells store the cut value on the base and on the head.
  The first sentence of the owner rule covers it (a narrowing the analyzer inserted); the
  third sentence and the brief limit the unit to a NULL branch, and the text's advice does
  not fit. *Lean:* not this unit; ICE-TSNS-COERCION-1 decides the common type of a
  nanosecond and a microsecond timestamp.
- **Q6 (C-018, RULING).** Should the unfiltered `UPDATE` store the microsecond value for
  `SET v = CAST(c AS TIMESTAMP)`? *Premise:* it keeps nine digits (15 cells per target and source:
  `UPDATE` with no `WHERE` in both modes and on a branch, five zones) where every other
  door stores the value the cast was written to produce (§7). *Lean:* yes, in a unit of its
  own; it changes a stored value, which this unit does not do.

## Q. Questions after fold 1

- **Q7 (RULING).** Should the `UNION` cell of Q3 keep storing its cut value?
  *Premise:* §11.4. The ruling on Q3 reads "not a refusal in this unit"; the ruling on Q5
  refuses a nanosecond value narrowed beside a microsecond one, and measured, that is what
  the cell is. *Lean, taken:* it refuses, with the Q5 text; never worse holds (a cut value
  became a refusal). Storing it again is an exception in `settle_union_branches` and the
  root mark for a branch whose sibling was typed `STRING`.
- **Q8 (RULING).** Should the 80 base-error cells of §11.5 refuse by name again?
  *Premise:* they refused at `91f4e2f4` and raise the base's error at this head; nothing is
  stored either way. Refusing them needs the guard to tell the planner's cast of an
  `UPDATE`'s `SET` value from a written `CAST(… AS TIMESTAMP)`, which Q1 says stores.
  *Lean:* leave them; the base's errors under `EXPLAIN ANALYZE` are a defect of their own
  (out of scope, observed).

## Q. Questions after fold 2

The three orchestrator rulings of the fold are provisional and built; the owner may overturn
each. Two questions are new.

- **V1 (RULING, provisional, built).** A written `CAST(v AS TIMESTAMP)` over a column whose
  subquery, view or frame holds the analyzer's narrowing stores, as the inline spelling
  does. *Premise:* the verdict's S2 (80 cells refused where the inline cast stores).
  *Built:* §12.1, §12.8; C-032. Overturning it is deleting `Marks::keep_written_cast`'s
  second arm.
- **V2 (RULING, provisional, built).** A narrowed value that leaves through STRING, BIGINT
  or JSON text and returns refuses. *Built:* S2 of §12.1; C-030.
- **V3 (RULING, provisional, built).** A read is what main reads: no tag, no lost field id,
  no mark name in `EXPLAIN VERBOSE` or an error text, and planning at most 1.05 of main.
  *Built:* §12.5, §12.8, §12.12; C-034, C-037.
- **V4 (RULING).** May a written `CAST(v AS TIMESTAMP)` over a column of a view the
  analyzer cannot read show in `EXPLAIN VERBOSE` as the narrowing function?
  *Premise:* §12.8. V1 through a temporary or a catalog view needs the cast kept, and the
  analyzer cannot see whether that view holds a narrowing, so it keeps the cast over every
  such view's microsecond column. Values, schemas, plain `EXPLAIN` and the optimized plan
  are main's; three of 72 compared `EXPLAIN VERBOSE` outputs print
  `__repark_narrow_timestamp_ns__(tv.ts)` in their analyzed rows where main prints `tv.ts`.
  The other way is to keep the cast only where the narrowing is visible to the analyzer
  (a subquery, a common table expression, a frame, an inlined view), and let the cast over
  a temporary or catalog view's narrowed column refuse. *Lean, taken:* keep it; V1 names
  views, and no mark name is printed.
- **V5 (RULING).** Should a written call over a narrowed value store when another call
  sits between them and the value is still equal?
  *Premise:* §12.6. `CAST(coalesce(ns, ts) + INTERVAL '0' SECOND AS TIMESTAMP)` stored a cut
  value on main and refuses now: S4 forgives only through transparent nodes, because it has
  no proof of equality through an arbitrary call. Ruling 1 read to the letter stores it.
  *Lean, taken:* refuse; never worse holds and the remedy is to write the cast on the
  narrowed value itself.
- **Q7 and Q8** stand as ruled for this fold (C-026, C-027).
- **A note on the fold's seam rule.** The brief names `repark-sql`, `repark-iceberg` and
  `repark-spark`; the mark has lived in `repark-functions` since the unit's first commit
  and the redesign changed it there. No crate edge was added.

## Q. Questions after fold 3

- **Q9 (OWNER).** Addendum A asks that the five card spellings refuse on every write door.
  Three owner rulings and one door family stand against it, and the build kept them:
  (a) a cached or persisted frame stores the cut value, as on main (ruling 4, §11.6):
  the door `cached_frame_append`, every narrowing spelling, five zones; (b)
  `nvl(ns, NULL)` and `ifnull(ns, NULL)` are typed STRING (ruling 3, Q3, parity row R-019)
  and are not this unit's narrowing; (c) the four doors that create their table from the
  query type the column microseconds, as on main; (d) `EXPLAIN ANALYZE UPDATE` raises
  main's error (Q8). *Premise:* §13.11. *Lean:* keep the rulings; (a) can be built only by
  reading the plan a cache was built from, which ruling 4 declined.
- **V2 as narrowed, V5 as reversed, "reads are main's", `TRY_CAST`, F3** (RULING,
  provisional, built): §13.1. Overturning V5's reversal is deleting rows of `ROLES`.
- **Q7 and Q8** stand as ruled (C-026, C-027); Q8 is wider by 40 cells (§13.9 item 7).
