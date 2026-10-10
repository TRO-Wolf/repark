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
| C-024 | The fold adds no plan pass, and INSERT and MERGE over a table with no nanosecond column take the time they took on the base. | §11.9. | **PROVEN** | §11.9. |
| C-025 | The gates are green on the fold's final source with real exit codes. | §11.10. | **PROVEN** | §11.10. |
| C-026 | The `UNION` cell of `nvl` and `ifnull` (registry row R-019) keeps storing its cut value, as the ruling on Q3 reads. | Not built: measured, the cell is a nanosecond branch narrowed beside a microsecond one and the ruling on Q5 refuses it (§11.4). | **OPEN** | Q7. |
| C-027 | The 80 cells that refused by name before the rulings and raise the base's own error after them refuse by name again. | Not built: the guard cannot tell the planner's cast of an `UPDATE`'s `SET` value from a written `CAST(… AS TIMESTAMP)`, which Q1 says stores (§11.5). | **OPEN** | Q8. |

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

Recorded by the commit that closes the fold.

### 11.9 Cost

Recorded by the commit that closes the fold.

### 11.10 Gates on the fold

Recorded by the commit that closes the fold.

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
