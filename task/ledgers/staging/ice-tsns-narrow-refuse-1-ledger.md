# Unit ledger — ICE-TSNS-NARROW-REFUSE-1 · a narrowing the analyzer inserted beside an untyped NULL refuses by name

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

## PROPOSITION LEDGER — ICE-TSNS-NARROW-REFUSE-1 — 2026-10-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The base `8d1c4f49` is measured before any product change, five zones, values read from the Parquet files, and the 650-cell core of parity row R-017 reproduces. | §1, the recorder and the committed fixture. | **OPEN** | §1: recorded at commit `e5950aad`. |
| C-002 | Where the written and the inserted narrowing can be told apart is decided, with its alternatives, before the product code. | §2. | **OPEN** | §2. |

## 1. The base, measured before any change (C-001)

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

1. *Before the first coercion* (`HigherOrderPreparation`, the pass that already visits every
   expression before `type_coercion`): an untyped `NULL` literal that is a direct branch of a
   function call or a `CASE`, beside a branch whose Arrow type is a nanosecond timestamp,
   takes a tag in its literal metadata (`null_narrowing::tag_untyped_nulls`). The tree keeps
   its shape, so every rule between the two passes treats the literal as before.
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
two crates share only the wrapper's name, pinned equal as the store kernels' names are.
