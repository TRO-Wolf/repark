# NTZ-LTZ-WIDENING-1 — `date_trunc` over `timestamp_ntz` returns `timestamp` on the session calendar, as Spark does

**Severity:** S1. Main returned the wrong type, and under a session built with NTZ it stored the wall where Spark
stores the instant.

**Parent:** [fexpr-timestamp-zone-1-card-2026-10-01.md](fexpr-timestamp-zone-1-card-2026-10-01.md), "Update
2026-11-27". This unit lands before #1444 (FEXPR-TIMESTAMP-NTZ-1 narrow).

**Evidence:** [../../wo/ntz-ltz-widening-1/evidence/](../../wo/ntz-ltz-widening-1/evidence/), machine counts only.

## Scope as shipped: the owner's round-3 split

The unit was meant to also widen an NTZ wall that meets an instant: union, CASE, `coalesce`, comparison, array
and map literals. The owner ruled on 2026-11-28 that the widening merges only if a two-seat fence holds; otherwise
`date_trunc` ships alone.

The fence failed because a third cancelling seat appeared. At the analyzer, the typed `TIMESTAMP '…Z'` literal
and `CAST('…Z' AS TIMESTAMP)` are the same `Cast`, yet Spark types them differently. Telling them apart needs the
SQL AST, and the fragment door has no AST lowering on main. The fragment door also types a column-free NTZ cast
as an instant, and main's UTC store cast cancels that.

`date_trunc`'s result type therefore ships alone. The coercion rule, the NTZ-session store cast and the zoned
literal are a separate design note, held by the orchestrator.

## The fix (`crates/repark-functions/src/`)

- `datetime.rs::DateTrunc::coerce_types`: a µs `timestamp_ntz` input coerces to the instant, so `date_trunc`
  returns `timestamp`. A `timestamp_ns` wall stays a wall, so a second analysis does not widen it.
- `datetime/ntz_ltz_widen.rs::SparkNtzLtzWiden` (with `date_trunc_input`) runs after the second `type_coercion`. The wall-to-instant cast on
  `date_trunc`'s timestamp argument becomes `zone_seam::seam_call`, the session-zone conversion the explicit
  cast uses. A UTC round trip collapses back to its instant.
- `zone_seam.rs::seam_call` is now `pub(crate)`.

## Cells closed

All counts are in the evidence directory.

- **Step 0** (76,104 cells; Spark 4.1.2 against main and head; sessions built LTZ, built NTZ and NTZ set at
  runtime; New York, Kolkata and UTC; both ANSI modes; `spark.sql` and the DataFrame API; select, Iceberg CTAS,
  `INSERT … SELECT`, `writeTo` and Parquet, read back on Spark):
  - TARGET: 7,708 closed, 0 other.
  - KNOWN_DIVERGENCE: 932 reach Spark's value.
  - 0 of 40,969 PROTECTED cells moved.
  - `new_wrong_store=0`.
- **Prior corpora** (TIMESTAMP-LTZ-EXPLICIT-CAST-1's grid, wrong-value probe, scout and cancel probes, and
  #1432's review suites):
  - 342 changed cells closed toward Spark, 0 moved, 119 changed but still unlike.
  - The 119 unlike cells are mostly `date_trunc` cells that now match Spark's type and value. Their remaining
    difference is a column name or nullability that main already had.
  - On main, the corpora reproduce their recorded answers: 39,093 cells, 0 differ.

## Still unlike, and where each piece goes

1. **NTZ wall meeting an instant under a session built with NTZ** (union, CASE, `coalesce`, comparison, literals)
   reads the wall as UTC; Spark localizes it. The coercion rule and the NTZ-session DML store cast must land
   together, after #1444. Measured alone, either one moves the cells where main's two UTC readings cancel.
   Design note: `ntz-session-note.md`, held by the orchestrator.
2. **A zoned `TIMESTAMP '…Z'` literal under NTZ** types `timestamp_ntz` (the UTC wall); Spark gives `timestamp`.
   It needs a session-aware AST lowering in both doors. Its own unit, after #1444.
3. **An NTZ value into an LTZ column under NTZ (R2)** refuses. Same store symbol as item 1.
4. **The 2 cells `replay_r|neighbours/TIMESTAMP_NTZ|America/New_York|ansi={false,true}/date_trunc_hr_ntz|select`:**
   the fragment door hands `date_trunc` a wall that holds an instant's UTC ticks. #1444 is their fix. This unit
   keeps them as main answers them, through the round-trip collapse.
5. **`date_format`, `from_utc_timestamp`, `to_utc_timestamp` and `months_between` over NTZ:** Spark takes
   `TimestampType` only and casts NTZ to it implicitly. `unix_micros`, `unix_millis` and `unix_seconds` refuse NTZ
   on Spark, but main answers. These are step 0's `ts-only-*` families, carded and not fixed.
6. **The NTZ string kernel's gap wall and pre-1970 literal refusal:** unit 2 (TIMESTAMP-NTZ-STRING-PARSE-1).

## Pins

- `python/repark/tests/timestamps/test_ntz_ltz_widening_1.py` holds 13 cells recorded on Spark by
  `_record_ntz_ltz_widening_1.py`. On main, the 3 TARGET cells are red and the 10 PROTECTED cells green; on head
  all 13 are green.
- `datetime/ntz_ltz_widen.rs` has two unit tests:
  - Unregistering the rule or reverting the coercion turns `date_trunc_of_a_wall_is_an_instant_on_the_session_calendar` red.
  - `a_wall_only_or_instant_only_union_keeps_its_type` guards the main risk: a union of walls or of instants
    changing type.
- `crates/repark-spark/tests/timestamp_ns_nested_shapes.rs` is unedited. Spark refuses `timestamp_ns`, and that
  test catches removal of the ns-wall arm.
