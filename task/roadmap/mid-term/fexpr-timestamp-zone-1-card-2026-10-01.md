# FEXPR-TIMESTAMP-ZONE-1 — F.expr TIMESTAMP literals resolve in UTC, not the session zone

**Filed: 2026-10-01 from the overnight owner-items backlog** (the owner-items list of
2026-09-29, line 72; #888 re-verify 10). **Severity:** wrong results, not yet rated (owner). **Status:** C-001 fixed by
DATE-PART-SESSION-ZONE-1 (PR #1420, 2026-11-24). The NTZ remainder below is open, and S2 is proposed for it.
**Target:** mid-term.

## The divergence it records

`F.expr` TIMESTAMP literals resolve in UTC instead of the session zone (a
DataFrame `F.expr` door gap; SQL resolves them in the session zone).

## Repro (from the source description; re-measure on the day)

```python
spark.conf.set("spark.sql.session.timeZone", "America/New_York")
spark.sql("SELECT TIMESTAMP '2024-06-01 12:00:00'").collect()  # session zone
F.expr("TIMESTAMP '2024-06-01 12:00:00'")  # RePark: UTC; Spark 4.1.2: session zone
```

## Cause and suggested fix direction (a suggestion, not a decision)

The `F.expr` door parses TIMESTAMP literals without the session zone the SQL
door applies. Any fix direction is suggested by the verifier/orchestrator, not
a decision.

## Clauses (draft)

- **C-001** `F.expr` TIMESTAMP literals answer the SQL door's values under a non-UTC zone.
- **C-002** UTC and fixed-offset zones pinned as controls.
- **C-003** The neighbouring session-zone literal cells still answer Spark.

## Update 2026-11-24: the zone is fixed, the NTZ settings are not

**What #1420 fixed.** `F.expr` eagerly analyses a column-free fragment in one process-global context that has no
session settings. That context folded `TIMESTAMP '…'`, `CAST('…' AS TIMESTAMP)` and `TRY_CAST(…)` into UTC instants.
In New York, `hour`, `day`, `date_format`, `CAST … AS STRING`, `unix_timestamp` and comparisons over those literals
were therefore wrong. PR #1420 (part 3) marks the context as zone-deferred: zoneless literal text binds the session
zone at use, and text carrying its own zone still folds. Those cells now equal Spark, and C-001 holds.

**What is left**, measured on #1420's merged head and on Spark 4.1.2, in both ANSI modes, in America/New_York,
Asia/Kolkata and UTC. `T` is `TIMESTAMP '2024-01-01 02:00:00'`.

| Cell | Door | Spark 4.1.2 | RePark |
|---|---|---|---|
| `CAST(T AS TIMESTAMP_NTZ)` | `spark.sql` | `timestamp_ntz` | `timestamp_ntz` |
| `CAST(T AS TIMESTAMP_NTZ)` | `F.expr` | `timestamp_ntz` | **`timestamp`** (same displayed value) |
| `T`, with `spark.sql.timestampType=TIMESTAMP_NTZ` | `spark.sql` | `timestamp_ntz` | `timestamp_ntz` |
| `T`, with `spark.sql.timestampType=TIMESTAMP_NTZ` | `F.expr` | `timestamp_ntz` | **`timestamp`** (same displayed value) |
| unaliased `F.expr("TIMESTAMP '2024-01-01 02:00:00Z'")`, New York | column name | `TIMESTAMP '2023-12-31 21:00:00'` | `TIMESTAMP '2024-01-01 02:00:00Z'` |

**Likely cause (read, not traced).** `build_expr_context()` (`crates/repark-python/src/column/expr_build.rs`) carries
the default `timestampType`, LTZ, so the NTZ cast and the typed literal resolve there instead of in the session. The
risk is an LTZ-typed column holding an NTZ wall clock that is later read as an instant, after a write into an NTZ
column or a zone change. That is not yet measured.

**Not this card** (measured on both doors, the same on main):
- `TIMESTAMP 'bad'` and `TIMESTAMP '2024-01-01 02:00:00[UTC]'`: Spark raises `ParseException`. RePark answers NULL with
  ANSI off and raises `CAST_INVALID_INPUT` with ANSI on. This belongs with
  [SPARK-DOOR-OVER-ACCEPT-SYNTAX-1](cards-2026-11-23/spark-door-over-accept-syntax-1-card-2026-11-24.md).
- `T - TIMESTAMP '2024-01-01 00:00:00'`: `dtypes` reports `string` where Spark reports `interval day to second`, and
  the value agrees.

## Update 2026-11-27: FEXPR-TIMESTAMP-NTZ-1 narrow, the fragment door (unit 1 of #1432's split)

#1432 was closed and its work split in three units:
- **unit 1 (this one):** the fragment door;
- **unit 2, TIMESTAMP-NTZ-STRING-PARSE-1:** the NTZ string kernel and `date_trunc` over NTZ;
- **unit 3, TIMESTAMP-LTZ-EXPLICIT-CAST-1:** the SQL door's explicit LTZ lowering.

Unit 1 leaves the SQL door byte-identical. The SQL-door grid of 7,560 cells equals main on every cell, and so does
every `spark.sql` and `selectExpr` cell in step 0. Measured on Spark 4.1.2 in New York, Kolkata and UTC, both ANSI
modes. The counts are machine-emitted under
[`task/wo/fexpr-timestamp-ntz-1-narrow/evidence/`](../../wo/fexpr-timestamp-ntz-1-narrow/evidence/).

**What changes.**
- `F.expr` lowers an explicit `CAST`/`::`/`TRY_CAST … AS TIMESTAMP_NTZ` in a column-free fragment to the NTZ cast
  kernel, so `CAST(T AS TIMESTAMP_NTZ)` is `timestamp_ntz`.
- Under `spark.sql.timestampType=TIMESTAMP_NTZ`, a bare `TIMESTAMP` is respelled `TIMESTAMP_NTZ` where it names a
  type. The setting is read from the active session when the `F.expr` is built, so a runtime `spark.conf.set` counts.
- A zoned `TIMESTAMP '…Z'` literal stays an instant, as Spark's parser keeps it.

**Closed:** 1,824 of 4,102 TARGET cells.
- Main stores 236 TARGET values that Spark does not store. This unit fixes 176 of them.
- Of the other 60, 48 stay as on main, and 12 get the right type with the value still an hour off. By cause, the 60
  are:
  - 39 column-bearing casts, which keep main's route until unit 2;
  - 15 DST-gap string casts, which wait on unit 2's kernel;
  - 6 zoned literals written into an NTZ column under an NTZ-built session.
- NTZ walls now survive a session-zone change: 34 cells.

**The named exception: 60 cells that move off Spark**, listed id by id in
[`moved-60.tsv`](../../wo/fexpr-timestamp-ntz-1-narrow/evidence/moved-60.tsv).
- **R2 (30 refusals).** Under an NTZ-built session, `writeTo` of an NTZ fragment into a `TIMESTAMP_LTZ` Iceberg
  column refuses with `cannot convert text Timestamp(µs) into byte Timestamp(µs, "UTC")`.
  - Main already refuses so for `TIMESTAMP_NTZ '…'` and `to_timestamp_ntz`.
  - The cause is `repark-iceberg` `write/conform_stage.rs::leaf_store`. Its fix is a follow-up unit after unit 3.
- **R3 (24 nullability flags).** `unix_timestamp(<non-null NTZ>)` is not nullable under ANSI off, where Spark says
  nullable. `spark.sql` answers the same on main. The cause is `unix_timestamp`'s nullability in `repark-functions`.
- **R4 (6 comparisons).** A comparison with `s.cast(TimestampNTZType())` at a DST gap now answers false where Spark
  and main answer true. The column holds unit 2's gap shift, and main was right only because both sides shifted
  alike.

**Beyond the named 60 (delta 1, 2026-11-27, merged with main at `9df269acb`).** The prior corpora, run on head and
main the same day, and the verifier's probes move 167 more cells. Each is listed by id, family and cause in
[`moved-beyond-60.tsv`](../../wo/fexpr-timestamp-ntz-1-narrow/evidence/moved-beyond-60.tsv).
- **Excepted by family (owner, 2026-11-27):**
  - 51 R2 refusals;
  - 15 nullability-only cells (R3-like);
  - 8 EXPLAIN texts;
  - 26 cells with no Spark answer.
- **Not excepted; NTZ-LTZ-WIDENING-1 fixes them before this unit lands:**
  - 37 `date_trunc` type cells. `date_trunc` over an NTZ fragment stays `timestamp_ntz`, so CTAS and Parquet store
    an NTZ wall where Spark and main store an instant.
  - 22 union-widening cells (F3). A DataFrame `union` of an NTZ fragment with an LTZ column widens the NTZ side as
    UTC, not through the session zone. In New York `TIMESTAMP '2024-03-10 02:30:00'` is stored at
    `1710037800000000`, where Spark and main store `1710055800000000`.
- **Other:**
  - 6 `insert_fexpr_lit_ltz` inserts store nothing. This is the R2 refusal, read back as an empty table.
  - 1 build-time-binding cell, carded below.
  - 1 R4 comparison beyond the 60 (`vexpr`, runtime NTZ). The ruling names no R4 family.
- The 198 time-only corpus cells were measured on head and main on the same day. They move onto Spark: NULL with ANSI
  off, `CAST_INVALID_INPUT` with ANSI on. In total, 1,108 corpus cells move onto Spark.

**Fixed in this unit's own rounds.**
- **Fix round:**
  - an unparsable `CAST('bad' AS TIMESTAMP_NTZ)` under ANSI off answers NULL;
  - a pre-1970 bare literal no longer fails to plan.
- **Delta 1:**
  - **Int32 walls (F1).** A bare `TIMESTAMP '…'` whose NTZ wall fits Int32 is respelled
    `CAST('…' AS TIMESTAMP_NTZ)`. That range runs from `1969-12-31 23:24:12.516352` to
    `1970-01-01 00:35:47.483647`.
    - Main's `TIMESTAMP_NTZ '…'` rewrite passes the wall as an integer argument. On the fragment door that integer
      is narrowed to Int32, and the wall function refuses an Int32.
    - Before delta 1, `TIMESTAMP '1970-01-01'` failed to plan here. It now answers, and it is stored as Spark
      stores it.
    - Both edges are pinned.
  - **String casts the kernel cannot read (F2).** The fragment door lowers a string-literal NTZ cast only where
    the NTZ kernel reads the text, or where Spark's cast answers no value.
    - The kernel reads only 1677-09-21 to 2262-04-11. It does not read `hh:mm`, single-digit fields or padded
      text.
    - Any other literal keeps main's route. So `CAST('0001-01-01 00:00:00.000001' AS TIMESTAMP_NTZ)` stores the
      wall again, where the fix round stored NULL or raised `CAST_INVALID_INPUT`.

**Still unlike Spark, by owner.**
- **Unit 2:**
  - the NTZ string kernel moves a DST-gap wall: `CAST('2024-03-10 02:30:00' AS TIMESTAMP_NTZ)` answers `03:30` in
    New York on `spark.sql` too, where Spark answers `02:30`;
  - it accepts bracketed zones on column sources;
  - `TIMESTAMP '… hh:mm'` without seconds is not respelled, since the NTZ wall parser refuses it.
- **Unit 3, and the SQL-door runtime `timestampType` line:**
  - `selectExpr`, which runs on the SQL door;
  - the SQL door's own explicit-LTZ and zoned-literal gaps.
- **This card:**
  - under an NTZ-built session, a string NTZ cast the kernel cannot read keeps main's route, and main's stored
    wall, shifted by the zone or local mean time;
  - an `F.expr` built before a runtime `spark.conf.set("spark.sql.timestampType", …)` keeps the type in force when
    it was built, while Spark binds at use;
  - `unix_timestamp`, `make_timestamp_ntz` and `hour` over NTZ keep their nullability as on `spark.sql`.

## Pointers

- Up: [map.md](map.md)
