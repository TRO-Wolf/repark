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

**Beyond the named 60 (fix round, 2026-11-27).** The prior corpora, replayed against fresh Spark recordings, move
102 more cells. They are listed by id in
[`moved-beyond-60.tsv`](../../wo/fexpr-timestamp-ntz-1-narrow/evidence/moved-beyond-60.tsv):
- 27 `date_trunc` type cells (unit 2). `date_trunc` over an NTZ fragment stays `timestamp_ntz`, so CTAS and Parquet
  store an NTZ wall where Spark and main store an instant.
- 26 R2 refusals, plus 6 `insert_fexpr_lit_ltz` inserts that store nothing.
- 9 nullability-only cells.
- 8 EXPLAIN texts.
- 1 build-time-binding cell.
- 25 cells with no Spark analogue.

This round fixed two of this unit's own cells:
- an unparsable `CAST('bad' AS TIMESTAMP_NTZ)` under ANSI off now answers NULL;
- a pre-1970 bare literal is respelled `CAST('…' AS TIMESTAMP_NTZ)`. Main's `TIMESTAMP_NTZ '1969-…'` fails to plan on
  both doors, which is a card line for unit 2.

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
  - an `F.expr` built before a runtime `spark.conf.set("spark.sql.timestampType", …)` keeps the type in force when
    it was built, while Spark binds at use;
  - `unix_timestamp`, `make_timestamp_ntz` and `hour` over NTZ keep their nullability as on `spark.sql`.

## Pointers

- Up: [map.md](map.md)
