# TIMESTAMP-LTZ-EXPLICIT-CAST-1 — an explicit LTZ instant types and stores a mixed `VALUES` column as Spark does

**Severity:** S1 (main stores a wrong value). **Unit 3 of #1432's split**, run before unit 2 (owner, 2026-11-27).
Parent: [fexpr-timestamp-zone-1-card-2026-10-01.md](fexpr-timestamp-zone-1-card-2026-10-01.md), "Update 2026-11-24".
Evidence: [../../wo/timestamp-ltz-explicit-cast-1/](../../wo/timestamp-ltz-explicit-cast-1/).

## The divergence

Spark widens a `VALUES` column to `timestamp` when an LTZ cell meets an NTZ cell, localizing each NTZ wall in the
session zone. Main missed the evidence in four shapes and, in sessions built with `timestampType=TIMESTAMP_NTZ`, did
not localize. New York, Iceberg, read back by Spark:

| Session | Cells | Spark 4.1.2 | Main |
|---|---|---|---|
| built LTZ, NTZ column | `COALESCE(CAST('2024-06-01 12:00:00' AS TIMESTAMP_LTZ), NULL)`, `TIMESTAMP_NTZ '2024-03-10 02:30:00'` | `12:00`, `03:30` | `12:00`, `02:30` |
| built NTZ, NTZ column | `to_timestamp_ltz('2024-06-01 12:00:00')`, the same NTZ literal | `12:00`, `03:30` | `16:00`, `02:30` |
| built NTZ, LTZ column | the same | `12:00`, `03:30` | `12:00`, `2024-03-09 21:30` |

## The fix (`crates/repark-functions/src/timestamp_ns_cast{,/values_evidence}.rs`)

- `coalesce`, `nvl`, `ifnull`, `if` and `CASE` answer their branches' instant evidence (cause 1).
- A named instant stays one under `± INTERVAL` (cause 2), and `to_timestamp_ltz` is one (cause 3).
- Under NTZ: the insert's own naive target cast over a named instant is read as the instant (cause 4); walls are
  localized with `to_timestamp` (cause 5); an NTZ-target column stays naive and stores the session wall.

## Cells closed (machine counts in the evidence directory)

- Extended grid (22,680 cells): 540 TARGET-A, 270 TARGET-B (stores Spark's value; the rest keep main's refusal) and
  474 TARGET-C cells closed; 0 of 16,624 cells that equal Spark moved, including the 736 KERNEL-CANCEL cells.
- Wrong-value probe (2,880 cells): 166 closed (INSERT VALUES, MERGE from VALUES, CTAS from VALUES, Parquet), 0 moved.
- Review suites of #1432 (10,587 cells) and the scout probes: 0 moved; 3 scout cells closed.

## Still unlike, and where each family goes

1. **Built NTZ, explicit `CAST(… AS TIMESTAMP_LTZ)`, `'…'::TIMESTAMP_LTZ`, `TIMESTAMP_LTZ '…'` and the zoned
   `TIMESTAMP '…+00:00'`** type `timestamp_ntz` on the SQL door (`keyword_lower.rs` lowers `TIMESTAMP_LTZ` to bare
   `TIMESTAMP`; `instant_ts.rs::rewrite_cast_as_ntz` types it as a wall; the zoned literal drops its offset). The
   `VALUES` widening cannot see an instant, so an NTZ gap literal beside one stores `02:30` (Spark `03:30`) and an LTZ
   column refuses (Spark stores). This is cause 6 and its cast siblings: a follow-up unit with a seat in
   `instant_ts.rs`, which this unit's fence excludes. Grid: 3,240 TARGET-B refusals kept; the remaining 912
   TARGET-A and 620 TARGET-C cells (families 1 and 2) keep main's answers.
2. **`NVL(<LTZ>, NULL)`** beside an NTZ cell refuses `CANNOT_SAFELY_CAST` (the `NULL` types as a string), and with the
   NTZ literal it stores `02:30`. Its own card.
3. **`INSERT … SELECT … UNION ALL` under built NTZ** stores an instant's UTC wall into an NTZ column (Kolkata `06:30`
   for `12:00`) and shifts an LTZ column: the DML store cast is not retargeted under NTZ. Not a `VALUES` column; it
   goes with the [`leaf_store`](../../../crates/repark-iceberg/src/write/conform_stage.rs) follow-up.
4. **The bare NTZ cast and `to_timestamp_ntz` of a New York gap wall** answer `03:30` where Spark keeps `02:30`:
   unit 2 (the NTZ string kernel). This unit keeps the 736 cells where that bug cancels on Spark's answer.
5. **NOT NULL columns:** a `NULL` into a required column refuses with Arrow's text where Spark raises
   `NOT_NULL_ASSERT_VIOLATION` (unchanged from main).
6. **From #1432's verdict.** F4: a 2,000-column projection of LTZ casts takes about 17 s on main's LTZ path against
   about 4 s on the NTZ path. F5: under NTZ, `coalesce(ntz, ltz)` and `coalesce(d, ltz)` give `timestamp_ntz` where
   Spark gives `timestamp`; `CAST(dbl AS TIMESTAMP_LTZ)` is not nullable where Spark says nullable;
   `to_timestamp_ntz(ltz)` is nullable where Spark says not nullable; `to_timestamp(<format>)` and `to_timestamp(d)`
   nullability under LTZ; `to_timestamp_ltz(<non-null ntz>)` is nullable under LTZ sessions too.
7. **Built NTZ, three more shapes store wrong values, unchanged from main** (New York, Iceberg, read back by Spark;
   found by #1445's verifier):
   - a named instant beside a wrapped NTZ wall, such as `coalesce(TIMESTAMP_NTZ '2024-11-03 01:30:00', NULL)`: into
     an NTZ column the instant stores its UTC wall (`16:00`, Spark `12:00`), and into an LTZ column the wall stores
     as UTC (`2024-11-02 21:30`, Spark `01:30`); CTAS, Parquet and `MERGE` too. A wrapped wall answers no evidence;
   - a named instant beside a `DATE` cell: into NTZ `16:00`; into LTZ, and in `SELECT`, the date is UTC midnight
     (`2024-03-09 19:00`, Spark `2024-03-10 00:00`);
   - `current_timestamp()` or `now()` beside an NTZ gap wall: the wall stores `02:30` into NTZ and `2024-03-09 21:30`
     into LTZ, where Spark stores `03:30`.

   They go to the same follow-up unit as cause 6.

## Out of scope, byte-identical to main

The fragment door (`values_marker.rs::plan_marked_fragment`, `keyword_lower.rs` fragment entry points,
`ntz_cast_lower.rs`, `timestamp_type.rs::typed_literal_resolves_ntz`, `expr_build.rs`, the Python `F.expr` facade):
unit 1. The NTZ string kernel `__repark_cast_timestamp_ntz__` and `date_trunc` over NTZ: unit 2.
`write/conform_stage.rs::leaf_store`. The SQL door's runtime `spark.sql.timestampType` (sessions that set NTZ at
runtime keep main's answers in the grid).

## Pins

`python/repark/tests/timestamps/test_timestamp_ltz_explicit_cast_1.py` replays 19 Spark-recorded cells
(`_record_timestamp_ltz_explicit_cast_1.py`): 11 TARGET (red on main), 7 PROTECTED and 1 Spark-or-refuse.
`crates/repark-functions/src/timestamp_ns_cast/tests.rs`: three tests cover the five causes; each cause's mutant
turns at least one red (M3 turns three, and M4 and M5 share one).

## Pointers

- Up: [map.md](map.md)
