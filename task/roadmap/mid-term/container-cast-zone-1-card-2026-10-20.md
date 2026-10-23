# Card CONTAINER-CAST-ZONE-1: a container cast renders a zoned timestamp leaf in UTC, not the session zone, for every target type, on every door

**Date:** 2026-10-20. **Filed by:** Claude (claude-fable-5-1), orchestrator, from the second
hostile verification of PR #1124 (ICE-NTZ-NESTED-REFUSE-1, U-8) on main `c516c54a`.

**Status 2026-10-22: fixed by ZONE-SEAM-1** (PR #1139, pending the verifier): the seam converts
every container cast, `TRY_CAST` and `to_json` this card names; its tables are pinned in
`python/repark/tests/timestamps/test_zone_seam_1.py` and the corpus cells in
`zone_oracle_1_main_difference.json`. Follow-up lines (rulings ZS-1, ZS-5):
- the ZONE-ORACLE-1 residue that is not a zone defect becomes two small units:
  **TIMESTAMP-LEGALITY-PARITY-1** (error conditions and function legality: `STRING` and `DATE` to
  `BIGINT` in and out of a container, `unix_micros` and the `BIGINT` arguments of
  `to_timestamp_ntz`, `to_utc_timestamp`, `from_utc_timestamp`, `date_format`, `to_date`; 80
  cells) and **DATE-FORMAT-FRACTION-1** (`date_format` with the `S` letter; 10 REFUSED cells);
- native-door zone semantics: owner decision, unscheduled.

**Status (2026-10-20, superseded above):** open, not scheduled. The owner rules whether U-8 grows to gate the write-side cells
below, or whether this card is a unit of its own that fixes the cast. The orchestrator's lean is
the second: the mechanism is a read-path defect that a write gate can only paper over, and the
cells it loses on a plain `SELECT` and on the six table-creating writers are not U-8's.

**Retires:** when `CAST(<struct|array|map holding a zoned leaf> AS <the same shape with STRING,
DATE, BIGINT or TIMESTAMP_NTZ at that leaf>)`, `TRY_CAST` of the same, and `to_json` of a zoned
leaf render in the session zone as the leaf casts `CAST(x AS STRING)` and
`CAST(x AS TIMESTAMP_NTZ)` already do, on every door, with the controls below unchanged and a
pin per spelling.

## What is wrong

RePark's leaf cast of a zoned timestamp is session-aware: in `America/New_York`,
`CAST(TIMESTAMP '2026-01-02 03:04:05.123456+00:00' AS TIMESTAMP_NTZ)` stores Spark's
`1767305045123456`. The same value cast inside a container is rendered in UTC. The verifier
measured these on main `c516c54a` and on PR #1124's head `c20c3e25`; both answer alike, so
nothing here is new on the PR. Spark 4.1.2 `local[1]`, session `America/New_York`, the instant
`TIMESTAMP '2026-01-02 03:04:05.123456+00:00'` as `x`, values in microseconds.

**Plain `SELECT`, no write:**

| Statement | RePark | Spark 4.1.2 |
|---|---|---|
| `SELECT CAST(array(x) AS ARRAY<TIMESTAMP_NTZ>)[0]` | `1767323045123456` | `1767305045123456` |
| `SELECT s.v FROM (SELECT CAST(named_struct('v', x, 'n', 1) AS STRUCT<v: TIMESTAMP_NTZ, n: INT>) AS s) q` | `1767323045123456` | `1767305045123456` |
| `SELECT CAST(map('a', x) AS MAP<STRING, TIMESTAMP_NTZ>)['a']` (a map literal; see MAP-COLUMN-PANIC-1 for a map over a column) | `1767323045123456` | `1767305045123456` |
| control `SELECT CAST(x AS TIMESTAMP_NTZ)` | `1767305045123456` | `1767305045123456` |

**Through another type.** Source `ice.ns.src (id INT, x TIMESTAMP, w TIMESTAMP_NTZ)`, target
`ice.ns.t (id INT, st STRUCT<v: TIMESTAMP_NTZ, n: INT>, k INT)`, both Iceberg through the
catalog:

| Write | RePark | Spark 4.1.2 |
|---|---|---|
| `INSERT INTO ice.ns.t SELECT id, CAST(CAST(named_struct('v', x, 'n', 1) AS STRUCT<v: STRING, n: INT>) AS STRUCT<v: TIMESTAMP_NTZ, n: INT>), 0 FROM ice.ns.src` | `1767323045123456` | `1767305045123456` |
| the same as `MERGE … WHEN MATCHED THEN UPDATE SET t.st = CAST(CAST(named_struct('v', s.x, 'n', 1) AS STRUCT<v: STRING, n: INT>) AS STRUCT<…>)` | `1767323045123456` | `1767305045123456` |
| `ARRAY<TIMESTAMP_NTZ>` column: `… CAST(CAST(array(x) AS ARRAY<STRING>) AS ARRAY<TIMESTAMP_NTZ>) …` | `1767323045123456` | `1767305045123456` |
| the leaf cast over a string the container made: `… named_struct('v', CAST(CAST(array(x) AS ARRAY<STRING>)[0] AS TIMESTAMP_NTZ), 'n', 1) …` | `1767323045123456` | `1767305045123456` |
| through `DATE`: `… CAST(CAST(named_struct('v', x, 'n', 1) AS STRUCT<v: DATE, n: INT>) AS STRUCT<…>) …` | `1767312000000000` (the UTC date) | `1767225600000000` (the New York date) |
| through `BIGINT`: `… STRUCT<v: BIGINT, n: INT> …` | `1767323045123456` | `[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION]` |
| `… from_json(to_json(named_struct('v', x, 'n', 1)), 'v TIMESTAMP_NTZ, n INT') …` | `1767323045123000` | `NULL` |

**The six table-creating writers**, each with `CAST(named_struct('v', x, 'n', 1) AS
STRUCT<v: TIMESTAMP_NTZ, n: INT>)` in the select: `CREATE TABLE … USING iceberg AS SELECT`,
`CREATE OR REPLACE TABLE … AS SELECT`, `REPLACE TABLE … AS SELECT`,
`df.write.mode("overwrite").saveAsTable(...)`, `df.writeTo(...).create()`, `.replace()` and
`.createOrReplace()`. RePark stores `1767323045123456`; Spark stores `1767305045123456`
(measured for CTAS and `CREATE OR REPLACE`). R-015 lists CTAS as allowed, and U-8's gate does
not reach these writers.

**A value made before the write.** A cached frame of a container cast, written with
`writeTo().append()`, carries the UTC wall into the table on main and on the PR head; the
cast, not the write, produced it.

## 2026-10-20, later: what the ZONE-ORACLE-1 corpus adds (PR #1128, 1,640 plain `SELECT` cells on main `754fb6e5`)

- **Confirmed on plain `SELECT`.** Every container cast of an instant to a naive leaf is DIFFERENT
  in `America/New_York`, `Asia/Kolkata` and `+05:30`, and EQUAL in `UTC` and `Etc/UTC`; the date
  cast differs only in New York (the UTC date and the Kolkata date coincide for the probe instant).
- **The container cast is format-wrong, not only zone-wrong.** A container cast to `STRING` renders
  ISO text with `T` and `Z` where Spark renders `yyyy-MM-dd HH:mm:ss[.SSSSSS]` in the session zone,
  and a container cast to `BIGINT` gives microseconds where Spark gives seconds, in every zone, UTC
  included. Fixing the zone alone leaves those DIFFERENT; routing each container leaf through the
  leaf UDFs (the ZONE-SEAM-1 shape) fixes both at once.
- **`to_json` is narrower than first written.** `to_json` of an instant leaf is EQUAL in all five
  zones; `to_json` of a `TIMESTAMP_NTZ` leaf is DIFFERENT in all five (a trailing `Z`). The seam's
  `to_json` step is the NTZ rendering, not the instant.
- **A leaf conversion inside a container is EQUAL in every zone** wherever the engine answers:
  `named_struct('v', CAST(x AS TIMESTAMP_NTZ))` is right; only the cast of the container is wrong.
- **Two leaf defects join the seam.** The SQL leaf `TRY_CAST(x AS TIMESTAMP_NTZ)` is not
  session-aware on this corpus (the DataFrame `try_cast` is, per U-8's verifier). `to_utc_timestamp`
  and `from_utc_timestamp` of a wall literal return `TIMESTAMP_NTZ` with a shifted wall where Spark
  returns `TIMESTAMP` (NTZ-7 pinned the instant input only).

## Why U-8 does not close it

U-8 (row R-011, T-3) refuses an instant that reaches a nested microsecond `TIMESTAMP_NTZ` leaf
outside UTC, and after its first fix round also refuses a `CAST`/`TRY_CAST` that turns a zoned
type into a naive one. Its gate fires when one cast both reads a zoned type and produces a
naive one. Every row above passes through STRING, DATE, BIGINT or JSON first, so the cast the
gate sees reads a naive type, and the UTC wall was made upstream by the container cast. A
gate that followed the zoned value through every intermediate type would be the lineage walk
that U-8's second verifier also asked for on the over-refusal side, and it would still leave
the plain `SELECT` and the six writers wrong. The honest fix is the cast.

## Where to look

- The leaf cast is lowered to a session-aware function on the Spark door (the fix hand-back of
  PR #1124 names the compile path; U-8's verifier confirmed every leaf spelling stores the
  session wall). The container cast goes through DataFusion's `CastExpr` with no such lowering.
- The same lowering, applied to a `Cast`/`TryCast` whose target type holds a zoned leaf at any
  depth, is the candidate shape. `to_json` is a second site.
- The U-8 verifier's probe script and its measured jsonl for main, head and Spark are the
  measurement corpus; when this unit is scheduled, they move under
  `task/wo/container-cast-zone-1/evidence/` with a `map.md`.

## Out of scope

U-8's own refusal (its row says this card by name as a residue); the `map(<column>)` panic
(MAP-COLUMN-PANIC-1); whether a view captures the session zone of its creation (U-8 verifier
note S3-5, unmeasured on Spark).
