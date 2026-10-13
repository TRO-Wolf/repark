# v1.5.5 release notes (2026-10-13)

v1.5.5 is the fifth patch on 1.5.0. It follows v1.5.4 (`f4e424c1`, notes
[v1-5-4-release-notes-2026-10-10.md](v1-5-4-release-notes-2026-10-10.md)). It carries two
timestamp fixes that lead this release — one instant for one wall in `timestamptz_ns` on
every write door (R-008) and the pandas/polars nanosecond frame door (R-019) — plus
`current_date` in the session zone, the Postgres write core with both-door routing, a
merge-module split, two lock-only dependency bumps, and test and docs groundwork.

The tag target is the release commit; the last product commit is `f8f1d252`
(CASESENS-1, metadata by version number). The tag and the PyPI upload are TODO(owner).

## One instant for one wall in `timestamptz_ns` (R-008, `26b378a4`)

Every write door stores INSERT's instant into a top-level `timestamptz_ns` column, at full
nanosecond precision. Into such a column INSERT localises a naive wall in the session zone;
before this release `INSERT OVERWRITE`, every MERGE arm, `UPDATE`, `overwritePartitions`
and `insertInto(overwrite=True)` stored the same wall as if it were UTC, so one table held
two instants for one input, and `UPDATE` with no `WHERE` raised a raw Arrow error. A `DATE`
past the range no longer panics, and Dictionary-encoded walls store INSERT's instant on the
overwrite doors.

Spark 4.1.2 with Iceberg 1.11 cannot write the type, so the reference is what INSERT
stores: 0 of 300 INSERT cells differ from the `zoneinfo` rule, and 0 cells of the three
door matrices differ from INSERT at head.

Ledger: [ice-tstzns-wall-1-ledger.md](../../ledgers/staging/ice-tstzns-wall-1-ledger.md).
Parity row ICE-TSNS-SQL-1-R-008 is FIXED 2026-10-10. Nested `timestamptz_ns` leaves still
read a wall as UTC on every door (card ICE-TSNS-NESTED-1), and the `EXPLAIN ANALYZE` and
`PREPARE`/`EXECUTE` carriers run the write without the INSERT conform (card
WRITE-BODY-CARRIERS-1).

## The frame door (ICE-TSNS-FRAME-DOOR-1, R-019, `f3f98823`)

In plain words, three things change in this release:

- pandas and polars nanosecond frames enter as microseconds, floored as Spark floors;
- appends into an existing nanosecond column now store microseconds;
- the native door still creates nanosecond tables that Spark 4.1.2 cannot open, until the
  create-door card lands (planned 1.5.6).

The detail: `createDataFrame` floors a top-level `datetime64[ns]` column on the int64
ticks, toward negative infinity as Spark does (`-1 ns` is `-1 us`, `-1001 ns` is `-2 us`),
never through local time, so it answers in every repeated and skipped hour and for every
int64 value. A categorical of nanoseconds is covered. A declared `timestamp_ntz` over a
nanosecond column answers Spark's microseconds where main raised `ArrowInvalid`. Under a
declared type that is not a timestamp (`bigint`, `string`, `date`) a nanosecond column
answers what main answered. The four append doors (`writeTo().append()`, `insertInto`,
`INSERT … SELECT` from a temp view, `saveAsTable` in append mode) store microsecond
precision with no error and no warning; keeping the digits is the create-door card's
session setting.

Ledger: [ice-tsns-frame-door-1-ledger.md](../../ledgers/completed/ice-tsns-frame-door-1-ledger.md)
(oracle: 90 live Spark cells; the UTC relabel and the 1.5.5 behaviour-change date are the
owner's rulings of 2026-10-13).

## `current_date` answers the session-zone date (`0e709d2f`)

DataFusion's builtin folds `current_date()` at plan time from `execution.time_zone`, which
RePark never sets, so it answered the UTC date under every session zone. The owned UDF
folds the query start time through the `SessionTimeZoneConfig` carrier instead; the native
door, which carries no session zone, keeps the builtin. The pre-existing strict-xfail TZ-9
pin converges and its registry row reads FIXED. SQL `curdate()` stays open (ledger C-006,
other cause).

Ledger: [current-date-session-zone-1-ledger.md](../../ledgers/staging/current-date-session-zone-1-ledger.md).

## The rest, briefly

- **Postgres write core (`4427fe63`, C-4 step 1):** one encoder writes every mapped type's
  binary wire form; the bulk path sends it as `COPY FROM STDIN BINARY`, the row path as
  binary `INSERT` parameters, with bulk-vs-row parity pinned. **Both-door routing
  (`caa57a50`, C-4 step 2):** the ANSI and Spark doors route `INSERT` to the shared sink
  with automatic row fallback; `overwrite` and upsert spellings refuse with the
  CONNECT-DECL rows; the `write.path` writer option (`bulk` default, `row` forces INSERT)
  is parsed by one core parser. Ledger: [c-4-ledger.md](../../ledgers/staging/c-4-ledger.md).
- **Merge module split (`7caa0cb7`):** `write/merge/mod.rs` sat exactly on its 1,569-line
  exception ceiling; the SQL builders, the copy-on-write arm and the merge-on-read arm move
  to sibling modules byte for byte. No behaviour change; no test file is edited.
- **Lock-only dependency bumps:** thiserror, tokio and the futures family (`572130dd`);
  pyo3 0.29.2 to 0.29.3 (`1640421b`). No dependency moves.
- **Tests and docs:** the coercion slice-0 recorders, Spark lattice, cost baseline and four
  censuses (`2ac03089`); the ten tests the nightly live run fails on (`26d8f33a`); the
  coercion-gate clerk round (`8e451f51`); the CASESENS metadata-by-version fix (`f8f1d252`);
  the cluster-status monotone-walk pin (`2c33f289`); the Iceberg streaming open decision,
  owner-held (`07be1dc9`); the R-011 park record (`8c51fbc4`); production-readiness and
  performance audits (`6fe92e12`); the cluster-feature tests in CI (`ba71b593`).

## Known, not in this release

- The read-side dropped digits beside an untyped NULL (card
  [ice-tsns-coercion-1-card-2026-10-10.md](ice-tsns-coercion-1-card-2026-10-10.md), in review).
- The create door (1.5.6).
- `applyInPandas` storing nanosecond integers as microseconds for nanosecond arrays and
  nested nanosecond fields (card NS-ARRAY-APPLY-IN-PANDAS-REFUSE-1, in review). OWNER-DECISION-PENDING

## Matrix

No ledger on main records a fresh 842-cell scoreboard run since the v1.5.4 build of
`e845b73b` (scoreboard 2026-10-10), so these notes keep the v1.5.4 count:
**705 EQUAL / 132 SPARK-CANNOT / 5 REFUSED-REGISTERED / 0 DIFFERENT**. The rerun on the
release build is TODO(orchestrator).
