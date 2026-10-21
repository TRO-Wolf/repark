# ZONE-ORACLE-1 — an expression-level Spark oracle for timestamps and zones     grade: C   engine band: Claude Sonnet 5.5 (xhigh), no verifier; the orchestrator reviews   release: 1.5.6

**Status:** order, **adopted by the owner on 2026-10-20** with the stability pathway. Runs on a
Sonnet 5.5 lane beside the Opus lane (the 2026-10-07 ruling: Sonnet for lower-logic work).
Written by Claude (claude-fable-5-1), orchestrator, on 2026-10-20.

## 0. Why

U-8's door matrix (3168 cells, two Opus rounds) still missed shapes its verifier found in an
afternoon, because every cell is a write through a door, and the number of door × shape × zone
combinations is unbounded. A write stores what `SELECT` yields. If an expression is EQUAL on a
plain `SELECT` against Spark in every zone, every door inherits it. The corpus below is recorded
once from Spark 4.1.2 and replayed on RePark; it is ZONE-SEAM-1's pin and, after that, the
parity-live nightly's guard against drift. No product code.

## 1. Deliverable

Under `python/repark/tests/timestamps/` (the directory #1113 created; its `map.md` row):

- `_zone_oracle_1_corpus.py` — a deterministic generator of SQL expressions. Axes: **value
  sources** (an instant literal `TIMESTAMP '2026-01-02 03:04:05.123456+00:00'`, a wall literal
  `TIMESTAMP_NTZ '…'`, a string `'2026-01-02 03:04:05.123456'`, a date `DATE '2026-01-02'`, a
  `BIGINT` epoch, NULL, and a column of each type from a one-row temp view); **leaf
  conversions** (`CAST`, `TRY_CAST`, `::`, `to_timestamp_ntz`, `to_timestamp_ltz`,
  `to_utc_timestamp`, `from_utc_timestamp`, `date_format`, `to_date`, `unix_micros`,
  `to_json`); **containers** (bare, `named_struct`, `array`, `map` with a literal key, one and
  two deep, struct inside array); **target types** (`TIMESTAMP_NTZ`, `TIMESTAMP`, `STRING`,
  `DATE`, `BIGINT`); **zones** (`UTC`, `America/New_York`, `Asia/Kolkata`, `+05:30`,
  `Etc/UTC`). Every cell is a `SELECT <expr> AS v` (or `SELECT <expr>['a']`, `[0]`, `.v` to
  read the leaf). Target about 1,500 cells; the generator prints its count and the cell ids
  are stable across runs.
- `_record_zone_oracle_1.py` — records Spark 4.1.2 (`local[1]`, its own interpreter, the
  recorder guard pattern of PR #1122: refuse any master other than `local[1]`) into
  `zone_oracle_1_spark_oracle.json`: per cell the id, the SQL, the zone, the value as Spark's
  `collect()` renders it (microseconds for timestamps, ISO for dates, the text for strings) or
  the error class. Provenance header: Spark version, date, the generator's hash.
- `test_zone_oracle_1.py` — replays every cell on RePark through `ReparkSession.sql()` in a
  session set to the cell's zone, and asserts EQUAL against the fixture. Cells DIFFERENT on
  main are **not** skipped and **not** xfailed one by one: the test loads
  `zone_oracle_1_main_difference.json`, a dated list recorded by the lane on main with the
  measured RePark value per cell, and asserts that the set of DIFFERENT cells is exactly that
  list. A cell that leaves the list (fixed) or joins it (regressed) fails the test, so the
  seam's progress and any regression both show. A live-oracle cell (`REPARK_PARITY_LIVE=1`)
  re-records twenty cells chosen by hash and compares them to the fixture.
- A short report `task/wo/zone-oracle-1/evidence/corpus-main-<sha>.md` with the counts per
  axis (EQUAL / DIFFERENT / REFUSED / ERROR) on main, the ten most common difference shapes,
  and the cells of card CONTAINER-CAST-ZONE-1 named by id.

## 2. Rules

No product code; no edit under `crates/`; no skips or xfails; the fixture is recorded, never
hand-edited; the recorder runs in its own interpreter; the test must pass co-collected with
`python/repark/tests/parity` and `python/repark/tests/writes` (one shared JVM in CI; the
live-cell rules of 2026-10-14 apply: guard the shared session, private catalog, no env pop).
Runtime of the replay under 60 s on the `ci` profile; say the measured number.

## 3. Gates

`make ci`, `python3 scripts/sync_map_md.py --check`, `uvx ruff@0.15.22 format` and `check`,
`uvx typos@1.47.2`, the facade suite with the new module, the live leg alone and co-collected.

## 4. Hand-back

The counts per axis on main, the runtime, the fixture size, the generator hash, and any axis
you dropped and why. The orchestrator reviews the corpus against the cards before the PR is
offered for merge; no Opus verifier.

Standing rules from [TEMPLATE.md](../TEMPLATE.md): no code comments from Anthropic models;
`map.md` lockstep; the scratch clone, never the live worktree; the TRO-Wolf identity and
exactly one trailer, `Authored-By: Claude (claude-sonnet-5-5) <noreply@anthropic.com>`.
