# STRING-HOUR-MINUTE-SECOND-1 — the build's bar (seat A, head `58b2725e`)

Head package `pkg-shms-58b2725e` (`BUILT_FROM` `58b2725e`, the product commit) against the base `pkg-sum-845766fd`
(main `2900efde`; `git diff --name-only 2900efde 81ae984e -- crates python/repark/src` is empty) and Spark 4.1.2's
recording from step 0. The base package reproduces step 0's `pkg-main-4c365814` on all 75,032 cells (0 moved). Lane
A, under the shared measure lock; every run printed `repark._native.__file__`. The probes are in
[evidence/built/](evidence/built/map.md); raw runs stay in `N/string-hms-out/`.

## The rulings this unit carries

- **Owner, 2026-11-19, about 12:17 UTC, verbatim: "Defaults on all six, merge 1369 on green".** It adopted the six
  defaults of [sketch.md](sketch.md) §7: (1) seat A, in `time_family.rs` only, a `STRING` argument coerced to the
  instant type, `datetime.rs` not touched; (2) scope `hour`, `minute` and `second` only, N1–N11 cards filed by the
  orchestrator's clerk; (3) the silent neighbours S1 and label-blocking, the loud ones S2; (4) the cost bar: K1
  `hour(c)` at 1.05x of K1x, `hour(CAST(c AS TIMESTAMP))`, K2 and C-012 at 1.05x of today; (5) N8, the `F.expr`
  literal cast read as UTC, its own card; (6) under `TIMESTAMP_NTZ`, `hour`, `minute` and `second` over `off`,
  `zulu`, `gap` and `tonly` pinned to Spark's instant answers.
- **Fable's guidance** (`claude-fable-5-2`, 2026-11-19, reader-parse step 0 reply §2 Q6; guidance, not an owner
  ruling): the card is S1 by class and label-blocking, as its own unit.
- **Owner, 2026-11-19, about 14:00 UTC, verbatim: "go with recommendations".** It adopted Fable's recommendations
  (reader-parse units, cost noise and the next queue, §9) as the orchestrator listed them. Two bind this unit: the
  order (§5: this unit builds on lane A beside unit 0's re-check verifier; neither it nor unit 1 waits for the other;
  whichever merges second re-runs the 92 D4 cells and records their final verdict, and until then both name them "D4,
  wrong on base and head alike") and the cost gate for short rows (§2.2, applied in [cost.md](cost.md)). It also
  grants a merge on green CI and a clean verifier PASS.
- **Owner, 2026-11-19, about 15:47 UTC, verbatim: "accept the three N8 cells, carry on".** It accepts the three
  EQUAL cells lost inside the N8 forecast ("Lost EQUAL: 3" below); they stay with the N8 card.

## What was built

- `crates/repark-functions/src/time_family.rs`: `DatePartWithoutTime::coerce_types` answers
  `instant_ts::ltz_timestamp_type()` (`Timestamp(µs, "UTC")`) for one `Utf8`, `LargeUtf8` or `Utf8View` argument and
  delegates every other type to the inner `DatePartUdf` as before. Five Rust tests in its `mod tests`.
- `DatePartWithoutTime` has three constructors, `hour`, `minute` and `second`, reached by the registry
  (`datetime::functions`, `datetime.rs:52-54`) and by `F.hour` / `F.minute` / `F.second` (`expr_fn.rs:225-238`).
  No other function shares it: `year` … `weekday` are `DatePartUdf` without the wrapper and keep their `Date32` cast
  (`EXPLAIN` below), `EXTRACT` and `F.extract` / `F.date_part` reach DataFusion's `DatePartFunc`, SQL `date_part`
  datafusion-spark's `SparkDatePart`. The wrapper registers no alias.

## Step 0's whole table (75,032 cells per engine, all 58 functions)

| scope | base EQUAL / WRONG / RAISE | head EQUAL / WRONG / RAISE |
|---|---|---|
| `hour`, LTZ projection (1,024) | 128 / 384 / 512 | **1,014** / 10 / 0 |
| `minute`, LTZ projection (1,024) | 128 / 384 / 512 | **1,024** / 0 / 0 |
| `second`, LTZ projection (1,024) | 256 / 256 / 512 | **1,024** / 0 / 0 |
| `hour`, NTZ (256) | 32 / 96 / 128 | **251** / 5 / 0 |
| `minute`, NTZ (256) | 32 / 96 / 128 | **256** / 0 / 0 |
| `second`, NTZ (256) | 64 / 64 / 128 | **256** / 0 / 0 |
| the three, multi-row doors (216) | 0 / 0 / 216 | **216** / 0 / 0 |
| the other 55 functions, LTZ (56,896) | 24,410 / 9,964 / 22,522 | the same, 0 moved |
| the other 55 functions, NTZ (14,080) | 5,708 / 2,542 / 5,830 | the same, 0 moved |
| all | 30,758 / 13,786 / 30,488 | 34,159 / 12,521 / 28,352 |

- **The forecast holds cell for cell:** `hour` 1,014, `minute` and `second` 1,024 of 1,024 LTZ; 251 / 256 / 256 of
  256 NTZ. The 15 cells left are N8 (`F.expr` over a string literal, Los Angeles, `hour` of `date`, `full`, `tsep`,
  `frac`, `gap`, in `a0-la`, `a1-la`, `a1-la-ntz`): the literal is read as UTC (`19` for `full`, Spark `3`). No cell
  outside the forecast.
- **Moved:** 3,419 cells, all `hour` (1,195), `minute` (1,192) and `second` (1,032); 3,404 gained EQUAL.
- **Lost EQUAL: 3**, all inside the forecast's N8 residue: `hour` of `date` (`2024-01-01`) on `F.expr` over the
  literal in `a0-la`, `a1-la` and `a1-la-ntz`. Main answered `0`, Spark's answer, because it read a midnight `DATE`;
  the head reads the text as the UTC instant on that door (N8) and answers `16`. The other 12 N8 cells were WRONG on
  main (`0`) and are WRONG on the head (`19`, `18`, `16`). The owner accepted the three at about 15:47 UTC (above).
- **The 216 multi-row cells** (`WHERE`, `GROUP BY`, `ORDER BY` on SQL and on the frame, inputs `I`, `P`, `V`, four
  configurations, the twelve-row table): all 216 RAISE on base (the table holds `lead`, `trail`, `nosec`, `yonly`,
  which arrow's date cast refuses), all 216 EQUAL on the head.

## `EXPLAIN` ([evidence/built/explain-base.jsonl](evidence/built/explain-base.jsonl), [explain-head.jsonl](evidence/built/explain-head.jsonl))

One Iceberg row, `America/Los_Angeles`, ANSI on.

| query | base | head |
|---|---|---|
| `hour(c)` | `hour(CAST(lc.db.t.c AS Date32))`, `0` | `hour(to_timestamp(lc.db.t.c)) AS hour(lc.db.t.c)`, `3` |
| `minute(c)` | `minute(CAST(lc.db.t.c AS Date32))`, `0` | `minute(to_timestamp(lc.db.t.c))`, `4` |
| `second(c)` | `second(CAST(lc.db.t.c AS Date32))`, `0` | `second(to_timestamp(lc.db.t.c))`, `5` |

Twelve plans that do not change, byte for byte (logical and physical), with the same answer: `year(c)`, `month(c)`,
`dayofmonth(c)` (`CAST(c AS Date32)`), `extract(HOUR FROM c)` (`date_part('HOUR', __repark_cast_timestamp_ns__(c))`),
`hour(CAST(c AS TIMESTAMP))` (`hour(to_timestamp(c))`, the head's `hour(c)` plan), `hour(ts)`, `hour(d)`,
`date_trunc('HOUR', c)`, `date_format(c, 'HH')`, `unix_timestamp(c)`, `CAST(c AS DATE)`, `to_timestamp(c)`.

## The pins

`python/repark/tests/timestamps/test_string_hour_minute_second_1.py`: 3,480 cells against
`string_hour_minute_second_1_spark_oracle.json` (Spark 4.1.2's answers from step 0's recording, `oracle.py`): 3,072
LTZ projection cells (16 texts × 4 inputs × 4 doors × 4 configurations × 3 functions), 192 NTZ cells (ruling 6:
`off`, `zulu`, `gap`, `tonly`, inputs `L` and `I`, `a1-utc-ntz` and `a1-la-ntz`), 216 multi-row cells; the 11 N8
cells in the set (10 LTZ, the NTZ `gap`) are named `n8:` in their test id and pinned to the head's answer, with
Spark's answer asserted to differ, so an N8 fix turns them red; three plan tests; one count test.

| package | passed | failed |
|---|---|---|
| head `pkg-shms-58b2725e` | 3,484 | 0 |
| base `pkg-sum-845766fd` | 527 | 2,957: projection 2,552, NTZ 175, multi-row 216, N8 11, plan 3 |

On base the 2,952 cells step 0 classes not EQUAL in the set are all red (projection 2,560 less the 8 N8 cells among
them, NTZ 176 less the N8 `gap`, the 216); the two `date` N8 cells, EQUAL on base, are red because they are pinned to
the head's answer. Rust: `time_family::tests` 11 of 11 (five new: a string becomes the instant, `Date32` / `NULL` /
timestamps keep their coercion, `TIME` still refuses after the coercion, the plan is `hour(to_timestamp(t.c))` and
answers `3`, `4`, `5` and NULL, `year` keeps its `Date32` cast).

## The sweeps (base against head)

- **Reader sweep** (`N/view2-delta3-work/p1probes/readers.py`, ORC fixtures from `N/csv-ts-work/fix`): 1,176
  RePark records, EQUAL to Spark 660 → 776, **0 lost**, 116 gained: `hour` over the inferred string column of the
  `json_i`, `csv_ns` and `text_v` readers (`h`, `oh`, `sh_*`, `sa_*` cells, both ANSI modes, both zone orders). Two
  other records move: the time-travel error text names the run's own clock (`ice_t/create`), on both builds.
- **Part 2's write sweep** (`wsweep.py`, ANSI on and off): 1,551 + 1,551 cells, **0 moved**.

## Unit 1's D4 cells (over main's readers)

`rp.py` from unit 1's step 0 (`W0`–`W3`, and `Z0`–`Z7` on the midnight data) on base and head, against Spark's
recording; the D4 set is the `json_i` writes and the `b` column of `json_i`, `csv_ns`, `csv_0` that unit 1's head
`pkg-rp1-f9cc698e` still answers unlike Spark ([evidence/built/d4.txt](evidence/built/d4.txt)).

| D4 cells | base | head |
|---|---|---|
| 32 `json_i` writes (`hour` of the inferred string, CTAS / INSERT / `writeTo` / `insertInto`) | 32 WRONG (`0`) | **32 EQUAL** |
| 60 midnight `b` cells, the cross-setting passes `Z0`–`Z3` | 60 WRONG | **60 EQUAL** |
| 60 midnight `b` cells, the matching passes `Z4`–`Z7` | 60 WRONG | **60 EQUAL** |

92 of the 92 D4 cells reach EQUAL (and the 60 matching twins). No other `rp.py` cell moves (10,336 compared). By
the owner's 14:00 order, the unit that merges second re-runs them at their final verdict.

## The doors outside the card

[evidence/built/doors.md](evidence/built/doors.md): on a Spark-extended session the Rust `AnsiDialect` door plans
`hour(to_timestamp(c))` and answers as the Spark door on the head (main: `0` and `Cast error` as on the Spark door);
a session without the extension and the native `repark.sql()` door have no `hour` (`UNRESOLVED_ROUTINE`) on both.
Sketch §4 risk 1 does not materialize.

Up: [map.md](map.md).
