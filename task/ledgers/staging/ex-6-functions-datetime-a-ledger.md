# Unit ledger — EX-6 · v0.7 backfill — datetime arithmetic and parts (35 names)

**Retires:** this ledger moves to `../completed/` in the family's last commit
(the orchestrator's departure move). It closes when the `F.*` datetime family
PR merges, or when the owner closes the slate row.

**Unit:** EX-6 · **Date:** 2026-09-03 · **Model:** muse-spark-1.2-contributor (batch, continuation of glm-5.3-flash); glm-5.3-flash (remediation) · **Branch:** `feat/ex-6-functions-datetime-a` · **Base:** `a0fe83a`
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), batch roster row 6.
**Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v0.7 — Full example documentation".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/functions/`, `docs/examples/backlog.txt`, the
`BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`, lockstep
`map.md` files, and this ledger with its `staging/map.md` row. Closed:
`crates/`, `python/repark/src/`, every other `scripts/` line, `.github/`,
`STATUS.md`, every other ledger, `briefs/next-sequence.md`.

## Scope

35-name roster, every one a row of `docs/examples/backlog.txt` at `a0fe83a`:

`F.add_months` `F.curdate` `F.current_date` `F.currentDate` `F.current_timestamp` `F.currentTimestamp` `F.now` `F.date_add` `F.dateadd` `F.date_sub` `F.date_diff` `F.datediff` `F.date_format` `F.date_part` `F.datepart` `F.date_trunc` `F.trunc` `F.day` `F.dayofmonth` `F.dayofweek` `F.dayofyear` `F.dayname` `F.weekday` `F.weekofyear` `F.month` `F.monthname` `F.months_between` `F.quarter` `F.year` `F.hour` `F.minute` `F.second` `F.last_day` `F.next_day` `F.extract`

As landed: 33 kept, 2 dropped (measured divergence / engine refusal, both recorded). See outcome below.

**Grouping.** Seven files, grouped by the idea a reader learns in one breath:

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `calendar_parts.py` | `F.year`, `F.quarter`, `F.month`, `F.weekofyear`, `F.day`, `F.dayofmonth`, `F.dayofyear`, `F.dayofweek`, `F.weekday`, `F.hour`, `F.minute`, `F.second` | The numeric calendar parts of a date and the clock parts of a timestamp. |
| `current_datetime.py` | `F.curdate`, `F.current_date`, `F.currentDate`, `F.current_timestamp`, `F.currentTimestamp`, `F.now` | The six current date/timestamp spellings, shown agreeing within each trio. |
| `date_arithmetic.py` | `F.date_add`, `F.dateadd`, `F.date_sub`, `F.last_day`, `F.next_day` | Moving a date by days, the month's last day, and the next weekday. |
| `date_difference.py` | `F.date_diff`, `F.datediff` | End minus start in days, alias pair shown agreeing. |
| `date_format.py` | `F.date_format`, `F.dayname`, `F.monthname` | Rendering a date as text and the name shorthands beside it. |
| `date_parts_sql.py` | `F.date_part`, `F.datepart`, `F.extract` | The SQL field-extraction trio, shown agreeing. |
| `date_truncation.py` | `F.date_trunc`, `F.trunc` | Truncating a timestamp and a date at year/month/day/quarter granularity. |

`F.col` appears in several files as the honest repark-rooted receiver; it is already covered by `abs.py` and does not move the ratchet. `F.lit` similarly joins `date_parts_sql.py`.

## Orchestrator rulings (build-to)

- Every asserted value is measured against the live Spark oracle (PySpark 4.1.2 + Iceberg 1.11.0, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `PYTHONPATH=python/repark/tests`, `_live_parity.build_spark_iceberg_engine`) before it is written; a name whose repark value differs from Spark, or that repark refuses, is dropped and listed with both values.
- The gate is the acceptance bar: a `COVERS` entry the script does not exercise is a defect, and every script runs green locally with no network, no cloud and no JVM.
- The backlog count moves down by exactly the names this batch covers (33), `BACKLOG_BASELINE` 842 → 809.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-ex-6-datetime-a
  agent: Actor
  action: land 35-name datetime batch (33 kept, 2 dropped) over seven files
  charter_trace: C-001
  preconditions:
    - branch feat/ex-6-functions-datetime-a at a0fe83a: SATISFIED (git)
    - all 35 names are backlog rows at the base, none covered, none excepted: SATISFIED (grep)
    - the EX-0 gate is in make ci: SATISFIED (Makefile ci target)
  success_condition: every name the batch can teach honestly leaves the backlog, the ratchet moves by exactly that count, both gate legs exit 0
  step_risks:
    - a COVERS entry the script does not really exercise: HANDLED(each script asserts on the value the name produces)
    - a name grouped where it is decoration: HANDLED(grouping table states why each name is in its file)
  contingencies: [example exposes a product defect: EXECUTABLE(report it, drop the name, move the baseline by the names actually removed)]
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Batch lands runnable local examples for the 33 roster names the live oracle confirms, in seven files under `docs/examples/functions/`, every asserted value measured against live PySpark 4.1.2 before it was written and every `COVERS` entry exercised by an assertion on that measured value; those 33 leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 33, 842 → 809, with no other `scripts/` change; the remaining two, `F.add_months` (divergence on negative offset from a month end) and `F.months_between` (engine refusal R-FN-BATCH1), stay on the backlog with both values recorded, and no product file is touched; the gate's static half and its `--require-execute` leg both exit 0. | Red-first capture (35 findings), the oracle table, the divergence/refusal rows, the green counts line, and the recorded gate exit codes. | **OPEN** |

`LOGIC_SCORE` = **0/1 `PROVEN`** — the clause stays `OPEN` until the family lands. The
worker's green is directional by the campaign contract; the orchestrator's independent
re-run from a clean checkout is what closes it, and the pins a `PROVEN` verdict owes
live in `python/repark-parity/tests/`, which this unit may not write.

## Red-first (docs/testing.md "Gate provocation proofs")

Captured at the base `a0fe83a` with no batch files and the 35 rows already removed from `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moved `842 → 807` with nothing else changed. `python3 scripts/check_example_coverage.py --require-execute` exited **1** with exactly 35 findings, one per roster name and no others:

```
example-coverage: 35 finding(s)
  public name F.add_months has no example COVERS row and is not in the backlog or exceptions
  public name F.curdate has no example COVERS row and is not in the backlog or exceptions
  public name F.currentDate has no example COVERS row and is not in the backlog or exceptions
  public name F.currentTimestamp has no example COVERS row and is not in the backlog or exceptions
  public name F.current_date has no example COVERS row and is not in the backlog or exceptions
  public name F.current_timestamp has no example COVERS row and is not in the backlog or exceptions
  public name F.date_add has no example COVERS row and is not in the backlog or exceptions
  public name F.date_diff has no example COVERS row and is not in the backlog or exceptions
  public name F.date_format has no example COVERS row and is not in the backlog or exceptions
  public name F.date_part has no example COVERS row and is not in the backlog or exceptions
  public name F.date_sub has no example COVERS row and is not in the backlog or exceptions
  public name F.date_trunc has no example COVERS row and is not in the backlog or exceptions
  public name F.dateadd has no example COVERS row and is not in the backlog or exceptions
  public name F.datediff has no example COVERS row and is not in the backlog or exceptions
  public name F.datepart has no example COVERS row and is not in the backlog or exceptions
  public name F.day has no example COVERS row and is not in the backlog or exceptions
  public name F.dayname has no example COVERS row and is not in the backlog or exceptions
  public name F.dayofmonth has no example COVERS row and is not in the backlog or exceptions
  public name F.dayofweek has no example COVERS row and is not in the backlog or exceptions
  public name F.dayofyear has no example COVERS row and is not in the backlog or exceptions
  public name F.extract has no example COVERS row and is not in the backlog or exceptions
  public name F.hour has no example COVERS row and is not in the backlog or exceptions
  public name F.last_day has no example COVERS row and is not in the backlog or exceptions
  public name F.minute has no example COVERS row and is not in the backlog or exceptions
  public name F.month has no example COVERS row and is not in the backlog or exceptions
  public name F.monthname has no example COVERS row and is not in the backlog or exceptions
  public name F.months_between has no example COVERS row and is not in the backlog or exceptions
  public name F.next_day has no example COVERS row and is not in the backlog or exceptions
  public name F.now has no example COVERS row and is not in the backlog or exceptions
  public name F.quarter has no example COVERS row and is not in the backlog or exceptions
  public name F.second has no example COVERS row and is not in the backlog or exceptions
  public name F.trunc has no example COVERS row and is not in the backlog or exceptions
  public name F.weekday has no example COVERS row and is not in the backlog or exceptions
  public name F.weekofyear has no example COVERS row and is not in the backlog or exceptions
  public name F.year has no example COVERS row and is not in the backlog or exceptions
```

That is the red the batch closes: the gate names each of the 35, and it names nothing else.

**Verdict grammar, measured rather than assumed.** `C-001` is `OPEN`, not `PROVEN`, and the reason is a gate result. With the clause marked `PROVEN`, `scripts/check_ledger_grammar.py` exits with two findings — `1 PROVEN clause(s) with no pins: ex-6-functions-datetime-a/C-NNN citation (ceiling 0)` and `no COVERAGE_ATTESTATION block`. The citation must live under `crates/`, `python/` or `scripts/`, all of which this unit's writable paths close. The clause therefore stays `OPEN` until the orchestrator's independent re-run and the family's pin file close it.

## Outcome — 33 kept, 2 dropped

Two names are dropped from the batch and stay on the backlog under the campaign rule that a worker never edits product code. Measured 2026-09-03 on live PySpark 4.1.2 + Iceberg 1.11.0 (`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, session zone UTC) against the engine on the same inputs (throwaway script under `/tmp/oc-ex6-oracle/`, `_live_parity.build_spark_iceberg_engine`, per-name tables printed).

### Oracle table (per-name Spark value vs repark value, kept/dropped, file)

| Name | Spark value | repark value | Kept | File |
|---|---|---|---|---|
| F.add_months | [2024-04-30, 2023-07-29, 2025-12-15, 2023-12-31, None] | [2024-04-30, 2023-07-31, 2025-12-15, 2023-12-31, None] | dropped — diverges on 2024-02-29 minus 7 months (`2023-07-29` Spark vs `2023-07-31` repark) | — |
| F.curdate | [2026-09-03]* | [2026-09-03]* | kept | current_datetime.py |
| F.current_date | [2026-09-03]* | [2026-09-03]* | kept | current_datetime.py |
| F.currentDate | [2026-09-03]* | [2026-09-03]* | kept | current_datetime.py |
| F.current_timestamp | 2026-09-03T07:26:29* | 2026-09-03T07:26:24* | kept | current_datetime.py |
| F.currentTimestamp | 2026-09-03T07:26:28* | 2026-09-03T07:26:24* | kept | current_datetime.py |
| F.now | 2026-09-03T07:26:36* | 2026-09-03T07:26:24* | kept | current_datetime.py |
| F.date_add | [2024-02-03, 2024-02-22, 2023-12-10, 2023-12-31, None] | [2024-02-03, 2024-02-22, 2023-12-10, 2023-12-31, None] | kept | date_arithmetic.py |
| F.dateadd | [2024-02-03, 2024-02-22, 2023-12-10, 2023-12-31, None] | [2024-02-03, 2024-02-22, 2023-12-10, 2023-12-31, None] | kept | date_arithmetic.py |
| F.date_sub | [2024-01-28, 2024-03-07, 2023-10-21, 2023-12-31, None] | [2024-01-28, 2024-03-07, 2023-10-21, 2023-12-31, None] | kept | date_arithmetic.py |
| F.date_diff | [39, -116, 66, 381, None] | [39, -116, 66, 381, None] | kept | date_difference.py |
| F.datediff | [39, -116, 66, 381, None] | [39, -116, 66, 381, None] | kept | date_difference.py |
| F.date_format | ["2024-01-31", "2024-02-29", "2023-11-15", "2023-12-31", None] / ["31/01/2024", …] | same | kept | date_format.py |
| F.date_part | [2024, 2024, 2023, 2023, None] (year) / [13, 23, 0, 6, None] (hour) | same | kept | date_parts_sql.py |
| F.datepart | [2024, 2024, 2023, 2023, None] | same | kept | date_parts_sql.py |
| F.date_trunc | [2024-01-01 00:00, 2023-01-01 00:00, 2024-01-01 00:00, 2023-01-01 00:00, None] (year) | same | kept | date_truncation.py |
| F.trunc | [2024-01-01, 2024-01-01, 2023-01-01, 2023-01-01, None] (year) | same | kept | date_truncation.py |
| F.day | [31, 29, 15, 31, None] | same | kept | calendar_parts.py |
| F.dayofmonth | [31, 29, 15, 31, None] | same | kept | calendar_parts.py |
| F.dayofweek | [4, 5, 4, 1, None] | same | kept | calendar_parts.py |
| F.dayofyear | [31, 60, 319, 365, None] | same | kept | calendar_parts.py |
| F.dayname | ["Wed", "Thu", "Wed", "Sun", None] | same | kept | date_format.py |
| F.weekday | [2, 3, 2, 6, None] | same | kept | calendar_parts.py |
| F.weekofyear | [5, 9, 46, 52, None] | same | kept | calendar_parts.py |
| F.month | [1, 2, 11, 12, None] | same | kept | calendar_parts.py |
| F.monthname | ["Jan", "Feb", "Nov", "Dec", None] | same | kept | date_format.py |
| F.months_between | [1.32258065, -3.77419355, 2.16129032, 12.48387097, None] | REFUSED UnsupportedOperationException: functions.months_between is not supported yet (engine gap; disclosed R-FN-BATCH1) | dropped — engine gap | — |
| F.quarter | [1, 1, 4, 4, None] | same | kept | calendar_parts.py |
| F.year | [2024, 2024, 2023, 2023, None] | same | kept | calendar_parts.py |
| F.hour | [13, 23, 0, 6, None] | same | kept | calendar_parts.py |
| F.minute | [45, 59, 15, 30, None] | same | kept | calendar_parts.py |
| F.second | [30, 59, 0, 0, None] | same | kept | calendar_parts.py |
| F.last_day | [2024-01-31, 2024-02-29, 2023-11-30, 2023-12-31, None] | same | kept | date_arithmetic.py |
| F.next_day | [2024-02-05, 2024-03-04, 2023-11-20, 2024-01-01, None] (Mon) / [2024-02-04, 2024-03-03, 2023-11-19, 2024-01-07, None] (Sun) | same | kept | date_arithmetic.py |
| F.extract | [2024, 2024, 2023, 2023, None] | same | kept | date_parts_sql.py |

`*` current-date/timestamp values are session-clocked; the oracle checks only the trio-agreement and the type (date vs datetime), not a literal, and the repark rows show the three spellings agreeing within each trio on one local session.

The kept rows' expected literals are exactly the Spark values printed above; each was checked against Spark before it was written and again by re-running every file's body with `pyspark.sql.functions` on the live session (all seven exited 0 on Spark).

## Gates (2026-09-03, on this tree)

| Command | Exit |
|---|---|
| `.venv/bin/python scripts/check_example_coverage.py` (static half) | 0 |
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | 0 |
| `make check-map-sync` | 0 |
| `make check-ledger-grammar` | 0 |
| `make check-ledgers` | 0 |
| `uv run --no-sync ruff check docs/examples` | 0 |
| `uv run --no-sync ruff format --check docs/examples` | 0 |
| `.venv/bin/python docs/examples/functions/calendar_parts.py` | 0 |
| `.venv/bin/python docs/examples/functions/current_datetime.py` | 0 |
| `.venv/bin/python docs/examples/functions/date_arithmetic.py` | 0 |
| `.venv/bin/python docs/examples/functions/date_difference.py` | 0 |
| `.venv/bin/python docs/examples/functions/date_format.py` | 0 |
| `.venv/bin/python docs/examples/functions/date_parts_sql.py` | 0 |
| `.venv/bin/python docs/examples/functions/date_truncation.py` | 0 |

Counts line (both legs, the execute leg imports the native module so no skip line):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 102 covered; 809 backlog; 2 exceptions; 22 examples`

Before this batch (base `a0fe83a`): `913 public names; 69 covered; 842 backlog; 2 exceptions; 15 examples`
(delta +33 covered, −33 backlog, +7 examples; the two dropped names stay on the backlog with the reason above).

## Pointers

- Up: [map.md](map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Sibling: [ex-2-functions-math-bitwise-ledger.md](ex-2-functions-math-bitwise-ledger.md)
- Archive: [../archive/2026-09/2026-09-02-ex-1-class-surfaces-ledger.md](../archive/2026-09/2026-09-02-ex-1-class-surfaces-ledger.md)
