# Unit ledger — DF-DESCRIBE-STR-1 · describe() answers NULL statistics on string columns

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when DF-DESCRIBE-STR-1 merges, or when the
owner closes the slate row.

**Unit:** DF-DESCRIBE-STR-1 · **Date:** 2026-09-11 · **Model:** swe-2-high · **Branch:** `fix/df-describe-str-1` · **Base:** `f413241b` (branch point at dispatch; no merge performed — the orchestrator merges)
**Slate:** card DF-DESCRIBE-STR-1, `fix/df-describe-str-1` build lane, step 1 of 1.

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `python/repark/src/repark/spark/dataframe/statistics.py`,
`python/repark/tests/test_examples_dataframe_d.py`,
`python/repark/tests/test_examples_dataframe_a.py`,
`python/repark/tests/test_examples_dataframe_c.py` (the shared `_summary` leg flips
EX-DF-15's order and string arms), `docs/spark-sql-iceberg-parity.md` §7 EX-DF-4 and
EX-DF-15, lockstep `map.md` files (`python/repark/tests/map.md`,
`python/repark/src/repark/spark/dataframe/map.md`, `task/ledgers/staging/map.md`), and this
ledger. Closed: `crates/`, every other `scripts/` line, `.github/`, `STATUS.md`,
`briefs/next-sequence.md`, every other ledger.

## Scope

EX-29 measured (2026-09-11, live PySpark 4.1.2): `frame.describe("g")` on a string column —
and bare `frame.describe()` over a frame holding one — raises `AnalysisException` in repark
because the `avg` / `stddev` legs reject `Utf8`. Spark answers the same five rows with NULL
cells under the string column's `mean` / `stddev` and the string ordering's `min` / `max`.
This unit makes `_summary`'s `mean`/`stddev` legs run over `try_cast(col AS DOUBLE)` for
string columns (the oracle shows Spark casts strings to double, so `"10","2","a"` answers
`mean` `6.0` rather than NULL), restricts the bare column set to numeric+string like Spark
(naming a non-numeric non-string column raises `PySparkValueError`), and orders the
UNION ALL legs by a stat ordinal so the collect order is the requested order — `count,
mean, stddev, min, max` for `describe`. That flips §7 EX-DF-4 whole and EX-DF-15's order and
string arms; EX-DF-15 stays for the bare-`summary()` percentile refusal (engine gap, D-4).

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Spark's answer re-measured on the oracle (D-1): `describe("g")` and bare `describe()` over a frame holding a string column answer five rows in the order count/mean/stddev/min/max with `None` mean/stddev cells and string min/max, all cells strings; the boundary arms are measured — string `mean`/`stddev` run over a silent double cast (`["10","2","a"]` answers `6.0`), non-numeric non-string columns are skipped by the bare forms and refused with `PySparkValueError` when named. | The oracle table below, measured on live PySpark 4.1.2 (ANSI on, UTC, zulu-17) in one session, stopped before any gate ran. | **PROVEN** |
| C-002 | The pins rewritten to assert Spark's answer fail on the base tree (red first, D-3); the red output is pasted in this ledger's evidence cell. | The pytest run under "Red-first" — four pins red at base `f413241b` with the fix reverted. | **PROVEN** |
| C-003 | The rewritten pins are green after the fix and every other describe/summary pin stays green: the `-k "describe or summary"` selection over the pin files plus the whole `python/repark/tests` suite, plus an 8-collect determinism probe on `describe()`. | The pytest runs in the gates table and the probe in the C-003 note below. | **PROVEN** |
| C-004 | §7 EX-DF-4 reads FIXED, dated 2026-09-11, unit DF-DESCRIBE-STR-1 — both the string arm and the order arm (D-2: the same `_summary` code path fixed the order at no extra cost); §7 EX-DF-15 is narrowed to the bare-`summary()` percentile refusal. | The amended §7 rows and the map.md entries. | **PROVEN** |

`LOGIC_SCORE` = **4/4 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

The four rewritten/added pins run at base `f413241b` with `statistics.py` reverted
(`git checkout`), exit 1:

```
FAILED python/repark/tests/test_examples_dataframe_d.py::test_describe_string_column_null_stats
FAILED python/repark/tests/test_examples_dataframe_d.py::test_describe_non_describable_column_arms
FAILED python/repark/tests/test_examples_dataframe_a.py::test_describe_row_order_matches_spark
FAILED python/repark/tests/test_examples_dataframe_c.py::test_summary_divergent_arms
4 failed, 15 deselected in 0.59s
```

Each red is the divergence it pins: `test_describe_string_column_null_stats` and
`test_summary_divergent_arms` raise `AnalysisException` (`Function 'avg' requires Decimal,
but received String (DataType: Utf8)`); `test_describe_non_describable_column_arms` raises
`AnalysisException` on `avg(Boolean)` where Spark skips the column; and
`test_describe_row_order_matches_spark` reds on the order itself — index 1 answered
`('min', '1', '10.0')` where Spark's second row is `('mean', '1.8333333333333333', '30.0')`.

## Oracle (live PySpark 4.1.2, ANSI on, UTC, 2026-09-11)

One PySpark session (`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `.venv/bin/python`,
`spark.sql.ansi.enabled=true`, `spark.sql.session.timeZone=UTC`, `local[1]`, warehouse a
tmpdir, `scratch/df-describe-str-1/oracle2.py`), stopped before any gate ran.

| Arm | Spark answer |
|---|---|
| `describe("g")` on `[("a",1),("b",2)]` over `g`,`k` | `[('count','2'),('mean',None),('stddev',None),('min','a'),('max','b')]` — identical across three collects |
| bare `describe()` on the same frame | columns `['summary','g','k']`; rows `('count','2','2')`, `('mean',None,'1.5')`, `('stddev',None,'0.7071067811865476')`, `('min','a','1')`, `('max','b','2')` — identical across three collects |
| `describe("k","g")` | columns follow the request order: `['summary','k','g']` |
| `summary("count","mean","stddev","min","max")` on the same frame | same cells as `describe()` |
| `describe("s")` on `["10","2","a"]` | `('count','3'),('mean','6.0'),('stddev','5.656854249492381'),('min','10'),('max','a')` — Spark casts strings to double non-strictly for `mean`/`stddev`; repark's `avg(try_cast(s AS DOUBLE))` reproduces `6.0`/`5.656854249492381` byte-identically |
| `describe("g")` with a NULL row `[("a",1),(None,2),("b",3)]` | `('count','2','3'),('mean',None,'2.0'),('stddev',None,'1.0'),('min','a','1'),('max','b','3')` — count is non-null rows |
| `describe()` on `[(True,1),(False,2)]` over `b`,`k` | columns `['summary','k']` — the boolean column is skipped |
| `describe("b")` on the boolean frame | raises `PySparkValueError` |
| `summary("count","mean","stddev","min","max")` on the boolean frame | columns `['summary','k']` — skipped there too |
| `describe()` / `summary(...)` on `date`, `timestamp`, `array` columns | same skip; `describe("d")`/`describe("t")` raise `PySparkValueError` |
| `describe("m")` on `decimal` `[1.5, 2.5]` | `('count','2'),('mean','2.0000000000000000000000'),('stddev','0.7071067811865476'),('min','1.500000000000000000'),('max','2.500000000000000000')` — decimal stays numeric |
| bare `summary()` | answers the eight-row `count,mean,stddev,min,25%,50%,75%,max` table (repark keeps refusing — engine gap, EX-DF-15) |
| `describe("nope")` | raises `AnalysisException` `UNRESOLVED_COLUMN.WITH_SUGGESTION` (repark already raises `AnalysisException` — class holds) |
| all-`None` inference | PySpark `createDataFrame([(None,1)])` itself raises `CANNOT_DETERMINE_TYPE` — no `describe` arm exists |

Residue not claimed: bare `describe()`/`summary()` over a frame with zero numeric+string
columns keeps repark's pre-existing `AnalysisException` ("zero-column frame"); Spark's answer
there was not measured this unit (one-session cap). `describe("g","missing")` keeps silently
dropping the missing name where Spark raises `UNRESOLVED_COLUMN` — pre-existing, outside the
card.

## Gates (2026-09-11, on this tree)

Each command run independently; no JVM running during any gate (the oracle session stopped
before the first). The C-003 determinism probe collected `describe()` eight times —
identical ordered output each time.

| Command | Exit |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_examples_dataframe_d.py python/repark/tests -q -k "describe or summary"` | 0 — 33 passed, 1 skipped |
| `.venv/bin/python -m pytest python/repark/tests -q` | 0 — 5968 passed, 359 skipped |
| `make check-docs-links` | 0 — 765 files, 4894 links clean |
| `make check-ledger-grammar` | 0 — 101 live ledgers clean |
| `make check-lib-py` | 0 — 648 files clean |
| `make verify` | 0 — fmt/clippy/panic-ban/crate-dag/file-size/conventions/docstring/coverage/manifest/ledger/docs/owner-ruling/parity-live/matrix/rust-check/ruff/rust tests all clean |

## Cost

The Devin (SWE-2) leg started 2026-09-11: read the contract, the EX-29 ledger, the grammar
gate, §7 EX-DF-4/EX-DF-15, `statistics.py`, and the pin files; measured live Spark 4.1.2
cells and repark probes; fixed `_summary`, flipped the pins and the §7 rows, the maps, and
this ledger.

## Disk

Pickup: the clone is a scratch lane; the oracle probes live under the gitignored
`scratch/df-describe-str-1/` (removable at close). The lane venv carries a prebuilt native.

## Dual-wire

Unchanged by this unit — a facade body, three pin files, two §7 rows, maps. `.github/` is
closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: df-describe-str-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Four pins red at base with the fix reverted — string mean AnalysisException, boolean avg AnalysisException, the order pin red on index 1 ('min' where 'mean' belongs), and the summary leg red; all green after the fix.
      artifacts: [python/repark/tests/test_examples_dataframe_d.py, python/repark/tests/test_examples_dataframe_a.py, python/repark/tests/test_examples_dataframe_c.py, python/repark/src/repark/spark/dataframe/statistics.py]
    - id: AT-2
      status: ATTACKED
      evidence: Every asserted cell was measured on live PySpark 4.1.2 in the oracle table — NULL mean/stddev, the '6.0' cast-mean arm, the boolean/date/timestamp/array skip, and the PySparkValueError refusal class.
      artifacts: [task/ledgers/staging/df-describe-str-1-ledger.md]
    - id: AT-3
      status: N/A
      justification: The unit changes facade Python and pins; no static gate exists whose provocation applies — the red-first pins are the control.
    - id: AT-4
      status: ATTACKED
      evidence: describe() collected eight times post-fix answers the identical ordered table; the ORDER BY ordinal is generated collision-free against the engine column set.
      artifacts: [python/repark/src/repark/spark/dataframe/statistics.py]
    - id: AT-5
      status: N/A
      justification: No new execution surface, network, or cloud path; the fix emits one more SELECT wrap and a try_cast in the existing temp-view SQL path.
    - id: AT-6
      status: ATTACKED
      evidence: Non-describable columns are refused with PySparkValueError when named and skipped when implicit; the zero-column guard still raises AnalysisException.
      artifacts: [python/repark/src/repark/spark/dataframe/statistics.py]
    - id: AT-7
      status: ATTACKED
      evidence: No guessed cell — the numeric-string mean arm exists in the pin only because the oracle answered it; the unmeasured all-non-describable arm is disclosed as not claimed.
      artifacts: [task/ledgers/staging/df-describe-str-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: All gate commands ran on the shipped tree with recorded exit codes; the single JVM session was stopped before the first gate.
      artifacts: [Makefile]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface; errors surface through the existing exception classes.
    - id: AT-10
      status: ATTACKED
      evidence: Every touched directory's map.md updated in the same commit — tests map, dataframe map, staging map.
      artifacts: [python/repark/tests/map.md, python/repark/src/repark/spark/dataframe/map.md, task/ledgers/staging/map.md]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](map.md)
- Fix: [../../../python/repark/src/repark/spark/dataframe/statistics.py](../../../python/repark/src/repark/spark/dataframe/statistics.py)
- Pins: [../../../python/repark/tests/test_examples_dataframe_d.py](../../../python/repark/tests/test_examples_dataframe_d.py), [../../../python/repark/tests/test_examples_dataframe_a.py](../../../python/repark/tests/test_examples_dataframe_a.py), [../../../python/repark/tests/test_examples_dataframe_c.py](../../../python/repark/tests/test_examples_dataframe_c.py)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7 EX-DF-4, EX-DF-15
