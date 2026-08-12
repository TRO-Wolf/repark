# Unit ledger — X-1 / G6: cast-failure semantics differential corpus

**Unit:** X-1 (G6) of overnight conductor #3 (workspace brief
`BRIEF-x1-g6-cast-corpus.md` + amendment A3; not committed to this repo)
· **Date:** 2026-08-11 · **Worktree:** `/tmp/grok-x1` · **Branch:** `grok/x1-g6-cast-corpus`
· **Base (frozen A11):** `9acb566` · **Engine:** octo×2 early_stop + claims_critic

**This ledger covers the corpus half ONLY.** Live-tier disclosures are **§6 paste-true handoff**
(A3 amendment): this lane does **not** edit `_live_parity.py`, `test_parity_live.py` size pins, or
`docs/spark-sql-iceberg-parity.md`. The orchestrator lands both halves post-merge.

---

## 0. §0 premise re-verification (MANDATORY — before any row was authored)

**Oracle basis (same as every other corpus record driver):** PySpark 4.1.2, zulu-17,
`master("local[2]")`, `spark.sql.ansi.enabled=true`, `spark.sql.shuffle.partitions=2`,
`spark.ui.enabled=false`. TIMESTAMP probes also pin `spark.sql.session.timeZone=UTC` (load-bearing
for unix-seconds).

### 0.1 Probe transcript (Spark 4.1.2 ANSI ON + repark, same recipes)

| Probe | Spark 4.1.2 ANSI ON | repark | Class |
|---|---|---|---|
| `CAST('abc' AS INT)` | **RAISE** `NumberFormatException` / `CAST_INVALID_INPUT` | **RAISE** `PySparkException` / Arrow `Cast error` | shared-raise equality |
| `CAST('not-a-date' AS DATE)` | **RAISE** `DateTimeException` / `CAST_INVALID_INPUT` | **RAISE** Cast error | shared-raise equality |
| `CAST('not-a-ts' AS TIMESTAMP)` | **RAISE** `DateTimeException` / `CAST_INVALID_INPUT` | **RAISE** Parser error | shared-raise equality |
| `CAST(200 AS TINYINT)` | **RAISE** `ArithmeticException` / `CAST_OVERFLOW` | **RAISE** Cast error | shared-raise equality |
| `CAST(40000 AS SMALLINT)` | **RAISE** `CAST_OVERFLOW` | **RAISE** Cast error | shared-raise equality |
| `try_cast('abc' AS INT)` | NULL `int32` nullable | NULL `int32` nullable | content equality |
| `try_cast(200 AS TINYINT)` | NULL `int8` nullable | NULL `int8` nullable | content equality |
| `try_cast('not-a-date' AS DATE)` | NULL `date32` nullable | NULL `date32` nullable | content equality |
| `CAST(123.45 AS DECIMAL(3,2))` | **RAISE** `NUMERIC_VALUE_OUT_OF_RANGE` | **RAISE** too large to store | shared-raise equality |
| `CAST(1.239 AS DECIMAL(3,2))` | VALUE `1.24` decimal128(3,2) nullable | VALUE `1.24` decimal128(3,2) (literal non-null / VALUES nullable) | equality on VALUES path |
| `CAST(TIMESTAMP '2020-01-01 00:00:00' AS INT)` (UTC) | VALUE `1577836800` int32 nullable | **RAISE** Cast error (ns→Int32 overflow) | **SPLIT** |
| `CAST(TIMESTAMP … AS BIGINT)` (UTC) | VALUE `1577836800` int64 | VALUE `1577836800000000000` int64 (nanos) | value divergence (TZ-5 class; not duplicated here) |
| `CAST(DATE '2020-01-01' AS INT)` | **RAISE** `AnalysisException` / `DATATYPE_MISMATCH` | VALUE `18262` int32 non-null | **SPLIT** |
| `CAST('42' AS INT)` via VALUES | VALUE `42` int32 nullable | VALUE `42` int32 nullable | content equality |
| DF `col.cast("int")` on `'abc'` | **RAISE** `CAST_INVALID_INPUT` | **RAISE** Cast error | shared-raise equality (CP-11) |
| DF `col.try_cast("int")` on `'abc'` | NULL | NULL | content equality |

**VALUES-path recheck** (non-constant-fold) confirmed the same raise/NULL classes as the literal
probes for malformed string→int/date, overflow tinyint, try_cast twins, decimal overflow.

### 0.2 Classification outcome (drives the corpus)

- The slate prose ("repark raises where **non-ANSI** Spark yields NULL") is **narrowed under ANSI
  ON**: for the classic malformed / overflow casts, **both engines raise**. Those rows are
  **error-needle equalities**, not disclosures.
- **True divergences found: 2** (fewer than the 4–6 live-tier budget; **not manufactured**):
  1. `DATE → INT` — Spark analysis refuse vs repark days-since-epoch.
  2. `TIMESTAMP → INT` — Spark unix-seconds vs repark raise (ns overflow).
- Related `TIMESTAMP → BIGINT` nanoseconds class is already TZ-5; not re-budgeted here.
- Nullability-only noise on bare literal casts is avoided by preferring the VALUES path for
  content equalities.

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Differential corpus | `python/repark/tests/test_cast_failure_parity.py` | 10 G6 rows + budget pin + 3 classifier provocations |
| Record driver | `python/repark/tests/_record_cast_failure_goldens.py` | re-derives every Spark half from live 4.1.2 |
| Tests map | `python/repark/tests/map.md` | lockstep navigation + debug |
| Task map | `task/map.md` | links this ledger |
| This ledger | `task/x1-cast-failure-ledger.md` | §0 + decisions + gates + §6 handoff |

### 1.1 Budget (met)

| Bucket | Budget | Landed |
|---|---|---|
| G6 differential rows | 8–10 | **10** |
| Equality-class (content eq + shared-raise error) | ≥3 | **8** (3 content + 5 error) |
| Shared-raise error rows | ≥3 | **5** |
| try_cast twins (name-gated `try_cast_*`) | ≥2 | **2** |
| DF API `Column.cast` (CP-11) | ≥1 | **1** |
| True disclosures/splits | true count (≤4 pin) | **2** (not manufactured) |
| Classifier provocations | CP-1 arms | **4** (repark-raises CONVERGED + regression; spark-raises CONVERGED; error-row success CONVERGED) |

### 1.2 Row inventory

**Error equalities (5)** — both engines raise; needles pinned:

1. `malformed_string_to_int_both_raise` — `CAST(a AS INT)` on `'abc'` (VALUES)
2. `malformed_string_to_date_both_raise` — `CAST(a AS DATE)` on `'not-a-date'`
3. `overflow_int_to_tinyint_both_raise` — `CAST(a AS TINYINT)` on `200`
4. `decimal_narrowing_overflow_both_raise` — `CAST(a AS DECIMAL(3,2))` on `123.45`
5. `df_cast_malformed_string_to_int_both_raise` — DF `Column.cast("int")` on `'abc'` (CP-11)

**Content equalities (3)**:

6. `try_cast_malformed_string_to_int_null` — try_cast twin of #1 → NULL int32
7. `try_cast_overflow_tinyint_null` — try_cast twin of #3 → NULL int8
8. `valid_string_to_int_control` — well-formed `'42'` → 42

**Splits (2)** — true ANSI-ON divergences:

9. `date_to_int_spark_refuses_repark_days` — Spark `DATATYPE_MISMATCH`; repark `18262` int32
10. `timestamp_to_int_spark_seconds_repark_raises` — Spark `1577836800` (UTC); repark Cast error

### 1.3 Record mode

```
JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \
  PYTHONPATH=python/repark-parity/src \
  .venv/bin/python python/repark/tests/_record_cast_failure_goldens.py
```

Held `/tmp/grok-jvm-record.lock`. Captured:

```
[G6] malformed_string_to_int_both_raise [error] PASS (CAST_INVALID_INPUT)
[G6] malformed_string_to_date_both_raise [error] PASS (CAST_INVALID_INPUT)
[G6] overflow_int_to_tinyint_both_raise [error] PASS (CAST_OVERFLOW)
[G6] decimal_narrowing_overflow_both_raise [error] PASS (NUMERIC_VALUE_OUT_OF_RANGE)
[G6] try_cast_malformed_string_to_int_null [content] PASS
[G6] try_cast_overflow_tinyint_null [content] PASS
[G6] valid_string_to_int_control [content] PASS
[G6] df_cast_malformed_string_to_int_both_raise [error] PASS (CAST_INVALID_INPUT)
[G6] date_to_int_spark_refuses_repark_days [split/spark-raise] PASS (DATATYPE_MISMATCH)
[G6] timestamp_to_int_spark_seconds_repark_raises [split/spark-success] PASS

record mode: 10 rows re-derived, 0 mismatch(es)
```

---

## 2. Decisions

**D-X1-1 — Build against ANSI ON reality, not slate prose.** §0 is authoritative. Shared-raise
rows are first-class error equalities (join-corpus `kind="error"` mold).

**D-X1-2 — Join-corpus row mold** (`content` / `error` / `split` + `entry` sql|df_cast). Split
supports **both directions** via `which_raises` (`repark` or `spark`) so DATE→INT and
TIMESTAMP→INT both fit without inventing a fourth kind.

**D-X1-3 — UTC session zone in the record driver** for TIMESTAMP→INT unix-seconds stability
(`1577836800`). Documented in the driver docstring.

**D-X1-4 — A3: no edit to `_live_parity.py` / live pins / registry.** §6 carries BOTH halves for
the 2 divergent rows. SCENARIOS stays 42; LIFECYCLE stays 2.

**D-X1-5 — True divergence count = 2** (flag; do not manufacture to hit 4–6).

**D-X1-6 — No engine production source edits.** Tests + maps + ledger only.

**D-X1-7 — VALUES path preferred for content equalities** to avoid literal-cast nullability noise.

---

## 3. Gate evidence

### 3.1 Record mode

```
record mode: 10 rows re-derived, 0 mismatch(es)
```

### 3.2 Facade unit suite

```
python/repark/tests/test_cast_failure_parity.py — 15 passed in 0.44s
```

(10 differential + 1 budget + 4 classifier provocations)

### 3.3 Node ids (pytest --collect-only)

```
python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[malformed_string_to_int_both_raise]
python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[malformed_string_to_date_both_raise]
python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[overflow_int_to_tinyint_both_raise]
python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[decimal_narrowing_overflow_both_raise]
python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[try_cast_malformed_string_to_int_null]
python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[try_cast_overflow_tinyint_null]
python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[valid_string_to_int_control]
python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[df_cast_malformed_string_to_int_both_raise]
python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[date_to_int_spark_refuses_repark_days]
python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[timestamp_to_int_spark_seconds_repark_raises]
python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row_set_covers_g6_budget
python/repark/tests/test_cast_failure_parity.py::test_split_repark_raises_classifier_converged_arm
python/repark/tests/test_cast_failure_parity.py::test_split_repark_raises_classifier_regression_arm
python/repark/tests/test_cast_failure_parity.py::test_split_spark_raises_classifier_converged_arm
python/repark/tests/test_cast_failure_parity.py::test_error_row_classifier_success_arm
```

### 3.4 Provocation transcripts (CP-1)

All four classifier tests green (monkeypatch):

- `test_split_repark_raises_classifier_converged_arm` → AssertionError matching `CONVERGED` +
  "flip it to a content equality" + "Do not delete"
- `test_split_repark_raises_classifier_regression_arm` → AssertionError matching `regression` +
  "Re-derive" + "not a clean convergence"
- `test_split_spark_raises_classifier_converged_arm` → AssertionError matching `CONVERGED` +
  "shared-raise error equality"
- `test_error_row_classifier_success_arm` → AssertionError matching `CONVERGED` +
  "flip it to a content equality" (shared-raise error row starts succeeding)

### 3.5 ruff

```
ruff check test_cast_failure_parity.py _record_cast_failure_goldens.py → All checks passed
```

### 3.6 `make ci` / verify

Recorded in the final gate section after pre-PR run (see §7 / COMPLETE).

---

## 4. Oracle re-derivation spot-check (corpus-lane audit)

Independent re-run of the full record driver (fresh Spark session, same lock protocol) after the
corpus was authored: **10/10 PASS, 0 mismatches** (transcript in §1.3). Sampled content + error +
both split arms all covered by the full re-derive (not a 3-row subsample — the budget is 10).

---

## 5. Deviations FLAGGED

| ID | Deviation | Reason (true) |
|---|---|---|
| DEV-1 | Live-tier disclosures = **2**, not 4–6 | §0 found only 2 real ANSI-ON divergences; brief forbids manufacturing |
| DEV-2 | No edit to `_live_parity.py` / live size pins | A3 amendment (mirror gate would red); §6 BOTH halves instead |
| DEV-3 | BL-1 registry prose still says "non-ANSI NULL" | Orchestrator updates registry from §6; lane must not edit the file (B6) |

---

## 6. Registry + live-tier paste-true handoff (orchestrator lands post-merge)

**Do not apply from this PR.** After this PR merges, the orchestrator lands the registry rows and
the `_live_parity.py` / pin updates **together** so `test_disclosures_mirror_the_registry` stays
green at every commit. SCENARIOS stays **42**; LIFECYCLE stays **2**.

### 6.1 Replace / supersede BL-1 (registry) — cast-failure class under ANSI ON

Paste into `docs/spark-sql-iceberg-parity.md` §7 (or the H-1d / G6 home the orchestrator chooses),
superseding the non-ANSI BL-1 prose:

```markdown
### G6-1 — malformed string→INT raises on both engines under ANSI ON

- **repark** — `CAST('abc' AS INT)` (VALUES path) raises an execution-class error whose message
  contains `Cast error` (Arrow cast failure), through both `spark.sql()` and DataFrame
  `Column.cast`.
- **Apache Spark** — under ANSI ON raises `CAST_INVALID_INPUT` / `NumberFormatException`.
  *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[malformed_string_to_int_both_raise]`
  and `…[df_cast_malformed_string_to_int_both_raise]`
- **Rationale** — DOCUMENTED equality under the recorded oracle (ANSI ON). Supersedes BL-1's
  non-ANSI "Spark yields NULL" claim for this recipe. The migration path that wants NULL is
  `try_cast` (G6-2).

### G6-2 — try_cast of a failing cast yields NULL on both engines

- **repark** — `try_cast('abc' AS INT)` / `try_cast(200 AS TINYINT)` yield NULL at the target
  Arrow type (int32 / int8 nullable).
- **Apache Spark** — same NULL result under ANSI ON. *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[try_cast_malformed_string_to_int_null]`
  and `…[try_cast_overflow_tinyint_null]`
- **Rationale** — DOCUMENTED equality. Soft-cast door is the honest NULL-on-bad-cast path.

### G6-3 — DATE→INT: Spark refuses; repark yields days-since-epoch

- **repark** — `CAST(DATE '2020-01-01' AS INT)` yields non-null int32 `18262` (days since epoch).
- **Apache Spark** — raises `AnalysisException` / `DATATYPE_MISMATCH` (suggests `UNIX_DATE`).
  *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[date_to_int_spark_refuses_repark_days]`
- `live-mirror: cast_date_to_int_spark_refuses`
- **Rationale** — BACKLOG / disclosure. Silently-wrong-result class in the migration direction
  that assumes Spark's refuse: a job that casts partition dates to int succeeds here and fails
  on Spark.

### G6-4 — TIMESTAMP→INT: Spark unix-seconds; repark raises (ns overflow)

- **repark** — `CAST(TIMESTAMP '2020-01-01 00:00:00' AS INT)` raises Arrow `Cast error` (ns epoch
  does not fit Int32).
- **Apache Spark** — under `session.timeZone=UTC` yields int32 nullable `1577836800` (unix
  seconds). *(oracle: recorded.)*
- **Pin** — `python/repark/tests/test_cast_failure_parity.py::test_cast_failure_row[timestamp_to_int_spark_seconds_repark_raises]`
- `live-mirror: cast_timestamp_to_int_repark_raises`
- **Rationale** — BACKLOG / disclosure. Related to TZ-5 (TIMESTAMP→BIGINT nanos) but the INT
  path is raise-vs-value, not a scale-factor value bug.
```

### 6.2 Live-tier `Disclosure(...)` blocks (exact — orchestrator only)

Paste into `python/repark/tests/_live_parity.py` `DISCLOSURES` list (names must match the
`live-mirror:` bullets above exactly — the mirror gate parses that spelling):

```python
Disclosure(
    name="cast_date_to_int_spark_refuses",
    repark_check=_disc_cast_date_to_int_repark,
    spark_check=_disc_cast_date_to_int_spark,
    note=(
        "CAST(DATE '2020-01-01' AS INT): repark yields days-since-epoch 18262; "
        "ANSI Spark refuses with DATATYPE_MISMATCH. Corpus: "
        "test_cast_failure_parity.py::test_cast_failure_row[date_to_int_spark_refuses_repark_days]."
    ),
),
Disclosure(
    name="cast_timestamp_to_int_repark_raises",
    repark_check=_disc_cast_timestamp_to_int_repark,
    spark_check=_disc_cast_timestamp_to_int_spark,
    note=(
        "CAST(TIMESTAMP '2020-01-01 00:00:00' AS INT) under UTC: Spark yields unix seconds "
        "1577836800; repark raises Cast error (ns overflow). Corpus: "
        "test_cast_failure_parity.py::test_cast_failure_row[timestamp_to_int_spark_seconds_repark_raises]."
    ),
),
```

Helper stubs (orchestrator implements against `Engine` the same way other disclosures do):

```python
def _disc_cast_date_to_int_repark(engine: Engine) -> None:
    out = engine.arrow_of(engine.session.sql("SELECT CAST(DATE '2020-01-01' AS INT) AS n"))
    assert out.schema.field("n").type == pa.int32()
    assert out.column("n").to_pylist() == [18262]


def _disc_cast_date_to_int_spark(engine: Engine) -> None:
    _expect_raises(
        lambda: engine.arrow_of(engine.session.sql("SELECT CAST(DATE '2020-01-01' AS INT) AS n")),
        needle="DATATYPE_MISMATCH",
    )


def _disc_cast_timestamp_to_int_repark(engine: Engine) -> None:
    _expect_raises(
        lambda: engine.arrow_of(
            engine.session.sql("SELECT CAST(TIMESTAMP '2020-01-01 00:00:00' AS INT) AS n")
        ),
        needle="Cast error",
    )


def _disc_cast_timestamp_to_int_spark(engine: Engine) -> None:
    # Requires session timeZone=UTC on the live Spark engine (record basis).
    out = engine.arrow_of(
        engine.session.sql("SELECT CAST(TIMESTAMP '2020-01-01 00:00:00' AS INT) AS n")
    )
    assert out.schema.field("n").type == pa.int32()
    assert out.column("n").to_pylist() == [1577836800]
```

### 6.3 Exact-set pin update text (`test_parity_live.py`)

The mirror gate and any exact-set pin of `DISCLOSURES` names (≈ line 215 on `9acb566`) must add
exactly:

```text
cast_date_to_int_spark_refuses
cast_timestamp_to_int_repark_raises
```

**Do not change** `SCENARIOS` size pin (**42**) or `LIFECYCLE` size pin (**2**). Only the
`DISCLOSURES` exact-set grows by these two names, landed in the same orchestrator commit as the
registry `live-mirror:` bullets.

---

## 7. Critic engine

- **Engine:** octo cycles=2 early_stop + claims_critic (Critic-4 every findings pass)
- **Status:** **OCTO-CONVERGED** (cycle 1 fixed CP-1 error-row arm + dead `_DEC_3_2` + CL-GHOST
  brief path; cycle 2 CLEAN at floor with early_stop)

### 7.1 Cycle 1 findings → fix

| ID | Class | Disposition |
|---|---|---|
| C1-Q-001 | CP-1 dead classifier on `kind="error"` success path | **FIXED** — drive lifecycle; CONVERGED flip guidance; provocation `test_error_row_classifier_success_arm` |
| C1-Q-002 | dead code `_DEC_3_2` / `_DATE` unused constants | **FIXED** — removed |
| C1-CL-001 | CL-GHOST ledger linked non-repo `planning/grok/*` path | **FIXED** — cite workspace brief by name only |

### 7.2 Cycle 2 findings (CLEAN)

Corpus-failure taxonomy null-report (CP-1..CP-12):

| ID | Attack | Result |
|---|---|---|
| CP-1 | classifier reachability on error + both split directions | PROVEN (4 monkeypatch arms green) |
| CP-2 | name-gated families vs control | HOLD (malformed/overflow/try_cast name gates) |
| CP-3 | simulated-not-executed | N/A — real `sql()` / `Column.cast` / `to_arrow` |
| CP-4 | vacuous refuse | N/A — error needles from live raise messages |
| CP-5 | paste-true §6 + collect-only node ids | HOLD (verified collect-only) |
| CP-6 | f-string guidance | HOLD (all `{FIX_G6}` under f-strings) |
| CP-7 | golden drift | HOLD (full record re-derive 10/10) |
| CP-8 | oracle-pin tautology | N/A — no version self-pin |
| CP-9 | landmine non-vacuity | N/A |
| CP-10 | budget drift | HOLD (8–10 pin matches 10 rows) |
| CP-11 | entry-point DF cast | HOLD (`df_cast_malformed_*`) |
| CP-12 | leftover state | N/A — stateless SELECT casts |

Claims critic (CL-*) null-report: inventory of budget counts, row inventory, "2 divergences",
"15 passed", "no edit to _live_parity", A3/B6 bans — re-checked against tree. No OPEN ≥ floor.

---

## 8. Identity / hygiene

- Commits: `git -c user.name=TRO-Wolf -c user.email=…` only; trailer
  `Authored-By: Grok (grok-4.5) <noreply@x.ai>`
- Identity check after every commit: `git log --format='%an %ae' -1`
- Two-pass hygiene before push
- Never touch: CLAUDE.md, AGENTS.md, PROJECT.md, STATUS.md, registry, Cargo.lock, uv.lock,
  .github/, planning/hardening
