# Unit ledger — FN-FIX-2-CTRL-1 · the incidental controls FN-FIX-2's critic found missing

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when FN-FIX-2-CTRL-1 merges, or when the owner closes the slate row.

**Unit:** FN-FIX-2-CTRL-1 · **Date:** 2026-09-04 · **Executor:** Muse Spark (Actor) ·
**Branch:** `fix/fn-fix-2-ctrl-1-controls` · **Base:** `e3600a1`
**Model:** muse-spark-1.3
**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

No Rust change. Live PySpark 4.1.2, zulu-17, `TZ=UTC`, `spark.sql.legacy` defaults.
Control 1 diverges (repark has no `regexp_extract`): HALT with the measured pair, no fix.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | One throwaway oracle script under gitignored `scratch/` measures every control on live Spark (one JVM; ANSI on AND off where the table says so) and on repark; every pair recorded in the oracle table below. | Oracle table (§Oracle), `scratch/oracle_spark.py`, `scratch/oracle_spark2.py`, `scratch/oracle_repark.py`, `scratch/oracle_repark2.py`. | **PROVEN** |
| C-002 | The controls land in the six pin files as parametrized cases (repark answers == Spark's measured answers); the same cells co-collect on the shared `spark_engine` in the two FN-FIX-2 live tests. | Six pin files + `test_parity_live.py` legs below; control 1 unpinned (FINDING F-FN-FIX-2-CTRL-1-1). | **OPEN** (control 1 needs a ruling: repark has no `regexp_extract` on either door) |
| C-003 | Every new pin reds under one mutation (one flipped expected value in a scratch copy). | Mutation table (§Mutation): 8 red of 8. | **PROVEN** |
| C-004 | Registry §7 rows for the six FN-FIX-2 fixes gain a one-line controls note; `staging/map.md` and `python/repark/tests/map.md` lockstep; STATUS untouched. | Registry notes + maps (§9). | **PROVEN** |

## Oracle (live PySpark 4.1.2 vs repark, 2026-09-04, JDK 17, `TZ=UTC`)

| # | Control | Spark ANSI on | Spark ANSI off | repark ANSI on | repark ANSI off | Verdict |
|---|---|---|---|---|---|---|
| 1 | `regexp_extract('alpha', '([[:alpha:]]+)', 1)` | `'alpha'` | n/a (ANSI-independent) | `UnsupportedOperationException: functions.regexp_extract is not supported yet (engine gap; disclosed R-FN-BATCH1)`; SQL door: `Invalid function 'regexp_extract'` | same | **HALT** |
| 1 | `regexp_extract('fox', '([[:alpha:]]+)', 1)` (non-match) | `''` | n/a | same unsupported pair as above | same | **HALT** |
| 2 | `rlike` / `regexp_like` `'[[:alpha:]x]'` on `'x'`, `'fox'` | `true`, `true` both spellings | n/a (ANSI-independent) | `True`, `True` via `F.rlike`, `F.regexp_like`, SQL `regexp_like` (SQL `RLIKE` keyword stays unsupported: pre-existing door gap, out of scope) | same | PINNED |
| 3 | `elt` index 3 / 0 / −1 | raises `INVALID_ARRAY_INDEX` SQLSTATE 22003 | NULL / NULL / NULL; NULL index NULL | raises `INVALID_ARRAY_INDEX` | NULL / NULL / NULL; NULL index NULL | PINNED |
| 4 | `LIKE` escape-at-end: `F.like('ab', 'ab\')`, `'ab' LIKE 'ab\\'`, `'a%' LIKE 'a\\' ESCAPE '\\'` | raises `INVALID_FORMAT.ESC_AT_THE_END` SQLSTATE 42601, all three spellings | same, all three | raises `INVALID_FORMAT.ESC_AT_THE_END` SQLSTATE 42601, all three | same, all three | PINNED |
| 5 | `trim` / `ltrim` / `rtrim` of `'abc'` with `''` | `'abc'` all three; `TRIM(BOTH '' FROM 'abc')` `'abc'` | n/a (ANSI-independent) | same | same | PINNED |
| 5 | NULL trim set | `TRIM(BOTH NULL FROM 'abc')` NULL; API NULL | n/a | same | same | PINNED |
| 6 | `initcap('ünï_9 ab')` / `''` / NULL | `'Ünï_9 Ab'` / `''` / NULL | n/a (ANSI-independent) | same | same | PINNED |
| 7 | `chr(0)` / `(256)` / `(65536)` / `(1114112)` / `(-1)` / NULL | `'\x00'` ×4 / `''` / NULL | n/a (ANSI-independent) | same | same | PINNED |

Single-backslash SQL text (`'ab' LIKE 'ab\'`) is a `ParseException` on Spark under both modes
(parser-level, not the escape rule); the pinnable spellings are the doubled-backslash SQL text
and the `F.like` API. `regexp_extract` group index past the last group raises
`REGEX_GROUP_INDEX` SQLSTATE 22003-class 22023 on Spark (observed, not pinned: out of scope).

## Mutation

| Knob (one flipped expected in a scratch copy) | Red of M |
|---|---|
| elt ANSI-off `NULL` → `"a"` | 1 red of 1 (`test_elt_out_of_range_returns_null_with_ansi_off`) |
| like explicit `42601` → `42000` | 1 red of 1 (`test_like_escape_at_end_explicit_escape_raises`) |
| like ANSI-off `ESC_AT_THE_END` → `ESC_AT_THE_START` | 1 red of 1 (`test_like_escape_at_end_raises_with_ansi_off`) |
| trim noop `"abc"` → `"ab"` | 1 red of 1 (`test_fn_trim_empty_charset_is_noop`) |
| trim SQL expected `"abc"` flipped wrong | 1 red of 1 (`test_fn_trim_empty_and_null_charset_sql`) |
| initcap `"Ünï_9 Ab"` → `"Ünï_9 AB"` | 1 red of 1 (`test_fn_initcap_non_ascii_underscore_digit_boundaries`) |
| chr `65536 → \x00` → `\x01` | 1 red of 1 (`test_fn_chr_zero_large_negative_null`) |
| posix bracket `True` → `False` | 1 red of 1 (`test_bracket_posix_class_with_extra_literal_matches`) |

8 red of 8 new pin functions (21 parametrized cases).

```yaml
FINDING:
  id: F-FN-FIX-2-CTRL-1-1
  severity: S2
  category: AT-8
  clause: [C-002]
  claim: Control 1 cannot pin equality because repark implements regexp_extract on neither door, while Spark answers 'alpha' and ''.
  evidence: scratch/oracle_repark.py repark/on/extract_alpha_from_alpha (UnsupportedOperationException, disclosed R-FN-BATCH1); scratch/oracle_spark2.py spark/on/extract_alpha_from_alpha ('alpha')
  disposition: OPEN
```

## 9. Delivery

| Item | Path |
|---|---|
| Registry | `docs/spark-sql-iceberg-parity.md` §7 control notes on the six FN-FIX-2 rows |
| Pins | six `python/repark/tests/test_fn_*.py` files (controls 2–7); control 1 Spark oracle cells in `test_parity_live.py` only |
| Live legs | `test_parity_live.py::test_live_fn_fix_2_strings`, `::test_live_fn_fix_2_regex_like` on `spark_engine` |
| Maps | `task/ledgers/staging/map.md`, `python/repark/tests/map.md` lockstep |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: fn-fix-2-ctrl-1-controls
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every brief clause walked against the oracle table and the pins; control 1 filed as OPEN finding F-FN-FIX-2-CTRL-1-1 instead of absorbed.
      artifacts: [task/ledgers/staging/fn-fix-2-ctrl-1-controls-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: Empty, NULL, negative, zero, huge (65536, 1114112), non-ASCII, and ANSI-off inputs exercised on both engines.
      artifacts: [python/repark/tests/test_fn_chr_divergence.py, python/repark/tests/test_fn_trim_chars.py]
    - id: AT-3
      status: ATTACKED
      evidence: ANSI-on errors unchanged (INVALID_ARRAY_INDEX, ESC_AT_THE_END); ANSI-off elt answers NULL; LIKE still raises with ANSI off.
      artifacts: [python/repark/tests/test_fn_elt_out_of_range.py, python/repark/tests/test_fn_like_escape_end.py]
    - id: AT-4
      status: N/A
      justification: Scalar string kernels have no shared mutable state; one JVM ran the oracle sequentially.
    - id: AT-5
      status: N/A
      justification: No AWS, IAM, secrets, .github, or dependency-file change.
    - id: AT-6
      status: ATTACKED
      evidence: No product change; repark answers equal Spark's on controls 2-7 across doors; the control-1 gap is loud, not silent.
      artifacts: [python/repark/tests/test_fn_regex_posix_class.py, python/repark/tests/test_fn_initcap_divergence.py]
    - id: AT-7
      status: N/A
      justification: Pin-only unit; no loops, growth, or hot paths added.
    - id: AT-8
      status: ATTACKED
      evidence: Facade, SQL-door, and live-Spark spellings all measured; SQL RLIKE-keyword and regexp_extract gaps named, not presumed.
      artifacts: [python/repark/tests/test_parity_live.py]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: 8 new pin functions (21 cases) each red under one flipped expectation; live legs co-collect with disclosure.
      artifacts: [scratch/run_mutations.py, python/repark/tests/test_parity_live.py]
```
