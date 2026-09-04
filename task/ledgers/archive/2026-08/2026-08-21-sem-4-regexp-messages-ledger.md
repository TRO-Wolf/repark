# Unit ledger — SEM-4 · the regexp refusals say Spark's words

**Date:** 2026-08-21 · **Branch:** `fix/spark-semantics` · **Base:** `8c660f6` (`main`, post-#191) ·
**Charter:** [sem-0-charter-ledger.md](../../completed/sem-0-charter-ledger.md)

Message-only. **No computed value changes in this unit** — every assertion here is about what a
refusal *says*, and the two tests that exercise a legal index prove the accepting path is
untouched.

Sequenced first on the branch so that SEM-1, which makes the group-index refusal reachable from an
ordinary two-argument call, writes its pins against the final wording rather than against a message
about to change.

## 1. Reproduced first

Measured on `8c660f6` against the wheel built from it, through the SQL door:

| Call | This tree said |
|---|---|
| `regexp_extract_all('a1b2','([a-z])([0-9])', 3)` | `Execution error: regexp_extract_all group index 3 is out of range for a pattern with 2 groups` |
| `regexp_extract_all('a1b2','([a-z])([0-9])', -1)` | `Execution error: regexp_extract_all group index must not be negative, got -1` |
| `regexp_extract_all('a')` | `'regexp_instr' expects 2 or 3 arguments, got 1` |
| `regexp_substr('a')` | `'regexp_count' expects 2 arguments, got 1` |
| `regexp_extract_all('a1b2','…', array(1))` | `'regexp_instr' idx must be an integer (Spark casts STRING), got List(…)` |

The last three are repark misreporting **itself**: `coerce_regexp_args` hard-coded one of two
names, and two of its four callers are neither of them. Found while reading the file for the group
index, not from any finding.

## 2. The oracle

Live PySpark 4.1.2 (see [../docs/design/low-risk-sweep.md](../../../../docs/design/low-risk-sweep.md) §7).
Spark folds **both** directions into one condition, and its text carries the pattern's group count:

```
[INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] The value of parameter(s) `idx` in
`regexp_extract_all` is invalid: Expects group index between 0 and 2, but got 3. SQLSTATE: 22023
```

`-1`, `3` and `99` all produce it with the bound filled in; a zero-group pattern reads
`between 0 and 0`. Transcribed from the oracle, not read back out of repark.

**Spark's exception class is `SparkRuntimeException`.** repark raises `PySparkException` here,
because a `DataFusionError::Execution` maps to that class and adding a new public error type is a
separate seam. The **condition name is what a migrating user greps for** and it is now exact; the
class difference is a recorded residual, not a claim. This is the same shape LRS-2 used for
`[WRONG_NUM_ARGS.WITHOUT_SUGGESTION]`.

## 3. The change

`crates/repark-functions/src/spark_regexp.rs`:

- **`extract_rows` stops validating** and passes the index through raw (`usize` → `i32` in the
  callback). Its own negative check could never produce Spark's text: the bound comes from
  `regex.captures_len()`, which is only known after the pattern compiles, in the caller.
- **`invoke_extract_all` validates** via a new `validate_group_index`, one `Result<usize>` carrying
  Spark's wording verbatim.
- **`coerce_regexp_args` takes the caller's `name`** instead of guessing it from `allow_index`. All
  four kernels now pass their own.

`regexp_substr` is unaffected in behavior: `coerce_regexp_args(.., false)` caps it at two
arguments, so it can never receive an index to validate. Its `_group` binding was already unused
and stays unused.

## 4. Pin, red before the fix

`python/repark/tests/test_sem4_regex_group_index_message.py` — 11 assertions.

- Written and run **before** any edit: **8 failed, 3 passed**. The 3 that passed are the two
  already-correct arity names (`regexp_count`, `regexp_instr`) and the legal-index test, which is
  the control.
- After the fix: **11 passed**.

## 5. Gates

Each captured alone with its own `$?`:

| Gate | Result |
|---|---|
| `cargo test -p repark-functions` | 232 passed, 0 failed |
| facade, regexp-adjacent (`-k "regexp or lrs6 or fnp6 or critic"`) | 83 passed, 5 skipped, 0 failed |
| `make ci` | exit 0 |

## 6. Found in passing, not fixed here

Nothing. The two message defects above were the whole of what reading this file turned up.
