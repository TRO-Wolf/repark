# Unit ledger — FNP-6c · validate_utf8, try_validate_utf8, assert_true

**Unit:** FNP-6c · **Date:** 2026-08-20 · **Executor:** Claude (Opus 5) ·
**Branch:** `feat/spark-function-parity` · **Base:** `a38a853` (FNP-6b) ·
**Charter:** [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) clauses **C-007**, **C-012** ·
**Design:** [../docs/design/spark-function-parity.md §7](../docs/design/spark-function-parity.md).
**SEPMO:** STANDARD. Floor S1.

**Writable:** `crates/repark-functions/src/{validate.rs,lib.rs,expr_fn.rs}`,
`crates/repark-python/src/column/function_dispatch.rs`,
`python/repark/src/repark/spark/{functions.py,functions_expr.py}`, the facade tests, this ledger,
`task/map.md`, the touched `map.md` files.

## One shape, three names

Inspect a value, hand it back when acceptable, fail loudly or yield NULL when not. None of the
three computes anything, which is why they share a module.

| | valid / true | invalid / false / NULL |
|---|---|---|
| `validate_utf8` | the decoded string | **error**, `[INVALID_UTF8_STRING]` |
| `try_validate_utf8` | the decoded string | **NULL** |
| `assert_true` | NULL (typed `null`) | **error**, caller's message if given |

`assert_true` raises on a NULL condition as well as on `false`: only `true` passes, and NULL is
not true. Pinned, because "NULL passes silently" is the version of this function that is worse
than not having it.

## The structural note the UTF-8 pair needs

An Arrow `Utf8` array **cannot hold invalid UTF-8** — Rust's `&str` forbids it — so on a string
column these two are tautologies. The case that can actually fail is `Binary`, and that is already
how `datafusion-spark`'s `is_valid_utf8` behaves: measured, `X'61FF62'` → `false`, and
`make_valid_utf8` on the same bytes → `a\u{FFFD}b`. These kernels follow it — accept binary, judge
the bytes, return the decoded string.

Spark's own strings are `UTF8String` byte arrays that *can* carry invalid sequences, so a Spark
program can reach these on a STRING column where repark structurally cannot. **That is a
difference in value representation, not a behaviour choice**, and it is written at the module, in
the facade docstrings and here rather than left for someone to discover. The tests exercise the
binary path, where the two engines genuinely agree.

## Findings

| ID | Sev | Finding | Disposition |
|---|---|---|---|
| F-FNP6C-1 | S1 | All three absent from both doors | `REMEDIATED` — registered, dispatched, pinned per behaviour and per door |
| F-FNP6C-2 | S2 | On a STRING column the UTF-8 pair can never fail in repark, but can in Spark | `ACCEPTED_FLAGGED` — a value-representation difference; documented at three sites. Registry row handed to FNP-Z |
| F-FNP6C-3 | S3 | `crates/repark-functions/src/lib.rs` hit its 175-line `check_lib_rs` ceiling | `REMEDIATED` — the ceiling ratchets DOWN only, so it was **not** raised: registration moved into `validate::register` (the `decimal_spark` / `higher_order` precedent) and the `approx_percentile` alias block collapsed from a scoped `use` to a direct call. 176 → 174. |

## The ceiling was the right constraint

F-FNP6C-3 is worth a line. Adding a module and a registration loop pushed the crate root one line
over, and the sanctioned outs are "split the module" or "edit the EXCEPTIONS table with a reason"
— where the table only ratchets down. Taking the first out produced a *better* `register_all`:
two fewer inline blocks, and the new module owns its own installation exactly as `decimal_spark`
and `higher_order` already do. A ceiling that can only be met by improving the code is doing its
job.

## Gates

| Gate | Result |
|---|---|
| `cargo test --workspace --no-fail-fast` | **45 binaries, 1,990 passed, 0 failed**, cargo exit 0 |
| `make ci` | exit **0**. Three reds on the way: clippy `unnecessary_literal_bound` and `redundant_closure_for_method_calls`, and one over-long docstring. |
| facade pytest (full) | first run **1 failed, 3,516 passed, 70 skipped** — the fourth fence, below; then green |

## The fourth fence

`test_functions_f.py::test_deferred_fn_f_names_are_absent` listed `assert_true` among FN-F's
deferred names. That is now four units in a row — FN-D's `datediff`, FN-C's `grouping` and
`approx_count_distinct`, and now this — where the blocker was a prior unit's scope fence recorded
as a passing assertion.

The list is marked **RATCHETS DOWN** and every remaining entry now carries **why** it is still
absent: the camelCase bitwise names are PySpark's deprecated aliases; `try_add`/`try_sum` belong
to the ANSI-seam sweep rather than being independent builds; `to_number`/`to_binary` need real
numeric-format kernels because DataFusion's `to_char` is a false friend.

`assert_true` was never engine work. It is a raise kernel over the pattern `ansi.rs` already
established for `__repark_ansi_nonzero_divisor__` — which is precisely the kind of thing a bare
name on a deferred list hides.
