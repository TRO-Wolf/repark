# Unit ledger — FNP-6b · randstr + uniform

**Unit:** FNP-6b · **Date:** 2026-08-20 · **Executor:** Claude (Opus 5) ·
**Branch:** `feat/spark-function-parity` · **Base:** `0fd7f00` (FNP-6a) ·
**Charter:** [fnp-0-charter-ledger.md](../../staging/fnp-0-charter-ledger.md) clauses **C-007**, **C-012** ·
**Design:** [../docs/design/spark-function-parity.md §7](../../../../docs/design/spark-function-parity.md).
**SEPMO:** STANDARD. Floor S1.

**Writable:** `crates/repark-functions/src/{random.rs,expr_fn.rs}`,
`crates/repark-python/src/column/function_dispatch.rs`,
`python/repark/src/repark/spark/{functions.py,functions_expr.py}`, the facade tests, this ledger,
`task/map.md`, the touched `map.md` files.

## What was inherited

`random.rs` implements Spark's `XORShiftRandom` bit-exactly — Marsaglia xorshift over a
double-`MurmurHash3` `hashSeed`, with the Java/Scala bit-width casts spelled out — and `rand` /
`randn` were validated against Spark on it in an earlier campaign (r20 G2). Both new kernels draw
from that stream rather than introducing a second PRNG, and a pin asserts the integer and float
forms of `uniform` are the same draws scaled, not two generators that happen to look plausible.

## What the pins prove, and what they do not

This is the honest limit of this unit, stated at the top of the test module as well.

**Inherited and already pinned:** the *stream* is Spark-faithful.

**DOC-SPARK, not MEASURED-SPARK:** the per-function derivation — whether Spark's `randstr` indexes
its 62-character pool in this order and consumes one draw per character, and whether `uniform`
scales `nextDouble()` this way. No live Spark runs in this worktree.

So the rows pin the properties Spark's documentation *states*: length, character pool, range, the
integer-vs-double return rule, determinism per seed, difference across seeds, and loud refusal of
non-constant bounds. They deliberately do **not** assert specific generated values. A value pin
here would read as parity evidence while being nothing but repark agreeing with itself — the
failure mode `test_functions_gt2.py`'s own header calls out after a previous round claimed a live
oracle it did not have.

Closing this to MEASURED-SPARK needs the live differential harness, and is a FNP-Z item.

## Semantics implemented

| | |
|---|---|
| `randstr(length[, seed])` | `length` characters from `0-9a-zA-Z`. Pool ORDER is load-bearing — the index comes from the shared stream, so a different ordering changes the output for a given seed. |
| `uniform(min, max[, seed])` | i.i.d. in `[min, max)`. **Return type follows the bounds**: two integers give `Int64`, anything else `Float64` — Spark's documented rule, and a silent type change if got wrong, so it is pinned on the type. |

Both refuse a non-constant bound loudly. Spark requires literals, and the alternative failure —
silently reading row zero and applying it to every row — is the kind that produces plausible
output forever.

## Findings

| ID | Sev | Finding | Disposition |
|---|---|---|---|
| F-FNP6B-1 | S1 | `randstr` / `uniform` absent from both doors | `REMEDIATED` — registered and dispatched; pinned on documented properties |
| F-FNP6B-2 | S2 | Neither can be value-verified without a live Spark | `ACCEPTED_FLAGGED` — stated in the module docstring and here; no value pin pretends otherwise. Closing it is a FNP-Z live-oracle item. |
| F-FNP6B-3 | S3 | A non-constant bound could have silently used row zero | `REMEDIATED` — refused loudly in the kernel, pinned per function |

## Gates

| Gate | Result |
|---|---|
| `cargo test --workspace --no-fail-fast` | **45 binaries, 1,990 passed, 0 failed**, cargo exit 0 |
| `make ci` | exit **0**. Two clippy reds on the way, one of which improved the semantics — see below. |
| facade pytest (full) | **3,508 passed, 70 skipped, 0 failed** |

## A lint that was right about behaviour, not style

`clippy::neg_cmp_op_on_partial_ord` rejected `if !(low <= high)`. That is not a formatting
complaint: with a NaN bound the expression is `true`, so NaN was being refused with the message
*"uniform min must not exceed max"* — a different mistake wearing the wrong explanation. A NaN
bound is **incomparable**, not out of order.

Rewritten over `partial_cmp`, the two cases refuse separately, and the NaN case is pinned. Worth
recording because the lint is filed under readability and the defect it exposed was a wrong error
message on a reachable input.
