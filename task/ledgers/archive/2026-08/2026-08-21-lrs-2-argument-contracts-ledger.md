# Unit ledger — LRS-2 · argument contracts that match PySpark

**Date:** 2026-08-20 · **Branch:** `fix/low-risk-sweep` · **Charter:**
[lrs-0-charter-ledger.md](2026-08-21-lrs-0-charter-ledger.md) · **Design:**
[../docs/design/low-risk-sweep.md](../../../../docs/design/low-risk-sweep.md) §3 LRS-2 ·
**Source findings:** round 2 F-R3-10, FNP-R3-6

## The oracle refuted two of the three suggested fixes

This unit came in as three suggestions from an adversarial review. Measured against a live PySpark
4.1.2 before any edit, **two were wrong** — and both would have shipped a new divergence.

| Suggestion | Spark's actual answer | Outcome |
|---|---|---|
| accept `F.xxhash64()` and emit `lit(42)` | `AnalysisException [WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The 'xxhash64' requires > 0 parameters but the actual number is 0` — facade and SQL alike | **inverted**: refuse, do not accept |
| reject every parameter kind that is not `POSITIONAL_OR_KEYWORD` | `lambda x, /: x > 2` **works** in Spark | **narrowed**: positional-only is allowed |
| guard a non-callable `f` with a `PySparkValueError` | plain `TypeError: 'nope' is not a callable object` — byte-for-byte what repark already raises | **dropped**: no change |

The review was right that all three call sites were wrong; it was wrong about what right looks like
in two of them. That is the argument for the oracle, and it is why the refutations are **pinned**:
a test holding the rejected behaviour in place is what stops it being "fixed" again by the next
reader of the same finding.

## What changed

- **`xxhash64()`** now raises with Spark's own error class and message. Only the message was ever
  wrong — the refusal was correct — but `call_scalar(xxhash64) expects at least 1 args, got 0`
  names an internal dispatcher the user never called.
- **`_lambda_arity`** checks parameter *kinds* against a named allowlist
  (`POSITIONAL_ONLY`, `POSITIONAL_OR_KEYWORD`) rather than blacklisting two of the four. That
  covers keyword-only — which used to pass the gate and fail later as a raw Python
  `TypeError: <lambda>() takes 0 positional arguments but 1 was given` — and it keeps
  positional-only working. The message is Spark's:
  `[UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION] Function \`<lambda>\` should use only
  POSITIONAL or POSITIONAL OR KEYWORD arguments.`, verified identical against the oracle for the
  `*args` case.
- **Nothing** for the non-callable path.

## Evidence

- 7 pins in `python/repark/tests/test_lrs2_argument_contracts.py`. Against the base with the two
  source files stashed: **4 failed, 3 passed**. The three that pass on both sides are the
  refuted-suggestion pins — that is exactly what they are for. Two of the four reds are message
  changes on paths that already refused, which the ledger states rather than dressing up as new
  refusals.
- `xxhash64("k")` returns **-6698625589789238999** here and in Spark — value parity checked against
  the oracle, not read back from repark, so the refusal is proven not to have narrowed the working
  form.
- facade — **3,564 passed, 70 skipped, 0 failed** (3,557 before, +7). `make ci` exit 0.

## Disposition

**DELIVERED.** Charter C-001 held: `xxhash64()` and a keyword-only lambda both failed before this
unit, and every shape that worked still works.
