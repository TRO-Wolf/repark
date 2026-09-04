# Unit ledger — SEM-5 · `regexp_substr`'s zero-width match, registered honestly

**Date:** 2026-08-21 · **Branch:** `fix/spark-semantics` · **Base:** `219957f` (SEM-3) ·
**Charter:** [sem-0-charter-ledger.md](../../completed/sem-0-charter-ledger.md)

Registration only. **No code changes and no value changes** — this unit adds a registry row, splits
it out of a row where it was misfiled, and corrects a claim in a test docstring that measurement
showed to be false.

## 1. How it was found, which is the point of this ledger

While writing SEM-1's pin I added a `regexp_substr` control:

```python
assert _sql("SELECT regexp_substr('a1b2','[0-9]*') AS r") == [""]
```

That value was **read out of repark**, not measured against Spark. It is precisely what
[../docs/testing.md](../../../../docs/testing.md) forbids — "a test asserts a measured value, never one read
back out of the code under test" — and it was written by someone who had quoted that rule earlier
the same day, into a pin whose whole subject was Spark parity.

It was caught because the unit's own procedure ran every unmeasured assertion past the oracle
before committing. Spark returns **NULL**. Had the control shipped, it would have been a *green
pin asserting a divergence as if it were parity* — the most expensive kind of wrong test, because
it looks like evidence.

## 2. What the measurement actually showed

Live PySpark 4.1.2 vs this tree, all on **plain ASCII**:

| Call | repark | Spark |
|---|---|---|
| `regexp_substr('ab', '')` | `''` | **NULL** |
| `regexp_substr('a1b2', '[0-9]*')` | `''` | **NULL** |
| `regexp_substr('ab', 'b*')` | `''` | **NULL** |
| `regexp_substr('ab', 'x')` (no match) | NULL | NULL |
| `regexp_substr('a1b2', '[0-9]+')` | `'1'` | `'1'` |

The difference is exactly the **zero-width match**. A genuine no-match is already right, and a
non-empty match is already right.

## 3. The correction this forces

`RE-2` is titled "a zero-width match at a mid-surrogate position" and carried
`regexp_substr('🎉ab','')` → `''` vs NULL as one of its bullets. That filed a **general** difference
under a **surrogate-shaped** heading, and the LRS-6 pin file went further, asserting in a docstring
that "both divergences are confined to supplementary-plane text". The table above shows that is
false for the substr half.

- `RE-2` is **narrowed** to the count divergence, which really is surrogate-bound, with a dated note
  saying what left and why.
- `RE-3` is a **new row** for the substr behavior, stated generally, measured on ASCII.
- The LRS-6 pin becomes `test_re3_substr_of_a_zero_width_match_is_empty_not_null`, covering ASCII
  and astral text, with the two controls that keep the row narrow.
- `test_bmp_text_already_agrees_with_spark_everywhere` is renamed to
  `test_bmp_counting_and_collecting_already_agree_with_spark` and its docstring restricted to what
  it actually asserts.

## 4. Why RE-3 is not fixed here

It changes what a working query returns, and the owner's 2026-08-21 ruling authorized exactly one
such change (`RE-1`). The fix looks small — `invoke_substr` returning `None` for an empty match —
but "looks small" is what the charter says is not sufficient grounds. It is registered with a pin
that codifies today's behavior, so the unit that closes it reds on purpose.

## 5. Gates

| Gate | Result |
|---|---|
| `python/repark/tests/test_lrs6_regexp_divergences.py` | 3 passed, 0 failed |
| facade, regexp-adjacent | 270 passed, 6 skipped, 0 failed |
| `make ci` | exit 0 |
