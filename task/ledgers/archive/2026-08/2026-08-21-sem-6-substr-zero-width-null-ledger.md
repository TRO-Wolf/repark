# Unit ledger — SEM-6 · `regexp_substr` returns NULL for a zero-width match

**Date:** 2026-08-21 · **Branch:** `fix/re3-substr-null` · **Base:** `f3eaa9d` (`main`, post-#192) ·
**Charter:** [sem-0-charter-ledger.md](../../staging/sem-0-charter-ledger.md) · **Closes:** registry row `RE-3`

**This unit changes what a working query returns**, on the owner's ruling of 2026-08-21 ("lets fix
RE-3 too"). `regexp_substr('ab', '')` returned `''` and now returns NULL.

`RE-3` was registered one commit earlier by [SEM-5](2026-08-21-sem-5-substr-zero-width-ledger.md) and is
retired here. That order was deliberate: the difference was measured and written down as a row
before anyone proposed fixing it, so the decision was taken against evidence rather than against a
patch.

## 1. Spark's rule, measured across the space rather than inferred

The rule is **take the first match; if it is empty, the result is NULL** — Spark does not go looking
for a later non-empty match. `regexp_substr('a1b2', '[0-9]*')` is NULL *even though* `'1'` matches
at position 1. That is the distinction that separates this from "NULL when there is no match",
which repark already had right.

Twelve cases, both engines, before the fix:

| Call | repark (before) | Spark 4.1.2 |
|---|---|---|
| `regexp_substr('ab', '')` | `''` | **NULL** |
| `regexp_substr('ab', 'b*')` (empty at 0) | `''` | **NULL** |
| `regexp_substr('a1b2', '[0-9]*')` (empty at 0, `'1'` later) | `''` | **NULL** |
| `regexp_substr('', '')` | `''` | **NULL** |
| `regexp_substr('ac', '(b)?')` | `''` | **NULL** |
| `regexp_substr('ab', '$')` | `''` | **NULL** |
| `regexp_substr('🎉ab', '')` | `''` | **NULL** |
| `regexp_substr('ab', 'a*')` (**non-empty** at 0) | `'a'` | `'a'` |
| `regexp_substr('a1b2', '[0-9]+')` | `'1'` | `'1'` |
| `regexp_substr('a1b2', '([a-z])([0-9])')` | `'a1'` | `'a1'` |
| `regexp_substr('ab', 'x')` (no match) | NULL | NULL |
| NULL subject / NULL pattern | NULL | NULL |

Seven diverge, five already agreed. All transcribed from the live oracle
([../docs/design/low-risk-sweep.md](../../../../docs/design/low-risk-sweep.md) §7).

**`a*` on `'ab'` is the case that makes the fix precise.** A change written as "empty *pattern* →
NULL" instead of "empty *match* → NULL" would pass every one of the seven and still be wrong. It is
pinned from both sides for that reason.

## 2. The change

`crates/repark-functions/src/spark_regexp.rs`, `invoke_substr`'s closure — one filter:

```rust
Some((text, regex, _group)) => regex
    .find(text)
    .map(|found| found.as_str())
    .filter(|matched| !matched.is_empty())
    .map(str::to_owned),
```

`regexp_extract_all` shares `extract_rows` but not this closure, and is untouched — it has its own
empty-match convention (an empty string is a legitimate element there), which is exactly why the
two functions differ and why `RE-2` still stands.

## 3. Pin, red before the fix

`python/repark/tests/test_sem6_substr_zero_width_null.py` — 13 assertions, **7 failed / 6 passed**
before the edit, **13 passed** after. The 6 that passed are the controls: the three non-empty
matches and the three already-NULL paths.

## 4. Collateral — one site, and it was the pin that was supposed to break

| Site | Resolution |
|---|---|
| `test_lrs6_regexp_divergences.py::test_re3_substr_of_a_zero_width_match_is_empty_not_null` | **Retired.** The row is closed, so its pin leaves with it, exactly as that file's contract says. |

Every other `regexp_substr` call site in the tree uses a `+`-quantified pattern, so none of them
could be affected. Verified by reading all of them rather than by the suite staying green.

## 5. Found in passing, and fixed

`test_fnp6_regexp.py` carried `assert got[1] is not ""` with an `F632` suppression — an **identity**
comparison against a string literal. It happened to work only because CPython interns the empty
string; it would have read as passing for any other value that was not literally that object. It is
now `assert got[1] is None`, which is what it was always trying to say. A lint suppression was the
only thing keeping it in the tree.

## 6. Registry

`RE-3` is deleted from
[../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md), per §6. `RE-2`'s dated
note is updated to say the half that left it was closed the same day rather than merely relocated.

The facade docstring for `regexp_substr` now states **both** NULL cases; it previously named only
"no match", which is how the zero-width behavior went unexamined for so long.

## 7. Gates

Each captured alone with its own `$?`:

| Gate | Result |
|---|---|
| `cargo test -p repark-functions` | 232 passed, 0 failed |
| facade, regexp-adjacent | 282 passed, 6 skipped, 0 failed |
| `make ci` | exit 0 |
| `make preflight` | **exit 0** — 45 Rust binaries / 1,991 passed / 0 failed; facade 3,636 passed / 70 skipped / 0 failed |
