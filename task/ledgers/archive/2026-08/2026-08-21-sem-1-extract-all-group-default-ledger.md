# Unit ledger — SEM-1 · `regexp_extract_all` defaults to capture group 1

**Date:** 2026-08-21 · **Branch:** `fix/spark-semantics` · **Base:** `44df821` (SEM-4) ·
**Charter:** [sem-0-charter-ledger.md](../../completed/sem-0-charter-ledger.md) · **Closes:** registry row `RE-1`

**This unit changes what a working query returns**, on the owner's dated ruling. It is the first
such change since the port. `regexp_extract_all('a1b2', '([a-z])([0-9])')` returned `['a1','b2']`
and now returns `['a','b']`.

## 1. Reproduced first

On `44df821`, against the wheel built from it:

| Call | This tree | Spark 4.1.2 |
|---|---|---|
| facade `regexp_extract_all(s, PAIRS)` | `['a1','b2']` | `['a','b']` |
| SQL door, same call | `['a1','b2']` | `['a','b']` |
| either door, `idx=0` | `['a1','b2']` | `['a1','b2']` |
| either door, `idx=1` | `['a','b']` | `['a','b']` |
| `regexp_extract_all('a1b2','[a-z]([0-9])')` | `['a1','b2']` | `['1','2']` |
| `regexp_extract_all('a1b2','[0-9]*')` | `['','','','','']`-shaped list | **raises** `REGEX_GROUP_INDEX` |
| `regexp_substr('a1b2', PAIRS)` | `'a1'` | `'a1'` |

Every Spark value transcribed from the live oracle
([../docs/design/low-risk-sweep.md](../../../../docs/design/low-risk-sweep.md) §7), none read back out of
repark.

## 2. The change — one line, and the charter was right about that

`crates/repark-functions/src/spark_regexp.rs`, `extract_rows`: `None => 0` becomes `None => 1`.

One knob serves both doors, because the facade passes **no default of its own** —
`functions_expr.regexp_extract_all` omits the third argument entirely when the caller omits it.
`regexp_substr` shares the walk but binds the group as `_group` and never reads it, so it is
provably untouched; the pin asserts it at `'a1'` on both sides anyway.

Two docstrings that stated the old default were corrected in the same commit (the Rust kernel doc
and the facade docstring).

## 3. Pin, red before the fix

`python/repark/tests/test_sem1_extract_all_group_default.py` — 10 assertions, **6 failed / 4 passed**
before the edit. The 4 that passed are the controls: the explicit `idx=0/1/2` cases and
`regexp_substr`, all of which had to stay exactly as they were.

**One assertion in the first draft of this pin was wrong and was caught by measuring it.** It read
`regexp_substr('a1b2','[0-9]*') == ''` — which is repark's answer, written back into a test as if
it were Spark's. Spark returns **NULL**. The line was removed and the finding routed to SEM-5,
which registers it properly. This is exactly the failure the testing contract names, and it
survived a draft.

## 4. Collateral — three sites, two of them not assertion failures

The charter predicted all three, and predicted the shape of the surprise:

| Site | How it went red | Resolution |
|---|---|---|
| `test_lrs6_regexp_divergences.py` — both `RE-1` pins | By design | **Retired**, not flipped. The row is closed, so its pins leave with it; `test_sem1_…` owns the assertions now. The file's docstring records that it went exactly as its own contract promised. |
| `test_fnp6_regexp.py::test_extract_all_reuses_the_java_matcher_stepping` | **Runtime error.** `[0-9]*` has no capture group | `idx=0` named explicitly — the test is about the stepping walk, not the group default |
| `test_fnp_critic_remediation.py::test_empty_pattern_agrees_between_counting_and_collecting` (all 4 parametrizations) | **Runtime error.** Same mechanism, empty pattern | Same resolution |

The two runtime-error sites are the ones worth remembering: a reader working from the registry row
alone would have hit them with no warning, because nothing in the repo connected them to `RE-1`
before the charter enumerated them.

## 5. Registry

`RE-1` is **deleted** from [../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md),
per §6 — a closed row leaves the registry rather than being marked closed in place. STATUS carries
the state.

## 6. Gates

Each captured alone with its own `$?`:

| Gate | Result |
|---|---|
| `cargo test -p repark-functions` | 232 passed, 0 failed |
| facade, regexp-adjacent (`-k "regexp or lrs6 or fnp6 or critic or sem"`) | 262 passed, 6 skipped, 0 failed |
| `make ci` | exit 0 |

## 7. Found in passing

**Spark's `regexp_substr` returns NULL for any zero-width match, on any text** — `''`, `[0-9]*` and
`b*` all give NULL on plain ASCII, where repark gives `''`. That makes `RE-2`'s stated bound
("both divergences are confined to supplementary-plane text") **false** for its `regexp_substr`
half. Registered and corrected by SEM-5, in the next commit; not folded in here, because it is a
different mechanism from the group default and a value change the owner has not ruled on.
