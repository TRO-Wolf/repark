# Unit ledger — LRS-4 · giving the C-012 guard a real domain

**Date:** 2026-08-20 · **Branch:** `fix/low-risk-sweep` · **Charter:**
[lrs-0-charter-ledger.md](lrs-0-charter-ledger.md) · **Design:**
[../docs/design/low-risk-sweep.md](../docs/design/low-risk-sweep.md) §3 LRS-4 ·
**Source finding:** round 2 FNP-R3-7

## The guard was checking a list someone had to remember to extend

Charter clause C-012 says the facade and the SQL door must resolve the same kernel, and
`door_parity_tests.rs` was the mechanical proof of it — over a **hand-maintained** `SCALAR_NAMES`.
So every kernel the parity campaign added sat outside the guard until somebody thought to add it,
and "makes the policy mechanical" claimed more than it enforced.

The new test walks the **session's own registry** instead: whatever `register_all` installed, at
every arity the facade will build, with `EXPECTED_DIVERGENCES` as the only way out. A name added to
either side joins the checked set on the day it is added, and nobody maintains a list.

**Domain: 20 hand-listed names → 341 registered ones.**

## What it found on the first run

Four names. Each was adjudicated against the live Spark oracle rather than sanctioned to make the
test pass — the design's rule for this unit, and the reason it was scheduled last.

| Name | What differs | Adjudication |
|---|---|---|
| `log` | facade lowers to `ln` and returns **2.0794** for `log(8)`; the SQL door returns **0.9031**, DataFusion's base 10 | **registry row `LOG-1`** — Spark gives 2.0794. A silently wrong answer on a common function |
| `from_unixtime` | facade returns STRING, SQL door returns TIMESTAMP | **registry row `UNIX-1`** — Spark returns STRING, so the facade is right |
| `array` | facade builds `make_array`, the door resolves the `array` alias | sanctioned — values agree, only kernel identity differs |
| `array_element` | facade reaches `element_at` (returns `20`), the door's `array_element` returns `NULL` for a valid index | sanctioned — Spark has no `array_element` at all (`UNRESOLVED_ROUTINE`), so this is an engine defect on a non-Spark spelling, not a parity gap |

**`LOG-1` is the point of this unit.** `SELECT log(x)` through the SQL door has been returning a
number off by a constant factor — plausible-looking, never wrong enough to notice, and invisible to
every test because the guard's domain did not include the name. Widening the domain found it in one
run.

Neither `LOG-1` nor `UNIX-1` is **closed** here: `log` needs a Spark-semantics kernel registered
over DataFusion's, and both change what a working query returns, which this campaign's invariant
does not allow. Both pins codify today's behavior so the fix reds them.

## Evidence

- `EXPECTED_DIVERGENCES` grew 20 → 24, and the asserted length moved with it in the same commit —
  which is the mechanism that forced each of the four to be justified in writing rather than
  appended quietly.
- 3 pins in `python/repark/tests/test_lrs4_door_domain.py`, all measured on both doors and against
  the oracle. One asserts the two-argument `log(2, 8)` already agrees at `3.0`, bounding `LOG-1` to
  the one-argument form.
- `cargo test --workspace` — 45 binaries, **1,991 passed, 0 failed** (+1: the new guard).
- facade — **3,596 passed, 70 skipped, 0 failed** (3,593 before, +3). `make ci` exit 0.

## Disposition

**DELIVERED.** No code behavior changed; a guard's domain grew by 17x and two real divergences came
out of it. Charter C-008 held — the sanctioned-out table could not grow without the assertion going
red first.
