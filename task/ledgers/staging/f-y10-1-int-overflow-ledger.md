# Charter ledger — F-Y10-1 · integer arithmetic overflow raises where Spark raises

**Date:** 2026-08-30 · **Branch:** `feat/f-y10-1-int-overflow` (opens when the owner confirms
this gate; may run in a fork-wait window) · **Base:** `main` at charter time · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Verify before done" and
[../../../docs/testing.md](../../../docs/testing.md) · **Path:** STANDARD (kernel work in
`crates/repark-functions` on the DEC U5 pattern; the live Spark 4.1.2 oracle is on this box).

**Retires:** moved to `completed/` in this unit's departure commit.

**Why now.** A wrong answer on ordinary addition outranks any missing function
(`docs/design/spark-function-parity.md` §7.1), and FNP-7b's four `try_*` names are blocked on
this unit: with no raising path they would be no-op wrappers asserting a parity that does not
exist. The item is already tracked (F-Y10-1 / DEC U5 / G13, named in STATUS since the V2
hardening campaign) and the decimal analog DEC-6 is FIXED by exactly the shape this unit
proposes — a checked kernel reading the landed ANSI knob. The tree also carries two
contradictory measurements that must be reconciled before any edit: Y-10 (2026-08-13, Z-4
handoff) observed `CAST(2147483647 AS INT) + CAST(1 AS INT)` **wrap** to Int32 `-2147483648`
on both doors; the FNP-7b row (2026-08-20) measured the same expression **widening** to int64
`2147483648`. At least one description is stale or path-specific; the unit's first clause is
the measurement that settles it.

## PROPOSITION LEDGER — F-Y10-1 — 2026-08-30

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | **The behavior matrix is measured before any edit.** For `+`, `-`, `*` on int32 and int64 at the overflow boundary, on all three doors (Spark SQL door, ANSI door, facade expression), with `spark.sql.ansi.enabled` TRUE and FALSE: the current repark result (wrap / widen / raise / NULL) and the live Spark 4.1.2 result are recorded side by side, and the Y-10-vs-FNP-7b wrap-vs-widen contradiction is resolved with the mechanism named (which path widens, which wraps, and why). | The matrix in this ledger, each cell from an executed probe; the contradiction's resolution names the code path. | OPEN | Charter question: which description is stale — Y-10's both-door Int32 wrap (2026-08-13) or FNP-7b's int64 widen (2026-08-20)? |
| C-002 | **Spark door raises where Spark raises.** With `ansi=true` (the landed default), integer `+`, `-`, `*` at the boundary raise `ARITHMETIC_OVERFLOW` exactly where live Spark raises, as shared-raise equality pins on the DEC-6 pattern; with `ansi=false`, the result equals live Spark's non-ANSI result (two's-complement wrap) cell for cell, value and Arrow type. No silent widening remains on any Spark-door path the matrix touched. | Checked kernels in `crates/repark-functions` reading the ANSI knob (DEC U5 shape); red-first corpus pins per cell; oracle read-back. | OPEN | The knob and the checked-UDF pattern exist (DEC-6 / #94 / #99); this clause extends them to the integer kernels. |
| C-003 | **The ANSI door serves standard SQL.** Overflow on the ANSI door raises per the standard (its own oracle, owner ruling 2026-08-12 Option A) — it does not silently wrap once the Spark-door kernels are checked. Any INTENDED door-vs-door split this creates is pinned in `cross_door.rs` like the six existing ones, not left implicit. | ANSI-door value pins; a cross-door pin per intended split. | OPEN | If the two doors share the arithmetic path, this may fall out of C-002; the matrix decides. |
| C-004 | **Documents match the pins.** The registry's routed note (F-Y10-1 under "routed, not invented as DEC rows") moves to a dated FIXED row or an updated finding; gap G13's integer half is closed or narrowed with the residue named; the FNP-7b row in `docs/design/spark-function-parity.md` flips from BLOCKED to unblocked; STATUS; maps in lockstep. | `check-map-sync`, `check-ledger-grammar`, registry diff. | OPEN | G5b-R3-ANSI (window RANGE wrap) stays open and out of scope — same G13 campaign body, different unit; the registry note must keep saying so. |
| C-005 | **Green on the whole surface, and the hot path is not quietly slower.** `make verify`, `make preflight`, full `make py-test`; the checked kernels' cost on non-overflowing arithmetic is measured (a micro-benchmark or the existing perf harness) and recorded — an order-of-magnitude regression is a finding, not a silent tax. | Gate output; the recorded measurement. | OPEN | DEC-6 accepted checked decimal ops; integers are hotter. Measure, do not assume either way. |

VERDICT: OPEN — 5 clauses, 0 PROVEN, 0 REJECTED. The gate passes when every row is PROVEN with
its pin (`pins: f-y10-1-int-overflow/C-NNN`) and the owner confirms.

## 1. Out of scope

- `try_add` / `try_subtract` / `try_multiply` / `try_avg` themselves — FNP-7b builds them after
  this unit closes.
- G5b-R3-ANSI: the ANSI door's negative-interval window RANGE wrap (same G13 campaign body,
  separate unit; pin named in the Z-4 handoff).
- F-Y10-2: ANSI float `/ 0` = IEEE `+Inf` (a recorded residual with an INTENDED cross-door pin).
- Decimal arithmetic — DEC-6 closed it; nothing here reopens `decimal_spark.rs` beyond reading
  the same knob.

## 2. Sequence

1. C-001 matrix first, as a docs-only commit to this ledger — no engine edit before the matrix.
2. C-002 kernels red-first, one operator at a time; C-003 falls out or is pinned as a split.
3. C-005 measurement, C-004 docs, departure.

## 3. Owner actions

- Confirm this gate (the unit is fork-independent and can run in any window).
- No AWS, no IAM, no fork interaction anywhere in this unit.
