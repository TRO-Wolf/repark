# Unit ledger — EX-2 · v0.7 example backfill, `F.*` math + bitwise

**Retires:** this ledger moves to `../completed/` in the family's last commit
(the orchestrator's departure move). It closes when the `F.*` math + bitwise
family PR merges, or when the owner closes the slate row.

**Unit:** EX-2 · **Date:** 2026-09-01 · **Model:** opus-5 (1M context), Actor ·
**Branch:** `feat/ex-2-functions-math-bitwise` · **Base:** `dc74a40`
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md),
batch roster row 1 (the campaign pilot). **Ruling:** owner, 2026-08-31,
[release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md)
§"v0.7 — Full example documentation", and the 2026-08-31 ruling that each family
PR carries its own charter ledger with one clause per batch.

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/functions/`, `docs/examples/backlog.txt`, the
`BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`, lockstep
`map.md` files, and this ledger with its `staging/map.md` row. Closed:
`crates/`, `python/repark/src/`, every other `scripts/` line, `.github/`,
`STATUS.md`, every other ledger, `briefs/next-sequence.md`.

## Scope

The family is the `F.*` math and bitwise names the campaign left on the backlog.
This unit is its charter and its first batch; later batches land as further
clauses on this ledger, on this branch, under the same PR.

**Batch 1 roster (12 names, measured against `docs/examples/backlog.txt` at
`dc74a40`, where all twelve are rows):**

`F.sqrt`, `F.cbrt`, `F.exp`, `F.expm1`, `F.hypot`, `F.pow`, `F.power`,
`F.sign`, `F.signum`, `F.negative`, `F.positive`, `F.rint`.

**Grouping.** Five files, grouped by the idea a reader learns in one breath
rather than one file per name:

| File | `COVERS` (batch names) | Why these together |
|---|---|---|
| `roots.py` | `F.sqrt`, `F.cbrt`, `F.hypot` | Roots, and the Pythagorean length `F.hypot` computes without the intermediate overflow `F.sqrt(a*a + b*b)` invites. The example asserts the two agree on ordinary inputs, which is what makes the third name belong here. |
| `exponentials.py` | `F.exp`, `F.expm1` | `expm1` exists only because `exp(x) - 1` loses its significant digits near zero. The example measures that loss, so the pair is one lesson. |
| `power.py` | `F.pow`, `F.power` | Alias pair. The example demonstrates the alias relation explicitly — same inputs, identical column, and `F.pow is F.power` checked at the door. |
| `sign.py` | `F.sign`, `F.signum`, `F.negative`, `F.positive` | Sign and unary negation are one idea: `signum` reports the sign, `negative` applies it, `positive` is the identity that completes the pair. `sign`/`signum` are an alias pair, demonstrated as one. |
| `rint.py` | `F.rint` | Its own file: `rint` rounds half to **even**, unlike `F.round`, and that single edge case is the whole reason the name exists. |

No existing example under `docs/examples/functions/` demonstrates any of the
twelve — `abs.py` is the only file there, and it exercises `F.abs`, `F.col`,
`F.lit` only. So no name joins an existing `COVERS`.

## Orchestrator rulings (build-to)

- The gate is the acceptance bar in both directions: a `COVERS` entry the script
  does not exercise is the defect the campaign will not tolerate, and every
  script runs green locally with no network, no cloud and no JVM.
- The backlog count moves down by exactly the names this batch covers, and
  `BACKLOG_BASELINE` moves with it: 892 → 880.
- No product edit, ever. A name whose example exposes an engine defect is
  reported and dropped back to the backlog; the baseline then moves by the names
  actually removed.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-ex-2-batch-1
  agent: Actor
  action: charter the F.* math + bitwise family and land batch 1 (12 names)
  charter_trace: C-001
  preconditions:
    - branch feat/ex-2-functions-math-bitwise at dc74a40: SATISFIED (git)
    - all twelve names are backlog rows, none covered, none excepted: SATISFIED (grep)
    - the EX-0 gate is in `make ci`: SATISFIED (Makefile ci target)
  success_condition: the twelve names leave the backlog, the gate's execute leg exits 0
  step_risks:
    - a COVERS entry the script does not really exercise: HANDLED(each script asserts on the value the name produces)
    - a name grouped into a file where it is decoration: HANDLED(grouping table states why each name is in its file)
    - the leaf-conflation hazard in docs/examples/map.md: HANDLED(every batch name is an F.* door name, split from the class surfaces by door kind)
  contingencies: [example exposes a product defect: EXECUTABLE(report it, drop the name, move the baseline by the names actually removed)]
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Batch 1 lands runnable local examples for exactly the twelve roster names above, in five files under `docs/examples/functions/`, every `COVERS` entry exercised by an assertion on the value that name produces; the twelve leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves 892 → 880 with no other `scripts/` change; the gate's static half and its `--require-execute` leg both exit 0. | Red-first capture below (the twelve are uncovered before the batch, and the gate reds by name when the rows are removed without examples), the green counts line, and the recorded gate exit codes. | **OPEN** |

`LOGIC_SCORE` = **0/1 `PROVEN`** — the batch's clause stays `OPEN` until the
family lands. The worker's green is directional by the campaign contract; the
orchestrator's independent re-run from a clean checkout is what closes it, and
the pin that a `PROVEN` verdict owes lives in `python/repark-parity/tests/`,
which this unit may not write.

## Red-first (docs/testing.md "Gate provocation proofs")

Captured on the branch, before any example was written.

_(filled in below by the batch commit)_

## Gates

_(filled in below by the batch commit)_

## Pointers

- Up: [map.md](map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Sibling: [ex-0-example-drift-gate-ledger.md](ex-0-example-drift-gate-ledger.md),
  [ex-1-class-surfaces-ledger.md](ex-1-class-surfaces-ledger.md)
