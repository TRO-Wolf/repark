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

**Batch 1 roster as dispatched (12 names, measured against
`docs/examples/backlog.txt` at `dc74a40`, where all twelve are rows):**

`F.sqrt`, `F.cbrt`, `F.exp`, `F.expm1`, `F.hypot`, `F.pow`, `F.power`,
`F.sign`, `F.signum`, `F.negative`, `F.positive`, `F.rint`.

**As landed: eleven.** `F.expm1` is dropped and stays on the backlog — see
"Batch 1 outcome" below for the measurement and the reason.

**Grouping.** Four files, grouped by the idea a reader learns in one breath
rather than one file per name:

| File | `COVERS` (batch names) | Why these together |
|---|---|---|
| `roots.py` | `F.sqrt`, `F.cbrt`, `F.hypot` | Roots, and where they part company: a square root of a negative is NaN, a cube root is signed. `F.hypot` is the name a reader reaches for next — the same length as `sqrt(a*a + b*b)`, spelled as the geometry reads — and the example asserts the two agree, which is what makes the third name belong here. |
| `powers.py` | `F.pow`, `F.power`, `F.exp` | `pow`/`power` are an alias pair, demonstrated as one — they are *not* the same object (`F.pow is F.power` is **False**, measured), so the alias relation is shown as identical output on identical input rather than as identity. `F.exp` is the same idea with the base fixed, checked against `pow(e, x)` to a float tolerance. |
| `sign.py` | `F.sign`, `F.signum`, `F.negative`, `F.positive` | Sign and unary negation are one idea: `signum` reports the sign (always a float, even on an integer column), `negative` applies it, `positive` is the identity that completes the pair. `sign`/`signum` are an alias pair — again two distinct callables, shown agreeing row for row. |
| `rint.py` | `F.rint` | Its own file: `rint` rounds ties to the **even** neighbour, and that single edge case is the whole reason the name exists. |

No existing example under `docs/examples/functions/` demonstrates any of the
twelve — `abs.py` is the only file there, and it exercises `F.abs`, `F.col`,
`F.lit` only. So no name joins an existing `COVERS`. The four new files list
`F.col` (and `powers.py` also `F.lit`) in `COVERS` because they genuinely use
them; both are already covered by `abs.py`, so neither moves the ratchet.

## Orchestrator rulings (build-to)

- The gate is the acceptance bar in both directions: a `COVERS` entry the script
  does not exercise is the defect the campaign will not tolerate, and every
  script runs green locally with no network, no cloud and no JVM.
- The backlog count moves down by exactly the names this batch covers, and
  `BACKLOG_BASELINE` moves with it — measured at 892 → 881, eleven of the twelve dispatched.
- No product edit, ever. A name whose example exposes an engine defect is
  reported and dropped back to the backlog; the baseline then moves by the names
  actually removed.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-ex-2-batch-1
  agent: Actor
  action: charter the F.* math + bitwise family and land batch 1 (12 dispatched, 11 landed)
  charter_trace: C-001
  preconditions:
    - branch feat/ex-2-functions-math-bitwise at dc74a40: SATISFIED (git)
    - all twelve names are backlog rows, none covered, none excepted: SATISFIED (grep)
    - the EX-0 gate is in `make ci`: SATISFIED (Makefile ci target)
  success_condition: every name the batch can teach honestly leaves the backlog, the ratchet moves by exactly that count, the gate's execute leg exits 0
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
| C-001 | Batch 1 lands runnable local examples for the eleven roster names it can demonstrate honestly, in four files under `docs/examples/functions/`, every `COVERS` entry exercised by an assertion on the value that name produces; those eleven leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly eleven, 892 → 881, with no other `scripts/` change; the twelfth, `F.expm1`, stays a backlog row with its divergence measured and reported, and no product file is touched; the gate's static half and its `--require-execute` leg both exit 0. | Red-first capture below (the twelve are uncovered before the batch, and the gate reds by name when the rows are removed without examples), the `expm1` measurement table, the green counts line, and the recorded gate exit codes. | **OPEN** |
| C-002 | Batch 2 lands runnable local examples for the 37 roster names the live oracle confirms, in six files under `docs/examples/functions/`, every asserted value measured against live PySpark 4.1.2 before it was written and every `COVERS` entry exercised by an assertion on that measured value; those 37 leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 37, 881 → 844, with no other `scripts/` change; the 38th, `F.log1p`, stays a backlog row with its divergence measured and reported, and no product file is touched; the gate's static half and its executing `--require-execute` leg both exit 0. | Batch 2 evidence section: red-first capture (37 named findings), the Critic's oracle verification, the divergence table, the counts line, the gate exit codes. | **PROVEN** |

`LOGIC_SCORE` = **0/2 `PROVEN`** — the clauses stay `OPEN` until the family lands. The
worker's green is directional by the campaign contract; the orchestrator's independent
re-run from a clean checkout is what closes them, and the pins a `PROVEN` verdict owes
live in `python/repark-parity/tests/`, which this unit may not write.

## Red-first (docs/testing.md "Gate provocation proofs")

Captured on the branch at `826b5b4`, before any example file existed. The twelve
rows were deleted from `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moved
892 → 880 with nothing else changed. `./scripts/check_example_coverage.sh` then
exited **1** with exactly twelve findings, one per roster name and no others:

```
example-coverage: 12 finding(s)
  public name F.cbrt has no example COVERS row and is not in the backlog or exceptions
  public name F.exp has no example COVERS row and is not in the backlog or exceptions
  public name F.expm1 has no example COVERS row and is not in the backlog or exceptions
  public name F.hypot has no example COVERS row and is not in the backlog or exceptions
  public name F.negative has no example COVERS row and is not in the backlog or exceptions
  public name F.positive has no example COVERS row and is not in the backlog or exceptions
  public name F.pow has no example COVERS row and is not in the backlog or exceptions
  public name F.power has no example COVERS row and is not in the backlog or exceptions
  public name F.rint has no example COVERS row and is not in the backlog or exceptions
  public name F.sign has no example COVERS row and is not in the backlog or exceptions
  public name F.signum has no example COVERS row and is not in the backlog or exceptions
  public name F.sqrt has no example COVERS row and is not in the backlog or exceptions
```

That is the red the batch closes: the gate names each of the twelve, and it
names nothing else, so the batch's green cannot be borrowed from another name.

**Verdict grammar, measured rather than assumed.** `C-001` is `OPEN`, not
`PROVEN`, and the reason is a gate result, not a preference. With the clause
marked `PROVEN`, `scripts/check_ledger_grammar.py` exits with two findings —
`1 PROVEN clause(s) with no pins: ex-2-functions-math-bitwise/C-NNN citation
(ceiling 0)` and `no COVERAGE_ATTESTATION block`. The citation must live under
`crates/`, `python/` or `scripts/`, all of which this unit's writable-path list
closes. The clause therefore stays `OPEN` until the orchestrator's independent
re-run and the family's pin file close it.

## Batch 1 outcome — eleven names, not twelve

`F.expm1` is **dropped from the batch and left on the backlog** under the
campaign rule that a worker never edits product code. Measured, on this tree,
against `math.expm1` at three magnitudes:

| `x` | `F.expm1(x)` | `math.expm1(x)` | naive `exp(x) - 1` |
|---|---|---|---|
| `1e-13` | `9.992007221626409e-14` | `1.00000000000005e-13` | `9.992007221626409e-14` |
| `1e-08` | `9.99999993922529e-09` | `1.0000000050000001e-08` | `9.99999993922529e-09` |
| `1e-16` | `0.0` | `1e-16` | `0.0` |

`F.expm1` reproduces the naive form bit for bit, which is the one thing the name
exists not to do: `expm1` is in the API because `exp(x) - 1` cancels away its
significant digits near zero. The behaviour is consistent with the design
record — `expm1` is listed under `PY_COMPOSED` in
[docs/design/spark-function-parity.md](../../../docs/design/spark-function-parity.md)
§4.4, i.e. composed from existing expressions rather than given a kernel — so
this is a recorded implementation choice with an unrecorded consequence, not a
surprise regression. Spark's `expm1` is `java.lang.Math.expm1`, which is
accurate here. An example could still print `exp(x) - 1` and pass the gate, but
it would teach the name by demonstrating everything except its reason for
existing, which the campaign contract calls a review rejection. The name stays
on the backlog for the owner to rule on.

**Also observed, not dropped:** `F.hypot(1e200, 1e200)` returns `inf`, where
`java.lang.Math.hypot` (Spark's implementation) returns `1.414…e200` — the
overflow-avoidance that distinguishes `hypot` from the long form is absent.
Unlike `expm1`, `hypot`'s ordinary behaviour is exactly right and is what an
example teaches, so the name stays in the batch and `roots.py` claims only what
holds: `hypot` equals `sqrt(a*a + b*b)` on ordinary input. The overflow corner is
reported rather than demonstrated.

The roster is therefore eleven names and the ratchet moves **892 → 881**.

## Gates (2026-09-01, on the batch tree)

| Command | Exit |
|---|---|
| `./scripts/check_example_coverage.sh` (static half) | **0** |
| `.venv/bin/python -I scripts/check_example_coverage.py --require-execute` | **0** |
| `python docs/examples/functions/{roots,powers,sign,rint}.py`, each | **0** |
| `make ci` | **0** |
| `make check-ledger-grammar` | **0** |
| `make py-test` | **2** — see below; the failure is a hardcoded EX-1 snapshot, not this batch |

Counts line, both legs identical (the execute leg imports the native module, so
no skip line and every example is run):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 30 covered; 881 backlog; 2 exceptions; 9 examples`

19 → 30 covered is the eleven names; 892 → 881 backlog is the same eleven; 5 → 9
examples is the four new files.

## Pointers

- Up: [map.md](map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Sibling: [ex-0-example-drift-gate-ledger.md](ex-0-example-drift-gate-ledger.md),
  [ex-1-class-surfaces-ledger.md](ex-1-class-surfaces-ledger.md)

## Blocker (RESOLVED by orchestrator ruling, 2026-09-01) — the EX-1 pins hardcode the pre-batch backlog count

`make py-test` exits **2** on this tree, with two failures and 494 passes:

```
FAILED python/repark-parity/tests/test_ex_0_example_coverage.py::test_ex_0_seed_examples_declare_covers_and_leave_the_backlog
FAILED python/repark-parity/tests/test_ex_0_example_coverage.py::test_ex_1_every_new_name_is_in_the_backlog
```

Both assert the literal `892` — `assert gate.BACKLOG_BASELINE == 892` and
`assert len(backlog) == 892` — which is EX-1's snapshot of a count the campaign
exists to drive to zero. Every backfill batch reds them, not just this one, and a
batch that lands fewer names than dispatched reds them at a different number, so
the two assertions cannot be satisfied by any correct batch.

The fix is one file and two lines, and it is **outside this unit's writable
paths**: `python/repark-parity/tests/` is not a docs path, and the campaign's
standing rules close it. It is an orchestrator decision whether the two
assertions become `== gate.BACKLOG_BASELINE` (the ratchet is already pinned
exactly one line above the first of them, so the literal adds nothing but a
per-batch edit) or `<= 892` (a ratchet-direction pin, which is what EX-1's
clause C-006 actually claims). This ledger states the collision and stops; the
batch itself is unaffected — `make ci`, both gate legs, and all four example
scripts are green.

**Resolution (orchestrator, 2026-09-01):** the two pins loosen to the campaign-true invariants —
`gate.BACKLOG_BASELINE <= 892` (a down-only direction pin, which is what EX-1 C-006 claims) and
`len(backlog) == gate.BACKLOG_BASELINE` (the lockstep the adjacent pin already asserted). The
exact per-batch count lives in each family ledger clause, where it belongs.

## Batch 2 outcome — thirty-seven names, not thirty-eight

`F.log1p` is **dropped from the batch and left on the backlog**: measured 2026-09-02 on live
PySpark 4.1.2 + Iceberg 1.11.0 against the engine on the same inputs.

| Input | Spark `log1p` | repark `F.log1p` | Verdict |
|---|---|---|---|
| `0.0` | `0.0` | `0.0` | equal |
| `1e-10` | `9.999999999500001e-11` | `1.000000082690371e-10` | **diverges** — the engine computes `ln(1 + x)` and loses the precision `log1p` exists to keep |
| `1e-13` | `9.9999999999995e-14` | `9.992007221625909e-14` | **diverges** (same cause) |
| `-1.0` | `NULL` | `NULL` | equal |
| `-2.0` | `NULL` | `NULL` | equal |
| `NULL` | `NULL` | `NULL` | equal |

Product finding, reported here for the owner to file (accuracy, not a wrong-answer class);
the example lands when the kernel is fixed. Batch 2 wall-clock on the mechanical tier
(GLM 5.3 Flash): 2 h 1 min to a complete tree, then a stall at the commit step; the
orchestrator committed the tree after re-running every gate.

### Batch 2 evidence (recorded by the orchestrator after the worker stalled at the commit step)

**Red-first capture** (scratch worktree at `73cdfa4` with the six batch-2 files removed and the
backlog rows already gone): `check_example_coverage.py` exits 1 with **37 findings**, one per
roster name and no others — `F.acos F.acosh F.asin F.asinh F.atan F.atan2 F.atanh F.ceil
F.ceiling F.cos F.cosh F.cot F.csc F.degrees F.e F.factorial F.floor F.greatest F.least F.ln
F.log F.log10 F.log2 F.pi F.pmod F.radians F.round F.sec F.sin F.sinh F.tan F.tanh F.try_add
F.try_divide F.try_multiply F.try_subtract F.width_bucket`. With the files present the gate is
green.

**Oracle verification** (independent Critic, 2026-09-02, live PySpark 4.1.2 + Iceberg 1.11.0):
every value each file asserts was dumped from the oracle on the file's own inputs and matched
by repr; then each of the six files was re-run **unmodified** with `repark.functions` swapped
for `pyspark.sql.functions` on the live session — all six exited 0, so every assertion holds
on Spark itself. Edges confirmed on Spark: `asin(2)`/`acos(2)` NaN, `cot(0)`/`csc(0)`
Infinity, `acosh(0.5)` NaN, `atanh(±1)` ±Infinity, `round` half-up away from zero on both
signs, `pmod(-7,3) = 2`, `width_bucket` max one past the last bucket, `try_divide` by zero
NULL, `factorial(21)` untaught (input set stops at 10).

| Gate | Exit |
|---|---|
| `.venv/bin/python scripts/check_example_coverage.py` | 0 |
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | 0 |
| the six example scripts, each, via `.venv/bin/python` | 0 |
| `uv run --no-sync ruff check docs/examples` / `ruff format --check docs/examples` | 0 / 0 |
| `make check-map-sync` / `make check-ledger-grammar` | 0 / 0 |

Counts line: `913 public names …; 67 covered; 844 backlog; 2 exceptions; 15 examples`
(was `30 covered; 881 backlog; 9 examples` before batch 2).

**Throughput record (the pilot this batch was):** mechanical tier (GLM 5.3 Flash), 38-name
roster, 70 steps, $0.16, 2 h 01 min to a complete tree, then a silent stall before the commit;
one Critic pass (Opus) found no value divergence and two ledger-record gaps, closed here.

