# Unit ledger — EX-0 · v0.7 example drift gate + public-surface inventory

**Retires:** this ledger moves to `../completed/` in the unit's last commit (the
orchestrator's departure move). This file closes when EX-0 merges, or when the
owner closes the slate row.

**Unit:** EX-0 · **Date:** 2026-08-31 · **Executor:** Grok (grok-4.6), Actor ·
**Branch:** `feat/ex-0-example-drift-gate` · **Base:** `749eff4166abbbb6c590bcd4af5a9d929b1c6319`
**Ruling:** [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md)
§"v0.7 — Full example documentation" deliverable 2 (the drift gate + inventory).
Deliverable 1 (the per-name backfill) is sibling-lane work; this unit defines
"done" for that backfill.

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `scripts/check_example_coverage.py` + `.sh` wrapper; Makefile
target `check-example-coverage` in `make ci`; `docs/examples/`; this ledger;
lockstep `map.md` files; DEVELOPMENT.md pointer; python/repark-parity tests that
pin the gate. Closed: `crates/`, `python/repark/src/` product code, the
divergence registry, `Cargo.toml [patch]`, `briefs/next-sequence.md`,
v3/maintenance surfaces, archival bins (`completed/`, `archive/`).
`.github/` is writable only for the F-3 dual-wire (ci.yml static half +
wheels.yml `--require-execute`) and SEC-001 (`python -I` on that execute
leg).

## Scope

v0.7 is two deliverables. This unit ships deliverable 2 first:

1. An enumerator of the public surface (deterministic sorted names + family tag).
2. The example contract (`docs/examples/<family>/`, module docstring, `COVERS`,
   exit 0, local-only).
3. A CI drift gate that fails on uncovered names, stale backlog rows, and
   covered names still listed in the backlog. The backlog is a down-only ratchet
   seeded with today's uncovered set so CI stays green from this PR.
4. A handful of seed examples proving the contract, removed from the backlog in
   the same commit.

## Enumeration policy (binding for this unit)

Walked from source (AST) so `make ci` stays native-build-free. When
`repark._native` imports, the same script also executes every example and
cross-checks `F.__all__` / `ta.__all__` against the AST walk.

| Family | Public names | Name spelling |
|---|---|---|
| `functions` | `repark.spark.functions.__all__` | `F.<name>` |
| `dataframe` | public members of `DataFrame`, `GroupedData`, `DataFrameNaFunctions`, `DataFrameStatFunctions` | `<Class>.<name>` |
| `ta` | `repark.spark.ta.__all__` | `ta.<name>` |
| `io` | public members of `DataFrameReader`, `DataFrameWriter`, `DataFrameWriterV2` | `<Class>.<name>` |
| `session` | `repark.sql` plus public members of `ReparkSession` (spelled `SparkSession`) and nested `Builder` | `repark.sql` / `SparkSession.<name>` / `SparkSession.Builder.<name>` |

Skip names that start with `_` and every dunder (`DataFrame.__getitem__` is
excluded by the dunder rule). Keep camelCase aliases (`groupBy`,
`createDataFrame`) as distinct names. Declared-absent `F.*` names stay in the
inventory (they are public). SparkSession/ReparkSession/ReParkSession are one
type; the inventory uses the Spark-door spelling `SparkSession`.

**Closed EX-0 set (F-4, orchestrator 2026-08-31):** the roadmap colon list plus
session. Not in this inventory (measured 2026-08-31, public members unless
noted): Column **40**, Window **14**, WindowSpec **8**, Catalog **28**, Row **4**
public, `types.__all__` **28**, `ml.__all__` **28**, RuntimeConfig **5**,
SparkContext **3**, UDFRegistration **3**, StorageLevel **0** public. Widening
is an owner decision (on the order of 120+ names). This unit does not widen.

A public name whose only honest example needs a cloud service goes in
`docs/examples/exceptions.txt` with a one-line reason. Local Iceberg (memory
catalog / local warehouse) is not a cloud exception.

## Orchestrator rulings (build-to)

Location `docs/examples/<family>/`. `COVERS: list[str]` at module level and
each name must be used in that script body. Backlog ratchet. Exceptions
ratchet (`EXCEPTIONS_BASELINE`). Local-only execution. `make ci` and ci.yml's
python job run the static half. wheels.yml smoke runs `--require-execute`
against the packaged wheel (the CI leg that instantiates C-007 on the real
679-name set).

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-ex-0-charter
  agent: Actor
  action: file the EX-0 charter ledger and lockstep staging map
  charter_trace: C-001
  preconditions:
    - branch feat/ex-0-example-drift-gate at 749eff4: SATISFIED (git)
    - v0.7 ruling names the drift gate as deliverable 2: SATISFIED (roadmap)
  success_condition: staging ledger and staging/map.md are committed on the unit branch
  step_risks: [wrong bin or missing map lockstep: HANDLED(staging/ + map.md same commit)]
  contingencies: [grammar red: EXECUTABLE(fix the ledger in the next amend)]
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | The enumerator emits a deterministic sorted list of public names with a family tag, covering the five families in the policy table, from an AST walk of the facade sources. | Inventory file + family counts in this ledger. Pin in the parity test. | **PROVEN** |
| C-002 | Examples live under `docs/examples/<family>/` as runnable Python. The module docstring states what is demonstrated. Each file declares `COVERS: list[str]`. Every `COVERS` name is used in that script body (family-aware AST). | Seed examples + unused-COVERS pin; stuffing `DataFrame.agg` into `abs.py` is red. | **PROVEN** |
| C-003 | The gate fails when an enumerated public name is neither in some example's `COVERS` nor in the backlog nor in the exceptions file. | Provocation: uncovered name, captured in this ledger, then reverted. | **PROVEN** |
| C-004 | The gate fails when the backlog names a public name that no longer exists, or that an example now covers. | Provocation: stale backlog row; covered-but-listed row. | **PROVEN** |
| C-005 | The backlog count is an exact baseline in the gate script and ratchets down only. Seed is today's uncovered set after the seed examples. | `BACKLOG_BASELINE` equals the backlog file length. Seed examples are absent from the backlog. | **PROVEN** |
| C-006 | A public name whose only honest example needs a cloud service is listed in `docs/examples/exceptions.txt` with a one-line reason. Local filesystem / memory catalog examples are not exceptions. `EXCEPTIONS_BASELINE` is exact; a covered or nonexistent exception name is red. | Exceptions file + baseline pin. | **PROVEN** |
| C-007 | When `repark._native` is importable the gate executes every example script and fails on a nonzero exit. When it is not importable, execution is skipped with a visible reason so `make ci` stays native-build-free. CI execution of the real example set is `wheels.yml` smoke `--require-execute` after the packaged wheel is installed. | Dummy nonzero pin; wheels.yml step; skip path asserted. | **PROVEN** |
| C-008 | Seed examples cover at least one `F.*` function, one reader/writer round trip, one DataFrame method chain, and one TA kernel, and those names are removed from the backlog in the same commit. | Seed files + backlog absence. | **PROVEN** |
| C-009 | `make check-example-coverage` is wired into `make ci` and dual-wired into ci.yml's python job (static half). | Makefile recipe + ci.yml step + pin. | **PROVEN** |
| C-010 | Examples and the gate touch only public API and local resources. No engine or `python/repark/src` product behaviour changes. | Diff scope + seed scripts. | **PROVEN** |

`LOGIC_SCORE` = **10/10 `PROVEN`**.

## Family counts

Measured 2026-08-31 by `enumerate_public_surface` on base `749eff4` plus this unit's
docs/scripts (no product-code change). This is the size of the v0.7 backfill.

| Family | Count | As-of |
|---|---|---|
| functions | 360 | 2026-08-31 |
| dataframe | 150 | 2026-08-31 |
| ta | 86 | 2026-08-31 |
| io | 42 | 2026-08-31 |
| session | 41 | 2026-08-31 |
| **total** | **679** | 2026-08-31 |
| covered by seed examples | 19 | 2026-08-31 |
| exceptions | 2 | 2026-08-31 |
| backlog after seeds | 658 | 2026-08-31 |

## Provocation proofs (docs/testing.md "Gate provocation proofs")

Not committed. Captured 2026-08-31, then the tree restored.

1. Empty `docs/examples/backlog.txt`, `BACKLOG_BASELINE = 0`, `--skip-execute`:
   exit **1**. Head: `example-coverage: 658 finding(s)` /
   `public name DataFrame.agg has no example COVERS row and is not in the backlog or exceptions`.
2. Backlog file `NotARealPublicName` only: exit **1**.
   `backlog names NotARealPublicName, which is not in the inventory`.
3. `coverage_findings` with covered `F.abs` still in the backlog:
   `backlog still lists F.abs, which an example now covers`.

Seed plus backlog restore made the static gate green.

## Critic round 1 (2026-08-31)

| Finding | Disposition | Red-first |
|---|---|---|
| F-1 S1 COVERS stuffing | REMEDIATED: family-aware AST use check. | Stuffed `DataFrame.agg` into `abs.py` COVERS, dropped it from the backlog, `BACKLOG_BASELINE=657`: exit **1**, `docs/examples/functions/abs.py: COVERS names DataFrame.agg which the script body never uses`. Reverted from copies. |
| F-2 S1 exceptions hatch | REMEDIATED: `EXCEPTIONS_BASELINE = 2`; covered or missing names red. | Moved `DataFrame.agg` into `exceptions.txt` with reason `not today`, backlog 657, baseline 2: exit **1**, `exceptions count is 3, baseline is 2`. Reverted from copies. |
| F-3 S1 no CI execute | REMEDIATED: ci.yml python job dual-wires the static half; wheels.yml smoke `--require-execute` is the native execute leg on the real 679-name set. | Workflow files + pin. |
| F-4 overclaim | ACCEPTED_FLAGGED: closed set documented in this ledger and the gate docstring; inventory unchanged. | — |

## Critic round 2 (2026-08-31)

| Finding | Disposition | Red-first |
|---|---|---|
| L-001 S1 last-component class bind | REMEDIATED: assignment dataflow; class-surface covers need a repark-rooted local; `repark.sql` is module-alias only. | `object().agg` plus COVERS DataFrame.agg and GroupedData.agg: exit **1**, both `never uses`. Adding SparkSession.sql to sql.py COVERS: exit **1**, `COVERS names SparkSession.sql which the script body never uses`. F-1 stuffing stays unused. Reverted from copies. |
| Q-001 S2 C-002 pin bypass | REMEDIATED: `run_gate` on a scratch tree with stuffed abs.py exits 1 and prints `never uses`. | Pin `test_ex_0_run_gate_rejects_stuffed_covers`. |
| SEC-001 S2 wheel-not-source | REMEDIATED: wheels.yml `python -I`; `execution_environment` drops PYTHONPATH/PYTHONSTARTUP/PYTHONHOME. | Pin + workflows map note. |
| CL-001 S3 record staleness | REMEDIATED: writable-paths fence names the lifted `.github/` F-3/SEC-001 wires; wrapper header lists both CI legs. | — |

## Disk

Pickup: 520 GB free of 1.8 TB (71% used). No worktree. `target/` reused if a
native build is needed for live example execution; that build is optional for
the static gate.

## Dual-wire

Static half: `make check-example-coverage` and ci.yml python job
`./scripts/check_example_coverage.sh`. Execute half: wheels.yml smoke
`/tmp/wheeltest/bin/python -I scripts/check_example_coverage.py --require-execute`
after the packaged wheel is installed. Pattern-keeper for wiring claims:
`scripts/check_parity_live_dual_wire.py` (this gate is not a load-bearing-flag
comparator; it is Makefile + workflow step agreement).

```yaml
COVERAGE_ATTESTATION:
  pr_unit: EX-0
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: AST enumerator emits 679 names across five families; inventory snapshot must match.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, python/repark-parity/tests/test_ex_0_example_coverage.py]
    - id: AT-2
      status: ATTACKED
      evidence: Uncovered names, stale backlog names, covered-in-backlog names, and baseline mismatch are red.
      artifacts: [python/repark-parity/tests/test_ex_0_example_coverage.py]
    - id: AT-3
      status: ATTACKED
      evidence: Example scripts that exit nonzero are findings; missing COVERS or docstring is fail-closed.
      artifacts: [python/repark-parity/tests/test_ex_0_example_coverage.py]
    - id: AT-4
      status: N/A
      justification: Gate is a read-only process over source and scripts; no shared mutable engine state.
    - id: AT-5
      status: ATTACKED
      evidence: Example child env drops AWS_* keys; examples are local-only; exceptions are an exact baseline.
      artifacts: [scripts/check_example_coverage.py, docs/examples/exceptions.txt]
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; inventory is a walk of existing public names.
    - id: AT-7
      status: N/A
      justification: Static gate is AST-only; example execution is skipped when the native module is absent.
    - id: AT-8
      status: ATTACKED
      evidence: make ci stays native-build-free; execution is optional on import of repark._native.
      artifacts: [Makefile, python/repark-parity/tests/test_ex_0_example_coverage.py]
    - id: AT-9
      status: N/A
      justification: Findings print to stderr; no new log/metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: One pin file cites C-001 through C-010; seed examples cite C-002, C-008, C-010.
      artifacts: [python/repark-parity/tests/test_ex_0_example_coverage.py, docs/examples/functions/abs.py]
  reattested: []
  complete: true
```

