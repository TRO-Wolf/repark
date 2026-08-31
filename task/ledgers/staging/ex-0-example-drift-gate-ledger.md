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
divergence registry, `.github/`, `Cargo.toml [patch]`, `briefs/next-sequence.md`,
v3/maintenance surfaces, archival bins (`completed/`, `archive/`).

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

Skip names that start with `_` and every dunder. Keep camelCase aliases
(`groupBy`, `createDataFrame`) as distinct names. Declared-absent `F.*` names
stay in the inventory (they are public). SparkSession/ReparkSession/ReParkSession
are one type; the inventory uses the Spark-door spelling `SparkSession`.

Not in this inventory (out of the v0.7 list as written): `Column` methods,
`Window`/`WindowSpec`, `Catalog`, `types`, `ml`, `Row`, `StorageLevel`,
`UDFRegistration`. A later unit can widen the walk; this unit does not.

A public name whose only honest example needs a cloud service goes in
`docs/examples/exceptions.txt` with a one-line reason. Local Iceberg (memory
catalog / local warehouse) is not a cloud exception.

## Orchestrator rulings (build-to)

Location `docs/examples/<family>/`. `COVERS: list[str]` at module level.
Backlog ratchet. Local-only execution. `make ci` runs the static half (enumerate
+ coverage + ratchet). Example execution runs when the native module imports
(same target; skip with a visible reason otherwise). `.github/` stays untouched
per the standing fence; the Makefile target is the local/CI-via-`make ci` wire.

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
| C-002 | Examples live under `docs/examples/<family>/` as runnable Python. The module docstring states what is demonstrated. Each file declares `COVERS: list[str]`. | Seed examples + gate parser. | **PROVEN** |
| C-003 | The gate fails when an enumerated public name is neither in some example's `COVERS` nor in the backlog nor in the exceptions file. | Provocation: uncovered name, captured in this ledger, then reverted. | **PROVEN** |
| C-004 | The gate fails when the backlog names a public name that no longer exists, or that an example now covers. | Provocation: stale backlog row; covered-but-listed row. | **PROVEN** |
| C-005 | The backlog count is an exact baseline in the gate script and ratchets down only. Seed is today's uncovered set after the seed examples. | `BACKLOG_BASELINE` equals the backlog file length. Seed examples are absent from the backlog. | **PROVEN** |
| C-006 | A public name whose only honest example needs a cloud service is listed in `docs/examples/exceptions.txt` with a one-line reason. Local filesystem / memory catalog examples are not exceptions. | Exceptions file + pin. | **PROVEN** |
| C-007 | When `repark._native` is importable the gate executes every example script and fails on a nonzero exit. When it is not importable, execution is skipped with a visible reason so `make ci` stays native-build-free. | Dummy nonzero script in the parity test; skip path asserted. | **PROVEN** |
| C-008 | Seed examples cover at least one `F.*` function, one reader/writer round trip, one DataFrame method chain, and one TA kernel, and those names are removed from the backlog in the same commit. | Seed files + backlog absence. | **PROVEN** |
| C-009 | `make check-example-coverage` is wired into `make ci`. `.github/` is not edited (standing fence). | Makefile recipe + pin. | **PROVEN** |
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

## Disk

Pickup: 520 GB free of 1.8 TB (71% used). No worktree. `target/` reused if a
native build is needed for live example execution; that build is optional for
the static gate.

## Dual-wire gap

House gates are dual-wired `make ci` + `ci.yml`. Standing delegated fence:
never edit `.github/`. This unit wires the Makefile only and records the
GitHub Actions mirror as follow-up (not this PR).

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
      evidence: Example child env drops AWS_* keys; examples are local-only; standing fence leaves .github/ untouched.
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

