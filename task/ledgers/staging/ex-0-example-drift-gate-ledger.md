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
| C-001 | The enumerator emits a deterministic sorted list of public names with a family tag, covering the five families in the policy table, from an AST walk of the facade sources. | Inventory file + family counts in this ledger. Pin in the parity test. | **OPEN** |
| C-002 | Examples live under `docs/examples/<family>/` as runnable Python. The module docstring states what is demonstrated. Each file declares `COVERS: list[str]`. | Seed examples + gate parser. | **OPEN** |
| C-003 | The gate fails when an enumerated public name is neither in some example's `COVERS` nor in the backlog nor in the exceptions file. | Provocation: uncovered name, captured in this ledger, then reverted. | **OPEN** |
| C-004 | The gate fails when the backlog names a public name that no longer exists, or that an example now covers. | Provocation: stale backlog row; covered-but-listed row. | **OPEN** |
| C-005 | The backlog count is an exact baseline in the gate script and ratchets down only. Seed is today's uncovered set after the seed examples. | `BACKLOG_BASELINE` equals the backlog file length. Seed examples are absent from the backlog. | **OPEN** |
| C-006 | A public name whose only honest example needs a cloud service is listed in `docs/examples/exceptions.txt` with a one-line reason. Local filesystem / memory catalog examples are not exceptions. | Exceptions file + pin. | **OPEN** |
| C-007 | When `repark._native` is importable the gate executes every example script and fails on a nonzero exit. When it is not importable, execution is skipped with a visible reason so `make ci` stays native-build-free. | Dummy nonzero script in the parity test; skip path asserted. | **OPEN** |
| C-008 | Seed examples cover at least one `F.*` function, one reader/writer round trip, one DataFrame method chain, and one TA kernel, and those names are removed from the backlog in the same commit. | Seed files + backlog absence. | **OPEN** |
| C-009 | `make check-example-coverage` is wired into `make ci`. `.github/` is not edited (standing fence). | Makefile recipe + pin. | **OPEN** |
| C-010 | Examples and the gate touch only public API and local resources. No engine or `python/repark/src` product behaviour changes. | Diff scope + seed scripts. | **OPEN** |

`LOGIC_SCORE` = **0/10 `PROVEN`** — charter only; implementation slices follow.

## Family counts

Filled when the enumerator first runs; updated if the walk changes.

| Family | Count | As-of |
|---|---|---|
| functions | (pending) | |
| dataframe | (pending) | |
| ta | (pending) | |
| io | (pending) | |
| session | (pending) | |
| **total** | (pending) | |
| backlog after seeds | (pending) | |
| exceptions | (pending) | |

## Disk

Pickup: 520 GB free of 1.8 TB (71% used). No worktree. `target/` reused if a
native build is needed for live example execution; that build is optional for
the static gate.

## Dual-wire gap

House gates are dual-wired `make ci` + `ci.yml`. Standing delegated fence:
never edit `.github/`. This unit wires the Makefile only and records the
GitHub Actions mirror as follow-up (not this PR).
