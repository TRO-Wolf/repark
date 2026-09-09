# Unit ledger — BALLISTA-AUDIT-0 steps 1–2 · Half A facts, Half B judgement

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when BALLISTA-AUDIT-0 merges.

**Unit:** BALLISTA-AUDIT-0 steps 1–2 · **Date:** 2026-09-08 · **Model:** muse-spark-1.3-contributor · **Branch:** `docs/ballista-audit-0`
**Document:** [../../roadmap/epic-term/ballista-audit-2026-09-08.md](../../roadmap/epic-term/ballista-audit-2026-09-08.md)
**Upstream:** Apache DataFusion Ballista tag `54.1.0`, commit `f4e66525`, at `upstream-ballista/` (git-excluded, never committed).

"Red first" does not apply to this docs-only reading unit: there is no pin
against base-tree behaviour, only measured numbers and read judgements from
the pinned upstream checkout. Each clause below is OPEN until its document
section lands, then green. Step-2 work below; **Model:** muse-spark-1.3-contributor.

## Clause table

| Clause | Inventory item | Verdict | Evidence (command + head of output) |
|---|---|---|---|
| C-001 | Line counts by crate and directory, tests split into `tests/` vs `#[cfg(test)]` | OPEN | `for d in ballista/client ballista/core ballista/executor ballista/scheduler ballista-cli benchmarks examples; do echo "== $d"; find $d -name '*.rs' \| xargs wc -l \| tail -1; done` → `ballista/core: 19233 total`, `ballista/scheduler: 30182 total`, 187 files / 76209 lines overall. `grep -rl '#\[cfg(test)\]' …` → 73 files; brace-matching counter → 20110 `#[cfg(test)]` lines (scheduler `src/` 7508, core `src/` 4084). |
| C-002 | Direct dependencies per crate (D-8: `Cargo.toml` source, no `cargo tree`) | OPEN | `sed -n '/^\[dependencies\]/,/^\[/p' <crate>/Cargo.toml` per member. Head: client deps `async-trait, ballista-core, ballista-executor (opt), ballista-scheduler (opt), datafusion, log, tokio, url`; every Ballista crate depends on `ballista-core`. |
| C-003 | Protobuf file inventory with line counts | OPEN | `find . -name '*.proto'` → 4 files: `core/proto/ballista.proto` (883), `core/proto/datafusion.proto` (1529), `core/proto/datafusion_common.proto` (687), `scheduler/proto/keda.proto` (62). Checked-in `prost` output `serde/generated/ballista.rs` (3129); `serde/mod.rs` (1280) holds both extension codecs. |
| C-004 | Extension-point inventory by grep, file and line per hit | OPEN | `grep -rn <symbol> ballista ballista-cli benchmarks examples --include='*.rs'` → `PhysicalExtensionCodec` 62 hits/10 files, `LogicalExtensionCodec` 57/9, `SessionState` 137/28, `RuntimeEnv` 64/13, `TableProvider` 21/7, `ObjectStore` 29/5. Codec defs at `ballista/core/src/serde/mod.rs:185,352`. |
| C-005 | File list per lifecycle step (submit → result) | OPEN | `wc -l` on lifecycle files → `execution_graph.rs` 3020 (largest file in tree), `shuffle_reader.rs` 2430 vs writer 828, `scheduler/planner.rs` 1934, `grpc.rs` 1292, `task_manager.rs` 1175, `executor_process.rs` 1158. |

## Notes

Step 1 writes Half A only: the document's §26 A–H + §27 headings exist, the
five appendix tables are filled, §26 prose and the §28 decision gate stay for
step 2. D-1 confirmed measured: `git log --oneline -1` → `f4e66525`,
`git describe --tags` → `54.1.0`, root `Cargo.toml` line 37
`datafusion = "54"` against this workspace's `datafusion = "54.1.0"`.

## Step-2 clause table (Half B judgement, D-2/D-3/D-4)

| Clause | Judgement item | Verdict | Evidence (document section + measured anchor) |
|---|---|---|---|
| C-006 | §26.A–C: version scope, inventory and dependency judgements | OPEN | Doc §26.A–C. Anchors: workspace `datafusion = "54.1.0"` vs upstream `datafusion = "54"`; scheduler 30182/76209 lines, `state/` 16427; `spark-compat`/`substrait` optional in member `Cargo.toml` files. |
| C-007 | §26.D: serialization chapter + ADR-0004 answer | OPEN | Doc §26.D, ADR read at `docs/adr/0004-server-prep-disciplines.md`. Anchors: codec defs `serde/mod.rs:185,352,364`; five physical node types `try_decode` 365–551; table passthrough 292–310; single-slot overrides `extension.rs` + `scheduler/config.rs:261-263` + `executor_process.rs:183-185`. |
| C-008 | §26.E: lifecycle judgement | OPEN | Doc §26.E. Anchors: `SessionProvider` hook `scheduler_server/mod.rs:65`; `shuffle_reader.rs` 2430 vs writer 828; appendix A5 file map. |
| C-009 | §26.F + appendix A6: classification, crate structure, python gap | OPEN | Doc §26.F + A6. Anchors: `wc -l python/...` → 497 lines, `dev/msrvcheck` 71; `pyballista` coupling in `python/Cargo.toml` (`datafusion-python` 54, full path deps). |
| C-010 | §26.G: risks R-1–R-7 | OPEN | Doc §26.G; each risk cites its measured anchor (single-slot override, vendored protos, 3020-line graph, passthrough, credential plumbing, §8 gap, ops stack). |
| C-011 | §26.H + §28: depend recommendation + decision gate | OPEN | Doc §26.H + §28 table; seven criteria derived from the card (plan §26–§28 text not in repo), one evidence line each. |

Step-2 notes: appendix numbers untouched (D-7 not triggered except the new
A6 row, which carries its own command). Headings unchanged per N-3.
`upstream-ballista/` never staged (excluded; moved aside for the commit only
if hooks scan it, then restored at `f4e66525`).

## Step-3 clause (orchestrator, tier O)

| Clause | Item | Verdict | Evidence |
|---|---|---|---|
| C-012 | The ADR-0004 disposition is recorded, and the unification brief points at the audit as its Milestone 0 | OPEN | `docs/adr/0004-server-prep-disciplines.md` gains a "Disposition — the Ballista audit (2026-09-08)" section: the write ban STANDS, its stated reason narrows from "cannot serialize" to "must not commit from tasks", citing doc §26.D. `rust-unification-implementation-brief-2026-09-04.md` §4 gains the Milestone 0 row. Orchestrator re-ran `make check-docs-compaction` and `make check-ledgers` green, and reproduced two step-1 numbers (187 files / 76209 lines; 4 `.proto`) and five step-2 citations (`ballista.proto` 883 lines; `serde/mod.rs` 185/352/364; scheduler 30182; `execution_graph.rs` 3020) exactly against the pinned upstream. |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ballista-audit-0
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each card clause was walked against the document itself, not a paraphrase. The orchestrator reproduced two Half A numbers exactly (187 files / 76209 lines; 4 .proto files) and five Half B citations exactly (ballista.proto 883 lines; serde/mod.rs lines 185, 352, 364; scheduler 30182; execution_graph.rs 3020) against the pinned upstream checkout at f4e66525.
      artifacts: [task/roadmap/epic-term/ballista-audit-2026-09-08.md, docs/adr/0004-server-prep-disciplines.md]
    - id: AT-2
      status: ATTACKED
      evidence: The measurement boundary was attacked and found short — the seven enumerated directories omitted five upstream .rs files (python/ binding, dev/msrvcheck). Appendix A6 closes that gap with its own command, and the python/ binding is now covered in §26 because it bears on the Python-API requirement.
      artifacts: [task/roadmap/epic-term/ballista-audit-2026-09-08.md]
    - id: AT-3
      status: N/A
      justification: A reading unit. No code, no execution path, no failure mode is introduced; the deliverable is one document plus a disposition paragraph in an existing ADR.
    - id: AT-4
      status: N/A
      justification: No state, no ordering, no concurrency is introduced. The only shared artifact is the git-excluded upstream scratch checkout, verified byte-identical at f4e66525 after each worker moved it aside for a commit, and now moved out of the clone entirely.
    - id: AT-5
      status: N/A
      justification: No privileged action, no secret, no deserialization, no path handling. Nothing from the audited upstream is vendored; the scratch checkout never entered a commit.
    - id: AT-6
      status: ATTACKED
      evidence: The compatibility question this unit exists to answer — whether Ballista's protobuf plan serialization can carry RePark's Iceberg write/commit nodes — was attacked at the source and the ADR's stated premise was found imprecise. The codec CAN carry custom nodes through a single-slot override; the write ban stands on the commit-coordinator ground instead. Recorded as a disposition in the ADR rather than a silent correction.
      artifacts: [docs/adr/0004-server-prep-disciplines.md, task/roadmap/epic-term/ballista-audit-2026-09-08.md]
    - id: AT-7
      status: N/A
      justification: No execution path is added, so there is no resource or performance behaviour to break. The audit's own cost numbers (about 59k source plus 20k test lines to import) are inputs to the depend-versus-import recommendation, not a performance claim about this repository.
    - id: AT-8
      status: ATTACKED
      evidence: The upstream contract is the whole subject. D-1's version pair was verified by the orchestrator, not the worker: tag 54.1.0, commit f4e66525, upstream root Cargo.toml `datafusion = "54"` against this workspace's `datafusion = "54.1.0"`. Every extension-point claim cites file and line in the pinned tree.
      artifacts: [task/roadmap/epic-term/ballista-audit-2026-09-08.md]
    - id: AT-9
      status: N/A
      justification: No failure path exists to diagnose. The document's own reproducibility is the operability property, and it is held by D-7: every number carries the command that produced it.
    - id: AT-10
      status: N/A
      justification: A documentation unit with no code to pin. The gates that do apply were re-run by the orchestrator, not taken on the workers' word: check-docs-compaction and check-ledgers green on the step-1, step-2 and departure trees, and make preflight before the PR.
  complete: true
```

## Why every clause reads OPEN, not PROVEN (orchestrator, 2026-09-08)

Each clause below is discharged: the evidence cells hold the commands and the measured output, and the orchestrator independently reproduced two Half A numbers and five Half B citations exactly against the pinned upstream. They stay OPEN because of the contract, not because of doubt. [docs/testing.md](../../../docs/testing.md) "Pinning a charter clause" defines PROVEN as "cited by at least one test", and `scripts/check_ledger_grammar.py` enforces it by reading `pins: <unit>/C-NNN` citations from tracked files under `crates/`, `python/` and `scripts/`. BALLISTA-AUDIT-0 is a reading unit: it adds no code and therefore no test that could carry a citation, so no clause here can honestly be PROVEN under that definition. The ledger stays in `staging/` for the same reason.

**Parked for the owner.** The gate has no vocabulary for a unit whose propositions are measurements rather than behaviours. Two candidate rulings: give reading units an `EXCEPTIONS` row in the gate (ceiling equal to their clause count, attestation required), or rule that a reading unit files its evidence without a clause table at all. Either is a change to a ratchet-down-only gate, so it is the owner's, not the orchestrator's.
