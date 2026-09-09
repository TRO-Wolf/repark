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
| C-001 | Line counts by crate and directory, tests split into `tests/` vs `#[cfg(test)]` | GREEN | `for d in ballista/client ballista/core ballista/executor ballista/scheduler ballista-cli benchmarks examples; do echo "== $d"; find $d -name '*.rs' \| xargs wc -l \| tail -1; done` → `ballista/core: 19233 total`, `ballista/scheduler: 30182 total`, 187 files / 76209 lines overall. `grep -rl '#\[cfg(test)\]' …` → 73 files; brace-matching counter → 20110 `#[cfg(test)]` lines (scheduler `src/` 7508, core `src/` 4084). |
| C-002 | Direct dependencies per crate (D-8: `Cargo.toml` source, no `cargo tree`) | GREEN | `sed -n '/^\[dependencies\]/,/^\[/p' <crate>/Cargo.toml` per member. Head: client deps `async-trait, ballista-core, ballista-executor (opt), ballista-scheduler (opt), datafusion, log, tokio, url`; every Ballista crate depends on `ballista-core`. |
| C-003 | Protobuf file inventory with line counts | GREEN | `find . -name '*.proto'` → 4 files: `core/proto/ballista.proto` (883), `core/proto/datafusion.proto` (1529), `core/proto/datafusion_common.proto` (687), `scheduler/proto/keda.proto` (62). Checked-in `prost` output `serde/generated/ballista.rs` (3129); `serde/mod.rs` (1280) holds both extension codecs. |
| C-004 | Extension-point inventory by grep, file and line per hit | GREEN | `grep -rn <symbol> ballista ballista-cli benchmarks examples --include='*.rs'` → `PhysicalExtensionCodec` 62 hits/10 files, `LogicalExtensionCodec` 57/9, `SessionState` 137/28, `RuntimeEnv` 64/13, `TableProvider` 21/7, `ObjectStore` 29/5. Codec defs at `ballista/core/src/serde/mod.rs:185,352`. |
| C-005 | File list per lifecycle step (submit → result) | GREEN | `wc -l` on lifecycle files → `execution_graph.rs` 3020 (largest file in tree), `shuffle_reader.rs` 2430 vs writer 828, `scheduler/planner.rs` 1934, `grpc.rs` 1292, `task_manager.rs` 1175, `executor_process.rs` 1158. |

## Notes

Step 1 writes Half A only: the document's §26 A–H + §27 headings exist, the
five appendix tables are filled, §26 prose and the §28 decision gate stay for
step 2. D-1 confirmed measured: `git log --oneline -1` → `f4e66525`,
`git describe --tags` → `54.1.0`, root `Cargo.toml` line 37
`datafusion = "54"` against this workspace's `datafusion = "54.1.0"`.

## Step-2 clause table (Half B judgement, D-2/D-3/D-4)

| Clause | Judgement item | Verdict | Evidence (document section + measured anchor) |
|---|---|---|---|
| C-006 | §26.A–C: version scope, inventory and dependency judgements | GREEN | Doc §26.A–C. Anchors: workspace `datafusion = "54.1.0"` vs upstream `datafusion = "54"`; scheduler 30182/76209 lines, `state/` 16427; `spark-compat`/`substrait` optional in member `Cargo.toml` files. |
| C-007 | §26.D: serialization chapter + ADR-0004 answer | GREEN | Doc §26.D, ADR read at `docs/adr/0004-server-prep-disciplines.md`. Anchors: codec defs `serde/mod.rs:185,352,364`; five physical node types `try_decode` 365–551; table passthrough 292–310; single-slot overrides `extension.rs` + `scheduler/config.rs:261-263` + `executor_process.rs:183-185`. |
| C-008 | §26.E: lifecycle judgement | GREEN | Doc §26.E. Anchors: `SessionProvider` hook `scheduler_server/mod.rs:65`; `shuffle_reader.rs` 2430 vs writer 828; appendix A5 file map. |
| C-009 | §26.F + appendix A6: classification, crate structure, python gap | GREEN | Doc §26.F + A6. Anchors: `wc -l python/...` → 497 lines, `dev/msrvcheck` 71; `pyballista` coupling in `python/Cargo.toml` (`datafusion-python` 54, full path deps). |
| C-010 | §26.G: risks R-1–R-7 | GREEN | Doc §26.G; each risk cites its measured anchor (single-slot override, vendored protos, 3020-line graph, passthrough, credential plumbing, §8 gap, ops stack). |
| C-011 | §26.H + §28: depend recommendation + decision gate | GREEN | Doc §26.H + §28 table; seven criteria derived from the card (plan §26–§28 text not in repo), one evidence line each. |

Step-2 notes: appendix numbers untouched (D-7 not triggered except the new
A6 row, which carries its own command). Headings unchanged per N-3.
`upstream-ballista/` never staged (excluded; moved aside for the commit only
if hooks scan it, then restored at `f4e66525`).

## Step-3 clause (orchestrator, tier O)

| Clause | Item | Verdict | Evidence |
|---|---|---|---|
| C-012 | The ADR-0004 disposition is recorded, and the unification brief points at the audit as its Milestone 0 | GREEN | `docs/adr/0004-server-prep-disciplines.md` gains a "Disposition — the Ballista audit (2026-09-08)" section: the write ban STANDS, its stated reason narrows from "cannot serialize" to "must not commit from tasks", citing doc §26.D. `rust-unification-implementation-brief-2026-09-04.md` §4 gains the Milestone 0 row. Orchestrator re-ran `make check-docs-compaction` and `make check-ledgers` green, and reproduced two step-1 numbers (187 files / 76209 lines; 4 `.proto`) and five step-2 citations (`ballista.proto` 883 lines; `serde/mod.rs` 185/352/364; scheduler 30182; `execution_graph.rs` 3020) exactly against the pinned upstream. |
