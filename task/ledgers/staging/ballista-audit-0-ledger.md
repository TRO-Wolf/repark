# Unit ledger — BALLISTA-AUDIT-0 step 1 · Half A facts

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when BALLISTA-AUDIT-0 merges.

**Unit:** BALLISTA-AUDIT-0 step 1 · **Date:** 2026-09-08 · **Model:** muse-spark-1.3-contributor · **Branch:** `docs/ballista-audit-0`
**Document:** [../../roadmap/epic-term/ballista-audit-2026-09-08.md](../../roadmap/epic-term/ballista-audit-2026-09-08.md)
**Upstream:** Apache DataFusion Ballista tag `54.1.0`, commit `f4e66525`, at `upstream-ballista/` (git-excluded, never committed).

"Red first" does not apply to this docs-only measurement step: there is no pin
against base-tree behaviour, only measured numbers from the pinned upstream
checkout. Each clause below is OPEN until its appendix table lands, then green.

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
