# Unit ledger — REVIEW-FIX-13 · the Ballista audit is trued up

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when REVIEW-FIX-13 merges, or when the owner closes the slate row.

**Unit:** REVIEW-FIX-13 · **Date:** 2026-09-11 · **Executor:** Muse Spark (muse-spark-1.3-contributor), Actor ·
**Branch:** `fix/review-fix-13-15` · **Base:** `cadf2382`
**Model:** muse-spark-1.3-contributor
**Card:** card REVIEW-FIX-13 in `task/roadmap/mid-term/review-1-findings-2026-09-10.md` (Q-42, Q-43, Q-44, Q-45, Q-53, Q-54)
**Path:** READING
**Document:** [../../roadmap/epic-term/ballista-audit-2026-09-08.md](../../roadmap/epic-term/ballista-audit-2026-09-08.md)
**Upstream:** Apache DataFusion Ballista tag `54.1.0`, commit `f4e66525`, measured at a scratch clone outside the repo (never committed).

Red first on a measurement card means the base document fails reproduction:
each clause below names the command, shows the base figure failing against
tag `54.1.0` at `f4e66525` (red), then the corrected document passing the
same command (green). No test pins: a READING unit proves on document
evidence under R-10 (2026-09-09), as BALLISTA-AUDIT-0 did.

## Proposition ledger

| ID | Clause | Evidence | Verdict |
|---|---|---|---|
| C-001 | D-1 §26.B/§26.E line arithmetic is re-measured: `state/aqe`, `cluster`, `physical_optimizer` counted, and `state/` 16427 no longer attributed to the A5 stage-graph set, with the producing command beside each number. | Red: base §26.B reads the whole 16427 as stage-graph/stage/task-manager; `find ballista/scheduler/src/state -maxdepth 1 -name '*.rs' \| xargs wc -l \| tail -1` gives 7491 and `find ballista/scheduler/src/state/aqe -name '*.rs' \| xargs wc -l \| tail -1` gives 8936, so 8936 aqe lines were misattributed. Green: §26.B now splits 7491 + 8936, names the six A5 stage-graph files as 6101 of the 7491, and calls `state/aqe` the largest scheduler subtree; `cluster` 2069, `physical_optimizer` 1845, `scheduler_server` 3105, `api` 1354 all re-measured unchanged. In passing the same commands corrected three more §26.E sums: executor run 5008 (`~5000`, was `~4700`), shuffle write 3358 across nine files (was eight), largest hand-written file `execution_graph.rs` 3020 (generated `ballista.rs` is 3129). | **PROVEN** |
| C-002 | D-2 The per-session seat is named `SessionBuilder` with its signature read from source. | Red: base names `SessionProvider` five times (§26.E, §26.F twice, §26.H, appendix A4); `grep -rn SessionProvider ballista/` at the tag returns nothing, the name does not exist upstream. Green: all five sites read `SessionBuilder`; §26.E carries the signature `Arc<dyn Fn(SessionConfig) -> datafusion::common::Result<SessionState> + Send + Sync>` from `scheduler_server/mod.rs:64-65`, re-exported at `lib.rs:49`. | **PROVEN** |
| C-003 | D-3 R-7 distinguishes default from optional features: prometheus, graphviz, KEDA optional (Q-54); REST/axum not gateable and scheduler defaults pull AWS (Q-43). | Red: base R-7 lists `axum`, `prometheus`, `graphviz-rust`, KEDA flat. Green: §26.C second paragraph, R-7, the §26.F DROP reason, and a new appendix A2 block state the measured split with the feature table (scheduler `Cargo.toml` lines 36-37, 41-46, 51-52; core `build-binary` line 39) plus `cargo tree` in a scratch crate pinning `ballista-scheduler = "=54.1.0"`: depth 1 shows `axum` + `tower-http` unconditional (`use axum` ungated at `scheduler_server/grpc.rs:18`); `-i aws-config` resolves under `ballista-core` under defaults; `-i prometheus` and `-i graphviz-rust` fail `did not match any packages`; `-i tonic-prost` resolves via `arrow-flight`, so the KEDA feature gates only the scaler module. | **PROVEN** |
| C-004 | D-4 The A1 reproduction command is real; the unreproducible `20110` figure is replaced. | Red: base A1 shows `python3 -c "<brace-matching counter; see ledger C-001 evidence>"`, which is not runnable, and no stated method yields 20110 (naive brace match gives 19950 over the same 73 files). Green: A1 carries the full runnable counter plus `wc -l` over the two wholly test-gated files, yielding 19651 block lines + 3598 external lines = 23249 total; §26.B reads 30% / scheduler 10806 and §26.H reads 23 kilolines. | **PROVEN** |
| C-005 | D-5 `task/roadmap/epic-term/map.md:91` no longer says step 3 is pending. | Red: base map reads `Step 3 (read, ADR disposition line, PR) pending` while the audit header reads landed (#426, 2026-09-09) and the step-3 clause C-012 is PROVEN. Green: the map reads landed (#426, 2026-09-09). | **PROVEN** |

## Fix

| Name | Layer |
|---|---|
| §26.B split | `task/roadmap/epic-term/ballista-audit-2026-09-08.md`: `state/` 16427 split 7491 + 8936, stage-graph-proper 6101, test weight 23249 / 30% / scheduler 10806, largest hand-written file, executor `~5000`, shuffle write nine files |
| seat rename | Same document, five sites: `SessionProvider` reads `SessionBuilder` with the tag signature |
| dependency tiers | Same document (§26.C, R-7, §26.F DROP reason, appendix A2): default stack versus `prometheus-metrics` / `graphviz-support` / `keda-scaler`, with the `cargo tree` invocations |
| real counter | Same document appendix A1: runnable counter plus external-file count, `20110` replaced by `23249` |
| map truth-up | `task/roadmap/epic-term/map.md:91`: step 3 reads landed (#426, 2026-09-09) |
| M2 inherit | `docs/design/distributed-m1.md`: one paragraph on the measured default-dependency surface as scheduler-placement input, inherited by BALLISTA-M2-A |

The M1-B card in `task/roadmap/mid-term/cheap-tier-slate-2-2026-09-09.md`
copies the wrong seat name (Q-42 notes it); that card is outside this
unit's Home and is left for its owner, recorded here as out of scope.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: review-fix-13
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every corrected number re-measured at tag 54.1.0 commit f4e66525 with the command beside it; base figures shown failing the same commands in C-001 through C-005.
      artifacts: [task/roadmap/epic-term/ballista-audit-2026-09-08.md]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary arms both ways: optional features proven absent from the default tree by failing -i lookups, unconditional deps proven present by depth-1 tree plus ungated use sites.
      artifacts: [task/roadmap/epic-term/ballista-audit-2026-09-08.md]
    - id: AT-3
      status: N/A
      justification: Reading unit. No code, no execution path, no failure mode is introduced.
    - id: AT-4
      status: N/A
      justification: No state, no ordering, no concurrency is introduced. The scratch upstream clone lives outside the repo and never enters a commit.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, push, gh, or dependency-file change; no comments added to code.
      artifacts: [task/ledgers/staging/review-fix-13-ledger.md]
    - id: AT-6
      status: N/A
      justification: No public API change. Document corrections only.
    - id: AT-7
      status: N/A
      justification: No live oracle exists for upstream line counts; the pinned tag plus runnable commands are the oracle.
    - id: AT-8
      status: ATTACKED
      evidence: No .rs/.py/.toml/.sh/.yml files changed, so no size, lint, or comment gate is armed; the fence grep from the brief prints nothing.
      artifacts: [task/ledgers/staging/review-fix-13-ledger.md]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: Ledger listed in its directory map in the same commit; docs-links gate run over the tree.
      artifacts: [task/ledgers/staging/review-fix-13-ledger.md, task/ledgers/staging/map.md]
```
