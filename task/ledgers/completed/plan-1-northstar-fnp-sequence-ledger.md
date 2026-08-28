# PLAN-1 — North Star and FNP sequence truth-up

**Date:** 2026-08-28 · **Branch:** `codex/plan-northstar-fnp` · **Base:** `ecbd6a4`
(`#253`) · **Path:** LIGHT (planning documents, navigation, one read-only plan-contract test,
and a lifecycle-only repair to an existing meta-pin; no runtime, dependency, workflow, or
public-interface change) · **Policy:**
[AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle".

**Retires:** moved to `../completed/` when this plan-document PR is ready for review.

The owner approved the reviewed North Star and FNP recommendations, then requested a RePark PR
to update the plan documents. This unit single-homes the detailed sequence in the existing
format-v3 and FNP campaign documents. STATUS carries only current state, the rolling slate carries
only the next work, and the fork handoff carries only the F-17 request.

## PROPOSITION LEDGER — PLAN-1 — 2026-08-28

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The North Star sequence is guarded RP-2 → fork F-17 → immutable RP-3 → V3-3 → V3-4/V3-5 → production gate, while V3-6 may run in parallel after its own fork support. | Design and epic plan agree; tree pin fixes the order. | **PROVEN** | `test_north_star_sequence_keeps_the_guard_before_the_fork_fix`; plan and epic carry the same critical path. |
| C-002 | Current state and the rolling slate retire merged V3E-5, queue RP-2 then FNP-15/16, and state that RP-3 preempts the side lane after F-17. | Lifecycle mover output; STATUS and slate pin. | **PROVEN** | V3E-5 moved to `completed/`; `test_live_slate_retires_v3e_5_and_queues_the_safe_work`. |
| C-003 | Fork F-17 records the measured shared-Puffin failure, its path-keyed mechanism, maintenance reuse point, exact closure ask, sabotage case, and Java read-back acceptance. | Handoff section and mutation-resistant tree pin. | **PROVEN** | `test_fork_handoff_records_the_shared_puffin_failure_and_acceptance`; Critic corrected physical offset handling. |
| C-004 | The FNP design, brief, and STATUS use one coherent PR per remaining unit or coupled pair and one order: FNP-15/16 → F-Y10-1 → FNP-4c → FNP-7a/7b → FNP-9/10 → FNP-8 → FNP-11/12 → FNP-Z. | Cross-document order pin; stale one-branch phrase absent. | **PROVEN** | `test_fnp_documents_share_one_remaining_order_and_delivery_shape`; frozen charter carries the PLAN-1 amendment pointer. |
| C-005 | The FNP charter retires at FNP-Z, and the plan states that FNP and TA performance consume no F-17 surface and do not gate v1.0. | Charter, v3 design, and tree pin. | **PROVEN** | `test_fnp_retirement_and_fork_independence_are_explicit`. |
| C-006 | Every touched directory map stays in lockstep, the RP-2 ledger carries the narrowed salvage ruling, and all required gates pass. | Map pin; map, ledger, docs-compaction, verify, and preflight gates. | **PROVEN** | Map/lifecycle gates green; `make verify` and `make preflight` exit 0; departure uses the sanctioned mover. |

VERDICT: PASS (OPEN=0). LOGIC_SCORE = 6/6.

## Self-logic review

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-PLAN-1
  agent: Orchestrator
  action: PRE_EXECUTION_REVIEW then ACTOR_BUILD
  charter_trace: C-001..C-006
  preconditions:
    - owner approved the reviewed recommendations and requested this PR: SATISFIED
    - origin/main includes merged V3E-5 at ecbd6a4: SATISFIED
    - LIGHT/STANDARD rubric: SATISFIED (documents + read-only plan and lifecycle pins only)
  success_condition: every clause PROVEN and all PR gates green
  step_risks:
    - duplicate truth across plans: HANDLED (existing authoritative homes retained)
    - unsafe RP-2 capability claim: HANDLED (live-DV refusal stays until F-17/RP-3)
    - FNP side lane delays North Star: HANDLED (RP-3 explicitly preempts it)
  contingencies:
    - revert this additive docs branch: EXECUTABLE
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## Readiness record

`uv run --package repark-parity pytest python/repark-parity/tests -q` passed 459 tests.
`make verify` passed the full Rust, Python, structure, document, and lifecycle surface.
`make preflight` passed 3,757 facade tests with 74 expected skips, the Rust and Python security
audits, dependency policy, and all 13 workflow parse/security checks. The focused PLAN-1 pin
passed 6 tests. The repaired V3E-5 meta-pin passed in the complete facade run.

Disk was checked before artifact-heavy gates: 810 GB free at entry, 787 GB before preflight, and
784 GB after preflight. The shared `target/` grew from 54 GB to 74 GB and remains as the warm
repository cache; deleting it would discard expensive artifacts used by other tasks. The local
`.venv` remains at 1.1 GB. Temporary wheel directories cleaned themselves; no worktree or
task-private artifact remains.

```yaml
KILLED_ASSUMPTIONS:
  - copied Puffin blobs retain old physical offsets: REMOVED (replacement offsets, lengths, and file size are recomputed)
  - RP-3 depends only on F-17: REMOVED (RP-2 and F-17 must both land)
  - a completed ledger can stay hard-coded under staging: REMOVED (V3E-5 pin follows the lifecycle)
RISK_HEATMAP:
  - unsafe live-DV capability implied by RP-2: MITIGATED (second-delete and shared-Puffin refusals are chartered)
  - side-lane FNP work delays v1.0: MITIGATED (RP-3 preempts after RP-2 and F-17)
CLARIFYING_QUESTIONS: []
```

```yaml
COVERAGE_ATTESTATION:
  pr_unit: plan-1-northstar-fnp-sequence
  cycle: final
  risk_tier: low
  critic_engine: self_logic_review
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: exact North Star and FNP order sections are pinned, including parallel V3-6
      artifacts: [docs/design/format-v3-track.md, docs/design/spark-function-parity.md, python/repark-parity/tests/test_plan_1_northstar_fnp_sequence.py]
    - id: AT-2
      status: ATTACKED
      evidence: RP-2, F-17, and RP-3 dependency transitions were reviewed; RP-3 now requires both predecessors
      artifacts: [briefs/next-sequence.md, task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md]
    - id: AT-3
      status: ATTACKED
      evidence: live-DV second DELETE, shared Puffin, COW UPDATE/MERGE, and rewrite guards remain explicit
      artifacts: [task/ledgers/staging/rp-2-fork-repin-ledger.md, docs/design/format-v3-track.md]
    - id: AT-4
      status: N/A
      justification: this unit changes no runtime, concurrency, lock, or retry behavior
      artifacts: [git diff]
    - id: AT-5
      status: ATTACKED
      evidence: no AWS, IAM, dependency pin, workflow, or destructive surface changed; preflight security gates passed
      artifacts: [git diff, make preflight]
    - id: AT-6
      status: ATTACKED
      evidence: STATUS, slates, campaign designs, roadmaps, maps, and ledger lifecycle were reviewed as one tree
      artifacts: [STATUS.md, briefs/map.md, docs/design/map.md, task/ledgers/completed/map.md]
    - id: AT-7
      status: N/A
      justification: no query value, Arrow type, public API, or data schema changes
      artifacts: [git diff]
    - id: AT-8
      status: ATTACKED
      evidence: full parity harness, verify, facade suite, dependency audits, and workflow checks passed
      artifacts: [python/repark-parity/tests, make verify, make preflight]
    - id: AT-9
      status: ATTACKED
      evidence: F-17 sabotage and pre-write failure requirements are explicit; V3E-5 lifecycle lookup fails on zero or duplicate ledgers
      artifacts: [task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md, python/repark/tests/test_v3_live_oracle.py]
    - id: AT-10
      status: ATTACKED
      evidence: all six clauses have live pins and ledger grammar is green
      artifacts: [python/repark-parity/tests/test_plan_1_northstar_fnp_sequence.py, scripts/check_ledger_grammar.py]
```

## Critic findings and disposition

| Finding | Severity | Disposition |
|---|---|---|
| F-001 — F-17 told a repacked Puffin to preserve old offsets and file size | S1 | REMEDIATED — preserve logical metadata and payload; recompute physical offsets, lengths, and size |
| F-002 — the slate let RP-3 open after F-17 without naming RP-2 | S2 | REMEDIATED — RP-3 requires both RP-2 and F-17 |
| F-003 — FNP map grammar and first-tranche STATUS were stale | S3 | REMEDIATED — map sentence corrected; STATUS links #190–#193 |
| F-004 — V3E-5 meta-pin hard-coded its ledger in `staging/` | S1 | REMEDIATED — exactly one staging, completed, or archived ledger must exist |
| F-005 — V3E-5 diff allowlist inspected the current branch instead of its landed unit | S1 | REMEDIATED — the pin inspects immutable landing commit `ecbd6a4` |
| F-006 — Ruff requested one lifecycle-test reflow | S3 | REMEDIATED — pinned formatter applied; format check green |

No open finding at or above the S1 floor remains.
