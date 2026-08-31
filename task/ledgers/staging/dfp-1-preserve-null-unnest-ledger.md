# Unit ledger — DFP-1 · preserve-null Unnest fast path

**Retires:** this ledger moves to `../completed/` in the implementation branch's last commit. It
closes when DFP-1 merges or the owner removes the unit from the slate.

**Unit:** DFP-1 · **Date:** 2026-08-31 · **Executor:** Codex, sequential Actor–Critic ·
**Planning branch:** `codex/dynamic-flatten-plan` · **Implementation branch:**
`codex/dynamic-flatten-preserve-nulls` · **Base:** `8e087ff`

**Owner direction:** update the short-term plan, then implement the reviewed `dynamic_flatten`
optimization on a child branch. Keep comments minimal.

**Rubric:** STANDARD. The change is local, reversible, small, dependency-free, and keeps the public
API fixed. It changes a row-cardinality path whose failure can return plausible wrong rows, so the
sensitivity criterion fails. Floor S1. `risk_tier: standard`.

**Writable paths:** `crates/repark-core/src/dynamic_flatten.rs`, its file-backed tests and maps;
this ledger and its map; `STATUS.md`; `briefs/next-sequence.md`,
`briefs/v2-engine-hardening.md`, and `briefs/map.md`. Closed: dependencies, SQL doors, Python
compute, the optimizer guard, struct extraction, custom physical operators, and unrelated cleanup.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict | Evidence / closing question |
|---|---|---|---|---|
| C-001 | The unchanged baseline records the logical plan for two nullable ordinary List columns with `empty_as_null=false`. | A committed plan-shape test is shown red against the baseline before the code edit. | **OPEN** | Record the red baseline and exact redundant nodes. |
| C-002 | With `empty_as_null=false`, ordinary List, LargeList, and FixedSizeList columns reach Unnest without a null-replacement CASE, and Unnest preserves null input rows. | Plan-shape pins plus value/type pins for each container family. | **OPEN** | Implement and pin the three families. |
| C-003 | With `empty_as_null=true`, the rewrite replaces only empty lists with a singleton typed-null list; null lists flow to preserve-null Unnest unchanged. | A plan pin excludes `column IS NULL` from the rewrite; value/type pins cover null, empty, and populated List rows. | **OPEN** | Implement and pin the reduced predicate. |
| C-004 | Dictionary-of-List remains cast one level before Unnest because DataFusion Unnest does not accept the dictionary wrapper. | Dictionary pins cover null, empty, and populated rows under both `empty_as_null` modes. | **OPEN** | Preserve the cast-only projection and pin both modes. |
| C-005 | Row values and Arrow output types remain unchanged for the finite input partition. | Partition: List × {null, empty, populated} × {true, false}; LargeList × {null, empty, populated} × {true, false}; FixedSizeList × {null, populated} × {true, false}; Dictionary<List> × {null, empty, populated} × {true, false}. Existing pins may discharge cells they already cover. | **OPEN** | Cite every cell to a test before closing. |
| C-006 | Multiple list columns retain sequential Cartesian expansion and schema order. | Existing two-list value/order pin stays green; the plan pin counts one Unnest per list. | **OPEN** | Run and cite the existing pin with the new plan pin. |
| C-007 | The public options, error tokens, unsupported-element refusals, and depth behavior do not change. | Targeted dynamic-flatten suite and public facade collection remain green. | **OPEN** | Run the existing contract suites. |
| C-008 | The implementation adds no dependency, unsafe code, panic path, or new public interface. | Diff review plus clippy and panic-ban gates. | **OPEN** | Review the final diff and record gates. |
| C-009 | Code comments do not grow; names and tests carry the implementation detail. | Comment-density gate and final diff review. | **OPEN** | Confirm zero net new code comments. |
| C-010 | Every touched directory map and live planning document stays in lockstep. | Map, ledger, and docs-compaction gates. | **OPEN** | Update maps and run the gates. |
| C-011 | The implementation branch passes the unit and pre-merge gates, followed by a procedural context-break Critic pass with fresh public execution. | `make verify`, `make preflight`, complete AT-1..AT-10 attestation, closed findings ledger, and one novel collect/to-arrow input. | **OPEN** | Record real exits and Critic evidence. |
| C-012 | Follow-up work remains measure-gated and outside DFP-1. | The hardening slate records: optimizer-wrapper traversal, struct null-mask extraction, and a custom Cartesian multi-list operator as separate candidates; no candidate is scheduled without evidence. | **OPEN** | Land the planning-parent documentation. |

VERDICT: OPEN (12 clauses). The owner's 2026-08-31 direction approves this finite charter;
delivery verdicts close only when their pins and gate evidence exist.

## Sequence

1. Commit this charter and short-term slate on `codex/dynamic-flatten-plan`.
2. Create `codex/dynamic-flatten-preserve-nulls` from that commit.
3. Record the unchanged plan, add the structural pin, and show it red.
4. Implement the preserve-null Unnest path and the enumerated value/type pins.
5. Run the Actor gates, then the procedural Critic pass and fresh public execution.
6. Move this ledger to `completed/` in the departure commit.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-DFP-1-charter
  agent: Orchestrator
  action: File the DFP-1 charter and short-term plan without changing code
  charter_trace: DFP-1 C-001..C-012
  preconditions:
    - Synchronized clean main at 8e087ff: SATISFIED (git fetch and fast-forward)
    - Current dynamic_flatten implementation and tests inspected: SATISFIED (crates/repark-core/src/dynamic_flatten.rs and tests)
    - Owner approved planning and implementation: SATISFIED (2026-08-31 request)
    - Disk headroom: SATISFIED (501 GiB free on /)
  success_condition: The docs-only parent carries a grammar-clean OPEN ledger and an ordered DFP-1 slate row
  step_risks:
    - A structural reduction is reported as a wall-clock win: HANDLED(C-001 and C-012 forbid the claim)
    - Sequential list semantics are replaced by zip semantics: HANDLED(C-006)
    - The unit expands into adjacent optimizer work: HANDLED(C-012 and writable-path boundary)
  contingencies:
    - Correct the planning commit forward if a gate rejects it: EXECUTABLE(additive)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## Disk

Checked 2026-08-31: `/` has 501 GiB free of 1.8 TiB. The unit reuses the incremental target.
No cleanup or extra worktree is planned.
