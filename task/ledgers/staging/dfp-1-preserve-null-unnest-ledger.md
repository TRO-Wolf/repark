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
the required downward ratchet in `scripts/check_rust_file_size.py` and `scripts/map.md`; this
ledger and its map; `STATUS.md`; `briefs/next-sequence.md`,
`briefs/v2-engine-hardening.md`, and `briefs/map.md`. Closed: dependencies, SQL doors, Python
compute, the optimizer guard, struct extraction, custom physical operators, and unrelated cleanup.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict | Evidence / closing question |
|---|---|---|---|---|
| C-001 | The unchanged baseline records the logical plan for two nullable ordinary List columns with `empty_as_null=false`. | A committed plan-shape test is shown red against the baseline before the code edit. | **PROVEN** | Red run: two CASE projections and two Unnest nodes; `ordinary_lists_without_empty_rewrite_have_no_case_projection` failed 2 != 0. |
| C-002 | With `empty_as_null=false`, ordinary List, LargeList, and FixedSizeList columns reach Unnest without a null-replacement CASE, and Unnest preserves null input rows. | Plan-shape pins plus value/type pins for each container family. | **PROVEN** | `preserve_nulls` module: ordinary plan, LargeList matrix, FixedSizeList matrix. |
| C-003 | With `empty_as_null=true`, the rewrite replaces only empty lists with a singleton typed-null list; null lists flow to preserve-null Unnest unchanged. | A plan pin excludes `column IS NULL` from the rewrite; value/type pins cover null, empty, and populated List rows. | **PROVEN** | Red plan contained `IS NULL OR`; green `empty_as_null_rewrite_only_checks_length` excludes it. |
| C-004 | Dictionary-of-List remains cast one level before Unnest because DataFusion Unnest does not accept the dictionary wrapper. | Dictionary pins cover null, empty, and populated rows under both `empty_as_null` modes. | **PROVEN** | `dictionary_list_preserves_null_and_controls_empty_rows`; existing dictionary unwrap pin also green. |
| C-005 | Row values and Arrow output types remain unchanged for the finite input partition. | Partition: List × {null, empty, populated} × {true, false}; LargeList × {null, empty, populated} × {true, false}; FixedSizeList × {null, populated} × {true, false}; Dictionary<List> × {null, empty, populated} × {true, false}. Existing pins may discharge cells they already cover. | **PROVEN** | List: `null_and_empty_array_values`; the other families: `preserve_nulls` module; all assert Int64 output. |
| C-006 | Multiple list columns retain sequential Cartesian expansion and schema order. | Existing two-list value/order pin stays green; the plan pin counts one Unnest per list. | **PROVEN** | `two_lists_keep_cartesian_schema_order`; existing `multi_list_serial_explode_order` green. |
| C-007 | The public options, error tokens, unsupported-element refusals, and depth behavior do not change. | Targeted dynamic-flatten suite and public facade collection remain green. | **PROVEN** | `cargo test -p repark-core dynamic_flatten`: 48 passed; `make verify`: 0. |
| C-008 | The implementation adds no dependency, unsafe code, panic path, or new public interface. | Diff review plus clippy and panic-ban gates. | **PROVEN** | Rust candidate scans: 0; `make verify` clippy and panic-ban: 0. |
| C-009 | Code comments do not grow; names and tests carry the implementation detail. | Comment-density gate and final diff review. | **PROVEN** | New Rust test file has zero comments; one existing module comment removed; comment-density gate green. |
| C-010 | Every touched directory map and live planning document stays in lockstep. | Map, ledger, and docs-compaction gates. | **PROVEN** | `make verify`: map, ledger, grammar, and docs-compaction checks green. |
| C-011 | The implementation branch passes the unit and pre-merge gates, followed by a procedural context-break Critic pass with fresh public execution. | `make verify`, `make preflight`, complete AT-1..AT-10 attestation, closed findings ledger, and one novel collect/to-arrow input. | **OPEN** | Record real exits and Critic evidence. |
| C-012 | Follow-up work remains measure-gated and outside DFP-1. | The hardening slate records: optimizer-wrapper traversal, struct null-mask extraction, and a custom Cartesian multi-list operator as separate candidates; no candidate is scheduled without evidence. | **PROVEN** | Planning parent `8879934`; `briefs/v2-engine-hardening.md` dated intake. |

VERDICT: OPEN (1 clause: C-011). The owner's 2026-08-31 direction approves this finite charter;
delivery closes after the Critic and pre-merge gate.

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

## Actor evidence

| Command | Exit | Result |
|---|---:|---|
| `cargo test -p repark-core preserve_nulls -- --nocapture` before implementation | 101 | 4 passed; the two intended plan pins failed. |
| `cargo test -p repark-core fixed_size_list_preserves_null_rows_in_both_modes -- --nocapture` before fixed-size refinement | 101 | Positive-width FixedSizeList still had one CASE. |
| `cargo test -p repark-core preserve_nulls -- --nocapture` | 0 | 6 passed. |
| `cargo test -p repark-core dynamic_flatten` | 0 | 48 passed. |
| `make verify` | 0 | Full workspace unit gate green. |

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-DFP-1-actor
  agent: Actor
  action: Submit the preserve-null Unnest implementation for Critic review
  charter_trace: DFP-1 C-001..C-010, C-012
  preconditions:
    - Revert-red structural pins recorded: SATISFIED (Actor evidence)
    - Finite value and type matrix green: SATISFIED (preserve_nulls module and existing List pins)
    - Unit gate green: SATISFIED (make verify exit 0)
  success_condition: The staged diff removes the named CASE work while every DFP-1 semantic pin and workspace gate stays green
  step_risks:
    - Empty arrays survive false mode: HANDLED(value matrices)
    - Null arrays disappear: HANDLED(preserve-null matrices)
    - Multi-list semantics become zip/pad: HANDLED(C-006 Cartesian pin)
  contingencies:
    - Remediate any Critic finding with a new red pin: EXECUTABLE(additive)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```
