# PR #247 revalidation ledger — owner ruling and sound enforcement

**Retires:** this ledger moves to `../completed/` when PR #247 merges or closes without merge.

**Date:** 2026-08-27 · **Branch:** `docs/owner-ruling-no-comments-from-anthropic-models` ·
**Base integrated:** `3fd4cc2` (`main`, CAP-1)

## Scope

This unit preserves the owner ruling already recorded in PR #247. It revalidates the ruling against
the contributor contract after CAP-1. It does not compact comments across the repository.

The repository cannot prove which model authored source text. A source-wide density gate therefore
cannot enforce a model-family rule. The same gate also cannot set a zero default without rejecting
required Rust documentation, section banners, and comments that carry safety invariants. Immediate
mechanical enforcement is limited to byte-exact preservation of the ruling and its compatibility
contract. Contributor identity and compliance remain review duties.

## Frozen propositions

| # | Clause | State | Evidence |
|---|---|---|---|
| C-001 | The owner-ruling blocks in `AGENTS.md` and `CLAUDE.md` remain byte-for-byte equal to PR #247 commit `cd2e4be`. | PROVEN | Both extracted blocks hash to `1f588e8a6084ac37cedb17fcce9cd7ab21cd555fcf49685ac455a48837f00787`; the focused checker embeds the same bytes and mutation tests go red. |
| C-002 | The branch integrates `main` commit `3fd4cc2` and retains CAP-1's exact-baseline 1,000-line Rust and Python source-size gates. | PROVEN | `MERGE_HEAD` is `3fd4cc2`; CAP-1's test and gate files are byte-identical to that parent, and the combined focused suite passes 34 tests. |
| C-003 | Immediate enforcement makes no false claim that repository text can identify an Anthropic model; attribution and compliance are assigned to review. | PROVEN | The exact enforcement boundary states that authorship is undetectable and review holds the rule; its mutation test fails closed. |
| C-004 | The contract continues to permit required public docstrings, Rust documentation and 91-`=` banners, and concise comments that preserve non-obvious invariants. | PROVEN | The exact boundary preserves required docstrings, Rust banners, and invariant comments; the existing contract retains the detailed documentation and 91-`=` rules. |
| C-005 | PR #247 does not perform the separately planned repository-wide comment-compaction campaign. | PROVEN | The density script and seeded baseline are deleted; the focused tree test requires their absence and the boundary says `No sweep.` |
| C-006 | Every added or removed enforcement artifact is represented in the touched directory maps, and focused tests fail if the retained contract drifts. | PROVEN | Root, scripts, parity-test, and staging-ledger maps describe the final paths; `make check-map-sync` and `make check-ledger-grammar` pass. |
| C-007 | The final worktree passes focused tests, `make ci`, and `make verify`; `make preflight` runs if resource and runtime conditions remain reasonable. | PROVEN | The Critic reran the 34-test focused suite, `make ci`, `make verify`, and `make preflight` against the final staged tree. Each command exited 0. The final status check found no unstaged or untracked residue. |

## Actor plan

- [x] Resolve the forward-merge conflict additively and verify the ruling bytes against `cd2e4be`.
- [x] Remove the attribution-blind comment-density gate and replace its claims with a narrow,
  review-held enforcement statement outside the verbatim ruling blocks.
- [x] Add mutation-proof tree tests for ruling preservation, enforcement scope, CAP-1 compatibility,
  and required comment/docstring/banner compatibility.
- [x] Update every touched map and prove the ledger citations resolve.
- [x] Run focused and repository gates, re-check disk before broad validation, and stage the final
  merge result with no unstaged or untracked residue.

## Actor outcome

The ruling bytes are unchanged. Immediate automation now preserves the ruling and the compact
review boundary, but makes no attribution claim. CAP-1 remains integrated. The full local preflight
surface is green.

## Critic findings

| ID | Severity | Finding | Disposition |
|---|---|---|---|
| Q-001 | S1 | `check-owner-ruling` was absent from ci.yml's raw guards job, so PR CI did not run the new gate. Duplicate protected blocks also passed. | REMEDIATED — the raw CI step and exact-once mutation pins are present. |
| SAF-001 | S1 | Symlinked contract paths redirected validation to alternate files and returned clean. | REMEDIATED — protected paths must be regular files; the symlink mutation pin goes red. |
| L-001 | S1 | The enforcement boundary could move away from the ruling and still pass. | REMEDIATED — the boundary must be exact, unique, and adjacent; the relocation pin goes red. |

## Critic coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: pr-247-revalidation
  cycle: final
  risk_tier: standard
  critic_engine: ccc
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001 through C-007 were checked against the staged tree and merge parents.
      artifacts: [proposition ledger, byte oracle, git parent diff]
    - id: AT-2
      status: ATTACKED
      evidence: Missing, malformed, duplicated, relocated, mutated, and symlinked fixtures failed closed.
      artifacts: [test_pr_247_owner_ruling.py]
    - id: AT-3
      status: ATTACKED
      evidence: Read and decode failures return findings; the checker makes no partial write.
      artifacts: [scripts/check_owner_ruling.py, test_pr_247_owner_ruling.py]
    - id: AT-4
      status: N/A
      justification: The checker is a stateless read-only tree scan with no shared mutable state.
    - id: AT-5
      status: ATTACKED
      evidence: Symlink redirection, environment, network, subprocess, secret, and destructive surfaces were attacked.
      artifacts: [scripts/check_owner_ruling.py, test_contract_symlink_fails_closed]
    - id: AT-6
      status: ATTACKED
      evidence: Required docstrings, Rust banners, and invariant comments remain compatible with the exact adjacent boundary.
      artifacts: [AGENTS.md, test_relocated_enforcement_boundary_fails_closed]
    - id: AT-7
      status: N/A
      justification: Two fixed files are read once; the unit adds no system-breaking resource surface.
    - id: AT-8
      status: ATTACKED
      evidence: Makefile and ci.yml both invoke the checker; CAP-1 is byte-identical to MERGE_HEAD.
      artifacts: [Makefile, .github/workflows/ci.yml, git parent diff]
    - id: AT-9
      status: ATTACKED
      evidence: Every failure names the protected file and the violated contract without printing contents.
      artifacts: [scripts/check_owner_ruling.py]
    - id: AT-10
      status: ATTACKED
      evidence: Focused mutations go red for each retained predicate; full staged-tree gates exit 0.
      artifacts: [test_pr_247_owner_ruling.py, test_cap_1_source_file_line_cap.py]
```

Q-001, SAF-001, and L-001 are remediated with regression pins. No open finding remains at or above
the S1 floor.

## Scope boundary

No product behavior, dependency, AWS, Iceberg fork pin, source-comment sweep, commit, push, or PR
metadata change belongs to this unit.
