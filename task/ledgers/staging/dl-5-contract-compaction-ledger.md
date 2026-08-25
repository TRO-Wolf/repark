# Charter ledger — DL-5 · compact the live STATUS remainder and the contributor contract

**Date:** 2026-08-25 · **Branch:** `feat/dl-5-contract-compaction` · **Base:** `b33d2cd`
(`main`, #242) · **Policy:** [AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle" ·
**SEPMO path:** STANDARD (contract amendment + script change; LIGHT fails size and the
contract-class rule) · **Size:** M

**Retires:** this ledger moves to `../completed/` in the unit's last commit.

**Out of scope:** host-injection measurement; `.agents/roles/`; `CLAUDE.md` spawn-read edits;
closing H-2; generating Known correctness issues from the registry; moving a universal
invariant into a skill.

**Charter condition:** every universal contributor invariant remains explicit in `AGENTS.md`.
Procedural and mechanical details have one named canonical home, and `AGENTS.md` states the
binding invariant and routes to that home.

## Proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `STATUS.md` `## Current milestone` still states `milestone one is complete` and the v1 bugfix-only standing decision; the cutover inventory remains an open numbered step | Tree pin | **PROVEN** | `test_current_milestone_keeps_the_forward_path` |
| C-002 | `STATUS.md` does not contain the Y/Z/W-wave landing paste; the four increment ledgers still exist in the 2026-08 archive | Tree pin | **PROVEN** | `test_status_no_longer_pastes_the_h2_wave_diary` |
| C-003 | `CEILINGS["STATUS.md"]` is below the DL-4 seed of 31,000 B and `STATUS.md` is under it | Tree pin | **PROVEN** | `test_status_ceiling_ratcheted_down` |
| C-004 | `engineering-method/SKILL.md` Language-Specific and Navigation sections point at `AGENTS.md` and do not contain the panic-ban essay body | Tree pin | **PROVEN** | `test_engineering_method_points_at_agents_for_invariants` |
| C-005 | The method skill still contains `<risk_first>`, `<verification_gate>`, `<scope_boundaries>`, Mode Handling, and Naming Conventions | Tree pin | **PROVEN** | `test_engineering_method_keeps_the_method` |
| C-006 | `AGENTS.md` still contains the enumerated KEEP invariants (fork owned / never vendored; two SQL doors; tests in the same commit; map.md lockstep; unsafe forbid except the binding; `cargo test --workspace`; no Glue/S3 Tables/S3/IAM drop without user action; Cargo.toml pin SSOT) | Tree pin | **PROVEN** | `test_agents_md_keeps_the_universal_invariants` |
| C-007 | No `.agents/roles/` directory exists | Tree pin | **PROVEN** | `test_no_role_packet_directory` |
| C-008 | `CEILINGS` keys include `AGENTS.md` and the method skill; an over-ceiling `AGENTS.md` is red as (d) and is not reported as (a) or (c); the tree is green at the seeded ceilings | Provocation + green run | **PROVEN** | `test_ceilings_cover_the_contract_files` |
| C-009 | DL-4 C-008 still holds: the obituary sentence once, `make check-docs-compaction` in `AGENTS.md` | Tree pin | **PROVEN** | `test_dl_4_rule_text_still_holds` |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: DL-5
  categories:
    - id: AT-1
      status: N/A
      justification: docs and one gate script; no engine surface
    - id: AT-2
      status: N/A
      justification: no public API or schema change
    - id: AT-3
      status: ATTACKED
      evidence: C-008 over-ceiling provocation is red; green at seeded ceilings
      artifacts: [python/repark-parity/tests/test_dl_5_contract_compaction.py]
    - id: AT-4
      status: N/A
      justification: no session or catalog state
    - id: AT-5
      status: ATTACKED
      evidence: no AWS/IAM; check_docs_compaction reads tracked files only
      artifacts: [scripts/check_docs_compaction.py]
    - id: AT-6
      status: ATTACKED
      evidence: C-001/C-002 keep live path and archived wave homes
      artifacts: [python/repark-parity/tests/test_dl_5_contract_compaction.py]
    - id: AT-7
      status: N/A
      justification: not system-breaking
    - id: AT-8
      status: N/A
      justification: no crate dependency change
    - id: AT-9
      status: N/A
      justification: no failure-path product code
    - id: AT-10
      status: ATTACKED
      evidence: every PROVEN clause has a pins: citation in the test file
      artifacts: [python/repark-parity/tests/test_dl_5_contract_compaction.py]
  reattested: []
  complete: true
```

## Byte table (measured 2026-08-25, this branch, before departure)

| File | Before (main `b33d2cd`) | After |
|---|---|---|
| `STATUS.md` | 30,638 B | 24,307 B |
| `AGENTS.md` | 37,126 B | 30,341 B |
| `.agents/skills/engineering-method/SKILL.md` | 42,771 B | 34,100 B |
