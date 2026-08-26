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
| C-010 | `AGENTS.md` still contains the PYC-5 load-bearing tokens (`**Not** on the pre-commit hook as of PYC-5`, `sub-second budget`) | Tree pin | **PROVEN** | `test_pyc_5_load_bearing_tokens_remain_in_agents` |
| C-011 | `engineering-method` still contains how-to that never had an `AGENTS.md` home: iterators over indexing, Python-to-Rust FFI validation | Tree pin | **PROVEN** | `test_method_keeps_how_to_with_no_agents_home` |
| C-012 | The slate carries a marked DL-5 unit at #2 (V3E-5 stays #1) | Tree pin | **PROVEN** | `test_dl_5_is_slate_row_two` |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: DL-5
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: walked C-001..C-012 against the tree; KEEP invariants still in AGENTS.md (C-006); PYC-5 tokens restored (C-010)
      artifacts: [python/repark-parity/tests/test_dl_5_contract_compaction.py]
    - id: AT-2
      status: N/A
      justification: no public API or schema change
    - id: AT-3
      status: ATTACKED
      evidence: C-008 over-ceiling provocation is red; green at seeded ceilings; untracked CEILINGS key is a finding
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
      status: ATTACKED
      evidence: gate (d) now fails an untracked CEILINGS key; C-008 proves AGENTS.md over-ceiling is red without tripping (a)/(c)
      artifacts: [python/repark-parity/tests/test_dl_5_contract_compaction.py::test_ceilings_cover_the_contract_files]
    - id: AT-9
      status: N/A
      justification: no failure-path product code
    - id: AT-10
      status: ATTACKED
      evidence: every PROVEN clause has a pins citation; PYC-5 suite and C-010 pin the load-bearing tokens make verify does not run
      artifacts: [python/repark-parity/tests/test_dl_5_contract_compaction.py, python/repark-parity/tests/test_pyc_5_close.py]
  reattested: [AT-1, AT-3, AT-8, AT-10]
  complete: true
```

## Critic pass — cycle 1 (procedural, not amnesia)

Context break executed; attacking artifacts, not memory. Inputs: the charter clauses, the
diff, the PYC-5 tests, this taxonomy. The Actor's build narrative was not the attack basis.

**Cycle-1 input:** two independent reviews of PR #243 — the first named S1 (PYC-5 tokens),
S2 (self-reported attestation / AT-1 N/A), S3 (dropped Rust how-to), S3 (absent slate row);
the second confirmed the extra `sub-second budget` pin, ruled restore-tokens-not-retarget,
AT-1 must be ATTACKED, AT-8 ATTACKED citing C-008, KEEP-as-how-to for the two Rust lines,
and a slate **row at #2** (not the exclusion list). This section names those reviews as the
seed; findings below are filed against the tree after remediation.

**Risk tier:** standard. **Mode:** review-only (remediation is the Actor's, already applied
in this commit). **Break:** procedural in-session, named honestly.

**Lesson (C-011):** a method how-to that never had an `AGENTS.md` home is KEEP, not POINTER.
POINTER of a restated invariant is correct; POINTER of a unique how-to deletes it.

```yaml
FINDING:
  id: F-DL5-1
  severity: S1
  category: AT-10
  clause: C-010
  claim: AGENTS.md compaction dropped PYC-5 load-bearing tokens; two parity tests red in CI
  evidence: python/repark-parity/tests/test_pyc_5_close.py:70-71,81; AGENTS.md Python-conventions bullet
  disposition: REMEDIATED (tokens restored; test_pyc_5_load_bearing_tokens_remain_in_agents; make py-test)

FINDING:
  id: F-DL5-2
  severity: S2
  category: AT-1
  clause: C-006
  claim: attestation marked AT-1 N/A (no engine surface) on a unit whose charter is spec conformance of AGENTS.md
  evidence: prior ledger AT-1 row; C-006 is the clause walk
  disposition: REMEDIATED (AT-1 re-attested ATTACKED after this pass; complete: true re-derived)

FINDING:
  id: F-DL5-3
  severity: S3
  category: AT-1
  clause: C-011
  claim: Prefer iterators over manual indexing and FFI Python-to-Rust validation were dropped with no home
  evidence: git grep on 5375daf empty; engineering-method Language-Specific now carries both lines
  disposition: REMEDIATED (test_method_keeps_how_to_with_no_agents_home)

FINDING:
  id: F-DL5-4
  severity: S3
  category: AT-1
  clause: C-012
  claim: in-flight DL-5 had no slate unit row, so compact had nothing to remove at departure
  evidence: briefs/next-sequence.md table row 2 and unit id=dl-5 block
  disposition: REMEDIATED (test_dl_5_is_slate_row_two)
```

## Byte table (measured 2026-08-25, this branch, before departure)

| File | Before (main `b33d2cd`) | After |
|---|---|---|
| `STATUS.md` | 30,638 B | 24,307 B |
| `AGENTS.md` | 37,126 B | 30,341 B |
| `.agents/skills/engineering-method/SKILL.md` | 42,771 B | 34,100 B |
