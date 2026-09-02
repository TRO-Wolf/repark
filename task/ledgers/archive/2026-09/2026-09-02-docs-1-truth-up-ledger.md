# Charter ledger — DOCS-1 · truth-up after the 2026-09-01/02 merges

**Date:** 2026-09-02 · **Branch:** `docs/truth-up-2026-09-02` · **Base:** `28fb5f4` (`main`,
V3-8 merged) · **Contract:** [.agents/skills/compact-context-docs/SKILL.md](../../../../.agents/skills/compact-context-docs/SKILL.md)
· **Path:** STANDARD. **risk_tier:** standard. **Model:** claude-opus-5 (medium).

**Retires:** moves to `completed/` in this unit's last commit; the next pickup's
`make ledger-archive` files it under `archive/2026-09/`.

**Scope.** Documents only — no code file is edited. The delta is #291/#292/#295/#296/#297/#298
(V3-5, EX-0, SEM-1, EX-1, V3-6, REF), #300–#305 (nightly, RP-5, RP-6, V3-7, V3-8) and the fork
V3 production plan's close-out (fork `#250`–`#259`).

## What was stale, and what each document says now

| Document | Was | Now |
|---|---|---|
| `STATUS.md` | stamped 2026-09-01; "v0.6.0 **is cutting** 2026-08-31" restating the tag list already in Release state; the `Next:` bullet named no owner for the MoR cell; two ledger links displayed a bin the file had left | stamped 2026-09-02; "v0.6.0 shipped 2026-08-31" with a pointer to Release state (−61 B); `Next:` names **V3-9**; the SEM-1 and RP-5 link texts match their bins |
| north star §3 "Read/write: v3 types + default values" | ❌ absent — no engine surface reaches one | ⚠ V3-6 (2026-09-01): ns timestamps and column defaults consumed, DEFAULT DDL refuses Spark-equal, `unknown` refuse pinned; the residual is the dated `V3-VARIANT-SHRED-1` DECLARED row |
| north star §3 "Write: MOR DML via deletion vectors" | the unserved subquery-`WHERE` cell had no owner | the same one 🚫, now owned by follow-up unit **V3-9** |
| north star §4 engine lane | "a second COW DELETE after overwrite stays refused (F-rp3-c7)" | RP-6 lifted it — F-rp3-c7 is a layout artefact, not a defect; V3-7 lifted MERGE and V3-8 the subquery-`WHERE` COW rewrite (`V3-COW-1` FIXED); the MoR cell is V3-9's |
| fork handoff F-7 | "RePark-owned MERGE still reassigns and stays `V3-COW-1`" | V3-7 / V3-8 carry `_row_id` through MERGE and the subquery-`WHERE` COW rewrite; `V3-COW-1` FIXED, refusal seat deleted, no engine-side F-7 residue |
| fork handoff F-16 residue 2 | "RDF-1 stays BACKLOG" on a fork ratio-clause mechanism | REFUTED 2026-09-02 by fork `#259`: the ratio clause is identical to Java's and Spark reclaims via full `file_path` bounds; the gap is RePark's own position-delete writer, so RDF-1 re-homes to the engine (unit RDF-1, in flight on its own lane) |
| fork handoff Retirement | no record of the fork plan closing | "Fork V3 production plan closed out 2026-09-02" — rows ✅/🟡, gate 8 owner-run, PR-7 names RePark's gate 9, `GAP_MATRIX.md` stays the single home; still open: F-10, F-11, F-12 |
| registry `RDF-1` | mechanism attributed to the fork's ratio clause | one dated clause records the refutation and the re-home; the row's rewrite belongs to the in-flight RDF-1 unit |
| `docs/fork-sync.md` | — | verified: the RP-5 (`00cdde0`) and RP-6 (`fb0cacfa`) pin-history rows are present with their fork PR lists; no edit needed |
| `briefs/next-sequence.md` | — | verified: the queue table holds no unit rows; nothing to compact |
| `task/ledgers/staging/map.md` | six merged units listed "in flight" | only the four campaign charters remain (ex-2, fnp-0, sem-0, v3-0) |
| `task/ledgers/completed/map.md` | the six rows carried "in flight" / "parks in staging until the owner merges" verbatim | one merged row each with its merge date and PR |
| `docs/examples/map.md`, `briefs/example-backfill.md` | EX-0/EX-1 ledger paths displayed `staging/` | displayed path matches the bin |

## Clauses

| id | Proposition | Evidence | Verdict | Result |
|---|---|---|---|---|
| C-001 | The merged units' ledgers leave `staging/` by `ledger_lifecycle.py move`, the four campaign charters stay, `make ledger-archive` files what is on `main`, and every map moves in the same commit. | `check-ledgers`, `check-map-sync`, `git status`. | **PROVEN** | Six moved (ex-0, ex-1, ref, sem-1, v3-5, v3-6); none refused; ex-2 / fnp-0 / sem-0 / v3-0 stay; `archive` filed only v3-8 (the six are not yet on `main`, so they file at the next pickup); 183 ledgers in bins, 619 links resolve. |
| C-002 | STATUS.md states the merged truth for the v3 and REF workstreams, carries no stale lifecycle word inside the delta, and stays under the compaction ceiling. | `check-docs-compaction`; the two meta-pins that read STATUS. | **PROVEN** | 24,847 B → 24,788 B against a 25,000 B ceiling; "is cutting" gone; `Next:` names V3-9; markers unchanged. |
| C-003 | Every north-star §3 row the delta touched says what is proven and what residual remains, one dated clause each; the owner-run rows and the gate paragraph are untouched. | `test_v3r_1_rulings.py`, `test_plan_1_northstar_fnp_sequence.py`. | **PROVEN** | Types row ⚠ V3-6; MOR row keeps exactly one 🚫 and names V3-9; "Live: Glue + S3 Tables v3 legs" ❌ and "Scale" ⚠ unchanged; gate paragraph byte-identical. |
| C-004 | The fork handoff marks every ask the fork campaign answered as consumed with its PR, states F-rp3-c7 as a layout artefact, records F-16 residue 2 REFUTED with RDF-1 re-homed, and carries a close-out note pointing at the fork's own home for row state. | The handoff diff; `check-map-sync`. | **PROVEN** | F-0 / F-6b / F-6c / F-7 / F-8 / F-13 / F-14 / F-15 / F-17 consumed with fork PR numbers; F-9 ruled (`#233`, R126); F-10 / F-11 / F-12 open; close-out cites fork `#250`–`#259` and `GAP_MATRIX.md`. |
| C-005 | `docs/fork-sync.md` carries a pin-history row per pin-bump PR for RP-5 and RP-6 with the fork PR list, and the live pin is single-valued. | The pin-history table; `grep -oE 'rev = "[0-9a-f]{40}"' Cargo.toml \| sort -u`. | **PROVEN** | Both rows present and correct (`00cdde0` ← `33be9a0`; `fb0cacfa` ← `00cdde0`); one rev line; no edit was needed. |
| C-006 | The five doc gates and the meta-pins that read these documents are green after the edits, each run alone. | Gate exits recorded below. | **PROVEN** | `check-map-sync` / `check-ledgers` / `check-ledger-grammar` / `check-docs-compaction` / `check-manifest` exit 0; the three meta-pin files pass 14/14 with the five `REPARK_PARITY_LIVE`-gated live legs skipped, `test_v3_live_oracle.py`'s `_ledger_path` resolving the archived V3-8 ledger and `test_northstar_nightly_v3_leg_is_v3e_5` green. |

## Gate exits

| Gate | Exit |
|---|---|
| `make check-map-sync check-ledgers check-ledger-grammar check-docs-compaction check-manifest` | 0 |
| `pytest test_v3r_1_rulings.py test_plan_1_northstar_fnp_sequence.py test_v3_live_oracle.py -q` (after `make develop`) | 0 (14 passed, 5 skipped — the live legs are `REPARK_PARITY_LIVE`-gated, as routine CI is JVM-free) |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: docs-1-truth-up
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause C-001..C-006 is PROVEN against a gate exit or a diff hunk, not a paraphrase; each stale-to-now row names the document and the sentence that moved.
      artifacts: [task/ledgers/staging/docs-1-truth-up-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: The byte ceiling is the boundary case — STATUS.md started 153 B under it and the truth-up had to shrink, not grow; the compaction is a pointer replacing a restated tag list, so no fact was deleted.
      artifacts: [STATUS.md, scripts/check_docs_compaction.py]
    - id: AT-3
      status: ATTACKED
      evidence: A ledger `move` that a still-OPEN clause would refuse was tried on all six and none refused; `make ledger-archive` correctly declined the six not yet on `main` rather than dating them from the clock.
      artifacts: [task/ledgers/completed/map.md, task/ledgers/staging/map.md]
    - id: AT-4
      status: N/A
      justification: No code, no concurrency, no shared mutable state — documents only.
    - id: AT-5
      status: N/A
      justification: No AWS, IAM, secrets, or credentialed surface touched; the owner-run gate rows are left exactly as they stand.
    - id: AT-6
      status: ATTACKED
      evidence: The delta boundary is held — sections the merges did not touch are unedited, and the one out-of-delta correction (registry RDF-1) is a single dated clause that leaves the row's rewrite to the in-flight RDF-1 unit.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md]
    - id: AT-7
      status: N/A
      justification: No runtime resource claim is made or changed.
    - id: AT-8
      status: ATTACKED
      evidence: Fork-side row state stays single-homed in the fork's GAP_MATRIX.md; the close-out note points rather than restates, and no fork ledger path this tree cannot verify is invented.
      artifacts: [task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md]
    - id: AT-9
      status: ATTACKED
      evidence: Each lifted refusal is stated with the unit and date that lifted it, and each surviving refusal names its true cause — the MoR cell is predicate DML's V2-only delete-file gate, not V3-COW-1.
      artifacts: [task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md, STATUS.md]
    - id: AT-10
      status: ATTACKED
      evidence: The three pin files that read STATUS, the north star and the ledger bins were run after the edits (14 passed, 5 live legs gate-skipped); the five doc gates each ran alone and exited 0. No pin was adjusted to fit the docs.
      artifacts: [python/repark-parity/tests/test_v3r_1_rulings.py, python/repark-parity/tests/test_plan_1_northstar_fnp_sequence.py, python/repark/tests/test_v3_live_oracle.py]
  complete: true
```
