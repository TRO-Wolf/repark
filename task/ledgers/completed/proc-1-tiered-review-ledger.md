# Charter ledger — PROC-1 · review effort by tier, the MW-6 evidence home, two runbook truth-ups

**Date:** 2026-08-25 · **Branch:** `docs/proc-1-tiered-review` · **Base:** `5f64254` (`main`, #243)
· **Policy:** [AGENTS.md](../../../AGENTS.md) `## Precedence` (the manifest is the only
project-specific SEPMO file) · **SEPMO path:** STANDARD under the manifest *as it stands at
base* (the profile this unit binds would make it LIGHT — the stricter interpretation wins for the
unit that changes the rule) · **Size:** M · **Requested by:** the owner, 2026-08-25 evening
("we will add that to the next work group" + "add what you just mentioned to the next PR").

**Retires:** this ledger moves to `../completed/` in the unit's last commit.

**Why this unit exists (the measured case).** Over the last four engine units the defects were
caught by two instruments: the Critic's *novel-input fresh execution* through a public entry
point (V3R-1: SEC-001/002/003, all S1) and the *gates* (`make preflight`, the parity suite —
DL-5 review: two red pins). The four-phase CCC fan-out, the per-pin mutation probe, the separate
claims phase and the per-unit retrospective metrics produced record entries, not defects. The
process read for a STANDARD unit had grown to ~190 kB (spine 42 kB + references ~110 kB +
manifest 16 kB + CCC 24 kB) before the engineering method and the repo docs. Proportionality's
own rule — "adjust the amount of process, never the bar" — is what this unit applies.

**Out of scope:** any edit to the spine (`SKILL.md`) or `references/` (portable canon, D2:
defects are filed, never patched); CCC's absolute rules and taxonomies (CCC stays whole — this
unit changes *when* it is selected); the retrospective metrics format; `AGENTS.md` beyond a
pointer (it sits 624 B under its ceiling); the archived MW-6 ledger (immutable by the DL-1 rule
— the evidence home is *added*, the ledger's citations are *explained*, nothing in the archive
is rewritten); slate rows for tonight's owner-requested units (they were not queued; their
ledgers are their record).

**Charter condition.** The bar is unchanged on every tier — a green workspace, every clause
pinned, the severity floor, the R7 readiness audit, the fresh-execution rule for
silently-wrong-results claims. What changes is the *count* of Critic phases, spawns and probes
a STANDARD unit runs, and what a STANDARD unit has to read first.

## Proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `binding-manifest.md` carries a `review_profile` tunable row: a three-tier table — **LIGHT** (the spine's single in-line AC cycle — one procedural Critic pass, no fan-out/no external engine; its `COVERAGE_ATTESTATION` filed with categories `N/A` justified by the recorded rubric result; the Orchestrator may self-run the R7 audit), **STANDARD** (exactly one Critic pass — procedural or one fresh spawn — that walks CCC's Critic-1/2/3 taxonomies and the claims-vs-tree check as *one checklist* and files one attestation; this checklist is the manifest's lighter review under the spine's own Critic stage, not CCC merging its phases — CCC's absolute rule 1 governs only HIGH; obligations in C-004), **HIGH** (full CCC, every phase a fresh spawn on a scratch clone, unchanged) — with the tier chosen by CCC's own risk-tier auto-detect (riskiest touched path) | Tree pin | **PROVEN** | `test_manifest_carries_the_review_profile_table` |
| C-002 | `light_thresholds` is re-bound: a unit that changes only prose, `map.md`, ledgers, recorded evidence, or tests is LIGHT whatever its line count when the six spine criteria hold; a unit that changes code keeps the spine defaults (≤ 150 lines, ≤ 5 files) | Tree pin | **PROVEN** | `test_light_thresholds_name_the_prose_only_class` |
| C-003 | `critic_engine` binds CCC for **HIGH** units; STANDARD selects the spine's own Critic stage run per C-001; **LIGHT** runs the spine's single in-line AC cycle (no external engine); the ruling is dated 2026-08-25 (the CCC binding earlier that evening kept as history — no 2026-08-26 stamp), and the row still names the scratch-clone rule and the AT-1..AT-10 mapping | Tree pin | **PROVEN** | `test_critic_engine_row_binds_ccc_at_high` |
| C-004 | The STANDARD pass's two hard obligations are stated in the `review_profile` row: (a) for a silently-wrong-results claim, ≥ 1 **novel** input freshly executed through the public entry point, cited input → observed (the standing `s0_fresh_execution` row, unchanged); (b) every pin shown red before the fix and green after; plus a mutation probe **per new guard seat** rather than per pin. The row states the bar-unchanged list (green workspace, every clause pinned, S1 floor, R7 audit) | Tree pin | **PROVEN** | `test_standard_pass_states_its_two_obligations` |
| C-005 | `.agents/skills/sepmo/unit-runbook.md` exists, is ≤ 5,000 B, names and links every rule's home (manifest row, spine rule id, reference file, gate target) rather than restating it — and `scripts/check_docs_compaction.py` `CEILINGS` carries it at 5,000 B so it cannot regrow into a second spine | Tree pin + gate | **PROVEN** | `test_unit_runbook_is_small_and_pointer_only`, `test_ceilings_cover_the_runbook` |
| C-006 | Routing lands on the runbook first for LIGHT/STANDARD units: `sepmo/map.md` (Contents + an "I want to run a unit" row), `.agents/skills/map.md`, and the SEPMO section of `CLAUDE.md` each carry one pointer; no other file restates the profile table | Tree pin | **PROVEN** | `test_routing_points_at_the_runbook_once_each` |
| C-007 | CCC `SKILL.md` gains one scoped sentence — bound at HIGH under this repo's manifest; STANDARD units walk its taxonomies as a single-pass checklist — and nothing else in it changes (its absolute rules, tiers and taxonomies are byte-identical to base) | Tree pin (diff scope) | **PROVEN** | `test_ccc_changes_only_its_binding_sentence` |
| C-008 | `task/lessons.md` carries a dated (2026-08-25) DO / DO-NOT entry recording the ruling and the measured reason (which instruments caught the last four units' defects, which produced only record) | Tree pin | **PROVEN** | `test_lessons_record_the_ruling` |
| C-009 | The MW-6 Critic evidence the archived ledger cites (`test_critic_shapes.py`, `test_critic_shapes2.py`, `test_critic_bytes.py`, `oracle_critic.py/.log`, `oracle_k2.py/.log`, `oracle_r2.py/.log`, `jar/rmsa.txt`, `jar/rmp.txt`) lives verbatim under `task/mw-6-critic-evidence/` with a `map.md` that names the citing ledger lines; the directory is excluded from `ruff` lint/format (`pyproject.toml` `extend-exclude`, with the reason) and from `typos` (`.typos.toml`, same reason); `task/map.md` has its row; the archived ledger is unchanged | Tree pin + gates | **PROVEN** | `test_mw6_evidence_is_home_and_excluded_from_lint` |
| C-010 | `check-disk-headroom/SKILL.md` §2 carries a dated 2026-08-25 measurement block (timeshift 840 G on the same partition, `+ /home/<user>/**` included; the fork workspace target 207 G; per-unit worktree targets 23–51 G; Trash 12 G; coredumps 4 G; 24 stale kernels ≈ 6.8 G), §3 lists merged-unit worktree `target/` trees as the first reclaim, and the Gotchas name the two new rules: refute a scratch directory before `rm -rf` (it can hold the only copy of ledger-cited evidence) and `sudo` reclaim is owner-run; `map.md` in lockstep | Tree pin | **PROVEN** | `test_disk_runbook_carries_the_2026_08_25_block` |
| C-011 | The F-7 section of the iceberg-rust handoff carries a dated 2026-08-25 addendum: B-MOR-3 → extend `RewritePositionDeleteFiles` (R136) to v3, no DV-specific action — DVs-only tables return truthful zeros, v3 Parquet position deletes rewrite into one DV per data file merged with any existing DV, dangling DVs are compaction's job; acceptance = the four counts with DVs counted as delete files, the engine's B-MOR-3 refusal pin retired at the repin; sequenced after F-13. V3-DANGLE-1 → notes fork R137 and that the engine seat stays unreachable until F-7's lineage half lifts V3-LINEAGE-1 | Tree pin | **PROVEN** | `test_handoff_f7_records_the_unit_3_ruling` |

**Enumeration (C-006).** The routing homes are exactly three: `.agents/skills/sepmo/map.md`,
`.agents/skills/map.md`, `CLAUDE.md`. `AGENTS.md` is deliberately not one (ceiling headroom; its
SEPMO pointer already lands on the manifest, which now lands on the runbook).

## Scope / out of scope — the files

Touched: `.agents/skills/sepmo/binding-manifest.md`, `.agents/skills/sepmo/unit-runbook.md`
(new), `.agents/skills/sepmo/map.md`, `.agents/skills/map.md`, `CLAUDE.md`,
`.agents/skills/critic-critic-critic/SKILL.md` (one sentence), `task/lessons.md`,
`task/mw-6-critic-evidence/` (new, verbatim), `task/map.md`, `pyproject.toml`, `.typos.toml`,
`.agents/skills/check-disk-headroom/SKILL.md` + `map.md`,
`task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md`, `scripts/check_docs_compaction.py`
(one CEILINGS row), `python/repark-parity/tests/test_proc_1_tiered_review.py` (new),
`python/repark-parity/tests/test_dl_4_live_doc_compaction.py` (its fixture carries the new
unit-runbook `CEILINGS` key — a consequence of C-005),
`python/repark-parity/tests/test_dl_5_contract_compaction.py` (DL-5's slate-row pin turned over
at this unit's pickup, when #243 merged without DL-5's departure), this ledger,
`task/ledgers/*/map.md`.

Forbidden: `.github/`, `Cargo.toml`, any crate, AWS, the spine, `references/`, the archive.

## Execution record

_History note (2026-08-25, before push): the branch was rewritten into three commits because the owner-local pre-push hook scans every added patch line and the evidence's unscrubbed banner (and the pre-fix ledger text) sat in cycle-1 patches; cycle commits are therefore cited below by tree, not by hash. The cycle-1 and cycle-2 trees are the ones the Critics attacked; their content is what this branch carries._

**Base green (before any edit).** `make ci` on the base tree (the pickup commit, `main` + the DL-5 pickup)
exited `0` — every gate in the chain clean (`ledger-grammar`, `docs-compaction`, `map-sync`,
`manifest`, ruff, typos, taplo, rust-fmt/clippy/check). Log kept in the session scratch.

**Pins red → green.** The suite command (both runs):

```
PYTHONPATH=python/repark-parity/src uv run --no-project --with pytest --with pyarrow \
  --with 'pydantic>=2.10,<3' pytest python/repark-parity/tests/test_proc_1_tiered_review.py -q
```

- **RED** on the base tree, before the edits: **12 failed** — every clause's pin fails against the
  unedited documents.
- **GREEN** after the edits: **12 passed** — one test per clause, plus `test_ceilings_cover_the_runbook`
  as C-005's second pin.

The twelve tests and their clauses: `test_manifest_carries_the_review_profile_table` (C-001),
`test_light_thresholds_name_the_prose_only_class` (C-002), `test_critic_engine_row_binds_ccc_at_high`
(C-003), `test_standard_pass_states_its_two_obligations` (C-004),
`test_unit_runbook_is_small_and_pointer_only` + `test_ceilings_cover_the_runbook` (C-005),
`test_routing_points_at_the_runbook_once_each` (C-006), `test_ccc_changes_only_its_binding_sentence`
(C-007), `test_lessons_record_the_ruling` (C-008), `test_mw6_evidence_is_home_and_excluded_from_lint`
(C-009), `test_disk_runbook_carries_the_2026_08_25_block` (C-010),
`test_handoff_f7_records_the_unit_3_ruling` (C-011).

**Byte table (cycle 1 — measured this branch after the Actor build; cycle 2's table is in
"Remediation — cycle 2" below).**

| File | Before (the pickup commit) | After |
|---|---:|---:|
| `.agents/skills/sepmo/binding-manifest.md` | 16,044 B | 18,163 B |
| `.agents/skills/sepmo/unit-runbook.md` | 0 B (new) | 3,785 B |
| `.agents/skills/critic-critic-critic/SKILL.md` | 23,769 B | 23,980 B |
| `.agents/skills/check-disk-headroom/SKILL.md` | 6,238 B | 8,274 B |

The runbook sits under its `CEILINGS` seed; CCC grew by one sentence only. (Cycle 2 tightened
that seed to 5,000 B and reworded LIGHT/STANDARD — see below.)

**Attestation deferred to the Critic.** This unit runs Actor-only; the `COVERAGE_ATTESTATION` is
the Critic's artifact, filed at convergence/departure (manifest `review_profile` / `critic_engine`
rows; the DL-2 lifecycle). While it is pending, `make check-ledger-grammar` reports exactly one
finding on this ledger — the missing attestation block, rule C — and nothing else; every other
gate in `make ci` is green. The ledger stays in `staging/`; the departure `move` is not this
unit's act.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-PROC-1-conclude
  agent: Actor
  action: conclude the PROC-1 build — every clause C-001..C-011 made true and pinned, gates run
  charter_trace: C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011
  preconditions:
    - twelve pins RED on the base tree before the edits: SATISFIED (12 failed at the pickup commit)
    - twelve pins GREEN after the edits: SATISFIED (12 passed)
    - base make ci green: SATISFIED (exit 0 before any edit)
    - spine, references, the archived MW-6 ledger, .github, Cargo and crates untouched: SATISFIED (diff scope)
    - copied evidence byte-identical but for the one neutralised home-path token: SATISFIED (cmp clean on 10 of 11; oracle_k2.log +4 B, recorded in the evidence map)
  success_condition: every clause has a passing pins test AND make ci is green but for the Critic's pending attestation
  step_risks:
    - the runbook regrows into a second spine: HANDLED (CEILINGS 5,000 B; test_ceilings_cover_the_runbook)
    - the profile table drifts by restatement: HANDLED (test_routing_points_at_the_runbook_once_each proves single-home)
    - a forbidden home path or session id reaches the tree: HANDLED (forbidden-pattern scan clean; the one home path neutralised)
    - CCC rules or taxonomies changed by accident: HANDLED (test_ccc_changes_only_its_binding_sentence anchors them)
  contingencies:
    - the Critic files the COVERAGE_ATTESTATION at convergence: EXECUTABLE (pre-authorized — the manifest names it the Critic's artifact, filed at departure per DL-2)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## Remediation — cycle 2

The Critic's cycle-1 review (two PR reviews, findings accepted by the Orchestrator) returned ten
findings. Each is REMEDIATED below with its regression proof (pin name, red→green) or a one-line
justification where the fix is prose the ledger does not pin. The three logs' banner scrub and the
manifest/runbook rewordings are the substance; the rest are date, ceiling and record truth-ups.

- **S1 C2-1 (C-009, AT-5) — REMEDIATED.** Neutralised the Spark boot banner on line 3 of
  `oracle_critic.log`, `oracle_k2.log`, `oracle_r2.log`: hostname → `<host>`, both IPs → `<ip>`,
  interface → `<iface>` (−22 B each: 6235→6213, 13140→13118, 1557→1535). Recorded in the evidence
  `map.md` Substitution note with byte deltas. **Proof:** `test_mw6_evidence_is_home_and_excluded_from_lint`
  gained an absence loop over every file under `task/mw-6-critic-evidence/` (no hostname, IP,
  interface, home-dir or user-name token — the six literals live in the pin, which is the
  enforcement) — **RED** with the banner restored (the loop flags the restored hostname on
  `oracle_critic.log`), **GREEN** after the scrub. Whole-diff grep (`git diff the pickup commit) for those
  tokens plus any IPv4 / UUID / `local-…` / `application_…` session id: clean — the only literals
  that remain are the pin's own absence-assertion strings.
- **S1 C3-001 + C3-002 (C-001, C-003, AT-6/AT-1) — REMEDIATED.** Rewrote LIGHT in the manifest
  `review_profile` and `critic_engine` rows to the spine's single **in-line AC cycle** (one
  procedural Critic pass, no fan-out / no external engine) whose `COVERAGE_ATTESTATION` is filed
  with categories `N/A` justified by the recorded rubric result; the Orchestrator may self-run R7.
  Mirrored in `unit-runbook.md` §4 and ledger C-001/C-003; the lessons body ("LIGHT keeps none")
  corrected to match. **Charter amended:** the charter's "bar unchanged on every tier" now literally
  holds — LIGHT keeps the complete attestation (ref 05 *External critic engines* constraint 2;
  SKILL.md *Proportionality*; `check_ledger_grammar` rule C). **Proof:**
  `test_manifest_carries_the_review_profile_table` asserts `"in-line AC cycle"` present and
  `"no Critic stage"` / `"no attestation"` absent; `test_critic_engine_row_binds_ccc_at_high` asserts
  `"in-line AC cycle"` present and `"runs no Critic stage"` absent — **RED** on the old wording,
  **GREEN** after.
- **S3 C3-003 (C-001) — REMEDIATED.** Added the clarifying clause to `review_profile`: the STANDARD
  single-pass checklist is this manifest's lighter review under the spine's own Critic stage, **not
  CCC merging its phases** — CCC's absolute rule 1 governs only the HIGH engine. **Proof:**
  `test_manifest_carries_the_review_profile_table` asserts `"not CCC merging its phases"` and
  `"absolute rule 1"` — **RED** before, **GREEN** after.
- **S3 C3-006 (C-003, C-008) — REMEDIATED (date).** The tiering ruling was given 2026-08-25, not
  08-26. Changed every unit-written `2026-08-26` stamp to `2026-08-25`: manifest provenance,
  `task/lessons.md` header + body, `pyproject.toml` and `.typos.toml` comments,
  `scripts/check_docs_compaction.py` comment, ledger C-003/C-008, the sepmo / skills / task /
  scripts / tests / staging maps, the root map, and the mw-6 evidence map. **Proof:**
  `test_critic_engine_row_binds_ccc_at_high` asserts `"2026-08-26" not in row`;
  `test_lessons_record_the_ruling` asserts `"## 2026-08-25 — PROC-1"` present and `"## 2026-08-26"`
  absent — **RED** on 08-26, **GREEN** on 08-25.
- **S3 C3-004 + C-005 ceiling — REMEDIATED.** Tightened the runbook `CEILINGS` seed 8,000 → 5,000 B
  (script value + comment); updated ledger C-005 and the runbook Pointers line. The DL-4 fixture
  seeds the key with `# runbook\n` and does not name the number, so it needed no change. **Proof:**
  `test_ceilings_cover_the_runbook` asserts `CEILINGS[key] == 5_000`;
  `test_unit_runbook_is_small_and_pointer_only` asserts `st_size <= 5_000` — **RED** at 8,000,
  **GREEN** at 5,000. `make check-docs-compaction` clean (runbook 3,685 B, ~26 % under the seed).
- **S3 C3-005 / C1-2 (C-005/C-006) — REMEDIATED.** Trimmed the three glosses that restated a
  threshold or obligation (the line-3 claim, the `light_thresholds` size rule, the §4 LIGHT/STANDARD
  lines) to name-and-link, and softened the claim from "restates none of them" to "names and links …
  the home is authoritative". **Proof:** `test_unit_runbook_is_small_and_pointer_only` asserts
  `"the home is authoritative"` present and `"restates none of them"` absent — **RED** before,
  **GREEN** after.
- **S2 F-C4-1 / S3 C1-1 — REMEDIATED.** Added `python/repark-parity/tests/test_dl_4_live_doc_compaction.py`
  (its fixture carries the new unit-runbook `CEILINGS` key — a consequence of C-005) and
  `test_dl_5_contract_compaction.py` (DL-5's slate-row pin turned over at this unit's pickup) to the
  Touched list with those reasons. **Justification (no pin):** ledger record-completeness fix;
  verified by inspection — both files appear in the unit's diff.
- **S3 F-C4-2 (C-009) — REMEDIATED.** Tightened the ruff-exclusion assertion from the substring
  `"task/mw-6-critic-evidence"` (satisfied by the neighbouring comment alone) to the literal entry
  line `extend-exclude = ["task/mw-6-critic-evidence"]`; the typos assertion likewise tightened to
  the literal `"task/mw-6-critic-evidence/",`. **Proof (mutation):** emptying the ruff entry to
  `extend-exclude = []` with the comment intact turns the pin **RED**; restoring the entry turns it
  **GREEN**.
- **S3 C2-2 (C-009) — REMEDIATED.** Reworded the evidence `map.md` Substitution note: the
  neutralisation satisfies the repository's content-hygiene classes (`briefs/map.md` "Import gate" —
  no personal identifiers, local absolute paths or session identifiers), enforced locally by the
  **owner-local pre-push hook**; the non-artifact "pre-push forbidden-literal gate" phrasing is gone.
  **Justification (no pin):** map.md prose; verified by grep (`"pre-push forbidden-literal"` absent)
  and `make check-map-sync` clean (152 maps, the new `../../briefs/map.md` cross-link resolves).
- **S3 C2-3 (C-010) — REMEDIATED.** Neutralised the home-dir glob in the ledger's C-010 text to
  `+ /home/<user>/**` (a tracked file). **Justification (no pin):** ledger prose; covered by the
  whole-diff home-path grep (clean — the only literals that remain are the pins' absence-assertion
  strings).

**Byte table (cycle 1 → cycle 2).**

| File | cycle 1 (the cycle-1 tree) | cycle 2 |
|---|---:|---:|
| `.agents/skills/sepmo/binding-manifest.md` | 18,163 B | 18,801 B |
| `.agents/skills/sepmo/unit-runbook.md` | 3,785 B | 3,685 B |
| `.agents/skills/critic-critic-critic/SKILL.md` | 23,980 B | 23,980 B (unchanged) |

The runbook shrank under its new 5,000 B seed; the manifest grew by the LIGHT rewrite plus the
clarifying clause; CCC's binding sentence was not touched this cycle.

**Gates (cycle 2).** `make ci` green through every gate but the one permitted red —
`check-ledger-grammar`: `no COVERAGE_ATTESTATION block` (this ledger, the Critic's artifact, still
pending at STANDARD). The proc-1 + DL-4 + DL-5 pins: 42 passed. The full parity suite (CI command):
392 passed. `make check-docs-compaction` and `make check-map-sync` clean.

## CCC pass — findings and attestation (repo SEPMO, STANDARD under the manifest at base; risk standard)

Cycle 1 attacked the cycle-1 tree with four fresh Opus Critics, each on its own scratch clone (Critic-1
quality + test-coverage skeptic with mutation probes on all twelve pins; Critic-2 safety over the
lint/typos exclusions, the evidence contents and the forbidden-content classes; Critic-3 logic over
the manifest rows against the spine, references/05 and CCC; Critic-4 every written claim against
the tree). Thirteen findings, three S1 — all remediated in cycle 2 (cycle-2 (manifest/runbook/date/ceiling), cycle-2 (evidence scrub),
cycle-2 (pins), the cycle-2 tree) with regression proof. Cycle 2 re-attested on a fresh clone of the cycle-2 tree:
every remediation verified by command, all five claimed mutation probes reproduced, one S2
(`PROC1-CYC2-1`) found and fixed in the departure commit. **Verdict: CONVERGED.**

```yaml
COVERAGE_ATTESTATION:
  pr_unit: proc-1-tiered-review
  cycle: 2
  risk_tier: standard
  critic_engine: ccc
  complete: true
  note: >
    Cycle 1 (the cycle-1 tree): four fresh Critics on scratch clones of the unit branch returned thirteen
    findings, three at S1 — C2-1 (a Spark boot banner leaking the owner's hostname the owner's hostname,
    LAN IPs two LAN/loopback addresses and interface the interface name on line 3 of three evidence logs),
    PROC1-C3-001 (the LIGHT tier dropping the coverage attestation) and PROC1-C3-002 (the LIGHT tier
    reading "no Critic stage", over-reading references/05 constraint 2). The Actor's cycle-2 commits
    (cycle-2 (manifest/runbook/date/ceiling), cycle-2 (evidence scrub), cycle-2 (pins), the cycle-2 tree) remediated all three: the three logs are scrubbed to
    <host>/<ip>/<iface>; LIGHT is rewritten to the spine's single in-line AC cycle whose
    COVERAGE_ATTESTATION is filed with categories N/A justified by the recorded rubric result,
    byte-consistent with references/05-critic.md constraints 2 and its attestation-format LIGHT-path
    note and SKILL.md Proportionality. Every S1 pin was shown red under mutation and green on the
    tree. Cycle 2 (this re-attestation, on a fresh scratch clone of the cycle-2 tree): every cycle-1 finding
    re-verified against the tree with a command; all five claimed mutation probes reproduced (banner
    restore -> C-009 red; "no Critic stage" -> C-001 red; date -> 2026-08-26 -> C-003 + C-008 red;
    emptied ruff extend-exclude entry -> C-009 red, the F-C4-2 fix; CEILINGS 8,000 -> C-005 red);
    the full gate roster clean (check-map-sync, check-docs-compaction, check-manifest, py-lint,
    py-format-check, spell-check, check-python-conventions, check-docstring-presence) with the three
    pin files at 42 passed, and check-ledger-grammar reporting exactly the one expected finding
    (this ledger's pending attestation). One new finding this cycle: PROC1-CYC2-1 (S2, below the S1
    floor) — the ceiling remediation left five stale "8,000 B" restatements while the authoritative
    value is now 5,000 B. No open finding at or above the S1 severity floor.
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        Clause-walked the amended C-001/C-003/C-005 text against the tree: the manifest
        review_profile/critic_engine LIGHT rewrite matches references/05-critic.md constraint 2
        (:195), the attestation-format LIGHT-path note (:110) and SKILL.md Proportionality
        (:460-463). Ran mutation probes P2 (LIGHT reverted to "no Critic stage" -> C-001 red at
        test line 62), P3 (date reverted to 2026-08-26 -> C-003 red at :98 and C-008 red at :212),
        P5 (CEILINGS 5,000 -> 8,000 -> C-005 red at :160). Found a residual logic/consistency
        defect the remediation introduced: the 8,000 -> 5,000 ceiling change left five restatements
        still asserting 8,000 B (finding PROC1-CYC2-1).
      artifacts: [python/repark-parity/tests/test_proc_1_tiered_review.py, .agents/skills/sepmo/binding-manifest.md:59, .agents/skills/sepmo/references/05-critic.md:195, .agents/skills/sepmo/SKILL.md:460, task/ledgers/staging/proc-1-tiered-review-ledger.md:38]
    - id: AT-2
      status: ATTACKED
      evidence: >
        Attacked the hygiene scan for incomplete-match / silent-escape: the C-009 pin's forbidden
        loop uses _EVIDENCE.rglob("*") over every file under the evidence home (test lines 246-251),
        not a hardcoded subset, so no added evidence file can silently escape the identifier check;
        the forbidden tuple is complete (hostname, both IPs, interface, home dir, user name). Probe 1
        confirmed the rglob loop actually catches a restored leak on oracle_critic.log. No silent
        gap found.
      artifacts: [python/repark-parity/tests/test_proc_1_tiered_review.py:245, python/repark-parity/tests/test_proc_1_tiered_review.py:227]
    - id: AT-3
      status: ATTACKED
      evidence: >
        The unit's sole sensitivity surface is identifier hygiene. Scanned the whole unit diff
        (the pickup commit..HEAD) and every file under task/mw-6-critic-evidence/ for hostname, both LAN IPs,
        the loopback, the interface, home paths, user name, IPv4 literals, UUIDs and
        local-.../application_... session ids: clean except the pin's own absence-assertion strings
        and one ledger prose line naming the categories checked. No secret, credential or private
        identifier reaches the tree.
      artifacts: [python/repark-parity/tests/test_proc_1_tiered_review.py:245, task/mw-6-critic-evidence/oracle_critic.log, task/mw-6-critic-evidence/oracle_k2.log, task/mw-6-critic-evidence/oracle_r2.log]
    - id: AT-4
      status: N/A
      justification: A docs-and-pins unit with no shared or session state, no concurrency, no locks and no ordering with any other process; nothing to attack for shared-state or ordering hazards.
    - id: AT-5
      status: ATTACKED
      evidence: >
        Re-attested (remediation locus, the S1 C2-1). Verified the three logs are scrubbed to
        <host>/<ip>/<iface> (0 residual hostname/IP hits), ledger C-010 uses /home/<user>/**, and
        the evidence map.md attributes hygiene to the owner-local pre-push hook rather than a
        non-artifact gate. PROBE 1 (restore banner -> C-009 red at test line 251) and PROBE 4
        (empty the ruff extend-exclude entry, comment intact -> C-009 red at :256) both reproduce;
        each restores green.
      artifacts: [python/repark-parity/tests/test_proc_1_tiered_review.py:221, task/mw-6-critic-evidence/map.md:35, task/ledgers/staging/proc-1-tiered-review-ledger.md:47, pyproject.toml:20]
    - id: AT-6
      status: ATTACKED
      evidence: >
        Re-attested (remediation locus, the S1 C3-001/C3-002). Verified the manifest
        review_profile and critic_engine rows are consistent with the spine: LIGHT = single in-line
        AC cycle with a filed N/A attestation (ref 05 constraint 2 :195 and format LIGHT note :110;
        SKILL.md Proportionality :438-441/:460-463); STANDARD = one checklist pass that is the
        manifest's lighter review, not CCC merging its phases; HIGH = full CCC unchanged. PROBE 2
        (revert to "no Critic stage" -> C-001 red) reproduces. The one manifest-vs-tree consistency
        gap found is the ceiling-restatement drift (folded into PROC1-CYC2-1, AT-1).
      artifacts: [.agents/skills/sepmo/binding-manifest.md:59, .agents/skills/sepmo/references/05-critic.md:110, .agents/skills/sepmo/SKILL.md:440, python/repark-parity/tests/test_proc_1_tiered_review.py:48, python/repark-parity/tests/test_proc_1_tiered_review.py:87]
    - id: AT-7
      status: N/A
      justification: Per the spine's own rule, AT-7 is filed only when the change is system-breaking. This unit changes prose, maps, a ledger, recorded evidence, one gate CEILINGS row and tree pins — no contingency, rollback, orchestration or failure-path behavior — so it is justified N/A.
    - id: AT-8
      status: ATTACKED
      evidence: >
        Ran the full gate roster each alone: check-map-sync (152 maps clean), check-docs-compaction
        (runbook 3,685 B under the 5,000 B seed), check-manifest, py-lint, py-format-check (391
        files), spell-check, check-python-conventions (180 files), check-docstring-presence (159
        files) all green; the three pin files at 42 passed; check-ledger-grammar reports exactly the
        one expected "no COVERAGE_ATTESTATION block" finding and nothing else. Verified the CEILINGS
        gate value is 5,000 and the runbook is under it. Surfaced the stale pin docstring at test
        line 153 (folded into PROC1-CYC2-1).
      artifacts: [python/repark-parity/tests/test_proc_1_tiered_review.py, scripts/check_docs_compaction.py:51, scripts/check_ledger_grammar.py]
    - id: AT-9
      status: N/A
      justification: No failure-path product code exists in this unit — it adds no error-handling, no fallible runtime surface, only documentation and read-only tree-pin assertions; there is no product failure path to drive to its bad branch.
    - id: AT-10
      status: ATTACKED
      evidence: >
        Re-attested (the pins). Established a 42-passed baseline, then ran all five claimed
        mutation probes on the scratch clone, each red-on-mutation and green-on-restore: P1 restore
        banner -> C-009 red (:251); P2 "no Critic stage" -> C-001 red (:62); P3 date 2026-08-26 ->
        C-003 red (:98) + C-008 red (:212); P4 empty ruff extend-exclude entry with comment intact
        -> C-009 red (:256), confirming the F-C4-2 fix now binds the literal entry line not the
        comment; P5 CEILINGS 8,000 -> C-005 red (:160). Tree restored clean (0 modified files) after
        every probe. One pin docstring is stale (line 153, folded into PROC1-CYC2-1) but its body
        assertion is correct.
      artifacts: [python/repark-parity/tests/test_proc_1_tiered_review.py, task/ledgers/staging/proc-1-tiered-review-ledger.md:186]
  reattested: [AT-1, AT-5, AT-6, AT-10]
```

**Attack notes.** AT-1 (logic / consistency). I walked the amended C-001/C-003/C-005 clauses against the tree token by token. The manifest LIGHT rewrite is faithful to the spine, and the C-004 obligations and single-home routing all hold. The one defect the remediation itself introduced is the ceiling drift: the S3 C3-004 fix changed the authoritative CEILINGS from 8,000 to 5,000 B (script, comment, runbook Pointers line, ledger C-005) but left five restatements asserting 8,000 B — two live maps (sepmo/map.md:46, scripts/map.md:88), a test map (tests/map.md:12), a pin docstring (test line 153) and the SLR step_risks line. The diff proves sepmo/map.md, tests/map.md and scripts/map.md were edited in cycle 2 for the date change with the 8,000 line left untouched, so these went newly-wrong as a direct consequence of the fix. I filed PROC1-CYC2-1 at S2 (below the S1 floor: documentation accuracy, no gate or runtime impact, the gate and the C-005 pin both carry the correct 5,000), noting a maintainer reading the map would over-estimate the runbook's headroom by ~3 kB.

AT-2 (silent loss / incomplete matches). I attacked the identifier-hygiene scan for a silent escape hatch. The C-009 pin iterates _EVIDENCE.rglob("*") over every file under the evidence home rather than a fixed list, and the forbidden tuple covers hostname, both IPs, loopback, interface, home dir and user name — so a newly-added evidence file cannot slip past unscanned, and probe 1 confirmed the loop actually reddens on a restored leak. No incomplete-match or silent-loss gap survived.

AT-3 (safety / sensitivity surface). The unit's only sensitivity is machine-local identifiers escaping into recorded evidence. I scanned the whole unit diff and the entire evidence directory for hostnames, the two LAN IPs, the loopback, the interface, home paths, the user name, bare IPv4 literals, UUIDs and local-/application- session ids. The tree is clean except the pin's own absence-assertion literals and one ledger prose line that merely names the IPv4/UUID categories checked — no live identifier remains.

AT-5 (content hygiene — re-attested). This is the S1 C2-1 locus. The three Spark logs now carry <host>/<ip>/<iface> with zero residual hostname/IP hits, ledger C-010 reads /home/<user>/**, and the evidence map attributes enforcement to the owner-local pre-push hook (the C2-2 correction). Probe 1 (restore banner) and probe 4 (empty the ruff exclusion entry with the comment intact) both redden the C-009 pin and restore green — the pin binds the substance, not the surrounding comment.

AT-6 (record vs canon consistency — re-attested). This is the S1 C3-001/C3-002 locus. The manifest review_profile and critic_engine rows now describe LIGHT as the spine's single in-line AC cycle with a filed N/A attestation, STANDARD as one checklist pass explicitly distinguished from CCC merging its phases, and HIGH as full CCC — each consistent with references/05 constraint 2, the attestation-format LIGHT-path note, and SKILL.md Proportionality. Probe 2 confirms the C-001 pin reddens on the old "no Critic stage" wording. The only manifest-vs-tree consistency gap is the ceiling-restatement drift, filed under AT-1.

AT-8 (bug / quality of the pins and gate). I ran the full gate roster each alone — map-sync (152 maps), docs-compaction (runbook 3,685 B under seed), manifest, py-lint, py-format-check (391 files), spell-check, python-conventions (180 files), docstring-presence (159 files) all clean — the three pin files at 42 passed, and check-ledger-grammar returning exactly the one expected pending-attestation finding. The CEILINGS gate value is the correct 5,000 and the runbook is under it. The stale "8,000 B" docstring on test_ceilings_cover_the_runbook is a quality nit folded into PROC1-CYC2-1; its body assertion is correct.

AT-10 (pins / mutation-proofing — re-attested). From a 42-passed baseline I reproduced all five claimed mutation probes on the scratch clone, each red under mutation and green on restore: banner->C-009, "no Critic stage"->C-001, date 2026-08-26->C-003 and C-008, emptied ruff entry->C-009 (confirming the F-C4-2 tightening now binds the literal extend-exclude line, not the comment), and CEILINGS 8,000->C-005. The tree returned to zero modified files after every probe, and the live worktree outside the clone was never touched. Every clause is bound by a pin that genuinely reddens when its guarded fact is removed.

FINDING:
  id: C2-1
  severity: S1
  category: AT-5
  clause: C-009
  disposition: REMEDIATED
  claim: three evidence logs carried the Spark boot banner with the owner's hostname, two LAN IPs and the interface name on line 3
  evidence: git grep at the cycle-1 tree → oracle_critic.log:3, oracle_k2.log:3, oracle_r2.log:3; fix cycle-2 (evidence scrub) scrubs to <host>/<ip>/<iface>, recorded in the evidence map; pin absence loop in test_mw6_evidence_is_home_and_excluded_from_lint — red with the banner restored, green on the tree

FINDING:
  id: PROC1-C3-001
  severity: S1
  category: AT-6
  clause: C-001, C-003
  disposition: REMEDIATED
  claim: the LIGHT tier read 'no attestation', contradicting the spine's every-unit coverage-attestation invariant (SKILL.md Proportionality), R7, ref 05's LIGHT-path note and the tier-independent check_ledger_grammar rule C
  evidence: binding-manifest.md review_profile at the cycle-1 tree; fix cycle-2 (manifest/runbook/date/ceiling) — LIGHT files the attestation with categories N/A justified by the recorded rubric; pin test_manifest_carries_the_review_profile_table asserts 'no attestation' absent

FINDING:
  id: PROC1-C3-002
  severity: S1
  category: AT-1
  clause: C-001, C-003
  disposition: REMEDIATED
  claim: the LIGHT tier read 'no Critic stage', over-reading references/05 constraint 2 ('a LIGHT unit runs the single in-line AC cycle')
  evidence: fix cycle-2 (manifest/runbook/date/ceiling) — LIGHT is the spine's single in-line AC cycle, no external engine; pin asserts 'in-line AC cycle' present and 'no Critic stage' absent — red on the old wording

FINDING:
  id: F-C4-1
  severity: S2
  category: AT-1
  clause: C-005, C-009
  disposition: REMEDIATED
  claim: the charter's Touched list and the Execution record omitted test_dl_4_live_doc_compaction.py (fixture seeded with the new CEILINGS key) and test_dl_5_contract_compaction.py (slate-row pin turned over at pickup)
  evidence: git diff --stat the pickup commit..HEAD; fix the cycle-2 tree names both with reasons (F-PROC1-C1-1 is the same finding from Critic-1)

FINDING:
  id: PROC1-CYC2-1
  severity: S2
  category: AT-1
  clause: C-005
  disposition: REMEDIATED
  claim: the cycle-2 ceiling tightening (8,000 → 5,000 B) left five restatements at 8,000 B in two maps, the scripts map, a pin docstring and the SLR risk line
  evidence: sepmo/map.md:46, tests/map.md:12, scripts/map.md:88, test_proc_1_tiered_review.py:153, ledger:132; fixed in the departure commit; test_ceilings_cover_the_runbook already binds the authoritative 5_000

FINDING:
  id: F-C4-2
  severity: S3
  category: AT-10
  clause: C-009
  disposition: REMEDIATED
  claim: the ruff-exclusion sub-assertion was satisfied by the pyproject comment alone
  evidence: mutation: entry deleted, pin stayed green, make py-lint red; fix cycle-2 (pins) asserts the literal extend-exclude entry — red with the entry emptied

FINDING:
  id: PROC1-C3-003
  severity: S3
  category: AT-1
  clause: C-001
  disposition: REMEDIATED
  claim: CCC's added sentence sat in unreconciled tension with its absolute rule 1 for a reader landing on the rules
  evidence: fix cycle-2 (manifest/runbook/date/ceiling) — review_profile states the STANDARD checklist is the manifest's lighter review under the spine's own stage, not CCC merging its phases; pinned

FINDING:
  id: PROC1-C3-004
  severity: S3
  category: AT-1
  clause: C-005
  disposition: REMEDIATED
  claim: the runbook CEILINGS seed (8,000 B) was ~2.1× the file, unlike every other row
  evidence: fix cycle-2 (manifest/runbook/date/ceiling) — 5,000 B; test_ceilings_cover_the_runbook asserts == 5_000, red at 8,000

FINDING:
  id: PROC1-C3-005
  severity: S3
  category: AT-1
  clause: C-005, C-006
  disposition: REMEDIATED
  claim: three runbook lines restated rule content; the line-3 claim 'restates none of them' overstated (F-PROC1-C1-2 is the same finding from Critic-1)
  evidence: fix cycle-2 (manifest/runbook/date/ceiling) — glosses trimmed to name-and-link; claim softened to 'names and links; the home is authoritative'; pinned

FINDING:
  id: PROC1-C3-006
  severity: S3
  category: AT-1
  clause: C-003, C-008
  disposition: REMEDIATED
  claim: the ruling was stamped 2026-08-26; it was given 2026-08-25
  evidence: fix cycle-2 (manifest/runbook/date/ceiling) — every unit-written stamp is 2026-08-25; C-003/C-008 pins red on 08-26

FINDING:
  id: C2-2
  severity: S3
  category: AT-5
  clause: C-009
  disposition: REMEDIATED
  claim: the evidence map credited a 'pre-push forbidden-literal gate' that is not a repository artifact
  evidence: fix cycle-2 (evidence scrub) — the basis is the content-hygiene classes in briefs/map.md 'Import gate', enforced by the owner-local pre-push hook

FINDING:
  id: C2-3
  severity: S3
  category: AT-5
  clause: C-010
  disposition: REMEDIATED
  claim: the ledger's C-010 text carried a literal home path
  evidence: fix the cycle-2 tree — neutralised to /home/<user>
