# U-13 ICE-V2-LABEL-1 — the format-v2 PRODUCTION label     grade: C (clerk)   engine band: Haiku 5.5 xhigh or Muse xhigh clerk; one Muse MAX verifier   release: 1.5.6

Drafted by the orchestrator (Claude Fable 5.1) on 2026-10-26 from the 2026-10-13 closeout plan, the 2026-10-20 priorities (row 7), the production-grade definition (`docs/production-grade.md`, #1044) and the day's merges. Opens the moment #1191 (U-11) is on main. Date of record: v2 label Tue 2026-11-10, buffer 11-12.

## 0. Why, and what is out of scope

The v2 list is complete on main: boundary and secrecy (PA-01 to PA-04), the silent-value writes (U-8, EXPLAIN-DML-WRITE-1, ZONE-SEAM-1, GATE-RETIRE-1, WRITE-SPINE-1 steps 1 to 3), integrity under failure (U-9: K, U and X proven, live nightly green since 10-24), a second engine reading what we write (the nightly cross-read, 768 rows EQUAL on 10-26), and the stated resource bound (U-11). The two gates outside the list hold: parity-live green every morning since 10-21; aws-acceptance green 10-24, 10-25, 10-26; v1.5.5 tagged and on PyPI. This unit writes the label and nothing else: no product code, no new test, no new measurement beyond the one matrix run below.

Out of scope: v3 (V3-LABEL-1 follows as its own PR, decision D5); WRITE-SPINE-1 step 4; NESTED-LITERAL-SEAM-1; the nanosecond rows (they carry dated rows on the v3 label).

## 1. Rulings already made

- A label moves when every row is green on main with a dated pin or carries a dated owner carve-out; a silent value disqualifies and no carve-out covers one; a refusal with a hint does not (10-13 plan, adopted).
- Carve-out rows the label carries, each signed by the owner: ENC-1 items 1 and 5 (parked under ruling 121(b); item 1's harm on main is an unreferenced orphan file never served; rebuild in 1.5.7) and ENC-CTAS-REPLACE-1 (dated row 2026-10-15 under ruling 98(a)).
- Two green parity mornings before the label PR (10-17 decision D5): met and exceeded.
- The acceptance run id the label cites: the most recent green aws-acceptance run on the day the PR opens.
- The text-leaf leniency (102 census cells, the card's 2026-10-25 census at `d45eb72d`, where a string into a nested timestamp stores where Spark refuses) is a dated row on the v2 label, fix in 1.5.7 (owner, 2026-10-26).
- STATUS.md sits at 24,991 bytes of its 25,000-byte compaction ceiling: the maturity section lands only with a compaction of older prose of at least its own size; the gate `make check-docs-compaction` decides.

## 2. Steps (each its own commit; comment gate before each; docs only)

0. **The matrix, once, on the label's main**: the 842-cell scoreboard harness (the 2026-09-27 and 2026-10-13 runs, re-run 2026-10-26 on d114a2cf: 705 EQUAL / 132 SPARK-CANNOT / 5 REFUSED-REGISTERED / 0 DIFFERENT), run once more on the main that carries #1191; the orchestrator runs it (it is a shell chain, not model work) and hands the clerk the family tallies and the main sha. A DIFFERENT cell halts the unit and the owner hears first.
1. **Registry re-date** (`docs/spark-sql-iceberg-parity.md`): every row the v2 list names carries its dated status as of the label's main; the three carve-out rows get their dated label line; the text-leaf leniency row is filed (NESTED-TEXT-LEAF-STORE-1's card is the link).
2. **STATUS maturity section**: a per-capability maturity line per the definition's three levels, citing tests: format-v2 reads and writes PRODUCTION (with the five review questions answered by pointer: the parity doc rows, the integrity ledger, the bound record, the cross-read nightly, the carve-outs), format-v3 core HARDENED-NARROW until V3-LABEL-1, nanosecond columns and struct writes EXPERIMENTAL as rated 10-13. Compaction of older STATUS prose by at least the section's size; the 25,000-byte gate green.
3. **The label line and its evidence block** in the order's outcome: the matrix sha and tallies, the parity-live run ids of the two mornings before the PR, the acceptance run id, the merged PR numbers of every v2 unit, and the owner's signature line for the carve-outs (left for the owner to fill on the PR).
4. **Release notes**: the 1.5.6 draft's label line.
5. Maps, ledger `task/ledgers/staging/ice-v2-label-1-ledger.md` (one PROVEN row per list item citing its pins, one row per carve-out citing its ruling), card status lines, `make check-docs-links check-docs-compaction check-ledger-grammar check-ledgers`, `python3 scripts/sync_map_md.py --check`.

## 3. Gates

CI green before the verifier; one Muse MAX verifier reads every cited pin and run id against the tree and GitHub (every `test -e`, every `gh run view`), and attacks the maturity section's claims against the definition's disqualifiers. Merge on the owner's word after the owner signs the carve-out rows on the PR.

## 4. Halt rules

A DIFFERENT cell on the matrix; a cited pin that does not exist; a parity-live or acceptance red on the morning the PR opens; the compaction gate red with no older prose left to compact; any need for a product change.

## 5. Hand-back

One JSON object: `status`, `head`, `commits`, `matrix` (sha, tallies), `runs` (parity-live ids, acceptance id), `rows` (re-dated, carve-outs, new), `gates`, `questions`.

## Outcome (2026-10-26)

**Label:** format-v2 Iceberg reads and writes: **PRODUCTION**, on main `59d104d9bc9e54da0659c2295277c98b56a2a699` (ICE-V2-LABEL-1, 2026-10-26). The label moves on the owner's signature of the four carve-out lines below, given 2026-10-27 on PR #1195 (section 1).

**Step-0 facts (orchestrator, measured; copied, never re-measured):**

- Matrix main sha: `105d4625883fa10639c938a99cf411b9e1d26647` (re-run 2026-10-27, scoreboard `2026-10-27-label`; the main that carries #1196 and #1197). The step-0 run was on `59d104d9bc9e54da0659c2295277c98b56a2a699`, the main that carries #1191, and stays as the first record.
- 842 cells: 705 EQUAL / 132 SPARK-CANNOT / 5 REFUSED-REGISTERED / 0 DIFFERENT (families: PROPS 50/5/0, DDL 166/38/1, PUSHDOWN 9/0/0, WRITE 152/25/1, READ 122/22/2, PROC 109/30/0, EDGE 19/2/0, V3-LINEAGE 14/0/0, FUNCTIONS 15/0/0, VIEWS 9/1/0, TYPES 26/5/1, CATALOG 14/4/0 as EQUAL/SPARK-CANNOT/REFUSED-REGISTERED). Zero DIFFERENT. Identical family tallies to the 2026-09-27, 2026-10-13 and 2026-10-26 (d114a2cf) runs.
- Merged v2 PRs today: #1189 (git subject: WRITE-SPINE-1 step (2), the plan-time conform stage), #1190 (git subject: the v3 core label plan, `d114a2cf`), #1193 (DEPS-1), #1194 (U-4), #1192 (NS-TRUTHUP-1), #1191 (U-11). Corrected 2026-10-27 to the git subjects; the earlier label 'WRITE-SPINE-1 step 3 records' was wrong.
- parity-live run ids: 40287530976 (2026-10-26), 40172850047 (2026-10-25), 40074142767 (2026-10-24), all success. aws-acceptance run id: 40295982749 (2026-10-26, success; 40178125415 on 10-25 and 40143628929 on 10-24 also success).

**Clerk check of the merged list (from `git log` on main):** #1190 is commit `d114a2cf`, whose subject is "docs: the v3 core label plan adopted 2026-10-26, with the NS-TRUTHUP-1 and ICE-TSNS-UPDATE-RANGE-CLASS-1 orders". No merged subject carries "WRITE-SPINE-1 step 3 records"; the step-3 work is #1182 (steps 1 and 3a) and #1185 (step 3b).

**Merged PR list (every v2 unit, by main commit):**

| Unit | PR | Merge commit |
|---|---|---|
| PA-LOCAL-FS-POLICY-1 (audit PA-02, PA-03, PA-04) | #1075 (not in the subject) | `14ad3c6d` |
| PA-DBT-MISMATCH-REDACT-1 (audit PA-01) | not in the subject | `444dacfe` |
| ICE-TSNS-TYPE-PLANNER-1 (R-010, R-012, R-017) | #1082 (not in the subject) | `c7121437` |
| ICE-NTZ-NESTED-REFUSE-1 (U-8) | #1124 (record #1136) | `c91f69c5` |
| ZONE-SEAM-1 | #1139 | `3bc2915a` |
| EXPLAIN-DML-WRITE-1 | #1137 (P2 report #1138) | `b7a20167` |
| GATE-RETIRE-1 | #1187 | `cf6641a4` |
| WRITE-SPINE-1 step (0) | #1181 | `d50e694a` |
| WRITE-SPINE-1 steps (1) and (3a) | #1182 | `6bd98bc8` |
| WRITE-SPINE-1 step (3b) | #1185 | `e91dd8fa` |
| WRITE-SPINE-1 step (2) | #1189 | `9fd60149` |
| ICE-INTEGRITY-PINS-1, pin K | #1142 | `dc462ccb` |
| ICE-INTEGRITY-PINS-1, pins U and X | #1157 | `c874ab09` |
| ICE-INTEGRITY-PINS-1, legs U and X and the cross-read step wired | #1163 | `75b9e43b` |
| ICE-INTEGRITY-PINS-1, live fix rounds 2 to 4 | #1166, #1167, #1170 | `80e60ebb`, `1aeb1445`, `2d069e8b` |
| Cross-read diagnostics and records | #1172 to #1176 | `bc30f341`, `e765f897`, `ad0018b6`, `0b780f3a`, `e4f8d36b` |
| AWS-EPHEMERAL-RUNS-1 (self-contained runs; S3 Tables through the REST catalog) | #1180 (with #1183, #1184; card #1177) | `c2b90513` |
| ICE-SPARK-CROSS-READ-1 (U-10), test commit | not in the subject | `d96028ee` |
| ICE-RESOURCE-BOUND-1 (U-11) | #1191 | `59d104d9` |
| NS-TRUTHUP-1 | #1192 | `69b7375f` |
| ICE-TSNS-UPDATE-RANGE-CLASS-1 (U-4) | #1194 | `a8c51551` |
| DEPS-1 | #1193 | `42e43214` |
| ICE-REPLACE-KILL-DOORS-1 (local-catalog replace kill pins K-8 to K-10) | #1196 | `c3e634a2` |
| NESTED-LITERAL-SEAM-1 (map keys on the zone seam; NESTED-MAP-KEY-ZONE-1) | #1197 | `105d4625` |

**Holds, ruled 2026-10-27 (owner, adopting all three leans):** NESTED-MAP-KEY-ZONE-1 (ledger C-020, superseded by C-027): fixed first, folded into NESTED-LITERAL-SEAM-1, which opens now as the next product lane; the label re-dates on the main that carries it. The hadoop hint window (C-021 and C-026, superseded by C-028): declared on the format-v2 label; the path read walks past the hint as owner option 1 says, in its own unit (card ICE-HADOOP-HINT-WINDOW-1). The replace kill doors (C-024, superseded by C-029): card ICE-REPLACE-KILL-DOORS-1 plus a small pin unit; declared on the label until it lands. The label closes on the owner's signature after the matrix re-runs on the main that carries NESTED-LITERAL-SEAM-1 and ICE-REPLACE-KILL-DOORS-1 (ledger C-032). **Other open items, not closed by this outcome:** the carve-outs below; NESTED-TEXT-LEAF-STORE-1 (fix 1.5.7); the PA-LOCAL-FS-POLICY-1 closeout verifier, ledger clause C-018 (OPEN); the ICE-INTEGRITY-PINS-1 ledger clauses still read OPEN where the registry records live proof (C-012, C-013, C-059, C-060, C-068 to C-070). **Re-dated 2026-10-27:** both lanes are merged (#1196 at `c3e634a2`, #1197 at `105d4625`) and the matrix re-run on `105d4625` is identical; the label now closes on the owner's signatures (ledger C-038). The replace residuals and the hadoop window stay declared on the label (ledger C-035 and C-028).

**STATUS Last-updated line:** it keeps its 2026-09-12 text, because `python/repark-parity/tests/test_v1_gate_docs.py` pins it; the Iceberg maturity section carries its own date (2026-10-26).

**Owner signature (carve-outs), given 2026-10-27 on PR #1195:**

- Signed by the owner 2026-10-27 (ICE-V2-LABEL-1 PR #1195): ENC-1 item 1, ruling 121 (b).
- Signed by the owner 2026-10-27 (ICE-V2-LABEL-1 PR #1195): ENC-1 item 5, ruling 121 (b).
- Signed by the owner 2026-10-27 (ICE-V2-LABEL-1 PR #1195): ENC-CTAS-REPLACE-1, ruling 98 (a).
- Signed by the owner 2026-10-27 (ICE-V2-LABEL-1 PR #1195): NESTED-TEXT-LEAF-STORE-1, owner ruling 2026-10-26.

The label closes on the merge of PR #1195. The owner's words on 2026-10-27: "I sign all four, merge #1195 and #1198."

**Withdrawn pending (2026-10-28, owner ruling "proceed with your recommendations"; independent audit):** the format-v2 PRODUCTION label is withdrawn pending ICE-HADOOP-HINT-WINDOW-1 on AUD-001, a silent wrong value on the `type=hadoop` path read after a committed DELETE, which section 1's rule excludes from carve-outs. It re-dates on that unit's merge. The audit is filed at `docs/audits/iceberg-v2-label-independent-audit-2026-10-28.md` and names AUD-002 to AUD-005: ICE-POST-PUBLISH-OUTCOME-1, ICE-DELETE-FORMAT-GUARD-1, F-POSDEL-FORMAT-READ-1 and F-EQDEL-DROPPED-KEY-1 are dispatched on 2026-10-28; CROSS-READ-GATE-CLASS-1 and DEP-AUDIT-0414-1 are filed, not dispatched. The audit's section 5 gate dimensions are carried to the format-v3 label as its gate input. Ledger rows C-043 to C-052.

**Re-dated 2026-10-29 (owner ruling of 2026-10-28, on the merge of ICE-HADOOP-HINT-WINDOW-1):** that unit merged as PR #1205, squash `9ab67d7d`, on 2026-10-29 (UTC), after an Opus 5.5 verifier PASS and a Muse MAX critic PASS on the code at `564285cb`. AUD-001 is fixed on main: the path read of a `type=hadoop` table walks past `version-hint.text` to the newest metadata file, as Java does. The format-v2 PRODUCTION label re-dates today on main `9ab67d7d`. The carve-outs (ENC-1 items 1 and 5, ENC-CTAS-REPLACE-1, NESTED-TEXT-LEAF-STORE-1) and the declared replace residuals stand as signed 2026-10-27; the hadoop hint window is no longer declared, it is fixed. AUD-002 to AUD-005 remain dated rows with their units (PR #1206 open for AUD-002 and the AUD-003 guard; fork PRs #375 and #376 for AUD-003 and AUD-004; CROSS-READ-GATE-CLASS-1 filed). The compressed-codec metadata names Java also walks stay an open row of the unit ledger (ice-hadoop-hint-window-1 C-007), outside this label's gate. Ledger rows: ice-v2-label-1 C-054 (the re-date; it closes C-022, C-028, C-038, C-045 and C-053) and the dated VERDICT beside it.
