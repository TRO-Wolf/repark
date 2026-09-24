# report-xo-opus65 — run 29 lane D: U4 DDL-SHOW-DESCRIBE close-out (DONE, 06:31 EDT 2026-09-24)

## FINAL (tick 121, 06:31)
All three open PRs are merged. Nothing is left open. Every cell is EQUAL, a refusal that matches Spark, or a numbered residue.

### PRs (all merged TREE-EQUAL to the lane head that passed the critic)
- repark#810 U4-C SHOW CREATE TABLE: merged 20:47 (2026-09-23). Critic C r9 Sol PASS at 7131b2e9.
- repark#816 U4-A SHOW TABLE EXTENDED: merged 04:29, main 970ac11a. Critic A r11b Sol PASS at 42616a49.
- repark#813 U4-B DESCRIBE column / EXTENDED / owner stamp / CREATE default props: merged 06:24, main d4caca39, head 2733d6c4. Main tree 045e0b2c equals the lane-head tree. Critic B r3 Sol PASS at 2733d6c4 (51 tools). Local gate CB=0 R=0 T=0 U=0 L=0, lint all 0, CI green (9 SUCCESS, 2 SKIPPED), comment gate 0.

### Cells: before (main 88b6f59f) -> after (main d4caca39)
Main's tree equals each merged lane head, so the lane replays (c-lane, a-lane, b-lane3) count as the main replay.
| cell | before | after | PR |
|---|---|---|---|
| D-SHOW-CREATE | REFUSED-UNREGISTERED | EQUAL | #810 |
| D-SHOW-CREATE-PLAIN | REFUSED-UNREGISTERED | EQUAL | #810 |
| D-SHOW-TABLE-EXTENDED | REFUSED-REGISTERED | EQUAL | #816 |
| D-DESCRIBE-COLUMN | NOT-PARSED | EQUAL | #813 |
| D-DESCRIBE-COLUMN-PLAIN | NOT-PARSED | EQUAL | #813 |
| D-DESCRIBE-EXTENDED | DIFFERENT | EQUAL | #813 |
| D-CREATE-DEFAULT-PROPS | DIFFERENT | EQUAL | #813 |
| D-DESCRIBE-VERSION | DIFFERENT | SPARK-CANNOT: both refuse with PARSE_SYNTAX_ERROR near 'OF', 42601 (matching refusal) | #813 |
| D-SHOW-TBLPROPERTIES | REFUSED-REGISTERED | REFUSED-REGISTERED DBT-TBLPROPS-1 = residue R-U4-20 | — |
| D-SHOW-TBLPROPERTIES-KEY | REFUSED-REGISTERED | REFUSED-REGISTERED DBT-TBLPROPS-1 = residue R-U4-20 | — |
Totals: 0/10 EQUAL before -> 7/10 EQUAL + 1 matching refusal (Spark cannot run it either) + 2 residue.

### Final residue list (R-U4-7 was fixed in WO-B14 and is dropped; R-U4-9 was never assigned)
- R-U4-1 D-SHOW-CREATE front door: `SELECT 1 AS \`/* x` (unterminated backtick holding /*). Spark gives UNCLOSED_BRACKETED_COMMENT/42601; RePark gives TokenizerError text.
- R-U4-2 D-SHOW-CREATE / D-SHOW-TABLE-EXTENDED front door: `SELECT 1 --c\r/* x` and `-- ...\r` before SHOW TABLE EXTENDED. Spark gives UNCLOSED_BRACKETED_COMMENT/42601; RePark serves the query (sqlparser ends `--` only at \n).
- R-U4-3 D-DESCRIBE-COLUMN: `DESCRIBE cat.ns.t.snapshots snapshot_id`. Spark returns info rows; RePark gives 42P01 on the four-part name.
- R-U4-4 D-DESCRIBE-*: lexer-level near misses (AS JSON without EXTENDED, 1e, 1.2e, $x, 'a'', `a``, '-- x, '/* x, DESC SELECT..VERSION AS OF, `; SELECT 1`). The list is in B10 handback Q3.
- R-U4-5 DESCRIBE NAMESPACE/FUNCTION/QUERY ... 'x and DESCRIBEX t 'x: TokenizerError text outside DESCRIBE table (IPI-51).
- R-U4-6 D-CREATE-DEFAULT-PROPS neighbour: ALTER TABLE SET/UNSET TBLPROPERTIES ('owner') succeeds. Spark gives UNSUPPORTED_FEATURE.SET_TABLE_PROPERTY 0A000.
- R-U4-8 D-DESCRIBE-COLUMN facade: bare-name tails with comments or dots (DESCRIBE dc /* c */ id, DESCRIBE dc s.a) give raw ParserError text.
- R-U4-10 D-SHOW-TABLE-EXTENDED neighbour: the TRUNCATE parse refusal gets an AnalysisException 'Error during planning:' wrapper and a near identifier where Spark gives a ParseException.
- R-U4-11 INVALID_NUMERIC_LITERAL_RANGE: short text with SQLSTATE None, where Spark gives the full text with 22003.
- R-U4-12 D-DESCRIBE-EXTENDED: DESCRIBE t PARTITION(...) gives legacy 1111 with an 'Error during planning:' prefix and getCondition None.
- R-U4-13 D-SHOW-TABLE-EXTENDED: PARTITION (a=1 b=2). Spark gives PARSE_SYNTAX_ERROR near b; RePark gives PARTITION_MANAGEMENT_IS_UNSUPPORTED.
- R-U4-14 D-SHOW-TABLE-EXTENDED: PARTITION (a b). Spark adds ": extra input 'b'"; RePark gives only the head line.
- R-U4-15 D-SHOW-TABLE-EXTENDED: PARTITION (a==1). Spark accepts ==; RePark refuses near ==.
- R-U4-16 D-SHOW-TABLE-EXTENDED: the rows lack Spark's Owner: line. #813's owner stamp covers DESCRIBE only.
- R-U4-17 D-SHOW-TABLE-EXTENDED: IN 'x' LIKE ... Spark gives PARSE_SYNTAX_ERROR near ''x''; RePark gives SCHEMA_NOT_FOUND.
- R-U4-18 D-SHOW-TABLE-EXTENDED: IN LIKE 'pc'. Spark adds ": missing 'LIKE'".
- R-U4-19 D-SHOW-TABLE-EXTENDED: SHOW TABLE EXTENDEDX. Spark gives PARSE_SYNTAX_ERROR near EXTENDEDX; RePark gives the generic SHOW refusal.
- R-U4-20 D-SHOW-TBLPROPERTIES, D-SHOW-TBLPROPERTIES-KEY: REFUSED-REGISTERED DBT-TBLPROPS-1. The A2 table branch was never started because it waits on the parser owner (xo-opus59 PR4).

### Rounds per tier (all build and fix rounds after hand-off ran on opus)
- C: C10 terra (inherited), C11 opus. Critic Sol r8 NEEDS_REMEDIATION, r9 PASS.
- A: A7 luna (inherited), then A8–A23 on opus (A10 ended without a hand-back; A10b, A12 and A14 halted and were ruled). Critic Sol r4, r5, r6 NEEDS_REMEDIATION, r7 PASS, r8 NEEDS_REMEDIATION, r9 PASS, r10 NEEDS_REMEDIATION, r11 read-only VOID-like, r11b PASS.
- B: B9–B15 on opus (B9, B10 and B11 halted and were ruled). Critic Sol r1 NEEDS_REMEDIATION, r2 PASS, r3 PASS (final head).
- Critic finding classes: WRONG ERROR KIND, UNTESTED BRANCH, GATE SCOPE, BASELINE TWIN DRIFT, UNDER-PINNED NEAR MISS, UNTESTED SCANNER BRANCH, SUPERSEDED CLAIM, PARTIAL ERROR PIN, OWNER STAMP.

### Clones
/tmp/xd-create and /tmp/xr-xd-create were removed at 20:47. /tmp/xd-props and /tmp/xr-xd-props were removed at 04:29. /tmp/xd-show (81G) and /tmp/xr-xd-show were removed at 06:31. The lane owns no clones.

---
## History (tick log)

## PRs
- repark#810 U4-C SHOW CREATE TABLE (lane xd-create): critic r9 Sol PASS at 7131b2e9 (rebased on main 3383a542); gate+lint running; not queued yet.
- repark#816 U4-A SHOW TBLPROPERTIES / SHOW TABLE EXTENDED (xd-props): restacked on 7131b2e9; A9 opus docs repair -> cae8907b; critic A r4 Sol = NEEDS_REMEDIATION (V-001 PARTIAL ERROR PIN: literal-error tests pin first line only; V-002 UNTESTED SCANNER BRANCH: doubled-delimiter branch) -> WO-A10 opus fix + class sweep running (20:17); gate at cae8907 stopped; not pushed yet.
- repark#813 U4-B DESCRIBE column/EXTENDED/VERSION + CREATE default props (xd-show): WO-B10 opus (restack + class sweep) running.

## Cells before (scoreboard 2026-09-23, main 88b6f59f)
D-SHOW-CREATE REFUSED-UNREGISTERED · D-SHOW-CREATE-PLAIN REFUSED-UNREGISTERED · D-SHOW-TBLPROPERTIES REFUSED-REGISTERED ·
D-SHOW-TBLPROPERTIES-KEY REFUSED-REGISTERED · D-SHOW-TABLE-EXTENDED REFUSED-REGISTERED · D-DESCRIBE-COLUMN NOT-PARSED ·
D-DESCRIBE-COLUMN-PLAIN NOT-PARSED · D-DESCRIBE-EXTENDED DIFFERENT · D-DESCRIBE-VERSION DIFFERENT · D-CREATE-DEFAULT-PROPS DIFFERENT
Replay harness: /tmp/xo-xo-opus65/replay/replay.sh <venv> <tag> <ids>.

## Rounds per tier
- C: C10 terra (inherited from xo-opus61), C11 opus. Critic C: r8 Sol NEEDS_REMEDIATION (UNDER-PINNED NEAR MISS / UNTESTED SCANNER BRANCH / SUPERSEDED CLAIM), r9 Sol PASS (48 tools).
- A: A7 luna (inherited), A8 opus, A9 opus, A10 opus (running). Critic A: r4 Sol NR at cae8907b (2 findings). Orchestrator note: UNTESTED SCANNER BRANCH repeated from critic C r8 — WO-A8 lacked that sweep; A10 carries it.
- B: B9 opus (halted, ruled 19:20), B10 opus (running).

## Rulings / questions
- 19:59 RULING U4-C Q1-Q5 (run29 claims), measured on Spark 4.1.2 (/tmp/xo-xo-opus65/wo/spark-4.1.2-c11-measured.json).

## Residues
- R-U4-1 (cell family D-SHOW-CREATE front door): `SELECT 1 AS \`/* x` — Spark UNCLOSED_BRACKETED_COMMENT/42601; RePark raw TokenizerError. Main answers the same; not a regression.
- R-U4-2 (same): `SELECT 1 --c\r/* x` — Spark UNCLOSED_BRACKETED_COMMENT/42601; RePark serves 1 row (sqlparser ends -- only at \n).
- A2 SHOW TBLPROPERTIES on a table branch: not started, waits on xo-opus59 PR4 (parser owner) — likely residue.

### 20:20 tick 18 — #810 pushed at 7131b2e9
- Local gate at 7131b2e9: CB=0 R=0 T=0 U=0 L=0. Lint at 7131b2e9: clippy, panic-ban, ruff check+fmt, ledger grammar+lifecycle, map-sync, check-map, file-size all 0.
- Critic C r9 Sol PASS at 7131b2e9 (48 tools). PR #810 force-pushed to 7131b2e9 (on main 3383a542); body refreshed (Round 9). CI running on the new head.
- Replay of the C cells (D-SHOW-CREATE, D-SHOW-CREATE-PLAIN) on the lane head started (tag c-lane).

## Tick 19 (20:27)
C lane replay: D-SHOW-CREATE EQUAL, D-SHOW-CREATE-PLAIN EQUAL (main before: not EQUAL). #810 waits only on CI at 7131b2e9. A10 opus launching (cargo-lock wait), B10 opus running.

## Tick 23 (20:43)
- #810 CI green at 7131b2e9; critic r9 Sol PASS current; gate 0; CB 0; replay EQUAL x2 -> queued 20:43, drive-merge merge-810-204304.

## Tick 24 (20:47) — U4-C MERGED
- repark#810 "SHOW CREATE TABLE answers Spark 4.1.2's text for Iceberg tables" merged 20:43 EDT as main 458718b5 (drive-merge: TREE-EQUAL to the lane head 7131b2e9, so the lane replay is the main replay).
- Cells: D-SHOW-CREATE REFUSED-UNREGISTERED -> EQUAL; D-SHOW-CREATE-PLAIN REFUSED-UNREGISTERED -> EQUAL (2/2 EQUAL; logs /tmp/xo-xo-opus65/replay/logs/verdicts-c-lane.txt).
- Rounds: C10 terra (inherited), C11 opus; critic Sol r8 NR, r9 PASS at the merged head.
- Clones /tmp/xd-create and /tmp/xr-xd-create removed.

## Tick 28 (21:18) — U4-B B10 hand-back ruled
B10 (opus, 167 turns) HALT at 41e78b31 with 6 commits, 17 gates 0. Rulings (claims 21:22): Q1/Q3a,b/Q6 fixed in WO-B11 (opus resume, launched 21:17).
Residues declared from B10's Spark 4.1.2 measurements (/tmp/xo-xo-opus65/wo/spark-4.1.2-b10-measured.json):
- R-U4-3 `DESCRIBE cat.ns.t.snapshots snapshot_id`: Spark info rows; RePark TABLE_OR_VIEW_NOT_FOUND 42P01.
- R-U4-4 lexer-level near misses of the column-tail arm (AS JSON without EXTENDED, `1e`, `1.2e`, `$x`, `'a''`, `` `a`` ``, `'-- x`, `'/* x`, DESC SELECT … VERSION AS OF, `id; SELECT 1`).
- R-U4-5 TokenizerError text outside the DESCRIBE table form (IPI-51).
- R-U4-6 ALTER TABLE SET/UNSET TBLPROPERTIES ('owner') succeeds; Spark UNSUPPORTED_FEATURE.SET_TABLE_PROPERTY/0A000.
- R-U4-7 owner refusal exception class AnalysisException vs Spark ParseException.
- R-U4-8 facade bare-name column tails with comments or dotted paths answer raw ParserError text.

## Tick 30 (21:45) — A10b hand-back, A rebased, A11 launched
- A10b (opus resume) HALT, 3 test-only commits; the stray unbalanced_delimiter diff was a stranded mutant, reverted. Gates all 0.
- xd-props rebased onto main 458718b5 (C merged) -> head 2bbf67ba, clean, no skip-worktree.
- Rulings (claims 21:45): Q1 delete equivalent branch; Q2 nested '(' in PARTITION spec refuses near '(' (Spark P2/P3); Q3 -> R-U4-10;
  Q4 -> R-U4-11; Q5 folds into R-U4-2.
- A11 opus (--resume 111f601e) launched 21:41, WO /tmp/xo-xo-opus65/wo/WO-A11-scanner-q1q2.md.
- New residues: R-U4-10 TRUNCATE parse refusals are AnalysisException with 'Error during planning:' prefix and near identifier (Spark
  ParseException near 'EXISTS'/'ice'); R-U4-11 INVALID_NUMERIC_LITERAL_RANGE short message + None SQLSTATE (Spark full text, 22003).

## Tick 35 (22:24) — B11 hand-back
- B11 (opus, --resume 72982668) HALT with 4 commits, head 055fcfbe: B10-Q1 regression fixed (execute_calibrated skips the metadata rewrite when #806's parser claims the four-part DESCRIBE), `DESCRIBE t PARTITION (...)` answers _LEGACY_ERROR_TEMP_1111 after the table loads, UNRESOLVED_COLUMN re-escapes backticks with Spark's suggestion order, identity partition names back-quoted when needed. 16 gates 0, comment gate 0.
- Ruling B11-Q1: accept repo legacy-condition convention. New residue:
  - R-U4-12 `DESCRIBE t PARTITION (id=1)`: Spark `DESCRIBE does not support partition for v2 tables.` with condition _LEGACY_ERROR_TEMP_1111; RePark prefixes `Error during planning: [_LEGACY_ERROR_TEMP_1111] ` and getCondition() is None (facade condition grammar rejects leading `_`; owned by ice-error-conditions-1).
- B waits for #816 to merge, then rebases onto main (ruling 21:22 Q7), gates, critic B.

## Tick 37 (22:40)
- A12 (opus) HALT, 0 commits: router.rs reached 1003 lines (limit 1000) once the branch was rebased onto c7879a91, and the pre-commit hook refuses every commit. The A12 work is staged (7 malformed partition specs measured on Spark and pinned).
- Rulings (claims 22:40): the size breach is cleared by moving code into a helper, with no EXCEPTIONS row; EMPTY_PARTITION_VALUE gets fixed; new residues R-U4-13 `(a=1 b=2)` and R-U4-14 `(a b)` extra-input detail.
- A13 (opus, resume) launched 22:37.

## Tick 40 (23:36)
A13 (opus resume) concluded: router size fix + EMPTY_PARTITION_VALUE; #816 pushed at e64a0c57; critic A r5 Sol + local gate + lint running. New residue R-U4-15 `SHOW TABLE EXTENDED ... PARTITION (a==1)` (Spark accepts ==; RePark refuses near ==).

### Tick 42 (23:40) — #816
- Critic A r5 (Sol) NEEDS_REMEDIATION at e64a0c57: V-001 untested PARTITION scanner arms (third finding of class UNTESTED SCANNER BRANCH → WO-A15 arm table over every token-consuming fn), V-002 `Owner:` line → residue R-U4-16, V-003 ledger over-claim. CI Python red on the router guard → WO-A14. A14 (opus) launched 23:38.
## Tick 46–49 (23:59–00:40) — U4-A rounds A14/A15
- A14 opus (resume): router front door `canonicalize_verbatim` restored, the SHOW TABLE EXTENDED fallback moved into `show_table_extended::refusal_or`, ledger over-claims scoped. Residue R-U4-16: SHOW TABLE EXTENDED rows omit Spark's `Owner:` line (owner stamp lands with #813).
- A15 opus (resume): 43 Spark 4.1.2 answers measured; every scanner arm pinned at parser level and end to end; a word after PARTITION now answers `missing '('` like Spark. Residues R-U4-17 (`IN 'x'` accepted as a namespace; Spark PARSE_SYNTAX_ERROR), R-U4-18 (`IN LIKE 'pc'` missing Spark's `: missing 'LIKE'`), R-U4-19 (`SHOW TABLE EXTENDEDX` gets the generic SHOW refusal; Spark PARSE_SYNTAX_ERROR near EXTENDEDX).
- #816 rebased onto main 3cf263da, pushed at 66e1e108; gate, lint and critic A r6 (Sol) are running.
- 01:09 critic A r7 (Sol) PASS at 2c376f12, 40 tools. Its sandbox was read-only, so I'm running the mutation step myself (mut-a-r7). Gate, lint and CI still pending.
- 01:20 mut-a-r7 finished at 2c376f12: BASE rc=0; M1 (Type MANAGED->EXTERNAL) caught, 13 tests failed; M2 (RParen->Unclosed) caught, 12 tests failed. Test strength confirmed.
- 01:21 main moved to a6e8bcda (IPI-40 PR4). I rebased #816 to 4aa48713 and router.rs came out at 1011 lines, over the 1000 limit. Launched A17 (opus resume) to move the v2-command group into catalog_ops.rs. The r7 PASS is now stale, so r8 runs after A17.

## Tick 58 (01:22) — main moved; #816 rebased; router.rs over the size ceiling
- Critic A r7 (Sol) passed at 2c376f12, but main then moved to a6e8bcda (IPI-40 PR4 views), so that verdict no longer covers the head.
- Rebased xd-props onto a6e8bcda. The conflicts were in tests/mod.rs and in the docs row DBT-TBLPROPS-1; both are resolved and the comment gate is clean.
- router.rs is now 1011 lines, over the 1000 limit. Ruling A17 follows A12-Q1: refactor with no EXCEPTIONS row, target 985 lines or fewer so PR B's lines still fit. A17 (opus, resumed session) launched at 01:21.

## Tick 65 (01:54)
- Critic A r7 mutation check (own run on xr-xd-props at 2c376f12): BASE rc=0; M1 (Type MANAGED->EXTERNAL) caught, 13 tests failed; M2 (RParen->Unclosed) caught, 12 tests failed. The pins are strong enough; no under-pin gap.
- A18 opus fix round (critic r8 V-001 PARTIAL ERROR PIN, second finding of this class on A) is running: round 20260924T055338Z.

## Tick 69 (02:02)
- Critic A r9 (Sol, round 20260924T055820Z, 48 tools, valid) PASS at faf420e7 = #816's current head. Sandbox BLOCKED only its cargo/mutation step again (read-only); mutation evidence is my own r7 run (M1/M2 CAUGHT), and A18 changed tests + .md only.
- Waiting on the local gate, lint and CI at faf420e7 before the A replay and the queue.

## Tick 70 (02:12)
- #816: critic A r9 Sol PASS at faf420e7 (current head); local gate + lint + CI still running.
- #813 (B): restacked 48 commits onto faf420e7 with no conflicts -> 6bd0f90a (local, unpushed; backup bk/b-pre-restack-t70 = 055fcfbe). B's owner stamp updates A's SHOW TABLE EXTENDED expectations to carry Owner (addresses R-U4-16; confirm in the B replay).

## tick 72 (02:20)
- #816 local gate at faf420e7: CB=0 R=0 T=0 U=1 L=1. Sole failure: VIEWS PR4 near-miss d (main a6e8bcd) pinned the old SHOW TABLE EXTENDED fall-through; superseded by #816's Spark PARSE_SYNTAX_ERROR. WO-A19 (opus resume) updates the pin + map.md; then re-gate, lint, critic r10, re-restack B.

## tick 90 (03:52)
- #816 critic A r11b Sol PASS at 42616a49 (current head; 59 tools, valid; round codex-worker/xr-xd-props/20260924T074600Z). r11 (074113Z) had been NR with zero findings only because cargo could not run read-only; brief fixed (Round 11b) and refired.
- Waiting on #816 local gate + lint + CI at 42616a49; B (xd-show aa16a2b2) gate + lint running.
## tick 96 (04:29) — PR A MERGED
- repark#816 (U4 PR A, SHOW TABLE EXTENDED) MERGED TREE-EQUAL, main 970ac11a (head 42616a49). Evidence at queue: critic r11b Sol PASS at 42616a49, local gate 0/0/0/0/0, lint all 0, CI green, comment gate 0. First drive failed because the PR was still a draft; marked ready and requeued.
- Replay A at 42616a49 (before -> after): D-SHOW-TABLE-EXTENDED REFUSED-REGISTERED -> EQUAL; D-SHOW-TBLPROPERTIES and D-SHOW-TBLPROPERTIES-KEY REFUSED-REGISTERED -> REFUSED-REGISTERED = residue R-U4-20 (A2 table branch waits on the parser owner, xo-opus59 PR4).
- Clones /tmp/xd-props and /tmp/xr-xd-props removed.
- #813 (PR B): replay at aa16a2b2: D-DESCRIBE-COLUMN (NOT-PARSED), -COLUMN-PLAIN (NOT-PARSED), D-DESCRIBE-EXTENDED (DIFFERENT), D-CREATE-DEFAULT-PROPS (DIFFERENT), D-SHOW-TABLE-EXTENDED -> EQUAL; D-DESCRIBE-VERSION DIFFERENT -> matched refusal (both PARSE_SYNTAX_ERROR near 'OF', 42601).
  CI "Python" red: cap-1 twin baseline table not updated for session_core.py (2327 -> 2325), class BASELINE TWIN DRIFT; fix round B13 opus launched; the test is now in the local gate paths.

### tick 107 (05:10) — #813 critic B r2
Critic B r2 Sol at 1873ca86: PASS (verdict.sh rc=0, 54 tools, valid). r1 V-001..V-003 verified fixed. The round's BLOCKED status came only from the read-only sandbox rejecting its handback write, so the verdict stands. Local gate and lint are still running at 1873ca86.
