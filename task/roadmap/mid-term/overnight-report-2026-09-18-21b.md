# Morning report — run 21b of 2026-09-17 → 18 (identifiers, defaults, the fork residues, the registry)

**Session:** one Opus orchestrator (overnight-21b), 2026-09-17 19:50 → 2026-09-18 07:00 EDT, resumed once after the 05:02 reboot ·
**Orchestrator:** Claude (claude-opus-5) · **Lanes:** `kb-` · **Actor tiers:** Claude Opus 5 at high effort (the two design units),
Devin SWE-2 (the fork lane), Muse Spark 1.3 contributor (one pins-only round) · **Reviewers:** Grok 4.6 (critic-logic, Rust perf,
verification) · **Beside:** run 21a (read path, DML, OCC, nested) and run 21c (maintenance, writer knobs, timestamp_ns).
STATUS.md edits: only this run's own residue-clause deletion. No release, tag or pipeline touched.

**In one paragraph.** Two of this run's four units landed: **fork #293** (the three fork residues, the `rollback_to_time` message and the cherry-pick plan reuse — merged `8477b249`, tree-equal) and **#678 ICE-V3-WRITE-DEFAULT-1** (rating row V3-03b closed on main, merged `93606656`, tree-equal, every local gate green on the merged tree). **#676 ICE-MIXED-CASE-1** stays a draft: verified PASS, but its whole facade suite hung at 99 % on round 2 and the cause was not found before the 05:02 reboot; it is DIRTY against main and lost one unpushed commit. The **RP-26 pin bump** that turns #293 into RePark behaviour waits on 21a's RP-25 (#690, still open); the **registry sweep part B** was not started. Every PR's comment gate: 0 hits. Measurements overturned four premises (§4), two of them reviewer claims that were the reverse of Spark.

## 0. The freeze, plainly

The box ran out of memory (125 GiB, no swap; OOM kills from 00:10, desktop frozen ~04:46) and the owner rebooted at 05:02. `/tmp` was
wiped. **Lost:** every lane clone, the fork clone, every reviewer clone, the Grok/Devin/Muse run directories (per-round cost records),
and `/tmp/oc-worker/kb-rv/` (the review briefs and the eight reviewer reports of this run) even though it sat under the durable root.
**Survived:** everything pushed to GitHub, the Spark oracle fixtures and recorders (`/tmp/oc-worker/kb-oracle/`), every actor brief and
follow-up, the Opus hand-backs. What each review found is preserved in the unit ledgers on the pushed branches (and, for the fork, in
the merged fork ledger `task/f-ice-residues-21b-ledger.md`); the costs below are the figures I read from each run's `out.json` during the
night. Two pieces of work existed only in a lane and are **gone**: the Muse N-04 commit on ICE-MIXED-CASE-1 (`4813b5fa`, never pushed)
and the verbose facade re-run that was hunting a hang on that branch (§3). They are not reconstructed here as if measured.

This run contributed to the memory pressure: it ran two release-native lanes, a fork build, full facade suites at `-n 8`, and up to
four Grok reviewers that each compiled test targets, at the same time as runs 21a and 21c. At 03:24 it flagged in `claims.txt` that
`/tmp/kc-wo` was linking ~70 test binaries at once and had OOM-killed this run's `make verify`; it re-ran its own gates behind a
"≥ 40 GB free" wait, but did not reduce its own concurrency. The new caps (one cargo thing per orchestrator, 6 jobs, the two-slot lock)
are right, and this run's post-reboot work runs under them.

## 1. Residual matrix — before / after (measured on merged main unless stated)

| Rating row / residue | Before (start of run 21) | After | Where |
|---|---|---|---|
| F-HADOOP-VN-REPLACE-1 — a stale `CREATE OR REPLACE` on a Hadoop `vN` table wrote a uuid file on the stale catalog (split brain) | OPEN in the fork (ICE-HADOOP-VN-1-R-001) | **FIXED in the fork** (fork main `8477b249`): a staged replace on a `vN` base stages `v(N+1)` and creates it once, at commit, with its final bytes, under exclusive create; a stale replace fails retryable `CatalogCommitConflicts`, winner bytes intact. **RePark still on RP-25** — the strict-xfail cells flip in the RP-26 bump (not opened, §4) | fork #293 |
| F-CHERRYPICK-WAP-ORDER-1 — duplicate WAP pick refused with the fork's message | OPEN (ICE-BRANCH-OPS-1-R-001) | **FIXED in the fork** (WAP duplicate validated first, Java's `DuplicateWAPCommitException` text); RePark pin flips at RP-26 | fork #293 |
| F-UPDATE-SCHEMA-SAME-1 — a no-op schema update minted a schema id and committed | OPEN (ICE-COLUMN-REORDER-1-R-001) | **FIXED in the fork** (Java `base == metadata`: an update that changes nothing commits nothing; `is_same_schema` compares identifier ids as a set); RePark pin flips at RP-26 | fork #293 |
| Q-20b-C — `rollback_to_timestamp` ancestry walk into the fork | open | **Premise measured wrong**: the fork's `rollback_to_time` already matched Java's strict-`<` walk over current ancestry; the only divergence (snapshotless-table message) is fixed in the fork. Moving RePark's procedure onto the fork API is product code, left for a later unit | fork #293 |
| Branch-ops perf R-01 — cherry-pick replay planned twice | open | **FIXED in the fork** (one plan per validate→commit, `Arc`-cached, keyed on base identity) | fork #293 |
| V3-03b — `write-default` fills NULL on INSERT lists, MERGE inserts, DataFrame appends | MISSING, silent | **FIXED on main** (`93606656`): every omitted-column write path fills from the current schema's `write_default` in Rust and answers the measured Spark cells on both doors; `saveAsTable(overwrite)` DECLARED (Spark replaces), mixed static+dynamic PARTITION OPEN (loud) | #678 |
| V2-27 — unquoted mixed-case columns on a Spark-created table | loud, unregistered | **Not on main.** #676 carries the fix and a verification PASS; its whole-facade gate hung on round 2 and it is DIRTY against main (§3) | #676 |
| Registry sweep part B (V2-10d, V2-15 ORC/Avro, V2-24b, V2-29, V3-05, ENC-1, variant/geometry, puffin writers; claims C-2, C-7, C-10, C-11) | no rows | **not started** — it was scheduled after #676/#678 merged; brief ready at `/tmp/oc-worker/kb-docs/brief-1.md` | — |

## 2. Per-PR table

| PR | Unit | Actor and rounds | Reviews (Grok 4.6) | Comment gate | State |
|---|---|---|---|---|---|
| fork #293 | **F-ICE-RESIDUES-21B** — the three fork residues + Q-20b-C + branch-ops R-01, one commit each | Devin SWE-2, 4 rounds (r1 REJECTED by the comment gate: 16 ASF-header lines of a new test file, plus false "(#292)" subject suffixes; r2 moved the tests into an existing file; r3 review findings; r4 V-001) | logic **NEEDS_REMEDIATION** (L-001 P1 hollow pin, L-002 P2 two `vN` slots, L-003/L-004 P3) $2.12 · Rust perf **PASS** (R-01, R-02 P2) · verification **NEEDS_REMEDIATION** (V-001 P2: the L-002 fix overwrote a live `v(N+1)` before the CAS) $1.62 · verification 2 **PASS** $0.83 | 0 hits (every round after r1) | **MERGED** `8477b249`, tree-equal, 04:18; fork CI 14/14; orchestrator re-test on the rebased head: lib 3769 passed, interop, clippy green |
| #678 | **ICE-V3-WRITE-DEFAULT-1** | Opus high, 2 fresh sessions (r1 ended while its gates ran in the background; r2 gated in the foreground), after run 20b's 4 Muse rounds | verification **NEEDS_REMEDIATION** (V-01, V-02 P2; every fix mutation-proven) $1.45 · verification 2 **PASS** $1.35 | 0 hits | **MERGED** `93606656`, tree-equal, 06:07; see §2a/§2b |
| #676 | **ICE-MIXED-CASE-1** | Opus high, 2 fresh sessions + 1 Muse pins-only round (lost), after run 20b's 6 Muse rounds | verification **NEEDS_REMEDIATION** (N-01, N-02 P1; N-03 P2) — cost not recovered (the first launch false-concluded after one turn, $0.01, and was resumed) · verification 2 **PASS** (N-04 P2) $0.89 | 0 hits | draft, DIRTY; see §3 |

**Grok cost recorded:** $8.26 across the six reviews with a recorded figure; two runs (mixed verification 1 after its resume, fork Rust
perf) have no surviving figure. **Opus actor sessions:** four (mixed r1, r2; write-default r1, r2); their turn and cost figures are not in
the text-mode logs and did not survive. **Devin:** free tier. **Muse:** one round (N-04), lost.

### 2a. #678 at the time of writing

Head pushed before the freeze: `f9a29013` (rebased onto #682 and ICE-EVO-DML-1). On that head GitHub CI is green (Rust lint/clippy,
workspace tests, the facade smoke). Local gates on the previous head `d99df18c`: unit file 24 offline / 25 live, **whole facade 9,752
passed / 0 failed**, parity 756 passed, `make verify` and `make rust-clippy` green. The post-rebase re-gate was cut by the freeze.
After the reboot one capped lane rebased it onto main `3bd667e6` (ICE-SORTED-INSERT-1) cleanly and is re-running the release native,
the unit files offline and live, clippy and `make verify`, under the new caps (6 jobs, the two-slot lock, one cargo thing at a time). **Result on `0e6b4a36` (05:41): comment gate 0 hits; unit files offline 340 passed (write-default, EVO-DML, both sorted-insert units, the writer); live leg 25 passed; `make rust-clippy` and `make verify` green.** The whole facade suite was not re-run locally after this rebase (it ran green, 9,752/0, on `d99df18c`; CI's required smoke job runs it on the head). Un-drafted and queued at 05:42 (`678 21b 05:42`, driver `merge-678`) — see §2b.

### 2b. #678 merged

The first drive (05:42) stopped on RESULT=CONFLICT: run 21a's ICE-OCC-SCOPED-1 (`e7857f9f`) landed at 05:45 on the same MERGE / predicate-DML files. The rebase replayed every code commit cleanly; the only conflict was the shared STATUS.md residue entry (both runs' clause deletions kept). Re-gated on `fc25a13c` under the caps: comment gate 0; unit files offline 363 passed (write-default, OCC-SCOPED, EVO-DML, sorted-insert, the writer); live 25 passed; clippy green; CI 9/9 green. Re-queued 06:06; **merged `93606656`, tree-equal, 06:07.** `make verify` on that head was still running when the driver merged (CI was already green, so the driver did not wait for it); its result: **green** (06:12) — every local gate passed on the merged tree.

## 3. What did not land, and exactly what it still needs

- **#676 ICE-MIXED-CASE-1 (draft, DIRTY).** Pushed head `743c0592`. Done and verified: V-01, V-02, V-04, L-08 (every reference to an
  ASCII case twin refuses `[AMBIGUOUS_REFERENCE]` **42704**, one option per matching field — Q-21b-1/2, measured), R-02/V-03 early exit
  (cost before/after in the ledger, default profile), R-03, the N-02 pin with teeth, N-03 DECLARED, the V2-27 registry rows. Round-2
  unit gates: clippy green, `repark-core` 600, `repark-spark` 1,129, unit file 64 offline / 108 live. **Open:** (1) the whole facade
  suite on round 2 **stopped making progress at 99 % for 85 minutes** (03:00 → 04:25; round 1's run finished in 35 minutes) — the
  hung test was not identified before the freeze; it must be found before this merges (the new fold-and-replan loop is the first
  suspect); (2) N-04 (P2): the always-run N01 pin must also assert Spark's output column names — the one-assertion fix was made and
  lost; (3) conflicts against main (21a's #682 / EVO-DML / sorted-insert); (4) the V2-27 STATUS.md clause deletion.
- **RP-26 fork pin bump (not opened).** Fork main `8477b249` carries #293. RP-25 (#690, 21a, fork `64705c99`) is still open at 06:13. The bump flips: the two strict-xfail HADOOP-VN target cells (and deletes the two "split brain" current-behaviour pins), the
  duplicate-WAP message pin, the reorder strict-xfail and `alter_column_move.rs:113`. Brief ready: `/tmp/oc-worker/kb-bump/brief-1.md`.
- **ICE-REGISTRY-SWEEP-1 part B (not started).** Brief ready: `/tmp/oc-worker/kb-docs/brief-1.md`.

## 4. What the measurements overturned (every one recorded before the actor touched it)

- **Write-default L-01 was not unreachable.** Run 20b re-scoped it as "a ParserError on the Spark door". Spark 4.1.2 **parses** all three
  `INSERT OVERWRITE t PARTITION (k) (cols)` shapes and fills the write-default (`[[null,'x',5]]`, `[[10,'x',5]]`). The silent null-fill
  arm was live. (Q-21b-3)
- **The ambiguity SQLSTATE is 42704, not 42702**, and Spark refuses even an exact-case reference when a case twin exists. (Q-21b-1)
- **Two reviewer premises were the reverse of Spark.** Write-default V-02 claimed `DEFAULT` in the outer SELECT of a `WITH` query fills —
  Spark refuses it (42703). Mixed-case N-01 claimed `SELECT USERID FROM mc WHERE EVENTNAME IN (SELECT EVENTNAME FROM other)` fails in
  Spark — Spark answers `[[1],[2]]` through a correlated reference. Neither became a fix. (Q-21b-9, Q-21b-11)
- **Spark `saveAsTable(overwrite)` replaces the table** (the schema narrows to the frame) — DECLARED beside F-002. (Q-21b-5)
- **A missing nullable column without a default is accepted and written NULL** by Spark on `writeTo().append()` and
  `saveAsTable(append)` — the roll-call merge condition, answered. (Q-21b-6)
- **The fork brief's Q-20b-C premise was wrong** (Java's `rollbackToTime` uses strict `<`; the fork already matched) — the fork actor
  measured it against the Java 1.10.0 source and fixed only the real gap.
- Fixtures: `/tmp/oc-worker/kb-oracle/{mc,wd,wd}-truth{,-2}.json`, `probe_mc2.log`, recorders `probe_*.py`; the cells used by the units
  are committed in each unit's fixture.

## 5. Rulings taken (G-2; full text in the unit ledgers)

- **Q-21b-1** — case-twin ambiguity SQLSTATE 42704 (measured), replacing Q-20b-2's 42702.
- **Q-21b-2** — one ambiguity option per matching field, requested spelling, qualified as Spark qualifies it.
- **Q-21b-3** — write-default L-01 not re-scoped: the PARTITION arms fill on both doors.
- **Q-21b-4** — `DEFAULT` fills on INSERT OVERWRITE as on INSERT INTO.
- **Q-21b-5** — `saveAsTable(overwrite)` DECLARED (Spark replaces).
- **Q-21b-6** — accept-and-NULL for a missing nullable no-default column; the removed Python refusal is a measured decision.
- **Q-21b-7** — the three fork residues + Q-20b-C + R-01 ship as one fork PR (one fork CI cycle, one bump).
- **Q-21b-8** — `overwritePartitions()` fills an omitted defaulted column.
- **Q-21b-9** — `DEFAULT` under a `WITH` clause refuses, as Spark does.
- **Q-21b-10** — mixed static+dynamic `PARTITION (id=1, cat)` is an OPEN registry row with both Spark cells; RePark refuses loudly.
- **Q-21b-11** — N-01 is Spark's answer; RePark's loud refusal at physical planning is a pre-existing engine limit (all-lowercase too),
  DECLARED ID-1b.
- **Q-21b-12** — `SELECT *` over case twins: the refusal broke the DataFrame filter door, so RePark's answer is DECLARED (ID-1a).
- **Q-21b-13** — the actor's deviation from Q-21b-11's "pin the answer" (it pinned the refusal) accepted on the measurement.
- **Fork Q1 / Q2** — no fork `task/map.md` (the fork indexes lanes in `task/todo.md`); the zero-update transaction skip is Java's
  `base == metadata` short-circuit (confirmed by the logic critic against Java 1.10.0 for every action type).
- **Coordination** — with 21a in `claims.txt`: RP-25 = fork `64705c99` (#292 only, 21a); #293 rides RP-26.

## 6. Owner questions (with recommendations)

- **Q-21b-A — memory budget for three orchestrators.** Tonight's freeze was nine concurrent cargo test/link invocations. *Recommendation:*
  keep tonight's caps as standing rules and add one: reviewers that need to build (mutation) take a build slot like any gate.
- **Q-21b-B — the SQLSTATE change.** Q-21b-1 moves the case-twin ambiguity from 42702 to Spark's measured 42704. *Recommendation:*
  accept; it is the only answer the oracle supports.
- **Q-21b-C — `SELECT *` over case twins and correlated IN-subquery select lists** stay DECLARED (ID-1a, ID-1b). *Recommendation:*
  accept for 1.x; neither is reachable on a non-twin Iceberg table, and the second is a DataFusion physical-planning limit.

## 7. STATUS.md

This run's only STATUS.md edit is the V3-03b clause deletion, merged with #678 (main now reads: V2-20a, V2-10d, V2-27 left in the residue entry). When #676 merges it deletes the V2-27 clause. No other line needs correcting from this run.

## 8. Rust-first roll-call (Q-17a-2)

| Unit | Left in Python | Why |
|---|---|---|
| ICE-V3-WRITE-DEFAULT-1 | the DataFrame writers emit the target column list | binding names; the fill, the refusal of a missing REQUIRED column and every value decision are Rust |
| ICE-MIXED-CASE-1 | the `spark.sql.caseSensitive` conf carrier | carries the flag; the fold, the audit and every refusal are Rust |
| fork residues | — | fork Rust only |

## 9. Disk

Before the reboot this run removed every reviewer clone as its report was read and ~6 G of actor scratch (bench copies, "before"
targets). After the reboot it ran exactly one lane (`/tmp/kb-wd`, one cargo build at a time through the two-slot lock) and removed it at 06:12 once #678 merged; it holds no lane now. 1.3 T free on `/` at 06:12.

## 10. Pointers

Briefs, follow-ups and hand-backs: `/tmp/oc-worker/kb-{mixed,wd,fork,bump,docs}/`; oracles `/tmp/oc-worker/kb-oracle/`; coordination
`/tmp/oc-worker/run21/claims.txt`; state `/tmp/oc-worker/run21/state-21b.md`.
