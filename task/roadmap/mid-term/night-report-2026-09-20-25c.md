# Run 25c report — 2026-09-19 → 20 night run: the Spark–Iceberg parity campaign, RePark side

**Run:** 25c (unit `night-25c`, lane prefix `qc-`). **Window:** 17:22 → 05:30 EDT. **Orchestrator:** claude-opus-5.

**Actors:** Claude Opus 5 at high effort — ICE-SESSION-WRITE-CONF-1 (7 rounds), ICE-REPLACE-COLUMNS-1 (1 round),
ICE-WAP-BRANCH-1 (2 rounds). Muse Spark 1.3 contributor — ICE-PROCS-ROUTE-1 (4 rounds) plus 1 clerk round.
**Reviewers:** Grok 4.6 verification critics, mutation first, 7 runs.
**Beside:** 25a (parity RePark halves + every bump), 25b and 25d (fork lanes), 25e (plan packets). Coordination
through `claims.txt`; I filed four fork asks there and took 25d's and 25b's fork merges as they landed.
**Box:** the inherited build clone `/tmp/nd-build` plus two lanes (`/tmp/qc-procs`, `/tmp/qc-wap`); at most two
cargo things at a time; every gate through `build-slot.sh`, every JVM through `jvm-lock.sh`. No AWS call of any kind.

## 1. Slate rows — before / after (measured, by replaying the inventory cells)

| Row | Before (measured) | After | PR |
|---|---|---|---|
| IPI-55 `ALTER TABLE … REPLACE COLUMNS` | 22 of 34 cells differ (silent: old values kept under new ids) | **FIXED on main.** 34/34 equal. Every column dropped and re-added with fresh ids; complex types go through the one shared column-type path; a partition or sort field that loses its source refuses with Spark's text | #736 `b66a0146` |
| IPI-30 `ancestors_of`, `compute_table_stats`, `compute_partition_stats`, `rewrite_table_path` | all four refuse ("not supported"), 0 of 27 cells | **FIXED on main.** 29 of 31 cells equal (the oracle grew to 31 while closing the critic); 3 strict xfails on two fork gaps | #742 `ae08c218` |
| IPI-05 `spark.wap.branch` (the RePark half) | 5 of 19 cells (writes published to main and reads stayed on main — silent, both directions) | **FIXED on main.** 27/27 equal (the oracle grew to 27 while closing the critic) | #747 `e4160a58` |
| IPI-14 session confs `spark.sql.iceberg.snapshot-property.*` / `compression-codec` | 22 of 25 cells differ (silent) | **HAND-OVER: ready, gate-green, queued twice, not merged** at `acde6658`; 106-cell fixture, both doors, all four critics' findings closed | #733 |
| IPI-45 `position_deletes` metadata table | refuses | **fork-blocked**, ask F-POSDEL-SCAN-1 filed (the fork has the schema only) | — |
| IPI-22 incremental + changelog reads | refuses | **not started.** 37 Spark cells recorded and the Opus-tier brief written | — |

## 2. Pull requests

| PR | Unit | Actor, rounds | Reviews (Grok 4.6) | Comment gate | State |
|---|---|---|---|---|---|
| #736 | ICE-REPLACE-COLUMNS-1 (IPI-55) | Opus high r1 (159 turns, $22.40) | verification **PASS**, mutation a/c/d/e/f red, 4 P3 ($2.29) | 0 | **MERGED** `b66a0146`, tree-equal (first attempt CHECKS-RED: the CAP-1 parity test mirrors `check_rust_file_size.py` and the two ratchets were not mirrored) |
| #742 | ICE-PROCS-ROUTE-1 (IPI-30) | Muse r1–r4 + 1 clerk round | verification r1 NEEDS_REMEDIATION (2 P1, 6 P2, $1.76) → r2 no P1, 5 pin-strength P2 ($1.30) → closed | 0 (two TOML comments caught and sent back) | **MERGED** `ae08c218`, tree-equal (first attempt CHECKS-RED on one clippy `too_many_lines`) |
| #747 | ICE-WAP-BRANCH-1 (IPI-05 RePark half) | Opus high r1 (172 turns, $27.17), r2 (71, $6.82) | verification NEEDS_REMEDIATION (1 P1, 1 P2, 1 P3, $1.59) → closed | 0 | **MERGED** `e4160a58`, tree-equal |
| #733 | ICE-SESSION-WRITE-CONF-1 (IPI-14) | Opus high r1 (301 turns, $53.24, hit the turn cap), r2 (138, $13.81), r3 (230, $32.96), r4 (196, $26.16), r5 (113, $10.69), r6 merge (95, $6.13), r7 merge (41, $2.10) | **four** verification critics on four heads: 4 P1 → 4 P1 → 2 P1 → 1 P1, every one closed; every mutation red at the end ($16.12) | 0 | **READY, QUEUED** at `acde6658` |

**Costs.** Grok 4.6: **$21.77** across 7 runs. Claude Opus 5 actors: **$201.48** across 1,516 turns. Muse: 5 launches
(4 unit rounds + 1 clerk), on the subscription. **Clerk rounds: 1 launched, 1 closed, 0 failed** — plus two mechanical
fix-ups I did myself *before* the 19:56 clerk ruling, and three after it that were merge-conflict resolutions in
product Rust (the ruling keeps those with me) or a one-line `ruff format` with the run ending.

## 3. What the measurements showed

- **Spark does not stamp everything.** A TRUNCATE and a metadata-only DELETE commit a `delete` snapshot with NO
  session snapshot property, and no collision refusal either. Two of #733's "missing" behaviours were therefore the
  opposite of what the critic assumed — measured, then pinned as *not* stamping.
- **The collision rule is "an ACTUAL collision".** A session property naming a summary key the engine also produced
  for that commit fails the write (`Multiple entries with same key: …`); a key it did not produce is stamped AND
  feeds Spark's totals (an injected `deleted-records=5` on an INSERT makes `total-records` = previous + added − 5).
  A blanket refusal of metric-named keys — tried in run 24c — is wrong and breaks the recorded `COLL-*` cells.
- **Two critic premises were refuted by measurement.** Spark ignores a case-folded `spark.sql.iceberg.*` conf key
  (RePark's exact match was already right, but the property SUFFIX is verbatim and RePark had been lowering it), and
  Spark does NOT stamp its `rewrite_manifests` replace snapshot.
- **A conf must not change layout.** Setting any session write conf rerouted a plain UPDATE onto a path whose
  committed FILE COUNT followed the plan's batch count. Fixing it (one rolling writer over
  `survivors UNION ALL new values`) also closed `F_V3_8_UPDATE_FILES`, the artefact pinned on 2026-09-02 ("2 data
  files where Spark writes 1").
- **`spark.wap.branch` redirects reads too**, and only when the table carries `write.wap.enabled=true`; a wap branch
  that does not exist is created from main for every DML family (measured); `wap.branch` + `wap.id` refuses.
- **A read-side redirect must visit every relation.** The first critic pass found `FROM t a, t b` reading the branch
  for `a` and main for `b` — silent. The read pass is now a parenthesis-depth walk.
- **`compute_table_stats` argument semantics are Spark's, not the fork's**: `columns => array()` refuses
  `Columns cannot be null/empty`, a nested name `st.a` ANSWERS, and `columns => array('st')` refuses
  `Can't compute stats on non-primitive type column: st (…)`.
- **The fork's new `manifests-created/-kept/-replaced` keys agree with Spark exactly** (probed on three appends and a
  CoW DELETE at RP-40) — measured, not yet pinned (§5, item 3).

## 4. Rulings (G-2)

- **Q-25c-1.** No fifth verification critic on #733 after round 5: every round-5 finding was closed against newly
  measured Spark cells with a mutation-proven pin, which is the Q-24c-2 precedent.
- **Q-25c-2.** No second verification critic on #747 after round 2, on the same grounds.
- **Q-25c-3.** `#733`'s IPI-08 xfail (`QS-DELETE-PART-META`) was re-measured the moment 25a's #739 merged and
  flipped to a PIN rather than carried as a residue.
- **Q-25c-4.** The uuid-v7 file-naming fix (see §5) was NOT taken tonight: it needs a root-manifest change while a
  pin bump was open, and 25a holds every bump.
- **Q-25c-5.** `#733`'s `spark.wap.id`-alone shape stays as it is (commits to main) rather than being turned into a
  new refusal: that is the fork half (F-STAGE-ONLY-1, now open as fork #330) and changing it would change what
  `spark.wap.id` does today.

## 5. Gate items and owner questions

The owner's 19:36 ruling makes class-only and layout-only residues gate items, not footnotes. Mine, all measured:

1. **Exception class on Iceberg validation refusals** (#736): RePark raises `PySparkException` where Spark surfaces
   `Py4JJavaError` wrapping `ValidationException`; message cores are identical. Cells `RC-PART-DROP-SOURCE`,
   `RC-PART-KEEP-NAME`, `RC-SORTED` (v2 and v3). This is IPI-51's shape and wants that unit.
2. **Parse refusals carry no `== SQL ==` caret block** (#736, cell `RC-NOT-NULL`).
3. **`manifests-*` summary keys are measured equal but not pinned** (#733): pinning them means widening
   `SUMMARY_KEYS` and re-recording all 106 cells. One small unit.
4. **UUID v4 file naming** (#733, and everywhere): RePark's owned writers name data files with a random UUID v4
   where the fork uses time-ordered v7, so multi-file scan order is unstable run to run — this is the whole
   "order-only" difference class in my replays. The fix is `uuid` feature `"v7"` in the root `Cargo.toml` plus five
   call sites. **Recommendation:** take it first, right after RP-41.
5. **`rewrite_table_path` version range** and **nested `compute_table_stats` columns** (#742): three strict xfails,
   fork asks F-RTP-VERSION-RANGE-1 and F-CTS-NESTED-1.
6. **A partition overwrite does not remove superseded position-delete files** (#733): RePark's snapshot carries
   `total-delete-files=1` and no `removed-delete-files` where Spark reports `removed-delete-files=1` /
   `removed-position-deletes=1` / `total-delete-files=0`, and the superseded delete file leaks. Reads stay correct.
   Fork ask F-OVERWRITE-REMOVE-DELETES-1.
7. **`rewrite_data_files` ignores the session codec** (#733): fork ask F-RDF-SESSION-CONF-1, narrowed this run to
   its codec half only.
8. **A wap branch is created in a separate commit** (#747): a write that then fails leaves an `audit` ref pinned at
   main where Spark would leave none. Loud, never a wrong answer.
9. **`SET spark.sql.iceberg.snapshot-property.team = s`** is accepted by RePark's door where Spark refuses it with
   `ParseException [INVALID_SET_SYNTAX]` (a bare dotted key). A RePark superset, recorded as ledger clause C-017.

**Fork asks filed tonight (all in `claims.txt` with their Spark cells):** F-POSDEL-SCAN-1 (IPI-45 — the fork has the
`position_deletes` schema only), F-RTP-VERSION-RANGE-1, F-CTS-NESTED-1, F-OVERWRITE-REMOVE-DELETES-1.

## 6. STATUS.md

No edit. No STATUS clause names these units.

## 7. Rust-first roll-call

| Unit | Left in Python | Why |
|---|---|---|
| ICE-REPLACE-COLUMNS-1 | — | parser, planner and refusals in `crates/repark-spark/src/replace_columns.rs` |
| ICE-PROCS-ROUTE-1 | — | one router module per procedure over the fork's actions |
| ICE-WAP-BRANCH-1 | forwards the two conf strings | the resolver and the read-side pass are Rust |
| ICE-SESSION-WRITE-CONF-1 | forwards the three conf spellings | carrier, resolver, collision rule and codec order are Rust |

## 8. Machine, lanes and disk

- `/tmp/nd-build` (inherited) carried #733 through seven rounds; `/tmp/qc-procs` and `/tmp/qc-wap` were made with
  `r9-lane.sh RELEASE=1`. Never more than two cargo things at once; `free -g` stayed above 40 GB at every lane setup.
- **A trap worth naming:** a fresh lane clone commits as the machine's default git identity (a personal address) unless BOTH `user.name`
  and `user.email` are set locally — the pre-push hook blocks the owner's personal email as a forbidden literal.
  25a hit the same thing; I rewrote four commits with `filter-branch` before #736's first push.
- **`comment_ban.py` does not scan `.typos.toml`** — a Muse round added two trailing `#` comments there and the gate
  reported `hits=0`. I caught it by reading the diff. Worth teaching the script every text source file.
- New scripts: `_lib/qc-opus.sh` (headless Opus actor with `--output-format json`), `_lib/qc-review.sh` (critic
  launcher, prefix `qc-rv-`), `_lib/qc-wait.sh` (the foreground wait loop that also prints new `claims.txt` lines).
- Disk: `/` had 1.3 T free at launch, 677 G used at the end after I removed eight `qc-rv-*` critic clones (50 G) and the three lanes' `target/debug` trees (55 G) — 1.1 T free.

## 9. Next run, in order

1. **#733** if it did not merge (it is ready, queued and CI-green apart from whatever the last run reports).
2. **IPI-22 incremental + changelog reads** — 37 Spark cells at `/tmp/oc-worker/qc-meas/spark-qc4.json` (cells
   `cells_qc4.py`) and an Opus-tier brief at `/tmp/oc-worker/qc-incr/brief-1.md`, both written tonight and unused.
   25e's packet `ipi-22-incremental-changelog.md` covers the same ground from the other side.
3. **The uuid-v7 file-naming item** (§5.4) — small, and it removes a whole class of replay noise.
4. **IPI-45 `position_deletes`** once F-POSDEL-SCAN-1 lands; **`add_files`** CALL routing now that fork #325 merged
   (25d handed it over); the RePark halves of fork #324 (branch-read schema, IPI-07) and #330 (WAP stage-only).
5. **The refusal band in rank order:** IPI-19, IPI-56, IPI-32, IPI-21, IPI-25, REF-2, IPI-23/MT-1, IPI-20 — 25e wrote
   plan packets for most of these tonight; use them as briefs.
6. **The extractor string-argument follow-up to #730** (`year('2020')`), still untouched.

## 10. End state (05:30)

- **Merged tonight (3 PRs, all tree-equal):** #736 `b66a0146`, #742 `ae08c218`, #747 `e4160a58`.
- **Open — the hand-over:** #733, READY and gate-green at `acde6658`, queued twice. It did not merge because main
  moved under it four times in the last two hours (#739, #745, RP-40, and my own #747). The last merge has ONE
  semantic conflict, `crates/repark-spark/src/write_to_branch.rs`: #747 restructured `apply_write_to_branch` so the
  wap-branch target is resolved when there is no explicit `t.branch_x` selector, while #733 made the branch-write
  head "owned" so the session confs reach the commit — both must hold. I stopped rather than resolve product Rust at
  04:50 with CI needing twenty minutes. The exact steps are in a PR comment on #733 (merge, re-gate with
  `local-gate.sh`, re-measure the `QS-BRANCH-*` / `QZ-BRANCH-*` cells, queue). Nothing else about the unit is open.
- **Durable artifacts:** `/tmp/oc-worker/qc-meas/` (every cells file and Spark recording this run made: `qc1`, `qc3`,
  `qc4`, `qc5` plus the actors' `qc6`–`qc13`), `/tmp/oc-worker/qc-swc/`, `/tmp/oc-worker/qc-procs/`,
  `/tmp/oc-worker/qc-wap/`, `/tmp/oc-worker/qc-incr/` (briefs, hand-backs, critic findings, PR bodies),
  `/tmp/oc-worker/qc-rv-*/` (critic briefs).
- **Lanes:** `/tmp/nd-build` (7.7 G) kept — it holds #733, the hand-over; `/tmp/qc-procs` (3.2 G) and `/tmp/qc-wap`
  (3.5 G) are removable now that both PRs are on main. Every `qc-rv-*` critic clone was removed after reading.
