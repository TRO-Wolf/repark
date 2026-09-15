# Day report: run 16a, the functions

**Date:** 2026-09-15 07:59 to 20:15 EDT
**Orchestrator:** Claude (claude-opus-5), one of three run-16 orchestrators. Run 16b owns DataFrame, Column, Session, Catalog and types; run 16c owns the SQL door and the parser/planner.
**Charter:** the 1.5 row of the release roadmap — every `pyspark.sql.functions` name answers PySpark 4.1.2 on both doors, or is a dated declared refusal. Builders on Muse Spark 1.3 contributor (owner, 2026-09-15); reviewers on Grok 4.6.

## 1. Census slice (functions), before and after

PySpark 4.1.2 `functions`: 519 names in `/tmp/oc-worker/run15/pyspark-surface.json`, 508 real after dropping the 11 typing re-exports.

| Point | Missing names |
|---|---|
| Run start (main 11ae1595) | **51** |
| After #606 FNP-11A (merged aae8db51) | **40** |
| After #613 FNP-BITMAP-FACADE-1 (merged 230468c5) | **37** |
| After #618 FNP-WIN-1 (gated green, CI green, queued 19:08) | **34** on merge |


## 2. Per-PR table

| PR | Unit | Names / scope | Builder rounds | Reviews (verdict, cost) | Gates | State |
|---|---|---|---|---|---|---|
| #606 | FNP-11A | 11 temporal constructors, intervals, timestamp_add/diff (run 15a's green PR) | — (run 15a) | — (run 15a) | rebased three times as main moved (#616, #610, #612); CI green | **merged aae8db51** 11:04, tree-equal |
| #623 | FNP-6D-FOLLOWUP-1 | P1 in #609: bitmap_or/and_agg refuse non-BINARY; construct raises CAST_INVALID_INPUT / CAST_OVERFLOW | Muse ×3 (R1 red pins + HALT ruled D-5, R2 fix, R3 critic+perf remediation) | Grok critic-logic NEEDS_REMEDIATION (L-001 P1 NaN/Inf silent zero — confirmed live; L-003 refuted by oracle); Grok S2-21 Rust perf no P1, 2 P2 copies removed | 54 pins, 16 cargo, verify 0, parity 757 | **merged bee2cde3** 11:40, tree-equal |
| #613 | FNP-BITMAP-FACADE-1 + DEGREES-RUST-1 + uniqueness pin | bitmap_construct_agg / bitmap_or_agg / bitmap_and_agg; degrees/radians as a Spark-exact Rust UDF pair on both doors | Muse ×4 so far (run 15a GLM ×2 before) | run-15a critic NEEDS_REMEDIATION (L-001…L-005, closed); verification round 1 NEEDS_REMEDIATION (L-006 P1 BOOLEAN degrees — confirmed live, L-007/L-008 P2 — confirmed live, L-009 P3); Python perf no P1 (PYPERF-001 P2 fixed); verification round 2 no P1 (L-010 refuted by 60 live DEGI cells, L-011 P3; oracle-found `'1d'`/`'1f'` P2) | 5e6d5e10 on 4bd43fc8: facade 8016, verify 0, pins 610, example coverage executed, census 71; a359e25e (ledger departure) CI green | round 4 (suffix parse, DEGI pins, L-011 tidy-up) + ledger departure; rebased onto #622 and TYPES-GEO-DDL-1 | **merged 230468c5** 15:24, tree-equal |
| #618 | FNP-WIN-1 | window, window_time, session_window | Muse ×8 in pa-build (step 2 resumed from the run-15 carry-over; steps 3–5; two audit rounds; a remediation round in a fresh session after two provider stalls; an S-2 size round) + verification round 2 in progress | round-1 critic HALTED (L-001 refuted live; L-002…L-008 confirmed and fixed); Rust perf six P2 fixed; Python perf P3; verification round 2 HALTED (L-001 P1 confirmed live, L-003 upgraded to P1 by the oracle, L-002 pins, L-004 message) | 40fd42e8 on 230468c5: facade 8060, verify 0, pins 519, sizes clean, example coverage executed, census 71 (EX-0 1060) | squashed onto main, then rebased onto #613, #630 and #632; three full re-gates after the round-2 fix-up (facade 8171 / 8248, verify 0, pins 470, EX-0 1062) | **gated green at 05fceb17, CI running** |
| #625 | FNP-AGG-1 | 16 aggregate names | Muse ×1 (step 1) | — | red-first 162 failed / 4 passed | draft |
| #627 | FNP-11B | formats, TIME family, BL-13, BL-14, to_char family, make_timestamp widening | Muse ×3 (step 1 thin clone; step 2 Java-pattern parser in qa-build2; step 3 in progress) | — (step drafts; the unit's critic round runs at step 7) | step 2 f79cadd0: sizes, executed example coverage and verify clean; facade 240 reds all the unit's own open pins; to_date 22/22, to_timestamp 42/44, unix_timestamp 28/28; EX-FN-21 FIXED | draft |
| #628 | FNP-MATH-1 | bround conv mask collate sentences hash format_number AES, split facade half, BL-6 | Muse ×1 (step 1) | — | red-first 227 failed / 16 passed | draft |
| #629 | FNP-GEN-1 | inline, inline_outer, posexplode, posexplode_outer, json_tuple, from_csv, schema_of_csv; from_xml / schema_of_xml stay declared (Q-15B-1) | Muse ×1 (step 1) | — | red-first 32 failed / 5 passed | draft |

### Worker spend (from the runs.tsv files)

| PR | Grok reviewers (critic-logic, S2-21 perf) | Muse rounds |
|---|---|---|
| #623 FNP-6D-FOLLOWUP-1 | critic-logic $0.75, Rust perf $0.60 | 4 (qa-build) |
| #613 FNP-BITMAP-FACADE-1 + DEGREES-RUST-1 | verification critic $0.60, verification critic round 2 $0.59, Python perf $0.50 | 2 (qa-build) |
| #618 FNP-WIN-1 | critic-logic $0.59, verification critic round 2 $0.57, Rust perf $0.89, Python perf $0.49 | 8 (pa-build; 2 lost to provider stream stalls before any commit) + round 2 in progress |
| #625 / #627 / #628 / #629 step 1 | — | 1 each (thin clones) |
| FNP-11B steps 2–3 | — | 2 (qa-build2) |
| **Total, run 16a** | **$5.57** (9 runs; run 15a's first bitmap critic, $0.59 at 06:17, is not counted) | **18 completed or stalled rounds + 2 in progress**, Muse contributor at no metered cost |


## 3. Oracles recorded (live PySpark 4.1.2, this box, one JVM)

| Fixture | Cells | Purpose |
|---|---|---|
| `fnp_6d_followup_1_spark_oracle.json` (FU) | 50 | bitmap payload types, malformed casts, ANSI on/off |
| FU2 (appended) | 24 | NaN / ±Inf / 1e30 / huge-decimal CAST_OVERFLOW, overflow digit strings, `+1`, fixed-binary fold |
| `win_resid_spark_oracle.json` (R-*) | 39 | window/session_window residuals: America/New_York day buckets, DST sliding, month/year refusals, NTZ struct types, DATE input, zero/negative gaps, pre-epoch, startTime ≥ slide |
| `deg_spark_oracle.json` (DEG-*) | 193 | degrees/radians/toDegrees/toRadians over BOOLEAN, STRING, DATE, TIMESTAMP, BINARY, numerics, both doors, both ANSI settings; FU-and-float |
| `win_crit_spark_oracle.json` (C-L*) | 34 | FNP-WIN-1 critic claims: exact-gap sessions, NULL ts in select, signed startTime, dynamic NULL/zero/negative/month gaps, slide > window, nested/CTE/UNION SQL, session_window over DATE, window_time on a plain struct |

| `deg_inf_spark_oracle.json` (DEGI-*) | 120 | degrees / radians / CAST AS DOUBLE over 20 STRING shapes (Infinity/NaN spellings, suffixes, hex, signs, spaces), both doors, both ANSI settings |
| `win_crit2_spark_oracle.json` (C2-*) | 24 | FNP-WIN-1 verification round 2: window_time provenance through plan nodes, month-end / leap dynamic sessions, running-end chaining discriminator, two session specs |
**Oracle results that overturned a claim:**
- Critic L-003 on #623 (overflow digit strings should raise CAST_OVERFLOW) is refuted: Spark raises CAST_INVALID_INPUT.
- FNP-ALIAS-1's attestation that STRING `degrees` refuses is wrong for well-formed strings: Spark casts `'1.0'` on both doors.
- #613 verification critic L-010 (`'Inf'` / `'inf'` must refuse) is refuted: Spark answers ±Infinity on both doors and in CAST. The same recording found a gap the critic missed: Spark accepts Java type-suffixed strings (`'1d'`, `'1f'`), which RePark's kernel refused.
- FNP-WIN-1 critic L-001 (sessions exactly `gap` apart must split) is refuted: Spark merges them into one session.
- FNP-WIN-1 verification critic L-003 was filed as a P2 pin gap, and the critic did not re-record Spark. The live cell upgraded it to a P1. Spark chains dynamic-gap sessions by the running maximum of ends: `(10:00,'60 minutes'), (10:10,'1 minute'), (10:20,'5 minutes')` form one session. RePark's previous-row comparison split them. The same round's L-001 (`window_time` answering on a plain struct after filter / limit / sort / distinct / join / union) was confirmed on 9 cells.
- FNP-WIN-1's round believed no Spark 4 JVM existed on the box and pinned residuals differentially; the orchestrator recorded them live.

## 4. Rulings taken

1. **Run order inside the functions slice.** The first build clone (pa-build) resumed FNP-WIN-1 from the run-15 carry-over. A second build clone (qa-build) was opened under Q-15a-6 while free space was above 250 G, for FNP-6D-FOLLOWUP-1 and then #613.
2. **FNP-6D-FOLLOWUP-1 D-5 (concat-Utf8 HALT).** OR/AND refuse Utf8; the one FNP-6D C-011 arm moves to `CAST(concat(...) AS BINARY)`; `concat(BINARY, BINARY)` typing STRING is a registry residual owned by DOOR-CONVERGE-2 (run 16c, their clause C-001).
3. **FNP-6D-FOLLOWUP-1 critic.** L-001 confirmed live (NaN/Inf/1e30/huge-decimal raise CAST_OVERFLOW); L-003 refuted live; L-002 pinned as a builder-config ANSI-off expected divergence citing SET-ANSI-RUNTIME-1 (Q-15c-3); L-004 accepted; L-006 fixed-binary fold accepted.
4. **Q-15c-4 applied twice without raising a baseline.** #606's `check_example_coverage.py` and FNP-WIN-1's F-4 census fix both reflowed module prose instead of adding an exception row. The repark-functions crate-root ceiling 175 → 182 in FNP-WIN-1 is the one-time +N that PR needed.
5. **#606 carried over from run 15a and merged by this run** after three rebases (EX-0 1046 → 1052, map-row tails).
6. **#613 rebuilt by cherry-pick onto #623's squash** instead of merging the pre-squash follow-up branch; its Q1 (the follow-up ledger's grammar red) was ruled "leave it to its own unit", and #613 carries #623's missed ledger departure.
7. **DEGREES-RUST-1 (Q-15a-1) done as a Spark-exact Rust UDF pair on both doors**, after the verification critic's L-006 (BOOLEAN answered on the facade) and L-008 (STRING casts on both doors) were confirmed on 193 live `DEG-*` cells. The Python `cast("double")` goes (PYPERF-001).
8. **FNP-WIN-1 R-8 cross-run seam.** The one-line `spark_ast.rs` hook and `crates/repark-spark/src/time_window.rs` stay in this unit (run 16c confirmed); no further edits under crates/repark-spark.
9. **FNP-WIN-1 critic dispositions** follow the live `C-L*` cells: L-001 refuted and pinned as Spark's merge; L-002…L-008 fixed; the six Rust perf P2s fixed with measurements.
10. **Step-1 drafts on thin clones while both build clones were busy.** FNP-AGG-1 (#625), FNP-11B (#627), FNP-MATH-1 (#628) and FNP-GEN-1 (#629) each landed a ledger, a fixture and red-first two-door pins from a copied `.venv` and native, with no product code. This kept four families moving without a third build clone.
11. **FNP-MATH-1 D-6 scope.** `to_char` / `to_varchar` / `to_number` / `to_binary` moved into FNP-11B (D-7 there); the `split` facade half waits for run 16c's `spark_split` kernel.
12. **FNP-GEN-1 D-6 (Q-15B-1).** `from_xml` / `schema_of_xml` stay dated DECLARED refusals; LATERAL VIEW cells are listed as blocked on run 16c.
13. **Build-clone order after #613 merges.** qa-build takes FNP-11B step 2 (the shared Java-pattern parser) before FNP-AGG-1 step 2, following the owner's family order (FNP-11B is item 4, FNP-AGG-1 item 5).
14. **Disk.** At 247–250 G free, under the Q-15a-6 line, no new build clone was opened and qa-build was held to in-flight work; free space recovered to 280+ G as other runs removed clones.

15. **Fresh Muse session for FNP-WIN-1's remediation.** Two resumes of the unit's long-lived session (`01a0a478…`, running since run 15) stalled on `model stream idle timeout after 180000ms` after 30 and 2 tool calls. The other runs' Muse lanes streamed normally at the same time, so this was not a provider outage but most likely the session's context size. Neither stalled round had committed anything. The resume-never-restart rule protects in-flight context during an outage, so a fresh session with a self-contained brief (state, commits, dispositions, cells) loses nothing on disk.

16. **Oracle-found gaps get routed by ownership.** The `'1d'` / `'1f'` suffix gap goes back to #613's actor for its own `degrees` / `radians` kernel (P2, round 4). The same shapes break the shared SQL `CAST` with an internal `spark_float_stringify` engine error, which was handed to run 16c; they accepted it as C-006 of JAVA-DOUBLE-FD-1 (registry row JAVA-DOUBLE-CAST-SUFFIX-1).
17. **Conflicts on a queue-ready PR are resolved semantically, not by hunk.** #613 met #622's moved dispatch arm. The resolver took main's file and re-applied only #613's additions, then compared #613's delta over the old base with its delta over the new base. The only mismatch was a base-context artefact.

18. **No third verification critic for #613.** Round 4 only fixed a parse and added pins. Every shape it touches was measured against 160 live `DEGI` cells, with 0 mismatches under ANSI on and 0 under ANSI off. The orchestrator also probed `'1.5F'`, `'1e2d'`, `'Infinityd'`, `'NaNf'`, `'1dd'` and `'d'`, and two critic rounds had already converged with no open P1. The method's one critic-logic round per PR was met twice over, so #613 went to the queue after the re-gate instead of a third Grok round.

19. **#618 was squashed onto main before its final gate.** Its ten commits conflicted with main in `lib.rs` (the module list, and the analyzer-rule imports that #618 had moved into `registration.rs`) and in the CAP-1 mirror. A one-commit squash resolved those three hunks exactly once, with assertions that the dropped imports are unused, instead of re-resolving them at several replayed commits. Every `Authored-By` trailer from the branch is kept, and the pre-squash head stays on a local `wip/` branch. The precedent is run 15a's tree-identical squashes of #594 and #606. The PR squash-merges anyway, so main's history is unchanged.

20. **The second build clone came back when the disk did.** qa-build retired right after #613 merged, at 244 G free, under the Q-15a-6 line. The two PR branches were first proven tree-equal to their squash commits on main, and no alternates or processes depended on the clone. Removing its 38 G brought free space to 281 G, above the line again, so a fresh second build clone (`/tmp/qa-build2`) opened for FNP-11B step 2 (the Java-pattern parser) instead of waiting for #618 to free pa-build.

21. **No third Grok critic for #618.** Verification round 2's fixes (L-001 `window_time` provenance, L-003 running-end chaining) are pinned against 24 live `C2-*` cells on both doors, and the orchestrator re-probes both P1 shapes independently after the round. The method's one critic-logic round per PR was met twice. A third 30-minute Grok review would push #618's merge past the 19:30 lane cutoff with CI still ahead.

22. **#618's own size rows grew in verification round 2 (Q-15c-4).** `analyzer/time_window/mod.rs` went 1268 → 1416 and `test_fnp_win_1.py` 1209 → 1454 (ratcheted to 1449 by the orchestrator's fix-up), the rows this PR itself created. No row for a pre-existing file moved. Under Q-15c-4 that is still the one-time +N in the PR that needs it. Splitting both files now would push the merge past the 19:30 cutoff, so both split seams are carry-over debt with owners (section 9): extract the `window_time` grouping/provenance rule from `mod.rs`, and split the pin battery by clause family.
23. **Disk dipped to 239 G again at 16:41**, with three orchestrators' clones swinging it. `qa-build2` opens nothing new; it finishes FNP-11B step 2 as a gated draft push and then retires unless free space is back above 250 G.

24. **FNP-11B step 2 audited clean; qa-build2 ends on a gated draft.** e77580c8 adds a shared Rust Java-datetime-pattern parser (`java_datetime.rs`, 997 lines, 11 unit tests) behind `to_date` / `to_timestamp` / `unix_timestamp` with formats. Oracle pins: to_date 22/22, to_timestamp 42/44, unix_timestamp 28/28. The two remaining cells need double-quoted pattern literals on the SQL door, which is run 16c's parser. `unix_timestamp`'s `format` default became Spark's `'yyyy-MM-dd HH:mm:ss'`, and the API-freeze test accepts it. The facade shim is line-neutral at its 2235 baseline. Free space was 247 G, so the clone gates and pushes #627 as a draft instead of opening step 3.
25. **#618 verification round 2: the substance passed, three cleanups done by the orchestrator.** The fixes for L-003 (running-end chaining), L-001 (`window_time` provenance through plan nodes) and L-002 (month-end pins) passed audit. Three things did not:
    - every round-2 commit carried a malformed `Authored-By: Muse Code (muse-spark)` trailer instead of the owner's exact form;
    - L-004's Python-door message was fixed with a two-line pre-check in `crates/repark-python/src/dataframe.rs`, which is run 16b's binding (and run 16c's open #611 edits the same file), and it raised that pre-existing file's size baseline 1019 → 1021;
    - a follow-up added five `///` doc-comment lines to satisfy clippy.

    The orchestrator reverted the binding call and the baseline, removed the doc comments, and kept Spark's `_LEGACY_ERROR_TEMP_1039` text on the SQL door. The Python door's duplicate-name error became a recorded divergence with its seam named for run 16b. The four trailers were rewritten to the exact form non-interactively.

26. **#618 met #630 (IO-TEXT-1) after its fix-up gate had pushed.** The merge watcher saw CONFLICTING and was stopped before update-branch. The branch was rebased with `--onto` over main's squash: the EX-0 count was recomputed by hand (main 1057 → 1059, #618 +3 → 1062), and `crates/repark-python/src/map.md` resolved to main's text twice, because the round-2 row the conflict carried is the one the fix-up reverts. The diff-of-diffs against the pre-rebase branch differed only in the EX-0 line. The whole gate re-ran before the second push.
27. **EX-FN-21 flipped early.** FNP-11B step 2 answers `unix_timestamp`'s format argument, so the EX-28 call-time refusal pin went red and stopped the step-2 draft push. The pin was retired and registry row `EX-FN-21` flipped to FIXED in the draft (ledger D-11), ahead of the card's step-7 registry flip, pinned by the unit's oracle cells on both doors.
28. **FNP-11B step 3 opened at 17:30 on qa-build2.** Free space was back at 283 G, above the Q-15a-6 line, and the lane cutoff was two hours away. The round is bounded to `to_timestamp_ltz` / `to_timestamp_ntz` / `try_to_timestamp` on the step-2 parser (86 red cells) with a 19:00 hard stop, so it ends as a gated draft push before 20:15 either way.

29. **FNP-11B step 3 audited: the substance passed, one fence repair by the orchestrator.** c61a0341 adds `timestamp_ltz_ntz.rs` (735 lines) behind `to_timestamp_ltz` / `to_timestamp_ntz` / `try_to_timestamp` on the step-2 parser, destubs the names with their census obligations and an executed example, and flips the `try_to_timestamp` registry row. Oracle pins: 86/86 on both doors, both ANSI settings and both zones. To fit a new `pub mod` under the `repark-functions` crate-root ceiling, the round moved `install_shared_analyzer_rules` into `bool_decimal` and rewrote run 16b's `crates/repark-python/src/session.rs` call. The orchestrator kept the move, re-exported the function from the crate root (172 of 175 lines), and restored `session.rs` and its map row byte-for-byte, so no file outside the fence changes.

30. **FNP-11B ends the day three steps in, as a gated draft.** Step 3 pushed to #627 at 078fd4f2 with every facade red belonging to the unit's own later-step pins and 0 outside them. Steps 4–7 (the TIME family, BL-13/BL-14, the `to_char` family, the `make_timestamp` widening and the registry flips) are the carry-over, with the card and ledger as the brief.


## 5. Owner questions (with recommendations)

- **Q-16a-2 (second build clone and the 250 G line).** Free space hovered between 244 and 253 G most of the afternoon, with three orchestrators each holding one or two build clones (36–47 G each) and pruning reviewer clones promptly. Under Q-15a-6 that forces a working second build clone to retire after its PR merges even when the other orchestrators' clones are the swing. It delays the next Rust family (FNP-11B step 2) until the first build clone frees. *Recommendation:* count thin and reviewer clones against the line, and let a build clone with in-flight or next-queued work stay down to 230 G free; below that, retire it.


- **Q-16a-1 (`posexplode` / `posexplode_outer` parameter name).** The frozen 1.0 facade names the parameter `column`; PySpark 4.1.2 names it `col` (FNP-GEN-1 step 1, measured from the recorded signatures). Keyword callers (`F.posexplode(col=...)`) fail today. Renaming a frozen public parameter is a signature change that runbook §6 reserves for the owner. *Recommendation:* rename to `col` in FNP-GEN-1's step 2 as a freeze-update commit, the precedent being Q-15a-3's `make_timestamp` widening; positional callers are unaffected.

## 6. Rust-first roll-call (Python-only logic left in Python, with the reason)

Everything added today is in Rust behind thin wrappers:
- FNP-6D-FOLLOWUP-1 signatures and parsing: `bitmap_agg.rs`.
- DEGREES-RUST-1, which replaced a Python multiply with a Spark-exact Rust UDF pair on both doors, and its `'1d'` / `'1f'` suffix parse.
- `window` / `window_time` / `session_window` as analyzer rewrites and kernels, plus the SQL-door staging in `crates/repark-spark/src/time_window.rs`.

Python-only logic still in the functions slice, with the reason each stays for now:

| Where | What | Why it is still Python | Next |
|---|---|---|---|
| `functions_math.py::rint` | casts to DOUBLE in Python before the kernel, so BOOLEAN fail-opens (BL-6 facade half) | the same shape as the `degrees` bug fixed today; not in today's units | FNP-MATH-1 D-9 (#628) |
| `functions_expr.py::split` | `UnsupportedOperationException` stub | run 16c's `spark_split` kernel landed with #622 mid-afternoon; binding it belongs to FNP-MATH-1 D-8 | FNP-MATH-1 (#628) |
| `functions.py::expr` | display / projection name computed from the raw fragment text (C-029 backtick display) | handed over by run 16c's #611 as a strict-xfail pin | small follow-up after #611 merges |
| `functions_byname.py` resolver | tries the scalar dispatch and falls back to the facade on `ValueError` (PYPERF-002) | API plumbing, not a kernel | a later by-name pass |
| `functions_window.py::session_window` | wraps a static string gap in a `lit` Column (FNP-WIN-1 Python perf P3-1) | thin argument coercion; the duration parse is Rust | optional native string-gap overload |


## 7. Mechanics lessons

- A squash merge leaves the pre-squash commits outside main: rebasing a branch that merged them in replays them. Rebuild with `git rebase --onto origin/main <old-base>` or a cherry-pick of the unit's own commits, never a plain `git rebase origin/main`.
- `make py-test-facade` rebuilds a DEBUG native: reviewer clones must get a release rebuild after it.
- The CAP-1 prose pin reds on a numeric line ceiling written in any `map.md`.
- The harness refuses to delete the shell's current working directory; move the directory first.
- Main moved roughly every 20 minutes with three orchestrators merging; the smoke check takes ~45 minutes, so a PR was rebased up to three times before its queue slot.

- A Rust change on main stales a build clone's native even when it overlaps none of the unit's files: the example-coverage gate executes main's new examples (#618's re-gate failed on `repark._native.same_semantics` from DF-PLAN-INTROSPECT-1). Rebuild the release native after every rebase that brings Rust from main, before the executed gates.
- Probe with the unit's supported shapes before calling a regression. The orchestrator's first re-probe of FNP-WIN-1's remediation used two shapes RePark already has registered gaps for: dotted struct access (`F.col("window.start")` — the same failure as `F.struct(...).alias("s")` then `F.col("s.a")`) and `VALUES (…, TIMESTAMP'…')` literals (µs/ns schema mismatch). Four cells looked broken. Rebuilt with `createDataFrame` frames and `getField` / row access, all four matched Spark. Run a control shape without the new function before blaming it.
- GitHub creates check runs lazily as each workflow starts, so "no pending check runs" can be a false green. The #618 ledger-departure watcher reported green at 16:02 on 7 of 11 check runs, before Python, Rust test, Rust lint and smoke existed. Nothing queued on it. Every later queue decision used a helper that requires the nine required jobs by name, all completed success/skipped, stable for 90 s.
- A conflict on a queue-ready PR can arrive after its gate has already pushed. #618 was rebased three times in four hours (#630, then #632) and re-gated in full each time; the diff-of-diffs against the pre-rebase branch, line by line, is what made each re-resolution auditable — both times it reduced to the one line the rebase was supposed to change.
- A worker that must fit a crate-root ceiling will move a function and rewrite its callers, and a caller can sit outside the unit's fence. A re-export at the old path keeps the move and returns the foreign file byte-for-byte.
- Retiring a pin is part of implementing the thing it pinned. FNP-11B step 2 made `unix_timestamp`'s format argument answer, which turned a completed unit's refusal pin red and stopped the draft push; the registry row had to flip in the same commit.

## 8. Hand-offs received and carried

Run 16c handed three items to the functions slice during the day (full table: kept beside the lane briefs). #622 DOOR-CONVERGE-2 rewrites one 16a pin's refusal text and registers `spark_split` for FNP-MATH-1's facade half. #633 JAVA-DOUBLE-FD-1 (queued 19:04) carries the fix for the CAST suffix P1 this run reported, flips 16a's JDK-spelling `to_json` pin to equality, closes registry row FNP10-JAVA-DOUBLE-TEXT-1, and records that Java's `Formatter` has no `%F` (Spark refuses `format_string('%F', …)`) — noted in FNP-MATH-1's carry-over brief. #611 FNP-4B leaves C-029 (`F.expr` backtick display) and C-026 (HOF packing visible through `selectExpr`) as strict-xfail pins for 16a, and carries two already-ruled hunks in 16a files (no objection).


## 9. Carry-over debt and next-run briefs

| Item | Where | Brief / owner |
|---|---|---|
| Split `window_time` grouping/provenance out of `analyzer/time_window/mod.rs` (1416 lines under a one-time row) | crates/repark-functions | functions slice, next Rust lane |
| Split `test_fnp_win_1.py` (1449 lines) by clause family | python/repark/tests | functions slice |
| FNP-AGG-1 step 2 (any_value, max_by, min_by, product, kurtosis, skewness, mode) | #625 | `/tmp/oc-worker/run16a-carryover/fnp-agg-1-step2.md` |
| FNP-MATH-1 step 2 (bround, conv, hash, format_number, split facade half, BL-6) | #628 | `/tmp/oc-worker/run16a-carryover/fnp-math-1-step2.md` |
| FNP-GEN-1 step 2 (posexplode, posexplode_outer, inline, inline_outer; Q-16a-1 pending) | #629 | `/tmp/oc-worker/run16a-carryover/fnp-gen-1-step2.md` |
| C-029 `F.expr` backtick display; C-026 HOF packing through `selectExpr` (16c hand-offs) | functions.py / HOF rewrite | after #611 merges |
| FNP-11B steps 4–7 (TIME family, BL-13, BL-14, the `to_char` family, the `make_timestamp` widening, registry flips) | #627 | the card plus `task/ledgers/staging/fnp-11b-ledger.md`; steps 1–3 are on the branch |
| `format_string('%F', …)` refuses as Spark does; `%e` / `%g` / `%a` and multi-verb formats stay upstream residue (16c #633 hand-off) | #628 | `/tmp/oc-worker/run16a-carryover/fnp-math-1-step2.md` |
| Registry WIN-4: Python-door two `session_window` specs raise DataFusion's duplicate-name error where Spark raises `_LEGACY_ERROR_TEMP_1039`; the seam is `PyDataFrame::aggregate` | `crates/repark-python/src/dataframe.rs` | run 16b |
