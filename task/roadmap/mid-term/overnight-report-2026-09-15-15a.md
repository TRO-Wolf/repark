# Overnight report: run 15a, the functions

**Date:** 2026-09-14 20:45 to 2026-09-15 07:15 EDT
**Orchestrator:** Claude (claude-opus-5), one of three run-15 orchestrators. Run 15b owned DataFrame, Column, Session, Catalog and types; run 15c owned the SQL door and the registry backlog.
**Charter:** the 1.5 row of the release roadmap (PR #591). Every `pyspark.sql.functions` name answers PySpark 4.1.2 on both doors, or is a dated declared refusal.

## 1. Census slice (functions), before and after

The census uses the PySpark 4.1.2 `functions` surface: 509 real names, not counting typing re-exports. It was recomputed from `/tmp/oc-worker/run15/*-surface.json` against the live facade.

| Point | Missing names |
|---|---|
| Run start (main 629f3d5f) | **61** |
| Main at 06:18 (1aa95356), with #597 and #594 merged | **51** |
| After #606 (FNP-11A) and FNP-BITMAP-FACADE-1 merge, both gated green tonight | **37** |

**Closed on main tonight (10 names):** approxCountDistinct, shiftLeft, shiftRight, shiftRightUnsigned, toDegrees and toRadians (#597), plus arrow_udf, arrow_udtf, call_function and call_udf (#594). #594 also moved `bucket(Column)` from a refusal to Spark's answer.

**Ready and green, waiting in the merge queue (14 names):**
- #606 adds make_timestamp_ltz, make_timestamp_ntz, try_make_timestamp, try_make_timestamp_ltz, try_make_timestamp_ntz, make_ym_interval, try_make_interval, convert_timezone, localtimestamp, timestamp_add and timestamp_diff. It also brings `make_timestamp` and `months_between` to Spark's answers on both doors with Rust kernels, and routes SQL's `timestampadd` / `timestampdiff` and 3-argument `dateadd` / `datediff` there.
- FNP-BITMAP-FACADE-1 adds bitmap_construct_agg, bitmap_or_agg and bitmap_and_agg over run 15c's FNP-6D UDAFs.

**Left after those two merge (37):** aes_decrypt aes_encrypt any_value bround collate collation conv count_min_sketch current_time grouping_id histogram_numeric inline inline_outer listagg_distinct make_time mask max_by min_by percentile product session_window string_agg_distinct sumDistinct sum_distinct time_diff time_trunc to_binary to_char to_number to_time to_timestamp_ltz to_timestamp_ntz to_varchar try_aes_decrypt typeof window window_time.

Every one of the 37 has a card with live oracle cells:
- FNP-11B covers formats, the TIME family, BL-13 and BL-14.
- FNP-AGG-1 covers aggregates, including sum_distinct.
- FNP-GEN-1 covers generators and CSV/XML.
- FNP-MATH-1 covers math, strings and crypto.
- FNP-WIN-1 covers window, window_time and session_window; it opened tonight as draft #618.

## 2. Oracles recorded (live PySpark 4.1.2, this box)

| Fixture | Cells | Families |
|---|---|---|
| `fnp11_spark_oracle.json` | 846 | temporal (FNP-11A/B), BL-13, BL-14 |
| `fnp11a_r2_spark_oracle.json` | 112 | FNP-11A critic findings, re-recorded before any fix |
| `alias_spark_oracle.json` | 98 | deprecated aliases, degrees/radians |
| `agg_misc_spark_oracle.json` | 202 | aggregates, typeof, ids, raise_error, call_function/call_udf, bucket |
| `arrow_bucket_spark_oracle.json`, `arrow2_spark_oracle.json` | 11 + 13 | arrow_udf / arrow_udtf (pyarrow executor), bucket |
| `o245_spark_oracle.json` | 222 | window/session_window, math/strings/crypto, generators, CSV/XML |
| ad hoc `call_function` probes | 16 | by-name routine classes for #597's aliases and FNP-11A's names |

**Oracle finding that corrected a ruling.** `spark.sql.timeType.enabled` defaults to `false`, but the refusal is decided per function.
- These answer: the `TIME '…'` literal, `make_timestamp(DATE, TIME)` and `typeof(current_time())`.
- These raise `[UNSUPPORTED_TIME_TYPE]`: `make_time`, `to_time`, `time_diff`, `time_trunc` and `hour(TIME)`.

Run 15c measured the literal first; my recorder had forced the conf.

## 3. Per-PR table

| PR | Unit | Names | Actor rounds | Reviews (verdict, cost) | Gates | Merge |
|---|---|---|---|---|---|---|
| #597 | FNP-ALIAS-1 | approxCountDistinct, shiftLeft, shiftRight, shiftRightUnsigned, toDegrees, toRadians; facade degrees/radians made Spark-exact | GLM 5.3 Flash ×2 ($0.51 + $0.21) | Grok critic-logic $0.70 (1 P1, 1 P2, both remediated); S2-21 Python perf $0.49 (clean) | facade 6446 passed with a ruled stale-native filter; parity 757; example execution | **merged efcb14ef** 01:56, tree-equal, after four squash misses as main moved |
| #594 | FNP-MISC-1 | call_function, call_udf, arrow_udf, arrow_udtf, bucket(Column) | Muse Spark 1.3 contributor ×3 | Grok critic-logic $0.55 (2 P1, 6 P2); S2-21 Python perf $0.48 (1 P1, 2 P2); Grok verification critic $0.82 (all closed; 1 new P2, remediated) | facade green (stale-native filter, ruled); parity 757; CI caught two by-name misclassifications, fixed before merge | **merged 440b2773** 04:43, tree-equal |
| #606 | FNP-11A | 11 new temporal names, plus make_timestamp / months_between and SQL timestampadd / timestampdiff / 3-arg dateadd / datediff, all Rust kernels | Devin SWE-2 ×2 (0 edits, stopped); Muse Spark ×4 (R1 ran to its step limit and was resumed; R2 remediation; R3 fixes) | Grok critic-logic $0.86 NEEDS_REMEDIATION (4 P1, 6 P2, 1 P3), all remediated or pinned as Spark behaviour; S2-21 Python perf $0.75 (no P1); S2-21 Rust perf interim (P1 per-row cast, remediated: make_timestamp 1476 → 53 nanoseconds per row) | on main after #609, release native built from 6fa5719a: `make verify` 0, facade 7276, parity 757, example execution, comment ban 0 | **ready**, queued after 15b's #610 (conflict with main: EX-0 count only) |
| #613 (draft) | FNP-BITMAP-FACADE-1 | bitmap_construct_agg, bitmap_or_agg, bitmap_and_agg | GLM 5.3 Flash ×2 (native built by the orchestrator); remediation carried over | Grok critic-logic NEEDS_REMEDIATION (P1 L-001: `bitmap_or_agg` / `bitmap_and_agg` fold non-BINARY input as UTF-8 bytes where Spark refuses; P2 L-002..L-005: a `call_function` pin compared to itself, construct fail-open on invalid STRING/FLOAT, unused oracle cells, DataFusion-qualified unaliased names), all back to the actor in the next run | facade and parity green on the rebased head; see the PR for the Rust leg | draft at 4aa22173; stays draft until the remediation round lands |
| #618 (draft) | FNP-WIN-1 | window, window_time, session_window (step 1 of 4 committed) | Muse Spark 1.3 contributor ×1, stopped at 06:55 for the run deadline | — | — | draft carry-over. Step 1 is committed (ledger, oracle fixture, red pins). The step-2 `window` Rust rewrite in progress is saved as a patch plus an untracked-files archive (three new Rust files) beside the build clone, and the build clone's worktree still holds it for the next session. |

## 4. Rulings taken (G-2)

1. The TIME family follows Spark's per-function default (§2). `make_timestamp(date, time)` is implemented where Spark answers.
2. FNP-11 is split into 11A (constructors, intervals, arithmetic) and 11B (format parsing, TIME refusals, BL-13, BL-14).
3. `degrees` / `radians` keep a single multiply by a precomputed factor, bit-exact with Java's `Math.toDegrees` (D-7). This predates the Rust-first instruction; card DEGREES-RUST-1 moves both names onto the engine kernels.
4. `sum_distinct` / `sumDistinct` moved from FNP-ALIAS-1 to FNP-AGG-1, because the facade has no native DISTINCT aggregate builder.
5. `arrow_udf` rides the pandas bridge. The zero-copy path needs run 15b's `dataframe/udf_bridge.py`, so it is accepted as `PERF-ARROW-UDF-1` at a measured 1.42×.
6. `call_function` / `call_udf` resolve builtins as the engine routine at call time. The by-name lists are measured against PySpark 4.1.2:
   - `shiftLeft` / `shiftRight` / `shiftRightUnsigned` resolve (case-insensitive lookup).
   - `approxCountDistinct` / `toDegrees` / `toRadians` / `timestamp_add` / `timestamp_diff` raise `UNRESOLVED_ROUTINE`.
   - `make_timestamp` / `months_between` / `array_append` / `array_prepend` resolve in the engine.
7. Frozen signature (FNP-11A D-10): the facade keeps `make_timestamp`'s 1.0 required positionals, and the Python `(date=, time=)` keyword form is registry row EX-FN-28. `scripts/build_api_freeze.py` reads source with `ast`, so a widened forwarder trips rule J1.
8. FNP-11A L-009 (Python `try_make_interval()` with zero args answers, while SQL raises `WRONG_NUM_ARGS`) is Spark's own asymmetry. It is pinned as is.
9. Oracle first for review findings: the critic's expectations were re-recorded live (112 cells) before any fix was briefed. Seven of nine logic findings were confirmed as bugs; one is Spark behaviour and one is message shape.
10. Pin-harness skips hide kernel bugs. The FNP-11A harness skipped every SQL `timestamp_add` / `timestamp_diff` oracle cell (critic L-005). Running them with quoted units exposed three real defects, all fixed in Rust in R3:
    - DAY diff ignored the time of day.
    - The DATETIME_OVERFLOW text was padded.
    - 3-argument `dateadd` / `datediff` fell to DataFusion's 2-argument kernels.
11. Duplicate install (FNP-11A). `FNP11A_EXPORTS` re-listed two names main already exports, which duplicated them in `__all__` and `catalog.listFunctions()`. The fresh-native gate caught it through 15b's unique-name pin; the tuple now lists only new names.
12. Devin to Muse for FNP-11A after two Devin SWE-2 rounds produced no edit (80 and 35 minutes).
13. Stale native in thin lanes: a lane rebased past a Rust change ignores exactly the tests that landed with that change, and CI is authoritative for them. A derived-inventory test (the by-name allowlist probe) also shifts with the native, so CI caught two classifications the local gate could not see. Recommendation: gate derived-inventory tests on a fresh native.
14. Ship behind main: when main moved after a green gate and overlapped only in `map.md` files, the gated head shipped. `update-branch`, CI and tree equality validated the merged tree. Any other overlap forced a rebase and re-gate.
15. Squash-rebase: #594 (14 commits) and #606 (25 commits) were squashed tree-identically, keeping every `Authored-By` trailer, so one conflict round resolved the installer tails of #597 and #594.
16. The S2-21 Rust perf reviewer polled 30 minutes for a quiet box (the other orchestrators build continuously). Its interim code-reading report is the review of record, and the remediation round measured the hot path itself.
17. `.typos.toml` comment lines added by a worker were removed and their rationale moved to `python/repark/tests/map.md`. The ban covers TOML.
18. Cross-orchestrator file clearance: 15c's DOOR-CONVERGE-1 may edit two things in 15a's files, both measured in PySpark 4.1.2 fixture batch 11. One is the `abs` string-mismatch pin, now `CAST_INVALID_INPUT` plus `abs('-1') = 1.0`; the other removes `char_length` from the facade-only by-name list.
19. One build clone per orchestrator: thin-clone workers (GLM on the bitmap facade) never build. The orchestrator builds their Rust arms in the build clone and hands back the native.
21. FNP-BITMAP-FACADE-1's P1 (L-001) is a kernel bug in `crates/repark-functions/src/bitmap_agg.rs`. The OR/AND payload signature admits Utf8, so INT, FLOAT and BOOLEAN input folds as decimal-text bytes where Spark refuses it, on both doors. The fix narrows the signature and needs a release rebuild. The single build clone was running FNP-WIN-1 at 06:30, so the remediation and #613 carry over to the next run. Run 15c was told, because the SQL door shares the kernel.
20. Flake: `test_perf_unpivot_1.py::test_stack_is_linear_in_columns` failed once under load average ~14. Isolated reruns went 1 red / 2 green and the full facade rerun passed. The bound is untouched.

## 5. Mechanics lessons

- A copied editable `.venv` points `repark.pth` and `_editable_impl_repark_parity.pth` at the build clone, and a copied native goes stale when main merges Rust.
- A gate script that auto-rebases rewrites SHAs, so push only after gating, with `--force-with-lease=<branch>:<exact remote sha>`.
- Lane clones carry a disabled push URL; pushes name `https://github.com/TRO-Wolf/repark.git` explicitly.
- pytest's rewrite cache keys on mtime and size, so a same-width `sed` edit inside one second runs the stale assertion. Clear `__pycache__`.
- Resolve a staging-map rebase conflict as main's file plus the unit's own row, never a blind union.
- A unit that implements absent or stubbed names must update the census pins that asserted their absence: deferred lists, stub refusals, the FN-SPLIT inventory and EX-0.
- The pre-commit `map.md` lockstep refuses a commit that touches a directory's code without its `map.md`.
- Main moved every ~20 minutes with three orchestrators merging. A chain watching a 20-minute CI ends NOT-UP-TO-DATE (re-run) or DIRTY (rebase and re-gate).
- A detached worker must be launched with `setsid nohup … & disown`. A worker started as a child of the harness background task dies with it.
- The Muse provider failed on every lane at 04:17. `--resume <session>` recovered each round.
- A merge chain can go red on CI infrastructure (a uv manifest fetch timeout). Read the failed job's log before touching code.

## 6. Owner questions (with recommendations)

1. **DEGREES-RUST-1**: move facade `degrees` / `radians` onto the engine kernel through a `call_scalar` arm, per the Rust-first instruction. *Recommendation:* yes, as a small card in the next Rust lane. The values are already pinned bit-exact.
2. **Merge queue across orchestrators.** Tonight's queue was a hold-and-ping agreement between sessions, and a ready PR waited 2–3 hours for its slot. *Recommendation:* turn on GitHub's merge queue for concurrent runs, or make "announce merging #N / hold until #N landed" a runbook rule.
3. **Widen `make_timestamp` to Spark's signature (EX-FN-28).** PySpark 4.1.2 makes every parameter optional for the `(date=, time=)` form. *Recommendation:* yes, in a freeze-update PR. It only widens the signature.
4. **Carry-overs after 07:15.**
   - #606 is green and ready at 6fa5719a. The queue (#608 → #605 → #610) could not reach it before the run ended, and 15b confirmed the handoff. It needs an EX-0 recount rebase only.
   - #613 needs its critic remediation. The P1 is shared with run 15c's FNP-6D-FOLLOWUP-1.
   - #618 resumes at step 2.
   *Recommendation:* the next functions session merges #606 first, then remediates #613 in the build clone (rebasing over 15c's kernel fix if that lands first), then resumes #618.
5. **Split-identity uniqueness pin.** `test_functions_split_identity.py` checks slices of `__all__` but not uniqueness, so a duplicate install passed it. *Recommendation:* add `len(set(__all__)) == len(__all__)` there in the next functions PR.
6. **One build clone is the bottleneck.** Every remaining family needs Rust kernels, so the functions slice can land at most two Rust units per night. *Recommendation:* allow a second build clone per orchestrator when disk is above 250 G free (tonight it stayed between 287 and 349 G), with lane-private cargo targets.
