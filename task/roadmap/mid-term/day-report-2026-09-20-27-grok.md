# run27 report — IPI-51 error-condition / SQLSTATE parity (orchestrator xo-grok)

Written 2026-09-20 21:03 EDT at the 21:00 deadline. Lane `xb-err`. RePark only; no fork work; pin never bumped (`886b94c1`).

## Scoreboard

| metric | value |
|---|---|
| Inventory cells closed EQUAL (type + condition + SQLSTATE, post-merge replay) | **10** |
| Critic rejections (product NEEDS_REMEDIATION) | **0** |
| Void critic HALTs (1-turn, not merge) | 2 |
| Local gate failures | **1** (false-red; crate spec) |
| CI failures (all fixed before merge) | **4** (3 on #757, 1 ruff on #762) |
| Claims QUESTION lines | **0** |
| Grok critic cost | **USD 3.21** |
| PRs opened / merged | 4 / 4 |

## Units and PRs

One packet, four PRs by error family. Original branch name `fix/ipi-51-error-conditions`; each PR used a family branch on lane `xb-err`.

| n | family | PR | squash | merged | result |
|---|---|---|---|---|---|
| PR1 | D-1 parser in `errors.py` + constructor pins + ledger start | [repark#753](https://github.com/TRO-Wolf/repark/pull/753) | `692dfc83` | 2026-09-20 15:04Z | MERGED TREE-EQUAL. Mechanism only — **not** an inventory close. |
| PR2 | Catalogue in `repark-common` (A-3) + T-4 SQLSTATE string moves | [repark#757](https://github.com/TRO-Wolf/repark/pull/757) | `f16a7bcd` | 2026-09-20 18:45Z | MERGED TREE-EQUAL. Mechanism only — **not** an inventory close. |
| PR3 | `TABLE_OR_VIEW_NOT_FOUND` / 42P01 + `TABLE_OR_VIEW_ALREADY_EXISTS` / 42P07 | [repark#762](https://github.com/TRO-Wolf/repark/pull/762) | `3dd7b754` | 2026-09-20 21:35Z | MERGED TREE-EQUAL. Replay **7/7 EQUAL**. |
| PR4 | `INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED` / 42601 | [repark#764](https://github.com/TRO-Wolf/repark/pull/764) | `e0b91bca` | 2026-09-21 00:54Z | MERGED TREE-EQUAL (trees `29b94bad`). Replay **3/3 EQUAL**. |

Final origin/main at close: **`e0b91bcae994df1fd0a029491dde4792875168c3`**. Parent `9bfd03bf` (IPI-19/56/37, not ours). Pin **`886b94c1`**. comment-ban on the squash vs parent: hits=0.

## Before / after cell counts

Tick-1 measurement on this unit (not the packet's stale 118):

| | before (tick 1) | after (21:03, post-merge replay) |
|---|---|---|
| both-refuse | 126 | 126 (same cells still refuse) |
| same Python exception type | 46 | ≥ 56 on the 10 closed cells (all 10 type-EQUAL) |
| same `getCondition()` | **0** | **10** |
| Spark names a condition, RePark names none | **45** | **35** among the original 45 (10 flipped) |

EQUAL means type **and** condition **and** SQLSTATE all match the recorded Spark oracle. Spark was not re-run; oracles are `nc-inventory/out/spark-*.json`.

### Closed EQUAL (10)

**PR3** — replay 2026-09-20 17:44 EDT on squash `3dd7b754` (`ipi-51/replay-pr3/summary.json`):

| cell | type | condition | SQLSTATE |
|---|---|---|---|
| D-DROP-TABLE-MISSING-ERR | AnalysisException | TABLE_OR_VIEW_NOT_FOUND | 42P01 |
| W-DF-V2-REPLACE-MISSING-ERR | AnalysisException | TABLE_OR_VIEW_NOT_FOUND | 42P01 |
| W-DF-V2-APPEND-MISSING-ERR | AnalysisException | TABLE_OR_VIEW_NOT_FOUND | 42P01 |
| D-CREATE-EXISTS-ERR | AnalysisException | TABLE_OR_VIEW_ALREADY_EXISTS | 42P07 |
| D-CTAS-EXISTS-ERR | AnalysisException | TABLE_OR_VIEW_ALREADY_EXISTS | 42P07 |
| W-DF-SAVEASTABLE-ERRORIFEXISTS | AnalysisException | TABLE_OR_VIEW_ALREADY_EXISTS | 42P07 |
| W-DF-V2-CREATE-EXISTS-ERR | AnalysisException | TABLE_OR_VIEW_ALREADY_EXISTS | 42P07 |

**PR4** — replay 2026-09-20 21:02 EDT on squash `e0b91bca` (`ipi-51/replay-pr4/summary.json`). Native so mtime 19:23 (gate-232301 on unique tree `6e2c7d0c`); Python from merged main. Import of `_native.abi3.so` succeeded.

| cell | type | condition | SQLSTATE |
|---|---|---|---|
| D-TRUNCATE-PARTITION | AnalysisException | INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED | 42601 |
| D-SHOW-PARTITIONS | AnalysisException | INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED | 42601 |
| D-X-ADD-PARTITION-HIVE | AnalysisException | INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED | 42601 |

PR1/PR2 are not inventory closes. D-TRUNCATE-PARTITION's message already contained the condition before PR1; `getCondition()` was still `None` until the parser landed, and the SHOW / Hive ADD PARTITION sites were wrong-class until PR4.

## Numbered residues (not EQUAL at 21:00)

No cell is closed as DECLARED or "later".

### Remaining TAKE raise-sites this unit did not ship (26)

1. `D-SET-SERDE` — `NOT_SUPPORTED_COMMAND_FOR_V2_TABLE` / 0A000 (parse-time; needs preparse intercept)
2. `D-DESCRIBE-AS-JSON` — same (parse-time; `try_parse_describe_table` does not consume `AS JSON`)
3. `D-X-MSCK` — same (parses; router `_ => execute_passthrough`)
4. `CAT-ANALYZE` — same (passthrough)
5. `R-MT-PARTITIONS-V3` — `UNRESOLVED_COLUMN.WITH_SUGGESTION` / 42703
6. `R-MC-ROW-ID-V2-ERR` — same
7. `R-MC-CHANGE-TYPE-ERR` — same
8. `W-MERGE-STAR-MISSING-COL` — same
9. `W-INSERT-OVERWRITE-HIDDEN-PART` — same (INDEX decision 16: `_LEGACY_ERROR_TEMP_3060`)
10. `D-CREATE-DEFAULT` — `UNSUPPORTED_FEATURE.TABLE_OPERATION` / 0A000 (collides with repark#759; skip)
11. `D-CREATE-DEFAULT-V2` — same
12. `D-ALTER-DROP-DEFAULT` — same
13. `D-CREATE-LIKE` — `PARSE_SYNTAX_ERROR` / 42601
14. `D-ALTER-TYPE-COMMENT` — same
15. `D-X-CHANGE-COLUMN-RENAME` — same (INDEX decision 16: `_LEGACY_ERROR_TEMP_0034`)
16. `W-UPDATE-TYPE-ERR` — `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` / KD000 (A-8 class miss)
17. `W-DF-INSERTINTO-POSITIONAL` — same
18. `W-INSERT-WRONG-ARITY-ERR` — `INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS` / 21S01
19. `D-CREATE-NOT-NULL-VIOLATE` — `NOT_NULL_ASSERT_VIOLATION` / 42000 (A-8 class miss)
20. `D-ALTER-TYPE-NARROW-ERR` — `NOT_SUPPORTED_CHANGE_COLUMN` / 0A000 (A-8 class miss)
21. `D-ALTER-SET-NOT-NULL` — same (INDEX decision 16: `_LEGACY_ERROR_TEMP_2330`)
22. `P-CALL-UNKNOWN-ERR` — `FAILED_TO_LOAD_ROUTINE` / 38000
23. `P-CALL-BAD-TYPE-ERR` — `CAST_INVALID_INPUT` / 22018
24. `E-CASE-TABLE-NAME` — `REQUIRES_SINGLE_PART_NAMESPACE` / 42K05
25. `TY-GEOMETRY` — `UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED` / 0A000
26. `W-MERGE-DUP-SOURCE-ERR` — `MERGE_CARDINALITY_VIOLATION` / 23K01 (A-8 class miss)

PR5 sites (1–4) were measured tick 57 (`wo-xo-grok/pr5-measure-tick57.md`). Catalogue already has the condition. SERDE + DESCRIBE-AS-JSON fail at parse; MSCK + ANALYZE fall through `router.rs`. After #761, `execute_inner` strips `MERGE WITH SCHEMA EVOLUTION` first — intercepts must not disturb that. WO was not cut because #764 held `/tmp/xb-err` until 20:55.

### Deferred to other units (A-7 / packet D-5 / C-1) — 10

27. `R-MT-BRANCH-FILES` — IPI-23
28. `R-REF-BRANCH-META-SNAPSHOTS` — IPI-23
29. `R-DESCRIBE-HISTORY` — A-10: do **not** stamp `TABLE_OR_VIEW_NOT_FOUND` on a ParseException
30. `D-REPLACE-MISSING` — IPI-25
31. `W-DF-MERGE-SCHEMA-NO-PROP-ERR` — IPI-19/56/37 (now on main as `9bfd03bf`; this unit did not replay them)
32. `W-DF-EXTRA-COL-ERR` — IPI-19/56/37
33. `P-CALL-MISSING-ARG-ERR` — IPI-30/31
34. `P-CALL-DUP-ARG-ERR` — IPI-30/31
35. `P-CALL-UNKNOWN-ARG-ERR` — IPI-30/31
36. `W-STREAM-WRITE` — C-1 / v1.6.0 (`_LEGACY_ERROR_TEMP_1008`, INDEX decision 16)

### Py4J 20 (INDEX decision 17 — out of scope)

37. `CAT-TYPE-HIVE-NO-METASTORE-ERR`
38. `CAT-TYPE-REST-UNREACHABLE-ERR`
39. `D-ADD-COL-NOT-NULL`
40. `D-ALTER-TYPE-STR-ERR`
41. `D-DROP-PART-SOURCE-ERR`
42. `D-SET-FORMAT-DOWNGRADE`
43. `P-ADD-FILES-CHECK-DUP`
44. `P-EXPIRE-GC-DISABLED-ERR`
45. `P-POS-CHERRYPICK`
46. `P-PUBLISH-CHANGES-MISSING-ERR`
47. `P-REGISTER-TABLE-EXISTS-ERR`
48. `P-ROLLBACK-SNAPSHOT-MISSING-ERR`
49. `P-ROLLBACK-SNAPSHOT-NONANCESTOR-ERR`
50. `P-SNAPSHOT`
51. `TP-GC-DISABLED-PURGE`
52. `TY-PROMOTE-DATE-TS`
53. `TY-VARIANT-V2-ERR`
54. `W-CONCURRENT-VALIDATE-ERR`
55. `W-DF-V2-OVERWRITE-COND`
56. `W-INSERT-BRANCH-MISSING`

Ledger C-011…C-014 stay OPEN. No `COVERAGE_ATTESTATION`. D-2.3 PARSE caret (C-013) unshipped.

## Rounds per executor tier

Executors were Devin (free) or Muse Spark. No Claude model ran.

| Tier | Rounds | Work | Result |
|---|---|---|---|
| Devin SWE-2 | 6 | PR1 r1 `131859` + r2 `134512`; unused-import clerk `171106`; PR2 rebase clerks `175750` `181633`; PR3 inner `190057` session `polyester-arithmetic` | all CONCLUDED exit 0 |
| Muse Spark | 5 | PR2 catalogue `152020`; grammar clerk `162012`; clippy HALT `162944`; lockstep resume `163420`; **PR4 `215453` ACCEPTED** (5 commits) | 4 CONCLUDED useful; 1 HALT then resume |
| muse-clerk / glmflash | 0 on this lane after PR2 | — | — |
| Grok critic (`xreview.sh` / `xr-xb-err`) | 9 stamps | see table below | 7 PASS with turns≫1; 2 void 1-turn HALTs |

PR4 Muse commits on pre-rebase `6fb68b7b`: `694555a1` truncate, `a16db1c7` Hive ADD PARTITION, `0c23ff69` SHOW PARTITIONS, `4b80885a` constructor pins + C-011, `6e2c7d0c` clippy line-cap. Identity TRO-Wolf + `Authored-By: Muse Spark (muse-spark-1.3-contributor)` on all five. GitHub update-branch merge `b376aa45` vanished on squash.

## Critic verdicts (Grok, mandatory, via xreview.sh)

Verdict = first token of `structuredOutput.summary`. Merge requires turns ≫ 1.

| stamp | scope | turns | USD | verdict |
|---|---|---|---|---|
| 20260920T141435Z | PR1 mechanism | 25 | 0.662 | **PASS** (merge) |
| 20260920T160914Z | PR2 `f60d7af8` | 20 | 0.490 | PASS (not merge) |
| 20260920T165147Z | PR2 `c6348ddf` | 21 | 0.416 | PASS (not merge) |
| 20260920T171905Z | PR2 void | 1 | 0.015 | HALT (empty) |
| 20260920T172146Z | PR2 void | 1 | 0.004 | HALT placeholder |
| 20260920T172253Z | PR2 `55dbb5b6` | 21 | 0.338 | PASS (not merge) |
| 20260920T182822Z | PR2 `20879fc1` | 17 | 0.270 | **PASS** (merge #757) |
| 20260920T204754Z | PR3 `2f279abf` | 33 | 0.602 | **PASS** (merge #762) |
| 20260920T233448Z | PR4 `6e2c7d0c` | 21 | 0.413 | **PASS** (merge #764; unique tree unchanged after #761 auto-merge) |

Critic cost sum **USD 3.210**. Product rejections: **0**. Two 1-turn HALTs were process (xreview launched before the tree was ready / placeholder); not findings. Did not relaunch the PR4 critic after `gh pr update-branch` because the unique three-dot tree vs new main was still those five commits.

## Questions asked

None. Zero `QUESTION` lines in claims from this lane.

Measured in-lane (not claims, binding for later WOs):

- `repark-common` is a repark-spark **DEV-dep only**. Spark-door stamps go through IPI-21 `catalog_ops` siblings; sql door uses `spark_error::message`. Do not promote the crate.
- Spark three-part path quoting: `` `{catalog}`.`{namespace}`.`{table}` ``. sql door iterates parts.
- Sibling helpers flatten template `\n` to spaces (IPI-21). Parser wants col-0 `[CONDITION]` after at most one known prefix, and last `SQLSTATE`.
- PR5: stamp Plan-class AnalysisException, not NotImplemented. SERDE/DESCRIBE-AS-JSON need preparse; MSCK/ANALYZE need router arms. IPI-26 has not claimed those four. A-10 stamp here when cut.

## Gate failures

| id | where | what | disposition |
|---|---|---|---|
| local `gate-xb-err-225108` | PR4 | T=1 | **False-red.** Crate spec passed `cargo+test`; `local-gate.sh` then ran `cargo test -p repark-spark cargo test`. Product tests never invoked. Relaunch `gate-xb-err-232301` with `repark-spark:--offline+--lib,repark-sql:--offline+--lib` → **CB=R=T=U=L=0** on `6e2c7d0c`. Orchestrator process error, not product. |
| CI #757 | 3 jobs | lint/fmt/lockstep fallout after catalogue move | clerked; merge critic PASS on `20879fc1` |
| CI #762 | Python ruff E501 | one line | fixed `33c7661e`; required checks then SUCCESS |

Local gates that counted: `gate-xb-err-203309` PR3 all-zero; `gate-xb-err-232301` PR4 all-zero. xgate does **not** include fmt/clippy/ruff/`check_ledger_grammar.py`.

## Merge-path notes (process, not product)

- `drive-merge.sh` first field must be the **bare** number. Queue line `repark#761` deadlocked the 761 driver (`until` loop never saw a number). This lane rewrote it to `761 IPI-19-56-37 20:07` then appended `764 ipi-51-pr4 20:25` behind it — not a jump. #761 then TREE-EQUAL `9bfd03bf`.
- r9-merge on #764 saw BEHIND, `gh pr update-branch` produced merge commit `b376aa45`, CI re-ran, then squash TREE-EQUAL `e0b91bca`. Unique 20-file PR4 diff survived the #761 router.hunk auto-merge (`alter.rs` 1446 + CAP-1 python mirror intact).
- `/tmp/nc-build` was gone at replay time (xo-opus 20:32). Replay used `/tmp/xb-err/.venv` + `PYTHONPATH=/tmp/xb-err/python/repark/src` + the 19:23 abi3 so.

## What is in flight

Nothing from this lane. Queue empty of our lines. `merge-764-202542` inactive success. Critic `grok-xr-xb-err-233448` CONCLUDED. Clone `/tmp/xb-err` parked on `fix/ipi-51-error-partition-mgmt` @ `e0b91bca` (matches origin/main). V-001 pin-tighten WO exists (`wo-xo-grok/pr1-pin-tighten.md`) and was never launched. PR5 WO was never cut.

## What I would do next

1. Cut PR5 from `e0b91bca` on a **new** branch (not while overlapping a live PR on `router.rs`): `NOT_SUPPORTED_COMMAND_FOR_V2_TABLE` / 0A000 for `D-SET-SERDE`, `D-DESCRIBE-AS-JSON`, `D-X-MSCK`, `CAT-ANALYZE`. Preparse intercepts for SERDE and `DESCRIBE … AS JSON`; router arms for MSCK and ANALYZE. Do not disturb the #761 schema-evolution strip at the top of `execute_inner`.
2. Leave the DEFAULT family (`D-CREATE-DEFAULT`, `D-CREATE-DEFAULT-V2`, `D-ALTER-DROP-DEFAULT`) until repark#759 (IPI-26/27 rewrite-bugs) merges.
3. Then UNRESOLVED_COLUMN (5), PARSE_SYNTAX_ERROR (3), A-8 class misses, remaining TAKE. Keep C-011…C-014 OPEN; do not file attestation.
4. Optional V-001 pin-tighten on a third branch from origin/main.
5. Do not bump the iceberg-rust pin. Do not open fork PRs. Do not re-queue #753/#757/#762/#764.
6. Replay any new family only after TREE-EQUAL, with the same harness (`harness.py --engine repark --only …`) and `compare_*.py` requiring type + condition + SQLSTATE.

## House constraints that held

No source comments (comment-ban 0 on every hand-back and on the #764 squash). Commit identity TRO-Wolf / `64240326+TRO-Wolf@users.noreply.github.com`. Trailers name the model; never co-author trailer. map.md lockstep. No `make bump-fork-pin`. No `--no-verify`. No STATUS.md edits. `xreview.sh` `rm -rf /tmp/xr-xb-err` — never two critics on that clone. xpr body files avoided the substring that trips the co-author grep.
