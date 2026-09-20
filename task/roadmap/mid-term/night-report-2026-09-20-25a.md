# Run 25a report — every pin bump, the perf remainder, and the RePark halves of what the fork merged

**Run:** 25a (unit `night-25a`, lane prefix `qa-`). **Window:** 17:22 EDT 2026-09-19 → 2026-09-20. **Orchestrator:** claude-opus-5.
**Beside:** 25b (read-perf fork lane), 25c (parity RePark side), 25d (parity fork lane). Everything was coordinated through `/tmp/oc-worker/run25/claims.txt`.

**Actors**
- Claude Opus 5, high effort: ICE-META-DELETE-1 rounds 1 and 2, ICE-MERGE-APPEND-1 round 1.
- Muse Spark 1.3 contributor: ICE-BENCH-BASELINE-1, ICE-RM-DELETES-1, ICE-WRITER-METRICS-1.
- Orchestrator (me): both bumps, the storm re-measure, the merge-append oracle, two verification-finding fixes, the size-ceiling ruling and every gate.

**Reviewers:** Grok 4.6 verification critics, mutation first, one per PR that changes product Rust, plus two focused re-verifications.

**Box:** one build clone `/tmp/qa-build`, one git-only clone `/tmp/qa-git`, one worker lane `/tmp/qa-bs` (a `--shared` clone, reused across three units), and short-lived critic clones. Two cargo things at most, every build and gate through `build-slot.sh`, every JVM through `jvm-lock.sh`. **No AWS call of any kind.**

## 1. The list — before / after

| # | Item | Before | After |
|---|---|---|---|
| 1 | **RP-39** (fork `44834673`: #317 F-EXACT-COUNT-1, #318, #320) | main pinned `f3bdd598` | **MERGED** `ff5a13c3`, tree-equal. One RePark test task needed fork #317's new `FileScanTask::file_record_count` |
| 1b | **#721 ICE-COUNT-FOLD-1**, held as a draft since 24a | a folded `count(*)` answered **0** after `rewrite_data_files` (the fork's snapshot summary) | **MERGED** `1aef92cd`. The repro answers 1,000 at every stage; Q1 reads **no data file of any op** in every bench mode. Claim C-9 of the read-perf slate is closed |
| 2 | **ICE-BENCH-BASELINE-1** (#737) | the AWS "before" was never measured and the bench workflow runs from main only | **MERGED** `17241c7d`. `--baseline` turns page selection off and passes no shared caches; the `ice-read-perf-bench` job gains a boolean dispatch input. No dispatch made |
| 3a | **IPI-08 ICE-META-DELETE-1** (#739) | every predicate DELETE scanned `(_file, _pos)` pairs; a no-match committed nothing | **MERGED.** One Rust decision over the fork's `can_delete_using_metadata`, both doors, both modes; **71 of 72** recorded Spark cells equal, one declared. **Six of IPI-11's declared cells and registry row V3-COV-4 close** with it |
| 3b | **IPI-11 ICE-RM-DELETES-1** (#741) | `CALL rewrite_manifests` never rewrote delete manifests; `spec_id` was refused | **MERGED** `52f695f9`. The fork #318 opt-in is on and `spec_id` is honoured; unknown ids raise Spark's `Invalid spec id` |
| 3c | **The 16-way append storm** (#740) | registry said 5–7 of 16 and blamed the un-jittered backoff | **MERGED**. Re-measured at the jitter pin: **5–10, mean 6.7/6.5 over 64 repetitions, no lost rows**; the hypothesis is refuted, and `commit.retry.num-retries=20` commits **16 of 16 in 8 of 8** |
| 3d | **IPI-11 merge-on-commit ICE-MERGE-APPEND-1** (#744) | RePark used `fast_append` everywhere; both `commit.manifest*` properties were dead letters | every RePark-owned append door commits through `merge_append`; Spark's series matched at **9/9 probes × 3 variants**, and 120 appends got **29 % cheaper** |
| 3e | **IPI-09 ICE-WRITER-METRICS-1** (#745) | `write.metadata.metrics.*` ignored by every writer; position deletes lost Java's delete-type marker | **MERGED** `b6ff5c92`. Every writer applies the table's `MetricsConfig`; **12 of 12** recorded Spark cells equal, nothing declared |
| 4 | **`rpd_target_small`** | four strict xfails naming a fork ask | **DECLARED layout divergence** in RP-40: the rule is Java's, the residue is parquet-rs's bytes |
| 5 | **RP-40** (fork `882f1300`: #307, #314, #319, #321, #323, #322) | — | **MERGED** `40b7fc13`. Carries the `rpd_target_small` declaration and the RP-39 pin-history row its own PR had left out. **No bump is open at hand-over** |

## 2. Pull requests

| PR | Unit | Actor, rounds | Verification (Grok 4.6) | Comment gate | State |
|---|---|---|---|---|---|
| #735 | RP-39 bump | orchestrator | no product Rust | 0 | **MERGED** `ff5a13c3` 18:48, tree-equal |
| #721 | ICE-COUNT-FOLD-1 (rebased, bench pins moved) | 24a's Opus round + orchestrator | **PASS** — 20 shapes checked (deletes, DVs, time travel, branches, windows, `FILTER`, metadata tables, empty table); each half's mutation reds its own pins | 0 | **MERGED** `1aef92cd` 21:15 |
| #740 | The append-storm re-measure (docs) | orchestrator | measurement only | 0 | **MERGED** |
| #737 | ICE-BENCH-BASELINE-1 | Muse r1 + orchestrator | **PASS** + 1 P2 (the human header had its own copy of the flag) — fixed by reading the report document | 0 | **MERGED** `17241c7d` |
| #741 | ICE-RM-DELETES-1 (IPI-11) | Muse r1 | **PASS, 0 findings.** The critic re-derived the cell classification by hand; 4 mutations red | 0 | **MERGED** `52f695f9` 23:15, tree-equal |
| #739 | ICE-META-DELETE-1 (IPI-08) | Opus r1 (191 turns, $29.43), r2 (75 turns, $9.15) | r1 **NEEDS_REMEDIATION** (V-001 P1 the case flag could not bind; V-002 P2 a re-shaped MW-9 pin proved less) → both fixed → **PASS**, no counter-examples in the hunt; r2 audited field by field (41–42 fields per closed cell), **PASS**, no weakened assertions | 0 | **MERGED** 03:17 |
| #744 | ICE-MERGE-APPEND-1 | Opus r1 (178 turns, $19.13) | **PASS, 0 findings.** The critic re-measured all three series itself and ran 7 integrity checks; 4 mutations red including a manifest-dropping merge | 0 | **MERGED** |
| #745 | ICE-WRITER-METRICS-1 (IPI-09) | Muse r1 (halted for a ruling) + orchestrator | **PASS** + 2 P3, both answered in the tree (see §4) | 0 | **MERGED** `b6ff5c92` 02:34, tree-equal |
| #746 | RP-40 bump + the `rpd_target_small` declaration | orchestrator | no product Rust | 0 | **MERGED** `40b7fc13` 03:48 |

**Costs**
- Grok 4.6: **$13.79** across 9 verification runs (one 1-turn stall, resumed).
- Claude Opus 5 (high): **$57.72** across 3 actor sessions (444 turns).
- Muse: 3 rounds, on the subscription.
- AWS: **nothing.** No dispatch, no call.

## 3. What the measurements showed

- **The `count(*)` fold is safe now, and the fork fix is why.** `repro-rdf-summary.py` — MoR MERGEs → `rewrite_position_delete_files` → `rewrite_data_files` → `expire_snapshots` — answers 1,000 at every stage at RP-39. The `replace` summary still reads `total-records=0` at that pin (fork #322 fixes it, and RP-40 carries it), but the answer no longer depends on it. That is the whole point of F-EXACT-COUNT-1: the count comes from the planned files.
- **The append storm was misdiagnosed.** With Java's retry jitter on the pin the band moved 5–7 → 5–10 and the mean did not move (6.69 v2, 6.50 v3 over 32 repetitions each, no repetition lost a row). Raising `commit.retry.num-retries` to 20 commits all sixteen, in eight of eight. So the residue is the **four-retry default** — Java's own default — not a missing or broken retry loop.
- **RePark never merged manifests.** Spark's `newAppend()` is the merging producer: 120 sequential appends leave **1** manifest at the 100th, honour `commit.manifest.min-count-to-merge` and honour `commit.manifest-merge.enabled=false`. RePark left 120 manifests in all three variants and ignored both properties. Fixed for every RePark-owned door — and merging turned out to be **29 % cheaper in CPU**, because a short manifest list makes every later commit cheaper than the merge costs.
- **One append door is still the fork's.** A bare `INSERT INTO t VALUES …` never reaches a RePark commit site: the router hands a non-overwrite INSERT to DataFusion, which plans it on the fork's provider, and the fork's `IcebergCommitExec` commits through `fast_append()`. Measured on the branch: bare INSERT 120 manifests, `INSERT … BY NAME` 1 at the 100th. Fork ask **F-DF-MERGE-APPEND-1** is filed in claims with the oracle.
- **The metadata-delete route moves IPI-11 toward Spark.** Six of that unit's ten declared cells — `part_mor`, `part_mor_spec`, `part_mor_nocache`, each in v2 and v3 — now equal Spark literally, because RePark's whole-partition DELETE stops writing position deletes where Spark consolidates. Four still differ, each measured and explained (`evolved_spec_v2/v3`: Spark holds five live spec-0 files in one manifest where RePark splits them — filed as MANIFEST-4).
- **A metrics config must not reach the delete writer.** On a `write.metadata.metrics.default=none` table the DATA file carries no bounds at all, while the position-delete file keeps its exact `file_path` bounds. That is the behaviour that matters: a v2 parquet delete carries no `referenced_data_file`, so the reader routes on those bounds, and a config that stripped them would silently stop applying deletes. It is pinned now; before tonight it was only true by reading the code.
- **`rpd_target_small` is about bytes, not rules.** Fork #307 measured Java's selection rule as identical on Spark-sized files. A parquet-rs delete file of this shape is **1,297 B** where parquet-mr writes **1,590 B** — about 218 B of it parquet-mr's deprecated min/max statistics — so a 2,000 B target selects nothing on Spark's files and two on RePark's.

## 4. Rulings (G-2)

- **Q-25a-1 — the bare-`INSERT INTO` append door stays DECLARED with a fork ask.** The alternative, intercepting `Statement::Insert` in both routers to re-stage every bare INSERT through RePark's write-options path, would move distribution, file layout, sort-for-write and the conform path for *every* insert in the engine to buy one commit-site swap. Not that, not tonight.
- **Q-25a-2 — a size ceiling never moves up.** Muse halted rather than raise `merge/mod.rs`'s baseline by the one chain line its wiring needed; that was correct. The builder construction moved into `writer_props::name_matched_parquet_builder` instead, so the file ended at 1,756 lines — five *below* its old baseline — and the exception ratcheted down.
- **Q-25a-3 — the case-sensitivity flag handed to the fork is a named constant, not a derived bool.** RePark resolves every reference to the schema's own spelling before the predicate is built, so the derived flag could not be load-bearing (the critic proved it by inverting it with every pin green). The doors' case rules live in that resolution, which is now pinned on a mixed-case schema.
- **Q-25a-4 — a pin that lost its premise is re-shaped, never quietly kept.** MW-9's "six one-row files, one delete file" is answered from metadata now. The pin seeds six two-row files and deletes one row from each; that writes six delete files, one per touched file, on this tree and on main alike (verified by stubbing the route).

## 5. Owner questions (with recommendations)

1. **Q-25a-A — `codex-worker` cannot run on this box.** Its first live use failed at the API: `The 'gpt-6-astra' model requires a newer version of Codex. Please upgrade to the latest app or CLI` (400). No round ran and nothing was spent. I did not upgrade the CLI mid-run, with three other orchestrators sharing the box. *Recommendation:* upgrade the Codex CLI between runs, then do the smoke unit the skill describes before trusting it with a unit.
2. **Q-25a-B — the bare-`INSERT INTO` commit path (F-DF-MERGE-APPEND-1).** Until the fork's DataFusion commit exec uses `merge_append`, the most common insert door leaves one manifest per insert while every other door merges. *Recommendation:* take it in the next fork lane; the oracle and the measurement are recorded.
3. **Q-25a-C — the AWS half of the v1.5.0 re-measure gate** is still owed (the campaign's R-1). The baseline switch that makes a single head record both halves is merged, so one dispatch on main now produces a real pair. No run made a dispatch tonight.
4. **Q-25a-D — `column_sizes` differ from Spark on every metrics cell** that records them. They are compressed byte sizes from two different writers, like the RPD residue. *Recommendation:* leave them unasserted and recorded, as done.

## 6. STATUS.md

No edit. No STATUS clause names these units. The v1.5.0 gate line now says the release waits for full Spark–Iceberg parity (main `859c6506`), so the read-perf slate's units are no longer the last word on the tag.

## 7. Rust-first roll-call

Every decision is Rust: the metadata-delete seat (`repark-iceberg`), the merge-append routing, the metrics-config wiring, the `rewrite_manifests` legs, the count-fold literal typing. Python changed only in tests and fixtures. The bench baseline switch is bench Rust; the workflow input is YAML plumbing with no decision in it.

## 8. Machine, lanes and disk

- Clones: `/tmp/qa-build` (build + Opus actor rounds), `/tmp/qa-git` (git-only, pushes), `/tmp/qa-bs` (`--shared`, three Muse units then the RP-40 bump). Critic clones removed as their reports were read.
- `free -g` stayed above 80 GB; `/` went 1.3 T → 914 G free with four orchestrators on the box.
- Two cargo things at most; every build and gate through `build-slot.sh`; the Spark recorder under `jvm-lock.sh`.

## 9. Two things that cost time, for the next run's briefs

- **The pre-push hook blocks the owner's personal email.** A headless Opus actor committed six commits as the machine's default git identity (a personal address) because the lane's `user.name` was `John`. Set BOTH `user.name` and `user.email` in every lane clone before launching an actor; the fix afterwards is `git rebase origin/main --exec 'git commit --amend --no-edit --reset-author --quiet'`.
- **Two gates that briefs keep missing:** `python3 scripts/check_ledger_grammar.py` (it reads only the FIRST unit of a `pins:` line, and a staging ledger with no OPEN clause needs its `COVERAGE_ATTESTATION` block) and the CAP-1 census `test_cap_1_source_file_line_cap.py`, which mirrors every size-ceiling ratchet and fails CI if the mirror is not updated. Both are now in my briefs.


## 10. What RP-40 and the metadata delete closed on their way through

Three registry rows closed tonight as a side effect of other units, each measured rather than argued:

- **V3-COV-4** — "a MoR `DELETE` covering every row writes a full-coverage DV where Spark drops the file." ICE-META-DELETE-1 is exactly the file-coverage check that row was waiting for. The recorded cell `delete-all-rows-mor` flips DIVERGES → EQUAL, and RePark's re-measured answer is byte-identical to the recorded Spark half.
- **ICE-MERGE-APPEND-SUMMARY-1** — "a merging append stamps no `manifests-*` summary keys." Fork #322 stamps all three on every operation, so ICE-MERGE-APPEND-1's strict xfail xpassed at the RP-40 pin and became a plain assertion (`1 / 0 / 4` on the five-append `min-count-to-merge=5` table). Nothing was ever synthesised engine-side, which is why the row could wait for the fork.
- **Six of ICE-RM-DELETES-1's ten declared cells** — `part_mor`, `part_mor_spec` and `part_mor_nocache`, each in v2 and v3. They were declared because RePark's partitioned DELETE wrote position deletes where Spark consolidates; with the metadata route they equal Spark literally, audited field by field (41–42 fields per cell).

And one row that moved the other way, honestly: **ICE-RDF-RPD-TARGET-SMALL-1** stopped being a fork ask and became a DECLARED byte-size divergence — with the additional finding that its four cells are environment-sensitive (a longer warehouse path pushes the delete file over the 2,000 B target), so they are no longer `strict` xfails. The same head xfails here and xpasses on a CI runner; neither is a regression.


## 11. The closing measurement, on merged main `40b7fc13`

Everything this run landed was re-run together at the final head, against a release native built there:

- **`repro-rdf-summary.py`** — MoR MERGEs → `rewrite_position_delete_files` → `rewrite_data_files` → `expire_snapshots`: `count*= 1000` and `count(id)= 1000` at every stage. And the defect that held #721 as a draft is now fixed at BOTH layers: at RP-39 the `replace` summary still read `total-records=0` while the answer came from the planned files; at RP-40 the summary itself reads `total-records=1000, added-records=1000` (fork #322).
- **The five units' pins together: 228 passed, 2 skipped, 6 xfailed** (`test_ice_meta_delete_1`, `test_ice_count_fold_1`, `test_ice_merge_append_1`, `test_ice_rm_deletes_1`, `test_ice_rdf_options_1`). The six xfails are the four `rpd_target_small` byte-size cells, the bare-`INSERT INTO` fork ask, and ICE-META-DELETE-1's declared v2 delete-granularity cell — every one dated, registered and named.

**Hand-over state:** nothing of mine is in flight, **no pin bump is open**, and the fork has moved past `882f1300` (fork #325, #327, #329 and later), so **RP-41 is the next run's first item**. 25c's `uuid` v4 → v7 change is unblocked by RP-40 landing, since the root manifest is free again.

**Lanes:** `/tmp/qa-build` and every `qa-rv-*` review clone are removed (`/` went 816 G → 868 G free). `/tmp/qa-bs` (44 G, a release native at merged main) and `/tmp/qa-git` (349 M, git-only) are left warm for whoever runs next; remove them if they are not wanted.

## Orchestrating-session note (added when this report was filed)

- **The AWS pair is done.** At 04:00 EDT the orchestrating session fired dispatches #5 (`baseline=true`, run
  35498368567) and #6 (run 35498376542) on main `40b7fc13`; both succeeded. The table is in
  `docs/perf/ice-read-perf-baseline-2026-09-19.md`. Q-25a-C is closed; the read-performance wave is complete.
- **First gate scoreboard** (the 842-cell inventory replayed on main `40b7fc13` at 04:03 EDT): EQUAL 424 → 461,
  non-EQUAL cells Spark answers 298 → 257 (254 after carve-out C-1), zero regressions. Eleven procedure cells read
  DIFFERENT only because the harness does not yet normalise timestamps, uuid file names and a warehouse prefix.
- Run 25's b, c and d reports were written under `report-24{b,c,d}.md` file names by mistake; they are filed here
  under their right names.
