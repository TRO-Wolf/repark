# Run 23b report: the RePark side and every pin bump, 2026-09-18 → 19 night

- **Run:** 23b (unit `night-23b`, lane prefix `nb-`).
- **Window:** 17:23 → 05:00 EDT.
- **Orchestrator:** claude-opus-5.
- **Actors:**
  - Muse Spark 1.3 contributor on 11 rounds.
  - Claude Opus 5 at high effort on CAST-MAP-SPELL-1 rounds 2–4, after Muse's round 1 was rejected.
- **Reviewers:** Grok 4.6, five critic runs (one post-merge logic critic, four verification critics).
- **Beside:**
  - run 23a, on the fork lane;
  - run 23c, the parity inventory;
  - run 23d, ICE-TT-RESOLVE-1.
- **Box:** one build clone (`/tmp/nb-build`) and one git-only clone (`/tmp/nb-git`). At most two cargo things at once. Every gate went through `build-slot.sh`.

## 1. Residual matrix — before / after, measured

| Row / claim | Before | After | Where |
|---|---|---|---|
| ICE-ARRAY-INSERT-1 (re-rating V2-10d "INSERT into a list column fails"; C-5/6/10 list exception) | At pin `3296ffc7`: `INSERT … VALUES` into `list<int>` / `list<struct>` failed loud (`It is not possible to concatenate arrays of different data types`). The `map_list` non-VALUES doors refused on CAST-MAP-SPELL-1. The other doors already answered. | **FIXED on main.** 30 Spark cells pinned (rows + parquet footer field ids), a live tier re-derives. `map_list` runs through `CASE WHEN` twins; the verbatim statements stay strict xfails on CAST-MAP-SPELL-1 (BACKLOG). | RP-29 #706 `177c8826` (fork #295) |
| ICE-OCC-SCOPED-1-INSERT-STORM (V2-20a remaining distance; unit ICE-APPEND-RETRY-1) | Registry: "Spark 16 of 16", rationale Q-21a-5 "rarely, not never" | **Corrected, BACKLOG.** Spark 4.1.2 commits 7–9 of 16 on InMemoryCatalog (32 repetitions) and 9–16 on Hadoop (all 16 in only 4 of 32). RePark commits 5–7 of 16 (10 repetitions). The fork already honours `commit.retry.*` (`Transaction::build_backoff`). No product change (Q-23b-1). | #707 `7667190e` |
| RANGE-TVF-ID-1 (re-rating side finding) | `SELECT id FROM range(10)` failed; column `value` | **FIXED on main.** Column `id`, non-null, 1–4 arguments, as on Spark (16 cells) | #708 `eaa5e67a` |
| ICE-WRITE-OPTIONS-1-R-RP | RePark always wrote `replace-partitions=true` | **FIXED on main.** A caller's value wins (5 Spark cells) | RP-30 #709 `6a140eb3` (fork #298) |
| ICE-ROWID-ORDER-1 (rating claim C-3, V3-02, V3-COV-3) | 6 distinct partition→first-`_row_id` mappings in 12 runs of `INSERT … SELECT` | **FIXED on main.** Deterministic 12/12 and equal to Spark on the a/b/c shape. Spark's general order is its hash-partitioner task order (8-category cell, run 23a): DECLARED divergence, pinned. V3-FILEORDER-1 brought true. C-3 is no longer false | RP-31 #710 `3e6a172c` (fork #300) |
| ICE-LIST-NULL-1 / -2 (Q-22a-D) | every nested `IS [NOT] NULL` DML failed `Accessor for Field xs not found` | **FIXED on main.** All 128 Spark cells answer rows and ids. #710 took 112 at fork #299. #715 took the 16 copy-on-write compound-predicate DELETE cells: that failure was RePark's own identity fast path, not the fork. Registered residues: 8 cells commit `overwrite` where Spark records `delete` (Q-23b-6), and 16 merge-on-read cells pack 1 delete file where Spark writes 2 | RP-31 #710 (fork #299) + #715 `9cada921` |
| RANGE-TVF-ID-2 (critic findings on #708) | `range(NULL)` → empty (silent); i64 overflow wraps and never ends (silent); INT/decimal/double arguments refused; numPartitions ≤ 0 accepted | **FIXED on main.** Every cell equals Spark (i128-counted `RangeTable`). P3 residue: `count(*)` over a huge range streams the elements | #712 `6feb1890` |
| ICE-RDF-COW-BYTES-1, ICE-RDF-DANGLE-2, RDF-DANGLING-1 | strict xfails / old "removed" behaviour | **FIXED on main.** Every cell equals Spark; 6 new Spark cells back the changed-meaning pins | RP-32 #711 `6a6f190c` (fork #301) |
| ICE-RDF-GRANULARITY-1 | 3 strict xfails | `target_small` **FIXED** (fork #302). `max_group_size` and `partial_progress_groups` stay xfailed; residual = parquet file size (fork writer ~47% larger; fork #306 in review) | RP-32 #711 |
| ICE-RDF-RPD-COMMITS-1 | 4 strict xfails (per-group commits) | **FIXED on main.** One commit, 8→8, 10 snapshots | RP-33 #714 `c6200f80` (fork #304) |
| CAST-MAP-SPELL-1 (owner list 20:58) | `CAST(… AS MAP<…>)` refused on every door (BACKLOG since 2026-09-06) | 21 + 17 + 8 Spark cells green on both SQL doors and the DataFrame door. Two verification passes: their silent wrong answers (legacy overflow, whitespace, key legality, DEL trim) are fixed. Residues are registered: EXPLAIN UDF name, the operand display, the NULL-key `try_cast`, and two native-door xfails. | #716 `b0fb6feb` |
| ICE-LIST-NULL-1 map seed, ICE-ARRAY-INSERT-1 `map_list` twins | substitute sources | **Verbatim Spark statements** | #716 |
| RP-34: fork #305 + #306 (parquet footer, dangling DVs) | — | **DRAFT** #717. The 3 remaining GRANULARITY xfails flip. New regression: `rpd_target_small` (4 cells) now rewrites 8→8 where Spark rewrites 0 (fork ask F-RPD-TARGET-SMALL-1). Clippy `large_future` ×3 at the new pin. Not mergeable tonight (Q-23b-8) | #717 draft |
| REF-2, MT-1, metadata columns (owner list) | DECLARED / refused | **Not started** (time). The REF-2 brief is ready: `/tmp/oc-worker/nb-build/brief-ref2-1.md`, built from 23c's measured cells | — |

## 2. Pull requests

| PR | Unit | Actor and rounds | Reviews | Comment gate | State |
|---|---|---|---|---|---|
| #706 | RP-29 bump → `9e67e000` (fork #295) + ICE-ARRAY-INSERT-1 | orchestrator bump; Muse r1 (114 steps); orchestrator CI fix (ledger attestation, 1 anchor) | — (bump + pins) | 0 | **MERGED** `177c8826` 19:21, tree-equal |
| #707 | ICE-APPEND-RETRY-1 (docs, fixture, recorder) | Muse r1 (72 steps, no build); orchestrator correction after a recorder replay | — (docs) | 0 | **MERGED** `7667190e` 18:40, tree-equal |
| #708 | RANGE-TVF-ID-1 | Muse r1 (199 steps); orchestrator bench migration | post-merge Grok logic critic **NEEDS_REMEDIATION** (2 P1, 2 P2; all measured true) — $0.82 | 0 | **MERGED** `eaa5e67a` 19:42, tree-equal |
| #709 | RP-30 bump → `18ab9761` (fork #297 + #298) + ICE-WRITE-OPTIONS-1-R-RP | orchestrator bump; Muse r1 (77 steps) | — (bump + pins) | 0 | **MERGED** `6a140eb3` 20:12, tree-equal |
| #710 | RP-31 bump → `50350e33` (fork #299 + #300) + ICE-LIST-NULL-1 + ICE-ROWID-ORDER-1 | orchestrator bump; Muse r1 (144 steps); orchestrator CI fixes (E501; parity docs pin) | — (bump + pins) | 0 | **MERGED** `3e6a172c` 21:38, tree-equal |
| #712 | RANGE-TVF-ID-2 (the #708 critic findings) | Muse r1 (131 steps) | Grok verification **PASS** (mutation 4/4 red; 1 P3 in the ledger) — $0.85 | 0 | **MERGED** `6feb1890` 22:28, tree-equal |
| #711 | RP-32 bump → `e3eef24f` (fork #301 + #302) + flips | orchestrator bump (in the git-only clone) + re-bump; Muse r1 (138 steps); orchestrator `target_small` flip | — (bump + pins) | 0 | **MERGED** `6a6f190c` 23:50, tree-equal |

| #715 | ICE-LIST-NULL-2 | Muse r1 (≈160 steps) | Grok verification **PASS** (mutation 10 red; 1 P3 in the ledger) — $0.94 | 0 | **MERGED** `9cada921` 02:56, tree-equal |
| #714 | RP-33 bump → `587d3592` (fork #304) + 4 RPD flips | orchestrator bump + flips (git-only clone) | — (bump + flips) | 0 | **MERGED** `c6200f80` 01:23, tree-equal |
| #716 | CAST-MAP-SPELL-1 | Muse r1 **rejected** (ceiling raise + Python logic); **Opus** r2–r4 (one resumed session, 4 invocations, 111 turns, $119.63); orchestrator merge with main (4 text conflicts) | Grok verification **NEEDS_REMEDIATION** ×2 (every claim measured; the fixes are red-first) — $0.95 + $0.69 | 0 | **MERGED** `b0fb6feb` 04:49, tree-equal (last CI fix `bf6b5038`: the parity cap census followed the two ratcheted ceilings) |
| #717 | RP-34 bump → `171deddf` (fork #306) | orchestrator bump | — | 0 | **DRAFT** (Q-23b-8) |

**Costs:**
- Grok 4.6: $4.25 across 5 runs (0.82 + 0.85 + 0.94 + 0.95 + 0.69).
- Claude Opus 5 actor: $119.63 across 111 turns. The first round-4 launch was stopped after about 2 minutes to correct its brief, and its cost is not in the log.
- Muse: 11 rounds, on the subscription.


## 3. What the measurements showed

- **Spark does not commit 16 of 16 concurrent appends.** The unit's premise came from one recorded repetition. Over three passes (six repetitions each, plus a replay of the committed recorder), Spark's InMemoryCatalog never exceeded 9 of 16. The Hadoop catalog reached 16 in only 4 of 32. Every loser is a `CommitFailedException`. So the retry wiring the brief made conditional was not built.
- **The fork already reads Java's retry budget.** `commit.retry.num-retries` (4), min/max wait, total timeout, factor 2. The remaining 5–7 vs 7–9 distance is likely the missing jitter; that is a hypothesis for a fork card.
- **Row-id order.** Spark was stable 12/12 on the a/b/c shape. The cause on RePark was fork `IcebergCommitExec` collecting data files in arrival order; 23a fixed it in fork #300. 23a's eight-category measurement (Q-23a-5) showed Spark's order is its hash-partitioner task order, so "Spark's order" in general is engine-specific.
- **ARRAY inserts on main.** Before the bump, 22 of 30 cells already answered Spark. Only `INSERT … VALUES` on list shapes failed, plus the CAST-MAP parser gap. CREATE succeeded for all three nested shapes, so no adopted Spark tables were needed.
- **Post-merge critic on #708.** Grok's four `range()` findings were each measured on Spark 4.1.2 and on RePark before ruling, and all four held. Two of them are silent wrong answers (NULL → empty; overflow wraps without end).
- **The list-null compound failure was RePark's, not the fork's.** My first read, from EXPLAIN, was wrong: EXPLAIN is planned by DataFusion. Execution of a compound WHERE takes RePark's identity DELETE fast path. 23a found this by reading the router, and #715 fixed it.
- **The measurements overturned three premises in CAST-MAP**, two of them mine:
  - PySpark `collect()` folds colliding map keys into one, but Spark's storage keeps both (`map_keys`).
  - Spark trims DEL but not the C1 control U+0085.
  - Spark's `try_cast` with an overflowing key returns a map with a NULL key.
- **RePark's scalar CAST lacks Spark's leaf semantics** on both doors. `CAST(' 1' AS INT)`, `CAST(128 AS TINYINT)`, `CAST('1.5' AS INT)` and `CAST('x' AS INT)` fail in the optimizer with Arrow cast errors, where Spark answers `1`, `-128`, `1` and NULL (verified by the orchestrator on the release native, ANSI off). Map casts now carry Spark's leaf rules in `cast_map/leaf.rs`; scalar casts do not.

## 4. Rulings (G-2)

- **Q-23b-1:** ICE-APPEND-RETRY-1 changes no product code. Spark does not commit 16 of 16; the row is corrected and stays BACKLOG.
- **Q-23b-2** (adopting 23a's Q-23a-5): the row-id pins assert determinism plus the a/b/c Spark cell. Spark's eight-category hash order is a DECLARED divergence, pinned.
- **Q-23b-3:** RP-30 went out with #297 + #298 instead of waiting for #300, so the build clone was not idle. #299 + #300 became RP-31. Each bump was the only one open at the time.
- **Q-23b-4:** the `map_list` cells whose recorded source spells `CAST(NULL AS MAP<…>)` kept the verbatim statement as a strict xfail on CAST-MAP-SPELL-1, plus a substitute-source twin measured identical on live Spark. #716 retires both.
- **Q-23b-5:** #302 folded into RP-32, because #301 and #302 both change `rewrite_data_files`.
- **Q-23b-6:** ICE-LIST-NULL-2's 8 copy-on-write OR cells commit `overwrite` where Spark records `delete` (rows equal). This is a registered residue.
- **Q-23b-7:** CAST-MAP L-004 (EXPLAIN shows the UDF name) and L-007 (the operand display) are P3 residues.
- **Q-23b-8:** RP-34 (#717) stays a draft. The `rpd_target_small` regression and the clippy `large_future` errors could not be fixed safely by 05:30.
- **Q-23b-9:** CAST-MAP L-008. Spark's `try_cast` with an overflowing key yields a map with a NULL key; RePark's loud refusal stays, registered.
- **Q-23b-10:** no third critic pass on #716. Every closing-critic finding is either fixed by a red-first pin against a measured Spark cell or registered.
- **Priority ruling:** the CAST-MAP Muse round was stopped 3 minutes in, so the RP-32 bump (list item 4) could take the one build clone first.

## 5. Owner questions (with recommendations)

1. **Q-23b-A — commit-retry jitter.** RePark commits 5–7 of 16 barrier-released appends where Spark's in-memory catalog commits 7–9. The fork already reads Java's retry budget; the likely difference is that Java randomizes each wait. *Recommendation:* a small fork card F-COMMIT-JITTER-1 (jitter on `build_backoff`), measured against `spark_occ_oracle4.json`.
2. **Q-23b-B — copy-on-write DELETE operation label.** On 8 recorded cells RePark commits `overwrite` where Spark records `delete` (rows equal). *Recommendation:* a fork unit that picks `delete` when a copy-on-write DELETE only removes whole files, as Java's `SparkCopyOnWriteOperation` does. Measure first.
3. **Q-23b-C — post-merge review gap.** #708 (the first `range()` fix) merged without the method's Grok logic round. The critic run afterwards found two silent wrong answers, and #712 fixed them. *Recommendation:* the orchestrator runbook should state that every PR changing product Rust gets a verification critic before the merge, including "side finding" units.
4. **Q-23b-D — `range()` statistics (P3, #712).** `count(*)` over `range(n)` walks n elements where Spark reads a row count. *Recommendation:* a v1.5.0 read-performance card, not tonight's scope.
5. **Q-23b-E — Muse on design-heavy units.** Muse's CAST-MAP round raised a size ceiling and put cast logic in Python, and was rejected. The Opus round did the design correctly. *Recommendation:* brief parser and type-system units at Opus tier from the start.
6. **Q-23b-F — scalar CAST parity (new, large).** RePark's scalar `CAST` has no Spark leaf semantics on either door (see §3). *Recommendation:* a unit, early in the next slate, that moves `cast_map/leaf.rs`'s Spark leaf rules into the shared scalar cast path. It is the same kernel, so the map and scalar answers cannot drift apart.
7. **Q-23b-G — the native `repark.sql` door's lexer.** It expands MySQL `/*! … */` comment hints in every statement (`SELECT 1 /*! 3 */, 2` refuses), and it types untyped integer literals as BIGINT where Spark uses INT. *Recommendation:* a native-door unit; both are pinned as dated strict xfails in `test_cast_map_spell_1.py`.

## 6. STATUS.md

No STATUS.md edit was made by this run.

## 7. Rust-first roll-call

| Unit | Left in Python | Why |
|---|---|---|
| RANGE-TVF-ID-1 / -2 | — | the table function, argument coercion and row counting are Rust (`repark-core`) |
| ICE-LIST-NULL-2 | — | the identity-path decline is Rust (`repark-iceberg`, both SQL doors) |
| CAST-MAP-SPELL-1 | a one-line forward in `Column.cast` to the native type-token parser | the parse, the rewrite and the element casts are Rust (`repark-functions`); round 1's Python logic was rejected |
| bumps, ICE-APPEND-RETRY-1 | tests, fixtures, recorders, docs | no product code |

## 8. Machine, lanes and disk

- **Clones:**
  - `/tmp/nb-build`, the one build clone: 72 G at 03:58, reused across 12 branches with a release rebuild after each switch.
  - `/tmp/nb-git`, git-only: 342 M. It did every bump (RP-32, RP-33, RP-34), every CI-fix commit, the merge of main into #716, and one ledger note, all while an actor held the build clone.
- **Grok clones:** every review clone was removed as soon as its report was read.
- **Load limits:** at most two cargo things at once, with `CARGO_BUILD_JOBS=6` and every gate through `build-slot.sh`. `free -g` never showed under 90 GB available.
- **Disk:** 1.3 T free at launch and 1.1 T free at the end. `/tmp/nb-git` was removed at 04:16. `/tmp/nb-build` (72 G) and the Spark scratch warehouse were removed at 04:49, after #716 merged. The actors' `/tmp/nb-*` probe scripts moved to `/tmp/oc-worker/nb-scratch/`. No `nb-` lane remains.
- **New script:** `_lib/nb-review.sh` (a copy of `lb-review.sh` with the `nb-rv-` lane prefix).
- **Durable artifacts:**
  - `/tmp/oc-worker/nb-meas/` (the storm, row-id and list-null probes and traces);
  - `/tmp/oc-worker/nb-castmap/`, `/tmp/oc-worker/nb-range/` (the Spark recorders and truths);
  - `/tmp/oc-worker/nb-build/`, `/tmp/oc-worker/nb-git/` (briefs, hand-backs, PR bodies, the rejected CAST-MAP round-1 WIP patch);
  - `/tmp/oc-worker/nb-rv-*/` (every critic hand-back).

## 9. End state (04:50)

- **Merged on RePark main tonight (10 PRs, all tree-equal):**
  - #707, #706, #708, #709, #710, #712, #711, #714, #715, #716 (merge order).
  - Five fork bumps are among them: RP-29 → RP-33, carrying fork #295 and #297–#304.
- **Open:** #717 (RP-34) DRAFT at fork `171deddf`. Its next run:
  - re-bump to fork main `43fcd243` (#305 + #306);
  - fix the three clippy `large_future` errors in `repark-spark`;
  - flip the three GRANULARITY xfails;
  - resolve `rpd_target_small` against Spark (fork ask F-RPD-TARGET-SMALL-1).
- **Not started:** REF-2, MT-1 and the metadata columns (owner list 20:58). The REF-2 brief is in `/tmp/oc-worker/run23/brief-ref2-1-for-next-run.md`.
- **Merge queue:** no line of mine remains. Claims were updated at every bump and merge.
