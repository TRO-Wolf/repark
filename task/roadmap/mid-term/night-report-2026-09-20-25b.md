# Run 25b report — fork lane one: the asks the RePark side is blocked on (night of 2026-09-19 → 20)

**Run:** 25b (unit `night-25b`, lane prefix `qb-`). **Window:** 17:23 → 05:30 EDT. **Orchestrator:** claude-opus-5.

**Actors:** Devin SWE-2 (swe-2-high, free tier), 8 rounds over 4 sessions (`/tmp/devin-worker/runs.tsv`, lanes `qb-fork`, `qb-fork2`). No Opus actor round was needed: every design ruling was fixed in the brief from a measured oracle.

**Reviewers:** Grok 4.6 — a verification critic with mutation on every PR before the queue, a combined logic + Rust-perf critic on the two large units, and one focused re-verify. 8 runs, **$7.11** (`/tmp/grok-worker/runs.tsv`, lanes `pb-rv-*`).

**Box:** one GitHub fork clone `/tmp/qb-fork`, one `--shared` checkout `/tmp/qb-fork2`, and (for one mechanical split I did myself) a git worktree `/tmp/qb-wt`. At most two cargo things at once, `CARGO_BUILD_JOBS=6`, every local gate through `build-slot.sh`, every Spark recorder under `jvm-lock.sh`. No AWS of any kind. No Cargo.toml or Cargo.lock edit in any unit.

## 1. The list — before / after

| # | Unit | Before (measured) | After | Fork PR |
|---|---|---|---|---|
| 1 | **F-RDF-SUMMARY-1** — wrong metadata on main | After MoR MERGEs → `rewrite_position_delete_files` → `rewrite_data_files`, the `replace` summary said `total-records=0`, `total-data-files=0`, no `added-*`, while 1,000 rows were live | **MERGED** `882f1300`. Cause: the added-file vectors were `mem::take`n by the manifest writers before the post-filter summary recompute. The summary now runs once, after the removed-delete set is final and before the vectors are consumed; and every commit carries Java's `manifests-created` / `-kept` / `-replaced` | #322 |
| 2 | **F-PAGE-PRUNE-2** (24b's hand-over) | Draft with two unpinned invariants | **MERGED** `457fd2c0`. V-001 (strip-when-deletes) and V-002 (an all-keep strip must not poison a shared footer-cache entry) pinned with a genuinely poisoning mutation; the `Arc::make_mut` mutant is recorded as equivalent | #319 |
| 3a | **F-BRANCH-SCHEMA-1** (IPI-07) | A branch read used the snapshot's schema; Spark uses the table's current schema (16 of 24 cells differed) | **MERGED** `ec30b2fd`. `Table::snapshot_ref` + `IcebergStaticTableProvider::try_new_from_table_ref`; branch (incl. `main`) → current schema, tag / snapshot id → snapshot schema, unknown ref → typed error. **Found on the way:** the current-table provider and `CREATE EXTERNAL TABLE` errored loud on `SELECT *` after a snapshot-less RENAME — fixed in the same PR | #324 |
| 3b | **F-OUTPUT-SPEC-ID-1** (IPI-06) | `merge_append` PANICKED on a two-spec commit (`debug_assert!(new_added_data.len() <= 1)`); the DataFusion write path was hard-wired to `default_partition_spec()` at every stage; a CoW DELETE/UPDATE restamped survivors under the default spec | **READY, re-verified, merging.** All three fixed with v2/v3 pins per door and per DataFusion stage; the re-verify reddened every mutant and closed V-01/V-03. Queued at `6a6861fc`; its CI was still running at 05:15 and the merge driver `fmerge-328b` was left running (§10) | #328 |
| 3c | **F-STAGE-ONLY-1** (IPI-05 fork half) | `stage_only()` existed on fast append and delete files only; no wap-id lookup, no publish primitive | **DRAFT, handed over (no critic yet).** `stage_only()` on merge append, overwrite files, replace partitions and row delta; `staged_snapshot_for_wap_id` with Java's unknown / non-unique / already-published messages; `Transaction::publish_changes(wap_id)`. 14 pins, 2 mutation legs | #330 |
| 4 | **F-RDF-SESSION-CONF-1** | The rewrite action takes no session writer/snapshot properties | **Not started** (time). Brief material is in 24c's report §4 Q-24c-6 | — |
| 5 | **Q-24b-A** — the writer honours `write.parquet.*` | Only codec and level are honoured | **Not started as code; the Spark oracle is recorded** (§4) | — |
| 6 | **Q-24b-B** — one NULL semantics for the core `TableScan` residual | — | **Design note written** (§5); no behaviour changed, as instructed | — |

## 2. Pull requests (all in the fork, TRO-Wolf/iceberg-rust)

| PR | Unit | Actor, rounds | Reviews (Grok 4.6) | Comment gate | State |
|---|---|---|---|---|---|
| #319 | F-PAGE-PRUNE-2 | Devin r3 (the two pins) | verify **PASS** (7 mutants red, 7 attack shapes green) | 0 | **MERGED** `457fd2c0`, tree-equal (one rebase after fork main moved) |
| #322 | F-RDF-SUMMARY-1 | Devin r1 (cause + fix + 11 pins), r2 (`manifests-*` on every operation), r3 (review findings) | logic+perf **PASS** (4 P3, 3 closed) · verify **PASS** (8 mutants red, 8 attack shapes, no findings) | 0 | **MERGED** `882f1300`, tree-equal. Two CI cycles: the file-size ceilings (I split the files myself) and one rebase |
| #324 | F-BRANCH-SCHEMA-1 | Devin r1 (30 pins), r2 (the critic's L-01/L-02) | logic+perf NEEDS_REMEDIATION (L-01 P2 = a pre-existing loud failure, L-02 P3) → fixed · verify **PASS** (7 mutation legs, 53 attack tests) | 0 | **MERGED** `ec30b2fd`, tree-equal. One CI cycle lost to a missing ASF header on two extracted test files (I added it) |
| #328 | F-OUTPUT-SPEC-ID-1 | Devin r1 (measure + fix + pins, ended at the step cap), r2 (mutations + ledger, ended at the cap), r3 (the three findings) | verify: every mutant red, V-01 S2 + V-02/V-03 S3 · focused re-verify **PASS**, no findings | 0 | **READY, queued** at `6a6861fc`; one CI cycle lost to `typos` reading the sha prefix `dbe` as a misspelling (cited as "#325" instead) |
| #330 | F-STAGE-ONLY-1 | Devin r1 (two hours of reading, zero commits — see §6), r2 ("stop reading, implement": 4 commits, 14 pins) | none yet | 0 | **DRAFT** at `b26c0ac8` |

**Costs.** Grok 4.6 **$6.50** across 7 runs (one 1-turn stall on the #319 verifier, resumed with the proceed follow-up). Devin SWE-2: free tier, 8 rounds. No Opus actor rounds. No AWS.

## 3. What the measurements showed

- **The RDF summary bug was one line of ownership.** `write_added_manifests` / `write_added_delete_manifests` consumed the added-file vectors with `std::mem::take`. When the delete sequence-number GC grew the removed set during `manifest_file`, the summary was recomputed — against empty added vectors. Every `total-*` on that commit was then wrong, and RePark's metadata-folded `count(*)` (#721) trusted it. The repair is structural: `summary()` runs ONCE, after `process_deletes` has finished and before the vectors are consumed.
- **Java stamps `manifests-created` / `-kept` / `-replaced` on EVERY operation, not only rewrites.** The actor's round-1 residue claimed the opposite; the recorded Spark oracle (`appends_rm_rdf`: four plain `INSERT` statements, each carrying `manifests-created=1` with `kept` climbing 0→3) refuted it, and round 2 extended the stamping. **This changes the summary key set of every RePark snapshot** — see §7.
- **A branch read is a schema question, not a snapshot question.** Spark: branch → the table's current schema; tag and snapshot id → the snapshot's. Measured over 24 cells (v2 and v3), including DROP, RENAME, a no-op widen, a branch write before and after the ADD, and a filter on the new column.
- **The current-table read was already broken on fork main.** `try_new_from_table` advertised the current schema but bound through the snapshot's, so `SELECT *` after a RENAME with no new snapshot failed loud (`Column data not found`) — through the static provider and through `CREATE EXTERNAL TABLE`. Nobody had measured that door; the branch-schema critic found it.
- **`merge_append` could not commit two specs at all.** Not a silent wrong answer: a `debug_assert` panic, and in release the carry loss silently drops files (the verifier's mutation (c) demonstrated the class).
- **Spark refuses `write.parquet.dict-size-bytes=0`** (`IllegalArgumentException: Dictionary page size must be > 0`) — a refusal cell for the Q-24b-A card, recorded with the rest.
- **`row-group-size-bytes` bites hard and `page-row-limit` does not show in row-group counts.** On 200,000 rows: default = 1 row group of 200,000; `65536` = 218 row groups (~877 rows each); `1048576` = 15 (~14,022 each). Page-level effects need the offset index, which pyarrow does not expose — the Rust pins must read it from the recorded Spark files.

## 4. Durable artifacts

- `/tmp/oc-worker/qb-oracle/` — the Spark 4.1.2 + iceberg-spark-runtime 1.11.0 recorders and truths:
  - `record_rdf_summary.py` → `rdf_summary_truth.json`: six programs (MoR merges → RPD → RDF → RM, default and `rewrite-all`, partitioned, appends, v3 DV), every snapshot's operation and full summary;
  - `record_write_props.py` → `write_props_truth.json` + the warehouse `wh-write-props/`: eight `write.parquet.*` cases with per-file row-group layout and encodings (the Q-24b-A oracle);
  - `repro.py` / `repro2.py`: the RePark-side reproduction of the wrong `replace` summary.
- `/tmp/oc-worker/qb-units/` — the three ask briefs, `spark_branch_schema_cells.json`, `spark_output_spec_cells.json`, and the Q-24b-B design note.
- `/tmp/oc-worker/qb-fork/`, `/tmp/oc-worker/qb-fork2/` — every brief, follow-up, reviewer brief and hand-back.

## 5. Q-24b-B — the core `TableScan` NULL-comparison design note (card only, as instructed)

Full note: `/tmp/oc-worker/qb-units/q-24b-b-design-note.md`. In brief: the fork's row-level residual uses Java `Evaluator` semantics (comparators ordered NULLS FIRST), so a NULL cell answers TRUE for `<`, `<=`, `!=`, `NOT IN`, `NOT STARTS WITH`. SQL is three-valued and drops those rows. Because page selection already SKIPS all-null pages for `<`, the core-door answer depends on whether pruning happened. **Java never evaluates a row-level residual with `Evaluator`** — it hands the leftover predicate to the engine, which is 3VL — so there is no parity argument for keeping it. Recommendation: make the row-level residual 3VL (Kleene `and`/`or`/`not`, coerce UNKNOWN → FALSE once at the top), keep nulls-first `Evaluator` where Java uses it (partition residuals, metrics evaluators, delete matching), and re-pin the truth table; the discriminating pin is the same query at the core door with `iceberg.row_selection_enabled` ON and OFF. Measure the missing-column case (audit BUG-002) against Spark before landing it — that is the one user-visible change beyond NULLs in data.

## 6. Rulings (G-2)

- **Q-25b-1.** The round-1 residue claim that Java stamps `manifests-*` only on rewrites was rejected against the recorded oracle, and round 2 extended the stamping to every operation. Parity with the measurement beats a smaller blast radius.
- **Q-25b-2.** The critic's `Arc::make_mut` mutant for #319's V-002 is EQUIVALENT (copy-on-write while the cache holds a reference); the pin was accepted once a genuinely poisoning mutation (writing the stripped metadata back into the cache entry) reddened it.
- **Q-25b-3.** L-01 on #324 (the current-table read) was a pre-existing defect outside the unit's brief. I took it into the same PR: it is the same knob, it was loud, and splitting it would have shipped a fix that only works on one door.
- **Q-25b-4.** Two CI-mechanical commits are mine, by the precedent of run 24d: the file-size split on #322 (moved code shed its comments, ceilings ratcheted DOWN or were removed) and the ASF header on #324's two extracted test files. No logic of any worker was touched.
- **Q-25b-5.** #328 stays a DRAFT with its findings filed rather than merging with an open S2. #330 stays a DRAFT because it has had no critic; the method rule (rule 8) is not waived for the last unit of a window.
- **Q-25b-6.** F-RDF-SESSION-CONF-1 and the Q-24b-A code were not started; the oracle for Q-24b-A is recorded so the next run starts from measurement, not from a survey.

## 7. For 25a / 25c / the orchestrating session

1. **The bump.** Fork main ended at `19dbe013` or later; this lane merged `457fd2c0` (#319), `882f1300` (#322) and `ec30b2fd` (#324). #322 unblocks 24a's held `count(*)` fold (#721): the `replace` summary it distrusted is now right, and #317's exact count already covers the read path.
2. **Expect summary-key pins to move.** Every RePark snapshot summary now carries `manifests-created`, `manifests-kept` and `manifests-replaced` (Java parity, oracle-backed). A RePark pin that asserts a closed key set will need updating at the bump; the fork's own suite was swept for this and one test was updated.
3. **IPI-07 RePark half** can use `Table::snapshot_ref` and `IcebergStaticTableProvider::try_new_from_table_ref`. The `branch_<name>` / `VERSION AS OF '<branch>'` spellings are RePark-side resolution onto that API.
4. **IPI-06 RePark half** waits for #328; the fork API is `with_output_spec_id(i32)` on the write path plus the resolver that raises Java's message. The option string parse (`NumberFormatException` for a non-integer) is RePark's.
5. **IPI-05 RePark half** can be written against #330's `Transaction::publish_changes(wap_id)` and `staged_snapshot_for_wap_id`, but #330 must get its verification critic first.

## 8. Next run (fork side), in order

1. **#328** — if the driver did not land it, re-queue it: gates and the re-verify are green, only CI and the queue remain (rebase first if fork main moved).
2. **#330 critics** — logic, Rust perf, then a verification critic with mutation; then the queue.
3. **Q-24b-A** `write.parquet.page-row-limit` / `page-size-bytes` / `row-group-size-bytes` / `dict-size-bytes` through one helper, with the recorded oracle and its warehouse; page counts read from the offset index in Rust, on both the Spark files and the fork's own.
4. **F-RDF-SESSION-CONF-1** — the rewrite action takes writer and snapshot properties from its caller (24c's IPI-14 residue, its strict xfails are waiting on it).
5. **Q-24b-B** — the 3VL residual card, after the missing-column cell is measured against Spark.

## 9. STATUS.md / Rust-first

No STATUS.md edit (no clause of mine). Every change in this lane is fork Rust; the only Python is measurement (the two Spark recorders). No dependency or feature was added, so nothing in this lane is blocked by the rustls/Cargo.toml gate.

## 10. Disk and lanes

**Left running at hand-over:** the merge driver `fmerge-328b` (systemd user unit) is waiting on #328's CI and will squash-merge it when green, writing `/tmp/oc-worker/fork-merge-328.done` (`RESULT=TREE-EQUAL <sha>` on success). Check that file first; if it says `CHECKS-RED` or `NOT-UP-TO-DATE`, rebase `/tmp/qb-fork` onto fork main, push and re-queue.

`/` had about 1.3 T free at launch. Still on disk at the end: `/tmp/qb-fork` (the GitHub clone, with its worktree `/tmp/qb-wt`) and `/tmp/qb-fork2` (its `--shared` dependent). Removal order: `/tmp/qb-wt` (`git worktree remove`), then `/tmp/qb-fork2`, then `/tmp/qb-fork`. Every branch in them is pushed. Every `pb-rv-*` review clone was removed by the launcher's next run; check `/tmp/pb-rv-*` before the next lane setup.
