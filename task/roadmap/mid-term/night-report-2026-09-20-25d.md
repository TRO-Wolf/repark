# Run 25d report — 2026-09-19 → 20 night run: the parity campaign, fork lane two

**Run:** 25d (unit `night-25d`, lane prefix `qd-`). **Window:** 17:22 → 05:30 EDT. **Orchestrator:** claude-opus-5.
**Actors:** Claude Opus 5 at high effort (RDF-SORT-1 r1–r3, F-ADD-FILES-1 r1–r3: 597 turns, **$87.55**) and
Devin SWE-2 (free; F-WRITE-ORDER-TRANSFORM-1 r1–r2, F-CONTAINER-ACCESSOR-1 r1 + 2 resumes).
**Merges tonight (fork main):** `c4cc2f8f` #307 → `9cb63641` #314 → `f2635075` #321 → `1c487cc5` #323 → `19dbe013`
#325 → `11c0889f` #327 → `686d4856` #329, with 25b's merges interleaved. Seven fork PRs, every one tree-equal.
**Reviewers:** Grok 4.6 — a logic critic, a Rust perf reviewer and a verification critic with mutation per PR,
16 runs, **$9.33** (`/tmp/grok-worker/runs.tsv`, lanes `qd-rv-*`).
**Beside:** 25b on the same fork main (preamble 15), 25a holding every bump, 25c the RePark parity side.
**Box:** one GitHub fork clone `/tmp/qd-fork`, one `--shared` checkout `/tmp/qd-fork2`, one git-only clone
`/tmp/qd-git` for PR management; at most two cargo things; every local gate through `build-slot.sh`; every Spark
recorder under `jvm-lock.sh`. No AWS call of any kind. No `Cargo.toml` or `Cargo.lock` edit in any unit.

## 1. The list — before / after

| # | Unit | Before | After | Fork PR |
|---|---|---|---|---|
| 1 | F-RPD-TARGET-SMALL-1 | READY and verified by 24d, unmerged | **MERGED** `c4cc2f8f` — rebased, CI green, tree-equal. 25c's `rpd_target_small` xfails can go DECLARED at the next bump | #307 |
| 2 | F-LOCATION-1 (IPI-10) | DRAFT; CI red on `catalog::tests::test_table_commit` | **MERGED** `9cb63641`. Java measured (below): a commit carrying `SetLocation` writes the next metadata file under the NEW location, and keeps it under `write.metadata.path` when that property is set — the fork's behaviour is Java's and the pin encoded the pre-unit fork. Re-pinned, `catalog::` 208/208, a fresh verification critic PASS with the mutation red | #314 |
| 3 | RDF-SORT-1 (IPI-43) | `rewrite_data_files` had bin-pack only; `strategy => 'sort'` and `zorder` refused | **MERGED** `1c487cc5`. `RewriteStrategy { BinPack, SortByTableOrder, Sort(SortOrder), ZOrder(ZOrderSpec) }`; one global order per file group under a spilling external merge sort (128 MiB budget, fan-in 16, multi-pass); `ZOrderByteUtils` ported byte for byte with Java's NaN canonicalisation; Java's stamping rule; Java's refusals | #323 |
| 4 | WRITE-ORDER-TRANSFORM-1 (IPI-49) | `ReplaceSortOrderAction` was identity-only, so `WRITE ORDERED BY bucket(…)` had to be refused | **MERGED** `f2635075`. `sort_by(name, Transform, direction, null order)`, Java's width/type validation and message shapes, and the equal-order id reuse | #321 |
| 5 | F-ADD-FILES-1 (IPI-30) | no `add_files` anywhere in the fork | **MERGED** `19dbe013` — adopts parquet in place with Java's name mapping (created and committed when absent), hive partition parsing, the duplicate check and every refusal; 52 pins, 33 mutations | #325 |
| 6 | F-CONTAINER-ACCESSOR-1 (Q-23a-C / IPI-34) | `IS NULL` on a list, map or struct refuses ("Accessor for Field … not found") | **DRAFT HAND-OVER, do not merge as is** (§3) | #326 |
| 8 | F-RESIDUAL-NESTED-NAME-1 (found on the way) | on main: the residual of `st.category IS NULL` came back as `category IS NULL`, rebound to a different top-level column and folded to `AlwaysFalse` — rows silently lost on an ordinary scan | **MERGED** `686d4856` — one product line, a red-first pin that asserts the row loss, mutation shown; verification critic: "the fix is correct and the bug is real on a normal SELECT scan" | #329 |
| 7 | F-S3-FIXTURE-RACE-1 | the bucket-root listing test raced its neighbours (24d's owner question 2) | **MERGED** `11c0889f`, test-only | #327 |

## 2. Pull requests

| PR | Unit | Actor, rounds | Reviews (Grok 4.6) | Comment gate | State |
|---|---|---|---|---|---|
| #307 | F-RPD-TARGET-SMALL-1 | 24d's work; orchestrator rebase | verified by 24d | 0 | **MERGED** `c4cc2f8f` 17:53, tree-equal |
| #314 | F-LOCATION-1 | 24d's rounds; orchestrator: the Java measurement, the re-pin, the rebase | final verification **PASS** (mutation red; every catalog checked for a split between old and new location) | 0 | **MERGED** `9cb63641` 18:29, tree-equal |
| #321 | F-WRITE-ORDER-TRANSFORM-1 | Devin r1 (6 commits), r2 (the critics' three residues) | perf **LOOKS-GOOD** · verification **PASS** (5 mutations red) · logic NEEDS_REMEDIATION — **refuted by measurement** (Q-25d-2) | 0 | **MERGED** `f2635075` 22:08, tree-equal (one NOT-UP-TO-DATE rebase) |
| #323 | RDF-SORT-1 | Opus r1 (140 turns, $30.33), r2 (80, $9.59), r3 (52, $2.98) | logic **2 P1** (z NaN, dotted z column) fixed · perf **P1** (per-row clones) fixed · verification r1 NEEDS_REMEDIATION (3 uncaught mutations) → r2 **PASS** → focused r3 **PASS** | 0 | **MERGED** `1c487cc5` 22:47, tree-equal |
| #325 | F-ADD-FILES-1 | Opus r1 (166 turns, $26.11), r2 (115, $14.70), r3 (44, $3.84) | logic 5 findings (2 P1) fixed · perf 3 P2 fixed, 1 P3 deferred with the reason · verification: **no silent wrong answer**, 3 pin gaps → closed in r3, each mutation-proven | 0 | **MERGED** `19dbe013` 03:19, tree-equal |
| #326 | F-CONTAINER-ACCESSOR-1 | Devin r1 + 2 resumes (both earlier attempts died on provider connection errors with the tree uncommitted) | none — nobody has reviewed this head | 0 | **DRAFT hand-over** (§3) |
| #329 | F-RESIDUAL-NESTED-NAME-1 | orchestrator | verification **PASS on the fix**, one S2 (the pin asserted only the Display string) closed at once: it now asserts the rebound residual is not `AlwaysFalse` AND that the leaf name alone folds to `AlwaysFalse`; mutation re-proven | 0 | **MERGED** `686d4856` 05:07, tree-equal (one CI cycle lost to the legacy size ceiling of `residual_evaluator.rs`; the pin moved to its own file) |
| #327 | F-S3-FIXTURE-RACE-1 | orchestrator | none needed (test-only) | 0 | **MERGED** `11c0889f` 03:56, tree-equal |

Every pushed head reported `comment-ban hits=0`. I stripped no worker's comments; the two fix-ups I made myself were
mechanical (two co-author trailer lines an Opus round added — stripped by message filter, tree byte-identical; and a
machine path plus a gmail identity the same round's filter-branch introduced).

## 3. #326 — what the next run must decide

The unit's ledger, its 13 Spark cells (v2 and v3), the `javap` evidence that Java builds a position accessor from
every field's TYPE, and seven red-first pins are sound. The implementation went past its brief: it replaces the
reader's `PredicateConverter` leaf-index `RowFilter` machinery with an evaluation of the bound predicate on the
projected batch — **−580 lines in `crates/iceberg/src/arrow/reader.rs`**, the file 25b's read-performance campaign is
tuning, decided at the narrow implementation tier and reviewed by nobody. Keep it, re-scope it to the accessor change
alone, or take the design deliberately at the actor tier — but do not merge it as it stands.

It also found **a silent wrong answer that was on main today**: the residual rebuild (`unbound_reference`) used the
leaf `field().name` instead of the full `column_name`, so `st.a IS NULL` became `a IS NULL`, bound to a different
top-level column and folded to `AlwaysFalse`. I took that out as its own one-line PR with a red-first pin (#329, §1
row 8), so #326's decision no longer holds it up.

A second pre-existing bug is carded in `task/todo.md` by #325: the Arrow reader decides `hasIds` from the FIRST field
only, where Java's `ParquetSchemaUtil.hasIds` is a recursive ANY, so a parquet file with uneven embedded ids makes the
table unreadable (`Found duplicate 'field.id'`). `add_files` refuses such a source, so this action cannot create one;
another writer's migration still can.

## 4. What the measurements showed

- **`SetLocation` (#314).** Iceberg 1.11's `InMemoryCatalog` writes `00001-*.metadata.json` under the NEW location's
  `metadata/` after `ALTER TABLE … SET LOCATION`, and under `write.metadata.path` when that is set; the next commit
  and the manifest list follow it. Recorder and truth: the run-25d `set_location` oracle.
- **`rewrite_data_files` sort and zorder (39 cells).** One global order per file group (a small target file size
  yields output files with disjoint ascending ranges), not independently sorted runs. `sort_order_id` is the id of
  the table sort order — default **or any historical entry** — that equals the order used, and 0 otherwise; z-order
  always stamps 0. An unsorted table with no explicit order refuses. MoR deletes are applied and their delete files
  are kept. v3 `_row_id` and `_last_updated_sequence_number` survive. Java's option split: `sort` accepts
  `shuffle-partitions-per-file` and `compression-factor` and REFUSES the zorder options.
- **Java's z-order encoder.** `floatingPointOrderedBytes` calls `Double.doubleToLongBits`, which collapses every NaN
  — sign bit, payload, signalling — to the canonical one before the mask; `f2d` keeps the sign and payload, so the
  canonicalisation happens after widening. The fork now matches, proven against Java's own byte vectors.
- **`WRITE ORDERED BY` (22 cells).** Transforms serialize as `bucket[4]`, `truncate[10]`, `day`, `hour`, `year`,
  `month`; ASC defaults to nulls-first, DESC to nulls-last; an order equal to an earlier one **reuses that order's
  id**; `WRITE UNORDERED` returns the default to 0.
- **Q-25d-2, the width bound.** Both critics claimed Spark refuses a bucket width at or above `Integer.MAX_VALUE`.
  Measured: `bucket(2147483647, id)` is **accepted**; only `2147483648` is refused, and `bucket(0, id)` is refused.
  Spark's `findWidth` has two arms and the int arm has no upper bound. The finding is refuted and the fork's boundary
  was already Java's.
- **`add_files` (13 cells + 3 recorder rounds of new cells).** Files are adopted IN PLACE. Java maps columns by NAME:
  importing `(id, v)` into `(id, other)` SUCCEEDS and the rows read back as `id` plus NULL — a silent column drop that
  is now pinned rather than accidental. The name mapping is CREATED and COMMITTED when `schema.name-mapping.default`
  is absent. `check_duplicate_files` defaults true and refuses on the whole path, not the basename. Spark unescapes
  hive directory values (`unescapePathName`), so `cat=a%20b` adopts the value `a b`.

## 5. Rulings (G-2)

- **Q-25d-1.** #314's `test_table_commit` was re-pinned rather than the code changed: Java writes the next metadata
  file under the new location, measured, so the fork's behaviour is right and the pin was stale.
- **Q-25d-2.** #321's logic L-001 / verification V-02 (the `i32::MAX` bucket width) is REFUTED by measurement (§4).
- **Q-25d-3.** RDF-SORT-1's deliberate divergence: a NULL boolean encodes as eight zero bytes in the z value where
  Java fails the job. Declared in the ledger with the measurement.
- **Q-25d-4.** RDF-SORT-1's R-02 (a `Vec<u8>` key per row of one run) and R-04 (keys re-encoded when a spill is read
  back) stand as deliberate consequences of the design, with the reason in the ledger; the single-run merge that
  bought nothing was removed instead.
- **Q-25d-5.** F-ADD-FILES-1 carries two named divergences: a field id the table's schema does not carry is dropped
  rather than converted (D-3a, invisible to a Spark data scan), and the partitioned path's snapshot summary keeps
  `added-files-size` and `changed-partition-count`, which Java loses only because its executors write manifests
  (D-9, visible in `snapshots`). `added_files_count` is identical in every cell.
- **Q-25d-6.** I moved 25a's #737 to the end of the merge queue at 22:07 under the 45-minute rule (it had been at the
  head, BLOCKED, for 56 minutes while two fork PRs in a different repository waited behind it), with a note in
  `claims.txt` offering to move it back.

## 6. Owner questions (with recommendations)

1. **#326 (§3).** *Recommendation:* re-scope it — take the accessor change and the `unbound_reference` fix as two
   small PRs, and let whoever owns `arrow/reader.rs` decide the `RowFilter` question deliberately, after the v1.5.0
   re-measure. It is not a decision for the narrow tier.
2. **The remaining pre-existing reader bug**: `hasIds` is decided from the FIRST field only where Java's
   `ParquetSchemaUtil.hasIds` is a recursive ANY, so a parquet file with uneven embedded ids makes the table
   unreadable. `add_files` refuses such a source; another writer's migration still creates one. *Recommendation:* one
   small red-first unit (the `unbound_reference` half is fixed in #329).
3. **Devin's connection drops.** Three rounds died mid-flight with `cognition.ai/errorKind: unavailable` and an
   uncommitted tree; the third lost about ninety minutes of work that had to be redone from the same session.
   *Recommendation:* the devin-worker launcher should commit-on-exit (a `git commit -am wip` in the trap) so a drop
   costs a resume, not a re-run.
4. **An Opus actor round added co-author trailer lines** (two commits) and, when filter-branch was
   used to strip them, rewrote the identity to the owner's personal email — the pre-push hook caught both.
   *Recommendation:* the Opus brief's attribution line is being overridden by the harness's own attribution
   instruction; keep the explicit "never a co-author trailer line of any kind" sentence in every brief (it is there
   now), and have the launcher set `user.email` in the clone before the round.

## 7. Next run (fork side)

1. **#326's decision** (§3) — the only piece of this lane left open.
2. The rest of IPI-30's procedure family: `ancestors_of`, and the RePark CALL router for `add_files` and for
   `rewrite_data_files` `sort`/`zorder` (25c's side; both fork halves are on main).
3. The remaining pre-existing reader bug (`hasIds` first-field-only, §6.2).
4. `add_files` residues recorded in its ledger §9: ORC and Avro sources (Java dispatches on the format), nested-column
   resolution beyond the top level.
5. Q-24b-A (the fork writer honours `write.parquet.page-row-limit` / `page-size-bytes` / `row-group-size-bytes`) is
   still unclaimed.

## 8. STATUS.md / Rust-first

No STATUS.md edit (no fork clause there). Every behaviour change in this lane is fork Rust; no Python outside the
Spark recorders, which are measurement tools.

## 9. Costs

- **Claude Opus 5 actors:** 597 turns, **$87.55** (RDF-SORT-1 $42.90 over three rounds; F-ADD-FILES-1 $44.65 over
  three rounds).
- **Grok 4.6 reviewers:** 16 runs, **$9.33**. Two failure modes: the one-turn stall (twice on the same lane — a fresh
  spawn with "your first action is a tool call and your last message is the JSON" fixed it) and one provider 403 after
  20 turns, re-run fresh.
- **Devin SWE-2:** free tier, 5 recorded rounds.
- **AWS:** none.

## 10. Machine, lanes and disk

- `/tmp/qd-fork2` (the `--shared` dependent) and `/tmp/qd-fork` were removed in that order at 04:34, and every
  `qd-rv-*` review clone as soon as its report was read. **`/tmp/qd-git` is kept**: it is a git-only clone (43 M) that
  the #329 merge driver used; it can be removed now that #329 has landed.
- `/` had 1.3 T free at launch, 784 G at 04:36 and about 957 G free after this lane's clones came off.
- Durable artifacts under `/tmp/oc-worker/`: `qd-oracle/` (the Spark 4.1.2 recorders and truths: `set_location`,
  `rdf-sort` 39 cells + digest, `write-order` 22 cells + the width-bound probe, `containers` 13 cells, `add-files`
  13 cells + three round-2 recorders and the JVM probes), and `qd/` (every brief, hand-back, PR body and reviewer
  report).
- New script: `_lib/qd-review.sh` (the reviewer launcher, prefix `qd-rv-`).
