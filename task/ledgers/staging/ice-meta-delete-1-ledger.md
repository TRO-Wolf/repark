# Unit ledger — ICE-META-DELETE-1 · a DELETE that covers whole files deletes the files (IPI-08)

**Date:** 2026-09-19 · **Branch:** `fix/ice-meta-delete-1` · **Base:** `5770a0f0` (origin/main + the RP-39 pin bump)
**Model:** claude-opus-5 (round 1, steps 1–7; round 2, the IPI-11 rebase) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard** (a routing decision above the DELETE write path; no
table-format semantics of our own — the commit is the fork's `DeleteFilesAction`).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

Clause ids are the brief's C-1…C-8 written in the gate's three-digit form: C-001 = C-1, …,
C-008 = C-8. **C-009 is round 2's own clause:** ICE-RM-DELETES-1 (#741, IPI-11) merged into
main while this branch sat in the merge queue, and the routing changes the before-state its
replay pins enter the procedure with.

**Why now.** Owner direction: 1:1 parity with Spark's Iceberg integration. Spark decides,
ABOVE `write.delete.mode`, whether a DELETE can be answered by removing whole data files; RePark
never asked, so every predicate DELETE wrote position deletes / DVs (merge-on-read) or rewrote
files (copy-on-write), and a no-match DELETE committed nothing at all.

**Measured (the fixture).** Spark 4.1.2 + Iceberg 1.11.0, 18 shapes × {v2, v3} × {mor, cow} =
72 cells, each holding the surviving rows, every snapshot's operation and the summary counters,
and the live data files' record counts. Recorded by the orchestrator's
`/tmp/oc-worker/pd-oracle/record_meta_delete.py`, committed as
`python/repark/tests/ice_meta_delete_1_spark_oracle.json` (source SHA-256
`3dd30491a9d9f9c58c10a8dd61077e3c07ea8b92bb57ef2952bbff69133828c9`) with
`_record_ice_meta_delete_1.py`, whose `--check` mode re-derives every cell on the live tier.
**On `main` 29 of the 72 cells differed.**

**Spark's rule.** `SparkTable.canDeleteWhere(Predicate[])` converts every conjunct to an Iceberg
expression — a conjunct it cannot convert answers false — and then asks
`canDeleteUsingMetadata`: true when the predicate selects whole partitions, else true when every
data file the scan plans for that predicate STRICTLY matches it. True issues
`DeleteFiles.deleteFromRowFilter(expr)`: ONE `delete` snapshot that removes those files and
writes no delete file, in BOTH `write.delete.mode` values. A predicate that plans NO file is
vacuously true, which is where Spark's empty `delete` snapshot on a no-match comes from.

**What the cells show about the modes.** The decision is mode-independent, and the recorded
cells agree — with one reading that matters: under copy-on-write a whole-file match is
INDISTINGUISHABLE in the summary from a metadata delete, because a rewrite that produces no
survivor adds no file and Iceberg's `OverwriteFiles` stamps `delete` when nothing is added. So
the 29 cells that differed on `main` are the merge-on-read whole-file shapes plus the four
`no_match` cells (both modes); `not_in_whole_*` is Spark's own row-level answer on
merge-on-read, which this unit reproduces by REFUSING to translate a negation (below).

**The decision point (step 1).** Both doors reach the row-level path through
`repark_iceberg::write::predicate_dml::execute_predicate_dml`, but only for the shapes the
identity allow-list accepts (`try_allowed_delete_in`, `try_allowed_update_in`,
`plain::try_allowed_plain_identity` — a scalar comparison only). `DELETE FROM t`, `WHERE true`,
`IS NULL`, `LIKE`, and an `IN` list are planned by DataFusion onto the fork's `TableProvider`
instead, so a decision placed inside `execute_predicate_dml` would miss five of the eighteen
recorded shapes. The decision therefore sits one level up, at the two door seats that own a
`Statement::Delete`, and is ONE shared Rust function:

| seat | file | where |
|---|---|---|
| shared decision + commit | `crates/repark-iceberg/src/write/meta_delete.rs` | `try_meta_delete_target` → `delete_predicate` → `plan_metadata_delete` → `commit_metadata_delete` |
| Spark SQL door | `crates/repark-spark/src/router.rs` | `execute_delete`, after every existing refusal, before `execute_passthrough` |
| native `repark.sql` door | `crates/repark-sql/src/router.rs` | `execute_identity_or_delegate`, before the identity allow-list; the MoR multi-spec guard runs between plan and commit so the cheap G3-E8 subquery valve still fires first |

**The translation, and why it refuses negations.** `delete_predicate` translates EXACTLY or
declines: `AND`/`OR`, parentheses, `=`/`<`/`<=`/`>`/`>=` against a literal on either side,
`IS [NOT] NULL`, a positive `IN` list, `LIKE 'prefix%'` (Spark's `STARTS_WITH`), literal `TRUE`,
and a missing `WHERE` (`AlwaysTrue`). Every negation — `NOT`, `<>`/`!=`, `NOT IN`, `NOT LIKE` —
declines, because Iceberg's negated predicates MATCH a null where SQL's three-valued logic does
not, and the recorded `not_in_whole_v2_mor` / `not_in_whole_v3_mor` cells show Spark taking the
row-level route for exactly that shape. `BETWEEN`, functions, casts, arithmetic, `false`,
subqueries, unknown columns and non-primitive columns decline too. A declined predicate keeps
the row-level route, whose rows are always right; the cost of a decline is a snapshot shape, not
a wrong answer.

**Not in this unit:** `STATUS.md`, `Cargo.toml`, `Cargo.lock`, UPDATE (Spark has no metadata
UPDATE), MERGE, the merge-on-read delete-file rewrite (registry ICE-META-DELETE-1-D1), adding a
branch-targeted DELETE (C-007).

## PROPOSITION LEDGER — ICE-META-DELETE-1 — 2026-09-19

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A DELETE whose predicate strictly selects whole data files removes those files in ONE snapshot with operation `delete` and the oracle's `deleted-data-files` / `deleted-records`, writing no position delete, no DV and no rewritten data file (`total-delete-files` and `total-position-deletes` hold the oracle's values), in both `write.delete.mode` values, on v2 and v3, on both doors. | The `whole_one_file_*`, `whole_two_files_*`, `string_eq_whole_*`, `partition_select_*`, `partition_select_bucket_*`, `partition_plus_metrics_*`, `or_whole_*`, `is_null_whole_*`, `nondeterministic_like_*`, `prior_deletes_then_whole_*` cells (40 cells) of `test_ice_meta_delete_1.py`; Rust `a_whole_file_delete_removes_the_file_in_both_delete_modes`; native door `the_ansi_door_removes_whole_files_in_one_delete_snapshot_on_both_modes`, `the_ansi_door_answers_a_string_equality_from_metadata`; `fork_table_provider_delete_is_not_this_writer`. | PROVEN | Red on `main` for all 20 merge-on-read cells of those shapes (copy-on-write already stamped `delete` with the same counters — see "What the cells show about the modes"); green after step 3. The Rust pin runs the SAME decision under both mode properties, which is the mode-independence claim: `try_metadata_delete` never reads `write.delete.mode`. |
| C-002 | A DELETE that matches no row commits an empty `delete` snapshot — a NEW snapshot exists, operation `delete`, nothing added and nothing removed — on both modes and both doors. | Cells `no_match_v2_mor`, `no_match_v2_cow`, `no_match_v3_mor`, `no_match_v3_cow`; Rust `a_no_match_delete_commits_an_empty_delete_snapshot`; native door `the_ansi_door_commits_an_empty_delete_snapshot_on_a_no_match`. | PROVEN | Red on `main` on all four cells (`main` committed no snapshot at all). The empty snapshot is not a special case in the code: the fork's scan plans no file for `id = 99`, `can_delete_using_metadata` is vacuously true, and `DeleteFilesAction` commits with no match — Java's own path. The fork permits the otherwise-empty commit because the snapshot properties carry the operation id. |
| C-003 | A partial (row-level) DELETE is unchanged, and the metadata route never swallows one: the DECISION is made before the commit, never as a failed commit plus a fallback. | Cells `partial_only_*`, `mixed_partial_and_whole_*`, `partition_partial_*`, `prior_deletes_then_rest_*` (except the declared cell), `not_in_whole_*` (16 cells); Rust `a_partial_match_declines_before_any_commit` (asserts `plan_metadata_delete` answers `None` AND that the snapshot count is unchanged) and `the_translation_keeps_spark_answerable_shapes_and_refuses_the_rest`; native door `the_ansi_door_keeps_a_partial_delete_on_the_row_level_route`. | PROVEN | Green before and after on 15 of the 16; `prior_deletes_then_rest_v2_mor` is registry row ICE-META-DELETE-1-D1 (below). The order is structural, not incidental: `plan_metadata_delete` returns `Option<MetaDeletePlan>` and the only caller of `commit_metadata_delete` is a `Some` arm, so a PARTIAL match can never reach the fork action (which would fail non-retryably). |
| C-004 | `DELETE FROM t` with no predicate, and `WHERE true`, follow the `all_rows_*` cells. | Cells `all_rows_true_*`, `all_rows_nopred_*` (8 cells); Rust `no_predicate_and_literal_true_delete_every_file`; native door `the_ansi_door_deletes_every_file_without_a_predicate`. | PROVEN | Red on `main` for the four merge-on-read cells (position deletes over every row). A missing `WHERE` and a literal `TRUE` both translate to `Predicate::AlwaysTrue`, which the fork's action turns into "every data file". |
| C-005 | Prior deletes on the table (position deletes / DVs already present) do not push a later whole-file delete onto the wrong route. | Cells `prior_deletes_then_whole_*` (4) and `prior_deletes_then_rest_*` (4); Rust `a_prior_position_delete_does_not_change_the_route` (a real `execute_predicate_dml` row-level delete first, then the metadata route). | PROVEN (one declared cell) | `prior_deletes_then_whole_*` red on `main` for both merge-on-read cells, green after; the v3 cell also proves the fork drops the DV of the removed data file (`removed-dvs=1`), and the v2 cell that the parquet position delete correctly stays. `prior_deletes_then_rest_v2_mor` is DECLARED as registry row **ICE-META-DELETE-1-D1**: a v2 merge-on-read DELETE over a data file that already carries a position-delete file adds a SECOND delete file where Spark rewrites the superseded one (`added-position-deletes` 2 vs 3, `total-delete-files` 2 vs 1). Rows are equal; the difference is in the row-level delete writer, which this unit's decision sits above. Pinned as a STRICT `xfail` naming the row. **Open question for the orchestrator: schedule the delete-file rewrite (`write.delete.granularity`) unit.** |
| C-006 | The decision passes the session's case sensitivity to the fork, and a lower-cased / wrong-cased column reference behaves as it does on the same door today. | Rust `the_decision_binds_columns_with_the_doors_case_sensitivity`; native door `the_ansi_door_folds_an_unquoted_column_and_stays_exact_on_a_quoted_one`. | PROVEN | What is pinned, not invented: the Spark door passes `spark_door_case_insensitive(...)` (Spark's `spark.sql.caseSensitive=false` default, the same flag Java's `SparkTable` reads) so the reference folds onto the schema column and the fork is called with `case_sensitive = false`. The native ANSI door passes `case_insensitive = false`, as `commit_identity_dml` already does, so the fork is called with `case_sensitive = true`; to bind exactly as that door's own planner binds, an UNQUOTED reference is lower-cased first (DataFusion's default ident normalization) and a QUOTED one is taken verbatim. `"ID"` therefore declines the metadata route and keeps the door's own refusal, touching no file — measured, not assumed: the first spelling of this pin asserted a refusal for unquoted `ID` and was red. |
| C-007 | Branch writes: the branch is passed to `can_delete_using_metadata` and to the commit, or the ledger states that a door cannot target a branch today. | Reading + Rust `a_branch_selector_and_every_non_identity_clause_decline`. | PROVEN (stated, not added) | **No door can target a branch with a DELETE today, so no branch is passed and none is added here.** A branch-selector target is the four-part name `cat.ns.table.branch_x`; `try_meta_delete_target` accepts three-part names only, so such a statement declines and keeps the route it has on `main` (`plain.rs` already routes four-part names to the fork `TableProvider`). WAP is fail-closed on both doors (registry REF-3: `spark.wap.branch` / `spark.wap.id` cannot be set at all), so Java's `stageOnly()` arm of `SparkTable.deleteWhere` has no reachable counterpart either. The decision and the action consequently use `None` / `MAIN_BRANCH`, which is what every other write on these doors does. |
| C-008 | Reverting the routing (always taking the row-level path) turns C-001 and C-002 red; the exact mutation and the failing test names are recorded. | Step 5, below. | PROVEN | See "Mutation (C-008)". |
| C-009 | The routing changes the before-state of ICE-RM-DELETES-1's (IPI-11) partitioned `rewrite_manifests` replay cells, and it moves them toward Spark: SIX of the NINE cells IPI-11 could not replay literally now equal the recorded Spark cell on every recorded field, and each cell that still differs is named with what differs and why. | The re-measured `crates/repark-spark/src/tests/call_rm_deletes.rs` and `python/repark/tests/test_ice_rm_deletes_1.py` pins; the cell-by-cell table below; the C-009 mutation. | PROVEN | Nine Rust pins moved on the rebased branch. Six cells CLOSE (`part_mor`, `part_mor_spec`, `part_mor_nocache`, each v2 and v3): their DELETE statements each cover a whole data file, so RePark now enters the procedure with Spark's own data-only before-state and answers Spark's `(3, 1)`, layout, rows, `replace` and `manifests-created/kept/replaced` literally. Three cells still differ and keep a rule shape over RePark's measured before-state: `evolved_spec_v2/v3` (registry **MANIFEST-4**, new) and `part_mor_real_v2` (registry **ICE-META-DELETE-1-D1**, unchanged by this PR and never named in IPI-11's declared list — corrected here). `part_mor_real_v3` was already literal. The rows are read BEFORE and AFTER every CALL and are equal in all fourteen cells. |

## Round 2 — the IPI-11 cells this routing changes (C-009)

ICE-RM-DELETES-1 recorded 14 Spark 4.1.2 `rewrite_manifests` cells. FIVE replayed literally
on `main`: `unpart_mor_v2/v3` and `no_deletes_v2/v3` (the DELETE statements there are partial matches or
absent, so the metadata route does not fire and nothing moved) **and `part_mor_real_v3`**,
whose recorded before-state already matched RePark's. The round-2 brief counts ten
non-literal cells; the measured number is **nine** — it counts `part_mor_real_v3`, which the
IPI-11 ledger itself records as "pinned literally" and which this re-measurement confirms
equal on every field. The nine could not replay literally because
**RePark's partitioned DELETE wrote position deletes where Spark's cells show copy-on-write
consolidation**: the two engines entered the procedure with different tables, so those pins
asserted Spark's two-leg RULE over RePark's own before-state.

This unit's routing removes that cause wherever the DELETE covers a whole data file. Measured
on this rebased branch at `98faa2b6`, cell by cell, whole cell — returned row, before and
after layout, rows, operation, and the `manifests-*` summary — against
`python/repark/tests/ice_rm_deletes_1_spark_oracle.json` and `…_oracle2.json`:

| Cell | Outcome | Pin before round 2 | Measured now | Spark's recorded cell |
|---|---|---|---|---|
| `part_mor_v2`, `part_mor_v3` | **A — CLOSED** | rule: before 6 manifests (3 data + 3 delete), result `(6, 2)`, after `[(0,0,6,0),(1,0,0,3)]`, summary `replace 2/0/6` | before `[(0,0,1,0)×3]`, result `(3, 1)`, after `[(0,0,3,0)]`, rows `[2,3,6]`, `replace`, summary `1/0/3` | identical on every field |
| `part_mor_spec_v2`, `_v3` | **A — CLOSED** | rule: before 5 manifests (3 data + 2 delete), result `(5, 2)`, after `[(0,0,6,0),(1,0,0,2)]`, summary `replace 2/0/5` | before `[(0,0,1,0),(0,0,1,0),(0,0,2,0)]`, result `(3, 1)`, after `[(0,0,4,0)]`, rows `[2,3,5,6]`, `replace`, summary `1/0/3` | identical on every field |
| `part_mor_nocache_v2`, `_v3` | **A — CLOSED** | rule: result `(5, 2)`, after `[(0,0,6,0),(1,0,0,2)]`, summary `replace 2/0/5` (no before assertion) | before `[(0,0,1,0),(0,0,1,0),(0,0,2,0)]`, result `(3, 1)`, after `[(0,0,4,0)]`, rows `[2,3,5,6]`, `replace`, summary `1/0/3` | identical on every field |
| `evolved_spec_v2`, `_v3` | **B — moved, still differs** | rule: before/after 6 manifests `[(0,0,2,0)×3,(0,1,1,0),(1,0,0,1),(1,1,0,1)]`, result `(0,0)` | before/after `[(0,0,1,0),(0,0,2,0),(0,0,2,0),(0,1,0,0)]`, result `(0, 0)`, no new snapshot, op `delete`, rows `[2,3,4,5,6]` | `[(0,0,5,0),(0,1,0,0)]`, result `[0,0]`, op `delete`, rows equal |
| `part_mor_real_v2` | B — unchanged by this PR | before `[(0,0,2,0)×4,(1,0,0,1)×3]`, result `(7,2)`, after `[(0,0,8,0),(1,0,0,3)]` | the same — the DELETE statements there are partial matches and keep the row-level route | before carries one EMPTY delete manifest, after `[(0,0,8,0),(1,0,0,2)]` |
| `part_mor_real_v3` | already literal | literal | unchanged | equal |
| `unpart_mor_v2/v3`, `no_deletes_v2/v3` | already literal | literal | unchanged | equal |

**Why the six close.** Each seed INSERT writes one data file per `cat` value, so
`DELETE … WHERE id = 1` (and `= 4`, `= 5`) selects a file whose every row matches. Spark
answered those from metadata all along — its recorded `delete` snapshots carry
`deleted-data-files: 1` and no delete file — and RePark now does the same, so the manifest
layout the procedure sees is Spark's: the manifests holding the removed files are rewritten
in place with the entry dropped, and nothing else moves. The result row, the after layout,
the surviving rows and all three `manifests-*` counters follow.

**What still differs on `evolved_spec`, and why.** Spark's recorded before-state holds the
five live spec-0 data files in ONE manifest; RePark holds them in the three append manifests
(1 + 2 + 2). The total is the same, the spec ids are the same, the empty spec-1 manifest is
the same, the procedure's answer `(0, 0)` is the same, no snapshot is committed on either
engine, and the rows are equal. Only the spec-0 manifest COUNT differs, and it is neither
caused nor moved by this unit's decision. **Where Spark merged, measured rather than
assumed:** the recorded warehouse
(`/tmp/oc-worker/pd-oracle/wh-rm/ns/evolved_spec_v2/metadata/v8.metadata.json`) shows the
merge happening at the **INSERT after the partition evolution** — that `append` snapshot
carries `manifests-created: 2, manifests-kept: 0, manifests-replaced: 3`, i.e. it replaced
the three old spec-0 append manifests with one and wrote the new spec-1 manifest beside it.
Neither DELETE merged anything (both are `manifests-replaced: 1`). RePark's append after a
spec evolution merges nothing, which is the OTHER half of IPI-11's inventory row — "manifest
merging: `commit.manifest.min-count-to-merge` / merge-on-commit is not applied"
(`TP-MANIFEST-MIN-MERGE`) — untouched by ICE-RM-DELETES-1, which fixed the
`P-RM-DELETE-MANIFESTS` half. It was already there on `main`, where the same pin asserted
three spec-0 manifests; the routing only removed the two delete manifests standing beside
them. A second measurement from this tree shows the layouts are otherwise the same table:
RePark reaches Spark's exact before-layout — `[(0,0,5,0),(0,1,0,0)]` — the moment the
non-current spec is rewritten explicitly (`spec_id => 0`, the
`rm_deletes_non_current_spec_rewrites_that_spec` pin). Filed as registry row **MANIFEST-4**.

**One thing the routing did NOT change, checked rather than assumed.** Spark's `evolved_spec`
cell records `manifests-created/kept/replaced` on its `delete` snapshot; RePark's `delete`
snapshot carries no `manifests-*` key at all. That is pre-existing and route-independent —
RePark's row-level delete snapshots (`unpart_mor`, `part_mor_real`) carry none either, and
only the CALL's own `replace` snapshot writes them. It is therefore not a divergence this PR
introduces or moves, and no pin asserts Spark's value for it.

**Rows before and after every CALL.** All fourteen cells were probed with the live row set read
immediately before and immediately after the procedure. Every cell is equal across the CALL
(`part_mor` `[2,3,6]`, `part_mor_spec` / `part_mor_nocache` `[2,3,5,6]`, `evolved_spec`
`[2,3,4,5,6]`, `part_mor_real` `[1,2,3,4,5,6,10]`, `unpart_mor` `[2,3,5,6]`, `no_deletes`
`[1..6]`), and equal to the recorded Spark rows. No manifest rewrite lost or resurrected a
delete. The assertion is now part of every partitioned pin, on both doors.

## Mutation (C-008)

**The mutation.** In `crates/repark-iceberg/src/write/meta_delete.rs`, `plan_metadata_delete`
was made to answer "never a metadata delete" — the routing reverted to always taking the
row-level path — by inserting, as its first statement:

```rust
) -> Result<Option<MetaDeletePlan>> {
    return Ok(None);
    #[allow(unreachable_code)]
    let Ok(table) = catalog.load_table(&target.target).await else {
```

**Red under the mutation** (release native rebuilt, then reverted and rebuilt again):

- `cargo test -p repark-iceberg --lib meta_delete` → 3 passed, **5 failed**:
  `a_whole_file_delete_removes_the_file_in_both_delete_modes` (C-001),
  `a_no_match_delete_commits_an_empty_delete_snapshot` (C-002),
  `no_predicate_and_literal_true_delete_every_file` (C-004),
  `a_prior_position_delete_does_not_change_the_route` (C-005),
  `the_decision_binds_columns_with_the_doors_case_sensitivity` (C-006).
  The three that stay green are the pure-decision pins that must stay green — the partial
  match still declines, the translation table is unchanged, and the target claim is unchanged.
- `cargo test -p repark-sql --test ansi_meta_delete` → 1 passed, **5 failed**:
  `the_ansi_door_removes_whole_files_in_one_delete_snapshot_on_both_modes`,
  `the_ansi_door_commits_an_empty_delete_snapshot_on_a_no_match`,
  `the_ansi_door_deletes_every_file_without_a_predicate`,
  `the_ansi_door_folds_an_unquoted_column_and_stays_exact_on_a_quoted_one`,
  `the_ansi_door_answers_a_string_equality_from_metadata`.
  `the_ansi_door_keeps_a_partial_delete_on_the_row_level_route` stays green, as it must.
- `pytest python/repark/tests/test_ice_meta_delete_1.py` → **28 failed**, 44 passed, 1 xfailed:
  every `*_v2_mor` / `*_v3_mor` cell of `whole_one_file`, `whole_two_files`, `all_rows_true`,
  `all_rows_nopred`, `string_eq_whole`, `partition_select`, `partition_select_bucket`,
  `partition_plus_metrics`, `prior_deletes_then_whole`, `is_null_whole`, `or_whole`,
  `nondeterministic_like` (24 cells, C-001 and C-004), and all four `no_match` cells
  (C-002). The declared `prior_deletes_then_rest_v2_mor` still xfails, so the strict xfail is
  not hiding a regression.

**After the revert** all three are green again: 8 passed / 6 passed / `72 passed, 1 skipped,
1 xfailed`.

## Mutation (C-009)

**The mutation.** The same edit as C-008 — `plan_metadata_delete` made to answer "never a
metadata delete" by returning `Ok(None)` as its first statement, so every DELETE takes the
row-level route again:

```rust
) -> Result<Option<MetaDeletePlan>> {
    return Ok(None);
    #[allow(unreachable_code)]
    let Ok(table) = catalog.load_table(&target.target).await else {
```

**Red under the mutation.** `cargo test -p repark-spark --lib call_rm_deletes` →
**7 passed, 9 failed** — exactly the nine pins that moved on the rebase, and each one fails
at its before-layout assertion having returned to the value the pre-round-2 pin asserted:

| Pin | Layout under the mutation | = the pre-round-2 pin's |
|---|---|---|
| `rm_deletes_part_mor_v2` / `_v3` | `[(0,0,2,0)×3, (1,0,0,1)×3]` | yes |
| `rm_deletes_part_mor_spec_v2` / `_v3` | `[(0,0,2,0)×3, (1,0,0,1)×2]` | yes |
| `rm_deletes_part_mor_nocache_v2` / `_v3` | `[(0,0,2,0)×3, (1,0,0,1)×2]` | yes |
| `rm_deletes_evolved_spec_v2` / `_v3` | `[(0,0,2,0)×3, (0,1,1,0), (1,0,0,1), (1,1,0,1)]` | yes |
| `rm_deletes_non_current_spec_rewrites_that_spec` | the same six | yes |

The seven that stay green are the cells the routing never touches (`unpart_mor`,
`no_deletes`, `part_mor_real`, the unknown-`spec_id` refusal) — the partial-match DELETE
statements keep the row-level route with or without the mutation.

**The whole cell, not just the layout.** A failed assertion stops the test at the before-layout,
so the mutation run above cannot show the result rows and summaries returning too. It was
checked directly: with the mutation still in place, `call_rm_deletes.rs` was replaced by its
**pre-round-2 copy** (`git show 98faa2b6:…`) and the suite answered **16 passed, 0 failed**.
The old pins assert `(6, 2)` / `(5, 2)` / `(0, 0)` / `(3, 1)`, the old after-layouts, the old
`manifests-created/kept/replaced` and the same rows — so the routing accounts for the entire
move, on every field, and nothing else on the branch does.

**After the revert** (both files restored, `git status` clean): `cargo test -p repark-spark
--lib call_rm_deletes` → `16 passed; 0 failed`.

## Gates

| Command | Last line |
|---|---|
| `cargo test -p repark-iceberg` | `test result: ok. 589 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 29.33s` |
| `cargo test -p repark-sql` | `test result: ok. 369 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out` (lib) — 23 result lines, all `ok`, including the six `ansi_meta_delete` integration pins |
| `cargo test -p repark-spark` | `test result: ok. 1207 passed; 0 failed; 5 ignored; 0 measured; 0 filtered out; finished in 186.15s` |
| `pytest python/repark/tests/test_ice_meta_delete_1.py -q -p no:cacheprovider` | `72 passed, 1 skipped, 1 xfailed` |
| `pytest <the 27 facade files that issue a DELETE>` | `841 passed, 145 skipped, 5 xfailed` (step 4 tree, after the two retired premises) |
| `cargo fmt --all` | clean (no output) |
| `cargo clippy -p repark-iceberg --all-targets -- -D warnings -A clippy::disallowed_methods` | `Finished \`dev\` profile [unoptimized + debuginfo] target(s)` — the repo's own `rust-clippy` spelling; `--all-targets` without the `-A` reports the pre-existing `expect` in every test module, which `make rust-panic-ban` covers on `--lib` only |
| `cargo clippy -p repark-iceberg -p repark-sql -p repark-spark --lib -- -D warnings -D clippy::disallowed_methods` | `Finished \`dev\` profile [unoptimized + debuginfo] target(s)` (the panic-ban leg, live `disallowed-methods`) |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/qa-build origin/main` | `comment-ban hits=0` |
| `python3 scripts/check_ledger_grammar.py` | `ledger-grammar: 224 live ledgers clean (1922 clauses, 2499 pinned clause ids, 2 exception rows)` |
| `python3 scripts/check_docs_links.py` | `docs-links: 1027 files, 6117 links checked — clean` |
| pre-commit (every commit) | `map-sync: 310 maps clean`, `rust-file-size: 707 files clean`, `manifest: 18 components … agree` |

## Retired premises

Two pins asserted behaviour this decision retires. Neither was weakened — both were re-shaped
so they still exercise the thing they were written for:

- `crates/repark-spark/src/tests/delete_granularity.rs::fork_table_provider_delete_is_not_this_writer`
  (MW-9/C-005) deleted six one-row files and asserted one delete file. That DELETE is now
  answered from metadata, as Spark answers it. The pin now deletes ONE row of a two-row file —
  a genuinely row-level DELETE, which is what has no granularity knob — and asserts on the way
  that the whole-file `DELETE … IN (1..6)` writes no delete file at all.
- `python/repark/tests/test_rdf_schema_evo_1.py::test_rewrite_v3_deletion_vectors_after_evolution_matches_spark`
  (RDF-SCHEMA-EVO-1) needed a live deletion vector for `rewrite_data_files` to drop, and made
  one by deleting from a one-row file. It now seeds three two-row files, so its DELETE is still
  a partial match and still writes the DV (rewritten files 6 → 3).

## Open questions for the orchestrator

1. **ICE-META-DELETE-1-D1** (registry, DECLARED 2026-09-19): schedule the merge-on-read
   delete-file rewrite — a new DELETE over a data file that already carries a position-delete
   file should rewrite the superseded delete file (Iceberg's `write.delete.granularity=file`)
   instead of adding a second one. One recorded cell, `prior_deletes_then_rest_v2_mor`, is
   strict-xfailed on it. v3 is unaffected.
2. The translation deliberately declines every negation (`NOT`, `<>`, `NOT IN`, `NOT LIKE`).
   For `NOT IN` that is measured (Spark's merge-on-read cells take the row-level route). For
   `<>` and `NOT LIKE` NO cell exists, and declining preserves `main`'s behaviour; if Spark
   answers those from metadata, the cells to record are `DELETE … WHERE id <> 7` and
   `DELETE … WHERE v NOT LIKE 'x%'` on the same 18-shape bed.
3. Likewise unmeasured and therefore declined: `BETWEEN`, `WHERE false` (Java's
   `SparkTable.deleteWhere` returns without committing any snapshot on `alwaysFalse`, which
   would be a THIRD no-op shape distinct from the empty `delete` snapshot), and a `DATE` /
   `TIMESTAMP` / `DECIMAL` literal (the shared `literal_datum` carries no datum for them).

```
COVERAGE_ATTESTATION:
  pr_unit: ice-meta-delete-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the 72 recorded cells and the brief's steps 1-7; the mode-independence reading (copy-on-write summaries cannot distinguish the two routes) and the NOT IN reading were derived from the cells, not assumed, and both are stated in the ledger head rather than left implicit.
      artifacts: [task/ledgers/staging/ice-meta-delete-1-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: 72 recorded cells on the Spark door (rows, every snapshot operation, fourteen summary counters, the live data files), 8 Rust pins on the shared decision seat, 6 native-door integration pins, and the 27 facade files that issue a DELETE re-run whole (841 passed).
      artifacts: [python/repark/tests/test_ice_meta_delete_1.py, crates/repark-iceberg/src/write/meta_delete/tests.rs, crates/repark-sql/tests/ansi_meta_delete.rs]
    - id: AT-3
      status: ATTACKED
      evidence: No unwrap, expect or panic in the product code; the module returns Option for every undecidable shape and DataFusionError for a fork error. clippy -D warnings clean on repark-iceberg --all-targets. The commit is the fork's action, so no manifest or snapshot assembly is written here.
      artifacts: [crates/repark-iceberg/src/write/meta_delete.rs]
    - id: AT-4
      status: N/A
      justification: No shared mutable state, no new task or lock; the decision is a read of table metadata followed by one transaction commit through the existing catalog handle.
    - id: AT-5
      status: N/A
      justification: Local catalogs and temp warehouses only; no credential, network or AWS surface is touched.
    - id: AT-6
      status: ATTACKED
      evidence: Red-first — the 72-cell pin was committed before the implementation and failed on 29 cells for the reason the unit names. The C-008 mutation (plan_metadata_delete returns None) was run on a rebuilt release native and reds 28 cells, 5 Rust pins and 5 native-door pins; it was then reverted, rebuilt, and all three are green.
      artifacts: [task/ledgers/staging/ice-meta-delete-1-ledger.md, python/repark/tests/test_ice_meta_delete_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: cargo test -p repark-iceberg, -p repark-spark and -p repark-sql all green; the facade DELETE sweep green; cargo fmt, clippy -D warnings and the comment ban clean (see Gates).
      artifacts: [task/ledgers/staging/ice-meta-delete-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: The only oracle is the recorded truth; every expectation in the Python pin is read from the fixture, never hand-written, and the recorder's --check mode re-derives all 72 cells on the live tier. The Rust pins assert the same counters the fixture carries for those shapes.
      artifacts: [python/repark/tests/ice_meta_delete_1_spark_oracle.json, python/repark/tests/_record_ice_meta_delete_1.py]
    - id: AT-9
      status: ATTACKED
      evidence: Two registry rows filed — ICE-META-DELETE-1 (FIXED) and ICE-META-DELETE-1-D1 (DECLARED) — and every touched directory's map.md carries its row; the two retired premises are named in this ledger and in the maps of the files that carried them.
      artifacts: [docs/spark-sql-iceberg-parity.md, crates/repark-iceberg/src/write/map.md, crates/repark-spark/src/map.md, crates/repark-sql/src/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: The one cell that does not match is a DECLARED registry row with a STRICT xfail that reds the day it converges, not a loosened assertion; every other cell is compared on rows, all snapshot operations, all recorded counters and the live files at once. The translation declines rather than guessing wherever no cell measures the shape, and the open questions name the cells that would settle each one.
      artifacts: [python/repark/tests/test_ice_meta_delete_1.py, docs/spark-sql-iceberg-parity.md]
  complete: true
```
