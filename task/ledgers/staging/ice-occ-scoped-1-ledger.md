# Unit ledger — ICE-OCC-SCOPED-1 · a DML's own target predicate scopes its conflict validation

**Date:** 2026-09-17 · **Branch:** `fix/ice-occ-scoped-1` · **Base:** `71482620` (`origin/main`)
**Model:** claude-opus-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** HIGH. **risk_tier: high** — the change decides which concurrent commits a MERGE /
UPDATE / DELETE may commit past; a filter narrower than the rows the statement reads would let
a serializable write commit over a conflicting one (lost update). Over-fixing is also a
divergence: Spark refuses some shapes, and those must still refuse.
**Fork:** `TRO-Wolf/iceberg-rust` #291 (F-OCC-SCOPED-1), merged. RePark main was at RP-23
`4151b488`; `origin/main` carried no RP-24 bump, so this unit's first commit pins `8fb44a39`
(the range `4151b488..8fb44a39` is exactly #291).

**Why now.** Rating row V2-20a / residue DML-5: a serializable Iceberg MERGE in RePark aborts
on ANY concurrent commit, because all three commit sites in
`crates/repark-iceberg/src/write/merge/snapshot_commit.rs` hard-coded `Predicate::AlwaysTrue`.

**Not in this unit:** `STATUS.md` (the orchestrator owns the deletion),
`crates/repark-spark/src/insert_overwrite.rs` (run 21a unit 2), every maintenance procedure
(including `repark-spark/src/call/rewrite_where.rs`).

## The measured oracle

Spark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0`, recorded 2026-09-17, checked in as
`python/repark-parity/fixtures/torture/data/ice_occ_scoped_1/` (see its `map.md`). The rating's
premise that Spark commits the 8 disjoint-key MERGEs is wrong: Spark commits 1 of 8. The real
gap is the partition- and range-scoped shapes.

## Fix

- `crates/repark-iceberg/src/write/conflict_filter.rs` (new) — derives the filter. MERGE: the
  `ON` conjuncts whose every column is qualified by the target alias; UPDATE / DELETE: the
  identity DML's `WHERE` (bare or target-qualified columns). Sound by widening: under `AND` an
  unconvertible side is dropped (the filter only grows); `OR` / `NOT` / `IN` / `BETWEEN` convert
  whole or not at all; parse failure, unknown or nested column, a literal that does not fit the
  column type, a subquery, a shadowed alias → `AlwaysTrue`.
- `merge/mod.rs` — `MergeTarget.conflict_filter`, computed once by `merge_conflict_filter`;
  `AlwaysTrue` whenever `WHEN NOT MATCHED BY SOURCE` is present.
- `merge/snapshot_commit.rs` — `CommitScope { isolation, conflict_filter }` threaded as a
  parameter to `commit_overwrite_on_ref` (both arms) and `commit_row_delta_kind_on_ref`
  (`RowDeltaPolicy { kind, scope }`); `case_sensitive(true)` unchanged.
- `predicate_dml.rs` — builds the scope from its isolation property and its own `WHERE`.
- `merge/target_scan.rs` — receives `residual_join_key_filter` verbatim (line budget for
  `merge/mod.rs`, baseline 1792 → 1773; `predicate_dml.rs` held 1142 exactly).

## Rulings

- **Q-21a-1 — take the RP-24 bump in this unit.** The brief expected `origin/main` at RP-24; it
  was at RP-23 and fetching changed nothing. Without the bump the RePark half cannot reach #291's
  partition projection, so the unit pins `8fb44a39` in its own first commit (docs/fork-sync.md
  asks for the bump in its own PR; the orchestrator may split commit 1 out). The range is the
  single fork commit #291.
- **Q-21a-2 — the conflict filter is never the join-key residual.** `residual_join_key_filter`
  derives min/max bounds from the SOURCE's keys; a filter from it would let the 8 disjoint-key
  MERGEs commit where Spark refuses. Only conjuncts of the statement's own predicate enter the
  filter (mutation M7 shows the refusal pin guarding the over-fix).
- **Q-21a-3 — snapshot isolation gets the same filter.** Java sets `conflictDetectionFilter`
  regardless of isolation; isolation only decides whether `validateNoConflictingDataFiles` is
  armed. Keeping `AlwaysTrue` under snapshot would refuse a partition-scoped MERGE Spark commits.
  "Snapshot stays exactly as it is" is read as: the armed walks and the property resolution are
  unchanged — they are, and the unscoped snapshot storm still answers Spark's message.
- **Q-21a-4 — the range table is seeded as two files.** Spark's `range(100)` under `local[8]`
  writes eight range-contiguous files, so its two range MERGEs rewrite different files; one
  RePark INSERT writes one file, which would make both engines conflict on the removed file, not
  on the filter. The pins seed `id < 50` and `id >= 50` with two INSERT statements.
- **Q-21a-5 — the 16-INSERT storm is a measured gap outside this unit, rowed BACKLOG.** On a
  release native RePark commits 5 of 16 (v2 and v3, before and after this unit — appends validate
  nothing, so the conflict filter never runs); every loser is `CatalogCommitConflicts`, the fork
  commit loop's retry budget (4 retries, exponential backoff) running out as sixteen writers
  rebase in lockstep. Spark commits 16 (v2) and 14 (v3, its own Hadoop retry exhaustion). The fix
  is retry jitter / budget parity in the fork's `Transaction::commit` — a fork unit. The facade
  pin asserts the deterministic part (every commit durable, every loser a commit conflict), not
  the scheduler-dependent count; registry row ICE-OCC-SCOPED-1-INSERT-STORM.
- **Q-21a-6 — UPDATE and DELETE are SQL-door only.** PySpark 4.1 has no DataFrame UPDATE /
  DELETE; the facade pins run them on the SQL door inside both door cells, and MERGE / INSERT run
  their DataFrame spellings (`mergeInto`, `writeTo().append()`).
- **Q-21a-7 — loser class.** Spark's loser is the JVM `ValidationException` (PySpark:
  `Py4JJavaError`); RePark's is `PySparkException` (DataInvalid → `Error::Iceberg`). The pins
  assert the class and Spark's message head; filter rendering differs (`true` vs `TRUE`,
  `(not_null(ref(name="id")) and ref(name="id") < 50)` vs `id < 50`).
- **Q-21a-8 — INSERT OVERWRITE … PARTITION needs no RePark change.** Its commit
  (`partition_overwrite.rs` `commit_overwrite_by_row_filter_to`) sets no explicit filter, and the
  fork's `data_conflict_detection_filter` falls back to the row filter exactly as Java's
  `BaseOverwriteFiles` does, now partition-projected by #291. Not an oracle row; no pin added;
  the INSERT OVERWRITE lowering is unit 2's.
- **Q-21a-9 — the two SQL-literal converters stay separate this round.** `conflict_filter.rs`
  repeats `rewrite_where.rs`'s literal typing; that file is a maintenance procedure (out of
  bounds) with a strict all-or-nothing contract. Unifying is left to a next round.

- **Q-21a-10 — the oracle's plain-`WHERE` UPDATE storm stays OPEN in this round.** The brief
  placed UPDATE on `predicate_dml*`; measured, only a subquery UPDATE goes there. A plain-`WHERE`
  UPDATE runs the fork's `iceberg-datafusion` exec (`physical_plan/delete.rs`, four
  `conflict_detection_filter(Predicate::AlwaysTrue)` sites) and commits 1 of 4 after the fix.
  Extending `predicate_dml/plain.rs` to UPDATE would move every non-subquery UPDATE on both doors
  off the fork exec (`scalar_set_assignments` accepts any non-subquery SET), reversing RP-9 r2's
  scoped decision with a blast radius this unit cannot audit; the fork-side fix is narrower.
  Registry row ICE-OCC-SCOPED-1-PLAIN-UPDATE (OPEN) carries a pin that reds when it closes.

### Round 2 (review L-01…L-05, R-01…R-03)

- **Q-21a-OCC-1 — a float literal scopes only when exact, and a float column only under
  equality.** L-01: `parse::<f32>("1e-50")` is `0.0`, so `f < 1e-50` became `f < 0.0` and excluded
  the zeros the DELETE removed. A FLOAT / DOUBLE literal now converts only when its decimal text
  and the parsed value's exact `{:.800e}` expansion normalize to the same digits and exponent
  (underflow, overflow, rounding, ±Inf refuse; NaN is no literal). Zero refuses too: the fork orders
  floats by `total_cmp` (`-0.0 < 0.0`), SQL equates them, so `f = 0.0` would skip a file of
  `-0.0`s. Reading the fork's `InclusiveMetricsEvaluator` for the same review found a second
  narrowing the reviewer did not name: a nans-only file answers `ROWS_CANNOT_MATCH` to `<` / `<=` /
  `>` / `>=` (and Iceberg bounds exclude NaN), while SQL orders NaN as a value (`NaN > 1.0` is
  true in Spark; Arrow's `total_cmp` also puts `-NaN` below every value), so `DELETE … WHERE
  f > 1.0` could miss a concurrent NaN insert. A float column therefore converts only under `=`,
  `<>`, `IN`, `NOT IN` (`not_eq` / `not_in` never exclude a file). Float ranges and `BETWEEN`
  stay `AlwaysTrue` — wider than Spark, which scopes them and carries the same NaN gap; no oracle
  cell uses a float column. Decimal, date, timestamp and every other type were already refused
  (`_ => None`) and stay so.

- **Q-21a-OCC-2 — L-02 is measured, deterministic, and matches Spark; the RePark pin is the
  Rust race, not a facade race.** Spark's interleaving is made deterministic with a Python UDF
  gate in the MERGE's source (the target snapshot is pinned at analysis, so the gate holds the
  MERGE between its scan and its commit while the append commits); all 16 cells (v2/v3 × MoR/COW
  × 4) gave one answer each, with the snapshot log as the ordering witness. An append inside the
  `ON` partition aborts the MERGE whatever its key; an append to another partition commits —
  including the review's duplicate `(1000, 'b')`, which is not an isolation anomaly: `t.k = 'a'`
  cannot match a `k = 'b'` row, so every serial order inserts it twice too. RePark answers every
  cell the same through `occ_scoped.rs`'s `RaceCatalog` (`occ_scoped_insert.rs`, reading the
  recording). No facade pin: the Python-UDF bridge materialises a UDF source in Python BEFORE
  the native MERGE loads its target (measured: the gate fired on `createOrReplaceTempView` in the
  main thread), and a registered UDF inside a SQL subquery is refused
  (`UnsupportedOperationException … nested subquery`), so a gated facade race would race
  nothing, and a barrier storm answers differently depending on which statement the scheduler
  runs first. Registry row ICE-OCC-SCOPED-1-NOT-MATCHED-INSERT (FIXED, matches Spark).
  `serde_json` joins `repark-iceberg`'s dev-dependencies (already a workspace dependency).

- **Q-21a-OCC-3 — dispositions of the P3s.** L-04 (a quoted MERGE alias `AS "Tgt"` never
  qualifies, so the filter is `AlwaysTrue`): accepted as is — it over-refuses, exactly the
  pre-unit behaviour, and matching quote-aware identifiers belongs with the Q-21a-9 converter
  unification. L-05 (the UPDATE race pin runs a plain `WHERE` the doors never route there):
  confirmed and corrected, not coded — `try_allowed_update_in` admits only a bare positive
  `col IN (SELECT …)`, whose filter is `AlwaysTrue`, so no door UPDATE is scoped today; C-007 now
  says the pin proves the executor seam, and the registry row is reworded. R-01 (a second
  sqlparser pass per statement): accepted — microseconds per statement, once, never per retry;
  sharing the parsed `Expr` rides the same unification. R-02: fixed — the exact
  spelling is one hash lookup in the TOP-LEVEL index `schema.as_struct().field_by_name` (not
  `Schema::field_by_name`, which also answers a dotted nested name such as `"outer.leaf"`); the
  case-insensitive fallback keeps its scan (the fork's builder refuses case-folded twins, so it
  finds at most one). New unit test `a_nested_leaf_never_resolves_as_a_top_level_column` pins the
  refusal; it holds under either lookup (the type lookup re-resolves the leaf's bare name and
  fails), so the top-level guarantee rests on the struct index itself. R-03 (`Predicate`
  cloned 2-3 times into the action): accepted — once per statement, the fork's
  `conflict_detection_filter` takes it by value, and the clones are of a tree the size of the
  statement's own predicate.

## PROPOSITION LEDGER — ICE-OCC-SCOPED-1 — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A MERGE's filter is the conjunction of the `ON` conjuncts that reference only the target alias; join equalities, source-only conjuncts, bare identifiers and a shadowed alias never enter it. | `conflict_filter.rs` unit tests `a_target_only_partition_conjunct_scopes_a_merge_filter`, `a_target_only_range_conjunct_scopes_a_merge_filter`, `a_join_equality_alone_leaves_the_filter_unscoped`, `a_source_only_conjunct_never_reaches_the_filter`, `a_bare_identifier_in_an_on_condition_is_not_target_qualified`, `an_alias_the_source_shadows_refuses_to_scope`. | PROVEN | 19/19 unit tests green; M7 reds 8 of them. |
| C-002 | Conversion only widens: an unconvertible `AND` side is dropped; `OR` / `NOT` / `IN` / `BETWEEN` convert whole or not at all; parse failure, unknown column, type-mismatched literal, subquery → `AlwaysTrue`. | Unit tests `an_unconvertible_conjunct_widens_instead_of_narrowing`, `an_unconvertible_disjunct_drops_the_whole_disjunction`, `a_literal_that_does_not_fit_the_column_type_leaves_the_filter_unscoped`, `a_subquery_selection_leaves_the_filter_unscoped`, `a_predicate_that_will_not_parse_leaves_the_filter_unscoped`. | PROVEN | M8 (drop an unconvertible disjunct) reds `an_unconvertible_disjunct_drops_the_whole_disjunction`. |
| C-003 | An UPDATE / DELETE's filter is its `WHERE`, over bare or target-qualified columns resolved to the schema's top-level spelling; another relation's qualifier refuses. | Unit tests `a_selection_takes_bare_and_target_qualified_columns`, `a_selection_refuses_another_relations_qualifier`, `a_column_spelled_in_another_case_resolves_to_the_schema_spelling`, `null_tests_and_lists_and_ranges_convert`, `a_reversed_comparison_keeps_its_direction`, `negative_literals_convert`, `a_nested_leaf_never_resolves_as_a_top_level_column` (round 2). | PROVEN | Green; M5 (DML derivation off) reds the UPDATE and DELETE race pins. |
| C-004 | A MERGE with `WHEN NOT MATCHED BY SOURCE` keeps `AlwaysTrue` and still loses to a concurrent write in another partition. | `occ_scoped.rs` `a_merge_with_not_matched_by_source_stays_unscoped` (v2/v3 × MoR/COW). | PROVEN | Green; M6 (guard removed) reds exactly this pin. |
| C-005 | All three commit sites take the filter as a parameter (`CommitScope`); isolation still decides only which walks are armed, so `snapshot` arms what it armed before. | `occ_scoped.rs` battery; `disjoint_key_merges_on_an_unpartitioned_table_still_conflict_on_true` (snapshot cell); M1 / M2 / M3. | PROVEN | M1 (row-delta site reverted) reds the 4 MoR-dependent pins; M2 (overwrite sites) reds the 5 COW-dependent pins; M3 (all three) reds 6, as the pre-fix head. Existing OCC batteries unchanged: `cargo test -p repark-iceberg --lib` 475 passed. |
| C-006 | Four partition-scoped MERGEs (`ON t.k = '<key>' AND t.k = s.k AND t.id = s.id`) all commit, v2/v3 × MoR/COW, both doors, rows as Spark. | `occ_scoped.rs` `partition_scoped_merges_commit_through_a_concurrent_write_to_another_partition`; `test_ice_occ_scoped_1.py::test_partition_scoped_storms_match_spark`. | PROVEN | See §Red evidence, §Facade evidence. |
| C-007 | Through RePark's identity UPDATE executor (`execute_predicate_dml` with SET assignments, driven with a convertible plain `WHERE` — round-2 correction, review L-05: the doors route only a bare positive `UPDATE … WHERE col IN (SELECT …)` there, whose filter is `AlwaysTrue`, so no door UPDATE is scoped today) four partition-scoped UPDATE commits all land, v2/v3 × MoR/COW; the plain-`WHERE` UPDATE storm of the oracle, which runs the fork's DataFusion exec, is pinned OPEN (commits < 4, losers `matching TRUE`) under registry row ICE-OCC-SCOPED-1-PLAIN-UPDATE. | `partition_scoped_updates_commit_through_a_concurrent_update_of_another_partition`; facade `_assert_plain_update_storm_is_open` inside `test_partition_scoped_storms_match_spark`. | PROVEN | The Rust pin is red-first on the pre-fix head; the facade storm measured 1 of 4 after the fix on every cell (Q-21a-10). |
| C-008 | Four partition-scoped DELETE statements commit, v2/v3 × MoR/COW. | `partition_scoped_deletes_commit_through_a_concurrent_delete_in_another_partition`; facade `test_partition_scoped_storms_match_spark`. | PROVEN | Red-first on COW only: a MoR DELETE's row delta arms no delete-file walk and a concurrent DELETE adds no data file, so MoR already committed pre-fix (M1 leaves this pin green for the same reason). |
| C-009 | A partition-scoped MERGE commits past a concurrent INSERT into another partition, v2/v3 × MoR/COW, both doors. | `a_partition_scoped_merge_commits_through_a_concurrent_insert_into_another_partition`; facade `test_partition_scoped_storms_match_spark`. | PROVEN | See §Red evidence. |
| C-010 | Two disjoint-range MERGEs on an unpartitioned copy-on-write table both commit. | `a_copy_on_write_range_scoped_merge_commits_through_a_disjoint_range_rewrite`; facade `test_range_scoped_merges_match_spark` (COW cells). | PROVEN | See §Red evidence. |
| C-011 | The same on merge-on-read commits 1 of 2 with Spark's reason: `Found new conflicting delete files that can apply to records matching id < 50`. | `a_merge_on_read_range_scoped_merge_still_loses_to_the_concurrent_delete_file`; facade `test_range_scoped_merges_match_spark` (MoR cells). | PROVEN | Pre-fix it lost for the wrong reason (`Found conflicting files … matching TRUE`), so the pin was red-first too. |
| C-012 | Eight MERGEs on disjoint keys with no target-only conjunct commit 1 of 8, losers raising Spark's serializable / snapshot message with filter `TRUE`, both isolation levels, both doors. | `disjoint_key_merges_on_an_unpartitioned_table_still_conflict_on_true`; facade `test_disjoint_key_merges_refuse_like_spark`; rating probe `p_retry.py` before/after. | PROVEN | Over-fix guard: green on both heads; M7 reds it. Probe numbers in §Before and after. |
| C-013 | Sixteen concurrent INSERT statements: every commit is durable (rows = snapshots = commits) and every loser is a `CatalogCommitConflicts` `PySparkException`, v2 and v3, both doors; the 5-of-16 vs Spark 16-of-16 gap is rowed BACKLOG (Q-21a-5), unchanged by this unit. | facade `test_insert_storm_loses_only_to_the_retry_budget`; `p_retry.py` before/after; registry row ICE-OCC-SCOPED-1-INSERT-STORM. | PROVEN | Probe numbers in §Before and after. |
| C-014 | RePark is pinned at fork RP-24 `8fb44a39` (#291) in all five `[patch]` lines and `Cargo.lock`. | `Cargo.toml` diff; root `map.md` RP-24 line. | PROVEN | Commit `26622dd9`; `cargo check --locked --workspace` rc=0. |
| C-015 | The Spark recordings are in the repository and every facade expectation is read from them. | `ice_occ_scoped_1/` fixture + `map.md`; the facade module loads both files. | PROVEN | Commit `7c361811`; only the warehouse prefix normalized (SHA-256s in the fixture map). |
| C-016 | Registry rows: DML-5 reversed by dated decision to FIXED; ICE-OCC-SCOPED-1 FIXED with the oracle table; ICE-OCC-SCOPED-1-PLAIN-UPDATE OPEN; ICE-OCC-SCOPED-1-REFUSED DECLARED; ICE-OCC-SCOPED-1-INSERT-STORM BACKLOG with pins. | `docs/spark-sql-iceberg-parity.md` §2 DML-5, §7 three rows; `make check-docs-links`. | PROVEN | See §Gates. |
| C-017 | Two concurrent whole-partition DELETE statements (`k = 'a'` / `k = 'b'`) both commit, v2 and v3, table empty after. | facade `test_whole_partition_deletes_both_commit`. | PROVEN | See §Facade evidence. |
| C-018 | A FLOAT / DOUBLE literal enters the filter only when its conversion is exact and non-zero, and a float column only under `=` / `<>` / `IN` / `NOT IN`; `f < 1e-50`, `d < 1e-400`, `f = 1e-50`, `f = 0.1`, `f = 1e39`, `f = 0.0`, `f < 0.5`, `f BETWEEN …` leave the node unconverted, while `f = 0.5`, `d = -1.25e2`, `f = 16777216`, `f IN (0.5, 2)` still scope. | `conflict_filter.rs` unit tests `an_underflowing_float_literal_leaves_the_filter_unscoped`, `an_inexact_float_literal_leaves_the_filter_unscoped`, `a_zero_float_literal_leaves_the_filter_unscoped`, `an_exact_float_literal_still_scopes_an_equality`, `a_float_range_leaves_the_filter_unscoped`. | PROVEN | M9 (exactness check removed) reds the underflow and inexact pins (`f = 0`, `f = 0.1`); M10 (float-range guard removed) reds `a_float_range_…` (`f < 0.5`). The round-1 converter is M9 + M10. Q-21a-OCC-1. |
| C-019 | Under serializable isolation a scoped DML still ABORTS on a concurrent INSERT into its own partition of a row its predicate matches — MERGE (merge-on-read and copy-on-write) `ON t.k = 'a' AND t.k = s.k AND t.id = s.id` vs `(1004, 'a')`, identity DELETE `k = 'a' AND id > 90` vs `(1004, 'a')`, identity UPDATE `k = 'a' AND id < 8` vs `(-4, 'a')`, v2/v3 × MoR/COW — with `Found conflicting files that can contain records matching <the scoped filter>`, and the table holds exactly the seed plus the concurrent row. | `occ_scoped.rs` `a_partition_scoped_merge_loses_to_a_concurrent_insert_into_its_own_partition`, `a_partition_scoped_delete_loses_to_a_concurrent_insert_its_predicate_matches`, `a_partition_scoped_update_loses_to_a_concurrent_insert_its_predicate_matches`. | PROVEN | Green on the fixed head (11 of 11 `occ_scoped`). M11 (an extra `id = 0` conjunct on every derived filter) reds all three: the victim COMMITS (`expect_err` on `Ok(())`, first cell V2/merge-on-read). Review L-03. |
| C-020 | A MERGE scoped by `ON t.k = 'a' AND t.k = s.k AND t.id = s.id` with `WHEN NOT MATCHED THEN INSERT *`, racing one append committed between its scan and its commit, answers as Spark 4.1.2 in every cell, v2/v3 × MoR/COW: an append inside partition `a` (the MERGE's key or another) aborts it with the scoped filter; an append to partition `b` commits beside it, including the same `(1000, 'b')` the MERGE inserts. Rows at or above id 1000 and the row count equal Spark's. | `occ_scoped_insert.rs` `a_merge_that_inserts_answers_a_concurrent_append_as_spark_does`, reading `spark_occ_oracle3.json`. | PROVEN | Green, 16 cells. M4 (MERGE filter → `AlwaysTrue`) reds it (`matching TRUE` where the scoped filter is expected); M11 reds it (cell (a) commits where Spark aborts). Q-21a-OCC-2; review L-02. |

## Red evidence

### Rust race pins on the pre-fix head (`26622dd9` sources + only `occ_scoped.rs`), 2026-09-17

`cargo test -p repark-iceberg --lib occ_scoped` (the NMBS pin was added after this run):

```
partition_scoped_updates_commit_… FAILED   V2/merge-on-read: … must commit: DataInvalid => Found conflicting files that can contain records matching TRUE: …/data/k=b/…parquet
a_copy_on_write_range_scoped_merge_… FAILED V2/copy-on-write: … must commit: DataInvalid => Found conflicting files that can contain records matching TRUE: …/data/…parquet
partition_scoped_deletes_commit_… FAILED   V2/copy-on-write: … must commit: DataInvalid => Found conflicting files that can contain records matching TRUE: …/data/k=b/…parquet
a_merge_on_read_range_scoped_merge_still_loses_… FAILED the loser must name `Found new conflicting delete files that can apply to records matching id < 50`, got: Found conflicting files that can contain records matching TRUE: …
a_partition_scoped_merge_commits_through_a_concurrent_insert_… FAILED V2/merge-on-read: … Found conflicting files that can contain records matching TRUE: …/data/k=d/…parquet
partition_scoped_merges_commit_… FAILED    V2/merge-on-read: … Found conflicting files that can contain records matching TRUE: …/data/k=b/…parquet
disjoint_key_merges_on_an_unpartitioned_table_still_conflict_on_true ... ok
test result: FAILED. 1 passed; 6 failed
```

Each loser names a file in the OTHER partition (or the other range) — the shape's reason.

### Mutation table (fixed head, `cargo test -p repark-iceberg --lib occ_scoped` / `conflict_filter`)

| Mutation | Race pins red | Unit tests red |
|---|---|---|
| M1 row-delta site back to `AlwaysTrue` | 4 of 8: partition MERGE, UPDATE, MERGE-vs-INSERT, MoR range refusal | 0 |
| M2 both overwrite sites back to `AlwaysTrue` | 5 of 8: partition MERGE, UPDATE, DELETE, MERGE-vs-INSERT, COW range | 0 |
| M3 all three sites (the fix reverted) | 6 of 8 (all but the two over-fix guards) | 0 |
| M4 `merge_conflict_filter` → `AlwaysTrue` | 4 of 8: every MERGE pin that commits + MoR range refusal | 0 |
| M5 `for_identity_dml` → `AlwaysTrue` | 2 of 8: UPDATE, DELETE | 0 |
| M6 NMBS guard removed | 1 of 8: `a_merge_with_not_matched_by_source_stays_unscoped` | 0 |
| M7 over-fix: nothing converts → `AlwaysFalse` | 1 of 8: `disjoint_key_merges_…_still_conflict_on_true` | 8 of 19 |
| M8 over-fix: `OR` drops an unconvertible disjunct | 0 of 8 | 1 of 19: `an_unconvertible_disjunct_drops_the_whole_disjunction` |
| M9 (round 2) float exactness check removed (`is_exactly` → `true`) | — | 2 of 24: `an_underflowing_float_literal_…` (`f = 0`), `an_inexact_float_literal_…` (`f = 0.1`) |
| M10 (round 2) float-range guard removed | — | 1 of 24: `a_float_range_leaves_the_filter_unscoped` (`f < 0.5`) |
| M11 (round 2) over-narrow: every derived filter gets an extra `AND id = 0` conjunct | 4 of 11: the three own-partition pins of C-019 (victim commits) + the MoR range refusal (its needle quotes `matching id < 50`, now `(id < 50) AND (id = 0)`); the round-1 pins stay green, which is the L-03 gap | 0 |

Green after (fixed head): `occ_scoped` 8 passed; `conflict_filter` 19 passed.

## Facade evidence (release native, `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 maturin develop --release`)

BEFORE — native built from the pre-fix sources (`26622dd9`: RP-24 pin, RePark half absent),
`pytest python/repark/tests/test_ice_occ_scoped_1.py -n 8` (the module as first written):
`16 failed, 14 passed`. Every red is the named reason — `v*_4_partition_scoped_merges/{sql,dataframe}`
`RePark committed 1 of 4, Spark 4; losers: DataInvalid => Found conflicting files that can contain
records matching TRUE`; COW range `1 of 2, Spark 2`; MoR range loser `… matching TRUE` where
Spark's is `Found new conflicting delete files … matching (not_null(…) and … < 50)`. Green on the
pre-fix head: the disjoint-key refusals, the whole-partition DELETE statements (2 of 2), and the
INSERT storm.

AFTER — native built from `c87c2572`: `30 passed in 46.01s`, then `30 passed in 48.70s` and
`30 passed in 52.93s` (three runs, no flake). The plain-`WHERE` UPDATE storm commits 1 of 4 on
every cell (pinned OPEN, Q-21a-10).

## Before and after — the rating's own probe `p_retry.py` (release native, codegen-units=16)

The probe copied verbatim with only its scratch prefix rewritten.

| Cell | Spark 4.1.2 | RePark before (`26622dd9` sources) | RePark after (`c87c2572`) |
|---|---|---|---|
| v2 16 concurrent INSERT statements | 16/16 | 5/16 (`CatalogCommitConflicts`), rows 5 | 5/16, rows 5 |
| v3 16 concurrent INSERT statements | 14/16 | 5/16, rows 5 | 5/16, rows 5 |
| v2 8 disjoint-key MERGEs, serializable | 1/8 `… matching true` | 1/8 `… matching TRUE`, rows (100, 1, 100) | 1/8, same |
| v2 8 disjoint-key MERGEs, snapshot | 1/8 `Found new conflicting delete files … true` | 1/8 same message, rows (100, 1, 100) | 1/8, same |
| v3 8 disjoint-key MERGEs, serializable | 1/8 | 1/8, rows (100, 1, 100) | 1/8, same |
| v3 8 disjoint-key MERGEs, snapshot | 1/8 | 1/8, rows (100, 1, 100) | 1/8, same |
| v2 / v3 2 concurrent INSERT statements, 2 DataFrame appends | — | 2/2, 2/2 | 2/2, 2/2 |

The disjoint-key cells match Spark before and after (the rating's premise was wrong); the INSERT
storm is the BACKLOG row of Q-21a-5.

## Gates

Final run, 2026-09-17 (HEAD `ccbecdd6`, crates identical to `c87c2572`):

| Command | Result |
|---|---|
| `comment_ban.py <repo> origin/main HEAD` (orchestrator tool) | `comment-ban hits=0` |
| `cargo clippy --all-targets --all-features -- -D warnings` (the brief's literal line) | rc=101: 36 pre-existing `disallowed_methods` errors (`unwrap`/`expect` in test code) in `crates/repark-ml/src/{cholesky,kmeans,linear_regression,logistic_regression}.rs`. No flagged file is in this branch's diff. The repo's gate passes `-A clippy::disallowed_methods` on purpose (Makefile `rust-clippy`) |
| `make rust-clippy` (`cargo clippy --locked --workspace --all-targets -- -D warnings -A clippy::disallowed_methods`) | clean (also inside `make verify`) |
| `make rust-panic-ban` (production `--lib --bins`, disallowed-methods live) | clean |
| `cargo test -p repark-iceberg --lib` | `475 passed; 0 failed` |
| release native `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 … maturin develop --release` | built from `c87c2572` crates (unchanged since), 7m32s |
| `.venv/bin/python -m pytest python/repark/tests -q -p no:cacheprovider -n 8` | `3 failed, 9714 passed, 398 skipped, 47 xfailed` in 21m29s. The 3 reds are unrelated and reproduce in isolation. `test_spark_sql_grammar_1.py::test_q14_current_date_bare_and_paren[ansi-off/on]` compares the engine's UTC `current_date` (2026-09-18) with the local date (2026-09-17) after 00:00 UTC, and passes under `TZ=UTC` (2 passed). `test_csv_infer_perf_1.py::test_infer_schema_true_stays_under_half_second` is a wall-clock bound (0.908 s under `-n 8`, 0.512 s alone, bound 0.5 s). The branch touches no CSV, date or facade source |
| `test_ice_occ_scoped_1.py` alone | `30 passed` × 3 runs |
| `make verify` | rc=0 in 47m44s: every static gate clean (rust-file-size 624 files, ledger-grammar 186 ledgers / 1505 clauses, docs-links 952 files / 5837 links, map-sync, crate-dag, lib-rs, lib-py, docstring-presence, manifest, owner-ruling, docs-compaction), `cargo test --locked --workspace` 57 binaries, 3835 passed, 0 failed |


## Gates — round 2

Run 2026-09-18 on HEAD `2baafc64` (every round-2 source commit; this table's commit adds only
this section and the attestation lines):

| Command | Result |
|---|---|
| `comment_ban.py <repo> origin/main HEAD` (orchestrator tool) | `comment-ban hits=0` |
| `make rust-clippy` | rc=0 (278 s) |
| `cargo test -p repark-iceberg --lib` | `485 passed; 0 failed` (475 + 10 new: five float, one nested-leaf, three own-partition, one NOT MATCHED INSERT) |
| release native `cd python/repark && CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 VIRTUAL_ENV=<repo>/.venv <repo>/.venv/bin/maturin develop --release` | rc=0, 500 s, from `2baafc64` |
| `.venv/bin/python -m pytest python/repark/tests/test_ice_occ_scoped_1.py -q -p no:cacheprovider` × 3 | `30 passed` in 224.73 s, 85.61 s, 33.42 s |
| `.venv/bin/python -m pytest python/repark/tests -q -p no:cacheprovider -n 8` | `9717 passed, 398 skipped, 47 xfailed` in 36m09s, 0 failed (round 1's three environment reds did not recur) |
| `make verify` | rc=0 in 51m10s: rust-file-size 625 files, ledger-grammar 187 ledgers / 1515 clauses, docs-links 953 files / 5841 links, map-sync, crate-dag, lib-rs, lib-py, docstring-presence, manifest, owner-ruling, docs-compaction clean; `cargo test --locked --workspace` 57 binaries, 3845 passed, 0 failed |
| Spark 4.1.2 recording `record_spark_occ3.py` (one `local[8]` JVM, driver 2g, UI off) | rc=0; 16 cells, one deterministic answer each (`spark_occ_oracle3.json`) |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-occ-scoped-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every oracle row maps to a clause and a pin that reads Spark's recording; the scoped shapes were red on the pre-fix head (Rust 6 of 7, facade 16 of 30) and green after; the refusal shapes are pinned to Spark's own messages.
      artifacts: [crates/repark-iceberg/src/write/merge/tests/occ_scoped.rs, python/repark/tests/test_ice_occ_scoped_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Derivation boundaries are unit-pinned — bare versus qualified columns, a shadowed alias, case-folded names, type-mismatched and negative literals, IN / NOT IN / BETWEEN / IS NULL, unconvertible conjuncts and disjuncts, subqueries, unparsable text.
      artifacts: [crates/repark-iceberg/src/write/conflict_filter.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Refusals stay loud where Spark refuses — disjoint keys (both isolation levels), MoR disjoint ranges (delete file without id bounds), NOT MATCHED BY SOURCE; M6 and M7 prove the guards bite.
      artifacts: [crates/repark-iceberg/src/write/merge/tests/occ_scoped.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The race is real ordering — the concurrent commit lands inside the victim's first update_table after it planned and wrote from the parent snapshot, so the fork's refresh-and-revalidate path runs; fired() is asserted on every pin.
      artifacts: [crates/repark-iceberg/src/write/merge/tests/occ_scoped.rs]
    - id: AT-5
      status: N/A
      justification: No privileged action, secret, path handling or deserialization changes; the filter is built from SQL the statement already parsed and bound against the table's own schema.
    - id: AT-6
      status: ATTACKED
      evidence: The risk is lost updates, so soundness is by widening only — every pin asserts the exact surviving rows, and a statement with NOT MATCHED BY SOURCE or no convertible predicate keeps AlwaysTrue; snapshot isolation arms the same walks as before.
      artifacts: [crates/repark-iceberg/src/write/map.md]
    - id: AT-7
      status: ATTACKED
      evidence: The filter is derived once per statement from already-rendered SQL (one sqlparser parse); the fork's partition evaluators are cached per spec; the lib suite time is unchanged in shape (475 passed).
      artifacts: [task/ledgers/staging/ice-occ-scoped-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: Java semantics are the contract — SparkScan.filterExpression() into conflictDetectionFilter for SparkPositionDeltaWrite / SparkCopyOnWriteOperation, isolation deciding only validateNoConflictingDataFiles, BaseOverwriteFiles' row-filter fallback for INSERT OVERWRITE; the fork pin RP-24 is taken in commit 1.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-9
      status: ATTACKED
      evidence: Losers keep the fork's typed DataInvalid / CatalogCommitConflicts errors surfaced as PySparkException with Spark's message head; the facade pin names the Spark cell and the RePark loser on any mismatch.
      artifacts: [python/repark/tests/test_ice_occ_scoped_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first on the pre-fix head at both layers, then eight single-seam mutations (three commit sites, both derivations, the NMBS guard, two over-fixes), each red at least one pin; M8's only witness is the unit test, named. Round 2 adds M9/M10 (float exactness and float-range guard, unit tests) and M11 (an over-narrow extra conjunct, which reds the three own-partition race pins and the NOT MATCHED INSERT pin that no round-1 pin caught).
      artifacts: [task/ledgers/staging/ice-occ-scoped-1-ledger.md, crates/repark-iceberg/src/write/merge/tests/map.md]
  complete: true
```

## Run 21a close-out (2026-09-18)

Verification critic (Grok 4.6) on the round-2 head `e99665ba`: **PASS**, no P1 or P2. It found
L-01 (float exactness), L-02 (MERGE `NOT MATCHED` insert, measured deterministically on Spark
and matched in 16 cells) and L-03 (own-partition abort pins, mutation M11) CLOSED, with the
mutations re-run. It filed two P3s, recorded here and not fixed:

- V-01 — a success cell's snapshot log does not prove a unique interleaving on its own. The
  abort cells, which fail only in the gated order, carry the order witness.
- V-02 — the ledger's M9 line counts "2 of 24" `conflict_filter` tests, but the module now
  has 25. The mutation result (the two float-exactness pins go red) is unchanged.

Orchestrator ruling **Q-21a-5**: a FLOAT or DOUBLE range predicate stays unscoped
(`AlwaysTrue`). That is stricter than Spark 4.1.2, which scopes it and carries the NaN/±0
metrics gap. A float-range DML under contention can therefore abort where Spark commits,
but it never commits a conflicting change.

Remaining OPEN rows: ICE-OCC-SCOPED-1-PLAIN-UPDATE closes when fork #294
(F-OCC-EXEC-1: the DataFusion DELETE/UPDATE exec scopes by its own scan filter) lands in a
pin bump. ICE-OCC-SCOPED-1-INSERT-STORM is the fork commit-retry budget, BACKLOG.
