# Unit ledger — WO RP-56 · case-twin columns build like Java

**Date:** 2026-09-28 · **Branch:** `chore/rp-56-case-twin-schema` · **Base:** `b1ee89fc`
(`origin/main`) **Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Under `spark.sql.caseSensitive=true`, Spark 4.1.2 + Iceberg 1.11 accepts
`CREATE TABLE t (a INT, A INT)` and CTAS `SELECT 1 AS a, 2 AS A`, and reads both columns
back (probes `tw_true_create_read`, `tw_true_ctas_read` in
`casesens-1-probes/p5-spark.json`). The fork refused both at `Schema::build` with
`Cannot build lower case index: a and A collide`, because the lower-case index was built
eagerly. The fork fix merged as #364 (`e1d74befb0d79c3cee03a344f00403a13af8e632`): the
index is lazy, as Java's. This unit repins the fork to that rev (RP-56) and carries the
Spark-door pins: CREATE and CTAS twins under `caseSensitive=true`, plus the bare-name
refusal under `false`. RePark residue R-CS1-12.

**What the fork fix changes (fork #364, F-SCHEMA-LCI-LAZY-1).** `Schema::build` no longer
builds the lower-case index eagerly; a collided schema builds, and the collision refuses
only on a case-insensitive lookup. The retained `field_by_name_case_insensitive` returns
`None` on a collided schema; the new fallible `try_field_by_name_case_insensitive` fails
with Java's text (`Cannot build lower case index: {first} and {second} collide`). The
fork's own case-insensitive resolvers (`update_schema`, `update_partition_spec`,
expression binding, z-order validation) moved to the fallible call. RePark-side, the one
resolver that mirrored the fork's not-found text
(`repark-iceberg/src/write/partition_spec.rs` `resolve_field_by_transform`) moved with
it; the other eight call sites stay on the infallible call (per-site reasons in the
unit hand-back).

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Under caseSensitive=true a table with case-twin columns creates and reads back as Spark does. | The Spark-door pins create `(a INT, A INT)` and CTAS `SELECT 1 AS a, 2 AS "A"` under `caseSensitive=true`, expecting columns `a`/`A` with the recorded rows, and refuse a bare-name `SELECT a` under `false`. | PROVEN | `case_twin_create_reads_both_columns_under_case_sensitive_true`, `case_twin_ctas_reads_both_values_under_case_sensitive_true`, `case_twin_bare_name_select_refuses_under_case_sensitive_false`: RED at fork `6e937f49` (all three fail at CREATE on `Cannot build lower case index: a and A collide`), GREEN at fork `e1d74bef` (3 passed, 0 failed). The CTAS pin quotes `"A"` because the unquoted twin-alias spelling refuses in DataFusion's projection-uniqueness check before reaching the fork (R-2). The false-door refusal text is recorded verbatim in R-3 and asserted only as a refusal. |

## Mutation record (2026-09-28)

Each line was broken, the named tests ran, and the file was restored.

| # | Mutation | Red |
|---|---|---|
| M1 | Hold the fork pin at `6e937f49` with the new tests in place | all three pins red at CREATE on `Cannot build lower case index: a and A collide`; repinned to `e1d74bef`, green |
| M2 | Flip the CTAS pin's expected row from `[[1, 2]]` to `[[1, 3]]` | `case_twin_ctas_reads_both_values_under_case_sensitive_true` reds; restored, green |

## Repin fallout (2026-09-28)

Three pins and one registry row moved to the fork-owned shapes in this commit, RP-54 style:

- `crates/repark-spark/src/tests/create_typed_partition.rs`
  `typed_partition_columns_differing_by_case_serve_under_case_sensitive` (renamed): twin
  typed-partition columns serve under `caseSensitive=true` with their schemas and identity
  fields; the exact-duplicate `COLUMN_ALREADY_EXISTS` leg is unchanged.
- `python/repark/tests/test_ice_typed_partition_refusals.py`
  `test_typed_partition_columns_differing_by_case_serve_under_case_sensitive` (renamed):
  the facade mirror — twins land, columns plus the empty count.
- `python/repark/tests/test_ice_mixed_case_1.py`: the twin-table adoption pin splits into
  `test_measured_case_twin_table_adopts_then_refuses_references` (the three L-08 reference
  cells refuse `AMBIGUOUS_REFERENCE` / 42704 at resolution); the star half moved again in
  the fold below.
- Registry `docs/spark-sql-iceberg-parity.md` ID-1a restated to the new truth (adoption
  succeeds, FIXED references, OPEN false-CREATE).

## RP-56 fold (2026-09-28)

Review of the repin found the adopted-table star pin depended on the fixture's absent data
files, and that with real data RePark answered the twin-table star where Spark refuses
(`L08_star_twin`, 42711). The fold takes the fix, in one module: `refuse_star_twins` in
`crates/repark-core/src/column_resolution.rs` refuses a written star over a non-scratch
twin relation with Spark's recorded 42711 sentence, after the existing ambiguity audit
(so written twin references keep refusing 42704) and only on the case-insensitive path
(the `true` door answers). The fix is 50 product lines plus a one-word probe alias, over
the ~40 estimate: union-branch stars and engine-generated twin projections each needed
one precise confinement (a written-star gate and a scratch skip), and no smaller shape
holds both without reopening a silent answer. Scratch relations keep answering, so the
DataFrame `filter` / `table` lowerings stay green — the five
`test_filter_predicate_rewrite.py` cells that reverted the first attempt (§10 step 4) pass,
as does the full facade suite. The one guard covers the frame star and the table star, so
C-020 closes: the two DECLARED pins flip to the refusal
(`n03_star_over_a_case_twin_refuses_column_already_exists`,
`test_star_over_a_case_twin_frame_refuses_column_already_exists`), the missing-data pin is
deleted, and `test_case_twin_table_star_answers_under_true_and_refuses_under_false` pins
the real-data twin table answering under `true` and refusing under `false`. Supporting
moves: the empty-overwrite target probe reads through a scratch alias (it is engine
machinery, like its sibling probes), and the ambiguous-overwrite read-back counts instead
of starring. Registry ID-1a restated again (star FIXED). Known limitation, unrecorded:
`spark.table()` lowers eagerly to `SELECT *` text, so it refuses at construction over a
non-scratch twin table where Spark's lazy plan would only refuse the star itself.

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-56
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001 is checked clause-for-clause, not paraphrased: the CREATE pin asserts SELECT * names, the 0-row body and the DESCRIBE (name, int) rows; the CTAS pin asserts names, Int32 types and values [[1, 2]]; the false pin asserts the bare-name refusal.
      artifacts: [crates/repark-spark/src/tests/case_twin_columns.rs]
    - id: AT-2
      status: ATTACKED
      evidence: The twin pair (a, A) is exercised on both CREATE doors (column-def and CTAS), over an empty table (0 rows) and a one-row CTAS, with quoted and unquoted alias spellings attempted (the unquoted spelling refuses pre-fork, R-2).
      artifacts: [crates/repark-spark/src/tests/case_twin_columns.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Both failure surfaces are recorded verbatim: the old-rev build refusal on all three pins and the new-rev false-door bare-name refusal; the moved refusal (build to lookup) is proven by the red-then-green run, and the one RePark not-found mirror now uses the fallible lookup.
      artifacts: [crates/repark-spark/src/tests/case_twin_columns.rs, crates/repark-iceberg/src/write/partition_spec.rs]
    - id: AT-4
      status: N/A
      justification: No commit, isolation or concurrency surface in RePark changes; the repin moves one refusal from schema build to lookup on the single-threaded DDL path.
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling; the lane never uses AWS credentials and never runs any live catalog.
    - id: AT-6
      status: ATTACKED
      evidence: The lock delta is only the six fork crates' source lines; the stored twin columns read back with their ids and types (DESCRIBE int/int, CTAS Int32 [[1, 2]]), and the false-door CREATE acceptance opened by the lazy index is recorded as R-1 rather than absorbed.
      artifacts: [Cargo.lock, crates/repark-spark/src/tests/case_twin_columns.rs]
    - id: AT-7
      status: N/A
      justification: No performance claim; the lazy index is the fork's measured effect, not pinned here.
    - id: AT-8
      status: ATTACKED
      evidence: No new dependency and no new crate edge; the one RePark resolver mirroring the fork's not-found text moved to try_field_by_name_case_insensitive with the fork's own resolver as the contract, and the eight unchanged sites carry per-site reasons in the hand-back.
      artifacts: [crates/repark-iceberg/src/write/partition_spec.rs]
    - id: AT-9
      status: N/A
      justification: No new observability surface; every new refusal is a plan error carrying its full text, recorded verbatim for the old-rev build refusal and the false-door bare-name refusal.
    - id: AT-10
      status: ATTACKED
      evidence: M1 holds the old pin and all three pins red before the repin restores green; M2 flips one expectation and reds its pin. Branch liveness: the try_ error arm's nameable input is a partition-spec replace on a twin table (collided lookup fails where not-found was returned); unpinned, no test added per the minimal brief.
      artifacts: [crates/repark-spark/src/tests/case_twin_columns.rs]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-1 | Dated 2026-09-28 (follow-up unit, CASESENS-1 scope): at the new rev, column-def `CREATE TABLE (a INT, A INT)` under `caseSensitive=false` SUCCEEDS (probed `Ok` on the Spark door), while Spark refuses `[COLUMN_ALREADY_EXISTS]` (`tw_false_create`). The old rev refused via the eager index (with fork text, not Spark text). Neither RePark door checks duplicate column names at CREATE; CTAS-under-false twin behaviour is unrecorded (no `tw_false_ctas` probe). Fixing either refusal is new write-path behaviour and rides its own unit, never this repin. |
| R-2 | Dated 2026-09-28 (pre-fork refusal, recorded not pinned): CTAS `SELECT 1 AS a, 2 AS A` with both aliases unquoted refuses before reaching the fork — DataFusion folds the second alias and its projection-uniqueness check fails with `Plan("Projections require unique expression names but the expression \"Int64(1) AS a\" at position 0 and \"Int64(2) AS a\" at position 1 have the same name. Consider aliasing (\"AS\") one of them.")`. The pin quotes the second alias (`2 AS "A"`); Spark answers both spellings identically under `caseSensitive=true`. Wiring ident normalization to the flag is a separate unit. |
| R-3 | Dated 2026-09-28 (observed refusal text, asserted only as a refusal): a bare-name `SELECT a FROM` a twin table under `caseSensitive=false` fails with `Error during planning: [AMBIGUOUS_REFERENCE] Reference \`a\` is ambiguous, could be: [\`ice\`.\`sales\`.\`twf\`.\`a\`, \`ice\`.\`sales\`.\`twf\`.\`a\`]. SQLSTATE: 42704` (both candidates render lowercase). Spark has recorded no false-door SELECT-on-twins answer, so the pin asserts refusal only. |
| R-4 | Dated 2026-09-28 (unchanged lookups): eight `field_by_name_case_insensitive` sites stay infallible — the two `DROP COLUMN IF EXISTS` existence filters (tolerant semantics; a collided schema skips rather than refuses), the nested-describe error-path lookup (already a refusal, different message), `rewrite_where` (Spark-side ambiguity, unrecorded), `evolve_merge_schema` (the skip defers to the fork apply, which refuses), `name_known` (boolean guard; fallibility would ripple through move resolution), and the two `nested_add_refusal` lookups (the already-exists arm falls through to the fork refusal; the parent arm's Spark answer is unrecorded). Revisit when CASESENS-1 records those doors. |
| R-5 | Dated 2026-09-28 (owned by ICE-MIXED-CASE-1): that unit's C-016 evidence cites `test_measured_case_twin_table_refuses_at_adoption[L08_*]` and its adoption-refusal clause; both are superseded by this repin (adoption succeeds, references refuse at resolution). This unit does not edit another unit's ledger; the owning unit truths up at its next pickup. |
