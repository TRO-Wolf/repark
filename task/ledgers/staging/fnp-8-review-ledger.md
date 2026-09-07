# Unit ledger — FNP-8-REVIEW · remediation round 1 for the merged-unreviewed FNP-8

**Date:** 2026-09-07 · **Branch:** `review/fnp-8-review` · **Base:** `origin/main`
`7a8e94ce` plus orchestrator `16ae1462` · **Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** FNP-8 (PR #412) merged unreviewed. The round-1 critic report
(`$HOME/repark-lanes/briefs/fnp8rev-critic/report-r1.md`, Muse Spark, 2026-09-07,
live PySpark 4.1.2, Zulu 17, `local[2]`, UTC, ANSI on) filed seven findings
F1–F7 against squash-merge `7a8e94ce` and a held set the remediation must not
regress. This unit fixes every finding red-first against the same live oracle.
The frozen FNP-8 ledger in `../completed/` is read-only; the orchestrator
prepends its errata. **Round 2 (2026-09-07):** the round-2 critic report
(`$HOME/repark-lanes/briefs/fnp8rev-critic/report-r2.md`, live PySpark 4.1.2,
same basis) SERVED F1–F7 and filed R2-F1 (join-fed width, S2), R2-F2 (multi-hop
nullability, S3), and R2-F3 (ledger accuracy, S4). C-009/C-010 carry the two
behavior findings; C-008's counts and the attestation are trued up at the
round's close.

**Not in this unit:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`,
`Cargo.lock`, dependency lists, the fork pin, completed ledgers, code comments.

## PROPOSITION LEDGER — FNP-8-REVIEW — 2026-09-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | F1 table-backed lambda-body width: integer literals inside `transform`, `zip_with`, `transform_values` bodies over table/view inputs answer Int32 dtype-exact on both doors, or the unfixable shape carries a registry row plus a red-when-fixed pin. | Table/view AND inline-literal pins per name per door; live oracle cells. | **PROVEN** | Oracle `list<element: int32 not null>` / `map<string,int32>` over the view (banner spark 4.1.2 tz UTC ansi true). Red: Int64 on the SQL door. Fix: prep narrows every HOF body plus direct ctor literals pre-coercion, skipping column-traced provisional shapes the late rules fix together. Pins green; zip_empty/zip_null converged to Spark (dispositions updated, FNP8-WIDTH narrowed). |
| C-002 | F2 left-shorter `zip_with` crash: the shorter-LEFT non-null-element shape answers on all three doors (SQL, Column, `F.expr`); right-shorter kept. | One pin per door plus the kept right-shorter leg; live oracle cells. | **PROVEN** | Oracle `{'r': [11, 20]}`, `list<element: int32> not null` (banner spark 4.1.2 tz UTC ansi true). Red: Arrow non-nullable crash on all three doors. Fix: nullable zip params. Pin green; right-shorter kept. |
| C-003 | F3 `zip_with` element nullability derives from the lambda: the null-proof `coalesce` body answers `not null`, the nullable counterpart stays nullable. | Both shapes pinned; live oracle cells. | **PROVEN** | Oracle null-proof `list<element: int32 not null>`, nullable `list<element: int32>`. Red: null-proof answered nullable. Fix: element derives `lambda.is_nullable()`. Both pins green. |
| C-004 | F4 nested outer-variable nullability: `transform(arr, a -> transform(a, b -> b + 1))` innermost is `int32 not null` with the nullable counterpart pinned, or `FNP8-NULLABILITY` names the divergent shape with its pin. | Pins for both shapes; registry row if divergent. | **PROVEN** | Oracle innermost `int32 not null`, nullable counterpart `int32` nullable. Red: innermost nullable. Fix: bottom-up nested-constructor wrapping. Pin green on SQL and F.expr. The staying-divergent Column-view instance joins FNP8-NULLABILITY with its pin. |
| C-005 | F5 Column-door nested-HOF refusal names the Column door as the refusing side and the SQL door as serving nested lambdas. | The reworded refusal; existing refusal pins still green. | **PROVEN** | `expr_build.rs` refusal reworded; `test_nested_higher_order_is_refused_rather_than_silently_wrong` and `test_nested_higher_order_stays_refused` match the kept phrases. |
| C-006 | F6 `sql_door_exists_and_forall_answer_three_valued` carries a null-predicate leg or is renamed. | The Rust pin with the added leg. | **PROVEN** | Null-predicate `exists`/`forall` legs added; `cargo test -p repark-spark lambda_door` green. |
| C-007 | F7 the `F-Y10-1` note cites `sql_door_lambda_body_overflow_divergence_wraps` and the error-oracle idx 25/51 dispositions. | The extended note. | **PROVEN** | Note cites the wrap pin plus error-25 (ANSI raise vs wrap) and error-51 (shared wrap). |
| C-008 | No regression of the critic's held set: the four pin files JVM-free, the live leg at 512 passed / 0 skipped, `make verify`, and the report's mutation knobs still red. | The gate commands with real exit codes; mutation table. | **PROVEN** | Round-2 close: JVM-free `429 passed, 93 skipped`; live `538 passed` (512 + 14 door + 12 live, 0 skipped); facade `5767 passed, 366 skipped`; `make verify` exit 0 with 2830 Rust passed; Rust `lambda_door` 25 passed; M1–M5 re-measured (2 / 1 / 6+1 / 3 / 4+61) and M6–M13 bite per arm (1 / 6 / 0-documented / probe / 1 / 1 / 1 / crash). |
| C-009 | R2-F1 join-fed HOF width: a literal-fed `transform` through Join (both sides, cross), Aggregate group keys, Window, scalar subqueries, and every other plan node answers Int32 dtype-exact with Spark's not-null elements on the SQL door; the Column door and `aggregate` stay immune. | One pin per shape, dtype AND nullability exact; live oracle cells; red-first evidence. | **PROVEN** | Oracle `int32 not null` on every Spark-served shape (banner spark 4.1.2 tz UTC ansi true). Red: Int64 nullable through Join/Aggregate (the tracer had no arm) and through scalar subqueries. Fix: full plan-lineage arms in `source_feeds_bare_integer` plus scalar-subquery tracing; the multi-hop element walk wraps not-null. Pins green; union-over-literals stays Int64 (pre-existing UNION coercion, out of scope, values agree). The scalar-subquery shape is a repark superset: Spark refuses it (`UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY`), repark answers its deterministic value. |
| C-010 | R2-F2 SQL-door multi-hop nullability: HOF over subquery-over-view, double CTE, DISTINCT/LIMIT/FILTER and every other multi-hop literal lineage answers Spark's not-null elements, or FNP8-NULLABILITY names each staying shape with a red-when-fixed pin. | Pins per shape; registry extension if any shape stays divergent. | **PROVEN** | Oracle `int32 not null` on all seven literal-lineage shapes (the critic's five plus CTE-over-view and FILTER). Red: nullable elements. The same multi-hop `source_element_nullable` walk serves every shape, so no registry extension was needed. CTE-over-parquet-table measures nullable/nullable on BOTH engines (table-ctrl), i.e. Spark-equal with no wrap. The round-1 Column-view nullable instance converged (pin, live leg, and NULLABILITY sentence updated). The walk also resolves VALUES rows (pinned Spark-equal) and wraps only join sides the join cannot pad: padded outer sides stay conservatively nullable (Spark: not-null elements, values agree, measured session 5) instead of crashing Arrow on a false non-null field. |

VERDICT: 10 clauses, 10 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: fnp-8-review
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: All ten clauses walked against both critic reports. F1-F4 fixed red-first against live PySpark 4.1.2 (banner spark 4.1.2 tz UTC ansi true); F5-F7 are the report's own wording asks; R2-F1/R2-F2 fixed red-first against the same oracle in five more probe sessions; R2-F3 trues up M4 and AT-2 below; C-008 re-ran the held set unchanged. Counts read from the pytest summary lines.
      artifacts: [python/repark/tests/test_fnp_8_sql_door.py, python/repark/tests/test_parity_live_fnp8.py]
    - id: AT-2
      status: ATTACKED
      evidence: Shorter-either-side zip, empty and NULL arrays, NULL elements, explicit BIGINT casts, out-of-i32 literals, negative literals, VALUES columns, subquery columns, unions (the R2-F3 gap, now pinned), temp views, nested arrays and maps, join-fed literals both sides plus cross, aggregate group keys, window above and below the constructor, scalar subqueries, multi-hop view and CTE lineage, aggregate immunity, the lateral-view refusal, both ANSI settings - every boundary the prep rule branches on has a pin that names its output.
      artifacts: [python/repark/tests/test_fnp_8_sql_door.py, crates/repark-spark/src/tests/lambda_door.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The F2 Arrow crash now answers on all three doors. Refusals kept loud: the reworded Column-door nested refusal, EX-FN-4, Spark arity errors. The 110-cell error oracle and its dispositions stay green. Round 2 pins the lateral-view refusal that keeps the Generate node unreachable.
      artifacts: [python/repark/tests/test_fnp_8_sql_door.py, python/repark/tests/test_fnp8_oracle_matrix.py]
    - id: AT-4
      status: N/A
      justification: Pure analyzer rewriting plus UDF metadata derivation. No shared or mutable state, no locks, no async, no spawn. The width tracer uses an explicit worklist; the round-2 nullability walk recurses over plan depth instead (one small frame per nesting level, union fan-in read off the branch results). Depth is bounded by what the recursive-descent SQL parser already survived with larger frames, so the walk cannot reach a depth the parse did not.
    - id: AT-5
      status: N/A
      justification: No authn/authz, no deserialization, no path, credential or network surface. unsafe_code stays workspace-forbidden and this unit adds none.
    - id: AT-6
      status: ATTACKED
      evidence: Every fixed shape pins values plus dtypes against the live oracle; the eight converged zip_empty/zip_null disposition cells were regenerated from measured output with row-equality asserted first, and FNP8-WIDTH/FNP8-NULLABILITY narrowed or extended to match. Round 2 converged the Column-view nullable instance the same way (pin, live leg, and registry sentence moved together). No silent drift.
      artifacts: [python/repark/tests/fnp8_repark_dispositions.json, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: N/A
      justification: Plan-time-only tree walks linear in HOF argument size, on queries that already analyze. No executor hot path touched, no new materialization, no unbounded growth. Not a system-breaking change.
    - id: AT-8
      status: ATTACKED
      evidence: DataFusion 54.1 coalesce nullability (non-null when any argument is non-null) read from the vendored source before relying on it for F3. The LambdaRebind seat, the error taxonomy, and the refusal contracts are unchanged; the map() rewrite shape was read off the measured plan. Round 2 derived every lineage mapping from the vendored source (the LogicalPlan variants, the join-schema builder, Unnest dependency indices, the single-pass analyzer), and the prep-runs-per-analysis observation from an instrumented run.
      artifacts: [crates/repark-functions/src/higher_order/zip_with.rs, crates/repark-functions/src/lambda_rebind.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The Column-door nested-HOF refusal now names the refusing door and the serving door, pinned by the pre-existing refusal pins matching the kept phrases. Error-oracle dispositions pin every loud failure path.
      artifacts: [crates/repark-python/src/column/expr_build.rs, python/repark/tests/test_fnp8_oracle_matrix.py]
    - id: AT-10
      status: ATTACKED
      evidence: Five new door pins ran 4 red / 1 green before any fix and went green slice by slice. Five mutations (M1-M5) red 2 / 1 / 6 / 3 / 4+61 selections (M4 corrected per R2-F3). Round 2: nine new door pins ran 5 red / 2 green-immune on the unfixed code (bite-check), with the VALUES pin red pre-arm via probe and the padded-side pin biting its guard; arm mutants M6-M13 bite per arm (M8's round-1 Union width arm is unbitten and documented as such); the critic's eight knobs re-measured K1 2, K2 1, K3 6+1, K4 3, R-A 361+23, R-B 3, R-C 4+61, R-D 1/7. Mutation tables below.
      artifacts: [task/ledgers/staging/fnp-8-review-ledger.md]
  complete: true
```

## 2026-09-07 VALUES follow-up (C-001)

`sql_door_higher_order_over_values_keeps_narrowed_int32` redded after the F1
slice: a direct constructor over provisional VALUES columns plus a narrowed
body bakes Int64. The provisional skip now traces value-side columns to bare
literals (VALUES rows, constructor sources, double-nested subqueries,
unions) with an iterative worklist; analyzed and base-table columns still
narrow early. Rust `lambda_door` 25 passed; the four pin files 420 passed.

## Mutation (2026-09-07, each reverted; tree clean after)

| Knob | Mutant | Selection | Result |
|---|---|---|---|
| M1 | F2 reverted (zip params non-nullable) | `test_fnp_8_sql_door.py -k "left_shorter or element_nullability"` | 2 failed: both zip pins crash (`Column 'x'` / `Column 'y' non-nullable`) |
| M2 | F3 reverted (zip element `true`) | same selection | 1 failed: `test_zip_with_element_nullability_follows_the_lambda`; left-shorter stays green |
| M3 | HOF preparation dropped | `test_fnp_8_sql_door.py` (40 tests) | 6 failed: indexed shapes, exists nullability, aggregate expr, F1 table pin, F4 pin |
| M4 | two-valued `exists` | `cargo test -p repark-spark --lib tests::lambda_door` | 3 failed: `exists_nullability`, `three_valued`, `edge_rows` (round-2 correction of the 4 claimed here — R2-F3; re-measured under R-B) |
| M5 | parameter packing skipped | Rust `lambda_door` + the four pin files | 4 Rust + 61 Python failed (critic: 4 + 59) |

Red-first record: the five new door pins ran 4 red / 1 green (inline control)
before any fix; F2/F3 went green after the zip slice, F1 after the prep
slice, F4 after the wrapping slice. Every mutant and every restoration
rebuilt clean; `git status` clean after each revert.

## Round-2 mutations (2026-09-07, each reverted; tree clean after)

| Knob | Mutant | Selection | Result |
|---|---|---|---|
| M6 | Join width arm dropped | 7 new door pins + `table_backed` | 1 failed: `test_join_fed_lambda_body_literals_answer_int32` |
| M7 | nullability walk reverted to single-hop | same selection | 6 failed: all but `aggregate_over_lineage` and `lateral_view` |
| M8 | Union width arm dropped (round-1 arm) | same selection | 0 failed: early-narrowed bodies converge for unions because each branch narrows late; kept as sound defense-in-depth, and the union pin bites the nullability Union arm via M7 |
| M9 | Window width arm dropped | same selection + ctor-below-window probe | 0 pin red on the old legs; the probe bakes Int64, so the shape joined the pin as the biting leg |
| M10 | Aggregate width arm dropped | same selection | 1 failed: `test_lineage_through_plan_nodes_keeps_int32` |
| M11 | scalar-subquery arm dropped | same selection | 1 failed: `test_scalar_subquery_hof_answers_int32` |
| K1 | zip params non-nullable | `test_fnp_8_sql_door.py -k "left_shorter or element_nullability"` | 2 failed, both zip pins |
| K2 | zip element `true` | same selection | 1 failed: the element-nullability pin; left-shorter stays green |
| K3 | pre-bind narrowing skipped | `test_fnp_8_sql_door.py` (47 tests) + Rust `lambda_door` | 6 Python failed (indexed x2, F1 table, F4, multihop, union) + 1 Rust failed (indexed width) |
| K4 | bottom-up wrap dropped | `test_fnp_8_sql_door.py` (47 tests) | 3 failed: indexed nonnull, exists nullability, F4 (the critic's narrower selection counted the F4 leg only) |
| R-A | Spark dialect forced to Generic | Rust `lambda_door` + the four pin files | 23 Rust + 361 Python failed (355 at R2 + 6 of the 7 new pins; full `--lib` adds the `lambda_arrow_selects_databricks` unit test for 24) |
| R-B | two-valued `exists` | `cargo test -p repark-spark --lib tests::lambda_door` | 3 failed: `exists_nullability`, `three_valued`, `edge_rows` |
| R-C | parameter packing skipped | Rust `lambda_door` + the four pin files | 4 Rust + 61 Python failed |
| R-D | early rebind seat dropped / both seats dropped | `cargo test -p repark-spark --lib tests::lambda_door` | early: 1 failed (VALUES test); both: 7 failed |
| M12 | VALUES nullability arm dropped | `test_fnp_8_sql_door.py -k "values_fed"` | 1 failed: `test_values_fed_hof_answers_int32` |
| M13 | join padding guard dropped | outer-join probes + `test_outer_join_padded_side_answers_nullable` | 3 probes hard-error (`Column 'r' is declared as non-nullable but contains null values`); the pin reds |

Not re-run, R1 evidence stands (files untouched, pins unchanged): aggregate-null-mask,
force-nullable-exists, arity-bypass, callable-name, name-preservation.

Round-2 red-first record: the seven new door pins ran 5 red / 2 green-immune
(`aggregate_over_lineage` and `lateral_view` lock behavior the fix must not move)
against the unfixed code, plus the converged `table_backed` leg red; the VALUES
pin redded pre-arm via probe and the padded-side pin bites its guard; all green
after the fix. Five oracle probe sessions plus the live legs, one JVM at a time,
none lingering. Every mutant and every restoration rebuilt clean; `git status`
clean after each revert.

## Measured but not filed (round 2)

Shapes measured against live PySpark 4.1.2 that stay out of scope, values agreeing
throughout: union-over-literals stays Int64 on repark vs Int32 on Spark
(pre-existing UNION coercion, no HOF involved); NULL-row VALUES stays Int64 vs
Int32 (pre-existing mixed-row coercion); padded outer-join sides stay
conservatively nullable on repark vs not-null elements on Spark (the padding
guard's documented tradeoff); recursive CTEs over arrays refuse at plan time
(pre-existing DataFusion limitation, HOF-independent); the scalar-subquery HOF
is a repark superset (Spark refuses the shape, repark answers it).
