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
| C-008 | No regression of the critic's held set: the four pin files JVM-free, the live leg at 512 passed / 0 skipped, `make verify`, and the report's mutation knobs still red. | The gate commands with real exit codes; mutation table. | **PROVEN** | JVM-free `420 passed, 93 skipped`; live `525 passed` (512 + 8 new live + 5 new door pins, 0 skipped); facade `5749 passed, 362 skipped`; `make verify` exit 0; Rust `lambda_door` 25 passed; M1–M5 all bite (2 / 1 / 6 / 4 / 4+61 reds). |
| C-009 | R2-F1 join-fed HOF width: a literal-fed `transform` through Join (both sides, cross), Aggregate group keys, Window, scalar subqueries, and every other plan node answers Int32 dtype-exact with Spark's not-null elements on the SQL door; the Column door and `aggregate` stay immune. | One pin per shape, dtype AND nullability exact; live oracle cells; red-first evidence. | **PROVEN** | Oracle `int32 not null` on every shape (banner spark 4.1.2 tz UTC ansi true). Red: Int64 nullable through Join/Aggregate (the tracer had no arm) and through scalar subqueries. Fix: full plan-lineage arms in `source_feeds_bare_integer` plus scalar-subquery tracing; the multi-hop element walk wraps not-null. Pins green; union-over-literals stays Int64 (pre-existing UNION coercion, out of scope, values agree). |
| C-010 | R2-F2 SQL-door multi-hop nullability: HOF over subquery-over-view, double CTE, DISTINCT/LIMIT/FILTER and every other multi-hop literal lineage answers Spark's not-null elements, or FNP8-NULLABILITY names each staying shape with a red-when-fixed pin. | Pins per shape; registry extension if any shape stays divergent. | **PROVEN** | Oracle `int32 not null` on all five literal-lineage shapes. Red: nullable elements. The same multi-hop `source_element_nullable` walk serves every shape, so no registry extension was needed. CTE-over-parquet-table measures nullable/nullable on BOTH engines (table-ctrl), i.e. Spark-equal with no wrap. The round-1 Column-view nullable instance converged (pin, live leg, and NULLABILITY sentence updated). |

VERDICT: 10 clauses, 10 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: fnp-8-review
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: All eight clauses walked against the critic report. F1-F4 fixed red-first against live PySpark 4.1.2 (banner spark 4.1.2 tz UTC ansi true); F5-F7 are the report's own wording asks; C-008 re-ran the held set unchanged. Counts read from the pytest summary lines.
      artifacts: [python/repark/tests/test_fnp_8_sql_door.py, python/repark/tests/test_parity_live_fnp8.py]
    - id: AT-2
      status: ATTACKED
      evidence: Shorter-either-side zip, empty and NULL arrays, NULL elements, explicit BIGINT casts, out-of-i32 literals, negative literals, VALUES columns, subquery columns, unions, temp views, nested arrays and maps, both ANSI settings - every boundary the prep rule branches on has a pin that names its output.
      artifacts: [python/repark/tests/test_fnp_8_sql_door.py, crates/repark-spark/src/tests/lambda_door.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The F2 Arrow crash now answers on all three doors. Refusals kept loud: the reworded Column-door nested refusal, EX-FN-4, Spark arity errors. The 110-cell error oracle and its dispositions stay green.
      artifacts: [python/repark/tests/test_fnp_8_sql_door.py, python/repark/tests/test_fnp8_oracle_matrix.py]
    - id: AT-4
      status: N/A
      justification: Pure analyzer rewriting plus UDF metadata derivation. No shared or mutable state, no locks, no async, no spawn. The provisional trace uses an explicit worklist, never recursion over plan depth.
    - id: AT-5
      status: N/A
      justification: No authn/authz, no deserialization, no path, credential or network surface. unsafe_code stays workspace-forbidden and this unit adds none.
    - id: AT-6
      status: ATTACKED
      evidence: Every fixed shape pins values plus dtypes against the live oracle; the eight converged zip_empty/zip_null disposition cells were regenerated from measured output with row-equality asserted first, and FNP8-WIDTH/FNP8-NULLABILITY narrowed or extended to match. No silent drift.
      artifacts: [python/repark/tests/fnp8_repark_dispositions.json, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: N/A
      justification: Plan-time-only tree walks linear in HOF argument size, on queries that already analyze. No executor hot path touched, no new materialization, no unbounded growth. Not a system-breaking change.
    - id: AT-8
      status: ATTACKED
      evidence: DataFusion 54.1 coalesce nullability (non-null when any argument is non-null) read from the vendored source before relying on it for F3. The LambdaRebind seat, the error taxonomy, and the refusal contracts are unchanged; the map() rewrite shape was read off the measured plan.
      artifacts: [crates/repark-functions/src/higher_order/zip_with.rs, crates/repark-functions/src/lambda_rebind.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The Column-door nested-HOF refusal now names the refusing door and the serving door, pinned by the pre-existing refusal pins matching the kept phrases. Error-oracle dispositions pin every loud failure path.
      artifacts: [crates/repark-python/src/column/expr_build.rs, python/repark/tests/test_fnp8_oracle_matrix.py]
    - id: AT-10
      status: ATTACKED
      evidence: Five new door pins ran 4 red / 1 green before any fix and went green slice by slice. Five mutations (M1-M5) red 2 / 1 / 6 / 4 / 4+61 selections; every added branch has a nameable input on which it flips the output. Mutation table above.
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
| M4 | two-valued `exists` | `cargo test -p repark-spark --lib tests::lambda_door` | 4 failed (critic: 2; the F6 null-predicate legs add the other two) |
| M5 | parameter packing skipped | Rust `lambda_door` + the four pin files | 4 Rust + 61 Python failed (critic: 4 + 59) |

Red-first record: the five new door pins ran 4 red / 1 green (inline control)
before any fix; F2/F3 went green after the zip slice, F1 after the prep
slice, F4 after the wrapping slice. Every mutant and every restoration
rebuilt clean; `git status` clean after each revert.
