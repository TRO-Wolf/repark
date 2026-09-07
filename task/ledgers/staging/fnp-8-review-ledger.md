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
prepends its errata.

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
| C-008 | No regression of the critic's held set: the four pin files JVM-free, the live leg at 512 passed / 0 skipped, `make verify`, and the report's mutation knobs still red. | The gate commands with real exit codes; mutation table. | **OPEN** | TBD: final gate run. |

VERDICT: 8 clauses, 7 PROVEN, 1 OPEN, 0 REJECTED.
