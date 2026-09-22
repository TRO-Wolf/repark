# Charter ledger — FNP-AGG-1 slice (d) · grouping_id plus the shared foundation

**Date:** 2026-09-21 · **Branch:** `fix/fnp-agg-grouping` · **Base:** `6fa0b7d8->2c782a99` · **Model:**
muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `FNP-AGG-1-18B` lands FIXED in this slice; no other registry row.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The 1.5 Spark-parity campaign measured fourteen aggregate names against live
PySpark 4.1.2 (`/tmp/oc-worker/pa-agg/agg_misc_spark_oracle.json`, 2026-09-14). This slice
ports the `grouping_id` family plus the shared foundation out of the rescue reference
`85497cb2` onto current `origin/main`, so `grouping_id` answers PySpark 4.1.2 on both
doors. Slices (a), (b) and (c) follow after this lands.

## Decisions (orchestrator rulings under G-2, carried from the unit charter)

| Id | Ruling |
|---|---|
| D-1 | Kernels in `crates/repark-functions/src/`, registered in `register_all` (`lib.rs`) for the SQL door. The facade reaches a new aggregate through the aggregate-kind match tables in `crates/repark-python/src/column/function_dispatch.rs` — ADD arms only, never edit or reorder existing arms. |
| D-2 | Never edit `dataframe/**`, `column.py`, `session/**`, `catalog.py`, `types.py`, the SQL parser/dialect. `cube`/`rollup` live in GroupedData: use them as they are. |
| D-3 | Registry: one section-7 row per residual divergence measured, each with a pin. Slice (d) lands `FNP-AGG-1-18B` FIXED with its passing pin. |
| D-4 | Pins: `python/repark/tests/test_fnp_agg_1.py`, both doors, parametrized over the oracle cells; Rust unit tests beside each UDAF. Floats compare exactly (Spark's value); nullability is pinned except the VALUES-derived grouping key. |
| D-5 | Performance: each UDAF implements `merge_batch`/`state` (multi-partition safe); `retract_batch` is NOT required. No per-row allocation in `update_batch` beyond what the algorithm needs. |

## Grouping rulings applied in this slice

| Id | Ruling |
|---|---|
| R-18a-7 | `grouping_id` next: Rust decides the bitmask and both error classes; GroupedData (`dataframe/**`) untouched. |
| R-18a-9 | Signature helper strips the `: annotation` in the `*args` branch so the oracle's `(*cols: 'ColumnOrName')` compares by name `cols`; the oracle file is never edited. |
| R-18a-10 | Spark does not guarantee emission order of rows tied on every `orderBy` key: tie pins compare order-insensitively within runs of equal sort keys, order-sensitively across runs; values, types, names and nullability stay exact. |
| R-18a-19 | `grouping_id(args)` accepted only when args equal the grouping-column list exactly, in order; otherwise `GROUPING_ID_COLUMN_MISMATCH` with Spark's message shape, both doors. |
| OP1 | Filed row `FNP-AGG-1-18B` is stale: the SQL `tinyint` pin passes unmarked at this head (the rebase closed LOGICAL-WIDTH-1 for this label). The row lands FIXED; no xfail is carried. |

## Proposition ledger

| Clause | Claim | Verdict | Evidence |
|---|---|---|---|
| C-001 | `grouping_id` is present on `repark.spark.functions` with the PySpark 4.1.2 signature. | PROVEN | `test_facade_names_present_and_exported` and `test_facade_signature_matches_spark[grouping_id]` green against the recorded `(*cols: 'ColumnOrName')` signature. |
| C-002 | The Python-door grouping value cells answer the fixture rows, types and nullability. | PROVEN | 4/4 card Python value cells green plus `test_python_cube_grouping_reaches_rust_rule` (int8 Arrow, `[0]*8 + [1]*5`) and the critic Python value cell; ties compare per R-18a-10. |
| C-003 | The SQL-door grouping value cells answer, and the `tinyint` label matches Spark. | PROVEN | 2/2 card SQL value cells green, `test_sql_grouping_reports_tinyint` passes unmarked (registry `FNP-AGG-1-18B` FIXED), the critic SQL value cell green, and the `test_types_1` grouping pins flipped to `int8`/`int8`. |
| C-004 | The grouping error cells fail with Spark's own error condition on their own door. | PROVEN | 6/6 card error pins green (`GROUPING_ID_COLUMN_MISMATCH` subset refusal, `UNSUPPORTED_GROUPING_EXPRESSION` outside grouping sets) plus 6/6 critic refusal pins including cube-reversed args. |
| C-005 | Multi-partition merge: the grouping accumulator merges cleanly across partitions. | PROVEN | Kernel `constant_accumulator_merges_cleanly` green beside the UDAF; all `grouping` kernel tests green. |
| C-006 | No regression: the touched suites stay green. | PROVEN | `cargo test -p repark-functions --lib grouping` green; card + critic + split-identity + grouping `test_types_1` cells green; EX-0 1083 and cap-1 mirrors green. |
| C-007 | Registry (`FNP-AGG-1-18B` → FIXED) and every touched `map.md` in lockstep. | PROVEN | 18B row lands FIXED with its passing pin; `agg_misc.py` covers `F.grouping_id` with inventory +1 and EX-0 1082 → 1083; all nine touched `map.md` files carry slice-(d) rows. |

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED.

## Gates (2026-09-21, this head)

| gate | result |
|---|---|
| `cargo test -p repark-functions --lib grouping` | exit 0 — 10 passed, 0 failed (9 `grouping::tests` + 1 pre-existing `java_double` filter match) |
| `test_fnp_agg_1.py` + `test_fnp_agg_1_critic.py` | exit 0 — 24 passed (16 card + 8 critic), 0 failed, 0 xfail |
| `test_functions_split_identity.py`, grouping `test_types_1.py` | exit 0 — 4 passed |
| `test_ex_0_example_coverage.py`, `test_cap_1_source_file_line_cap.py` | exit 0 — 49 passed (EX-0 1083, mirrors hold) |
| `check_example_coverage.py --require-execute` | exit 0 — 108 backlog, 250 examples; `agg_misc.py` prints the cube rows |
| `check_lib_rs.py`, `check_rust_file_size.py`, `check_lib_py.py` | exit 0 — all clean (184 / 1012 / 1984 re-measured head-vs-main) |
| `check_ledger_grammar.py`, `check_map_md.sh` | exit 0 — 243 ledgers clean; maps clean |
| `make rust-clippy`, `make rust-panic-ban` | exit 0 — both clean |
| comment-ban self-check vs `origin/main` | `comment-ban hits=0` |

```
COVERAGE_ATTESTATION:
  pr_unit: fnp-agg-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every grouping clause is pinned per door against the recorded 4.1.2 cells; the exact-order rule and both error classes are pinned on both doors.
      artifacts: [python/repark/tests/test_fnp_agg_1.py, python/repark/tests/test_fnp_agg_1_critic.py]
    - id: AT-2
      status: ATTACKED
      evidence: Zero-arg, subset, reversed-arg, non-set-column and plain-GROUP-BY shapes are all pinned; empty-frame behavior rides the shared cube path.
      artifacts: [python/repark/tests/test_fnp_agg_1_critic.py]
    - id: AT-3
      status: ATTACKED
      evidence: Both grouping error classes are pinned with Spark's message shape on both doors; the refusal mapping stamps UNRESOLVED_ROUTINE for unknown routines.
      artifacts: [python/repark/tests/test_fnp_agg_1.py, crates/repark-python/src/unresolved_routine.rs]
    - id: AT-4
      status: N/A
      justification: No shared state or ordering; the rule rewrites one plan and the UDAFs are pure accumulators.
    - id: AT-5
      status: N/A
      justification: No privileged action, injection surface, secret or deserialization; inputs are plan nodes.
    - id: AT-6
      status: ATTACKED
      evidence: The grouping label moves Int32 to Int8 on both doors with values unchanged; the live carve-out pins the (int8, int8) pair.
      artifacts: [python/repark/tests/test_types_1.py]
    - id: AT-7
      status: N/A
      justification: No hot path changes; the analyzer rule runs once per plan and the accumulator holds constant state.
    - id: AT-8
      status: ATTACKED
      evidence: The n-ary widening keeps the one pre-existing aggregate_binary caller green via the adapted call-site; size ceilings moved per the sanctioned outs.
      artifacts: [python/repark/src/repark/spark/functions_expr.py, scripts/check_lib_py.py]
    - id: AT-9
      status: ATTACKED
      evidence: Refusals carry Spark's condition and message shape, diagnosed from the pinned error text on both doors.
      artifacts: [crates/repark-functions/src/grouping.rs]
    - id: AT-10
      status: ATTACKED
      evidence: 16 card pins plus 8 critic pins plus 10 grouping-filter kernel tests; the exact-order, reversed-arg and subset branches each have a nameable refusing input.
      artifacts: [python/repark/tests/test_fnp_agg_1.py, crates/repark-functions/src/grouping.rs]
  reattested: []
  complete: true
```
