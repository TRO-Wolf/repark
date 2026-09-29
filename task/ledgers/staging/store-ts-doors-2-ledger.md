# Unit ledger — WO STORE-TS-DOORS-2 · a VALUES node inside INSERT … SELECT and the static-partition OVERWRITE refuse TIMESTAMP/DATE/`-NULL` sources like Spark

**Date:** 2026-09-29 · **Branch:** `fix/store-ts-doors-2` · **Base:** `57ebb57a` (`origin/main`, v1.5.1)
**Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Two silent pre-existing write doors from the v1.5.1 release differential,
both carried by v1.5.1: PE-18 — `INSERT INTO t SELECT a, b FROM VALUES (1,
TIMESTAMP'…') AS v(a, b)` into a BIGINT column stores epoch seconds where Spark
4.1.2 refuses `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`; PE-24 — `-NULL`
into a TIMESTAMP column through `INSERT OVERWRITE t PARTITION (p='a') VALUES (…,
-NULL)` stores NULL where Spark refuses. WO STORE-TS-TO-NUMERIC-1 (#885) closed
the same source classes on the other doors; this unit reuses #885's gate at the
two doors it missed.

**Step 0 (measured before any edit, 2026-09-29, zone UTC).** 78-statement matrix
plus 8 follow-up and 8 gap statements, RePark on `57ebb57a` against Spark 4.1.2
+ Iceberg 1.11.0 (`f0bb2e6a47d0ebda424ffd633fcea8644a597954`, session zone UTC
printed from a live session): the two repros; the VALUES-in-SELECT shapes through
a CTE (both spellings), a temp view, a join with a VALUES node (table side and
VALUES side), UNION of VALUES, a nested derived table with column aliases,
WHERE, and ORDER BY/LIMIT; the static-partition OVERWRITE with every #885 source
type (TIMESTAMP, `CAST(1 AS TIMESTAMP)`, TIMESTAMP column, DATE, `-NULL`, NULL,
INT, DECIMAL, STRING, BOOLEAN, `CAST(… AS TIMESTAMP_NTZ)`) into TIMESTAMP,
BIGINT, DATE and BOOLEAN targets; the SELECT, column-list, dynamic-partition and
BY NAME partition forms as controls.

Classes on base: 14 statements store where Spark refuses (the fix set: 9
VALUES-in-SELECT TIMESTAMP/`CAST(1 AS TIMESTAMP)` into BIGINT/DOUBLE, 3
`-NULL` through static-partition OVERWRITE VALUES, and 2 gap statements —
ORDER BY/LIMIT and SELECT-form `-NULL`); 4 both-refuse message differences the
fix retexts to Spark's exact refusal (DATE into INT via `DATATYPE_MISMATCH`
twice, VALUES⋈VALUES via `Inconsistent data length`, NTZ via
`DATATYPE_MISMATCH`); 4 both-refuse message differences kept as on base (view via
the WI-1 `repark_insert_store_assignment` text, CTE-before-INSERT via `not
implemented`, the arity probe bug, `unix_seconds`/`year` function-resolution
failures); 2 STRING stores owned by STORE-STRING-ASSIGN-1 kept as on base; every
other cell already matches Spark outcome and value (zone-clean readbacks via
`CAST AS STRING`: the first probe's 5-hour skew was driver-zone rendering in
`collect()`, not a product difference).

**Cause.** PE-18: the analyzer insert gate judges only a conform cast over a bare
source column, so a VALUES node inside a SELECT falls into its named residual
(`insert_gate.rs` `a_literal_values_row_is_the_named_residual_and_is_not_gated`),
and #885's VALUES gate reads only an INSERT whose body is a VALUES list.
PE-24: #885's INSERT `-NULL` gate maps the source against the full table width,
so a static-partition source (narrower than the table) bails on the width check,
and the partition-overwrite staging gate judges `Null` as storable anywhere.

**Fix.** Refusals only; nothing new is admitted. Both reuse #885's gate and its
`source_leaves` rule unchanged; no second copy.
- `void_type/select_values_arms.rs` (new): resolves an INSERT source into
  VALUES-cell arms — transparent projections (plain column refs, `*`) over
  derived tables, CTEs, UNION/INTERSECT/EXCEPT arms and joins, each output
  position mapped to the VALUES cells that flow into it, anything else (table
  and view columns, literals, CASTs, functions, ambiguous or unresolvable refs,
  unexpandable stars) left unmapped. `ltz_values_store.rs` projects each arm's
  rows and judges them through the unchanged `check_row`/`refuse_value`, so the
  STRING silence and the leaf-reliability rule apply exactly as on the VALUES
  door. The direct-VALUES path is untouched (its table load moved verbatim into
  `load_presented`).
- `void_type/insert_source_types.rs`: `refuse_partition_overwrite_sources` maps
  the filled partition-overwrite source onto the table columns minus the static
  partition columns (or by column list, bailing when a static column is listed)
  and runs the shared `refuse_negated_null_writes`. Called once from
  `execute_partition_overwrite`, so the static, dynamic, listed and BY NAME
  partition forms all pass it; the listed, dynamic and BY NAME forms already
  refused with the identical text through #885's earlier gate, so only the
  static-unlisted `-NULL` cells change.

**After the fix (same matrix, same recorded Spark answers).** All 14 fix-set
cells refuse with Spark's class, first line and SQLSTATE (`KD000`), byte-identical
to the recorded Spark text, and write nothing; the 4 retexted cells now also
carry Spark's exact text. 63 matrix cells are byte-identical to base (outcome,
text and stored value); zero cells where head refuses and base and Spark both
store; zero stored-value changes.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | TIMESTAMP, `CAST(1 AS TIMESTAMP)`, TIMESTAMP_NTZ and DATE sources through a VALUES node inside INSERT … SELECT into numeric columns refuse with Spark's recorded class, SQLSTATE and message body, and write nothing; the direct, star, CTE, UNION, alias, join, VALUES-join, WHERE and ORDER BY/LIMIT shapes are each pinned, and explicit `CAST(b AS BIGINT)`, INT and NULL still store. | `test_store_ts_doors_2.py` replays the recorded `vselect/*` refusal and store cells; the resolver classifier tests. | PROVEN | 15 refusal cells and 4 store cells equal `store_ts_doors_2_spark_oracle.json`; on `57ebb57a` 10 stored, 4 refused with a different text, and the WHERE shape stored (measured on a base rebuild). 8 resolver classifier tests pass. |
| C-002 | `-NULL` into TIMESTAMP, DATE and BOOLEAN columns through static-partition OVERWRITE VALUES and SELECT refuses with Spark's text naming `"DOUBLE"`, and writes nothing; `-NULL` into BIGINT still stores NULL; the listed, dynamic and BY NAME partition forms keep refusing with the identical text. | The replay test's `partovw/*` cells. | PROVEN | 6 refusal cells and 1 store cell equal the oracle; 4 refusals (VALUES into TIMESTAMP/DATE/BOOLEAN, SELECT into TIMESTAMP) stored NULL on `57ebb57a`. |
| C-003 | Every Step 0 cell where base already matches Spark is unchanged: all STRING-source stores, `CAST(ts AS BIGINT)`, the temp-view shape (WI-1 text), the CTE-before shape, and the #885 and WI-1 suites. | The Step 0 matrix re-run on head (63 cells byte-identical); the in-test view and STRING pins; `test_store_ts_to_numeric_1.py` and `test_insert_store_assign.py` green. | PROVEN | Matrix diff confines every change to the 18 intended cells; view and STRING pins pass; 28 tests green across the three files. |

## Mutation record (2026-09-29)

Each mutation rebuilt the native module (`make develop`), ran the replay test,
and was reverted; the final tree rebuilds green.

| # | Mutation | Replay cells red | Other files |
|---|---|---|---|
| M-PE18 | The SELECT-arm call site is skipped (`refuse_select_arms` unreachable) | 15: every `vselect/*` refusal; all stores and `partovw/*` stay green | view and STRING pins green |
| M-PE24 | The partition-overwrite call site is skipped (the helper call removed) | 4: `partovw/values/negnull/{ts,date,bool}`, `partovw/select/negnull/ts`; listed/dynamic stay green through the earlier gate | view and STRING pins green |

**Neighbours (2026-09-29).** The 78-statement Step 0 matrix plus 16 follow-up/gap
statements answer byte-identically on `57ebb57a` and on this unit except the 18
intended cells (14 store→refuse, 4 refuse-retext, each now byte-identical to the
recorded Spark answer). The `unix_seconds`/`year`-over-VALUES refusals, the view
WI-1 text, the CTE-before `not implemented` text and the STRING stores are
unchanged pre-existing divergences, each owned elsewhere (function resolution,
R-STN-1, the CTE-before door, STORE-STRING-ASSIGN-1).

**Existing pins changed (0).** No existing test file is touched; `git status`
shows only the four product files, the new resolver module, the new replay test
plus oracle, this ledger, and three `map.md` files.

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: store-ts-doors-2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause is walked against recorded Spark answers — 21 must-change refusal cells across every shape and partition form, 5 store cells, and the 94-statement Step 0 matrix re-run confining every change to the intended cells.
      artifacts: [python/repark/tests/test_store_ts_doors_2.py, python/repark/tests/store_ts_doors_2_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Direct, star, CTE (both spellings), view, both join sides, UNION, nested aliases, WHERE, ORDER BY/LIMIT, VALUES/SELECT/listed/dynamic/BY NAME partition forms, and multi-row VALUES are exercised; views, table columns, literals, CASTs, functions and ambiguous refs fall through to base behaviour.
      artifacts: [crates/repark-spark/src/void_type/select_values_arms.rs, crates/repark-spark/src/void_type/insert_source_types.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal is a planning error raised before any file is staged; the replay test reads back the target after each refusal and matches Spark's unchanged rows, including the seeded rows and the empty overwritten partitions.
      artifacts: [python/repark/tests/test_store_ts_doors_2.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state; the resolver borrows the statement AST and the helper plans the statement's own source.
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling; the gates read table metadata and plan the statement's own source.
    - id: AT-6
      status: ATTACKED
      evidence: No stored-format change; the silent epoch-seconds and NULL writes are closed and the replay test asserts refused statements leave prior rows intact.
      artifacts: [python/repark/tests/test_store_ts_doors_2.py]
    - id: AT-7
      status: ATTACKED
      evidence: The SELECT-arm resolver runs only when the INSERT body is not a VALUES list and bails before any catalog load when no arm maps a VALUES cell; the partition helper loads the target schema and plans the filled source once per partition overwrite. No new per-row work.
      artifacts: [crates/repark-spark/src/void_type/ltz_values_store.rs, crates/repark-spark/src/void_type/insert_source_types.rs]
    - id: AT-8
      status: ATTACKED
      evidence: No new crate edge or dependency; the VALUES path keeps its exact behaviour through the extracted loader; insert_overwrite.rs holds at 998 lines with no ceiling change; views, BY NAME and the DataFrame doors are untouched.
      artifacts: [crates/repark-spark/src/void_type/ltz_values_store.rs, crates/repark-spark/src/insert_overwrite.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Every new refusal carries Spark's class, condition and SQLSTATE through the facade's stamped-message parse; the replay test compares all three with the recorded oracle.
      artifacts: [python/repark/tests/test_store_ts_doors_2.py]
    - id: AT-10
      status: ATTACKED
      evidence: Each new call site has a skip mutation that reds exactly its pins while the other family stays green; the resolver has classifier tests for every shape and every fall-through.
      artifacts: [crates/repark-spark/src/void_type/select_values_arms.rs, python/repark/tests/test_store_ts_doors_2.py]
  complete: true
```
