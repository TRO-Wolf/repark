# Unit ledger — G6-3 / G6-5 cast legality + WI-2 plain-INSERT store gate

**Unit:** G6-3 / G6-5 + WI-2 · **Date:** 2026-08-15 · **Lane:** repark ·
**Branch:** `fable/cast-integrity-wi2-g6` · **Base (FROZEN):** `dadfa01` (conductor-15 closeout, #144)

**Charter:** close the two remaining cast-integrity holes in one change, because they share the
analyzer region and their error classes have to be ordered against each other:

1. **G6-3 / G6-5** — `CAST`/`TRY_CAST` between `DATE` and integer widths is silently wrong
   (`CAST(DATE '2020-01-01' AS INT)` → `18262`; `CAST(18262 AS DATE)` → `2020-01-01`), where Spark
   refuses at analysis. Design: `planning/hardening/G63-DATE-INT-DESIGN.md`.
2. **WI-2** — the four plain-INSERT doors WI-1 (#142) could not reach, because DataFusion's
   `insert_to_plan` injects the conforming `CAST` at SQL-planning time.

---

## 0. What was measured, before and after

Same session shape as WI-1's ledger (memory catalog, `t(k INT, v INT)` fed from
`s(k INT, v DATE)`, values read back), on this base with a fresh `maturin develop`.

### 0.1 SELECT side (G6-3 / G6-5)

| Door | Spark 4.1.2 ANSI (design §1.2/§1.3 oracle) | repark @ base | repark @ this unit |
|---|---|---|---|
| `CAST(DATE '2020-01-01' AS INT)` | refuse `DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION` | `18262` int32 non-null | **refuses, same class** |
| `CAST(… AS BIGINT)` | refuse, same class | `18262` int64 non-null | **refuses** |
| `try_cast(DATE … AS INT)` | refuse, same class, `TRY_CAST(…)` in the text | `18262` int32 | **refuses, spells `TRY_CAST`** |
| `Column.cast("int")` on a DATE column | refuse | `18262` int32 | **refuses** |
| `Column.try_cast("int")` on a DATE column | refuse | `18262` int32 | **refuses** |
| `CAST(18262 AS DATE)` (G6-5) | refuse, remedy `DATE_FROM_UNIX_DATE` | `2020-01-01` non-null | **refuses** |
| `CAST(DATE … AS TINYINT/SMALLINT)` | refuse, same class | raised a DataFusion `NotImplemented` needle | **refuses with Spark's class** |

### 0.2 Write side (WI-2)

| # | Door | Spark 4.1.2 ANSI | repark @ base (WI-1 in) | repark @ this unit |
|---|---|---|---|---|
| 1 | `INSERT INTO t SELECT k, v FROM s` | refuse `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` | writes `18262` | **REFUSES** |
| 2 | `INSERT INTO t VALUES (2, DATE '…')` | refuse, same class | writes `18262` | **REFUSES** — but by the G6-3 CAST gate, with the CAST class (§3) |
| 3 | `INSERT OVERWRITE t SELECT …` | refuse, same class | refuses (WI-1) | refuses |
| 4 | `df.writeTo("t").append()` | refuse | writes `18262` | **REFUSES** |
| 5 | `df.write.mode("append").insertInto("t")` | refuse | writes `18262` | **REFUSES** |

Refusal text on doors 1/4/5, verbatim:

```
INSERT INTO cannot store-assign column `v`: source type Date32 is not ANSI-store-assignable to
target type Int32 (Spark INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST; add an explicit CAST
only if the reinterpretation is intended semantics)
```

---

## 1. Proposition ledger

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | The cast gate must run at ANALYSIS, not the optimizer: `datafusion-spark`'s `unix_date` — the remedy the refusal names — lowers to a textually identical `CAST(a AS Int32)` in `simplify_expressions`. | PROVEN — `the_remedy_the_error_names_still_works` is green with the gate installed |
| C-002 | `Expr::TryCast` needed its own arm: it was matched NOWHERE in `analyzer.rs` and fell to the catch-all, so `try_cast(DATE AS INT)` was silently wrong on two doors. | PROVEN — the arm is new; two corpus rows pin it |
| C-003 | The gate is NOT ANSI-gated. `Cast.checkInputDataTypes` is a check on the type PAIR; eval mode governs values a legal cast cannot represent. | PROVEN — `the_legality_gate_fires_in_both_ansi_modes` |
| C-004 | The deny matrix cannot be `ansi_store_assignable`: Spark's store-assignment matrix permits `Date32 → Timestamp` and refuses `Timestamp → Int64`, the cast matrix does the reverse; wiring them together would break TZ-5. | PROVEN by construction — two matrices, two crates, one shared error idiom; `a_timestamp_source_never_reaches_the_legality_gate` |
| C-005 | Timestamp sources are unreachable from the gate, so TZ-5 / B-TZ-4 / TZ-8 are untouched. | PROVEN — the gate keys on `Date32`/`Date64` sources (or integer sources with a date target); G6-4's `1577836800` still pinned |
| C-006 | Every OTHER pair in the recorded G6 corpus is byte-identical. | PROVEN — `test_cast_failure_parity.py` 22/22; the nine untouched rows never re-recorded |
| C-007 | Non-integer DATE targets (`DOUBLE`/`BOOLEAN`/`DECIMAL`) keep today's DataFusion needle — excluded on purpose so `F.col("d") / 2` is not told it "wrote a CAST". | PROVEN — `date_to_non_integer_targets_keep_the_datafusion_needle` |
| C-008 | WI-2 judges ONLY synthesized conform casts. A user-written explicit `CAST` is legal Spark and must pass. | PROVEN — `an_explicit_user_cast_in_the_select_is_not_gated` (Rust) + `test_an_explicit_user_cast_still_writes` (facade) |
| C-009 | WI-2 imports the matrix; it never duplicates it. | PROVEN — `insert_gate.rs` has one `use super::store_assign::refuse_unless_write_store_assignable` and no type table of its own |
| C-010 | The write gate must be registered BEFORE `SparkExprSemantics` so a `DATE → INT` insert cites the WRITE class, which is what Spark raises for that statement. | PROVEN — measured message on door 1 is `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` |
| C-011 | The live tier moves in the same diff or the mirror gate reds in BOTH directions. | PROVEN — `_live_parity.py`, `test_parity_live.py`'s exact set and the registry §6 heading are all in this diff; `test_disclosures_mirror_the_registry` green |
| C-012 | No ceiling was raised except the G6 corpus budget, which the design requires be an explicit choice. | PROVEN — `check_rust_file_size` / `check_lib_rs` / `check_lib_py` untouched; `G6_BUDGET_MAX` 10 → 15 with its reason on the constant |
| C-013 | No expected-failure markers anywhere; every new pin asserts real behaviour. | PROVEN — the diff carries no such marker in any test file |

---

## 2. Implementation

**`crates/repark-functions/src/analyzer/cast_legality.rs` (new).** The deny matrix
(`{Date32, Date64} ↔ {Int8, Int16, Int32, Int64}`) and `refuse_spark_illegal_cast`, which emits
Spark's text verbatim: the bracketed class, `Cannot resolve "CAST(<expr> AS INT)"` (or
`TRY_CAST(…)`), both Spark type names, the named remedy (`UNIX_DATE` / `DATE_FROM_UNIX_DATE`) and
`SQLSTATE: 42K09`. A `DataFusionError::Plan` carrying a bracketed class folds to
`AnalysisException` at the PyO3 boundary — the `window_range.rs` precedent. It lives in a
file-backed submodule because the crate root is AT its `check_lib_rs` ceiling, so a top-level
`mod` decl was not available; `analyzer.rs` stays 1394/1500.

**`crates/repark-functions/src/analyzer.rs`.** `rewrite_timestamp_casts` returns
`Result<Transformed<Expr>>` now and calls the refusal at its head; a new `Expr::TryCast` arm calls
the same refusal and otherwise declines (the three timestamp rewrites stay `Expr::Cast`-only —
widening them to `TryCast` would be an unmeasured behaviour change).

**`crates/repark-iceberg/src/write/insert_gate.rs` (new).** `InsertStoreAssignment`, an
`AnalyzerRule` walking for `LogicalPlan::Dml(WriteOp::Insert(_))`. It reads the synthesized
`Projection`'s expressions and judges exactly `Alias(Cast(Column(c), target))`, taking the source
type off the projection's INPUT schema and handing it to `store_assign`'s
`refuse_unless_write_store_assignable`. The path label follows `InsertOp` (`INSERT INTO` /
`INSERT OVERWRITE` / `REPLACE INTO`).

**`crates/repark-spark/src/extension.rs`.** One `add_analyzer_rule` before the
`repark_functions::analyzer_rules()` loop (see C-010).

**`crates/repark-functions/src/expr_fn.rs` + `crates/repark-python/src/column/mod.rs` +
`python/repark/src/repark/spark/functions_datetime.py` — the one rider the design's §3.4 survey
missed.** `F.unix_date` was implemented in the Python facade as `.cast("date").cast("int")` — the
exact pair the gate refuses. §3.4 surveyed the RUST-side `Expr::Cast` producers and found no
`Date32` source; it did not survey the facade's own function bodies. The fix is not an exception in
the matrix: the facade now builds the ENGINE's `unix_date` (`datafusion-spark`'s `SparkUnixDate`,
new `expr_fn::unix_date` + a `call_scalar` arm), whose own `simplify` re-creates the same cast in
the optimizer, one stage after the gate — which is exactly the property §3.4 relies on. The remedy
Spark's message names now goes through the engine's implementation of that remedy.

**`python/repark/tests/test_a3_cast_vocab.py`.** The DATE row of the cast-vocabulary lockstep
sourced from `spark.range(1)`'s `id` (`BIGINT`), which Spark refuses to cast to `DATE` (G6-5). The
vocabulary claim is unchanged by which legal source column the cast starts from, so the DATE row
sources from a string column; the reason is on the constant.

---

## 3. What is NOT closed, named

**`INSERT INTO … VALUES` with a cast-legal-but-not-store-assignable literal.**
`insert_to_plan` hands the target schema to the VALUES planner, and
`LogicalPlanBuilder::infer_inner` rewrites each literal as `row[j].cast_to(field_type, schema)`
**inside the `Values` node**. A user-written `INSERT INTO t VALUES (CAST(x AS INT))` produces the
byte-identical node, because the outer conform is a no-op once the inner cast already yielded the
target type. The two are indistinguishable in the plan, and refusing a legal explicit cast is worse
than the gap — so the rule judges only `Cast(Column, …)`, the shape whose pre-cast type is visible.

The half of that residual that carried a silently-wrong VALUE is closed anyway, because the G6-3
gate refuses `DATE ↔ INT` wherever the cast appears, `Values` node included — with the CAST class
rather than the WRITE class, which `test_the_named_residual_is_a_literal_values_row` records rather
than hides. What is genuinely open is `INSERT INTO t(v INT) VALUES (true)` and its
`timestamp`/`string` siblings: they write a defined, non-reinterpreted value where Spark refuses.
A policy gap, not a corruption.

**Doors outside the Spark extension.** `InsertStoreAssignment` is registered by
`SparkExtension::register`, so it covers the Spark door and every facade lane that installs it
(which is all of them — `repark-python` installs the Spark door in its constructor). A bare
`repark-core` session with no extension, and the native ANSI door (`repark-sql`), do not get the
rule. That is deliberate rather than incidental: this is a **Spark-parity** policy, and the ANSI
door is a different dialect surface with its own semantics. Moving it to `repark-core`'s session
defaults (the G8 precedent for the uncorrelated-scalar-subquery guard) is the change to make if the
policy is ever declared door-neutral.

**`Column.cast` / `try_cast` from the DataFrame door remain gated by the same rule** — nothing
door-specific is needed there, because both build `Expr::Cast`/`Expr::TryCast` and converge on the
analyzer.

---

## 4. Files touched

- `crates/repark-functions/src/analyzer/cast_legality.rs` (new), `crates/repark-functions/src/analyzer/map.md` (new)
- `crates/repark-functions/src/analyzer.rs`, `crates/repark-functions/src/expr_fn.rs`, `crates/repark-functions/src/map.md`
- `crates/repark-iceberg/src/write/insert_gate.rs` (new)
- `crates/repark-iceberg/src/write/mod.rs`, `crates/repark-iceberg/src/write/map.md`
- `crates/repark-iceberg/src/lib.rs`, `crates/repark-iceberg/src/map.md`
- `crates/repark-spark/src/extension.rs`, `crates/repark-spark/src/map.md`
- `crates/repark-python/src/column/mod.rs`, `crates/repark-python/src/column/map.md`
- `python/repark/src/repark/spark/functions_datetime.py`, `python/repark/src/repark/spark/map.md`
- `python/repark/tests/test_cast_failure_parity.py`, `test_insert_store_assign.py`,
  `test_a3_cast_vocab.py`, `_live_parity.py`, `test_parity_live.py`, `python/repark/tests/map.md`
- `docs/spark-sql-iceberg-parity.md` (G6-3 CLOSED, new G6-5 CLOSED, BL-1 rewritten)
- `task/map.md`, `task/wi2-g6-cast-integrity-ledger.md` (this file)

`crates/repark-iceberg/src/write/store_assign.rs` is **not** in the list: the matrix was imported,
not edited (C-009).

---

## 5. Gate roster

| Gate | Result |
|---|---|
| `make verify` (`ci` + workspace `cargo test`) | green |
| `make preflight` (`verify` + `py-test-facade` + `audit` + `workflows-lint`) | green |
| facade suite, fresh `maturin develop` | 3271 passed, 70 skipped, 0 failed |
| `cargo test -p repark-functions` | 169 passed (5 new in `cast_legality`, 8 new in `analyzer::tests`) |
| `cargo test -p repark-iceberg` | 349 passed (7 new in `insert_gate`) |
| `test_cast_failure_parity.py` | 22 passed (15 rows, budget 8–15) |
| `test_insert_store_assign.py` | 24 passed (9 new WI-2 pins) |
| `test_merge_store_assign.py` | untouched, still green |
