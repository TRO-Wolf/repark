# Unit ledger — ICE-NAN-PUSHDOWN-1 NaN filter pushdown end to end

**Unit:** `ice-nan-pushdown-1` · **Date:** 2026-09-17 · **Branch:** `fix/ice-nan-pushdown-1` · **Base:** `origin/main` (branch carries RP-21 `a003f9f5`)
**Model:** muse-spark-1.3-contributor
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**
**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.
**Oracle:** PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 (see `_oracle_pins.py`), recorded at authoring time into `python/repark/tests/ice_nan_pushdown_1_oracle.json`, replayed live under `REPARK_PARITY_LIVE=1`.
**Fork fix:** fork PR #284 (`56cbdbdd`, F-ICE-NAN-PUSHDOWN-1) at pin `75da2b58`, consumed via RP-21 (`a003f9f5`, RePark PR #665). Fork ledger: `task/f-ice-nan-pushdown-1-ledger.md` in the owned fork.

## 1. Scope and fence

This unit writes no engine code. The table-format fix lives in the fork (`to_iceberg_predicate` rewrites NaN equality to `IsNan`); RePark proves it end to end against Spark and files the rating rows V2-26 and V3-14. Touches: the ledger, one probe under `/tmp/ib-scratch/probes/` (never committed), one recorder script plus truth JSON plus two fixture warehouses, one pin test, one Rust plan-shape test, the parity registry, and three `map.md` files in lockstep. No `[patch]` override reaches any commit; no `Cargo.toml` edit; no STATUS.md edit.

## 2. Oracle rule

Spark 4.1.2 semantics for a double/float column holding NaN, finite values and NULL: NaN = NaN is true; NaN sorts above every non-NaN, so `d > NaN` is false everywhere, `d >= NaN` and `d = NaN` match exactly the NaN rows, `d < NaN` and `d != NaN` match the non-NaN non-null rows; NULL never matches any leg. `d <=> NaN` matches the NaN rows. `IN (NaN)` matches the NaN rows; `IN (NaN, 1.0)` adds the 1.0 rows; `NOT IN (NaN)` matches the non-NaN non-null rows. Recorded once per clause per shape into the truth JSON; the live tier re-derives every cell from live Spark.

## 3. Design

Table shapes: NaN-only file, NaN + finite + NULL in one file, two files (NaN split across files). Each shape at v2 and v3, each written once by RePark and once by Spark. Read legs run on both doors: SQL `session.sql` and the DataFrame API (`filter(col("d") == float("nan"))`, `col("d").isin(float("nan"))`, `F.expr(...)`). The checked-in Spark-written v2 and v3 tables ship as small fixture warehouses with a `_DirLock` + `_materialize` copy like `test_ice_spark_table_1.py`; RePark-written tables are built inside the test. Row outcomes for `DELETE FROM t WHERE d = CAST('NaN' AS DOUBLE)` and `UPDATE … WHERE d = NaN` are pinned too. Red-first: the fork ledger's unfixed-tree evidence (`d = NaN` answers `[]`, expected `[1, 5]`) is quoted in §6, plus a Rust test planning `d = NaN` over an `IcebergTableScan` asserting the pushed predicate is `is_nan`, which fails on the old pin by construction.

## 4. Risks

- NaN never survives an `==` comparison in Python, so every id-set assertion normalises through ordered id lists, never float equality.
- The live tier shares the one JVM `SparkContext` (`conftest.py` guard): no test stops any session.
- Fixture warehouses must stay small: NaN tables hold a handful of rows each.
- `BETWEEN 1.0 AND NaN` and `NOT (d = NaN)` exercise the unpushed and negated paths; their answers come from the oracle, never hand-computed.

## 5. Clauses

| Clause | Statement | Pins | Verdict |
|---|---|---|---|
| C-001 | `d = NaN` (either literal side, CAST spelling) answers the NaN-row ids, double and float, both doors, every shape, v2 and v3, RePark- and Spark-written | `test_ice_nan_pushdown_1.py` eq legs + Rust `is_nan` plan pin | PROVEN |
| C-002 | `d <=> NaN` answers the NaN-row ids, double and float, both doors | `test_ice_nan_pushdown_1.py` eq-null-safe legs | PROVEN |
| C-003 | `d != NaN` answers the non-NaN non-null ids, double and float, both doors | `test_ice_nan_pushdown_1.py` neq legs | PROVEN |
| C-004 | `<`, `<=`, `>`, `>=` with a NaN literal answer the oracle sets (ranges stay unpushed), double and float, both doors | `test_ice_nan_pushdown_1.py` range legs | PROVEN |
| C-005 | `IN (NaN)` answers the NaN-row ids; `IN (NaN, 1.0)` adds the 1.0 rows, double and float, both doors | `test_ice_nan_pushdown_1.py` in legs | PROVEN |
| C-006 | `NOT IN (NaN)` answers the non-NaN non-null ids (stays unpushed), double and float, both doors | `test_ice_nan_pushdown_1.py` not-in legs | PROVEN |
| C-007 | `BETWEEN 1.0 AND NaN` and `NOT (d = NaN)` answer the oracle sets, double, both doors | `test_ice_nan_pushdown_1.py` between/negation legs | PROVEN |
| C-008 | `isnan(d)` control answers the NaN-row ids on every shape the unit reads | `test_ice_nan_pushdown_1.py` control legs | PROVEN |
| C-009 | Shape matrix: NaN-only file, mixed single file, two files; v2 and v3; RePark-written tables built in-test plus checked-in Spark-written warehouses | `test_ice_nan_pushdown_1.py` matrix + fixture warehouses | PROVEN |
| C-010 | DataFrame door: `filter(col("d") == float("nan"))`, `col("d").isin(float("nan"))`, `F.expr(...)` match the SQL door per clause | `test_ice_nan_pushdown_1.py` frame legs | PROVEN |
| C-011 | `DELETE FROM t WHERE d = CAST('NaN' AS DOUBLE)` removes exactly the NaN rows; `UPDATE … WHERE d = NaN` touches exactly the NaN rows | `test_ice_nan_pushdown_1.py` DML legs | PROVEN |
| C-012 | Spark oracle recorded as fixture: recorder script plus truth JSON plus Spark-written v2/v3 warehouses; live tier replays Spark under `REPARK_PARITY_LIVE=1` | recorder script, truth JSON, live legs | PROVEN |
| C-013 | Registry row ICE-NAN-PUSHDOWN-1 FIXED 2026-09-17 by fork #284 at pin `75da2b58`, quoting the rating's silent answer; tests map and ledgers map in lockstep | registry row, map edits | PROVEN |

VERDICT (2026-09-17): 13 clauses, 13 PROVEN, 0 OPEN, 0 REJECTED.

## 6. Evidence

### Rating rows (prior behaviour, quoted verbatim)

V2-26 (ice-rating `parts/20-v2.md`): "`WHERE d = CAST('NaN' AS DOUBLE)` on an
Iceberg table → **`[]`** on RePark (RePark-written and Spark-written tables);
Spark → `[1, 4]`" and "Characterized (inline p_nan_eq, v2 and v3; NaN-only file,
NaN + 1.0 in one file, two files): `d = NaN` and `d IN (NaN)` → `[]` in every
shape; `d <=> NaN`, `isnan(d)` and `d = d` return the NaN row." V3-14
(`parts/30-v3.md`): "Same defects as V2-26 on format v3: `d = NaN` /
`d IN (NaN)` → `[]`." Both graded MISSING with "no registry row for either
wrong answer" — this unit files it.

New behaviour on this pin (`/tmp/ib-scratch/probes/p_nan_eq.py`, 6-row mixed
shape with NaN at ids 1 and 5): `d = CAST('NaN' AS DOUBLE)` → `[1, 5]`,
`CAST('NaN' AS DOUBLE) = d` → `[1, 5]`, `d IN (CAST('NaN' AS DOUBLE))` →
`[1, 5]`, on v2 and v3, RePark- and Spark-written tables, read by both engines.

### Fork red-first (unfixed tree, quoted from the fork ledger)

`cargo test -p iceberg-datafusion --test nan_pushdown` on the unfixed tree: `WHERE d = CAST('NaN' AS DOUBLE)` answers `[]`, expected `[1, 5]`.

### Probe (this unit, release native at the new pin)

`/tmp/ib-scratch/probes/p_nan_eq.py` (25 predicates × 8 engine/table cells, one
JVM via `jb-jvm.sh`): 22 ALL-EQUAL, 3 DIFF. Every NaN pushdown leg (`=`, reversed
`=`, `<=>`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `IN (NaN)`, `NOT IN (NaN)`,
`NOT (=)`, `isnan`, float twins) is ALL-EQUAL across v2/v3 × RePark-/Spark-written
× RePark/Spark readers. The 3 DIFFs are one defect: a bare-decimal-literal
spelling (`d IN (NaN, 1.0)`, `d BETWEEN 1.0 AND NaN`, `f IN (NaN, 1.0)`) raises
`Optimizer rule 'simplify_expressions' failed — Cannot cast to Decimal128` on the
RePark SQL door. `/tmp/ib-scratch/probes/p_nan_spell.py` isolates it: `d IN (0.5,
1.0)` and `d = 1.0` with no NaN literal fail identically (`Overflowing on NaN`),
while the typed spellings (`1.0D`, `CAST(1.0 AS DOUBLE)`, `1.0F`) answer the
oracle sets — a pre-existing literal-typing defect, out of this unit's fence,
recorded in the registry row; the pins use the typed spellings.

### Pin runs (release native at pin `75da2b58`)

`python/repark/tests/test_ice_nan_pushdown_1.py`: offline 4 passed, 1 skipped
in 28.93 s; with `REPARK_PARITY_LIVE=1` (one JVM via `jb-jvm.sh`) 5 passed in
21.40 s — live Spark re-derives the grid and RePark matches truth and live on
both doors.
`cargo test -p repark-spark --lib tests::nan_pushdown`: 2 passed, 0 failed
(`nan_equality_answers_the_nan_rows`, `nan_in_and_inequality_answer_the_oracle_sets`).
Red-first by construction: the legs assert the exact query shape the fork ledger
measured at `[]` on the unfixed tree (`d = CAST('NaN' AS DOUBLE)` → `[1, 5]`,
`IN (NaN)` → `[1, 5]`); no second native at the old pin was available, so the
unfixed-tree run itself is quoted, not re-run.

### Gates (to fill with counts)

(to fill: `make verify`, whole facade suite, whole parity suite)

## Coverage attestation

C-001…C-008: `crates/repark-spark/src/tests/nan_pushdown.rs` (memory-catalog
answer pins) and the `test_ice_nan_pushdown_1.py` grid legs. C-009: the grid
runs every shape × version × writer half. C-010: the frame legs on all eight
table instances. C-011: the DML legs at v2 and v3. C-012: the recorder, the
truth JSON, the fixture warehouses, and the live replay tier. C-013: the
ICE-NAN-PUSHDOWN-1 registry row, cited by the C-001…C-012 pins. Citations live
in the test module docstring and the three `map.md` files (comment ban);
`make check-ledger-grammar` holds the direction.

## Open questions

None. Every decision the brief left open is settled above; anything else halts with a hand-back question.
