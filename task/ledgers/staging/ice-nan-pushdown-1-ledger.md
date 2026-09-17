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
| C-001 | `d = NaN` (either literal side, CAST spelling) answers the NaN-row ids, double and float, both doors, every shape, v2 and v3, RePark- and Spark-written | `test_ice_nan_pushdown_1.py` eq legs + Rust `is_nan` plan pin | OPEN |
| C-002 | `d <=> NaN` answers the NaN-row ids, double and float, both doors | `test_ice_nan_pushdown_1.py` eq-null-safe legs | OPEN |
| C-003 | `d != NaN` answers the non-NaN non-null ids, double and float, both doors | `test_ice_nan_pushdown_1.py` neq legs | OPEN |
| C-004 | `<`, `<=`, `>`, `>=` with a NaN literal answer the oracle sets (ranges stay unpushed), double and float, both doors | `test_ice_nan_pushdown_1.py` range legs | OPEN |
| C-005 | `IN (NaN)` answers the NaN-row ids; `IN (NaN, 1.0)` adds the 1.0 rows, double and float, both doors | `test_ice_nan_pushdown_1.py` in legs | OPEN |
| C-006 | `NOT IN (NaN)` answers the non-NaN non-null ids (stays unpushed), double and float, both doors | `test_ice_nan_pushdown_1.py` not-in legs | OPEN |
| C-007 | `BETWEEN 1.0 AND NaN` and `NOT (d = NaN)` answer the oracle sets, double, both doors | `test_ice_nan_pushdown_1.py` between/negation legs | OPEN |
| C-008 | `isnan(d)` control answers the NaN-row ids on every shape the unit reads | `test_ice_nan_pushdown_1.py` control legs | OPEN |
| C-009 | Shape matrix: NaN-only file, mixed single file, two files; v2 and v3; RePark-written tables built in-test plus checked-in Spark-written warehouses | `test_ice_nan_pushdown_1.py` matrix + fixture warehouses | OPEN |
| C-010 | DataFrame door: `filter(col("d") == float("nan"))`, `col("d").isin(float("nan"))`, `F.expr(...)` match the SQL door per clause | `test_ice_nan_pushdown_1.py` frame legs | OPEN |
| C-011 | `DELETE FROM t WHERE d = CAST('NaN' AS DOUBLE)` removes exactly the NaN rows; `UPDATE … WHERE d = NaN` touches exactly the NaN rows | `test_ice_nan_pushdown_1.py` DML legs | OPEN |
| C-012 | Spark oracle recorded as fixture: recorder script plus truth JSON plus Spark-written v2/v3 warehouses; live tier replays Spark under `REPARK_PARITY_LIVE=1` | recorder script, truth JSON, live legs | OPEN |
| C-013 | Registry row ICE-NAN-PUSHDOWN-1 FIXED 2026-09-17 by fork #284 at pin `75da2b58`, quoting the rating's silent answer; tests map and ledgers map in lockstep | registry row, map edits | OPEN |

VERDICT (2026-09-17): 13 clauses, 0 PROVEN, 13 OPEN, 0 REJECTED.

## 6. Evidence

### Rating rows (prior behaviour, quoted)

Rating report §8 #48/#50 (RePark main before fork #284): `WHERE d = CAST('NaN' AS DOUBLE)` and `WHERE d IN (CAST('NaN' AS DOUBLE))` return `[]` on Iceberg scans where Spark 4.1.2 returns the NaN rows (v2 and v3, RePark- and Spark-written tables, every file layout).

### Fork red-first (unfixed tree, quoted from the fork ledger)

`cargo test -p iceberg-datafusion --test nan_pushdown` on the unfixed tree: `WHERE d = CAST('NaN' AS DOUBLE)` answers `[]`, expected `[1, 5]`.

### Probe (this unit, release native at the new pin)

(to fill: `/tmp/ib-scratch/probes/p_nan_eq.py` output)

### Gates (to fill with counts)

(to fill: new-file offline and live, `make verify`, whole facade suite, whole parity suite)

## Coverage attestation

(to fill at close: every PROVEN clause names the pin that discharges it; the grammar gate holds the citation direction.)

## Open questions

None. Every decision the brief left open is settled above; anything else halts with a hand-back question.
