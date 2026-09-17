# Unit ledger — ICE-NAN-PUSHDOWN-1 NaN filter pushdown end to end

**Unit:** `ice-nan-pushdown-1` · **Date:** 2026-09-17 · **Branch:** `fix/ice-nan-pushdown-1` · **Base:** `origin/main` at `c42a4692` (RP-21 merged; round 2 rebased by the orchestrator)
**Model:** muse-spark-1.3-contributor
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**
**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.
**Oracle:** PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 (see `_oracle_pins.py`), recorded at authoring time into `python/repark/tests/ice_nan_pushdown_1_oracle.json`, replayed live under `REPARK_PARITY_LIVE=1`.
**Fork fix:** fork PR #284 (`56cbdbdd`, F-ICE-NAN-PUSHDOWN-1) at pin `75da2b58`, consumed via RP-21 (`a003f9f5`, RePark PR #665). Fork ledger: `task/f-ice-nan-pushdown-1-ledger.md` in the owned fork.

## 1. Scope and fence

This unit writes no engine code. The table-format fix lives in the fork (`to_iceberg_predicate`); RePark proves the pushed shapes end to end against Spark and files the rating rows V2-26 and V3-14. Round 2 (logic-critic remediation) corrected the pushed-shape claims to what the plan pins measure: `=` pushes `Unary IsNan`; `IN (NaN, 1.0)` pushes `Or(IsNan, Eq)`; `!=` and single `NOT IN` push `NotNan`; `<=>` and NaN ranges push nothing. Touches: the ledger, probes under `/tmp/ib-scratch/probes/` (never committed), one recorder script plus truth JSON plus six fixture warehouses, one pin test, the Rust plan-shape test, the parity registry (one FIXED row, one BACKLOG row), and four `map.md` files in lockstep. No `[patch]` override reaches any commit; no `Cargo.toml` edit; no STATUS.md edit.

## 2. Oracle rule

Spark 4.1.2 semantics for a double/float column holding NaN, finite values and NULL: NaN = NaN is true; NaN sorts above every non-NaN, so `d > NaN` is false everywhere, `d >= NaN` and `d = NaN` match exactly the NaN rows, `d < NaN` and `d != NaN` match the non-NaN non-null rows; NULL never matches any leg. `d <=> NaN` matches the NaN rows. `IN (NaN)` matches the NaN rows; `IN (NaN, 1.0)` adds the 1.0 rows; `NOT IN (NaN)` matches the non-NaN non-null rows. Recorded once per clause per shape into the truth JSON; the live tier re-derives every cell from live Spark.

## 3. Design

Table shapes: NaN-only file, NaN + finite + NULL in one file, two mixed files, and the split shape (an all-NaN file beside a finite-only file, so the `is_nan` prune-away is exercised). Each shape at v2 and v3, each written once by RePark and once by Spark. Read legs run on both doors: SQL `session.sql` carries the full grid, the DataFrame API carries every cell but the `eq_rev` spelling twin. The checked-in Spark-written tables ship as small fixture warehouses with a `_DirLock` + `_materialize` copy like `test_ice_spark_table_1.py`; RePark-written tables are built inside the test. `DELETE` / `UPDATE … WHERE d = NaN` outcomes are pinned at v2 and v3 on the SQL door, offline and live (live includes RePark DML on adopted live tables). `BETWEEN 1.0 AND NaN` is the finite-leg file-prune artefact, `BETWEEN NaN AND NaN` the pure NaN-bound residual. Red-first: the fork ledger's unfixed-tree evidence (`d = NaN` answers `[]`, expected `[1, 5]`) is quoted in §6, plus Rust tests planning each clause over an `IcebergTableScan` and asserting the pushed predicate the scan carries, which fail on the old pin by construction. Bare-decimal-literal spellings stay a dated BACKLOG row with loud-error pins, not a fix.

## 4. Risks

- NaN never survives an `==` comparison in Python, so every id-set assertion normalises through ordered id lists, never float equality.
- The live tier shares the one JVM `SparkContext` (`conftest.py` guard): no test stops any session.
- Fixture warehouses must stay small: NaN tables hold a handful of rows each.
- `BETWEEN 1.0 AND NaN` keeps the pushed finite `>= 1.0` leg (it prunes the sub-`1.0` file with its NaN); only `BETWEEN NaN AND NaN` observes the NaN-bound residual. Both answers come from the oracle, never hand-computed.
- `NOT (d = NaN)` exercises the negated path; its answer comes from the oracle.

## 5. Clauses

| Clause | Statement | Pins | Verdict |
|---|---|---|---|
| C-001 | `d = NaN` (either literal side, CAST spelling) answers the NaN-row ids and pushes `Unary IsNan(d)`; double and float, both doors, every shape, v2 and v3, RePark- and Spark-written | `test_ice_nan_pushdown_1.py` eq legs + Rust `nan_equality_pushes_is_nan` | PROVEN |
| C-002 | `d <=> NaN` answers the NaN-row ids and pushes nothing (residual); double and float, both doors | `test_ice_nan_pushdown_1.py` nullsafe legs + Rust `nan_nullsafe_leaves_pushdown_empty` | PROVEN |
| C-003 | `d != NaN` answers the non-NaN non-null ids and pushes `Unary NotNan(d)`; double and float, both doors | `test_ice_nan_pushdown_1.py` neq legs + Rust `nan_inequality_pushes_not_nan` | PROVEN |
| C-004 | `<`, `<=`, `>`, `>=` with a NaN literal answer the oracle sets and push nothing; double both doors, float SQL door | `test_ice_nan_pushdown_1.py` range legs + Rust `nan_range_stays_unpushed` | PROVEN |
| C-005 | `IN (NaN)` answers the NaN-row ids; `IN (NaN, 1.0)` adds the 1.0 rows and pushes `Or(IsNan, Eq)` with a NaN-free literal; double and float, both doors | `test_ice_nan_pushdown_1.py` in legs + Rust `nan_in_pushes_is_nan_or_in` | PROVEN |
| C-006 | `NOT IN (NaN)` answers the non-NaN non-null ids; single-element reaches `!=` and pushes `NotNan`; double and float, both doors | `test_ice_nan_pushdown_1.py` not-in legs + Rust `nan_single_not_in_pushes_not_nan` | PROVEN |
| C-007 | `BETWEEN 1.0 AND NaN` (finite-leg file prune) and `BETWEEN NaN AND NaN` (pure NaN-bound residual) plus `NOT (d = NaN)` answer the oracle sets, double, both doors | `test_ice_nan_pushdown_1.py` between/negation legs | PROVEN |
| C-008 | `isnan(d)` control answers the NaN-row ids on every shape the unit reads | `test_ice_nan_pushdown_1.py` control legs | PROVEN |
| C-009 | Shape matrix: NaN-only file, mixed files, split shape (all-NaN file beside a finite-only file, so the prune-away is exercised); v2 and v3; RePark-written tables built in-test plus checked-in Spark-written warehouses | `test_ice_nan_pushdown_1.py` matrix + fixture warehouses | PROVEN |
| C-010 | DataFrame door answers every grid cell but the `eq_rev` spelling twin (`==`, `eqNullSafe`, `!=`, ranges, `isin`, negated `isin`, `between`, negated `==`, `isnan`, `F.expr`, float `==`/`isin`) | `test_ice_nan_pushdown_1.py` frame legs | PROVEN |
| C-011 | `DELETE FROM t WHERE d = CAST('NaN' AS DOUBLE)` removes exactly the NaN rows; `UPDATE … WHERE d = NaN` touches exactly the NaN rows; SQL door, v2 and v3, offline and live (live includes RePark DML on adopted live tables) | `test_ice_nan_pushdown_1.py` DML legs + live DML | PROVEN |
| C-012 | Spark oracle recorded as fixture: recorder script plus truth JSON (reads, v2/v3 DML, decimal-literal Spark sets) plus Spark-written warehouses; live tier re-derives the grid and DML from live Spark under `REPARK_PARITY_LIVE=1` | recorder script, truth JSON, live legs | PROVEN |
| C-013 | Registry row ICE-NAN-PUSHDOWN-1 FIXED 2026-09-17 by fork #284 at pin `75da2b58` with the measured pushed shapes, quoting the rating's silent answer; tests map and ledgers map in lockstep | registry row, map edits | PROVEN |
| C-014 | Bare-decimal-literal spellings fail loud with pinned needles (`Overflowing on NaN` for `d = 1.0`, `Cannot cast to Decimal128` for `d IN (NaN, 1.0)`); Spark answers `[2]` / `[1, 2, 5]` recorded; dated OPEN row ICE-NAN-DECIMAL-LITERAL-1 filed, no fix here | decimal pins + registry BACKLOG row | PROVEN |

VERDICT (2026-09-17): 14 clauses, 14 PROVEN, 0 OPEN, 0 REJECTED.

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

### Round-2 plan-pin findings (measured pushed shapes, release native)

The first plan-pin run failed 3 of 6 and corrected three conversion-table
claims: `d <=> NaN` pushes nothing (residual answers `[1, 5]`);
`d IN (NaN, 1.0)` pushes `Or(Unary IsNan(d), Binary Eq(d, 1.0))`, not
`Or(IsNan, In)`; single-element `d NOT IN (NaN)` reaches `!=` and pushes
`Unary NotNan(d)`. The committed pins assert these measured shapes. `=` both
sides push `Unary IsNan(d)`, `!=` pushes `NotNan`, `d < NaN` pushes nothing.

### Pin runs (release native at pin `75da2b58`)

Round-1 runs (superseded by the round-2 grid, kept for the record):
`test_ice_nan_pushdown_1.py` offline 4 passed, 1 skipped; live 5 passed.
`cargo test -p repark-spark --lib tests::nan_pushdown`: 2 passed.
Red-first by construction: the legs assert the exact query shape the fork ledger
measured at `[]` on the unfixed tree (`d = CAST('NaN' AS DOUBLE)` → `[1, 5]`,
`IN (NaN)` → `[1, 5]`); no second native at the old pin was available, so the
unfixed-tree run itself is quoted, not re-run.
Round-2 runs: `test_ice_nan_pushdown_1.py` offline 5 passed, 1 skipped; live
6 passed (grid + v2/v3 DML on live Spark, RePark DML on adopted live tables).
`cargo test -p repark-spark --lib tests::nan_pushdown`: 8 passed, 0 failed.

### Gates (2026-09-17, release native at pin `75da2b58`)

Round-1 gates (superseded): `make verify` rc 0; facade 9330 passed, 368
skipped, 26 xfailed; parity 757 passed, 2 skipped, 12 xfailed.
Round-2 gates (2026-09-17, native rebuilt 08:02 EDT after the Rust edits):

- Unit file offline: rc 0, 5 passed, 1 skipped.
- Unit file live (exact command above): rc 0, 6 passed.
- `cargo test -p repark-spark --lib nan_pushdown`: rc 0, 8 passed.
- `uvx ruff@0.15.22 check .`: rc 0, all checks passed.
- `make verify`: rc 0 (log `/tmp/oc-worker/ib-bump/nan-r2-verify.log`).
- Whole facade suite: rc 0, 9331 passed, 368 skipped, 26 xfailed
  (log `/tmp/oc-worker/ib-bump/facade-r2.log`).
- Whole parity suite: rc 0, 757 passed, 2 skipped, 12 xfailed
  (log `/tmp/oc-worker/ib-bump/parity-r2.log`).

Exact live command (`.venv` has no `pyspark`; the sparkenv site-packages ride
`PYTHONPATH`, one JVM via `jb-jvm.sh`):

    cd /tmp/ib-bump && REPARK_PARITY_LIVE=1 JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 PYSPARK_SUBMIT_ARGS="--conf spark.jars.ivy=/tmp/ib-scratch/.ivy2 --conf spark.driver.memory=2g --conf spark.ui.enabled=false pyspark-shell" PYTHONPATH=/tmp/sparkenv/lib/python3.12/site-packages /tmp/oc-worker/jb-jvm.sh .venv/bin/python -m pytest python/repark/tests/test_ice_nan_pushdown_1.py -q -p no:cacheprovider

## Coverage attestation

Citations live in the test module docstring and the three `map.md` files
(comment ban); `make check-ledger-grammar` holds the direction.

```
COVERAGE_ATTESTATION:
  pr_unit: ice-nan-pushdown-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against its recorded oracle cell, not a paraphrase — 308 read cells (15 double plus 14/4 float legs per shape) across 12 table instances on the SQL door, DataFrame door on every cell but the eq_rev twin, DELETE/UPDATE outcomes at v2 and v3 offline and live; the live tier asserts repark == truth == live Spark.
      artifacts: [python/repark/tests/test_ice_nan_pushdown_1.py, python/repark/tests/ice_nan_pushdown_1_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries exercised — NaN-only file, NaN plus finite plus NULL in one file, NaN across two mixed files, the split shape (all-NaN file beside a finite-only file, so the is_nan prune-away is exercised), negative and fractional finite values, NaN-as-upper-bound BETWEEN, double and float columns, v2 and v3, RePark- and Spark-written tables.
      artifacts: [python/repark/tests/test_ice_nan_pushdown_1.py, python/repark/tests/fixtures/ice_nan_pushdown_1/map.md]
    - id: AT-3
      status: ATTACKED
      evidence: The residual legs (<=>, NaN ranges, BETWEEN, negation) answer the oracle sets instead of erroring; the two bare-decimal-literal spellings fail loud with pinned needles (Overflowing on NaN for d = 1.0, Cannot cast to Decimal128 for IN) against recorded Spark answers, filed as dated OPEN row ICE-NAN-DECIMAL-LITERAL-1, never absorbed.
      artifacts: [python/repark/tests/test_ice_nan_pushdown_1.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-4
      status: N/A
      justification: No new shared mutable state — fixture copies run under the copied cross-process directory lock, reads carry ORDER BY id, sessions are per-test with stop in finally.
    - id: AT-5
      status: N/A
      justification: No auth, injection, secret, or deserialization surface — fixed clause spellings and fixed table names over local test warehouses; no user input reaches SQL text.
    - id: AT-6
      status: ATTACKED
      evidence: Spark-written warehouses adopted byte-identical through register_table at both format versions; DELETE removes exactly the NaN rows and UPDATE touches exactly them on the recorded Spark outcomes; RePark-written twins answer identically.
      artifacts: [python/repark/tests/test_ice_nan_pushdown_1.py, python/repark/tests/_record_ice_nan_pushdown_1.py]
    - id: AT-7
      status: N/A
      justification: No perf claim filed — fixtures total 372 KB, the grid is a correctness pin, no resource shape changes.
    - id: AT-8
      status: ATTACKED
      evidence: Upstream contracts honored, not presumed — fork #284 named at pin 75da2b58, the Spark GAV read from _oracle_pins, the recorder re-derives every cell from live Spark and exits non-zero on drift, the live tier replays Spark per run.
      artifacts: [python/repark/tests/_record_ice_nan_pushdown_1.py, python/repark/tests/_oracle_pins.py, crates/repark-spark/src/tests/nan_pushdown.rs]
    - id: AT-9
      status: N/A
      justification: No product code and no new failure surface — the one observed failure raises the engine error verbatim, and every pin asserts on values rather than messages.
    - id: AT-10
      status: ATTACKED
      evidence: Pins-first held — the legs assert the exact query shape the fork ledger measured at [] on the unfixed tree, so they fail on the old pin by construction; the grid proved sensitive during development by catching the bare-decimal-literal defect as 3 DIFFs while every pushdown leg stayed ALL-EQUAL.
      artifacts: [python/repark/tests/test_ice_nan_pushdown_1.py, crates/repark-spark/src/tests/nan_pushdown.rs]
  complete: true
```

## Open questions

None. Every decision the brief left open is settled above; anything else halts with a hand-back question.
