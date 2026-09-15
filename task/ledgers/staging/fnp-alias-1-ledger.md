# Charter ledger — FNP-ALIAS-1 · PySpark alias names over existing kernels

**Date:** 2026-09-15 · **Branch:** `feat/fnp-alias-1` · **Base:** `origin/main` `acbb6a8e` ·
**Model:** GLM 5.3 Flash (zai/glm-5.3-flash) · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**
**Registry:** `FNP-ALIAS-DEGREES-1` BACKLOG → **FIXED** (docs/spark-sql-iceberg-parity.md §7).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The 1.5 Spark-parity campaign's shape rule: a name is done when it is on
`repark.spark.functions` and answers PySpark 4.1.2 for its argument shapes, NULLs and result
types, with pins. Eight PySpark names are absent and one pair of existing names is wrong. The
oracle (`python/repark/tests/fnp_alias_1_spark_oracle.json`) is live PySpark 4.1.2, recorded
2026-09-14 by the orchestrator (98 cells, 14 signatures).

**Not in this unit:** the SQL-door column naming (`sum(DISTINCT t.v)`,
`shiftleft(t.i,Int64(1))`) and the VALUES grouping-key nullability (D-2, run 15c's SQL-door
work); `approx_count_distinct` result nullability (D-3, registry row BL-18).

**Blocked this round — orchestrator ruling needed (Q-1, handback):** `sum_distinct` /
`sumDistinct` cannot be delivered under the card's "no Rust edits" fence. The card's directive —
"follow how `count_distinct`/`countDistinct` builds a DISTINCT aggregate in the facade" —
presupposes a generic native distinct-aggregate builder; the facade's only distinct builders are
count-specific (`PyColumn.count_aggregate(columns, distinct)`) and collect-specific
(`collect_aggregate(argument, distinct)`); `PyColumn.aggregate(kind, ignore_nulls)` has no
distinct modifier, and no other native surface builds one. `test_functions_c.py`'s deferred
census records the same gap ("it is not a kernel — DataFusion spells it `sum(DISTINCT x)`, a
modifier on the aggregate call, so it needs the facade's DISTINCT path rather than a dispatch
arm"). Wiring one needs a Rust arm (banned here). The two names stay on the deferred census and
their oracle cells stay unpinned this round; the delivered six names are pinned. `dataframe/**`
is also fenced by D-1, so the grouped-agg SQL fallback that could have carried a SQL-spelled
DISTINCT aggregate is not reachable either.

## Decisions (orchestrator rulings, binding)

- **D-1** Names live in the functions module family: added where the siblings live
  (`functions_math.py`, `functions_bitwise.py`, `functions_agg.py` or `functions.py`), exported
  in `__all__` like the siblings. No edits to `dataframe/**`, `column.py`, `session/**`,
  `catalog.py`, `types.py`, or any Rust.
- **D-2** SQL-door column naming (`sum(DISTINCT t.v)`, `shiftleft(t.i,Int64(1))`) and the VALUES
  grouping-key nullability are run 15c's SQL-door work: SQL-door cells are pinned on values and
  types only, and the name difference is recorded as an out-of-scope observation, not a fix.
- **D-3** `approx_count_distinct` result nullability (RePark nullable, Spark non-null) is
  registry row BL-18 — left as is; values and type are pinned, nullability is not.
- **D-4** Pins: `python/repark/tests/test_fnp_alias_1.py` — parametrized over the Python-door
  cells (column name, type, rows; nullability only where D-3 does not apply) plus
  warning-message pins, plus SQL-door value pins for `degrees`/`radians`. Red first on the base
  tree. Floats compared exactly (Spark's values are what the engine kernel produces).
- **D-5** Registry: `docs/spark-sql-iceberg-parity.md` §7 gains one row
  `FNP-ALIAS-DEGREES-1 — facade degrees/radians composed the formula — **FIXED 2026-09-15
  (FNP-ALIAS-1)**` in the style of the `FN-*` FIXED rows, oracle line
  `live PySpark 4.1.2, 2026-09-14`.

## Red-first record (base tree `acbb6a8e`, 2026-09-15)

`.venv/bin/python -m pytest python/repark/tests/test_fnp_alias_1.py -q` → **37 failed, 1
passed**. Breakdown: 1 name-presence pin (`AttributeError: module 'repark.spark.functions' has
no attribute 'approxCountDistinct'`), 6 signature pins, 6 warning pins, and all 24 Python-door
oracle cells. The degrees/radians cells red on the composed formula itself
(`AssertionError: assert '((rad * 180) / pi())' == 'DEGREES(rad)'`, last-digit drift
`179.99984796050427` vs Spark `179.9998479605043`, and `[ARITHMETIC_OVERFLOW]` on
`F.degrees('i')`); the alias cells red on the absent names. The 1 pass is the SQL-door
degrees/radians value control — the SQL door already answers Spark's values, which is exactly
the base the card's C-004 builds on.

## PROPOSITION LEDGER — FNP-ALIAS-1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The eight names are present on `repark.spark.functions` and in `__all__`, with parameter names and defaults equal to PySpark 4.1.2's recorded signatures. | `test_fnp_alias_1.py::test_delivered_names_present_and_exported` + `test_deprecated_signature_matches_the_spark_oracle` (6 names). | **OPEN** | Six of eight (approxCountDistinct, shiftLeft, shiftRight, shiftRightUnsigned, toDegrees, toRadians) are pinned green after commit 2. `sum_distinct`/`sumDistinct` stay absent — blocked, see the ruling question above; they remain on the deferred census. |
| C-002 | The Python-door oracle cells answer Spark-equal (column name, type, rows; nullability pinned except the D-2 group key and the D-3 approx result). | `test_fnp_alias_1.py::test_python_door_cell_matches_the_spark_oracle` (24 cells). | **OPEN** | All 24 delivered-subset cells green after commit 2 (values, types, names; floats exact). The 6 `sum_distinct`/`sumDistinct` grouped cells are unpinned this round — blocked (Q-1). |
| C-003 | Calling each delivered deprecated alias warns `FutureWarning` with Spark's exact message. | `test_fnp_alias_1.py::test_deprecated_alias_warns_the_spark_message` (6 messages). | **OPEN** | Six messages pinned exact: `Deprecated in 3.2, use sum_distinct instead.` is the seventh and stays unpinned with its name (Q-1). |
| C-004 | Facade `degrees`/`radians` answer Spark's values bit-exactly, display `DEGREES(x)`/`RADIANS(x)`, accept INT input, and the SQL door's degrees/radians values are unchanged. | `test_fnp_alias_1.py` degrees/radians cells + `test_sql_door_degrees_radians_values_match_the_spark_oracle`. | **OPEN** | Red on the base (display, drift, overflow). Fix: the card's named route — calling the engine scalar through `_scalar` — has no `call_scalar` dispatch arm and Rust is fenced, so the facade builds the same kernel arithmetic as a one-multiply plan by the precomputed `180/pi` / `pi/180` doubles (the `dayname`/`monthname` rewrap pattern), measured bit-equal to Spark on every oracle cell; deviation from the named mechanism flagged in the handback (Q-2). |
| C-005 | No regression on the functions suites. | The card's gates: `test_functions_a/b/c.py` + `test_fnp_alias_1.py`; plus `test_functions_f.py` and `test_functions_split_identity.py` (both edited here). | **OPEN** | Green after commit 2. The `__all__` surface pin moves 453→459 (six appended alias names, the FN-SPLIT ratchet pattern). |
| C-006 | Registry row + maps. | `docs/spark-sql-iceberg-parity.md` §7 `FNP-ALIAS-DEGREES-1`; map.md lockstep in every touched directory; ledger evidence complete. | **OPEN** | Commit 3. |

VERDICT: 6 clauses, 0 PROVEN, 6 OPEN, 0 REJECTED.
