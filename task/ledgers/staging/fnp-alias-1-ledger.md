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

**Moved out of this unit by ruling D-6 (orchestrator, 2026-09-15); the round-1 finding stands as the record:** `sum_distinct` /
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

- **D-6** (orchestrator ruling on Q-1, 2026-09-15) `sum_distinct` / `sumDistinct` leave this unit for FNP-AGG-1, the aggregates unit that runs on the build clone: it adds a DISTINCT flag to the native aggregate builder (`PyColumn.aggregate`) and pins the six `sum_distinct` / `sumDistinct` cells of `python/repark/tests/fnp_alias_1_spark_oracle.json` plus the `Deprecated in 3.2, use sum_distinct instead.` warning. The two names stay on the deferred census until then. Clauses C-001..C-003 are re-scoped to the six delivered names.
- **D-7** (orchestrator ruling on Q-2, 2026-09-15) The `degrees` / `radians` mechanism is accepted as built: a single multiply by the precomputed `180 / pi` (or `pi / 180`) factor under the `DEGREES(x)` / `RADIANS(x)` display. It is the same arithmetic shape as Java's `Math.toDegrees` / `Math.toRadians` that Spark evaluates, and all twelve Python-door cells plus the SQL-door value cell are measured bit-exact against the fixture. A dispatch arm for the engine kernel would add Rust for no observable difference.

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
| C-001 | The six delivered names (`approxCountDistinct`, `shiftLeft`, `shiftRight`, `shiftRightUnsigned`, `toDegrees`, `toRadians`; `sum_distinct` / `sumDistinct` moved to FNP-AGG-1 by D-6) are present on `repark.spark.functions` and in `__all__`, with parameter names and defaults equal to PySpark 4.1.2's recorded signatures. | `test_fnp_alias_1.py::test_delivered_names_present_and_exported` + `test_deprecated_signature_matches_the_spark_oracle` (6 names). | **PROVEN** | Six of eight pinned green: `approxCountDistinct`, `shiftLeft`, `shiftRight`, `shiftRightUnsigned`, `toDegrees`, `toRadians` — callable, in `F.__all__` (453→459, FN-SPLIT pin moved), parameter names/defaults equal to the recorded `inspect.signature` strings (annotation text follows the facade's `Column \| str` convention; the oracle's `'ColumnOrName'` spellings are PySpark's). `sum_distinct`/`sumDistinct` stay absent — blocked, Q-1 below; they remain on the deferred census (`test_functions_c.py`). Re-scoped to the delivered names by D-6. pins: fnp-alias-1/C-001 |
| C-002 | The Python-door oracle cells answer Spark-equal (column name, type, rows; nullability pinned except the D-2 group key and the D-3 approx result). | `test_fnp_alias_1.py::test_python_door_cell_matches_the_spark_oracle` (24 cells). | **PROVEN** | All 24 delivered-subset cells green (card gates: 79 passed; full facade suite 6119 passed, 368 skipped, 0 failed). Names, types and rows are compared, floats exactly; nullable is compared for every column except `g` (D-2) and the `approx_count_distinct` result (D-3). The 6 `sum_distinct`/`sumDistinct` grouped cells are unpinned this round — blocked (Q-1). Re-scoped to the delivered names by D-6. pins: fnp-alias-1/C-002 |
| C-003 | Calling each delivered deprecated alias warns `FutureWarning` with Spark's exact message. | `test_fnp_alias_1.py::test_deprecated_alias_warns_the_spark_message` (6 messages). | **PROVEN** | Six messages pinned exact: `Deprecated in 2.1, use approx_count_distinct instead.`, `Deprecated in 3.2, use shiftleft instead.` (shiftRight: `use shiftright instead.`, shiftRightUnsigned: `use shiftrightunsigned instead.`), `Deprecated in 2.1, use degrees instead.` (toRadians: `use radians instead.`). The seventh (`Deprecated in 3.2, use sum_distinct instead.`) stays unpinned with its name (Q-1). Re-scoped to the delivered names by D-6. pins: fnp-alias-1/C-003 |
| C-004 | Facade `degrees`/`radians` answer Spark's values bit-exactly, display `DEGREES(x)`/`RADIANS(x)`, accept INT input, and the SQL door's degrees/radians values are unchanged. | `test_fnp_alias_1.py` degrees/radians cells + `test_sql_door_degrees_radians_values_match_the_spark_oracle`. | **PROVEN** | The card's named route — `_scalar("degrees", …)` — has no `call_scalar` dispatch arm (measured: `call_scalar: unsupported function "degrees"`), and Rust is fenced, so the wrapper builds the same kernel arithmetic as a one-multiply plan by the precomputed `180.0/pi` / `pi/180` doubles (the `dayname`/`monthname` rewrap pattern, homes per D-1: `functions_math.py`). All 12 facade degrees/radians oracle cells and the SQL-door value cell are green, floats exact (`179.9998479605043` now; `F.degrees('i')` answers instead of raising `[ARITHMETIC_OVERFLOW]`); the SQL door's values and types are pinned unchanged. Registry row `FNP-ALIAS-DEGREES-1` filed FIXED (D-5). Deviation from the named mechanism flagged for ratification (Q-2). pins: fnp-alias-1/C-004 |
| C-005 | No regression on the functions suites. | The card's gates: `test_fnp_alias_1.py` + `test_functions_a/b/c.py`; plus `test_functions_f.py` and `test_functions_split_identity.py` (both edited here) and the full facade suite. | **PROVEN** | Card gates 79 passed; the six functions suites + the CAP-1 mirror: 112 passed; full `python/repark/tests`: **6119 passed, 368 skipped, 0 failed** (run in two passes — the suite minus `test_aws_acceptance.py`, which skips its 10 legs offline). The `__all__` surface pin moved 453→459; both deferred-absent censuses ratcheted down; the ceilings moved by recorded shrink only (1962→1960, 2247→2237, mirrored in CAP-1). pins: fnp-alias-1/C-005 |
| C-006 | Registry row + maps. | `docs/spark-sql-iceberg-parity.md` §7 `FNP-ALIAS-DEGREES-1`; map.md lockstep in every touched directory; ledger evidence complete. | **PROVEN** | The registry row is filed in the FN-* style with the `live PySpark 4.1.2, 2026-09-14` oracle line. Maps updated in-lockstep in the same commits: `python/repark/tests/map.md` (new test + fixture + census/surface ratchet notes), `python/repark/src/repark/spark/map.md` (three module entries + the move note), `scripts/map.md` and `python/repark-parity/tests/map.md` (ceiling ratchets), `task/ledgers/staging/map.md` (this ledger). pins: fnp-alias-1/C-006 |
| C-007 | The unit's review round ran on the merge head: a Grok critic-logic pass and an S2-21 Python performance pass, every P1/P2 remediated or ruled in this ledger, and AT-1..AT-10 attested from their evidence. | Reports `/tmp/oc-worker/pa-alias-crit/report.md` and `/tmp/oc-worker/pa-alias-perf/report.md`; dispositions recorded here. | **OPEN** | Launched by the orchestrator on 2026-09-15 after rulings D-6/D-7 and the example-coverage fix (`docs/examples/functions/deprecated_aliases.py`, the `INSTALL_NAMES` bindings in `scripts/check_example_coverage.py`); closes with the COVERAGE_ATTESTATION block. |

## Ruling questions — resolved by the orchestrator (Q-1 → D-6, Q-2 → D-7)

- **Q-1 (RULING)** — `sum_distinct`/`sumDistinct`: the card's "follow how `count_distinct`
  builds a DISTINCT aggregate" needs a native distinct-aggregate builder that the facade does
  not have (`count_aggregate` is count-specific; `aggregate(kind, ignore_nulls)` has no
  distinct modifier; no other PyO3 surface builds one), and every route around it is fenced
  (Rust by the card, `dataframe/**` by D-1). Options: a small Rust arm (generalize
  `count_aggregate` or add a distinct flag to `aggregate`) in a follow-up card, or a declared
  LOUD unsupported like the FNP-15 family. The two names stay on the deferred census either way.
- **Q-2 (RULING)** — the C-004 fix deviates from the card's named mechanism: the engine's
  `degrees`/`radians` scalar is reachable on the SQL door but has no facade dispatch arm, so the
  fix composes the kernel's single-multiply form instead and pins it bit-equal. If the critic
  wants the literal engine-scalar call, it is a two-arm Rust dispatch change plus moving the
  wrappers back.

VERDICT: 7 clauses, 6 PROVEN, 1 OPEN, 0 REJECTED.
