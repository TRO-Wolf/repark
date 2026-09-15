# Ledger — ROW-TUPLE-1 step 1 · `Row.count` and `Row.index` (tuple protocol)

**Date:** 2026-09-14 · **Branch:** `feat/row-tuple-1` · **Base:** `4d6b1ab0`
(`4d6b1ab038708cf27a95e335070978f1110a1acd`, origin/main) · **Model:** zai/glm-5.3-flash ·
**Policy:** [../../../AGENTS.md](../../../AGENTS.md). **risk_tier: standard.**

**Unit.** Spark's `Row` is a `tuple` subclass; repark's `Row` is not (it stores
`_Row__field_values`). This step adds `Row.count` and `Row.index`, answering exactly as
`tuple(values)` would, pinned cell-by-cell against the run-15b live PySpark 4.1.2 oracle
`facade_row_oracle.json`, copied unchanged into `python/repark/tests/`.

**Rulings.**

- R-1 (owner): repark does NOT become a tuple subclass in this unit — the other `row.*`
  cells (`isinstance_tuple`, `add`, `hash_eq`) are out of scope; the observed repark answers
  are recorded below.
- R-2 (owner): no SQL door exists for these names — recorded in C-003.

## PROPOSITION LEDGER — ROW-TUPLE-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `Row.count(value)` answers the five recorded count cells — `count` = 2, `count_missing` = 0, `positional_count` = 1 (`Row(1, 1, None).count(None)`), `count_nan` = 0 (`Row(a=float("nan")).count(float("nan"))`; two distinct NaN objects, tuple identity-then-`==` semantics), `count_factory` = 1 (`Row("a","b")` counts the field NAMES) — each pin driven from the committed fixture cell. | `test_row_tuple_1.py` five `test_count_reproduces_*` pins, red on the base. | **PROVEN** | Red on base `4d6b1ab0` (2026-09-14): `test_count_reproduces_row_count`, `test_count_reproduces_row_count_missing`, `test_count_reproduces_row_positional_count`, `test_count_reproduces_row_count_nan`, `test_count_reproduces_row_count_factory` — 9 failed, 1 passed; every behavioral pin died on `repark.errors.PySparkAttributeError: [ATTRIBUTE_NOT_SUPPORTED] Attribute `count` is not supported.` (the name did not exist). Green after the method landed on the stored values tuple (factory rows: the field-name tuple); the NaN cell rides CPython's tuple identity-first comparison, no hand-rolled NaN rule. pins: row-tuple-1/C-001 |
| C-002 | `Row.index(value, start=0, stop=sys.maxsize)` answers the four recorded index cells — `index` = 0, `index_start` = 2 (`index(1, 1)` on `Row(a=1, b=2, c=1)`), `index_factory` = 1 (`Row("a","b").index("b")` indexes the field NAMES), and `index_missing` raising builtins `ValueError` with Spark's exact message `tuple.index(x): x not in tuple`. | `test_row_tuple_1.py` four `test_index_reproduces_*` pins incl. the exact-class/message pin, red on the base. | **PROVEN** | Red on base (same run): `test_index_reproduces_row_index`, `test_index_reproduces_row_index_start`, `test_index_reproduces_row_index_factory`, `test_index_reproduces_row_index_missing` — same `AttributeError` shape (`index` not supported). Green after delegation to the stored tuple's `index`; the missing cell raises plain builtins `ValueError` with the recorded message byte-for-byte — no PySpark wrapper (the oracle MRO is `ValueError → Exception → BaseException → object`). pins: row-tuple-1/C-002 |
| C-003 | No SQL door exists for these names (R-2); the pre-existing `python/repark/tests/test_row.py` passes unchanged; the fixture file is in the tree with a `map.md` row. | The gate run over both test files; the map rows; the fixture-presence pin. | **PROVEN** | `.venv/bin/python -m pytest python/repark/tests/test_row_tuple_1.py python/repark/tests/test_row.py -q` → 33 passed (`test_row.py` untouched, 23 of the 33). `python/repark/tests/map.md` lists `test_row_tuple_1.py` and `facade_row_oracle.json`; `test_oracle_fixture_carries_the_row_cells` asserts the nine `row.*` cells and the PySpark 4.1.2 provenance line in the committed JSON. R-2: `count`/`index` are client-side Python tuple protocol on an already-collected row — neither engine gives them a SQL spelling, so there is no second door to pin. pins: row-tuple-1/C-003 |

VERDICT: 3 clauses, 3 PROVEN, 0 OPEN, 0 REJECTED.

## Per-name decision table

| Name | Decision | Reason |
|---|---|---|
| `Row.count` | implemented | Pure tuple protocol over the stored values (factory rows: the field-name tuple); no JVM, RDD, streaming, Spark Connect or engine-execution dependency. |
| `Row.index` | implemented | Same delegation, including the `start`/`stop` bounds and Spark's exact bare-`ValueError` refusal message; no engine dependency. |

## Out of scope (R-1) — measured in this clone, 2026-09-14

- `isinstance_tuple` (`isinstance(Row(a=1), tuple)`): repark answers `False`; Spark's recorded
  answer is `true` — repark's `Row` stays a non-tuple subclass by ruling R-1.
- `add` (`Row(a=1) + Row(b=2)`): repark raises `TypeError: unsupported operand type(s) for +:
  'Row' and 'Row'`; Spark's recorded answer is the plain tuple `(1, 2)` — `__add__` is out of
  scope under R-1.
- `hash_eq` (`hash(Row(a=1)) == hash((1,))`): repark answers `True`, equal to Spark's recorded
  `true` — the values-only hash already matches; no work.

The fixture also carries `len` (repark `2` = Spark's `2`; already pinned in `test_row.py`) and
`types_row_is_sql_row` (a types-surface cell, outside this card).

## What changed

| File | Change |
|---|---|
| `python/repark/src/repark/spark/row.py` | `count` and `index` delegating to the stored values tuple (factory rows: the field-name tuple); `import sys` for the `stop=sys.maxsize` default. |
| `python/repark/tests/test_row_tuple_1.py` | New. Ten pins: five count cells, four index cells, the fixture-presence pin. |
| `python/repark/tests/facade_row_oracle.json` | New. The run-15b live PySpark 4.1.2 oracle fixture, copied unchanged. |
| `python/repark/tests/map.md` | Lockstep rows for the two new files. |
| `task/ledgers/staging/map.md` | This ledger's row. |

`STATUS.md` and `briefs/next-sequence.md` untouched; no dependency change; no Rust change.
