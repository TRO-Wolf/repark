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
- R-3 (owner, critic round 1): both signatures are positional-only like CPython's `tuple` —
  `def count(self, value: Any, /)` and `def index(self, value: Any, start: int = 0,
  stop: int = sys.maxsize, /)` — so keyword calls raise `TypeError` the way tuple's do.

## PROPOSITION LEDGER — ROW-TUPLE-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `Row.count(value)` answers the five recorded count cells — `count` = 2, `count_missing` = 0, `positional_count` = 1 (`Row(1, 1, None).count(None)`), `count_nan` = 0 (`Row(a=float("nan")).count(float("nan"))`; two distinct NaN objects, tuple identity-then-`==` semantics), `count_factory` = 1 (`Row("a","b")` counts the field NAMES) — each pin driven from the committed fixture cell. | `test_row_tuple_1.py` five `test_count_reproduces_*` pins, red on the base. | **PROVEN** | Red on base `4d6b1ab0` (2026-09-14): `test_count_reproduces_row_count`, `test_count_reproduces_row_count_missing`, `test_count_reproduces_row_positional_count`, `test_count_reproduces_row_count_nan`, `test_count_reproduces_row_count_factory` — 9 failed, 1 passed; every behavioral pin died on `repark.errors.PySparkAttributeError: [ATTRIBUTE_NOT_SUPPORTED] Attribute `count` is not supported.` (the name did not exist). Green after the method landed on the stored values tuple (factory rows: the field-name tuple); the NaN cell rides CPython's tuple identity-first comparison, no hand-rolled NaN rule. pins: row-tuple-1/C-001 |
| C-002 | `Row.index(value, start=0, stop=sys.maxsize)` answers the four recorded index cells — `index` = 0, `index_start` = 2 (`index(1, 1)` on `Row(a=1, b=2, c=1)`), `index_factory` = 1 (`Row("a","b").index("b")` indexes the field NAMES), and `index_missing` raising builtins `ValueError` with Spark's exact message `tuple.index(x): x not in tuple`. | `test_row_tuple_1.py` four `test_index_reproduces_*` pins incl. the exact-class/message pin, red on the base. | **PROVEN** | Red on base (same run): `test_index_reproduces_row_index`, `test_index_reproduces_row_index_start`, `test_index_reproduces_row_index_factory`, `test_index_reproduces_row_index_missing` — same `AttributeError` shape (`index` not supported). Green after delegation to the stored tuple's `index`; the missing cell raises plain builtins `ValueError` with the recorded message byte-for-byte — no PySpark wrapper (the oracle MRO is `ValueError → Exception → BaseException → object`). pins: row-tuple-1/C-002 |
| C-003 | No SQL door exists for these names (R-2); the pre-existing `python/repark/tests/test_row.py` passes unchanged; the fixture file is in the tree with a `map.md` row. | The gate run over both test files; the map rows; the fixture-presence pin. | **PROVEN** | `.venv/bin/python -m pytest python/repark/tests/test_row_tuple_1.py python/repark/tests/test_row.py -q` → 33 passed (`test_row.py` untouched, 23 of the 33). `python/repark/tests/map.md` lists `test_row_tuple_1.py` and `facade_row_oracle.json`; `test_oracle_fixture_carries_the_row_cells` asserts the nine `row.*` cells and the PySpark 4.1.2 provenance line in the committed JSON. R-2: `count`/`index` are client-side Python tuple protocol on an already-collected row — neither engine gives them a SQL spelling, so there is no second door to pin. pins: row-tuple-1/C-003 |
| C-004 | Critic round 1 pins (L-001…L-005, logic critic 2026-09-14, report `/tmp/oc-worker/run15b/critic-row-report.md`, 9/9 oracle cells confirmed matching): L-001 `index` stop/negative-start bounds (`index(1, 0, 0)` raises the recorded `ValueError`, `index(1, 0, 1) == 0`, `index(1, -1) == 2`); L-002 names+values never searched together (`Row(a="a", b=1).count("a") == 1`; `Row(x="a").index("x")` raises); L-003 the tuple method wins attribute access over a same-named field (`callable(Row(count=3).count)`, `["count"] == 3`, same for `index`, and a collected `groupBy("g").count()` row: `["count"] == 1` while `.count` is callable — PySpark 4.1.2 answers the same); L-004 the EX-ROW-1 divergence arm (collected struct cell is a dict: `.count({"x": 1, "y": 2}) == 1`, `.count(Row(x=1, y=2)) == 0`; registry Pin list extended, no new row); L-005 / R-3 positional-only signatures (`index(1, start=0)` and `count(value=1)` raise `TypeError`). | `test_row_tuple_1.py` six critic-round tests; red-first runs below. | **PROVEN** | Red on the pre-R-3 tree: `test_index_and_count_reject_keyword_arguments` — `DID NOT RAISE TypeError` (the round-1 signatures accepted `start=`/`value=` keywords and answered 0); fixed by the R-3 `/`. Green on first run, and why: L-001 (`test_index_stop_and_negative_start_match_tuple`) — the base already passes `stop` through to `tuple.index`, the pin is a mutation guard against an ignore-stop mutant, proved discriminating by temporarily dropping `stop` from the pin's call (`index(1, 0)` instead of `index(1, 0, 0)`): `FAILED …::test_index_stop_and_negative_start_match_tuple — Failed: DID NOT RAISE ValueError` (finds `values[0]` → 0), then restored; L-002 (`test_names_and_values_are_never_searched_together`) — guard against the names+values mutant, proved by temporarily mutating the impl to `names.count(value) + values.count(value)`: `FAILED …::test_names_and_values_are_never_searched_together — assert 2 == 1 (where Row(a='a', b=1) = Row(a='a', b=1))`, then restored; L-003 (`test_method_wins_attribute_access_item_answers_the_field`, `test_collected_count_column_method_wins_and_item_answers`) — the method-wins rule already matched Spark the day the methods landed (attribute lookup finds the class method before `__getattr__`, exactly PySpark's tuple-MRO order); L-004 (`test_collected_nested_struct_counts_as_dict_not_row`) — pins today's EX-ROW-1 divergence on purpose, Spark's 1-with-nested-Row answer stays recorded in the registry row. pins: row-tuple-1/C-004 |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

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

## Critic round 1 (logic critic, 2026-09-14) — L-006 P3 note

L-006 (P3, note only): pre-change repark answered `Row(count=3).count` with the field value
`3` through `__getattr__` (no method existed); after this unit the tuple method wins attribute
access, matching PySpark 4.1.2's tuple MRO. `__getitem__` / `asDict()` still answer the field
(pinned in C-004). No product bug vs Spark; recorded so the shadowing change on a common
column name is not silent. L-001…L-005 are discharged in C-004; the critic's verdict
(NEEDS_REMEDIATION, 0 P1) is fully remediated by that clause.

## What changed

| File | Change |
|---|---|
| `python/repark/src/repark/spark/row.py` | `count` and `index` delegating to the stored values tuple (factory rows: the field-name tuple); `import sys` for the `stop=sys.maxsize` default. Critic round 1: both signatures positional-only (R-3, `/`). |
| `python/repark/tests/test_row_tuple_1.py` | New. Ten pins: five count cells, four index cells, the fixture-presence pin. Critic round 1: six more tests (L-001 stop/negative-start bounds, L-002 names+values, L-003 shadowing ×2 incl. the collected `groupBy().count()` row, L-004 EX-ROW-1 divergence arm, L-005 keyword `TypeError`). |
| `python/repark/tests/facade_row_oracle.json` | New. The run-15b live PySpark 4.1.2 oracle fixture, copied unchanged. |
| `docs/spark-sql-iceberg-parity.md` | EX-ROW-1's **Pin** list gains the L-004 count/index divergence pin (no new row). |
| `python/repark/tests/map.md` | Lockstep rows for the two new files. |
| `python/repark/src/repark/spark/map.md` | `row.py` row updated (count/index + R-3). |
| `task/ledgers/staging/map.md` | This ledger's row. |

`STATUS.md` and `briefs/next-sequence.md` untouched; no dependency change; no Rust change.

## Orchestrator rulings (run 15b, G-2)

- R-4 (2026-09-14): the critic's P2 findings L-001..L-004 were pin-only remediation; the orchestrator re-ran every
  added pin and read the diff line by line, so no second critic round runs. L-005 (P3) was taken as ruling R-3
  (positional-only signatures). L-006 stays a P3 note.
- R-5 (2026-09-14): the S2-21 perf reviewers do not run on this unit — two O(n) delegations to the stored tuple, no
  hot path, no Rust.
- R-6 (2026-09-14): the example-coverage gate owed an example for the two new names; the orchestrator added
  `docs/examples/dataframe/row_tuple.py` and moved the measured inventory counts (930 → 932, types 32 → 34).

```yaml
COVERAGE_ATTESTATION:
  pr_unit: row-tuple-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the card and the fourteen recorded PySpark 4.1.2 row cells; the nine count/index cells are pinned one-to-one, and the critic's cell table re-derived all nine against the fixture and pyspark/sql/types.py.
      artifacts: [python/repark/tests/test_row_tuple_1.py, python/repark/tests/facade_row_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Distinct and identical NaN objects, None fields, empty factory, factory versus value rows, names-plus-values double count, explicit stop, negative start, stop beyond length, keyword calls refused, fields named count and index, a collected groupBy().count() row, nested collected structs (EX-ROW-1 arm).
      artifacts: [python/repark/tests/test_row_tuple_1.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-3
      status: N/A
      justification: Two Python methods delegating to an immutable tuple slot; no Rust, no unwrap, no I/O.
    - id: AT-4
      status: N/A
      justification: No shared mutable state, no threads, no async.
    - id: AT-5
      status: N/A
      justification: No authn/authz, deserialization, path, credential or network surface.
    - id: AT-6
      status: ATTACKED
      evidence: Grok critic-logic round 1 (0 P1, 4 P2, 2 P3); the P2s became pins in 8097597d, each re-run by the orchestrator; the stop pin was mutation-proven red by dropping the argument.
      artifacts: [task/ledgers/completed/row-tuple-1-ledger.md]
    - id: AT-7
      status: ATTACKED
      evidence: Facade suite 6094 passed / 368 skipped and the parity suite on the release native of the rebased head; ruff, check_lib_py, ledger and map checks clean; example-coverage gate clean with the new example executed.
      artifacts: [docs/examples/dataframe/row_tuple.py, python/repark-parity/tests/test_ex_0_example_coverage.py]
    - id: AT-8
      status: ATTACKED
      evidence: The contracts used were read, not assumed - pyspark/sql/types.py `class Row(tuple)` and its `__new__` (factory rows are a tuple of field names, value rows a tuple of values, no count/index override) and CPython's `tuple.count` / `tuple.index` signatures (identity-then-equality comparison, positional-only, start/stop slicing semantics).
      artifacts: [python/repark/src/repark/spark/row.py, python/repark/tests/facade_row_oracle.json]
    - id: AT-9
      status: ATTACKED
      evidence: Both names raise the builtin ValueError with CPython's own text on a miss and TypeError on keyword calls, exactly as Spark's inherited tuple methods do; field access through `row["count"]` / `row["index"]` still returns the field, pinned beside the method-wins rule.
      artifacts: [python/repark/tests/test_row_tuple_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Mutations run, not reasoned - dropping `stop` from the index call reds the stop pin; searching values for a factory row reds count_factory; names-plus-values double counting reds the L-002 pin.
      artifacts: [task/ledgers/completed/row-tuple-1-ledger.md, python/repark/tests/test_row_tuple_1.py]
  complete: true
```
