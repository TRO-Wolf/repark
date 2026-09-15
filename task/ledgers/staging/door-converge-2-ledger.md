# Unit ledger — DOOR-CONVERGE-2 round 1 · P1 wrong answers on the Spark SQL door

**Date:** 2026-09-15 · **Branch:** `feat/door-kernel-converge-2` · **Base:** `origin/feat/door-kernel-converge-1`
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card DOOR-CONVERGE-2 round 1 (run 16c, 2026-09-15): four P1 names answer wrong on
the Spark SQL door — `concat` over arrays stringifies (`'[1][2]'`), `reverse` over arrays reverses
stringified text, `sequence` and `split` are "Invalid function". Each name converges when it answers
PySpark 4.1.2 on BOTH doors for argument shapes, NULL and edge cases, result VALUE and Arrow TYPE and
NULLABILITY, with pins; otherwise a dated DECLARED refusal with Spark's error class and a registry row.

**Not in this step:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.toml`,
`Cargo.lock`, `pyproject.toml`, `uv.lock`,
`python/repark/src/repark/spark/functions*.py`, `dataframe/**`, `column.py`, `catalog.py`,
`crates/repark-spark/src/{extension,normalize,spark_literals}.rs` (other lanes own them).
The decimal round/ceil/floor, date_part seconds, literal widths and element nullability clauses
are round 2.

**Oracle:** `/tmp/oc-worker/qc-oracle/fixtures-batch12.json` (PySpark 4.1.2, cells Q12-0…Q12-55;
temp view `t`: `a array<int> = [1,2]`, `an array<int> = [3,NULL]`, a NULL `array<int>` column,
`s string = 'a,b,,c'`, `n int = 5`, `sa array<string> = ['x','y']`) and
`/tmp/oc-worker/pc-oracle/fixtures-batch7.json` (cells N7-16 concat, N7-17 sequence, N7-18 split,
N7-22 reverse). Cells spelled `2.5D`/`1L` are re-pinned with `CAST` per the card note.

## Rulings

| Ruling | Source | Content |
|---|---|---|
| Q-15c-6 | owner 2026-09-15 | No global retag wrapper; per-function fixes, one oracle pin each. |
| Q-15c-7 | owner 2026-09-15 | concat/reverse on arrays and sequence/split on the SQL door are P1 and go first. |
| R-13 | DOOR-CONVERGE-1 round 4 | Literal-haystack nullability legs pin today's `nullable=True` as recorded divergence ARRAY-LITERAL-CONTAINSNULL-1 until the array-constructor fix lands. |

## PROPOSITION LEDGER — DOOR-CONVERGE-2 round 1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `concat` over arrays answers Spark on the SQL door (cells Q12-0…Q12-10, Q12-16 `||` over arrays); string/binary controls Q12-11…Q12-15 do not regress. Element type widens to the common type (Q12-3 bigint, Q12-4 `decimal(11,1)`), `containsNull` is the OR of the inputs (Q12-1, Q12-7), result nullable iff any input nullable, any NULL array → NULL (Q12-2), nested arrays keep inner nullability (Q12-8), empty `array()` joins (Q12-9), array mixed with string refuses `DATATYPE_MISMATCH.DATA_DIFF_TYPES` (Q12-10). BINARY stays BINARY (Q12-13). | `test_door_converge_2.py` Q12-0…Q12-16 legs green on both doors. | OPEN | Kernel done 2026-09-15: one `concat` UDF with string/binary/array arms (`collection/concat_array.rs` helpers, `PyColumn::concat` embeds the same UDF); Rust pins green (`concat_array::*`, `string::tests::concat_binary_stays_binary`). Python pins pending the native rebuild. `||` left on DataFusion's `array_concat` rewrite (values already right; element leg cites ARRAY-LITERAL-CONTAINSNULL-1). Mixed string/binary resolves `Binary` (Spark's any-binary-wins `dataType`). |
| C-002 | `reverse` over arrays answers Spark on the SQL door (Q12-17…Q12-25): element order reversed, element type and `containsNull` and nullability kept, NULL array → NULL, `reverse('abc')` still `'cba'`, untyped `reverse(NULL)` is a NULL STRING. | Q12-17…Q12-25 legs green on both doors. | OPEN | Redacted until the red-first run below. |
| C-003 | `sequence` is registered on the SQL door (Q12-26…Q12-40): integer widths kept (int, bigint, tinyint), descending default step, explicit step, dates with default 1-day step and INTERVAL MONTH step, timestamps with INTERVAL HOUR, NULL bound or step → NULL with `containsNull=false`, zero or wrong-sign step raises (closest RePark error mapping recorded), DECIMAL bound refuses `DATATYPE_MISMATCH.SEQUENCE_WRONG_INPUT_TYPES`. The existing expansion ceiling (`refuse_literal_expansion`) must still fire. | Q12-26…Q12-40 legs green on both doors. | OPEN | Redacted until the red-first run below. |
| C-004 | `split` is registered on the SQL door (Q12-41…Q12-55): Java-regex pattern (`'.'` splits every char → four empty strings Q12-51), `limit` > 0 caps the pieces with the remainder in the last, `limit` ≤ 0 keeps trailing empties (Q12-44/45; Spark 4 keeps them for 0 too — read the cell), empty pattern splits per character (Q12-47), `split('', ',')` → `['']`, NULL string / pattern / limit → NULL, `containsNull=false`, numeric first argument cast to string (Q12-55). | Q12-41…Q12-55 legs green on both doors. | OPEN | Redacted until the red-first run below. |
| C-005 | Each of C-001…C-004 pinned on the Python API as well (`F.concat`, `F.reverse`, `F.sequence`, `F.split` over a `createDataFrame` with the same columns) for value, Arrow type and nullability via `to_arrow`/`collect`. A facade cell whose fix lives in `functions*.py` is recorded as a P2 hand-off to run 16a and marked OPEN. | Facade legs green or handed off with the reason named. | OPEN | `F.split` raises `UnsupportedOperationException` in `functions_expr.py` (outside the fence) — likely P2 hand-off; confirmed in the red-first run. |
| C-006 | Registry rows in `docs/spark-sql-iceberg-parity.md` (inside the section that already holds the DOOR-CONVERGE rows; append, do not reorder) disposition FIXED with the pin names; any of these four names in `EXPECTED_DIVERGENCES` removed when the kernels converge. | Registry diff + `door_parity_tests` green. | OPEN | None of concat/reverse/sequence/split is in `EXPECTED_DIVERGENCES` or `SCALAR_NAMES` today — the ratchet half is the `dispatch_spark.rs` door-converged set. |

## Evidence

### Red-first run — base tree probes (2026-09-15)

`crates/repark-functions/tests/probe_door2.rs` (scratch, deleted before the last commit) on the
base tree, `register_all` + analyzer rules door:

```
PROBE SELECT concat(array(1), array(2))
  TYPE: Utf8 NULLABLE: false VALS: "[1][2]"
PROBE SELECT array(1) || array(2)
  TYPE: List(Field { data_type: Int32, nullable: true }) NULLABLE: false VALS: [1, 2]
PROBE SELECT reverse(array(1,2))
  TYPE: Utf8 NULLABLE: false VALS: "]2 ,1["
PROBE SELECT reverse(array(3, NULL))
  TYPE: Utf8 NULLABLE: false VALS: "] ,3["
PROBE SELECT reverse('abc')
  TYPE: Utf8 NULLABLE: false VALS: "cba"
PROBE SELECT sequence(1, 3)
  PLAN-ERR: Error during planning: Invalid function 'sequence'.
PROBE SELECT split('a,b', ',')
  PLAN-ERR: Error during planning: Invalid function 'split'.
```

Every clause red as reported in 15c: array `concat` stringifies, array `reverse` reverses the
stringified text, `sequence`/`split` are unregistered. `||` over arrays already answers `[1, 2]`
via DataFusion's nested-planner rewrite to `array_concat` — values right, element stays
`nullable: true` (the ARRAY-LITERAL-CONTAINSNULL-1 constructor divergence, round 2).

### Green run

TBD.

### Out-of-scope observed

TBD.

### P2 hand-offs to run 16a

TBD.
