# map — docs/examples/column/

## Purpose

Worked examples for the `Column` expression surface: naming, predicates, string
matching, bitwise and type conversion, CASE ladders, sort markers, windows, and
item access. Examples construct the session as `repark = ReparkSession.builder…`;
see [../map.md](../map.md).

## Contents

- [naming.py](naming.py) — `alias` rename read and `transform` method chaining.
- [predicates.py](predicates.py) — `between`, `eqNullSafe` (`<=>`), and the null
  checks in both spellings (`isNull` / `is_null`, `isNotNull` / `is_not_null`).
- [strings.py](strings.py) — `contains`, `startswith`, `endswith`, `like`,
  `ilike`, `rlike`, and `substr` (int and Column arguments, 0 start ≡ 1).
- [bitwise_cast.py](bitwise_cast.py) — `bitwiseAND` / `bitwiseOR` / `bitwiseXOR`,
  `cast` (value and kept name), `try_cast` (bad input → NULL).
- [when_chains.py](when_chains.py) — the chained `when` ladder closed by
  `otherwise`.
- [order_markers.py](order_markers.py) — `asc`, `asc_nulls_first`,
  `asc_nulls_last`, `desc`, `desc_nulls_first`, `desc_nulls_last` orderings with
  nulls placed explicitly.
- [window_over.py](window_over.py) — `over` a partition-ordered spec
  (`row_number`) and a partition frame (`sum`).
- [accessors.py](accessors.py) — `getItem` array element (0-based, name
  `arr[1]`) and `getField` struct field (aliased read).
- [round_ext.py](round_ext.py) — repark extension `round` (HALF_UP, delegates to
  `F.round`). No Spark analog: PySpark's `Column` has no `round`.

Two bare-name arms the live oracle measured divergent are filed as §7 registry rows
([EX-COL-1](../../spark-sql-iceberg-parity.md), EX-COL-2) with pins in
`python/repark/tests/test_examples_column_a.py`, while the examples keep the arms where the
engines agree: an unaliased `select(F.col("v").cast("double"))` names its column with the
engine qualifier where Spark answers `v` (the df-bound and aliased `cast` arms are
Spark-equal, and `bitwise_cast.py` keeps those), and an unaliased `getField` projects
`r['a']` where Spark answers `r.a` (`accessors.py` keeps the aliased read).

## Pointers

- Up: [../map.md](../map.md)
- Pins: [../../../python/repark/tests/test_examples_column_a.py](../../../python/repark/tests/test_examples_column_a.py)
