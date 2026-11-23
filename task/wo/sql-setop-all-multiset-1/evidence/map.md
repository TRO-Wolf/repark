# task/wo/sql-setop-all-multiset-1/evidence/ — SQL-SETOP-ALL-MULTISET-1 measurement evidence

## Provenance

- **RePark:** main `e80e6bc8` and the branch head (`head4` `2ce51f04`), release wheels (maturin 1.14.1, Python 3.12); the mutant (`f6376a1b`, pre-Delta-1) is head with `insert_multiset_set_operations` not seating the rule, so the ALL forms plan DataFusion's semi and anti join: the wrong answer main's refusal guarded.
- **Spark:** PySpark 4.1.2 with `iceberg-spark-runtime-4.1_2.13` 1.11.0, Java 17, `local[2]`, a Hadoop catalog `ice`, session zone UTC, ANSI on and off; recorded into `python/repark/tests/sql/setop_all_multiset_1_spark_oracle.json` by `python _setop_all_multiset_1_cells.py <warehouse> <fixture>` (persistent views live in `spark_catalog.default`: a Hadoop catalog has no views).
- **Doors:** `spark.sql` over a memory catalog `ice` in both ANSI modes; `repark.sql()` over a native session's memory catalog, against Spark's ANSI-on answer. Writes are read back cold by a new session loading the table folder.

## Domain table

[domain-table.md](domain-table.md) holds every cell. Count by `(mode, group)` across `main`, `mutant`, and `head`:

| mode | group | main | mutant | head |
|---|---|---|---|---|
| ANSI true | control | 6 EQUAL | 6 EQUAL | 6 EQUAL |
| ANSI true | dataframe | 52 REFUSED | 52 WRONG | 52 EQUAL |
| ANSI true | edge | 15 REFUSED | 12 EQUAL, 3 WRONG | 15 EQUAL |
| ANSI true | error | 6 ERR-DIFF | 6 ERR-CLASS | 6 ERR-CLASS |
| ANSI true | fragment | 12 REFUSED | 2 EQUAL, 10 WRONG | 12 EQUAL |
| ANSI true | guard | 12 EQUAL | 12 EQUAL | 12 EQUAL |
| ANSI true | shape | 36 REFUSED | 4 EQUAL, 32 WRONG | 36 EQUAL |
| ANSI true | spelling | 9 REFUSED | 9 WRONG | 9 EQUAL |
| ANSI true | type | 78 REFUSED | 34 EQUAL, 44 WRONG | 78 EQUAL |
| ANSI false | control | 6 EQUAL | 6 EQUAL | 6 EQUAL |
| ANSI false | dataframe | 52 REFUSED | 52 WRONG | 52 EQUAL |
| ANSI false | edge | 15 REFUSED | 12 EQUAL, 3 WRONG | 15 EQUAL |
| ANSI false | error | 6 ERR-DIFF | 6 ERR-CLASS | 6 ERR-CLASS |
| ANSI false | fragment | 12 REFUSED | 2 EQUAL, 10 WRONG | 12 EQUAL |
| ANSI false | guard | 12 EQUAL | 12 EQUAL | 12 EQUAL |
| ANSI false | shape | 36 REFUSED | 4 EQUAL, 32 WRONG | 36 EQUAL |
| ANSI false | spelling | 9 REFUSED | 9 WRONG | 9 EQUAL |
| ANSI false | type | 78 REFUSED | 34 EQUAL, 44 WRONG | 78 EQUAL |
| native | control | 6 EQUAL | 6 EQUAL | 6 EQUAL |
| native | edge | 15 REFUSED | 12 EQUAL, 3 WRONG | 10 EQUAL, 5 REFUSED |
| native | error | 6 ERR-DIFF | 6 ERR-CLASS | 5 ERR-CLASS, 1 ERR-DIFF |
| native | shape | 36 REFUSED | 4 EQUAL, 9 REFUSED, 23 WRONG | 18 EQUAL, 18 REFUSED |
| native | spelling | 9 REFUSED | 9 WRONG | 6 EQUAL, 3 REFUSED |
| native | type | 78 REFUSED | 34 EQUAL, 44 WRONG | 52 EQUAL, 26 REFUSED |

Main refuses every multiset cell and answers every `control` and `guard` cell. The mutant answers the old wrong multiset: `INTERSECT ALL` keeps every left duplicate and `EXCEPT ALL` drops every copy. Head answers Spark's rows in every cell on the Spark door in both ANSI modes, output types included. On the native `repark.sql()` door, `INTERSECT ALL` and `EXCEPT ALL` answer Spark's rows in every supported shape; every `MINUS ALL` cell refuses with `UnsupportedOperationException: MINUS ALL not implemented` (43 non-gap cells plus `error / column-type / MINUS ALL` via `make_array`, while `error / column-count / MINUS ALL` raises `AnalysisException` before the refusal); and the three pre-existing unsupported native shapes (CTAS into Iceberg, persistent view, `MERGE`) refuse for all three operators as on main.

**Residue:** a column count or type mismatch raises `AnalysisException`, Spark's class, with DataFusion's planner text and no `[NUM_COLUMNS_MISMATCH]` (`42826`) or `[INCOMPATIBLE_COLUMN_TYPE]` (`42825`) condition, as distinct `INTERSECT` / `EXCEPT` and `UNION` do on main.

## The candidate

Candidate B, Spark's own `RewriteIntersectAll` / `RewriteExceptAll`, as the analyzer rule `setop_all_multiset` right after `struct_setop_names` (`crates/repark-core/src/pre_execute/setop_all.rs`). The plan of `l INTERSECT ALL r`:

```text
Projection: the join's schema (casts back to the left types where coercion widened a key)
  Unnest: copies
    Projection: keys, range(0, CASE WHEN left < right THEN left ELSE right END) AS copies
      Aggregate: groupBy=[keys], aggr=[sum(left_tag) AS left, sum(right_tag) AS right]
        Union
          Projection: l keys, 1 AS left_tag, 0 AS right_tag
          Projection: r keys, 0 AS left_tag, 1 AS right_tag
```

`EXCEPT ALL` tags `1` and `-1` and keeps `CASE WHEN sum > 0 THEN sum ELSE 0 END` copies. A group one side lacks keeps zero copies, so no filter runs (a filter's statistics over list-of-struct keys tripped a DataFusion interval assertion). Keys group once by the same aggregate the distinct forms use, so NULLs, both zeros and every NaN are one key through the signed-zero key seat, and the output carries the normalised key, as Spark's aggregate does (`-0.0, -0.0, -0.0 INTERSECT ALL 0.0, 0.0` is two `0.0` rows in Spark). Cost on 1M rows a side (one BIGINT column, 400,000 and 600,000 distinct values, warm median of three `SELECT count(*)` runs, cores 32-47):

| query | Spark 4.1.2 | head | main (as user SQL) |
|---|---|---|---|
| `intersect-all` | 0.698 s (685714 rows) | 0.053 s (685714 rows) | n/a |
| `except-all` | 0.647 s (314286 rows) | 0.049 s (314286 rows) | n/a |
| `intersect` | 1.336 s (400000 rows) | 0.055 s (400000 rows) | 0.058 s (400000 rows) |
| `except` | 0.977 s (0 rows) | 0.055 s (0 rows) | 0.055 s (0 rows) |
| `candidate-a-intersect` | n/a | 0.27 s (685714 rows) | 0.266 s (685714 rows) |
| `candidate-a-except` | n/a | 0.263 s (314286 rows) | 0.287 s (314286 rows) |
| `candidate-b-intersect` | n/a | refused | refused |
| `candidate-b-except` | n/a | refused | refused |

## Red and green

Six setop pin files (`test_sql_setop_all_multiset_1.py`, `test_struct_set_ops.py`, `test_values_mixed_types.py`, `test_examples_dataframe_a.py`, `test_examples_dataframe_b.py`, `test_df_easy.py`), `pytest -n 4`:

- `head4` (`2ce51f04`): **1433 passed, 0 failed** (0 guard failures).
- `head3` (`1b1a00b3`): **9 failed, 1424 passed** (all 9 are Delta-2 finding pins; 0 guard failures).
- `head2` (`0add094c`): **54 failed, 1379 passed** (43 native `MINUS ALL` + 11 Delta-2 finding pins; 0 guard failures).
- `mutant` (`f6376a1b`): **416 failed, 1017 passed** (0 guard failures; EX-DF-7 kills the mutant via asymmetric multiplicity).
- `main` (`e80e6bc8`): **660 failed, 773 passed** (0 guard failures).

The mutant (`f6376a1b`, not on the branch) drops the multiplicity; the pins that catch it are the type, edge, shape, spelling, fragment and DataFrame cells of `test_sql_setop_all_multiset_1.py`, EX-DF-3, EX-DF-7, and `test_df_easy.py::test_set_ops_and_cross_join`. The Rust seat tests (`setop_all/tests.rs`) fail without the rule in the same way.

## Delta 1 (owner ruling 2026-11-23: native door follows Trino, then DuckDB, then refuses)

`MINUS ALL` is spelled `EXCEPT ALL` only under the Databricks parser: the statement seat (the session's parser), `filter` strings (the frame's session parser, so a `repark.sql()` frame refuses), stored view bodies (the reading session's parser), and `F.expr` (always Spark grammar).

| native `repark.sql()` | main `e80e6bc8` | head2 `0add094c` | head3 `1b1a00b3` / head4 `2ce51f04` |
|---|---|---|---|
| `MINUS` (top, `INSERT`, CTAS) | `UnsupportedOperationException: MINUS not implemented`, nothing written | same | same |
| `MINUS ALL` (four spellings) | `UnsupportedOperationException`, SQL-SETOP-ALL-REFUSE-1 text, nothing written | answers `1, 1, 4`, `INSERT` stores it | `UnsupportedOperationException: This feature is not implemented: MINUS ALL not implemented`, nothing written |

Guard pins, Spark 4.1.2 (ANSI on and off answer alike), equal on main and head in both modes (not red on main):

| join (SQL `<=>` and DataFrame `eqNullSafe`) | semi | anti | type |
|---|---|---|---|
| one column | `1, 1, NULL, NULL` | `2` | `a:int` |
| two columns, keys cover every column | `(1,x), (1,x), (NULL,NULL), (NULL,NULL)` | `(2,y)` | `a:int,b:string` |
| DECIMAL left, DOUBLE right | `1.10, 1.10, NULL` | `2.00` | `a:decimal(10,2)` |

Skew, 1M rows over 10 keys (100,000 copies/key) vs 600,000 rows, warm median of three `SELECT count(*)` runs: head `INTERSECT ALL` 0.016 s (600,000 rows), `EXCEPT ALL` 0.015 s (400,000), distinct `INTERSECT` 0.021 s, `EXCEPT` 0.019 s; Spark 4.1.2 0.380 s, 0.227 s, 0.516 s, 0.463 s.

## Delta 2 (verifier findings F-1 through F-5, B-1 through B-5, C-1)

- **F-1 (parser-verified `MINUS ALL`):** `minus_all_spans` verifies `SetExpr::SetOperation { op: Minus, set_quantifier: All }` with `sqlparser` (`DatabricksDialect`), preserving comments and spacing while leaving identifiers (`minus` column/table/CTE, `all` alias, string literals, quoted `` `minus` ``) and unparseable queries untouched across statements, stored view bodies, and `filter` strings.
- **F-2 (untyped `NULL` left column):** `rewrite_multiset` emits `lit(ScalarValue::Null)` when the output field is `DataType::Null`, matching RePark's distinct twin schema (`void` on Spark door, `null` on native door; Spark widens to the right column's type).
- **F-3 (`MAP` on either side and nested):** `map_ordering.rs` inspects both `join.left` and `join.right` and recursively walks `Struct` and `List*` fields for `DataType::Map` on multiset set operations, refusing with `[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE]`; distinct forms keep main's answers.
- **F-4 (`F.expr` boundary):** no code change; `F.expr` belongs to `pyspark.sql.functions` and is Spark grammar even when passed to a native frame. Pinned and recorded in the card, parity doc, and `crates/repark-python/src/column/map.md`.
- **F-5 (lossy round-trip cast refusal):** `rewrite_multiset` refuses at analysis time (before any write) with `AnalysisException` naming `SETOP-MIXED-TYPE-COERCION-1` (`task/roadmap/mid-term/cards-2026-11-23/setop-mixed-type-coercion-1-card-2026-11-23.md`) whenever casting a coerced key back to its output type is not lossless (`STRING` vs numeric/`DATE`/`TIMESTAMP`/`BOOLEAN`/`BINARY`, lossy int/float/decimal/date/timestamp pairs, and nested struct/list fields). Exact widenings (`INT`/`BIGINT`, `FLOAT`/`DOUBLE`, Spark-door `DECIMAL`/`DOUBLE`) keep answering; distinct forms stay unchanged.

Up: [../map.md](../map.md).
