# task/wo/sql-setop-all-multiset-1/evidence/ — SQL-SETOP-ALL-MULTISET-1 measurement evidence

## Provenance

- **RePark:** main `e80e6bc8` and the branch head, release wheels (maturin 1.14.1, Python 3.12); the
  mutant is head with `insert_multiset_set_operations` not seating the rule, so the ALL forms plan
  DataFusion's semi and anti join: the wrong answer main's refusal guarded.
- **Spark:** PySpark 4.1.2 with `iceberg-spark-runtime-4.1_2.13` 1.11.0, Java 17, `local[2]`, a Hadoop
  catalog `ice`, session zone UTC, ANSI on and off; recorded into
  `python/repark/tests/sql/setop_all_multiset_1_spark_oracle.json` by
  `python _setop_all_multiset_1_cells.py <warehouse> <fixture>` (the persistent views live in
  `spark_catalog.default`: a Hadoop catalog has no views).
- **Doors:** `spark.sql` over a memory catalog `ice` in both ANSI modes; `repark.sql()` over a native
  session's memory catalog, against Spark's ANSI-on answer. The writes are read back cold, by a new
  session loading the table folder.

## Domain table

[domain-table.md](domain-table.md) holds every cell. The count by group:

| package | mode | group | verdict | cells |
|---|---|---|---|---|
| head | false | control | EQUAL | 6 |
| head | false | dataframe | EQUAL | 52 |
| head | false | edge | EQUAL | 15 |
| head | false | error | ERR-CLASS | 6 |
| head | false | fragment | EQUAL | 12 |
| head | false | shape | EQUAL | 36 |
| head | false | spelling | EQUAL | 9 |
| head | false | type | EQUAL | 78 |
| head | native | control | EQUAL | 6 |
| head | native | edge | EQUAL | 15 |
| head | native | error | ERR-CLASS | 6 |
| head | native | shape | EQUAL | 27 |
| head | native | shape | REFUSED | 9 |
| head | native | spelling | EQUAL | 9 |
| head | native | type | EQUAL | 78 |
| head | true | control | EQUAL | 6 |
| head | true | dataframe | EQUAL | 52 |
| head | true | edge | EQUAL | 15 |
| head | true | error | ERR-CLASS | 6 |
| head | true | fragment | EQUAL | 12 |
| head | true | shape | EQUAL | 36 |
| head | true | spelling | EQUAL | 9 |
| head | true | type | EQUAL | 78 |
| main | false | control | EQUAL | 6 |
| main | false | dataframe | REFUSED | 52 |
| main | false | edge | REFUSED | 15 |
| main | false | error | ERR-DIFF | 6 |
| main | false | fragment | REFUSED | 12 |
| main | false | shape | REFUSED | 36 |
| main | false | spelling | REFUSED | 9 |
| main | false | type | REFUSED | 78 |
| main | native | control | EQUAL | 6 |
| main | native | edge | REFUSED | 15 |
| main | native | error | ERR-DIFF | 6 |
| main | native | shape | REFUSED | 36 |
| main | native | spelling | REFUSED | 9 |
| main | native | type | REFUSED | 78 |
| main | true | control | EQUAL | 6 |
| main | true | dataframe | REFUSED | 52 |
| main | true | edge | REFUSED | 15 |
| main | true | error | ERR-DIFF | 6 |
| main | true | fragment | REFUSED | 12 |
| main | true | shape | REFUSED | 36 |
| main | true | spelling | REFUSED | 9 |
| main | true | type | REFUSED | 78 |
| mutant | false | control | EQUAL | 6 |
| mutant | false | dataframe | WRONG | 52 |
| mutant | false | edge | EQUAL | 12 |
| mutant | false | edge | WRONG | 3 |
| mutant | false | error | ERR-CLASS | 6 |
| mutant | false | fragment | EQUAL | 2 |
| mutant | false | fragment | WRONG | 10 |
| mutant | false | shape | EQUAL | 4 |
| mutant | false | shape | WRONG | 32 |
| mutant | false | spelling | WRONG | 9 |
| mutant | false | type | EQUAL | 34 |
| mutant | false | type | WRONG | 44 |
| mutant | native | control | EQUAL | 6 |
| mutant | native | edge | EQUAL | 12 |
| mutant | native | edge | WRONG | 3 |
| mutant | native | error | ERR-CLASS | 6 |
| mutant | native | shape | EQUAL | 4 |
| mutant | native | shape | REFUSED | 9 |
| mutant | native | shape | WRONG | 23 |
| mutant | native | spelling | WRONG | 9 |
| mutant | native | type | EQUAL | 34 |
| mutant | native | type | WRONG | 44 |
| mutant | true | control | EQUAL | 6 |
| mutant | true | dataframe | WRONG | 52 |
| mutant | true | edge | EQUAL | 12 |
| mutant | true | edge | WRONG | 3 |
| mutant | true | error | ERR-CLASS | 6 |
| mutant | true | fragment | EQUAL | 2 |
| mutant | true | fragment | WRONG | 10 |
| mutant | true | shape | EQUAL | 4 |
| mutant | true | shape | WRONG | 32 |
| mutant | true | spelling | WRONG | 9 |
| mutant | true | type | EQUAL | 34 |
| mutant | true | type | WRONG | 44 |

Main refuses every multiset cell. The mutant answers the old wrong multiset: `INTERSECT ALL` keeps every
left duplicate and `EXCEPT ALL` drops every copy. Head answers Spark's rows in every cell on the Spark
door in both ANSI modes, output types included, and in every native cell except the three shapes the
native door has no statement for (CTAS into Iceberg, a persistent view, `MERGE`), which refuse on main's
native door for every source.

**Residue:** a column count or type mismatch raises `AnalysisException`, Spark's class, with
DataFusion's planner text and no `[NUM_COLUMNS_MISMATCH]` (`42826`) or `[INCOMPATIBLE_COLUMN_TYPE]`
(`42825`) condition, as the distinct `INTERSECT` / `EXCEPT` and `UNION` do on main. The error is raised
by DataFusion's planner and type coercion before the rewrite, for every set operation.

## The candidate

Candidate B, Spark's own `RewriteIntersectAll` / `RewriteExceptAll`, as the analyzer rule
`setop_all_multiset` right after `struct_setop_names` (`crates/repark-core/src/pre_execute/setop_all.rs`).
The plan of `l INTERSECT ALL r`:

```text
Projection: the join's schema (casts back to the left types where coercion widened a key)
  Unnest: copies
    Projection: keys, range(0, CASE WHEN left < right THEN left ELSE right END) AS copies
      Aggregate: groupBy=[keys], aggr=[sum(left_tag) AS left, sum(right_tag) AS right]
        Union
          Projection: l keys, 1 AS left_tag, 0 AS right_tag
          Projection: r keys, 0 AS left_tag, 1 AS right_tag
```

`EXCEPT ALL` tags `1` and `-1` and keeps `CASE WHEN sum > 0 THEN sum ELSE 0 END` copies. A group one
side lacks keeps zero copies, so no filter runs (a filter's statistics over list-of-struct keys tripped
a DataFusion interval assertion). Why B: the keys group once, by the same aggregate the distinct forms
use, so NULLs, both zeros and every NaN are one key through the signed-zero key seat, and the output
carries the normalised key, as Spark's aggregate does (`-0.0, -0.0, -0.0 INTERSECT ALL 0.0, 0.0` is two
`0.0` rows in Spark). Candidate A numbers the rows with a window over every column and runs the
distinct operator over `(columns, n)`: a window sort of both sides by every column, then a distinct and
a hash join on one key more; B is one hash aggregate over both sides and a replication. Cost on 1M rows a side (one BIGINT column, 400,000 and
600,000 distinct values, the warm median of three runs of `SELECT count(*)`, cores 32-47):

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

Pins (`test_sql_setop_all_multiset_1.py`, the struct set-operation, mixed-`VALUES`, EX-DF-3, EX-DF-7 and `test_df_easy.py` files), xdist `-n 4`:

- main: n/a
- mutant: n/a
- head2: n/a


The mutant (`f6376a1b`, not on the branch) drops the multiplicity; the pins that catch it are the type,
edge, shape, spelling, fragment and DataFrame cells of `test_sql_setop_all_multiset_1.py`, the EX-DF-3
and EX-DF-7 pins and `test_df_easy.py::test_set_ops_and_cross_join`. The Rust seat tests
(`setop_all/tests.rs`) fail without the rule in the same way.

Up: [../map.md](../map.md).
