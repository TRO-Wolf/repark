# SPARK-DOOR-OVER-ACCEPT-SYNTAX-1: measurement evidence

## Provenance

- **RePark:** main `14d2404a6` (package built from `57c92b41`, which differs only by one card row) and
  the branch head `d656cbaad` (product commit), built in release.
- **Spark:** live PySpark 4.1.2 (`local[2]`, Java 17, `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`,
  local hadoop catalog), both ANSI modes, session zone UTC.
- **Scripts:** [probe_over_accept.py](probe_over_accept.py) (122 cells: 91 expression cells on
  `spark.sql`, temp-view bodies, `F.expr`, `selectExpr` and `filter(str)`; 7 `DROP` statements; 24
  Iceberg `DELETE` / `UPDATE` / `MERGE` cells; 7 native-door controls), run once per engine and ANSI
  mode; [compare_over_accept.py](compare_over_accept.py) (the table);
  [summarize_moves.py](summarize_moves.py) (moves and remaining differences);
  [build_pin_oracle.py](build_pin_oracle.py) (the facade pin's oracle);
  [apply_mutant.py](apply_mutant.py) (one mutant per rewrite).
- **Table:** [domain-table.md](domain-table.md), 750 door cells. **Seats:** [seat-inventory.md](seat-inventory.md).

## Result

| main -> head | cells |
|---|---|
| `OVER-ACCEPT` -> `EQ` | 61 |
| `OVER-ACCEPT` -> `REFUSE~CLASS` (refuses; class differs, see below) | 124 |
| `NAME` -> `EQ` (`drop temporary`, ``1e2`foo` ``) | 20 |
| `REFUSE~CLASS` -> `EQ` (`* EXCLUDE` on `F.expr`, `DELETE` with a subquery) | 4 |
| `OVER-REFUSE` -> `EQ` (`SELECT 1e2foo FROM (SELECT 7 AS 1e2foo) t`) | 4 |
| `EQ` -> `OVER-REFUSE` (**ruling needed**, below) | 5 |
| `NAME` -> `OVER-REFUSE` (`F.expr` takes no alias; same as `F.expr("1e2 AS foo")` on main) | 4 |
| unchanged | 528 |

The native door's 14 control cells are identical on main and head.

## The five cells that equal Spark on main and refuse at head

| Cell | Spark | Main | Head | Cause |
|---|---|---|---|---|
| `named_struct('a', 1, 'b', 'x' + 1).a`, ANSI off, on `spark.sql`, view, `F.expr`, `selectExpr` | `1` | `1` | `Cannot coerce arithmetic expression Utf8 + Int64` | the sibling now plans; RePark cannot plan `'x' + 1` with ANSI off anywhere (`SELECT 'x' + 1` refuses on main) |
| `F.expr("named_struct('a', 1, 'b', 1 / 0).a")`, ANSI off | `1` | `1` | `DIVIDE_BY_ZERO` | the constant sibling now evaluates; `F.expr("1 / 0")` raises with ANSI off on main (`spark.sql` and `selectExpr` answer `NULL`) |

Spark analyses every sibling and folds the constant ones, so a faithful splice must plan them.
Each moved cell's sibling expression refuses on its own on main.

## Refusal classes that still differ

The door's own classes for these refusals are kept, as the brief directs where the door does not map
Spark's class: an unresolved identifier (`1e2foo`, a struct sibling) is `AnalysisException: Schema error:
No field named …` on `spark.sql` and `[UNRESOLVED_COLUMN.WITH_SUGGESTION]` on `selectExpr`, as for
`1e2L` on main; a field access on a scalar (`1e2.foo`) is the door's
`Cannot access field … is not Struct` / `Dot access not supported`, as for `(1e2).foo` on main. The
`DELETE`, `DROP TEMPORARY` and `* EXCLUDE` refusals carry Spark's `PARSE_SYNTAX_ERROR` text.

## Pins and mutants

- Facade pin `python/repark/tests/sql/test_spark_door_over_accept_syntax_1.py`: 685 door cells from
  the Spark-recorded oracle (213 fixed, 472 neighbours). Head: 686 passed. Main: 213 failed, 473
  passed; every failure is a fixed cell (E 82, S 67, T 22, W 16, D 14, U 12), for example
  `D01 dml: Spark refuses (PARSE_SYNTAX_ERROR), got ok … after [[2, 20.0], [3, 30.0]]`.
- Mutants (each restores the seat's main acceptance; `cargo test -p repark-spark --lib`):

| Mutant | Caught by |
|---|---|
| exponent fallback | `spark_rewrites::tests::exponent_then_identifier_characters_reads_as_one_identifier`, `…exponent_then_a_period_keeps_the_field_access`, `…exponent_typed_suffixes_and_spaced_aliases_keep_their_rewrites` |
| struct siblings dropped | `spark_rewrites::tests::struct_field_access_passes_every_sibling_to_the_display_wrapper`, `tests::spark_dialect::struct_field_access_resolves_every_sibling_and_evaluates_constant_ones` |
| `* EXCLUDE` to `* EXCEPT` | `spark_rewrites::tests::wildcard_exclude_with_a_column_list_refuses_like_spark`, `tests::spark_dialect::wildcard_exclude_refuses_and_wildcard_except_reads` |
| FROM-less `DELETE` inserts `FROM` | `tests::dml::g3e8_fromless_delete_refuses_before_any_row_moves`, `tests::spark_dialect::fromless_delete_refuses_with_the_spark_parse_error` |
| every `DROP TEMPORARY` loses `TEMPORARY` | `spark_rewrites::tests::drop_temporary_rewrites_only_a_leading_drop_temporary_function` |

The facade pin's fixed cells are the same five acceptances, all present on main.
