# SPARK-DOOR-OVER-ACCEPT-SYNTAX-1: measurement evidence

## Provenance

- **RePark:** main `14d2404a6` (package built from `57c92b41`, which differs only by one card row) and
  the branch head `6a35e8ec5` (delta 1; the struct seat back on main), built in release.
- **Spark:** live PySpark 4.1.2 (`local[2]`, Java 17, `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`,
  local hadoop catalog), both ANSI modes, session zone UTC.
- **Scripts:** [probe_over_accept.py](probe_over_accept.py) (141 cells: 107 expression cells on
  `spark.sql`, temp-view bodies, `F.expr`, `selectExpr` and `filter(str)`; 9 `DROP` statements; 25
  Iceberg `DELETE` / `UPDATE` / `MERGE` cells; 7 native-door controls), run once per engine and ANSI
  mode; [compare_over_accept.py](compare_over_accept.py) (the table);
  [summarize_moves.py](summarize_moves.py) (moves and remaining differences);
  [build_pin_oracle.py](build_pin_oracle.py) (the facade pin's oracle);
  [apply_mutant.py](apply_mutant.py) (one mutant per rewrite).
- **Table:** [domain-table.md](domain-table.md), 876 door cells. **Seats:** [seat-inventory.md](seat-inventory.md).

## Result

| main -> head (probe) | cells |
|---|---|
| `OVER-ACCEPT` -> `EQ` | 44 |
| `OVER-ACCEPT` -> `REFUSE~CLASS` (refuses; class differs, see below) | 72 |
| `NAME` -> `EQ` (`drop temporary`, ``1e2`foo` ``) | 20 |
| `REFUSE~CLASS` -> `EQ` (`* EXCLUDE` on `F.expr`, `DELETE` with a subquery) | 4 |
| `OVER-REFUSE` -> `EQ` (`SELECT 1e2foo FROM (SELECT 7 AS 1e2foo) t`) | 4 |
| `NAME` -> `OVER-REFUSE` (`F.expr("drop temporary")` and ``F.expr("1e2`foo`")``, both modes: `F.expr` takes no alias, as `F.expr("1e2 AS foo")` on main) | 4 |
| `EQ` -> anything else | 0 |
| unchanged | 728 |

The native door's 14 control cells are identical on main and head. The struct seat is main's, so its
card cells stay over-accepting (`card OVER-ACCEPT -> OVER-ACCEPT`, 13 door cells).

**The verifier's attack** (405 cells, every door, both modes; Spark, main `57c92b41`, head `6a35e8ec5`):
142 door cells fixed, 0 moved otherwise, and 2 tagged as regressions by its value rule: ``F.expr("1e2`foo`")``
in both modes, where main answered `100.0` under the wrong name ``1e2`foo` `` and head refuses (the
`F.expr` alias row above). No cell whose name, type and rows equal Spark on main differs on head.

## Delta 1 (orchestrator ruling, option B): the struct seat goes back to main

The first head planned every sibling and moved five cells main answers as Spark does. Ruling 1's option C
(resolve names, evaluate constants only with ANSI on, fall back on other typing failures) moved 240 more
door cells on the verifier's attack (siblings Spark coerces and RePark cannot type, lateral column
aliases, Spark built-ins RePark lacks, constants Spark never evaluates, a scalar-subquery sibling on the
DataFrame doors, and an `UPDATE` whose `SET` value Spark stores). Option C is superseded: `struct_field.rs`,
`spark_typed.rs`, `extension.rs`, `lib.rs` and `repark-python/src/column/expr_build.rs` equal main, and the
struct pins are gone. The probe now carries the verifier's shapes as neighbour cells (S30-S45, U07), green on
main and head.

`EXPLAIN DROP TEMPORARY FUNCTION IF EXISTS f`: Spark answers a one-row plan; main answered its two-column
`plan_type` / `plan` rows; the first head refused. The `DROP TEMPORARY` planner now looks past a leading
`EXPLAIN [EXTENDED | CODEGEN | COST | FORMATTED]`, so head answers as main. `EXPLAIN DROP TEMPORARY VIEW`
refuses with Spark's `PARSE_SYNTAX_ERROR` at `'VIEW'` (main answered).

The two root gaps are carded as present on main: RePark cannot plan `'x' + 1` (string arithmetic) in
either ANSI mode, and `F.expr("1 / 0")` raises `DIVIDE_BY_ZERO` with ANSI off.

The 2026-08-11 G3-E8 adjacent negative `g3e8_fromless_non_subquery_delete_still_executes`
([ledger](../../../ledgers/archive/2026-08/2026-08-11-g3e8-guard-ledger.md) row 82) is superseded by
this card: the FROM-less `DELETE` refuses with Spark's `PARSE_SYNTAX_ERROR`, pinned by
`g3e8_fromless_delete_refuses_before_any_row_moves`.

## Refusal classes that still differ

The door's own classes for these refusals are kept, as the brief directs where the door does not map
Spark's class: an unresolved identifier (`1e2foo`) is `AnalysisException: Schema error:
No field named …` on `spark.sql` and `[UNRESOLVED_COLUMN.WITH_SUGGESTION]` on `selectExpr`, as for
`1e2L` on main; a field access on a scalar (`1e2.foo`) is the door's
`Cannot access field … is not Struct` / `Dot access not supported`, as for `(1e2).foo` on main. The
`DELETE`, `DROP TEMPORARY` and `* EXCLUDE` refusals carry Spark's `PARSE_SYNTAX_ERROR` text.

## Pins and mutants

- Facade pin `python/repark/tests/sql/test_spark_door_over_accept_syntax_1.py`: 739 door cells from
  the Spark-recorded oracle (144 fixed, 595 neighbours, the verifier's struct shapes among them). Head:
  740 passed. Main: 144 failed, 596 passed; every failure is a fixed cell, for example
  `D01 dml: Spark refuses (PARSE_SYNTAX_ERROR), got ok … after [[2, 20.0], [3, 30.0]]`.
- Mutants (each restores the seat's main acceptance; `cargo test -p repark-spark --lib`):

| Mutant | Caught by |
|---|---|
| exponent fallback | `spark_rewrites::tests::exponent_then_identifier_characters_reads_as_one_identifier`, `…exponent_then_a_period_keeps_the_field_access`, `…exponent_typed_suffixes_and_spaced_aliases_keep_their_rewrites` |
| `* EXCLUDE` to `* EXCEPT` | `spark_rewrites::tests::wildcard_exclude_with_a_column_list_refuses_like_spark`, `tests::spark_dialect::wildcard_exclude_refuses_and_wildcard_except_reads` |
| FROM-less `DELETE` inserts `FROM` | `tests::dml::g3e8_fromless_delete_refuses_before_any_row_moves`, `tests::spark_dialect::fromless_delete_refuses_with_the_spark_parse_error` |
| every `DROP TEMPORARY` loses `TEMPORARY` | `spark_rewrites::tests::drop_temporary_rewrites_only_a_leading_drop_temporary_function` |

The facade pin's fixed cells are the same four acceptances, all present on main.
