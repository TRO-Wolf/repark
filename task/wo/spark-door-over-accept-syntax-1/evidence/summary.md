# SPARK-DOOR-OVER-ACCEPT-SYNTAX-1: measurement evidence

## Provenance

- **RePark:** main `14d2404a6` (package built from `57c92b41`, which differs only by one card row) and
  the branch head `b1cf4d0cc` (ruling 1, option C; first head `d656cbaad`), built in release.
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
| `OVER-ACCEPT` -> `REFUSE~CLASS` (refuses; class differs, see below) | 120 |
| `NAME` -> `EQ` (`drop temporary`, ``1e2`foo` ``) | 20 |
| `REFUSE~CLASS` -> `EQ` (`* EXCLUDE` on `F.expr`, `DELETE` with a subquery) | 4 |
| `OVER-REFUSE` -> `EQ` (`SELECT 1e2foo FROM (SELECT 7 AS 1e2foo) t`) | 4 |
| `NAME` -> `OVER-REFUSE` (`F.expr` takes no alias; same as `F.expr("1e2 AS foo")` on main) | 4 |
| `EQ` -> anything else | 0 |
| unchanged | 537 |

The native door's 14 control cells are identical on main and head.

## Ruling 1 (orchestrator, 2026-11-24): option C for the struct seat

The first head (`0988c56d5`) moved five cells that equal Spark on main to a refusal, because every
sibling planned and every constant sibling evaluated: `named_struct('a', 1, 'b', 'x' + 1).a` with ANSI
off on `spark.sql`, views, `F.expr` and `selectExpr`, and `F.expr("named_struct('a', 1, 'b', 1 / 0).a")`
with ANSI off. Option C refuses only where Spark's reason applies:

1. an unknown name in a sibling refuses (the planner resolves `((sibling) IS NULL)` without typing it);
2. a constant sibling stays for evaluation only with ANSI on;
3. a sibling that fails to type for any other reason drops every sibling, so the access answers as
   main does; a function-signature failure (`abs(1, 2)`) still refuses.

Re-measured on `b1cf4d0cc` against `0988c56d5`: the five cells answer `1` again (equal to Spark); their
ANSI-on spellings are `'x' + 1` with ANSI on (Spark `CAST_INVALID_INPUT`, main `1`, head `1`: unchanged
from main, a RePark coercion gap) and the `1 / 0` sibling with ANSI on (Spark `DIVIDE_BY_ZERO`, main `1`, head `DIVIDE_BY_ZERO`,
unchanged from `0988c56d5`) on every door. No other cell moved, and the card's struct cells still refuse: `UNRESOLVED_COLUMN` in both modes
(`selectExpr` carries the class) and `CAST_INVALID_INPUT` with ANSI on.

The two root gaps are present on main and are carded: RePark cannot plan `'x' + 1` (string arithmetic)
in either ANSI mode, and `F.expr("1 / 0")` raises `DIVIDE_BY_ZERO` with ANSI off.

The 2026-08-11 G3-E8 adjacent negative `g3e8_fromless_non_subquery_delete_still_executes`
([ledger](../../../ledgers/archive/2026-08/2026-08-11-g3e8-guard-ledger.md) row 82) is superseded by
this card: the FROM-less `DELETE` refuses with Spark's `PARSE_SYNTAX_ERROR`, pinned by
`g3e8_fromless_delete_refuses_before_any_row_moves`.

## Refusal classes that still differ

The door's own classes for these refusals are kept, as the brief directs where the door does not map
Spark's class: an unresolved identifier (`1e2foo`, a struct sibling) is `AnalysisException: Schema error:
No field named …` on `spark.sql` and `[UNRESOLVED_COLUMN.WITH_SUGGESTION]` on `selectExpr`, as for
`1e2L` on main; a field access on a scalar (`1e2.foo`) is the door's
`Cannot access field … is not Struct` / `Dot access not supported`, as for `(1e2).foo` on main. The
`DELETE`, `DROP TEMPORARY` and `* EXCLUDE` refusals carry Spark's `PARSE_SYNTAX_ERROR` text.

## Pins and mutants

- Facade pin `python/repark/tests/sql/test_spark_door_over_accept_syntax_1.py`: 686 door cells from
  the Spark-recorded oracle (209 fixed, 477 neighbours). Head: 687 passed. Main: 209 failed, 478
  passed; every failure is a fixed cell, for example
  `D01 dml: Spark refuses (PARSE_SYNTAX_ERROR), got ok … after [[2, 20.0], [3, 30.0]]`.
- Mutants (each restores the seat's main acceptance; `cargo test -p repark-spark --lib`):

| Mutant | Caught by |
|---|---|
| exponent fallback | `spark_rewrites::tests::exponent_then_identifier_characters_reads_as_one_identifier`, `…exponent_then_a_period_keeps_the_field_access`, `…exponent_typed_suffixes_and_spaced_aliases_keep_their_rewrites` |
| struct siblings dropped (re-run on `b1cf4d0cc`) | `spark_rewrites::tests::struct_field_access_passes_every_sibling_to_the_display_wrapper`, `tests::spark_dialect::struct_field_access_resolves_every_sibling_and_evaluates_constant_ones`, `tests::spark_dialect::struct_field_access_answers_past_siblings_spark_does_not_refuse` |
| `* EXCLUDE` to `* EXCEPT` | `spark_rewrites::tests::wildcard_exclude_with_a_column_list_refuses_like_spark`, `tests::spark_dialect::wildcard_exclude_refuses_and_wildcard_except_reads` |
| FROM-less `DELETE` inserts `FROM` | `tests::dml::g3e8_fromless_delete_refuses_before_any_row_moves`, `tests::spark_dialect::fromless_delete_refuses_with_the_spark_parse_error` |
| every `DROP TEMPORARY` loses `TEMPORARY` | `spark_rewrites::tests::drop_temporary_rewrites_only_a_leading_drop_temporary_function` |

The facade pin's fixed cells are the same five acceptances, all present on main.
