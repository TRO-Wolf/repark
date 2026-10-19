# map — python/repark/tests/sql

## Purpose

The SQL front ends: Spark-dialect grammar and literals, the native `repark.sql()` door, `SET` / `RESET`, statement routing and eagerness, identifier quoting, table-valued functions, subquery and deep-expression safety, and the fuzz battery.

Part of the FACADE-TESTS-LAYOUT-1 (2026-10-19) layout; the directory rules live in
[../map.md](../map.md) "Layout". A new test goes in the directory of the subsystem it
pins.

## Contents

- [test_deep_expr_build_small_stack_1.py](test_deep_expr_build_small_stack_1.py) — DEEP-FILTER-CHAIN-CRASH-1 CI-segv fold — deep expression builds never overflow a caller stack.
- [test_deep_filter_chain_crash_1.py](test_deep_filter_chain_crash_1.py) — DEEP-FILTER-CHAIN-CRASH-1 — deep operator chains answer instead of crashing.
- [test_deep_reverify_1.py](test_deep_reverify_1.py) — DEEP-FILTER-CHAIN-CRASH-1 re-verify fold — VD2 pins on main, small stacks, and GC.
- [test_deep_subquery_expression_1.py](test_deep_subquery_expression_1.py) — DEEP-FILTER-CHAIN-CRASH-1 verifier fold — subquery plans and deep expressions.
- [test_f1_sql_expander.py](test_f1_sql_expander.py) — Free-SQL bare-name expander — Path A statement forms via resolve_table_name SSOT.
- [test_fuzz_smoke.py](test_fuzz_smoke.py) — R-SQL-FUZZER — always-on smoke battery (200 queries, fixed seed 42).
- [test_qi1_idents.py](test_qi1_idents.py) — QI1 — quoting / path-escape single-source pins (CQ-006/007).
- [test_range_tvf_id_1.py](test_range_tvf_id_1.py) — RANGE-TVF-ID-1 — the range() table function names its column id like Spark.
- [test_range_tvf_id_2.py](test_range_tvf_id_2.py) — RANGE-TVF-ID-2 - range() argument edge cases answer Spark 4.1.2.
- [test_scalar_subquery_sort_pin.py](test_scalar_subquery_sort_pin.py) — DF 54.1 regression pin: ORDER BY must survive an uncorrelated scalar subquery.
- [test_spark_sql_grammar_1.py](test_spark_sql_grammar_1.py) — SPARK-SQL-GRAMMAR-1 door pins — Spark operators, keywords and type names.
- [test_sql_alias.py](test_sql_alias.py) — Q1 re-home pins — ``import repark.sql`` fails; ``repark.sql()`` is the ANSI door.
- [test_sql_dml_eager.py](test_sql_dml_eager.py) — Bare `spark.sql` DML executes eagerly (F-BR-2) — PySpark parity.
- [test_sql_passthrough_parity.py](test_sql_passthrough_parity.py) — Adversarial Spark-parity corpus for the raw ``spark.sql()`` passthrough (AR-WG-SQL, C-AR-005).
- [test_sql_set_door_1.py](test_sql_set_door_1.py) — SQL ``SET`` / ``RESET`` door pins — SQL-SET-DOOR-1 (oracle ``fixtures-batch1.json`` BTZ5-0..19).
- [test_sqp_1_string_literals.py](test_sqp_1_string_literals.py) — SQP-1 facade controls — Spark string-literal escapes and ``CAST … AS BINARY``.
- [test_string_literal_escape_1.py](test_string_literal_escape_1.py) — STRING-LITERAL-ESCAPE-1 facade pins — SQL string literals unescape as Spark does.
- [test_three_valued_logic_parity.py](test_three_valued_logic_parity.py) — Three-valued logic differential corpus — value AND Arrow type AND nullability.
- [_record_range_tvf_id_1.py](_record_range_tvf_id_1.py) — Record the range() table-function Spark oracles - ID-1 forms and ID-2 edge cases.
- [_record_tvl_goldens.py](_record_tvl_goldens.py) — Record mode for the three-valued-logic corpus — re-derive every Spark half from live PySpark.
- [range_tvf_id_1](range_tvf_id_1/map.md) — RANGE-TVF-ID oracle recordings (own `map.md`).

## Pointers

- Up: [../map.md](../map.md)
