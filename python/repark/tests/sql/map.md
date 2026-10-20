# map — python/repark/tests/sql

## Purpose

The SQL front ends: Spark-dialect grammar and literals, the native `repark.sql()` door, `SET` / `RESET`, statement routing and eagerness, identifier quoting, table-valued functions, subquery and deep-expression safety, and the fuzz battery.

Part of the FACADE-TESTS-LAYOUT-1 (2026-10-19) layout; the directory rules live in
[../map.md](../map.md) "Layout". A new test goes in the directory of the subsystem it
pins.

## Contents

- [test_deep_expr_build_small_stack_1.py](test_deep_expr_build_small_stack_1.py) — DEEP-FILTER-CHAIN-CRASH-1 CI-segv fold — deep expression builds never overflow a caller stack. Two shapes (caller thread, 8 MiB thread), each in its own interpreter (DEEP-BATTERY-SPLIT-1).
- [test_deep_filter_chain_crash_1.py](test_deep_filter_chain_crash_1.py) — DEEP-FILTER-CHAIN-CRASH-1 — deep operator chains answer instead of crashing. Nine shapes, one interpreter per test; no `xdist_group` (DEEP-BATTERY-SPLIT-1).
- [test_deep_reverify_1.py](test_deep_reverify_1.py) — DEEP-FILTER-CHAIN-CRASH-1 re-verify fold — VD2 pins on main, small stacks, and GC. The four main-thread shapes run one interpreter each; the 256 KiB, 512 KiB and GC fixtures were one shape each and stay (DEEP-BATTERY-SPLIT-1).
- [test_deep_subquery_expression_1.py](test_deep_subquery_expression_1.py) — DEEP-FILTER-CHAIN-CRASH-1 verifier fold — subquery plans and deep expressions. Six shapes, one interpreter per test; `xdist_group` only on the two DataFrame-door refusals (DEEP-BATTERY-SPLIT-1).
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

## Path expressions

The move put every file here one level below the old flat directory. These files had a
path expression repointed in the same unit and no other line changed:

| file | change |
|---|---|
| `_record_range_tvf_id_1.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `_record_tvl_goldens.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `test_fuzz_smoke.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_three_valued_logic_parity.py` | names its own or a sibling's new path in a docstring or recorded string. |

## DEEP-BATTERY-SPLIT-1 (2026-10-19)

The four deep batteries ran one subprocess per module fixture that drove every shape in
sequence and returned one JSON dict. Each test now launches its own shape: a shared preamble
(session plus the 50-row `base`), the shape's original lines, and the JSON epilogue, through
the same `sys.executable -c` form and the same per-subprocess timeout. Every assertion keeps its
key, value and error class. A shape that reads a view registers it itself (`t` for the 5,000-term
OR; the C-005 shape keeps scalar and `IN` in one interpreter because `IN` reads `deepv`). Ledger
reading: `deep-filter-chain-crash-1` C-001..C-012, C-014 and C-015 each ask for an
isolated-interpreter pin per clause, and C-010/C-011 already called two tests sharing one
interpreter "two isolated-interpreter pins", so isolation means out of the pytest process, not
one interpreter per battery. `test_deep_reverify_1.py`'s 256 KiB, 512 KiB and GC fixtures were
already one shape each and are unchanged.

`xdist_group` marks. A module-wide group would put every split shape back on one worker, so
both module marks go. `--dist loadgroup` queues multi-test groups first and single tests in
collection order; `sql/` starts at test 11,987 of 20,704, so an ungrouped 20,000-term OR shape
(about 290 s alone; its time is the Python-door `|` build, 2.3 s at 2,500 terms, 11 s at 5,000,
58 s at 10,000) starts about 100 s into a 16-worker run and ends last. One group pairs it with
the 2,000-deep `+1` refusal (1.7 s), the other DataFrame-door expression refusal, so the pair
starts first. `slow` stays on all four modules.

Measured 2026-10-19 on the `ci` profile, `taskset -c 0-25`, one-minute load 2.7 to 4.4 at each
start (the box's other work raised it to 10 to 17 during the full runs):

| run | before | after |
|---|---|---|
| the two batteries, `-n 8` | 398.2 s (fixtures 338.0 s and 156.9 s) | 307.5 s (20,000-term OR 286.2 s; next 67.7 s) |
| `test_deep_reverify_1.py` plus `test_deep_expr_build_small_stack_1.py`, `-n 8` | 56.6 s (the main fixture built on four workers, 20.7 s each) | 37.3 s |
| full facade suite, `-n 16` | 392.5 s, 20065 passed / 1 failed / 493 skipped / 151 xfailed | 355.2 s, 20066 passed / 493 skipped / 151 xfailed |

The before failure is `perf/test_perf_unpivot_1.py::test_stack_is_linear_in_columns`, a timing
exponent (1.415 against 1.1) under the load spike; that file is not in this change. With no
group at all the after suite ran 426.4 s, which is why the one group stays. The passed count is
unchanged because every shape already had its own test.

## Pointers

- Up: [../map.md](../map.md)
