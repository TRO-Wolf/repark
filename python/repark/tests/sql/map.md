# map — python/repark/tests/sql

## Purpose

The SQL front ends: Spark-dialect grammar and literals, the native `repark.sql()` door, `SET` / `RESET`, statement routing and eagerness, identifier quoting, table-valued functions, subquery and deep-expression safety, and the fuzz battery.

Part of the FACADE-TESTS-LAYOUT-1 (2026-10-19) layout; the directory rules live in
[../map.md](../map.md) "Layout". A new test goes in the directory of the subsystem it
pins.

## Contents

- [test_decimal_double_coercion_1.py](test_decimal_double_coercion_1.py) — DECIMAL-DOUBLE-UNION-COERCION-1 part 1 (both modes, Spark 4.1.2): DML, flips, signed zero, Q8, fuzz, writers, literals, K1/K2, `VALUES`, native door.
- [test_decimal_double_preimage_1.py](test_decimal_double_preimage_1.py) — part 1 delta: DOUBLE constants vs decimal edge rows, 8 operators, plan shape, `IN`/`BETWEEN`, frames, DML.
- [test_decimal_double_coercion_2.py](test_decimal_double_coercion_2.py) — part 2 (seat V, unions): the 11 cells part 1 broke, neighbours, seat V's family, the tripwire, native frames.
- [test_decimal_double_coercion_2_delta.py](test_decimal_double_coercion_2_delta.py) — its delta: fragment probes, outer references, a user marker.
- [test_decimal_double_in_ranges_2.py](test_decimal_double_in_ranges_2.py) — part 2 `IN` rewrite: step 0's 224 `in/` lists vs a `float(Decimal)` oracle; plan shape on a view and an Iceberg scan.
- [test_deep_expr_build_small_stack_1.py](test_deep_expr_build_small_stack_1.py) — DEEP-FILTER-CHAIN-CRASH-1 CI-segv fold — deep expression builds never overflow a caller stack. Two shapes (caller thread, 8 MiB thread), each in its own interpreter (DEEP-BATTERY-SPLIT-1).
- [test_deep_filter_chain_crash_1.py](test_deep_filter_chain_crash_1.py) — DEEP-FILTER-CHAIN-CRASH-1 — deep operator chains answer instead of crashing. Nine shapes, one interpreter per test; no `xdist_group` (DEEP-BATTERY-SPLIT-1).
- [test_deep_reverify_1.py](test_deep_reverify_1.py) — DEEP-FILTER-CHAIN-CRASH-1 re-verify fold — VD2 pins on main, small stacks, and GC. The four main-thread shapes run one interpreter each; the 256 KiB, 512 KiB and GC fixtures were one shape each and stay (DEEP-BATTERY-SPLIT-1).
- [test_deep_subquery_expression_1.py](test_deep_subquery_expression_1.py) — DEEP-FILTER-CHAIN-CRASH-1 verifier fold — subquery plans and deep expressions. Six shapes, one interpreter per test; `xdist_group` only on the two DataFrame-door refusals (DEEP-BATTERY-SPLIT-1).
- [test_double_decimal_store_round_1.py](test_double_decimal_store_round_1.py) — DOUBLE-DECIMAL-STORE-ROUND-1 (stores only): a FLOAT/DOUBLE stored into DECIMAL rounds the double's JDK 17 text half up as Spark 4.1.2 does, both ANSI modes, on the store doors (INSERT SELECT/UNION/OVERWRITE, UPDATE, MERGE, the DataFrame writers) and the native door's stores; NaN, ±Infinity and overflow still refuse. The former `WAITING` strict xfails now pass, since DECIMAL-DOUBLE-UNION-COERCION-1 part 1.
- [test_dq_identifiers_config_refuse_1.py](test_dq_identifiers_config_refuse_1.py) — DQ-IDENTIFIERS-CONFIG-REFUSE-1 (AUD-021) — `spark.sql.ansi.doubleQuotedIdentifiers=true` refuses (`UnsupportedOperationException`) on SQL `SET`, `conf.set` and builder config (reuse and fresh build), the configuration stays `false`, and the six DML-reach statements after a refused `SET` answer Spark 4.1.2 config-off; the AUD-021 cells from `docs/corpus/reaudit-cells-2026-11-05.jsonl` (skipped when absent) and [data/dq_identifiers_config_refuse_1_cells.jsonl](data/dq_identifiers_config_refuse_1_cells.jsonl). CORPUS-CHECKER-REPAIR-1 (2026-11-13): a missing cells file fails; 11 AUD-021 rows pinned.
- [data](data/map.md) — measured cell tables read by the pins (own `map.md`).
- [test_f1_sql_expander.py](test_f1_sql_expander.py) — Free-SQL bare-name expander — Path A statement forms via resolve_table_name SSOT.
- [test_fuzz_smoke.py](test_fuzz_smoke.py) — R-SQL-FUZZER — always-on smoke battery (200 queries, fixed seed 42).
- [test_ident_dialect_1.py](test_ident_dialect_1.py) — IDENT-DIALECT-1 and 1B — the AUD-015 domain table as the clause: every cell seeded as measured (1200 cells, the twelve 1B card cells among them), each answering Spark 4.1.2 (refusal condition, state change, rows); the control spellings answer RePark as measured at 4fd5c6c1.
- [test_ident_dialect_doors.py](test_ident_dialect_doors.py) — IDENT-DIALECT-1B — every facade door that takes a table identifier string reads it with Spark's grammar (double-quoted and non-ASCII spellings refuse, back-quoted reads); load() reports the legacy snapshot option before the grammar.
- [test_listview_inner_join_key_1.py](test_listview_inner_join_key_1.py) — LISTVIEW-INNER-JOIN-KEY-1 — the
  table's 283 cells against their oracle: every key-name join on ListView, LargeListView, RunEndEncoded
  and container keys (six join types, `[k, j]`, the aggregate, `writeTo`, `INSERT … SELECT`, the pandas
  `ArrowDtype` route, the 24 mixed-kind cells) gives Spark's rows by count and digest; `lateralJoin`
  key names give Spark's key-name join rows (LATERAL-USING-ACCEPT-1); SQL `USING` on a non-hashable key
  stays refused (LISTVIEW-SQL-USING-1). Each key reaches its measured Arrow type. `slow`: the 13
  1000 × 1000 `big` cells run one per xdist group, the rest share one module session.
- [test_native_ctas_iceberg_refuse_1.py](test_native_ctas_iceberg_refuse_1.py) — NATIVE-CTAS-ICEBERG-REFUSE-1: `repark.sql()` CTAS into an Iceberg table refuses, creates nothing and leaves an existing table as it was; controls: in-memory CTAS, the two-step workaround and the Spark-door CTAS store Spark's rows.
- [test_qi1_idents.py](test_qi1_idents.py) — QI1 — quoting / path-escape single-source pins (CQ-006/007).
- [test_range_tvf_id_1.py](test_range_tvf_id_1.py) — RANGE-TVF-ID-1 — the range() table function names its column id like Spark.
- [test_range_tvf_id_2.py](test_range_tvf_id_2.py) — RANGE-TVF-ID-2 - range() argument edge cases answer Spark 4.1.2.
- [test_round_double_exact_1.py](test_round_double_exact_1.py) — ROUND-DOUBLE-EXACT-1 — the step 0 `round`/`bround` table on every Spark door in both ANSI modes, with the native-door gap pin; oracle [round_double_exact_1_spark_oracle.json](round_double_exact_1_spark_oracle.json).
- [test_scalar_subquery_empty_null_1.py](test_scalar_subquery_empty_null_1.py) — SCALAR-SUBQUERY-EMPTY-NULL-1 (2026-11-23; Delta 1 2026-11-24): Spark SQL (both ANSI modes), DataFrame/`F.expr`/`filter`/`selectExpr`/`.scalar()`, temp views, cache, `PREPARE`, native `repark.sql()` (incl. `CREATE VIEW` + `DESCRIBE`), `enable_physical_uncorrelated_scalar_subquery=true`, plan quality, Iceberg Hadoop writes (`INSERT`/`CTAS`, `DELETE`/`UPDATE` refusals), and Delta 1 F-001/F-002/F-003 pins (generators, Iceberg/Parquet cast metadata, post-coercion non-null temp views and schemas). Cells: [_scalar_subquery_empty_null_1_cells.py](_scalar_subquery_empty_null_1_cells.py).
- [test_scalar_subquery_sort_pin.py](test_scalar_subquery_sort_pin.py) — DF 54.1 regression pin: ORDER BY must survive an uncorrelated scalar subquery.
- [test_spark_sql_grammar_1.py](test_spark_sql_grammar_1.py) — SPARK-SQL-GRAMMAR-1 door pins — Spark operators, keywords and type names.
- [test_sql_alias.py](test_sql_alias.py) — Q1 re-home pins — ``import repark.sql`` fails; ``repark.sql()`` is the ANSI door.
- [test_sql_dml_eager.py](test_sql_dml_eager.py) — Bare `spark.sql` DML executes eagerly (F-BR-2) — PySpark parity.
- [test_sql_passthrough_parity.py](test_sql_passthrough_parity.py) — Adversarial Spark-parity corpus for the raw ``spark.sql()`` passthrough (AR-WG-SQL, C-AR-005).
- [test_sql_set_door_1.py](test_sql_set_door_1.py) — SQL ``SET`` / ``RESET`` door pins — SQL-SET-DOOR-1 (oracle ``fixtures-batch1.json`` BTZ5-0..19).
- [test_sql_setop_all_multiset_1.py](test_sql_setop_all_multiset_1.py) — SQL-SETOP-ALL-MULTISET-1 (2026-11-23): `INTERSECT ALL` / `EXCEPT ALL` answer Spark 4.1.2 on `spark.sql` (both ANSI modes) and `repark.sql()`; `MINUS ALL` answers on `spark.sql` only (parser-aware, post-normalizers, incl. `VALUES` CTAS & `BY NAME (...)`) and refuses on `repark.sql()`. Covers types, edges, shapes (cold writes), spellings, fragments, stored views, DataFrame methods, null-safe join guards, `F.expr` boundary, untyped `NULL` left, MAP (both sides + nested) and `SETOP-MIXED-TYPE-COERCION-1`. Cells: [_setop_all_multiset_1_cells.py](_setop_all_multiset_1_cells.py); oracle [setop_all_multiset_1_spark_oracle.json](setop_all_multiset_1_spark_oracle.json).
- [test_sqp_1_string_literals.py](test_sqp_1_string_literals.py) — SQP-1 facade controls — Spark string-literal escapes and ``CAST … AS BINARY``.
- [test_string_literal_escape_1.py](test_string_literal_escape_1.py) — STRING-LITERAL-ESCAPE-1 facade pins — SQL string literals unescape as Spark does.
- [test_signed_zero_normalize_1.py](test_signed_zero_normalize_1.py) — SIGNED-ZERO-NORMALIZE-1 part A (P2, P6): replays the 301 clause cells of
  `data/signed_zero_normalize_1_cells.jsonl` (D2–D6, D13, A1–A3) on the five routes over the one-row-per-file
  seed, against Spark's truth; D6 pins the base names (D6-3…6 names only, values are part B). The 8 Spark-door
  `DECIMAL-EXPR-FLOAT-COMPARE-1` residues may refuse but never answer wrong. The delta adds 10 cells: V-B1, native
  `ROLLUP` / `CUBE` / `GROUPING SETS` over a zero comparison; V-A3, `selectExpr` and `F.expr` names of `<=>`
  (`(d <=> 0.0)`, never `__repark_spark_nonnull__`).
- [test_signed_zero_keys_1.py](test_signed_zero_keys_1.py) — SIGNED-ZERO-NORMALIZE-1 part B part 1 (P5): replays the 780 read cells of `data/signed_zero_keys_1_cells.jsonl` (470 key rows WRONG on `276dc503`, 310 controls) on `spark-sql`, `f-expr`, `typed` and native against Spark 4.1.2's view answers; a `SET` on either door cannot turn the key wrap off. Delta round (2026-11-13): the refused `max_passes = 0` / `skip_failed_rules = true` forms, OB-4 as ties, GB-8 in order, the canonical-NaN output bits, and `dropDuplicates(subset)` keeping the normalised key. Delta 2 (2026-11-13): every `SET` spelling found leaves the next query refused on both doors; the dedup pins assert key classes and normalised bits, not the surviving tied row.
- [test_signed_zero_const_compare_1.py](test_signed_zero_const_compare_1.py) — SIGNED-ZERO-CONST-COMPARE-1 (131 tests): every constant zero pair under every comparison operator answers like Spark on `spark.sql`, `F.expr`, the DataFrame and native doors, both ANSI modes; the contexts (WHERE, HAVING, joins, `OR`, scalar subquery), `filter(str)`, a temp view and `PREPARE`; constant-condition DELETE, UPDATE and MERGE on copy-on-write and merge-on-read; native DELETE and UPDATE; must-not-move column and NaN cells.
- [test_struct_compare_seat_family_1.py](test_struct_compare_seat_family_1.py) — STRUCT-COMPARE-SEAT-FAMILY-1 — every
  DataFrame typed and string door cell of the family table matches Spark: its rows, or Spark's class on a
  refusal (a carded class gap: a refusal; other known cells: no panic). Generated names never show the
  relabel, and a bound duplicate-named `nullif` gives Spark's measured rows (cell `DK22`).
- [test_struct_setop_reorder_refusal_1.py](test_struct_setop_reorder_refusal_1.py) — STRUCT-SETOP-REORDER-REFUSAL-1 — the set-operation domain table on `spark.sql` and the DataFrame set operations: reordered struct operands refuse with `[INCOMPATIBLE_COLUMN_TYPE]`, identical-struct controls answer, refusals write nothing; the ten `unionByName` over-refusals carry `STRUCT-SETOP-UNIONBYNAME-OVERREFUSAL-1`. Deltas 1 and 2 add the `F.expr` scalar-subquery, NULL-first, view and row-12 user-cast rows under `STRUCT-SETOP-USER-CAST-OVERREFUSAL-1`; item 5 names the `INTERSECT ALL` / `EXCEPT ALL` join refusals. SQL-SETOP-ALL-MULTISET-1 (2026-11-23): those SQL cells are checked like the distinct ones again, and a MAP operand refuses with `[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE]` as in Spark.
- [test_struct_setop_type_family_1.py](test_struct_setop_type_family_1.py) — STRUCT-SETOP-TYPE-FAMILY-1 — the kind-invariance pins: pandas, polars, Iceberg-against-polars and temp-view variant cells across union, subtract and intersect, read and insert; reordered variants refuse with the unit class, conforming map variants answer Spark's rows; the F-K typed-null rows answer last and refuse first on both doors. Item 2 adds the three cross-kind unionByName over-refusals, each carrying STRUCT-SETOP-UNIONBYNAME-OVERREFUSAL-1.
- [test_struct_compare_by_position_1.py](test_struct_compare_by_position_1.py) — STRUCT-COMPARE-BY-POSITION-1 — the step-0 DataFrame typed and string door cells answer or refuse as Spark does (pairing by position), the 435 kind-invariance cells, and the native-door divergence pin (`repark.sql()` still pairs by name). One `xdist_group`; every door is measured inside one fixture call, in its own session (`newSession`; the active one is restored, never stopped). A Rust panic (pyo3's `PanicException` is a `BaseException`) is that cell's outcome `PANIC`, so one panic fails one cell, never the module; CI's `ci` profile keeps DataFusion's debug assertions, and the no-panic shapes pin the DataFrame doors beyond the table there.
- [test_three_valued_logic_parity.py](test_three_valued_logic_parity.py) — Three-valued logic differential corpus — value AND Arrow type AND nullability.
- [_record_range_tvf_id_1.py](_record_range_tvf_id_1.py) — Record the range() table-function Spark oracles - ID-1 forms and ID-2 edge cases.
- [_record_tvl_goldens.py](_record_tvl_goldens.py) — Record mode for the three-valued-logic corpus — re-derive every Spark half from live PySpark.
- [range_tvf_id_1](range_tvf_id_1/map.md) — RANGE-TVF-ID oracle recordings (own `map.md`).

## Path expressions

The move put every file here one level below the old flat directory. Four files had a path expression repointed in the same unit and no other line changed: `_record_range_tvf_id_1.py`, `_record_tvl_goldens.py` and `test_three_valued_logic_parity.py` (their own or a sibling's new path in a docstring or recorded string), and `test_fuzz_smoke.py` (repository-relative anchors gain one level, `parents[N]` to `parents[N+1]`).

## DEEP-BATTERY-SPLIT-1 (2026-10-19)

Each deep-battery shape runs in its own isolated `sys.executable -c` interpreter (no module-wide `xdist_group`; `slow` stays on all four modules). One `xdist_group` pairs the 20,000-term OR shape (~286 s) with the 2,000-deep `+1` refusal so `--dist loadgroup` starts them first (full suite `-n 16` dropped 392.5 s → 355.2 s, 20,066 passed; `test_deep_reverify_1.py` + `test_deep_expr_build_small_stack_1.py` `-n 8`: 56.6 s → 37.3 s).

## Pointers

- Up: [../map.md](../map.md)
