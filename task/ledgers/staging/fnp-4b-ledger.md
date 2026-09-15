# Unit ledger — FNP-4B · Spark-door dialect and Spark expression strings

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when FNP-4B merges, or when the owner closes the slate row.

**Unit:** fnp-4b · **Date:** 2026-09-15 · **Model:** muse-spark-1.3-contributor · **Branch:** `feat/fnp-4b-spark-dialect`
**Card:** FNP-4B — the Spark-door dialect and Spark expression strings (run 15c, 2026-09-14).
**Path:** STANDARD.

**Owner shape rule (1.5 campaign, 2026-09-14):** a Spark-visible spelling is done when it answers
PySpark 4.1.2 on BOTH doors — `spark.sql(...)` and the Python API (`F.expr`, `df.filter("…")`,
`df.where("…")`, `df.selectExpr(...)`) — for its argument shapes, null and edge cases, result
values AND Arrow type/nullability, with pins; otherwise a dated DECLARED refusal with Spark's
error class and a registry row.

**Oracle:** `fixtures-batch1.json` / `fixtures-batch2.json` at the clone root (PySpark 4.1.2,
`<pyspark-4.1.2-oracle>`); `repark-batch1.json` / `repark-batch2.json` beside them are today's
RePark answers. Cells for this unit: `BL9-*`, `FNP4B-*`, `BL10-*`, `BL12-*`, and every cell whose
RePark error is `ParserError(... found: D ...)` (`BL6-sql-3`, `DIV-ceil-2`, `DIV-ceil-5`,
`DIV-round-0`, `DIV-round-1`, `DIV-round-5`, `DIV-sec-0`, `DIV-sec-2`, `DIV-sec-4`). Values pinned
into tests are copied from the fixtures; the cell id is cited in evidence.

**Audit note:** the `audit-repark-parity` skill calls for a live-JVM banner ritual; the card
supersedes it (oracle already measured, never start a JVM). Classification per the skill:
every divergence below becomes a fix, a pin, or a reported registry finding.

## Proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | D-1: the Spark door session parses with `Dialect::Databricks` for every statement (`apply_spark_parser_dialect` wired in `SparkExtension::configure`); `"abc"` is a STRING literal with Spark escapes, backticks quote identifiers; native ANSI door untouched. | `test_fnp_4b_*.py` BL9 + FNP4B door pins green on both doors; `cross_door.rs` suite green; `make verify` green | **OPEN** | Step-1 measure below. |
| C-002 | D-2: engine-internal and facade-generated SQL quotes identifiers with backticks (Rust `quote_ident_spark`, Python `_idents.quote_ident` / `quote_column_sql_expr`, embedded backtick doubled); consumers parsing under a backtick-rejecting dialect parse that internal statement with an explicit dialect. | backtick pins green; `cross_door.rs` DML tests green | **OPEN** | Step-1 measure below. |
| C-003 | D-3: `F.expr` (`expr_build.rs::sql_context`) and `filter_sql` parse with Databricks (same lexer as the SQL door, reusing SQP-1 canonicalisation, not forking it). | FNP4B-expr/filter/selectExpr/where pins green on the Python API door | **OPEN** | Oracle cells `FNP4B-*` listed above. |
| C-004 | D-4: Spark numeric literal suffixes on the Spark door — `1D`/`2.5D`/`1e200D` DOUBLE, `1.5F` FLOAT, `1S` SMALLINT, `1Y` TINYINT, `1.5BD` DECIMAL, `1L` BIGINT kept — as a token rewrite in the existing Spark literal/normalize pass. | D-suffix pins green (`BL6-sql-3`, `DIV-*` cells above) | **OPEN** | Oracle cells listed above. |
| C-005 | D-5: BL-10 — `spark.sql.parser.escapedStringLiterals` read from the session build conf (`SessionBuildConf`, the way `spark.sql.ansi.enabled` is read); `true` keeps backslashes verbatim (`BL10-on-*`); no runtime `SET` (B-TZ-5 owns the SET door). | `BL10-on-*` pins green with flag-on session; `BL10-off-*` green default | **OPEN** | Oracle cells `BL10-off-*`, `BL10-on-*` listed above. |
| C-006 | D-6: BL-12 — out-of-range `\U` yields Spark's Java artifact (`length('\U00110000')` = 2, `hex` = `3F3F`; `\UFFFFFFFF` → `ED9EBF3F`) at `spark_literals.rs::push_code_point`; `hex('\U0001F600')` nullability pinned if in this lexer's reach, else under out_of_scope_observed. | BL12 pins green; existing `test_out_of_range_unicode_escape_is_one_replacement` updated | **OPEN** | Oracle cells `BL12-0`…`BL12-5` listed above. |
| C-007 | No regression: `cargo test --workspace` green. | `cargo test --workspace` exit 0 | **OPEN** | Step-1 measure below. |
| C-008 | No regression: full facade suite `python/repark/tests` green. | `.venv/bin/python -m pytest python/repark/tests -q` exit 0 | **OPEN** | Step-1 measure below. |

## Step-1 measurement (D-1 on alone, 2026-09-15)

Wired `apply_spark_parser_dialect` in `SparkExtension::configure` with no other change
(`crates/repark-spark/src/extension.rs`; the now-live `#[expect(dead_code)]` removed), then ran
both suites. No HALT: every failure below is the identifier-quoting category D-2 exists to fix
(a `"col"` the Databricks lexer now reads as a STRING); sampled failures pass on the base tree.

### Rust: `cargo test --workspace --no-fail-fast` → exit 101, 6 failures

- `repark-sql/tests/cross_door.rs`: `cross_door_g3e8_not_in_delete_executes_identically`,
  `cross_door_g3e8_exists_delete_executes_identically`,
  `cross_door_g3e8_update_in_executes_identically`,
  `cross_door_g3e8_correlated_in_delete_executes_identically`,
  `cross_door_merge_produces_the_same_result_table` — the 5 DML tests FNP-4a predicted
  (engine-internal `"..."` SQL now lexes as strings).
- `repark-spark lib tests::run_maintenance::a_repark_toml_policy_reaches_run_maintenance` —
  same cause one layer up (`sum(Utf8)`):

```text
thread 'tests::run_maintenance::a_repark_toml_policy_reaches_run_maintenance' (2606614) panicked at crates/repark-spark/src/tests/run_maintenance.rs:796:10:
the file policy plans with no inline keys: Analysis("Error during planning: Internal error: Function 'sum' failed to match any signature, errors: Error during planning: Function 'sum' requires Decimal, but received String (DataType: Utf8).,Error during planning: Function 'sum' requires UInt64, but received String (DataType: Utf8).,Error during planning: Function 'sum' requires Int64, but received String (DataType: Utf8).,Error during planning: Function 'sum' requires Float64, but received String (DataType: Utf8).,Error during planning: Function 'sum' requires Duration, but received String (DataType: Utf8)..
```

```text
thread 'cross_door_g3e8_not_in_delete_executes_identically' (2611322) panicked at crates/repark-sql/tests/cross_door.rs:682:37:
NOT IN DELETE must execute: datafusion engine error: Internal error: identity SELECT `_pos` column is not Int64.
```

Base-tree check: the `run_maintenance` pin passes with the `extension.rs` change stashed
(`cargo test -p repark-spark --lib tests::run_maintenance::...` → ok, 2026-09-15), so all 6
are D-1-caused, none pre-existing.

### Facade: `.venv/bin/python -m pytest python/repark/tests -q` → 581 failed, 5442 passed, 357 skipped, 98 errors

Top files: `test_g4b_semi_join` (48), `test_v3_statement_coverage` (43), `test_udf` (43),
`test_ml_feature_oracle` (34), `test_ml_boost_oracle` (33), `test_ml_estimators_oracle` (33),
`test_select_global_agg` (29), `test_dml_subquery_parity` (21), `test_filter_predicate_rewrite`
(17), `test_explode_rewrite` errors (33). Sampled needles, all quoting-shaped:

- `Cannot cast string 'k' to value of Int64 type` (`test_g4b_semi_join`, a `"k"` ident as string)
- `Function 'sum' requires ... but received String (DataType: Utf8)` (`test_cache_persist`, `test_c6_census_r8`)
- `Cannot coerce arithmetic expression Utf8 + Int64` (`test_select_global_agg`)

Base-tree check (change stashed, native rebuilt): `test_cache_persist.py::test_derived_after_materialize_reads_cached`,
`test_c6_census_r8.py::test_catalog_register_function_camel_case_alias`, all of
`test_g4b_semi_join.py` → 129 passed. D-1-caused, quoting category, D-2 is the fix. No HALT.

### D-1 behavioral probes (native rebuilt with D-1, 2026-09-15)

- `SELECT "abc"` → `abc` (BL9-0 fixed by D-1 alone).
- `SELECT "a" || "b"` → `ab`; `SELECT "1" = 1` → true.
- `SELECT "a\"b"` → `TokenizerError(Unterminated string literal)` — needs the D-1
  double-quote canonicalisation (BL9-1).
- `SELECT length("a\nb")` → 4, `SELECT "it\'s"` → `it\'s` — backslashes kept verbatim by the
  stock tokenizer; needs Spark-escape processing in canonicalise (BL9-2, BL9-3).
- `SELECT 1L` / `SELECT 2.5D` → `No field named "1l"` / `"2.5d"` (Databricks
  `supports_numeric_prefix` folds suffixed numbers to words) — D-4 must cover `L` too, not
  just `D/F/S/Y/BD`, to keep the "already works" promise.
- `SELECT 'ID' = 1` → `PySparkException: Optimizer rule 'simplify_expressions' failed ...
  Cannot cast string 'ID' to value of Int64 type` — loud on both doors; class text differs
  from Spark's `CAST_INVALID_INPUT` under the BL-1 raise-vs-raise precedent (FNP4B-filter-dq-ID).
- Struct: `named_struct('a', 1)` → `struct<a: int32>`; `.a` on the call result refuses in the
  DataFusion planner while `['a']` and column-form `t.a` work (FNP4B-sql-struct-lit needs a
  call-base field-access rewrite).
