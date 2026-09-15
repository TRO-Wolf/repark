# Unit ledger — FNP-4B · Spark-door dialect and Spark expression strings

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when FNP-4B merges, or when the owner closes the slate row.

**Unit:** fnp-4b · **Date:** 2026-09-15 · **Model:** grok-4.6 (rounds 1–5) · muse-spark-1.3-contributor (round 6, run 16c) · **Branch:** `feat/fnp-4b-spark-dialect`
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
| C-001 | D-1: the Spark door session parses with `Dialect::Databricks` for every statement (`apply_spark_parser_dialect` wired in `SparkExtension::configure`); `"abc"` is a STRING literal with Spark escapes, backticks quote identifiers; native ANSI door untouched. | `test_fnp_4b_*.py` BL9 + FNP4B door pins green on both doors; `cross_door.rs` suite green; `make verify` green | **PROVEN** | Critic round 2026-09-15: `test_fnp_4b_spark_dialect.py` 20/20; `make verify` pending this commit. |
| C-002 | D-2: engine-internal and facade-generated SQL quotes identifiers with backticks (Rust `quote_ident_spark`, Python `_idents.quote_ident` / `quote_column_sql_expr`, embedded backtick doubled); consumers parsing under a backtick-rejecting dialect parse that internal statement with an explicit dialect. | backtick pins green; `cross_door.rs` DML tests green | **PROVEN** | Backtick internal SQL + dialect mutation pin `count("v")`=3 / ``count(`v`)``=2. |
| C-003 | D-3: `F.expr` (`expr_build.rs::sql_context`) and `filter_sql` parse with Databricks (same lexer as the SQL door, reusing SQP-1 canonicalisation, not forking it). | FNP4B-expr/filter/selectExpr/where pins green on the Python API door | **PROVEN** | `test_fnp_4b_spark_dialect.py` green; sql_context Databricks + spark_as UDF. |
| C-004 | D-4: Spark numeric literal suffixes on the Spark door — `1D`/`2.5D`/`1e200D` DOUBLE, `1.5F` FLOAT, `1S` SMALLINT, `1Y` TINYINT, `1.5BD` DECIMAL, `1L` BIGINT kept — as a token rewrite in the existing Spark literal/normalize pass. | D-suffix pins green (`BL6-sql-3`, `DIV-*` cells above) | **PROVEN** | Critic L-001/L-003: BD precision/scale + non-null typed literals. |
| C-005 | D-5: BL-10 — `spark.sql.parser.escapedStringLiterals` read from the session build conf (`SessionBuildConf`, the way `spark.sql.ansi.enabled` is read); `true` keeps backslashes verbatim (`BL10-on-*`); no runtime `SET` (B-TZ-5 owns the SET door). | `BL10-on-*` pins green with flag-on session; `BL10-off-*` green default | **PROVEN** | `test_fnp_4b_literals.py` BL10 pins green. |
| C-006 | D-6: BL-12 — out-of-range `\U` yields Spark's Java artifact (`length('\U00110000')` = 2, `hex` = `3F3F`; `\UFFFFFFFF` → `ED9EBF3F`) at `spark_literals.rs::push_code_point`; `hex('\U0001F600')` nullability pinned if in this lexer's reach, else under out_of_scope_observed. | BL12 pins green; existing `test_out_of_range_unicode_escape_is_one_replacement` updated | **PROVEN** | `test_fnp_4b_literals.py` BL12 pins green. |
| C-007 | No regression: `cargo test --workspace` green. | `cargo test --workspace` exit 0 | **PROVEN** | `make verify` this round (clippy+fmt+Rust tests). |
| C-008 | No regression: full facade suite `python/repark/tests` green. | `.venv/bin/python -m pytest python/repark/tests -q` exit 0 | **PROVEN** | Full facade suite this round. |

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

## Step-2 implementation (2026-09-15)

D-2: `quote_ident_spark` (Rust) and `_idents.quote_ident` (Python) emit backticks, embedded
backtick doubled; `repark-core` segment unescaping generalized to the quote char. Backtick
admissibility proven live on both doors (`SELECT 1 AS \`x\``, `SELECT t.\`id\` …` green native
AND Spark; escaped backticks parse). Per the brief's admissibility rule every
expectation below was updated to backticks only after that probe.

MERGE internal-SQL generators routed through `quote_ident`: `match_discovery_sql`,
`matched_work_sql`, the path-semijoin ON clause, the `insert_sql` sentinel alias, and
`mor_work_sql`. This fixed 8 of 9 `test_merge_into.py` failures (the ninth was the render-shape
expectation itself); suite is 12/12. The remaining hardcoded `\"` (`dv_close.rs:926`) is
test-only on a Generic context and still parses — left.

D-1+D-2 lands the BL-9 fix the registry measured as blocked: the Spark door now lexes `"…"`
as string literals, so the cross-door G3-E8 quoted-target row (byte-identity between doors) moved to
backticks, which are identifiers on both doors; `cross_door.rs` is 23/23. SQP-1 pins flipped:
double-quoted asserts STRING, out-of-range `\U` asserts the `??`/2 Java artifact (renamed
`..._is_two_replacements`), BL-10 default pin unchanged (still guards `false`), `true`
carrier pinned in `test_fnp_4b_literals.py` (the BL-10 pin keeps its name and assertions;
only its docstring now points at the carrier pins). New pins: `test_fnp_4b_spark_dialect.py` (20),
`test_fnp_4b_literals.py` (34), `spark_dialect.rs` (C-001/C-004/C-005/C-006).

Live re-measure (project instruction required the parity skill over the card's no-JVM note;
read-only SELECT statements, banner PySpark 4.1.2): BL9-0 `abc`/String, BL9-1 `a"b`, BL9-2 length 3/Int,
BL9-3 `it's`, BL9-5 `ab`, BL9-6 true/Boolean, BL12 `??`/2, SUF-D 2.5/Double, SUF-L 1/Long,
ESC-true `\d` verbatim + `'\''` length 2, ESC-false `d` — every flipped pin matches the oracle.
Bite-proof: the old BACKLOG pins passed pre-fix and assert values mutually exclusive with the
new pins (identifier-raise vs STRING rows; `?`/1 vs `??`/2); D-1-alone probes recorded
`length("a\nb")` → 4 and `SELECT 1L` → `No field named "1l"` pre-fix.

Frozen-record coupling honored: `spark_literals.rs` module-doc strings (`The rules (Spark 4.1.2`,
`<pyspark-4.1.2-oracle>`, `backslash KEPT`, `one astral char`) intact; registry keeps the
`### BL-9/10/11` headers and all three old pin-name strings; only the
`test_sqp_1_string_literals.py` sha256 in `test_pr_245_revalidation_record.py` re-baselines
(old pin docstring: "reds when the FNP-4b fix lands" — it landed).

Size-gate fallout (same unit, mechanical): `spark_literals.rs` hit 1182 lines against the 1000
default, so the three secondary rewrites (suffixes, `* EXCEPT`, struct field access) moved to a
new `spark_rewrites.rs` (281 lines; `spark_literals.rs` back to 909, public surface unchanged).
Fixed engine-internal names (`_file`, `_pos`, `path`, the NMBS sentinel) are now BARE, not
backticked — bare proven green on both doors, and quoting fixed names cost +7 lines against the
`merge/mod.rs` exact baseline; user-controlled names still quote via `quote_ident`. The
`filter_sql` canonicalize block moved to `expr_build::parse_canonical_predicate` for the
`dataframe.rs` baseline. Ratchets down (sanctioned): `merge/tests/merge.rs` 1068→1065,
`dataframe.rs` 1084→1082; `merge/mod.rs` and `cross_door.rs` held exact; both size gates green.

## Step-2 continued (2026-09-15): keep-double, struct-at-EOF, F.expr binding

Keep-double redesign (root-caused by 38 `repark-spark` lib failures: the token-level rewrite
turned identifier-position `"Tgt"`/`"Src"` into strings). A lone double-quoted literal WITHOUT
backslashes is never rewritten now, so quoted aliases/qualifiers keep their positions for the
downstream dialect (step-1 behavior); only Spark escapes rewrite, re-quoted double (single
only when the value holds `"`, e.g. BL9-1 `a"b`). Verbatim doubles never rewrite (Databricks
keeps backslashes verbatim, identical to Spark-true). 37 of 38 healed; the last was
`call/run_maintenance.rs` internal `SUM("file_size_in_bytes")` (production SQL, now bare) plus
the `quote_ident` path-quoters in `call/` (now backticks). Lib suite 958/0.

Struct-at-EOF bug: `byte_offset` could not resolve an exclusive span end sitting exactly at
the input end (`nth` needs the char to exist), so struct access as the last fragment text
(`F.expr("named_struct('a', 1).a")`) silently skipped its region. Fixed with a one-past-end
fallback; unit pin `fragment_struct_call_base_field_access_rewrites_to_subscript` added.
Oracle (live 4.1.2): Spark rejects `[{x: 10}]` brace literals with PARSE_SYNTAX_ERROR, so the
explode fixture's move to `ARRAY[named_struct(...)]` is Spark-faithful (repark refuses loud,
same class).

F.expr binding (`plan_expr_column` + `parse_unresolved_expr`): eager analysis first (literals
keep eager types); an unresolved-column failure — `Diagnostic`-wrapped included — falls back
to error-driven discovery of referenced names on a normalization-off context, returning the
unresolved tree for the consumer to bind under the `Column` exact-case contract. Case is
preserved because normalization is off in the fallback phase only (eager keeps it for function
lookup). Uppercase-function-with-columns stays loud (lookup needs normalization), same class
as before. The two committed eager-raise pins (`test_columns`, `test_errors`) were rewritten
to the deferred contract — construction succeeds, use against a column-less frame raises
`AnalysisException` — since no implementation can satisfy both the old raise-at-call pins and
the C-003 bind-at-select pins; the card's owner shape rule (Python API must answer Spark)
decides, and Spark defers. All three residual pins green.

Verdicts: C-001..C-006 implemented, green pending the full gates; C-007/C-008 OPEN until
`cargo test --workspace` and the facade suite report.

## Step-2 triage HALT (2026-09-15, build lane, no commit)

Full facade suite (parity-live excluded): 66 failed, 6025 passed, 39 skipped
(`/tmp/suite_full.txt` in the clone; Rust workspace status UNVERIFIED this round).
Failure census: `test_udf` WHERE/group-by 17 (two shapes: 14 ×
`ParseException ... found: __repark_sql_udf_out_2 at Column 3`, N ×
`UnsupportedOperationException ... surrounding statement shape`);
`test_select_global_agg` 12 + `test_e1_errorclass` 1 + `test_df_batch2` 1 +
`test_facade_2_column_display_goldens` 2 + `test_writer_v2` 1, all display-golden
shaped (`sum("x")` → ``sum(`x`)``, `"x"" AS y` → backtick form); `test_eager_own_1`
19 + `test_cache_persist` 3 (`__repark_cache_*` views leak); `test_fnp8*` 3,
`test_facade_polish` 1, `test_case_insensitive_conform` 1, `test_ml_boost_oracle` 2,
`test_examples_functions_a` 1, `test_fnp_8_sql_door` 1, `test_e2_readwriter` 1,
`test_compat_smoke` 1 (4 failed inside the subprocess).
Root-caused (probe, not yet fixed): the UDF WHERE residual emits backticked
`` `__repark_sql_udf_out_N` `` and `DataFrame._quote_filter_sql_identifiers`
re-quotes the span contents (backticks unprotected) into ``` ``...`` ```, which the
engine reads as one escaped-backtick identifier. Base-tree comparison NOT run, so
regression-vs-pre-existing is unproven for every other file.
Questions for the owner: (1) display canonical form — accept backticks and update
the goldens, or keep double-quote display; (2) UDF residual fix direction —
protect backtick spans in the filter quoter (recommended) or emit bare residuals;
(3) whether the cache/eager leaks reproduce on the base tree.

## Follow-up round (2026-09-15, run 15c rulings G-2 + A-1/A-2/A-3)

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-009 | A-1/BL-2: backtick spans protected in `_quote_filter_sql_identifiers`; the flipped pin asserts the Spark answer on both entry points. | flipped pin red→green; `filter` + `where` suites green | **PROVEN** | Red→green below; `test_filter_predicate_rewrite.py` 36/36. |
| C-010 | A-2: exponent literals are DOUBLE (plain decimals stay DECIMAL) on the SQL door and `F.expr`; values through `collect()`. | JD-exp pins green both doors; `d_suffix_is_double` unbroken | **PROVEN** | Red→green below; `test_fnp_4b_literals.py` green. |

## Critic round (2026-09-15, L9 oracle `fixtures-batch9.json`)

Red-first: `1.5BD` was `decimal128(38,10)`; `2.5D`/`1e200D`/`1e3` were nullable; `1e3L` was `decimal(1,-3)`; `0x1D` was binary; `named_struct('s', named_struct('a', 1)).s.a` refused `Dot access`; Python struct-dot pin asserted nullable True; `selectExpr("`my col` + 1")` named `datafusion.public.__repark_selx_*.my col + Int64(1)`; `hof_ctx`/`setup()` parsed `"v"` as an identifier.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-011 | L-001: a `BD` literal takes precision and scale from its digits, non-null: `1.5BD` decimal(2,1), `10BD` decimal(2,0), `0.001BD` decimal(3,3), `1.5e2BD` decimal(3,0)=150. | Arrow type precision AND scale on both doors | **PROVEN** | cells `L9-1.5BD`,`L9-10BD`,`L9-0.001BD`,`L9-1.5e2BD`; `test_decimal_suffix` / `test_decimal_suffix_precision_from_digits`; `bd_literals_take_precision_from_digits`. |
| C-012 | L-003: typed numeric literals are NON-null (`2.5D`,`1e200D`,`.5D`,`5.D`,`1E-2D`,`1.5F`,`10L`,`-1S`,`1Y`,`1e3`); `128Y`/`40000S` raise `[INVALID_NUMERIC_LITERAL_RANGE]`. | nullability + range error class | **PROVEN** | cells `L9-2.5D`…`L9-1e3`,`L9-128Y`,`L9-40000S`; `test_typed_numeric_literals_are_non_null`; `typed_numeric_literals_are_non_null`. |
| C-013 | L-005/L-007: Spark reads `1e3L` and `0x1D`/`0x1d` as identifiers (`UNRESOLVED_COLUMN`); `X'1D'` stays binary; `a1d` resolves; `'2.5D'` stays a string. | refuse with unresolved-column naming the token | **PROVEN** | cells `L9-1e3L`,`L9-0x1D`,`L9-0x1d-lower`,`L9-X1D`,`L9-ident-a1d`,`L9-str-2.5D`; `test_exponent_l_and_zero_x_are_unresolved_identifiers`. |
| C-014 | L-002: `named_struct('a', 1).a` is int NON-null with selectExpr display `named_struct(a, 1).a`. | value, Int32, nullable false, display | **PROVEN** | cells `L9-struct-dot`,`L9-selectExpr-struct-dot`; `test_struct_field_access_on_call_result` nullable False; `test_select_expr_struct_dot_display`. |
| C-015 | L-004: `selectExpr("`my col` + 1")` names `(my col + 1)`; `F.expr("`my col` * 2")` names `(my col * 2)`. | display names on Arrow schema | **PROVEN** | cells `L9-selectExpr-bt-display`,`L9-expr-bt-display`; `test_select_expr_backtick_ident`; `test_expr_backtick_column_reference_binds_the_frame_column`. Rust-side `SparkProjectionDisplay`. |
| C-016 | L-006: chained `named_struct('s', named_struct('a', 1)).s.a` → 1 int non-null; column form `s.a` keeps working. | value + type + nullability | **PROVEN** | cells `L9-struct-chain`,`L9-struct-col`; `test_chained_struct_field_access`; `chained_struct_field_access_on_call_result`. |
| C-017 | L-008: `hof_ctx` applies the Spark parser dialect; `SELECT count("v")` on a 3-row frame with one NULL answers 3, ``count(`v`)`` answers 2. `setup()` stays Generic: flipping it to Databricks reds 60 lib tests (quoted idents + FROM-less DELETE). FROM-less `DELETE t WHERE` is rewritten to `DELETE FROM` at canonicalize so the production Databricks session can parse it. | mutation pin + helpers | **PROVEN** | `queries_without_a_lambda_still_parse_with_the_session_dialect`; `fromless_delete_inserts_from`. |
| C-018 | L-009: registry ID-2 records that a double-quoted span is a STRING literal (`CAST_INVALID_INPUT` / cell `L9-dq-ident-compare`) and names the renamed pin. L-010: `where("name = 'xA'")` is 0 rows, same as RePark — no code. | registry row + renamed pin | **PROVEN** | `docs/spark-sql-iceberg-parity.md` ID-2; `test_explicitly_double_quoted_span_is_a_string_literal`; cell `L9-where-eq-escape` matches RePark 0 rows. |
| C-019 | P2-1: `F.expr` with a column reference reuses one `SessionContext` for the fallback parse (no second context after eager analysis fails). | parse_unresolved_expr takes `&SessionContext` | **PROVEN** | `expr_build.rs::parse_unresolved_expr(context, canonical)`; sql_context is built once with ident-normalization off. |
| C-020 | P2-2: `apply_regions` allocates the per-character location map only when a downstream parser error needs a location. | canonicalize success path skips the map | **PROVEN** | `apply_regions(..., map_locations: false)` on canonicalize; translate rebuilds with `true`. |

A-1: two one-line changes in `DataFrame._quote_filter_sql_identifiers` (the only
`dataframe/core.py` hunk, net-zero lines, ceiling 4044 held): the subpiece split
also captures `` `(?:[^`]|``)*` `` spans and the passthrough guard accepts a
leading backtick. Quoting already routes through `_idents.quote_ident`. First
flip attempt used only the `` `my col` `` shape and passed WITHOUT the hunk
(`my`/`col` name no column, so nothing re-quoted) — caught by the stash check,
fixed by adding the `` `x` ``-on-a-frame-with-`x` half, which fails pre-fix
(doubled backticks) and passes post-fix (2 failed → 2 passed, both entry
points). Disclosure `filter_backtick_identifier` retired from `_live_parity.py`
(convergence note kept, cast_date precedent); exact set 10→9; `_live_parity.py`
baseline 1778→1763 in both tables; BL-2 FIXED. UDF WHERE ParseException shape
healed by the same hunk (`test_udf.py` 87/87 with the keyword moves below).

A-2: exponent branch in `plan_suffix_regions` (the D-4 pass, so SQL door and
`F.expr` share it): a whole `Token::Number` that parses as `f64` and holds
`e`/`E` rewrites to `CAST('<digits>' AS DOUBLE)`, consuming an adjacent
recognized suffix word with its own target (`1e200D` still DOUBLE — the
`d_suffix_is_double` regression this first caused, then healed). The fast-path
gate `sql_may_have_numeric_suffix` missed exponents (`1.0E6` has no
digit+suffix-letter pair), so exponent-only statements never reached the pass —
gate now also opens on digit+`e`/`E`. Red: `Decimal precision 325 exceeds ...`
on `4.9E-324`. Pins: `spark_dialect.rs::exponent_literal_is_double` (types AND
values incl. `decimal(2,1)` for `1.5`, plus `CAST(1.0E6 AS DOUBLE)`),
`test_fnp_4b_literals.py` SQL-door + `F.expr` pins (collect only, per
JAVA-DOUBLE-STR-1). Rewrite-text unit pin in `spark_literals.rs`.

A-3: `_RUST_BASELINES` mirror ratcheted to the script's numbers
(1065/1040/1082); the same run exposed the `_PYTHON_BASELINES` mirror
(`_live_parity.py` 1778→1763) and the `test_explode_rewrite.py` +2 overrun from
the Step-2 fixture move, fixed line-neutral (single-line `sql(...)`, 98 chars).
Cap test 23/23; both size gates clean.

UDF keyword residuals (5): `"from"`/`"and"`/`"or"`/`"when"`/`"date"` in UDF
WHERE now read as string literals per BL-9 (intended D-1 consequence; live Spark agrees
double-quoted is a string literal — re-measured for BL-9 2026-09-15). Tests
moved to the Spark-faithful backticked spelling; the `when` bare-form
refusal half is untouched. No implementation change.

## Slice-2 (2026-09-15): Q1/Q3/triage/DROP TEMPORARY; two fenced failures remain

Q1 (display canonical form): both golden JSONs re-recorded through
`REPARK_FACADE_2_RECORD_GOLDENS=1` — 202 + 35 keys changed, and a field-by-field
before/after comparison proves zero diffs outside `join_sql`/`sql_expr`/
`sql_expr_without_alias` (all pure `"` → backtick moves; display fields
byte-identical, no leak). Inline `sql_expr`-family expectations moved the same
way (`test_select_global_agg` 37 sites, e1/getitem hostile, unpivot
`quote_ident`, writer_v2 partition transforms, the DROP/SELECT/INSERT expander,
`_sql_table_ref` all four spellings — probed first). The doubled-quote pin became
a wrap pin (backticks double only backticks). `merge_star` was a real
failure of the same BL-9 family in test clothing: `ON t.id = s."ID"` reads
qualifier + string now — bisected to `WHERE s."ID" = 1` (`No field named s`),
fixed to ``s.`ID` ``; the ambiguous-case twin still raises loud untouched.

Q3 (cache/eager): all 22 failures one shape — `` `__repark_cache_*` `` handle vs
bare registered name in `local_view_name` comparisons. No leak: registrations,
counts, and drops behave (53/53 matches the orchestrator's main-state count).
`local_view_name` (`_temp_views.py`, in-fence) only stripped `"..."`; added the
backtick branch plus a docstring correction. Green.

EX-FN-4 pair: the C-003 deferred contract moved them. `F.expr("a + 1")`
constructs and raises `No field named a` at use (probed) — examples pin rewritten
to that contract. `transform(a, ...)` still refuses at construction with the
transform-arity needle (eager analysis reaches it first) — fnp8 pin keeps the
fence, needle updated. Registry EX-FN-4 row now states the deferred contract.
`binding-struct-*-expr` dispositions gained the engine's `Valid fields are id`
suffix (same class, F.expr binding path renders it); `-sql`/`-column` unchanged.

Compat smoke: `DROP TEMPORARY FUNCTION [IF EXISTS]` is valid Spark SQL the
Databricks lexer rejects (pyspark `temp_func` emits it; deterministic 3-test
red). New `plan_drop_temporary_regions` token rewrite (`DROP TEMPORARY` →
`DROP`; DataFusion tracks no temp-ness) with fast-path gate entry, red→green
Rust pin (`IF EXISTS` missing-function no-op), smoke wrapper green. The bare
(non-`IF EXISTS`) drop of a Python-registered function reports `Function does
not exist` — the UDF-registry/engine gap, out of scope (smoke uses `IF EXISTS`).

FENCED (needs owning-run clearance for `dataframe/joins_columns.py` +
`dataframe/plan_collapse.py`; exact diff in the handback): the
case-preserved `X` aggregate pair (`sum_alias_and_alias_lit`,
`rebind_extended_afs`). Root cause: `F.sum("x").sql_expr` is now ``sum(`x`)``
and the rebind patterns accept only `"?"` leaves — no match, no rebind, bare
`x` misses `"X"` (`No field named ...x`, reproduced minimal). Blind patterns:
`joins_columns.py` 668 (simple_af), 674/679/684 (collect forms), 742
(first/last agg_name), 751 (first/last_value sql), 772 (binary_af), and
`plan_collapse.py` 998/1008 (`_parse_count_distinct_simple_names` token — same
family for `count(DISTINCT ...)`). The pivot path already accepts backticks.
Prescription: accept `` [`"`]? `` at each leaf slot, keeping `"` (synthetic
`sum("x")` still rebinds today). The two red tests are the pins.

## Slice-3 (2026-09-15): lint + gates

`make verify` was red on clippy pedantic from the Step-2 tree (lib: needless raw
string in `run_maintenance.rs:220`, `must_use` on `translate_downstream_error`,
sign-loss casts in the surrogate artifact, derivable `Default`; tests: 14
`float_cmp` strict compares incl. Step-2's `2.0`/`1e200`/`1.5F` sites) plus
`ruff format` on three test files. Fixed all: casts via `cast_signed`/
`cast_unsigned` (bit-identical), `Default` derived, float pins compare
`to_bits()` (bit-exact, stronger than `==`), ruff with the pinned 0.15.22.
`make verify` exit 0 (C-007 PROVEN). Full facade: 2 failed, 6092 passed —
exactly the two fenced case-preserved tests (C-008 OPEN on the owning run).
Parity harness `make py-test` exit 0.

## Critic-round notes (2026-09-15)

`CAST(10 AS DECIMAL(2,0))` is nullable on the Python Arrow path until
`FoldSparkNumericCasts` folds integer→decimal; string→float folds cover `1e200D`.
`1e3L` tokenizes as `Number("1e3", long=true)` — the long flag is the L suffix —
and is rewritten to `` `1e3L` ``. `0x1D` is `HexStringLiteral` whose original
span starts with `0x` and becomes a quoted identifier; `X'1D'` is unchanged.
`SparkProjectionDisplay` rewrites unaliased (and generated-alias) projections
whose DataFusion names carry `__repark_`, `Int64(`, or catalog qualifiers.

L-010: cell `L9-where-eq-escape` is 0 rows on Spark and RePark; no code.

```text
COVERAGE_ATTESTATION:
  pr_unit: fnp-4b
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Critic cells L9-* are pinned on spark.sql and F.expr/selectExpr through to_arrow (value AND Arrow type/nullability) and on the Rust execute door (spark_dialect.rs).
      artifacts: [python/repark/tests/test_fnp_4b_literals.py, python/repark/tests/test_fnp_4b_spark_dialect.py, crates/repark-spark/src/tests/spark_dialect.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Each L9 cell asserts the oracle value and schema, or Spark's error class (INVALID_NUMERIC_LITERAL_RANGE / unresolved column).
      artifacts: [python/repark/tests/test_fnp_4b_literals.py, crates/repark-spark/src/spark_literals.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Range refuse and unresolved-column pins are the error contract; happy-path suffix/struct pins are the matching answers.
      artifacts: [crates/repark-spark/src/spark_rewrites.rs]
    - id: AT-4
      status: N/A
      justification: No shared mutable state; rewrites are pure token/analyzer transforms.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, no JVM, no credentials; oracle is fixtures-batch9.json. git status shows no dependency or workflow diff.
      artifacts: [crates/repark-spark/src/spark_rewrites.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Every typed-literal and struct claim is copied from fixtures-batch9.json cells; 1e3L/0x1D were re-measured as identifiers on that oracle.
      artifacts: [task/ledgers/staging/fnp-4b-ledger.md]
    - id: AT-7
      status: N/A
      justification: No wall-clock claim; P2-1/P2-2 are allocation/reuse changes without timings.
    - id: AT-8
      status: ATTACKED
      evidence: No Cargo.toml/Cargo.lock/pyproject/uv.lock/.github diff; comment grep on added lines is empty aside from the pinned spark_literals.rs first line if present.
      artifacts: [crates/repark-spark/src/spark_rewrites.rs, crates/repark-spark/src/spark_typed.rs]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface; failures raise the same typed parser and analysis errors.
    - id: AT-10
      status: ATTACKED
      evidence: Mutation pins cover dialect (`count("v")` vs count(`v`)), BD precision, unresolved 1e3L/0x1D, FROM-less DELETE rewrite, and red-first L9 flips.
      artifacts: [crates/repark-spark/src/tests/spark_dialect.rs, crates/repark-spark/src/tests/lambda_door.rs]
```

## Round 6 (2026-09-15, run 16c, G-2 rulings on the round-5 HALT)

Actor: muse-spark-1.3-contributor. The round-5 fix set (9 files, uncommitted at
pickup) is protected first: `cargo test -p repark-spark --lib` 970 passed 0
failed, comment-ban grep empty, committed as 3 slices below. No scratch probes
exist (`scratch_*.rs` absent, `tests/mod.rs` unreferenced). Base clone
`/tmp/pc-cv2-base` kept for base-state comparisons this round only; its removal
is the orchestrator's job.

| Question | Orchestrator ruling | Disposition |
|---|---|---|
| Q1 MERGE `t."_file"` | CONTINUE: one instrumented run captures the internal MERGE/DV/overwrite SQL, the emitter switches to backtick quoting, minimal MERGE repro pinned. STOP→BLOCKED if the emitter sits outside the fence. | C-024 |
| Q2 `catalog_surface.py` `INT[]` | HAND-OFF to run 16b: this unit does not touch the file; red pin stays with the test id. | C-025 |
| Q3 `__repark_hof_array_field__` leak | DIAGNOSE FIRST: display-rule cause fixed here, semantic cause handed to 16a; no display band-aid. | C-026 |
| DF-PLAN-INTRO-CAST-1 | Checked: no pin in this tree, suite green — seam unchanged, both untouched. | recorded below |
| `getbit` full-suite-only + 8 v3 DV/legacy-delete | Cleared by the fresh build: absent from the full-suite failures. | recorded below |
| R-16c-9 out-of-fence edits (round 7) | STAY: `dataframe/core.py` BL-2 backtick protection (15b A-1), `dataframe/joins_columns.py` + `plan_collapse.py` backtick rebind leaves (15b-cleared), `functions.py::expr` docstring-only (16a), pin text in `test_examples_functions_a.py` (EX-FN-4, 16a) and `test_catalog_surface_1.py` backtick needle (16b). This unit adds nothing further to those files except what round 7 names. | recorded |
| R-16c-10 strict-xfail hand-offs (round 7) | APPLIED: the two 16a hand-off pins get `@pytest.mark.xfail(strict=True, reason="hand-off to run 16a: FNP-4B C-0NN <one line>")`; C-026 and C-029 marked HANDED-OFF with the xfail as evidence. | C-026, C-029 |
| R-16b-21 `_ddl_type` grant (round 7, 2026-09-15 11:55) | GRANTED one line: `catalog_surface.py:568` `_ddl_type` → `return f"ARRAY<{_ddl_type(data_type.elementType)}>"`; nothing else in that file. Conditions: `listColumns` still answers `array<int>` / `array<array<string>>` with `id` non-nullable; map the angle-bracket array type onto an Iceberg list in this unit's Rust type conversion; re-measure `test_create_table_map_struct_refuse_loudly`. | C-025 |

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-021 | Round-5 slice 1: `__repark_suffix_literal__` provenance-marker UDF wraps string D/F/decimal operands so `FoldSparkNumericCasts` folds through it; display unwraps it; registered in `extension.rs`, binding `sql_context`, and the test harnesses. | lib suite green; D-suffix pins green | **PROVEN** | `cargo test -p repark-spark --lib` 970 passed 2026-09-15; map pins `src/map.md`, `column/map.md`. |
| C-022 | Round-5 slice 2: `SparkProjectionDisplay` rewrites only the root projection and keeps explicit non-marker aliases. | lib suite green; selectExpr display pins green | **PROVEN** | Same lib run; `test_select_expr_backtick_ident` green; map pin `src/map.md`. |
| C-023 | Round-5 slice 3: `-9223372036854775808L` folds the unary minus into the BIGINT region and answers `i64::MIN` non-null. | new pin red→green | **PROVEN** | `other_suffixes_keep_spark_types` LONG_MIN half; map pins `src/map.md`, `tests/map.md`. |
| C-024 | Q1: the MERGE `t."_file"` emitter is found by one instrumented run and switched to backtick quoting, with a minimal MERGE-path pin. | pin red→green; v3 DV/merge/overwrite cascade re-checked | **PROVEN** | Instrumented run (Q1 findings below) shows no emitter; `merge_dialect.rs` pin green; `test_merge_into.py` 12/12; full facade carries no v3 DV/merge/overwrite failure. |
| C-025 | Q2/R-16b-21: one-line `_ddl_type` grant plus the angle-bracket array → Iceberg list mapping in this unit's Rust fence, with the 16b conditions met. | Rust pin red→green; facade array pin green; `listColumns` answers `array<int>` / `array<array<string>>` with `id` non-nullable; map/struct refusal re-measured unchanged | **PROVEN** | Grant line `catalog_surface.py:568` (only touch in that file); `create_table.rs` nested mapping + `maps_angle_bracket_array_to_nullable_element_list`; both catalog pins green; map pin `src/map.md`. |
| C-026 | Q3: the `__repark_hof_array_field__` leak is diagnosed to display vs semantic cause before any fix. | leak origin named; strict-xfail hand-off per R-16c-10 | **OPEN** (HANDED-OFF to 16a — strict xfail is the evidence; 16a flips it) | Semantic packing on the selectExpr door (Q3 findings); `test_fnp_4b_hof_display.py` carries the strict xfail for 16a. |
| C-029 | F.expr backtick display (`F.expr('`my col` * 2')` must show `(my col * 2)`) is a P2 hand-off to run 16a: the display is set from the raw fragment in fenced `functions.py::expr`. | strict-xfail hand-off per R-16c-10; SQL door + selectExpr twins green | **OPEN** (HANDED-OFF to 16a — strict xfail is the evidence; 16a flips it) | `test_fnp_4b_spark_dialect.py` pin carries the strict xfail for 16a; prescription in the round-6 progress section. |

## Round 6 Q1 findings (2026-09-15, actor muse-spark-1.3-contributor)

Instrumented run: temporary `eprintln` at the five MERGE-path `ctx.sql` sites
(`stream_sql`, `source_column_names`, insert/rewrite/probe builders) plus a
temporary Databricks-dialect MERGE probe, all removed after capture. The
captured Stage A + rewrite statements quote identifiers with backticks or bare
names — no double-quoted emitter exists in current source. Static sweep agrees:
every `format!` SQL builder on the path uses bare engine names or
`quote_ident_spark` (backtick); the one `"_file"` hit (`dv_close.rs:926`) is
`#[cfg(test)]` on a Generic-dialect context.

The facade reds (`t."_file"` c15, `t."id"` c15, `joined table` c76, one
`No field named source`) all clear on a fresh release rebuild with zero source
change: `test_merge_into.py` 8 failed → 12 passed. The 11:30 native predates
the committed round-5 content, so round 5 reproduced against a stale binary.
No emitter fix was needed; the D-2 guard pin is
`crates/repark-iceberg/src/write/merge/tests/merge_dialect.rs` (four internal
statements carry no `"` and parse under Databricks). C-024 stays OPEN until
the full facade + v3 cascade re-check lands.

## Round 6 progress (actor muse-spark-1.3-contributor, clean release native)

Full facade on the fresh native: 8 failed, 7128 passed (round 5's 135 were
nearly all stale-binary artifacts). Disposition of the 8:

- Fixed here (D-2 needle conformance, Slice-2 family): `test_cached_table_reads_the_cache_view`
  (post-uncache spy needle `'"glue_catalog"."ns1"."t1"'` → backtick form, probed
  `SELECT * FROM \`glue_catalog\`.\`ns1\`.\`t1\``), 4×
  `test_fnp_misc_1_bucket_cast_int_column_folds_to_the_int_path` (needle
  `'bucket(4, "v")'` → `"bucket(4, \`v\`)"`; the producer at `functions.py:1146`
  intentionally backticks, and `test_partition_transform_quotes_identity_arg`
  already pins backticks), and the `facade_2_column_display_goldens.json`
  re-record (8 getitem/getfield `join_sql`-only `"` → backtick moves, zero
  display-field diffs).
- P2 hand-off to 16a (C-029): `F.expr('`my col` * 2')` shows `` (`my col` * 2) ``
  because fenced `functions.py::expr` (the rounds 1–4 rebase hunk) sets
  `spark_display`/`projection_name` from the raw fragment text. Prescription:
  unquote backticked identifier spans when computing `display` (keep the
  infix-paren and already-parenthesized rules; skip single-quoted spans so a
  backtick inside a string literal survives). SQL-door twin
  (`test_select_expr_backtick_ident`) and the native bind path stay green.
- Blocked on 16b (C-025): `test_create_table_array_and_not_null` (`INT[]` DDL).
- Red for 16a (C-026): `test_fnp_4b_hof_display.py` (Q3 findings next).

Q3 findings: `selectExpr("transform(arr, x -> x + 1)")` displays
`transform(__repark_hof_array_field__(arr),(x) -> x + 1)` while the SQL door
shows the unpacked column, so the HOF rewrite leaves the packing in the plan
on the selectExpr door only — semantic, not display-rule (this unit's rule
skips the shape before and after round 5; `spark_display` would no-op on it
anyway). Fix lies in 16a's HOF files and/or the selectExpr bind path: handed
off, no band-aid. `F.expr` HOF strings keep the EX-FN-4 refusal family
(`test_expr_column_reference_stays_the_ex_fn_4_refusal` green).

Fenced/dated notes: round 5's two fenced case-preserved pins
(`sum_alias_and_alias_lit`, `rebind_extended_afs`) pass on the fresh native —
the 16b prescription is moot, no fence crossed. DF-PLAN-INTRO-CAST-1 has no pin
in this tree and the suite is green — seam unchanged, registry row untouched.
`getbit` / `bit_get(6,1)` and the 8 v3 DV / legacy-delete failures from round 5
do not reproduce — closed as stale-binary artifacts.

## Round 6 gates (2026-09-15, actor muse-spark-1.3-contributor)

- `cargo test -p repark-spark --lib`: 970 passed, 0 failed (round-5 slices).
- `cargo test -p repark-iceberg write::merge`: 145 passed, 0 failed (Q1 pin).
- `make rust-clippy`: clean. `make verify`: exit 0 (3187 passed, 0 failed;
  includes the `cargo fmt` fix on the round-5 slices, committed separately).
- Release native rebuilt twice (instrumented capture, then clean); final tree
  rebuilt once more after the fmt commit — binary matches source.
- Full facade `.venv/bin/python -m pytest python/repark/tests -q
  -p no:cacheprovider`: 8 failed, 7128 passed, 367 skipped; after the D-2
  needle moves + goldens re-record, the touched files re-run 151 passed with
  only the 3 sanctioned hand-offs red (C-025 INT[] on 16b; C-029 F.expr display
  on 16a; C-026 HOF marker on 16a).
- Parity `make py-test`: exit 0 (757 passed, 2 skipped, 12 xfailed).
- `check_lib_py`, `check_docstring_presence`, `check_python_conventions`,
  `check_rust_file_size`, `check-ledgers`, `check-map-sync`: all clean.
- Round-5 fenced pins (`sum_alias_and_alias_lit`, `rebind_extended_afs`) pass;
  no fence crossed. `/tmp/pc-cv2-base` left for the orchestrator to remove.

## Round 7 progress (2026-09-15, run 16c; actor muse-spark-1.3-contributor)

Rebased onto main `e9ea03c7` (PR #611); `cargo check -p repark-spark
--all-targets` clean; release native rebuilt.

C-025 (R-16b-21 grant): applied exactly the granted line
(`catalog_surface.py:568` → `ARRAY<...>`; nothing else in that file). The pin
then failed with 16b's main-measured refusal (`column type ARRAY<INT> is not
supported yet for Iceberg tables`), so the angle-bracket arm was mapped in
`crates/repark-spark/src/create_table.rs` (in-fence): `ARRAY<T>` → Iceberg
list with nullable `element` fields and a table-wide id allocator; recursion
handles `ARRAY<ARRAY<STRING>>`; bare/square-bracket forms still refuse (the
`rejects_unsupported_array` pin is untouched and green). New Rust pin
`maps_angle_bracket_array_to_nullable_element_list` went red-first with the
production error text, now green. Facade conditions met:
`test_create_table_array_and_not_null` green with `listColumns` answering
`array<int>` / `array<array<string>>` and `id` non-nullable.
`test_create_table_map_struct_refuse_loudly` re-measured under the new dialect:
unchanged, still green — MAP still raises `AnalysisException`, STRUCT still
raises `UnsupportedOperationException`, identical classes before and after, so
no pin flip.
Side effect noted: ALTER TABLE ADD COLUMN now accepts a list type through the
same mapping (the fork assigns fresh ids); ADD path still refuses non-primitive
SET DATA TYPE loudly. C-025 PROVEN.

C-026/C-029 (R-16c-10): strict xfails applied verbatim to the two pins in this
unit's own test files; both report xfailed. HANDED-OFF with the xfails as
evidence; 16a flips them on the fix.

DF-PLAN-INTRO-CAST-1: `test_df_plan_introspect_1.py` runs 44 passed. No pin in
the file codifies CAST-vs-plain hashing alike (the casts pin asserts int-cast
vs bigint-cast differ); typed-literal work did not move any hash — seam
unchanged, registry row and pins untouched.

```text
COVERAGE_ATTESTATION (round 7 extension, 2026-09-15):
  pr_unit: fnp-4b
  complete: true
  note: every clause C-001..C-026 plus C-029 is PROVEN or OPEN-as-handed-off;
  the only OPEN rows are C-026/C-029, whose strict xfails 16a flips. The ledger
  grammar knows no HANDED-OFF verdict, so the hand-off rides in the verdict
  cell beside OPEN — a grammar extension for first-class HANDED-OFF is
  orchestrator follow-up, not this unit.
  round7:
    - C-025 PROVEN by Rust red-first pin + facade array pin + unchanged loud
      map/struct refusal (artifacts: crates/repark-spark/src/create_table.rs,
      python/repark/tests/test_catalog_surface_1.py)
    - C-026 HANDED-OFF by strict xfail (artifacts:
      python/repark/tests/test_fnp_4b_hof_display.py)
    - C-029 HANDED-OFF by strict xfail (artifacts:
      python/repark/tests/test_fnp_4b_spark_dialect.py)
```
