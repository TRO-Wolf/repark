# Charter ledger — LOGICAL-WIDTH-1 · Spark's logical widths on the facade

**Date:** 2026-09-16 · **Branch:** `feat/logical-width-1` · **Base:** `33c87cbf41080e97d3c48ef2b62ee776de61e996` · **Model:**
muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `LOGICAL-WIDTH-1` (implemented) and `DF-LIT-BINARY-1` (BACKLOG, filed here) at the
`LOGICAL-WIDTH-1` section end of
[../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

**Originating ruling Q-16b-3 (owner, 2026-09-15 evening, binding).** "LOGICAL-WIDTH-1 is
scheduled: the facade reports smallint/tinyint as int, float as double and binary as string —
Spark's widths on both doors, in Rust." Recorded as R-1 below with Q-17a-2 and Q-17a-4.

**Why now.** The 1.5 PySpark-parity campaign (owner, 2026-09-14: full Spark parity is in the 1.5
release). The engine already carries the narrow Arrow types end to end (M-1); only the facade
label widens them (M-2). This unit moves the four widths through `dtypes` / `schema` /
`printSchema` / `schema.json()` on both doors, keeps `fillna` width-preserving, and pins the
Iceberg smallint/tinyint widening that matches Spark.

**Not in this unit:** nullability (W-7, recorded only); `toPandas` dtypes (M-4(e), unmeasured —
no pandas in the oracle env); `CAST … AS BINARY` under ANSI (W-6 withdrawn, BL-11 landed);
`functions*.py`, the Rust function registry, `function_dispatch.rs` (run 18a); the SQL
parser/dialect/router in `crates/repark-spark` (run 18c) beyond `type_table.rs` + its tests.

## Measured starting point (card M-1..M-5 — verified on this tree, not re-derived)

- M-1 VERIFIED: `df.to_arrow().schema` on this tree answers `int16 / int8 / float / binary`
  for the four-width frame (probe 2026-09-16: `sh: int16, ti: int8, f: float, b: binary`).
  The engine is exact; this unit does not re-type it.
- M-2 VERIFIED: `crates/repark-spark/src/type_table.rs::arrow_name_at_depth` widens
  Int8/Int16→`int`, Float16|Float32→`double`, Binary|LargeBinary|BinaryView→`string` on the
  `LogicalKey` surface; `logical_type_key` (~line 215) is its only entry point and its only
  caller is `crates/repark-python/src/dataframe.rs::arrow_type_key` (~line 155) feeding
  `logical_schema_fields()`. Base probe: `dtypes == [('sh','int'),('ti','int'),('f','double'),
  ('b','string')]` for the four-width frame.
- M-3 VERIFIED: `python/repark/src/repark/spark/dataframe/core.py::DataFrame.schema` (~line
  2193) already decodes `"short"` → `ShortType`, `"byte"` → `ByteType`, `"float"` →
  `FloatType`, `"binary"` → `BinaryType`. The LogicalKey arms must emit those four tokens.
- M-4(a) fillna: `DataFrameNaFunctions` (`actions_export.py`) composes `F.coalesce(bound,
  F.lit(value))` in Python; there is NO Rust fillna/na-fill plan builder in this tree
  (`grep "fn fill" crates/repark-python crates/repark-core` is empty). The width decision is
  the `_fill_expr_for_bound` cast arm, which today only re-casts `int`/`long`. See R-9.
- M-4(b) lit(bytes): `F.lit` in `python/repark/src/repark/spark/functions.py` (~line 143)
  raises `PySparkTypeError` for `bytes` BEFORE reaching `PyColumn.literal`; the file belongs
  to run 18a today. BACKLOG per the card. See C-008.
- M-4(c) `schema.json()`: `StructType.json` renders `typeName()`, which derives from the same
  `StructType` the fixed keys build — falls out of the M-2 fix. Pinned, then confirmed.
- M-4(d) RE-MEASURED 2026-09-16 on this tree: `F.col(bigint).cast("binary")` under ANSI
  raises `AnalysisException` with `DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION` and the conf
  suggestion text — BL-11 (#641) closed it. Regression guard only (C-009).
- M-4(e) `toPandas`: Spark cell `todf_toPandas` is a `PACKAGE_NOT_INSTALLED` error cell —
  unmeasured. OUT OF SCOPE; pin nothing.
- M-5 VERIFIED from the recorded files: Spark's own Iceberg read answers `sh:int, ti:int`
  (Iceberg has no narrow ints — Spark widens at the boundary too), `price:float`,
  `b:binary` (cells `iceberg_schema`, `iceberg_desc`); repark main answers `sh:int, ti:int`
  (right), `price:double, b:string` (wrong) plus matching rows (cell `iceberg_roundtrip` in
  `width_repark_main_2026-09-15.json`).

## PROPOSITION LEDGER — LOGICAL-WIDTH-1 — 2026-09-16

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The Python door reports the four widths: `createDataFrame` with a DDL string and with a `StructType` answers `dtypes` smallint/tinyint/float/binary, `simpleString` struct spellings, `json_types` short/byte/float/binary, `printSchema` short/byte/float/binary, and `schema.json()` short/byte/float/binary. | `test_logical_width_1.py` ddl/struct/schema-json pins from cells `ddl_schema`, `struct_schema`, `schema_json`. | OPEN | Red-first run §Red-first. |
| C-002 | The SQL door reports the four widths: `spark.sql("SELECT CAST(1 AS SMALLINT) a, …")` answers `.dtypes` / `.schema` smallint/tinyint/float/binary, and `DESCRIBE TABLE` answers are unchanged. | sql-cast + describe pins from cells `sql_cast`, `sql_describe`. | OPEN | Red-first run §Red-first. |
| C-003 | Widths survive cast, arithmetic, aggregates and union (`arith_width`: `smallint+tinyint→smallint`, `float*2→float`; `agg_width`: `max(tinyint)→tinyint`, `min(float)→float`; `union_width`; `cast_schema` guard). | arith/agg/union/cast pins from the same-named cells. | OPEN | Red-first run §Red-first. |
| C-004 | `fillna(0)` keeps `smallint, tinyint, float` (cell `fillna_width`) with Spark's values (cell `fillna_values`). | fillna pins from both cells. | OPEN | No Rust fillna builder exists (M-4(a)); fix extends the `_fill_expr_for_bound` cast arms over the Rust `lit`/`cast`/`coalesce` kernels (R-9). |
| C-005 | Inference: Python `int→bigint`, `float→double`, `bytes→binary` (cell `infer_schema`); nested widths already green, pinned as guard (cell `nested_schema`); `collect()` value types/values already green, pinned as guards (cells `ddl_collect_types`, `ddl_collect_values`). | infer/nested/collect pins. | OPEN | Inference already builds `pa.binary()`; only the label was wrong. |
| C-006 | A parquet write/read round trip keeps the four widths (cell `write_read_parquet`). | parquet pin. | OPEN | |
| C-007 | An Iceberg round trip (memory catalog + warehouse, per `probe_width.py` repark mode) answers `sh:int, ti:int, price:float, b:binary` with the recorded rows. | iceberg pins from Spark cells `iceberg_schema`/`iceberg_desc`/`iceberg_rows` + repark cell `iceberg_roundtrip`. | OPEN | Spark widens narrow ints at the Iceberg boundary too — the `int` answer is Spark-matching, not a residual. |
| C-008 | `F.lit(b"ab")` keeps today's `PySparkTypeError`, pinned; registry row `DF-LIT-BINARY-1` (BACKLOG) records the gap with the pin. | lit pin from cell `lit_width` (error cell). | OPEN | The bytes arm needs `functions.py` (run 18a); not edited (R-8). |
| C-009 | The `cast_schema` ANSI refusal stays: `CAST(bigint AS BINARY)` raises `DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION` (BL-11, W-6 withdrawn). | refusal pin from cell `cast_schema` (error cell). | OPEN | Re-measured on this tree 2026-09-16 (M-4(d)). |
| C-010 | Blast radius (W-3): base vs M-2-fix-alone suite lists written here; every changed pin classified (i) asserted the wrong widened answer, re-measured against a named Spark cell, or (ii) product regressed, product fixed. No pin updated without a named cell. | §Blast-radius lists + the reclassified-pin table. | OPEN | Base runs §Gates. Halt line: ~40 pins. |
| C-011 | Registry rows `LOGICAL-WIDTH-1` (implemented) + `DF-LIT-BINARY-1` (BACKLOG) appended inside their section, never reordered; every gate in the preamble green with real exit codes and counts; COVERAGE_ATTESTATION complete. | Registry diff; §Gates. | OPEN | Rebase before every gate run; keep BOTH sides on conflict. |

Note (not a clause): `toPandas` dtypes unmeasured — Spark cell `todf_toPandas` records
`PACKAGE_NOT_INSTALLED`; repark main answers float64/object per its own file. No pin.

## Rulings

- R-1 = Q-16b-3 (owner, 2026-09-15): this unit exists; the fix is Rust; both doors.
- R-2 = Q-17a-2 (owner, 2026-09-16): RUST FIRST — kernels, conversions, planner and type rules
  land in Rust; Python holds names, argument shapes and API plumbing only; one ledger line per
  piece that stays in Python, with the reason (§Rust-first roll-call).
- R-3 = Q-17a-4 (2026-09-16): the WHOLE suites, never a subset; each run ONCE into a log.
- R-4 = W-1: touch ONLY `crates/repark-spark/src/type_table.rs` + its tests in that crate.
  Nothing else in `crates/repark-spark` (no parser, dialect, router).
- R-5 = W-2: LogicalKey vs Describe — DECIDED: keep both surfaces. After the fix they still
  differ (`bigint`/`long`, `smallint`/`short`, `tinyint`/`byte`); Describe answers must not
  change (`spark_ddl_type_name` pins re-run).
- R-6 = W-3: blast-radius procedure (§Blast-radius); halt past ~40 reds.
- R-7 = W-4/W-5: pin coverage list (= C-001..C-009).
- R-8 = W-1 + M-4(b): `functions.py` / registry / `function_dispatch.rs` untouched (run 18a);
  `lit(bytes)` is BACKLOG `DF-LIT-BINARY-1`, not a silent stub — the pin asserts the error.
- R-9 = M-4(a) deviation, recorded not hidden: the card orders a Rust fillna fix, but this
  tree has no Rust fillna/na-fill plan builder — `na.fill` composes the Rust `lit`, `cast`
  and `coalesce` kernels from `actions_export.py`. The width decision (which key the literal
  is cast to) is argument plumbing over those kernels, so the cast-arm extension stays in
  Python. No new kernel, no Python compute. Rust-first roll-call line 5.
- R-10 = W-6: WITHDRAWN — no `CAST-ANSI-BINARY-1` row; `cast_schema` is a guard (C-009).
- R-11 = W-7: nullability out of scope — differing `nullable` lists recorded, never chased.

## Rust-first roll-call

| Piece | Home | Why it is there |
|---|---|---|
| LogicalKey narrow arms (`short`/`byte`/`float`/`binary`) | `crates/repark-spark/src/type_table.rs` (Rust) | Type rule; reaches the SQL door via `logical_type_key`. |
| `DESCRIBE` spelling stability | same file, Describe surface untouched (Rust) | Same type table; pinned by existing tests. |
| fillna literal cast to the column's own width | Rust `lit`/`cast`/`coalesce` kernels, composed in Python | No Rust fillna builder exists; the kernels already land in Rust. |
| `DataFrame.schema` key decode (`short`/`byte`/…) | already in `core.py` (Python, pre-existing) | Name/shape plumbing over `logical_schema_fields`. |
| `F.lit(bytes)` refusal text | `functions.py` (run 18a, untouched) | API plumbing owned by another lane; BACKLOG row. |

## Red-first

Base-tree probe (2026-09-16, RELEASE module at `33c87cbf`, before any product edit):

```text
dtypes: [('sh', 'int'), ('ti', 'int'), ('f', 'double'), ('b', 'string')]
```

for `spark.createDataFrame([(3,1,1.5,b"ab")], 'sh smallint, ti tinyint, f float, b binary')`
against Spark cell `ddl_schema` (`smallint/tinyint/float/binary`). Every C-001..C-007 pin
asserts the Spark cell value, so each reds on this base. C-008 pins today's
`PySparkTypeError` (green on base by construction — BACKLOG guard). C-009 pins the
re-measured BL-11 refusal (green on base — regression guard).

## Blast-radius (W-3 — M-2 fix alone, no other product edit)

Base (commit `33c87cbf`):

```text
facade: 9035 passed, 367 skipped, 34 xfailed — exit 0 (blast_base_facade.log)
parity: 756 passed, 2 skipped, 12 xfailed + 1 failed — exit 1 (blast_base_parity.log);
  the 1 failure is test_dl_6_docs_links.py::test_real_tree_is_green_under_the_seeded_allowlist,
  which reds ONLY because this unit's own fixture + ledger exist but are untracked yet
  ("facade_logical_width_oracle.json -> exists but is not tracked",
  "logical-width-1-ledger.md -> exists but is not tracked"). Self-inflicted; resolves at commit.
```

With the M-2 `type_table.rs` fix alone (rebuilt RELEASE, no pin edits):

```text
facade: 14 failed, 9021 passed, 367 skipped, 34 xfailed — exit 1 (blast_fix_facade.log)
parity: 756 passed + the same self-inflicted docs-link failure — exit 1 (blast_fix_parity.log)
```

14 < ~40: no HALT. Classification — every one is class (i), the pin asserted repark's wrong
widened answer and the new answer matches the named Spark cell:

| # | Pin | Old assert (wrong) | New answer | Spark cell justifying the new value |
|---|---|---|---|---|
| 1 | df_surface_a_1::test_to_narrow_reports_logical_width_1 | `struct<a:int>` | `struct<a:smallint>` | `ddl_schema` (ShortType→smallint) |
| 2 | df_surface_a_1::test_to_binary_follows_reported_schema_df_to_binary_1 | `struct<b:string>` | `struct<b:binary>` | `ddl_schema` (b→binary); DF-TO-BINARY-1 row states this pin "reds on purpose" when the binary report lands |
| 3 | facade_3 goldens `bin_*`/`tup_*` (9 cases) | `struct<_1:string>` | `struct<_1:binary>` | `infer_schema` (bytes→binary); rows/values byte-identical, label-only diff |
| 4-6 | facade_4 census D7/D8/D9 `reader` | string/double/int | binary/float/tinyint+smallint | `infer_schema` (b→binary), `ddl_schema` (f→float), `struct_schema` (sh→smallint, ti→tinyint) |
| 7-8 | facade_4 census D18/D19 `table_dtypes` (+`table_key`) | double/string | float/binary | `iceberg_schema` (price→float, b→binary) |
| 9-12 | io_text_2 probe6 float/smallint/tinyint/binary | `k:double/int/string` | `k:float/smallint/tinyint/binary` | LOGICAL-WIDTH-1 registry row lists the first three "red when fixed"; binary per `ddl_schema` |
| 13 | nullability_2::test_cast_nullability_matches_spark `_CAST_FLAG_ROWS` | `ts_to_short→int`, `ts_to_byte→int` | smallint, tinyint | `sql_cast` (CAST→smallint/tinyint) |
| 14 | nullability_2::test_narrow_logical_widths_report_wide_per_logical_width_1 | int/int/double | smallint/tinyint/float | `ddl_schema`; registry row names this pin "red when fixed" (name kept, body updated) |

Zero class (ii): no product regression. Parity cohort: zero pins changed.

FINDING F-1 (new engine divergence, exposed by the fix): `arith_width.f2` (`float * int`
literal) now reports `float` where Spark cell `arith_width` records `double` — on main it
matched only by accident of the widened display. Spark widens float×int to double; the engine
answers float32. Out of scope (needs a DataFusion coercion rule); recorded as BACKLOG registry
row `ARITH-FLOAT-INT-1` with a red-when-fixed divergence pin. `st` (smallint) and `fd`
(double) match and are pinned from the cell.

## Gates

(TODO: every gate with real exit codes and counts.)

## Questions

(None yet.)

```yaml
COVERAGE_ATTESTATION:
  pr_unit: logical-width-1
  categories: []
  complete: false
```

VERDICT: 11 clauses, 0 PROVEN, 11 OPEN, 0 REJECTED.
