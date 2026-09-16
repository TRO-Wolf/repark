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
| C-001 | The Python door reports the four widths: `createDataFrame` with a DDL string and with a `StructType` answers `dtypes` smallint/tinyint/float/binary, `simpleString` struct spellings, `json_types` short/byte/float/binary, `printSchema` short/byte/float/binary, and `schema.json()` short/byte/float/binary. | `test_logical_width_1.py` ddl/struct/schema-json pins from cells `ddl_schema`, `struct_schema`, `schema_json`. | **PROVEN** | Red 2026-09-16: 13 failed on the stashed base (incl. all C-001 pins); green with the fix. `schema.json()` byte-equals the cell. pins: logical-width-1/C-001 |
| C-002 | The SQL door reports the four widths: `spark.sql("SELECT CAST(1 AS SMALLINT) a, …")` answers `.dtypes` / `.schema` smallint/tinyint/float/binary, and `DESCRIBE TABLE` answers are unchanged. | sql-cast + describe pins from cells `sql_cast`, `sql_describe`. | **PROVEN** | Red on base, green with the fix. DESCRIBE pins re-run green (`spark_ddl_type_name` Rust battery + census describe rows + Iceberg describe subset). pins: logical-width-1/C-002 |
| C-003 | Widths survive cast, arithmetic, aggregates and union (`arith_width`: `smallint+tinyint→smallint`; `agg_width`: `max(tinyint)→tinyint`, `min(float)→float`; `union_width`; `cast_schema` guard). | arith/agg/union/cast pins from the same-named cells. | **PROVEN** | `st` smallint, `fd` double, agg/union full-cell match. EXCEPTION: `float*2` answers `float`, Spark records `double` — finding F-1, BACKLOG `ARITH-FLOAT-INT-1` with divergence pin, not absorbed. pins: logical-width-1/C-003 |
| C-004 | `fillna(0)` keeps `smallint, tinyint, float` (cell `fillna_width`) with Spark's values (cell `fillna_values`). | fillna pins from both cells. | **PROVEN** | dtypes AND values byte-match both cells with the fix. `fillna(1.5)` keeps widths with toward-zero truncation (Spark's cast-the-literal rule; values tree-measured, no cell). pins: logical-width-1/C-004 |
| C-005 | Inference: Python `int→bigint`, `float→double`, `bytes→binary` (cell `infer_schema`); nested widths already green, pinned as guard (cell `nested_schema`); `collect()` value types/values already green, pinned as guards (cells `ddl_collect_types`, `ddl_collect_values`). | infer/nested/collect pins. | **PROVEN** | Full-cell match on all five pins. Inference built `pa.binary()` all along; only the label was wrong. pins: logical-width-1/C-005 |
| C-006 | A parquet write/read round trip keeps the four widths (cell `write_read_parquet`). | parquet pin. | **PROVEN** | Full-cell dtypes + simpleString match. pins: logical-width-1/C-006 |
| C-007 | An Iceberg round trip (memory catalog + warehouse, per `probe_width.py` repark mode) answers `sh:int, ti:int, price:float, b:binary` with the recorded rows. | iceberg pins from Spark cells `iceberg_schema`/`iceberg_desc`/`iceberg_rows` + repark cell `iceberg_roundtrip`. | **PROVEN** | dtypes per-column vs `iceberg_schema`, both rows (incl. the all-NULL row) vs `iceberg_rows`, DESCRIBE types vs `iceberg_desc`. Zero Iceberg-path edits — the fix closed it. The `int` answer is Spark-matching (no narrow ints in Iceberg), not residual. pins: logical-width-1/C-007 |
| C-008 | `F.lit(b"ab")` keeps today's `PySparkTypeError`, pinned; registry row `DF-LIT-BINARY-1` (BACKLOG) records the gap with the pin. | lit pin from cell `lit_width` (error cell). | **PROVEN** | Refusal text pinned byte-exact; the Spark target dtypes (`f:double, b:binary`) asserted from the cell as the fix target. `functions.py` untouched (R-8). pins: logical-width-1/C-008 |
| C-009 | The `cast_schema` ANSI refusal stays: `CAST(bigint AS BINARY)` raises `DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION` (BL-11, W-6 withdrawn). | refusal pin from cell `cast_schema` (error cell). | **PROVEN** | Refusal at collect with the cell's condition + cannot-cast sentence + conf remedy. pins: logical-width-1/C-009 |
| C-010 | Blast radius (W-3): base vs M-2-fix-alone suite lists written here; every changed pin classified (i) asserted the wrong widened answer, re-measured against a named Spark cell, or (ii) product regressed, product fixed. No pin updated without a named cell. | §Blast-radius lists + the reclassified-pin table. | **PROVEN** | 14 changed pins, all class (i) with named cells (§Blast-radius table); zero class (ii); parity untouched. New-pin red run: 13 failed / 7 passed on the stashed base (the 7: nested/collect guards, sql_describe values, the float-fill invariant, lit + cast guards, typename spellings — green by construction). pins: logical-width-1/C-010 |
| C-011 | Registry rows `LOGICAL-WIDTH-1` (implemented) + `DF-LIT-BINARY-1` (BACKLOG) appended inside their section, never reordered; every gate in the preamble green with real exit codes and counts; COVERAGE_ATTESTATION complete. | Registry diff; §Gates. | **PROVEN** | Rows landed (`LOGICAL-WIDTH-1` implemented, `DF-LIT-BINARY-1` + `ARITH-FLOAT-INT-1` BACKLOG, `DF-TO-BINARY-1` closed by the report fix); rebased onto `a92a68db` conflict-free before the gates. pins: logical-width-1/C-011 |
| C-012 | Round 2 (A-1 per R-12): the fill expression is built by `repark_core::na_fill_expr` (cast decision from the column's own Arrow type + coalesce), bound via free `fill_expr_for_column`; `actions_export.py` holds no `type_key in (...)` branch and no `.cast(...)` for the fill value; behaviour frozen (cells `fillna_width`/`fillna_values`, every fillna pin, dict/subset/string/bool/mixed-case fills). | Rust unit test (every numeric width, float→int truncation, string untouched) + `grep -ln fillna` files + `test_logical_width_1.py` + /tmp differential probe (old vs new texts/values). | **PROVEN** | 8 core unit tests green; differential probe `/tmp/sb-r2/diff_fill.py`: 20 keys (dtypes/values/wrapper texts incl. CAST renderings, join tokens, mixed-case, origin frames, uint), 0 diffs after plan-ID normalization; all 9 fillna files green (212 passed); `uint` keeps DataFusion coercion (measured old = int32-planned, new identical — casting uint to its own type would CHANGE the planned Arrow type, so the kernel covers signed+float only); `PyColumn::expr()` verified a plain clone (twin-identity holds); dead `_fill_expr` removed while `_type_keys` stays — another lane pins it as a live `_inner` schema reader (`test_mapinarrow_unpersist_action_then_plan_child`), and a metadata accessor is neither a branch nor a cast. pins: logical-width-1/C-012 |

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
- R-12 = R-18b-4 (orchestrator, round 2, binding): AUDIT FINDING A-1 — the round-1 fillna
  width fix is Python-side value branching and violates Q-17a-2. The fill EXPRESSION
  (whether and to what the replacement is cast, from the target column's own Arrow type,
  plus the coalesce) is ONE Rust function in `crates/repark-core` (`na_fill.rs`),
  bound through `crates/repark-python` (free `fill_expr_for_column`), taking the
  bound column expression and the replacement value and reading the column's type from
  the plan schema in Rust. Python keeps: argument checks, subset normalization,
  column-name binding, the Column identity/alias wrapper. No `type_key in (...)` branch
  and no `.cast(type_key)` for the fill value remains in `actions_export.py`. The
  `key_to_cls` map stays: it selects projection targets by declared schema type for
  display-overlay name matching — name binding, never a value — one ledger line, no move.
  Behaviour frozen: cells `fillna_width` / `fillna_values` and every fillna pin stay green.
  Touch nothing outside A-1 this round.

## Rust-first roll-call

| Piece | Home | Why it is there |
|---|---|---|
| LogicalKey narrow arms (`short`/`byte`/`float`/`binary`) | `crates/repark-spark/src/type_table.rs` (Rust) | Type rule; reaches the SQL door via `logical_type_key`. |
| `DESCRIBE` spelling stability | same file, Describe surface untouched (Rust) | Same type table; pinned by existing tests. |
| fillna literal cast to the column's own width | `repark_core::na_fill_expr` (Rust, round 2 per R-12); R-9's composition retired | Value decision (cast or not, to what) is a kernel; SQL-door reachability is N/A (Spark has no SQL fillna). |
| fillna target-column selection (`key_to_cls`, family isinstance checks) | `actions_export.py` (Python, stays per R-12) | Selects projection targets by declared schema type for name matching; never raises, casts, coerces or branches on a value. |
| fillna texts (`coalesce(...)` / `CAST(... AS ...)` wrapper strings) | Rust binding renders cast text via `wrap_cast`; Python composes `coalesce(...)` from child parts (plumbing) | Byte-identity with round-1 proven by the /tmp differential probe, not by review. |
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

Final tree after rebase onto `a92a68db` (2026-09-16; the two upstream commits are
docs-only, so no rebuild was needed — the RELEASE module already matches this tree):

| gate | result |
|---|---|
| `make verify` | exit 0 (lint, format, clippy, Rust tests, map/ledger grammar) |
| unit file `test_logical_width_1.py` | 20 passed |
| reclassified files (`test_nullability_2.py`, `test_df_surface_a_1.py`, `test_facade_4_census_pins.py`, `test_facade_3_create_dataframe_goldens.py`, `test_io_text_2.py`) | 137 passed with the unit file (117 + 20), 6 skipped |
| whole facade `.venv/bin/python -m pytest python/repark/tests -q -p no:cacheprovider` | 9055 passed, 367 skipped, 34 xfailed — exit 0 |
| whole parity `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q` | 757 passed, 2 skipped, 12 xfailed — exit 0 |
| `cargo test -p repark-spark` | 1025 passed, 0 failed, 4 ignored |
| comment-ban `grep -P '^\+\s*(//|#(?!\[|!\[\| noqa))'` before every commit | empty every time |
| red-first new pins on stashed base | 13 failed, 7 passed (guards green by construction) |
| base suites (W-3) | facade 9035 passed exit 0; parity 756 passed + 1 self-inflicted docs-link fail (untracked fixture/ledger), resolved at commit |
| M-2-fix-alone suites (W-3) | facade 14 failed / 9021 passed; parity unchanged; all 14 class (i) |
| round-2 `make verify` | exit 0 (incl. clippy, rustfmt, ledger grammar with C-012) |
| round-2 whole facade | 9057 passed, 367 skipped, 34 xfailed — exit 0 (one self-caught red mid-round: `test_mapinarrow_unpersist_action_then_plan_child` pinned deleted `_type_keys`; helper restored as a schema reader, file green, suite re-run clean; +2 tests vs round 1 are the rebased main's `subq-cells-1` pins) |
| round-2 whole parity | 757 passed, 2 skipped, 12 xfailed — exit 0 |
| round-2 `cargo test -p repark-core na_fill` | 8 passed |
| round-2 differential probe | 20 keys, 0 diffs |

## Questions

(None — no HALT. The two card ambiguities resolved by measurement: no Rust fillna
builder exists (R-9), and `lit(bytes)` cannot land without run 18a's file (R-8).)

## Out-of-scope measurements (W-7, M-4(e))

- `fillna(0)` nullability: Spark cell `fillna_width` records `sh/ti` nullable true;
  repark answers non-nullable (coalesce with a literal folds nullability away). Recorded,
  not chased (W-7). The pin asserts dtypes + values only.
- Iceberg round-trip nullability: Spark `iceberg_schema` records `id` non-nullable; repark
  answers all-nullable. Recorded, not chased (W-7).
- `toPandas`: Spark cell `todf_toPandas` is `PACKAGE_NOT_INSTALLED` — unmeasured, no pin
  (M-4(e)). Repoint to the next oracle round with pandas installed.
- `to(string)` on a binary column answers `'hi'` (engine cast) — tree-measured; the VALUE
  is unrecorded on live Spark (DF-TO-BINARY-1 notes it for the next oracle round).

```yaml
COVERAGE_ATTESTATION:
  pr_unit: logical-width-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the card M-1..M-5, rulings W-1..W-7 and the recorded PySpark 4.1.2 width cells; the M-1/M-4(a) claims verified by measurement on this tree (engine int16/int8/float/binary; no Rust fillna builder), and the card's float*2-matches-Spark claim REFUTED by cell arith_width (finding F-1, filed ARITH-FLOAT-INT-1).
      artifacts: [python/repark/tests/test_logical_width_1.py, python/repark/tests/facade_logical_width_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: DDL-string, StructType and inferred frames; SQL-door CAST and hex literal; Python-door cast; arithmetic, aggregates, union; scalar, dict and float fillna; parquet and Iceberg round trips; schema.json bytes; lit(bytes) refusal; ANSI cast-to-binary refusal; typename spellings; DESCRIBE unchanged on both surfaces.
      artifacts: [python/repark/tests/test_logical_width_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Rust tests logical_key_reports_spark_narrow_widths + describe_keeps_spark_ddl_spellings_for_narrow_widths in type_table/tests.rs; full repark-spark suite 1025 passed; no unwrap/expect added (make verify clippy clean).
      artifacts: [crates/repark-spark/src/type_table/tests.rs]
    - id: AT-4
      status: N/A
      justification: Pure type-label and literal-cast changes; no shared mutable state, no new tasks or locks.
    - id: AT-5
      status: ATTACKED
      evidence: No endpoint, file or catalog write added — parquet/Iceberg pins write only to tmp_path warehouses; the refusals (lit, ANSI cast) raise before any plan executes.
      artifacts: [python/repark/tests/test_logical_width_1.py]
    - id: AT-6
      status: ATTACKED
      evidence: Single-agent round, no critic phase ordered; self-review caught the D9 key-vocabulary slip (byte/short keys vs tinyint/smallint labels) and the iceberg d-vs-date name collision before the gates.
      artifacts: [task/ledgers/staging/logical-width-1-ledger.md]
    - id: AT-7
      status: ATTACKED
      evidence: Round-2 tree: make verify exit 0; facade 9057 passed, 367 skipped, 34 xfailed, exit 0; parity 757 passed, 2 skipped, 12 xfailed, exit 0; unit file 20 passed; core na_fill 8 passed; differential probe 20 keys 0 diffs; comment-ban grep zero hits. Counts in §Gates.
      artifacts: [python/repark/tests/test_logical_width_1.py]
    - id: AT-8
      status: ATTACKED
      evidence: Every Spark expectation comes from facade_logical_width_oracle.json (byte-identical copy of the run-17b live recording, cmp clean); the three tree-measured values (fillna(1.5) truncation, to(string) 'hi', lit refusal text) are labelled tree-measured in §Out-of-scope measurements, never presented as oracle.
      artifacts: [python/repark/tests/facade_logical_width_oracle.json]
    - id: AT-9
      status: ATTACKED
      evidence: LOGICAL-WIDTH-1 implemented with cell citations; DF-LIT-BINARY-1 and ARITH-FLOAT-INT-1 BACKLOG with red-when-fixed pins; DF-TO-BINARY-1 closed by the report fix with the unmeasured value noted; no silent stubs.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Mutation guards observed — stashing the LogicalKey fix reddened 13 pins (red-first run); restoring the int/long-only fill arm reddened both fillna pins (2 failed, 18 deselected); the lit BACKLOG guard asserts the refusal text, so a bytes arm without retiring it reds.
      artifacts: [python/repark/tests/test_logical_width_1.py]
  complete: true
```

VERDICT: 12 clauses, 12 PROVEN, 0 OPEN, 0 REJECTED.
