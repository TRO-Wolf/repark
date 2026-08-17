# Unit ledger — conductor-25 bug-candidate fix round (B4/B1/B6/B2/B3/B5)

**Unit:** conductor-25 · **Date:** 2026-08-17 ·
**Lane:** repark · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-c25` · **Branch:** not cut (recon, detached `40e353f`) ·
**Base:** origin/main `40e353f` (#171; matches GitHub `main` at recon start).
Primary checkout SSH fetch failed; SHA confirmed via `gh api repos/TRO-Wolf/repark/commits/main`.

**Charter:** `BRIEF-conductor-25.md` + Round-1 Q&A A1–A11 (binding).
Code PRs wait for `GO for conductor-25` in `sync_file.md`. This file is the
single ledger (A7); oracle evidence is dated sections here, not separate
`task/c25-*-oracle.md` files. Transient scripts live in `/tmp/grok-c25-scratch/`
(uncommitted).

**Oracle env (A7):** `/tmp/grok-c25-pyspark` (`pyspark==4.1.2`),
`JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64`, `SPARK_LOCAL_IP=127.0.0.1`,
`local[2]`, `spark.sql.shuffle.partitions=2`. JVM lock
`/tmp/grok-jvm-record.lock` held for the record window (marker
`lane=conductor-25`).

---

## 0. Recon 2026-08-17 — pin localization + Spark oracles

### 0.1 Work order (unchanged)

B4 → B1 → B6 → B2 → B3 → B5. One PR per item. After GO, B4–B3 do not wait
on conductor-21; B5 stays last (shared `crates/repark-python/src/session.rs`).

### 0.2 B4 — smartCsv delimiter auto-detect (facade-pure)

**Pin:** `python/repark/tests/test_datasets_facade.py::test_smartcsv_delimiter_autodetect_picks_a_rival_delimiter`

**Site:** `python/repark/src/repark/spark/_csv_smart.py` `detect_delimiter`
scores `(agreement, mode_fields)`. `csv.reader` only honors a quote that
*starts* a field.

**Reproduced** against the real DS-4 generator (`write_files(rows=64, seed=42)`),
importing `_csv_smart` without the native module:

| scheme | auto delimiter | auto data rows | declared delimiter | declared rows |
|---|---|---|---|---|
| comma | `;` | 63 | `,` | 64 |
| semicolon | tab | 63 | `;` | 64 |
| tab | `;` | 63 | tab | 64 |
| pipe | `;` | 63 | `\|` | 64 |

Matches the pin's `misdetected` map and `ROWS - 1` (header eaten under the
wrong split). A 12-column zoo *without* ragged rows does **not** reproduce:
true-delimiter agreement (header+N) beats rival 2-field agreement (N). Ragged
short/long rows drop the true delimiter's 12-field agreement below the rival's
perfect 2-field agreement on every data line (embedded rival lives in
`embedded_delims`, kept on short rows).

**Fix sketch (after GO):** change the score so a high field-count split can
beat a 2-field perfect-agreement rival (or require the header line to join
the winning mode). Facade-only. Flip the pin in place (A5).

### 0.3 B1 — euro-comma decimal (oracle BOTH surfaces — A4)

**Pin:** `test_smartcsv_euro_comma_decimal_cast_refuses_loud` — inference
resolves `euro_decimal` to `decimal128(5,2)`, then materialize CAST of the
raw cell `'760,35'` refuses.

**Site:** `_csv_smart.py` `_decimal_from_text` (lines 209–211) normalizes a
euro-comma token to `.` for the **inference** ladder; `load_smart_csv`
(lines 871–884) then `CAST(... AS DECIMAL)` the **raw** cell through the
engine.

**Spark CAST (`pyspark==4.1.2`):**

| ANSI | SQL | result |
|---|---|---|
| off | `CAST('1.234,56' AS DECIMAL(10,2))` | NULL, type `decimal128(10,2)` |
| off | `CAST('760,35' AS DECIMAL(5,2))` | NULL, type `decimal128(5,2)` |
| on | same literals | `NumberFormatException` `[CAST_INVALID_INPUT]` malformed |

Spark does **not** locale-parse. Per A4: the kernel learns nothing new.

**Spark CSV (quoted cells `"760,35"` / `"1.234,56"`):**

| ANSI | path | result |
|---|---|---|
| off/on | `inferSchema` | `euro_decimal: string` — values kept as text |
| off/on | schema `DECIMAL(10,2)` | `760,35` → `76035.00` (comma = grouping); `1.234,56` → `1.23` (`.` decimal, scale-2) |
| off/on | schema `DECIMAL(5,2)` | `760,35` → NULL (overflow); `1.234,56` → `1.23` |

**Disposition lean (after GO):** do not teach a CAST kernel euro-comma.
The unique defect is the smartCsv ladder promising `decimal128` it cannot
materialize. Fix the ladder (stop classifying `_DECIMAL_COMMA_RE` as
`decimal128`, fall through — Spark infer stays string) **or** normalize
cells in Python *before* `selectExpr` CAST so the documented protocol is
honest. Registry payload if we keep infer+refuse as POLICY. Record this
as the B1 decision in the B1 PR section.

### 0.4 B6 — dotted-path `df.select("p.a")` / `df["r.a"]`

**Existing pin:** smoke_suite known-FAIL
`pyspark.sql.tests.test_column.ColumnTests.test_field_accessor` (A6:
census meta stays stable unless this known-FAIL actually flips).

**Site:** `DataFrame._resolve_getitem_column_name` (core.py:3545) matches
top-level `columns` only. `select("p.a")` / `df["r.a"]` look for a column
literally named `p.a` and raise `A column with name \`p.a\` cannot be
resolved`. `Column.getField` / `df.r.a` already work.

**Spark 4.1.2 oracle (NOT the brief's hypothesized top-level-name-wins):**

- Frame schema `p: struct<a:int, b:string>`:
  - `select("p.a")` → column `a`, value `1`
  - `select(col("p.a"))` / `df["p.a"]` → same
- Frame with **both** struct `p` and top-level column named `p.a` (value 99):
  - unquoted `select("p.a")` / `col("p.a")` → struct field `a` (value 1)
  - backticks `select("\`p.a\`")` / `col("\`p.a\`")` → top-level `p.a` (value 99)

**Dotted-path wins for unquoted names; backticks select the literal
top-level name.** A "top-level-name-wins" fix would be half-true — HALT
if a correct resolve looks deeper than this choke point. Lean after GO:
extend `_resolve_getitem_column_name` / `_bind_schema_column` (facade),
hazard-named pins in `python/repark/tests/`.

### 0.5 B2 — `explode_outer` on `array<struct>`

**Pin:** `test_nested_explode_outer_on_array_of_struct_refuses_loud`
(`explode_outer cannot resolve SQL element type` + `array<struct<leg_id:bigint`).

**Site:** `DataFrame._array_element_sql_type` → `_spark_array_element_to_sql`
(`plan_collapse.py:663`). Mapping covers scalars/decimal/nested array only;
`struct<…>` returns `None` → AnalysisException. Plain `explode` skips
element-type resolution (comment at core.py:3363–3364).

**Spark 4.1.2 oracle:** `explode_outer("Legs")` on
`array<struct<leg_id:int>>` **works**:

- row with two structs → two rows
- null array → one row, `leg=None`
- empty array → one row, `leg=None`
- plain `explode` drops the null/empty rows

Real divergence. Fix: map struct (and map?) element types to a DataFusion
SQL `STRUCT(...)` / `NULL` guard type, or use a type-preserving
`make_array` that does not need a spelled SQL type. Engine-side generator
path as briefed.

### 0.6 B3 — `count()` on deep `dynamicFlatten`

**Pin:** `test_nested_dynamic_flatten_count_action_refuses_loud` —
`to_arrow()` works; `count()` raises inside `push_down_leaf_projections`
(qualified `<explode-alias>."Legs"` vs unqualified `Legs`).

**Suspects (A9: follow evidence, one PR):**
`plan_collapse.py` already documents that native `get_field` projections
over createDataFrame MemTables leave qualified leaf names that poison
multi-pass unnest under `push_down_leaf_projections` (lines 819–821).
Shallow one-pass flatten and a plain explode both `count()` fine.

**Spark 4.1.2 oracle (two-level explode_outer analog):** `count()` = 3,
matches collected rows (including the null-Fills row). Real divergence.

### 0.7 B5 — `repark.sql()` sniff (LAST; recon only)

`PyReparkSession.native()` is a bare builder (stock DataFusion, no
`AnsiDialect`). A2 authorizes a product `repark-python → repark-sql`
`normal` edge **as part of B5** (ALLOWED-EDGES + Cargo.toml + comment
flip). Cargo.lock hunk only if it is a workspace path-dep delta; any
third-party version change → HALT. A3: dialect install + sniff
reachability only; no Iceberg-DDL residual, no catalog-config. origin/main
`lib.rs` is 182/190 — new module if wiring exceeds headroom.

Not started (A10: last for the shared binder file).

### 0.8 Native-module pin replay

`uv sync --frozen` in `/tmp/grok-c25` exit 0 (368s). Native import
smoke: `SELECT 1` → `int64 [1]`. Four DS-4 BUG-CANDIDATE pins replayed
on that module (`pytest` exit 0, 4 passed / 0.71s) — they still assert
the broken behavior:

- `test_smartcsv_delimiter_autodetect_picks_a_rival_delimiter`
- `test_smartcsv_euro_comma_decimal_cast_refuses_loud`
- `test_nested_explode_outer_on_array_of_struct_refuses_loud`
- `test_nested_dynamic_flatten_count_action_refuses_loud`

Re-run after GO recut. B6 smoke_suite known-FAIL not replayed here
(needs the Apache-tests cache); localized from source + Spark oracle.

### 0.9 Closed surfaces (untouched this recon)

`crates/repark-python/src/column/function_dispatch.rs`, facade
`functions*.py`, `crates/repark-ta`, bench files, declareSorted region
of `core.py`, `crates/repark-core/src/session.rs` declare seam,
`crates/repark-iceberg/src/write/`, `docs/spark-sql-iceberg-parity.md`.

---

## 1. B4 — delimiter auto-detect (2026-08-17)

**Branch:** `grok/c25-b4-csv-delimiter` · **Base:** `2cfcba9` (GO SHA)

**Decision:** rank `(mode_fields, agreement)` instead of `(agreement, mode_fields)`.
A 2-field rival with perfect agreement (quote mid-field under the wrong
delimiter) no longer beats a 12-field split whose agreement is lower because
of ragged rows. True 2-column `;` files still win: the comma candidate's
modal count is < 2 and scores `(0, 0)`.

**Tests (same names, flipped — A5):**
- `test_smartcsv_delimiter_autodetect_picks_a_rival_delimiter` now asserts
  auto-detect == declared scheme delimiter, `data_row_count == 64`, headers
  match (incl. `_c12`).
- New unit pin: `test_detect_delimiter_prefers_wide_split_over_two_field_rival`
  in `test_t4_csv_smart.py`.

**Generators:** untouched.

**Gates (echo \$?; never piped through tail):**
- flipped + unit pins: exit 0 (2 passed, 0.71s)
- `make py-test`: exit 0 (235 passed)
- `make ci`: exit 0
- `make py-test-facade`: exit 0 (3319 passed, 70 skipped)
