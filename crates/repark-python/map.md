# map — repark-python

## Purpose

The PyO3 `cdylib` that exposes the engine to Python as the `repark._native` module (crate-DAG
**tier 4**, the bindings adapter — it reaches down; nothing depends on it). The pure-
Python `repark` package imports from here. Rows cross the boundary as Apache Arrow via the **Arrow
PyCapsule interface** (`__arrow_c_stream__`), zero-copy. **This is the only crate allowed to use
`unsafe`** (the PyO3 / Arrow-FFI boundary).

## Contents

- `Cargo.toml` — `[lib] name = "_native"`, `crate-type = ["cdylib","rlib"]`. Deps: `pyo3` (0.29,
  abi3-py312), `arrow` (features `ffi` + `prettyprint` + `ipc`), `datafusion` (only to name the
  `DataFrame` type), and **five** repark crates (design §2.2): `repark-core` — v1's `repark-core`
  (error seed) and `repark-session` (session API) COLLAPSED into one entry, since `repark-core`
  re-exports the taxonomy from `repark-common` (EC-1); `repark-functions` (the date-function `Expr`
  builders); `repark-ta` (feature `datafusion` — the TA window UDFs `ta_window` builds);
  **`repark-ml`** (M3 estimator kernels); and **`repark-spark`** (NEW vs the pin — the Spark door
  the constructor installs, EC-2). **Deliberate non-edges:** no `repark-sql` (zero ANSI surface from
  Python — design §4 Q2) and no `repark-iceberg` (Iceberg is reached only through `ReparkSession`
  and SQL text). `repark-excel` is **dropped** — its two pymethods survive as EC-3 refuse-arms.
  Then `tokio`, `futures` (`StreamExt::next` to poll the streaming Arrow export one batch at a time —
  DataFusion does not re-export it), **`tracing`** (QUAL-05 / OBS1 PyO3 entry-family spans;
  workspace pin only), **`tracing-subscriber`** (R-TRACE-SUBSCRIBER: feature-lean
  `fmt` + `env-filter` + `std` + `ansi` — env-gated live phase profiles through the wheel). Dev-dep
  `pyo3` with `auto-initialize` so the Rust tests can boot an embedded interpreter, plus a dev-only
  `repark-common` edge that exists solely so the EC-1 type-identity guard in `src/tests.rs` can name
  both paths to the same `Error` type (declared in `scripts/check_crate_dag.py` `ALLOWED_EDGES`
  with kind `dev` — inspected like every other edge, exempt only from the layering rule).
  Spells out its own lints with `unsafe_code = "allow"`
  (it does NOT inherit `[lints] workspace = true`; the workspace root reserves the carve-out).
  `scripts/check_lib_rs.py` carries a `repark-python` EXCEPTIONS row (EC-10: the 180-line root is a
  manifest and already uses the sanctioned file-backed test module). The off-by-default `extension-module` feature stays OFF for
  `cargo test` (so libpython links).
- `src/lib.rs` — the `#[pymodule] _native` entry point + `to_py_err` (engine `Error` → `RuntimeError`)
  + **`try_init_repark_tracing`** (R-TRACE-SUBSCRIBER): on module init, if `REPARK_LOG` (preferred)
  or `RUST_LOG` is set, `tracing_subscriber::fmt` + `EnvFilter` + `FmtSpan::CLOSE` → stderr via
  `try_init` (never panics if a subscriber already exists); absent env = zero overhead; registers
  M3 `ml::register` fit functions.
- `src/fence.rs` — the shared **SAF-007 panic fence**. `fence(op, || PyResult<T>)` wraps every
  `#[pymethods]` body (via the `fenced!` macro): a Rust panic is caught and re-raised as the base
  `PySparkException` (a `RuntimeError`) with the message preserved under an "internal error" framing,
  instead of escaping to PyO3's trampoline as an uncatchable `PanicException` (a `BaseException`).
  **QUAL-05 / OBS1:** `fence_with_span(family, op, body)` + `fenced_span!` open a `py.entry` span
  with static `family`/`operation` fields (one span per entry-point family: `py.session` /
  `py.sql` / `py.read` / `py.action` / `py.catalog` — including `table_exists` /
  `list_temp_view_names` / `list_df_schema_table_names` under `py.catalog`) so a hang localizes
  to Python vs engine. Additive — same control flow as `fence`. `fence_stream_poll` wraps
  `StreamingBatchReader::next` — the Arrow C-stream `extern "C"` `get_next` callback, which PyO3's
  trampoline does NOT cover — so a poll panic becomes a terminal Arrow error instead of a **process
  abort** (poll-side hang is outside the open `__arrow_c_stream__` span duration — residual).
- `src/session.rs` — `PyReparkSession`: holds the process-wide tokio `Runtime` as a
  `repark_core::EngineRuntime` (EC-5 — type in core, instance here), `block_on`s the async engine;
  exposes the constructor (builder knobs + **EC-2**: `SparkDialect` + `SparkExtension` installed
  before `build()`, because a bare builder is stock DataFusion in this repository), `sql`,
  `read_parquet`, R1 `read_csv` / `read_json`,
  **EC-3 refuse-arms** `read_excel` / `excel_sheet_names` / `read_postgres` (name/arity/defaults
  preserved; each raises `UnsupportedOperationException` naming the surface, the
  post-milestone-one schedule, and the `task/todo.md` backlog row), I1 `read_iceberg_table` /
  `testing_create_ref` / `testing_list_snapshots`, **T6** `list_iceberg_table_names` /
  `list_temp_view_names` / `list_df_schema_table_names` / `refresh_catalog_provider` /
  `testing_oob_create_table` / `testing_oob_drop_table`, and the
  catalog/temp-view surface (`create_or_replace_temp_view`, `drop_temp_view`, `table_exists`,
  `register_memory_catalog`, `create_namespace`, `register_ipc_stream_as_temp_view` (IPC fallback),
  **I4/P1a** `register_arrow_stream_as_temp_view` — C-stream import under GIL → MemTable;
  createDataFrame prefers this over IPC).
- `src/column.rs (r24 SB1: `array_repeat`/`sequence`/`repeat` facade cardinality refuse)` — `PyColumn` + **`call_scalar`** match table (R-FN-BATCH1/2: string/math +
  reverse/array_*/map_*/size/slice/sequence/… via `datafusion::functions` + `functions_nested`;
  **E1 octo C1:** `array_element` → `__repark_array_get__`, `get_field`;
  **E1 octo C2:** `getitem` → `__repark_get_item__` polymorphic array/map GetItem;
  **E1 octo C7:** `substr`/`substring` → owned `repark_functions::string::substring_udf()`
  (not DF built-in — closes Column.__getitem__ slice / call_scalar 3-arg pos-0 divergence);
  **F2:** `regexp_replace` defaults flags `"g"` (Spark global); `sec`/`csc` → CASE Inf at
  exact zero divisor (global non-ANSI `/` nullif unchanged);
  **r22 C5:** `try_cast` → DataFusion `Expr::TryCast` (null on cast failure; facade
  `Column.try_cast` / SQL `TRY_CAST` display)).
  **r24 A3 QUAL-03:** `parse_data_type` full facade cast vocab (float/byte/short/binary +
  tinyint/smallint aliases); residual unknown → `AnalysisException` (not ValueError);
  test renamed `parse_data_type_maps_facade_primitive_cast_vocabulary` (rule 11).
  **r25 T3:** `PyColumn.collapse_identity_aliases` peels nested `Alias` chains to one outer
  rename (facade `_collapse_identity_projection_alias` only — Q7); unit
  `collapse_identity_alias_chain_peels_same_name_stack`.
- `src/dataframe.rs` — **F2:** `arrow_type_key` emits `struct<field:type,…>` for nested
  Struct (not Debug `Struct([Field…])`) so facade `schema`/`dtypes`/`printSchema` resolve
  nested createDataFrame structs.
- `src/ml.rs` — M3 native fit binder: `fit_linear_regression` / `fit_logistic_regression` /
  `fit_kmeans` stream `DataFrame::execute_stream` batches into `repark-ml` accumulators
  (params-only results; never full-row materialization). Registered on the `_native` module.
- `src/dataframe.rs` — `PyDataFrame`: wraps a DataFusion `DataFrame`; `count`/`show` /
  `limit` / **`limit_with_skip(skip, fetch)`** (DataFusion `Limit` with non-zero skip — used by
  the facade display-style tail preview `_preview_tail_rows`, R-DISPLAY; shareable with a later
  public `DataFrame.tail`) + **`analyzed_arrow_schema`** (analysis-only Arrow C schema PyCapsule —
  physical types, no execution; U7 pandas_udf pass-through / octo C6-Q-001; **P2b**
  `OnceLock<SchemaRef>` cache per plan handle) + the zero-copy
  **streaming** Arrow handoff `__arrow_c_stream__` (a
  PyCapsule over `FFI_ArrowArrayStream` wrapping the crate-local `StreamingBatchReader`, which
  pulls one batch per `next()` from `DataFrame::execute_stream` — O(batch) peak memory, not
  O(result)) + the crate-internal `inner()` accessor for session-side temp-view registration.
- `tests/bindings.rs` — Rust integration tests driving the pyclasses (session build, sql round-trip,
  count, show, and re-importing the exported Arrow stream to assert values).

## Arrow handoff (decided)

Zero-copy via the **Arrow PyCapsule interface** (`__arrow_c_stream__`), built on
`arrow::ffi_stream::FFI_ArrowArrayStream`. Chosen over arrow's `pyarrow` feature because that feature
pulls in `arrow-pyarrow`, which carries its own `pyo3` pin — two pyo3 versions will not link. The
PyCapsule path is independent of the pyo3 version and is exactly what `pyarrow.table(df)` /
`polars.from_arrow(df)` consume.

**Streaming (not collect-then-wrap).** `__arrow_c_stream__` opens a `DataFrame::execute_stream`
(physical plan, run once under `py.detach`) and hands its `SendableRecordBatchStream` to a
`StreamingBatchReader`. The consumer pulls batches through the FFI `get_next` callback; each pull
runs ONE `block_on(stream.next())` — so peak memory is O(one batch), not O(whole result) (the fix
for the "stream export lie", audit SAF-003 / finding #14; the end-to-end pin
`arrow_c_stream_export_is_lazy_and_does_not_materialize_up_front` in `src/dataframe.rs` counts
batches produced and goes red on a collect-then-wrap revert, F-BR-4). This is a **memory** bound, not
a batch/error *ordering* one: a parallel engine plan may surface a later batch's error before batch 1,
so batch-1-before-error is a sequential-reader property, never an end-to-end guarantee (F-BR-5). The
reader declares the analyzed LOGICAL
schema (`analyze_eagerly` — right TYPES, so no Arrow reinterpret, plus Spark-style `nullable = true`),
NOT the physical `stream.schema()` (`nullable = false` for computed columns); the F.expr
Arrow-boundary reinterpret class stays closed and the export matches Spark parity + the metadata path
(`task/lessons.md` 2026-07-13). A mid-stream engine execution error rides the Arrow C stream's error
channel; the facade `to_arrow` re-raises it as the base `PySparkException`. Runtime safety: the
per-batch `block_on` runs on the consumer's calling thread (never a runtime worker thread, never
nested), so it cannot re-enter the process-wide `OnceLock` runtime; the GIL is released per poll.
**r23 PG2 / OTH-009 / Q15:** SIGINT during a poll is deferred; shipping `check_signals` at the
stream-poll seam would type-launder `KeyboardInterrupt` through `ArrowError`/`PySparkException` —
documented residual in the `StreamingBatchReader` rustdoc (v1's
`task/pg2-pg-runtime-ledger.md` has no counterpart in this repository), no behaviour change here.

## I want to...

| ...do this | go to |
|---|---|
| Expose a class/function to Python | `src/lib.rs` (register on the `_native` module) |
| Add a session method (`sql`-like) | `src/session.rs` (`#[pymethods] impl PyReparkSession`) |
| Add a DataFrame action | `src/dataframe.rs` (`#[pymethods] impl PyDataFrame`) |
| Add an aggregate Column method (e.g. percentile) | `src/column.rs` (`approx_percentile_cont`, `aggregate`, …) |
| Cross Arrow data to Python | `__arrow_c_stream__` in `src/dataframe.rs` — zero-copy |
| Fence a panic at a new entry point | wrap the body in `fenced!("Type.method", { … })` (`src/fence.rs`); an FFI-callback poll uses `fence_stream_poll` |

## Component contract

- **Owns:** the PyO3 cdylib (`repark._native`) — a thin adapter exposing `PyReparkSession` /
  `PyDataFrame` / `PyColumn`, the PySpark exception taxonomy, the SAF-007 panic fence, the M3 ML fit
  binder, and the zero-copy Arrow C-stream handoff (`__arrow_c_stream__`). The **only** crate allowed
  `unsafe`.
- **Does not own:** engine logic (it wraps `ReparkSession` + `DataFrame`); SQL semantics (the doors);
  the wheel (`python/repark` via maturin).
- **Public inputs:** Python calls — session build, `sql`, readers, DataFrame actions, Column methods,
  `fit_*`.
- **Public outputs:** pyclasses; Arrow record batches streamed zero-copy via the PyCapsule interface;
  PySpark-typed exceptions.
- **State & lifecycle:** holds the process-wide tokio `Runtime` (as a `repark_core::EngineRuntime`)
  and `block_on`s the async engine; installs the Spark door (`SparkDialect` + `SparkExtension`) before
  `build()`; streaming export is O(one batch) peak memory.
- **Allowed internal deps:** `repark-core`, `repark-functions`, `repark-ta` (feature `datafusion`),
  `repark-spark`, `repark-ml` — five inward (tier-4→down) edges. Deliberate non-edges: `repark-sql`,
  `repark-iceberg`. Dev-only `repark-common` (the EC-1 type-identity guard).
- **Failure model:** engine `Error` → `RuntimeError` / the PySpark taxonomy (`to_py_err`); a Rust
  panic is caught by the fence and re-raised as `PySparkException` (never an uncatchable abort); a
  mid-stream execution error rides the Arrow error channel.
- **Extension points:** expose a class / function (`lib.rs`); add a session method (`session.rs`), a
  DataFrame action (`dataframe.rs`), or a Column method (`column.rs`); fence a new entry point
  (`fenced!`).
- **Test strategy:** `cargo test -p repark-python` (embedded interpreter via the `auto-initialize`
  dev-dep) + `maturin develop` import smoke; `extension-module` stays OFF for tests.
- **Known limitations:** SIGINT during a stream poll is deferred (cooperative cancel parked);
  `read_excel` / `read_postgres` are EC-3 loud refuse-arms (post-milestone-one); must never build with
  `--all-features`.

## Pointers

- Up: [../map.md](../map.md)
- Wrapped engine: [../repark-core/map.md](../repark-core/map.md) (v1's `repark-session`, re-homed).
- Spark door: [../repark-spark/map.md](../repark-spark/map.md) (`SparkDialect` + `SparkExtension`).
- Design: [../../docs/design/python-facade.md](../../docs/design/python-facade.md)
  (§2.2 dep edges, §3 EC-1/2/3/5/6/10, §4 Q7, §5 F3, §9 PR-3);
  ledger [p3c-binding-ledger.md](../../docs/history/port-v2/p3c-binding-ledger.md).
- Related: built into a wheel by `python/repark` (maturin) — **that package lands phase-3 PR-5**;
  no wheel is buildable from this PR and none is claimed.

## Debug

| Symptom | First check |
|---|---|
| PyO3 build can't find Python | `.cargo/config.toml` sets `PYO3_PYTHON=python3` |
| `cargo test --all-features` link error | Use `cargo test -p repark-python`; `extension-module` must stay off for tests |
| `cargo test` fails to init the interpreter | the `auto-initialize` dev-dep boots it; confirm libpython3.x is installed |
| `unsafe` lint fires elsewhere | Keep all `unsafe`/FFI in this crate only |
| `pyarrow.table(df)` raises on the capsule | capsule must be named `arrow_array_stream`; see `__arrow_c_stream__` |
| `pyarrow.table(df)` raises mid-read (`External error: …`) | a DataFusion execution error surfaced during streaming; the engine text is in the message — see `StreamingBatchReader::next` |
| Ctrl-C mid-collect/to_arrow seems ignored | Expected until the current `block_on` poll returns (OTH-009); cooperative cancel parked — see the `StreamingBatchReader` rustdoc |
| `read_excel` / `excel_sheet_names` / `read_postgres` raises `UnsupportedOperationException` | Expected: EC-3 refuse-arms; the reader crates are post-milestone-one (`task/todo.md`) |
| a Spark-only function or statement fails on a `PyReparkSession` | the EC-2 door install was dropped from the constructor — see `spark_doored_session_resolves_spark_function_and_routes_spark_statement` |
| a method raises `PySparkException: repark internal error in …` | a Rust panic was caught by the SAF-007 fence (`src/fence.rs`); the framed message names the entry point + preserves the panic text — this is a bug, check the stderr backtrace |
| `percentile_approx` / `approx_percentile_cont` missing | Q1: `PyColumn::approx_percentile_cont` + SQL aliases in `repark-functions::register_all` |

First checks: `cargo test -p repark-python`; then `maturin develop` + import smoke. Escalate to:
[../map.md#debug](../map.md).
