# FACADE-5 display-renderer baseline (step 0, 2026-09-14)

FACADE-5 asks whether the display renderer itself — the `display.py` bodies and
the `plan_collapse.py` / `polars_cells.py` formatters — is a measurable wall,
separate from the fetch/materialization work DFCORE-6 already removed. This
document splits each door's call into two legs and times them separately:

- **FETCH** — capped rows into an Arrow table, exactly what each door does
  before formatting: `frame.limit(n).to_arrow()` (ASCII `show`),
  `frame.limit(n + 1).to_arrow()` (vertical `show`, eager `repr`,
  `_repr_html_`), or the styled probe `frame.limit(max_rows + 1).to_arrow()` +
  `frame.count()` when the probe fills + `frame.limit(head_n).to_arrow()`
  (duckdb head) + `frame._preview_tail_rows(tail_n, total)` for the tail.
- **FORMAT** — the formatter over the pre-materialized table: `_format_show_table`,
  `_format_show_vertical`, `_format_eager_eval_table`, the `_repr_html` body,
  `_format_polars_show`, `_format_duckdb_show`, plus their shared
  `_table_to_cell_rows` / `_cell_text` / `_display_type_labels_from_arrow` cell
  work.

The leg-composed string is cross-checked against the real door's bytes in every
cell (`door_check`, 96/96 equal). A cell is a **wall** when the format leg is
>= 1 ms per user call or >= 20% of the call's wall.

## Machine / harness

- CPU: AMD Ryzen Threadripper 3970X 32-Core; kernel 6.8.0-138-generic;
  Python 3.12.3; pyarrow 25.0.0; native module 167,915,168 bytes, RELEASE
  (`repark._native.__debug_assertions__` False).
- Frames: 1,100 rows each — `flat7` (BIGINT/DOUBLE/STRING/BOOLEAN/DATE/
  TIMESTAMP/DECIMAL(38,18)), `wide50` (50 columns alternating BIGINT/STRING),
  `nested` (STRUCT/ARRAY/MAP) — built once, outside the timed region.
- Matrix: 6 renderers x n in {20, 1000} x truncate {on, off} x 3 shapes, plus
  styled cells at `repark.display.max_rows` in {10, n}: 96 cells.
- One warmup + five reps per cell; medians reported; `wall = fetch + format`.
- Before each cell: no cargo/rustc/maturin running and 1-minute load < 6; the
  load is recorded beside every cell (observed 4.96-5.52 across the run).
- Process under `systemd-run --user --scope -p MemoryMax=8G -p MemorySwapMax=0`
  with `OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8`. No JVM.
- Runner: [facade-5-display-baseline-2026-09-14/run_facade_5_display.py](facade-5-display-baseline-2026-09-14/run_facade_5_display.py);
  raw JSON is the runner's stdout payload (not committed).

## Results — median ms per leg (wall = fetch + format)

### flat7, n=20
| door | fetch ms | format ms | format share | load |
|---|---|---|---|---|
| ascii_show t=on | 0.853 | 0.753 | 47.4% | 4.98→4.98 |
| vertical_show t=on | 0.841 | 0.827 | 49.7% | 4.98→4.98 |
| eager_repr t=on | 0.828 | 0.742 | 47.2% | 4.98→4.98 |
| html t=on | 0.833 | 0.758 | 47.6% | 4.98→4.98 |
| polars_show t=on mr=10 | 1.387 | 0.416 | 22.7% | 4.98→4.98 |
| polars_show t=on mr=20 | 1.378 | 0.611 | 30.7% | 4.98→4.98 |
| duckdb_show t=on mr=10 | 3.127 | 1.090 | 25.9% | 4.98→4.98 |
| duckdb_show t=on mr=20 | 3.265 | 1.138 | 25.6% | 4.98→4.98 |
| ascii_show t=off | 0.822 | 0.716 | 46.5% | 4.98→4.98 |
| vertical_show t=off | 0.950 | 0.899 | 48.6% | 4.98→4.98 |
| eager_repr t=off | 0.835 | 0.750 | 47.4% | 4.98→4.98 |
| html t=off | 0.857 | 0.771 | 47.4% | 4.98→4.98 |
| polars_show t=off mr=10 | 3.392 | 1.012 | 23.5% | 4.98→4.98 |
| polars_show t=off mr=20 | 2.733 | 1.179 | 30.0% | 4.98→4.98 |
| duckdb_show t=off mr=10 | 3.128 | 1.060 | 25.3% | 4.98→4.98 |
| duckdb_show t=off mr=20 | 1.873 | 0.640 | 25.4% | 4.98→4.98 |

### flat7, n=1000
| door | fetch ms | format ms | format share | load |
|---|---|---|---|---|
| ascii_show t=on | 0.666 | 17.663 | 96.6% | 4.98→4.98 |
| vertical_show t=on | 0.494 | 18.628 | 97.4% | 4.98→4.98 |
| eager_repr t=on | 0.550 | 17.094 | 97.0% | 4.98→4.98 |
| html t=on | 0.593 | 17.531 | 97.3% | 4.98→4.98 |
| polars_show t=on mr=10 | 2.399 | 0.693 | 22.5% | 4.98→4.98 |
| polars_show t=on mr=1000 | 1.466 | 20.607 | 93.5% | 4.98→4.98 |
| duckdb_show t=on mr=10 | 1.862 | 22.423 | 92.3% | 4.98→4.98 |
| duckdb_show t=on mr=1000 | 1.943 | 22.386 | 92.0% | 4.98→4.98 |
| ascii_show t=off | 0.567 | 16.570 | 96.7% | 5.06→5.06 |
| vertical_show t=off | 0.536 | 18.032 | 97.3% | 5.06→5.06 |
| eager_repr t=off | 0.756 | 17.200 | 97.1% | 5.06→5.06 |
| html t=off | 0.498 | 17.065 | 97.2% | 5.06→5.06 |
| polars_show t=off mr=10 | 1.403 | 0.394 | 21.9% | 5.06→5.06 |
| polars_show t=off mr=1000 | 1.515 | 20.047 | 92.8% | 5.06→5.06 |
| duckdb_show t=off mr=10 | 1.909 | 24.971 | 93.0% | 5.06→5.06 |
| duckdb_show t=off mr=1000 | 1.998 | 25.115 | 93.0% | 5.06→5.06 |

### wide50, n=20
| door | fetch ms | format ms | format share | load |
|---|---|---|---|---|
| ascii_show t=on | 2.460 | 1.690 | 40.7% | 5.06→5.06 |
| vertical_show t=on | 3.897 | 2.351 | 37.6% | 5.06→5.06 |
| eager_repr t=on | 4.708 | 2.965 | 37.8% | 5.06→5.06 |
| html t=on | 4.372 | 2.982 | 40.5% | 5.06→5.06 |
| polars_show t=on mr=10 | 9.447 | 1.665 | 15.0% | 5.06→5.06 |
| polars_show t=on mr=20 | 8.903 | 2.504 | 21.9% | 5.06→5.06 |
| duckdb_show t=on mr=10 | 9.303 | 2.813 | 23.3% | 5.06→5.06 |
| duckdb_show t=on mr=20 | 8.784 | 2.935 | 23.4% | 5.14→5.14 |
| ascii_show t=off | 4.123 | 2.578 | 38.5% | 5.14→5.14 |
| vertical_show t=off | 4.120 | 2.659 | 39.1% | 5.14→5.14 |
| eager_repr t=off | 4.275 | 2.696 | 38.7% | 5.14→5.14 |
| html t=off | 4.890 | 3.235 | 39.8% | 5.14→5.14 |
| polars_show t=off mr=10 | 9.624 | 1.601 | 14.3% | 5.14→5.14 |
| polars_show t=off mr=20 | 8.982 | 2.399 | 21.1% | 5.14→5.14 |
| duckdb_show t=off mr=10 | 9.866 | 2.650 | 21.2% | 5.14→5.14 |
| duckdb_show t=off mr=20 | 8.637 | 2.631 | 23.4% | 5.14→5.14 |

### wide50, n=1000
| door | fetch ms | format ms | format share | load |
|---|---|---|---|---|
| ascii_show t=on | 3.006 | 74.155 | 96.0% | 5.14→5.14 |
| vertical_show t=on | 2.915 | 72.838 | 96.3% | 5.14→5.14 |
| eager_repr t=on | 2.923 | 63.040 | 95.6% | 5.14→5.14 |
| html t=on | 2.931 | 67.109 | 95.7% | 4.96→4.96 |
| polars_show t=on mr=10 | 8.242 | 1.663 | 16.8% | 4.96→4.96 |
| polars_show t=on mr=1000 | 6.777 | 66.083 | 90.8% | 4.96→4.96 |
| duckdb_show t=on mr=10 | 9.447 | 107.141 | 91.9% | 5.52→5.52 |
| duckdb_show t=on mr=1000 | 9.429 | 107.469 | 92.1% | 5.52→5.52 |
| ascii_show t=off | 2.923 | 61.030 | 95.3% | 5.52→5.52 |
| vertical_show t=off | 3.009 | 65.029 | 95.8% | 5.32→5.32 |
| eager_repr t=off | 2.790 | 61.857 | 95.8% | 5.32→5.32 |
| html t=off | 2.959 | 65.070 | 95.9% | 5.32→5.32 |
| polars_show t=off mr=10 | 6.232 | 1.202 | 16.2% | 5.32→5.32 |
| polars_show t=off mr=1000 | 6.426 | 59.440 | 90.3% | 5.32→5.32 |
| duckdb_show t=off mr=10 | 9.423 | 99.294 | 91.3% | 5.32→5.32 |
| duckdb_show t=off mr=1000 | 9.486 | 100.099 | 91.5% | 5.32→5.29 |

### nested, n=20
| door | fetch ms | format ms | format share | load |
|---|---|---|---|---|
| ascii_show t=on | 0.601 | 0.804 | 57.3% | 5.29→5.29 |
| vertical_show t=on | 0.412 | 0.535 | 56.6% | 5.29→5.29 |
| eager_repr t=on | 0.711 | 0.886 | 55.8% | 5.29→5.29 |
| html t=on | 0.740 | 0.890 | 54.5% | 5.29→5.29 |
| polars_show t=on mr=10 | 2.062 | 0.841 | 29.0% | 5.29→5.29 |
| polars_show t=on mr=20 | 1.203 | 0.788 | 39.5% | 5.29→5.29 |
| duckdb_show t=on mr=10 | 2.699 | 1.133 | 29.5% | 5.29→5.29 |
| duckdb_show t=on mr=20 | 1.622 | 0.679 | 29.5% | 5.29→5.29 |
| ascii_show t=off | 0.409 | 0.498 | 54.9% | 5.29→5.29 |
| vertical_show t=off | 0.416 | 0.507 | 54.9% | 5.29→5.29 |
| eager_repr t=off | 0.696 | 0.842 | 54.7% | 5.29→5.29 |
| html t=off | 0.905 | 1.182 | 56.7% | 5.29→5.29 |
| polars_show t=off mr=10 | 1.198 | 0.464 | 27.9% | 5.29→5.29 |
| polars_show t=off mr=20 | 2.036 | 1.325 | 39.3% | 5.29→5.29 |
| duckdb_show t=off mr=10 | 2.785 | 1.164 | 29.5% | 5.29→5.29 |
| duckdb_show t=off mr=20 | 2.844 | 1.230 | 30.1% | 5.29→5.29 |

### nested, n=1000
| door | fetch ms | format ms | format share | load |
|---|---|---|---|---|
| ascii_show t=on | 0.991 | 23.807 | 95.9% | 5.29→5.29 |
| vertical_show t=on | 0.485 | 23.706 | 97.8% | 5.11→5.11 |
| eager_repr t=on | 0.505 | 23.123 | 97.9% | 5.11→5.11 |
| html t=on | 0.440 | 22.495 | 98.1% | 5.11→5.11 |
| polars_show t=on mr=10 | 1.771 | 0.728 | 29.1% | 5.11→5.11 |
| polars_show t=on mr=1000 | 1.522 | 32.513 | 96.1% | 5.11→5.11 |
| duckdb_show t=on mr=10 | 1.971 | 26.266 | 93.0% | 5.11→5.11 |
| duckdb_show t=on mr=1000 | 1.618 | 26.207 | 92.8% | 5.11→5.11 |
| ascii_show t=off | 0.870 | 22.523 | 96.0% | 5.11→5.11 |
| vertical_show t=off | 0.427 | 22.555 | 98.1% | 5.11→5.11 |
| eager_repr t=off | 0.561 | 22.056 | 97.8% | 5.11→5.11 |
| html t=off | 0.463 | 22.397 | 98.1% | 5.11→5.11 |
| polars_show t=off mr=10 | 2.037 | 0.795 | 28.1% | 5.11→5.11 |
| polars_show t=off mr=1000 | 1.924 | 32.586 | 94.3% | 5.11→5.18 |
| duckdb_show t=off mr=10 | 1.642 | 27.767 | 94.4% | 5.18→5.18 |
| duckdb_show t=off mr=1000 | 1.642 | 26.041 | 92.2% | 5.18→5.18 |
## Reading

**Every one of the 96 cells is a wall** under the step-0 criterion (format >=
1 ms per call or >= 20% of the call's wall):

- **n=20 (the default door):** format is 0.4-3.2 ms and 14-57% of the wall.
  Every spark-door cell (ASCII, vertical, eager `repr`, HTML) sits at 37-57%
  format share; every styled cell clears the 20% share bar except the
  `wide50`/`mr=10` polars cells (14-15%), which still clear the 1 ms bar
  (1.6-1.7 ms).
- **n=1000:** format is the wall outright — 92-98% of the call on every
  non-styled door and on the `mr=n` styled doors. Flat frames cost 17-25 ms to
  format against 0.4-0.8 ms to fetch; 50-column frames cost 59-107 ms against
  2.8-9.5 ms; nested frames 22-33 ms against 0.4-2.0 ms. The format leg scales
  with rows x columns (per-cell `str`/`_cell_text`/`to_pylist` work) while the
  fetch leg is nearly flat in n — the fetch is a `limit` + Arrow export, not a
  scan.
- **Truncate on vs off** changes the bytes, not the wall: the truncation branch
  is a small constant factor inside the per-cell loop.
- The only cells that stay small at n=1000 are `mr=10` styled doors — the
  keep-set bound (`min(n, max_rows)`) caps the formatted rows at 10, so the
  format leg never sees the 1,000 rows. The wall returns the moment
  `repark.display.max_rows` is raised (`mr=1000` rows: 20-33 ms flat/nested,
  59-66 ms wide under polars; duckdb formats `n` rows regardless of `mr`, so
  its `mr=10` cells already sit at 22-27 / 99-107 ms).

The earlier `eager-preview-baseline` numbers were scan walls (a `count()` or a
re-run UDF); those are gone. What remains — and what these numbers isolate —
is the per-cell Python formatting loop itself.

## Step-1 target (C-007)

A measured format wall exists. Step 1 moves the **format leg** to Rust,
byte-identical, behind the same door structure:

- **Moves to Rust:** the whole per-cell + assembly pipeline in
  `polars_cells.py` (`_cell_text`, `_table_to_cell_rows`, `_polars_nested_text`
  and helpers, `_arrow_pa_type_label`, `_style_type_label`, `_polars_column_gap`,
  the polars float spellers) and the five grid formatters in
  `plan_collapse.py` (`_format_show_table`, `_format_eager_eval_table`,
  `_format_show_vertical`, `_format_polars_show`, `_format_duckdb_show`, plus
  `_show_grid_row` / `_eager_eval_grid_row` / `_column_widths` / `_box_rule` /
  `_polars_row_line` / `_duckdb_cell_is_numeric` / `_duckdb_row_line` /
  `_display_type_labels_from_arrow`), plus the `_repr_html` body (escape +
  `<table>` assembly) in `display.py`. Input: a pre-materialized Arrow table +
  names + caps; output: the rendered string. Byte-identity is pinned by the
  §8 goldens plus the 30 new `facade_5_display_goldens.json` cases.
- **Stays in Python:** the fetch leg — `limit`/`to_arrow`/`count`/
  `_preview_tail_rows`, the `_use_bridge_peek` machinery, eager-eval conf
  reads (`_eager_eval_limits`), arg normalization (`_normalize_show_args`),
  and anything touching `eager.py` (owned by another session; the eager
  doors stay thin Python wrappers that hand the fetched table to the Rust
  formatter).
- **Order:** cells (`_cell_text`/`_table_to_cell_rows` per style) first — that
  is the O(rows x cols) work — then the five grid formatters, then the HTML
  body. Each move lands behind the golden battery byte-identically.

## Census

The renderer x truncation-rule census, the bound/unbound pair tables, and the
eager-preview scan-count assertions live in
[task/ledgers/staging/facade-5-ledger.md](../../task/ledgers/staging/facade-5-ledger.md)
(C-003); the 30 unbound pairs each carry a golden in
`python/repark/tests/facade_5_display_goldens.json` (C-004) with one-line
mutation proofs recorded in the ledger (C-005).
