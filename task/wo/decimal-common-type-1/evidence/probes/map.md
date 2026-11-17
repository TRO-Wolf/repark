# task/wo/decimal-common-type-1/evidence/probes/ — the step 0 probes (comment-free Python)

Run from this directory; `cells` and `run_cells_lib` are imported by the runners. Commands and provenance:
[../map.md](../map.md).

- `cells.py` — the 20 columns and their eight rows, the 14 type pairs, the 28 expression and 18 plan positions,
  the constants, three-type, aggregate, `VALUES`, literal and special cells, the Iceberg statements and the
  legacy-flag sample.
- `run_cells.py` — one engine (`spark` or `repark`) in one ANSI mode: the Parquet tables `t` and `tx`, every SQL
  cell, the DataFrame and `F.expr` cells, the Iceberg stores, DML (copy-on-write and merge-on-read) and partitioned
  reads with the stored rows read back, then the legacy flag (`SET`, the sample, `RESET`).
- `run_cells_lib.py` — the session builder and the answer normaliser shared by the side probes.
- `run_native.py` — the native door `repark.sql()` over views of the same rows.
- `run_side.py` — the controls for the other causes (same-type `USING`, `nvl`, `nullif`, `EXCEPT`, `avg`, `lag`,
  `RANGE`, the union shapes, `UPDATE … SET`) and the legacy flag's `SET`/`RESET` round trip.
- `run_revision.py` — after the inventory critic: its four findings (map keys, `USING` joins aliased and not,
  `IN`-list integer literals, set-operation membership and `UNION DISTINCT`), the controls for each, and its pin
  candidates, SQL and DataFrame, one engine and ANSI mode per run.
- `explain_union.py` — the initial plan's union nesting on RePark.
- `compact.py` — copies the raw outputs into [../raw/](../raw/map.md) without the SQL text (kept for the side
  probes) and the timings, with scratch and home paths replaced by placeholders.
- `analyze.py` — pairs every cell with Spark's, classifies the cause, writes `verdicts.jsonl` and `summary.md`.
- `grids.py` — writes `grids.md`, the position-by-pair grids and the group tables.

Up: [../map.md](../map.md).
