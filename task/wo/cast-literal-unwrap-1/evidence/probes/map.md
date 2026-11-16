# task/wo/cast-literal-unwrap-1/evidence/probes/ — the step 0 probes (comment-free Python)

Run from this directory; `cells` is imported by the runners. Commands and provenance: [../map.md](../map.md).

- `cells.py` — the 29 cast kinds (source type, target type, values with NULL and the edges, constants, `IN` and
  `BETWEEN` operands, Python literals for the DataFrame door), the seven operators, the dense and the sampled
  `WHERE` grids, and the position cells (select list, `CASE WHEN`, `HAVING`, `JOIN … ON`).
- `run_cells.py` — one engine (`spark` or `repark`) in one ANSI mode: per kind a temp view, a Parquet file, an
  Iceberg v2 table written one row per file and one partitioned on the cast column; every `WHERE` and position
  cell; the DataFrame, `F.expr` and `filter(str)` cells; RePark's `EXPLAIN`; the `DELETE`, `UPDATE` and `MERGE`
  cells copy-on-write and merge-on-read, with the stored rows read back.
- `run_native.py` — the native door `repark.sql()` over a view per kind (it refuses local Parquet DDL by design).
- `run_held.py` — the kernel check: the same casts compared with a column holding the constant, which no
  simplifier can unwrap.
- `explain_side.py` — the side observation: `EXPLAIN` of a table the `SELECT` reads.
- `run_shapes.py` — after the critic: its seven uncovered shapes, the spelled-out operators, the negative half, the
  top-of-type cells (owner question 4) and the three side findings, one engine and ANSI mode per run.
- `shapes_table.py` — prints the shapes table of `domain-table.md` from the raw files.
- `compact.py` — copies the raw outputs into [../raw/](../raw/map.md) without the SQL text and the timings, and with
  scratch and home paths replaced by placeholders.
- `analyze.py` — pairs every cell with Spark's, writes `verdicts.jsonl` and `summary.md`; the tables in
  [../../domain-table.md](../../domain-table.md) are its output.

Up: [../map.md](../map.md).
