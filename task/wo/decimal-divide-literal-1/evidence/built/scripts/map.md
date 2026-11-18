# task/wo/decimal-divide-literal-1/evidence/built/scripts/ — the build's scripts (comment-free Python and shell)

- `record_oracle.py` — records Spark 4.1.2's answers for the pin file's cells (both ANSI modes, default and
  `pickMinimumPrecision=false`) into `decimal_divide_literal_1_spark_oracle.json`.
- `compare.py` — pairs the head's and main's step 0 cells with Spark's using step 0's `classify` (run beside a copy of
  [../../../../avg-decimal-round-1/evidence/probes/](../../../../avg-decimal-round-1/evidence/probes/map.md)).
- `ddl_classes.py` — the transitions, the EQUAL → non-EQUAL and protected-cell checks, and the named classes.
- `ddl_data.py`, `ddl_cost.py`, `run.sh`, `analyze.py` — the cost data, one sample, the alternating runner and the
  table (modelled on AVG-DECIMAL-ROUND-1's). Delta 1 added the SQL `mod(d10, 3)`, `pmod(d10, 3)` and
  `F.expr("d10 / 3")` cases, the verdict column and the three bases of the owner's `col(d10) / 3` row.
- Delta 1 (2026-11-18), each run as `<script> <engine> <ansi> <step 0 probes dir> <warehouse parent> <jar> <out>`
  beside step 0's `lib.py`:
  - `remainder_probe.py` — `mod`, `%`, `pmod` with a zero divisor and with negative operands at `d10_2`, `d28_2`,
    `d38_10`, Fable's 16 cells, integer `mod`/`pmod` by zero, and the DataFrame door (`F.expr`, `F.pmod`, `%`,
    `selectExpr`);
  - `sessview_ctas.py` — the `hdiv/sessview` CTAS cell (a session-catalog view of `d10_2 / 2` over an Iceberg
    table, `h / 2` stored by CTAS, the type read from the table's `metadata.json`) and its temp-view control;
  - `card_cells.py` — the proposed cards' cells: a written `CAST(3 AS DECIMAL(20,0))` in `*`, `+`, `/`, and a runtime
    `SET spark.sql.legacy.literal.pickMinimumPrecision=false`;
  - `remainder_table.py`, `delta_moves.py`, `card_table.py` — Spark against main and a head per cell, the cells that
    moved between two packages, and a side-by-side table.
- Delta 2 (2026-11-18):
  - `record_oracle.py` also records the pin file's `pmod` and runtime `SET` cells (`measure_pmod`,
    `measure_runtime_set`, a default build and a `false` build);
  - `repark_cells.py <cells dir> <warehouse parent> <out>` — the same `pmod` and runtime `SET` cells on a RePark
    package (`PYTHONPATH`), as JSON;
  - `delta2_compare.py <spark json> <name> <json> …` — each of those cells against Spark per package, with counts;
  - `raw_moves.py <before run dir> <after run dir>` — step 0's raw bar cells that moved between two heads (timings
    and the package path ignored);
  - `run.sh` takes `COST_DIR` and the SQL `d10 % 3` base row; `ddl_cost.py` has the `s1_sql_d10_pct3` case;
    `analyze.py` adds the `pmod(d10, 3)` row against main's `pmod(d10, 3)` and `d10 % 3`.
- Every script here passes `ruff check` and `ruff format --check` (delta 1 restructured the build's scripts).

Up: [../map.md](../map.md).
