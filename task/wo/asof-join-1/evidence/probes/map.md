# task/wo/asof-join-1/evidence/probes/ — the cell, cost and verdict probes (comment-free Python)

- `common.py` — both engines' session (`local[1]` and shuffle partitions 1 for Spark, session zone UTC, the ANSI
  mode as an argument), the error-condition reader, the row recorder (sorted rows as text, names, types, wall ms).
- `asof.py` — the data (14 left rows, 11 right rows on an integer grid, cast per as-of type), the tolerance literal
  per type, Spark's as-of condition as Column algebra, the direct call, and the two workarounds a user can write
  today: `w1` (a range join plus `row_number` per left row) and `w2` (a union of both sides plus a `last`/`first`
  `ignorenulls` window fill, then an equi-join back to the right row).
- `cells.py` — the cells: `cells.py <engine> <out> <ansi> <groups> [type]`; groups `G` (the grid: 7 types × 3
  directions × 2 exact × 3 tolerances × 2 joins × 3 key sets, three surfaces), `E` (errors), `M` (mixed types), `O`
  (integer bounds), `N` (NaN, signed zero, infinities), `T` (ties under input order, partitioning and shuffle
  partitions), `C` (call forms), `S` (SQL spellings and helper availability), `X` (`EXPLAIN` text).
- `pandas_cells.py` — `pyspark.pandas.merge_asof` cells beside the `_joinAsOf` call they lower to (Spark only):
  `pandas_cells.py <out> <ansi>`.
- `model.py` — Spark's rule as read from the jar, in plain Python: the condition, the distance, and the tie set
  per left row.
- `cmp.py` — verdicts: `cmp.py <raw dir> <verdicts.jsonl>`; EQUAL, TIE, WRONG (and its NAMES and TYPE kinds),
  RAISE, MISSING against Spark's `_joinAsOf`, plus the model check and each workaround's fidelity on Spark itself.
- `matrix.py` — the domain table's tables from the verdicts and raw cells.
- `ties.py` — which tied row each engine picks, from the grid and the `T` group.
- `cost.py` — the 2M × 2M cost cases: `cost.py <engine> <D|W1|W2> <rows> <keys|nokeys> <repeats> <out>`.

Up: [../map.md](../map.md).
