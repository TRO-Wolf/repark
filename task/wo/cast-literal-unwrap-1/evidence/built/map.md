# task/wo/cast-literal-unwrap-1/evidence/built/ — the build's bar and cost

The build of seat W (product commit `88999454` on `fix/cast-literal-unwrap-1`), measured from the package built from
that commit with a clean worktree, against main `cb7d3943`'s package and Spark 4.1.2, both ANSI modes.

- [bar.md](bar.md) — the bar: verdict counts per set and mode, main against head; the two named classes cell for cell
  (EQUAL→REFUSE (Q2), WRONG→REFUSE (Q4)); every other non-EQUAL class by kind.
- [cost.md](cost.md) — the cost rows (C1, the held planning rows, C2, C3 copy-on-write and merge-on-read, the
  `DELETE`, C4, the `IN` of 40), with medians, ratios and control bands.
- [raw/](raw/map.md): `verdicts.jsonl` — one row per cell: set, mode, key, main's and the head's verdicts, the class, the head's
  answer when not EQUAL. `raw/nonequal.tsv` — every non-EQUAL head cell. `raw/pin-cells-spark-*.json` — Spark 4.1.2
  on the pin cells step 0 did not measure. `raw/cost-*.jsonl` — every cost sample.
- [probes/](probes/map.md) — comment-free runners: `run_bar.sh` (step 0's `run_cells.py`, `run_shapes.py`, `run_native.py` and the
  critic's four probes, one package per run; expects copies of step 0's probes in `probes/` and the critic's in
  `critic/` beside it), `bar.py` (pairs each cell with Spark's and names the class), `pin_cells.py` (the extra Spark
  cells), `cu_data.py` / `cu_cost.py` / `run.sh` / `analyze.py` (the cost data, rows, alternation and tables),
  `build_pkg.sh` (the clean-worktree package). Paths come from `S`, `PY`, `JAR`, `BUILD_SLOTS` and `C012_DIR`.

Up: [../map.md](../map.md).
