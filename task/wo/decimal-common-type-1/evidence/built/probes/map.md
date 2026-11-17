# task/wo/decimal-common-type-1/evidence/built/probes/ — the build's runners (comment-free)

Paths are environment variables (`S` the orchestrator's scratchpad, `JAR` the Iceberg runtime jar, `LOCKS` the
build-slot lock directory, `C012` the C-012 replay directory); nothing here names a local path.

- `run_repark.sh` — re-runs step 0's runners ([../../probes/](../../probes/map.md)) on one package, both ANSI modes,
  and the native door.
- `bar.py` — pairs the head's verdicts with main's, counts per cause and verdict, names every non-EQUAL
  common-type cell and renders struct values alike.
- `record_oracle.py`, `make_oracle.py` — record the pin file's cells on Spark 4.1.2 (or RePark) through
  `python/repark/tests/types/_decimal_common_type_1_cells.py`, and write the pin oracle from both modes.
- `build_pkg.sh` — builds the package from a committed sha in a clean worktree, with `BUILT_FROM`.
- `run_cost.sh` — the alternating cost runner (main, head, main again per case and run, each sample under the
  exclusive measure lock); `dct_cost.py` — the K1, K2, K3, K4, K5 and reported cases; `dct_data.py` — their data;
  `analyze_cost.py` — the table (median per row, control band `main-aa / main`).
- `k2_probe.py` — K2's 200 comparisons, automatic against hand-written casts, on either package.

Up: [../map.md](../map.md).
