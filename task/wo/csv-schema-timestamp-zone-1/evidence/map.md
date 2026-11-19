# task/wo/csv-schema-timestamp-zone-1/evidence/ — probes and raw cells

- [probes/](probes/map.md) — the comment-free Python that produced every number in [../bar.md](../bar.md) and
  [../cost.md](../cost.md).
- [raw/](raw/map.md) — the cells, verdicts, plans, pin runs and cost samples.

## Provenance

- **Builds:** main `S/ddu-work/pkg-main-4c365814`; head `S/ddu-work/pkg-csvts-186c34b0`, built by
  `S/ddu-work/build_pkg_csvts.sh` (lane A's method, lock and cores) from `186c34b0`. Interpreter
  `S/lane-nan-write-canonical-1/.venv/bin/python` with the package on `PYTHONPATH`, never `-I`.
- **Spark 4.1.2:** the c26 oracle environment, Zulu JDK 17, through `spark_slot.sh`; step 0's table and the reader
  sweep use the recordings of step 0 (PR #1364) and part 2's delta 3; `x.py` ran live.
- **Runs (2026-11-19):** each command under `flock -s <measure lock> nice -n 10 taskset -c 32-47 prlimit
  --as=51539607552`, one at a time, warehouses in `<scratchpad>` and deleted after each command. Step 0's table:
  `rp.py repark <wh> <out> <part>` per part, main then head, then `cmp_h.py` and `bar.py`. Raw answers had scratch
  paths and random view names rewritten to `<scratchpad>` and `N` before comparison.

Up: [../map.md](../map.md).
