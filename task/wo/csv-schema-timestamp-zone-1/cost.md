# CSV-SCHEMA-TIMESTAMP-ZONE-1 — cost (K0 CSV, K2, C-012), like-for-like against main

**Method.** Main `S/ddu-work/pkg-main-4c365814` against the head `S/ddu-work/pkg-csvts-186c34b0`, each run in the
order main, head, main again; every sample alone under `flock -x <measure lock> flock <link lock> flock <opus-cargo
lock> flock <opus2-cargo lock> nice -n 10 taskset -c 32-47`, the clock started inside the locks. K0 and K2
(`evidence/probes/csv_cost.py`) run in one process each: a warm-up, then five timed repetitions; the ratio is taken on
the per-repetition time (the process wall is shown beside it). K0 builds the frame inside each repetition, so it
pays the re-read; K2 reads a frame view made once. The data is a two-million-row CSV (`n INT, ts TIMESTAMP`, session
zone Los Angeles). C-012 is `S/ddu-work/cost-sum-d3/run.sh`'s `c012` group (`replay_work.py`, `c012p6` and
`c012rest`), timed by the process wall. A row over 1.05x ran to 10 runs.

| case | main / main again (median) | head (median) | ratio | control band (main again / main) | wall ratio | head runs | at most 1.05x | unit |
|---|---|---|---|---|---|---|---|---|
| k0_count | 0.0171 / 0.0170 | 0.0177 | **1.036x** | 0.995 | 1.000x | 5 | yes | s per repetition, in process |
| k0_hour | 0.0407 / 0.0419 | 0.0570 | **1.379x** | 1.030 | 1.167x | 10 | no | s per repetition, in process |
| k2 | 0.0396 / 0.0407 | 0.0562 | **1.403x** | 1.030 | 1.188x | 10 | no | s per repetition, in process |
| c012p6 | 40.4700 / 39.8700 | 39.9500 | **0.995x** | 0.985 | 0.995x | 5 | yes | s wall |
| c012rest | 44.2500 / 45.4000 | 44.6700 | **0.997x** | 1.026 | 0.997x | 5 | yes | s wall |

**Reading.** `k0_count` does not evaluate the timestamp, so it carries only the re-read at frame creation. `k0_hour`
and K2 evaluate `ts` for every row they read: the parse moves from the CSV scan's native timestamp parser to the
zone-aware `to_timestamp` cast over text that the JSON reader already uses, and `n` is cast from text too. That is
the price of parsing under the session zone; no seat inside `reader.py` avoids it (a recast through a string costs
more: `evidence/raw/emu_ab-main.json`). Samples: `evidence/raw/cost-samples.jsonl` and `cost-raw-csv.jsonl`.

Up: [map.md](map.md).
