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

## Fable's guidance (2026-11-19)

Fable's guidance under the owner's delegation of 2026-11-19, about 01:28 UTC; guidance, not an owner ruling. **Source:**
the Fable reply of 2026-11-19 in the agents' lessons folder (file `Fable reply — CSV schema timestamp zone unit 0
2026-11-19.md`), §1. Fable's marks are kept.

- **The named exception, option (a) [Rec] yes.** **[R]** Reach: a CSV read whose user schema holds a `TIMESTAMP` field,
  on a query that evaluates that field. A `TIMESTAMP_NTZ` field, a read with no timestamp field, JSON and Parquet are
  untouched. **[M]** Size: about 10 ns of wall time per row here (20 ms over two million rows on ten cores).
- **The bar under (a) [Rec].** Rows that do not evaluate the field stay at 1.05x (`k0_count`, C-012). `k0_hour` and K2
  are recorded as measured (1.379x and 1.403x in process, 1.167x and 1.188x wall) under a named exception with a
  ceiling of 1.45x in process. The exception names the kernel card as its way out, TEXT-TIMESTAMP-PARSE-KERNEL-1.
- **The split [M].** The whole 1.38x is the text timestamp parse; `n`'s cast is about 1 ms of the 20 ms. `ts_only`
  never reads `n` and pays 20.35 ms, the same as `k0_hour`. `n_only` and `n_sum` sit at 1.052x and 1.038x, inside the
  control (`ntz_hour` 1.038x, a path the fix does not touch).
- **The `file://` path, 1.360x [M].** `url_k0_hour` runs at 1.360x against 1.379x on a local path. **[R]**
  `csv_utf8_schema_from_path` returns `None` for any path holding `://`, so only the named columns are re-read; the
  head's plan there keeps the typed cast of `n`.
- **Option (b), named-column re-read, declined [Rec] no.** **[M]** It buys about 1 ms of 20 and costs a second product
  file and a Rust build.
- **Option (c), a faster kernel [Rec] yes, as its own Rust unit, not tonight and not label-blocking.** The card is
  TEXT-TIMESTAMP-PARSE-KERNEL-1 (S3, cost only). **[M]** Indicative floor: a typed scan plus a zone conversion
  (`ntz_cast_hour`, 63.40 ms on main) is already about 1.2x of main's `ts_only`, so a kernel unit should take its bar
  from that, for example at most 1.20x.
- **Option (d), converting the scan's typed column, declined [Rec] no.** **[M]** It gives the head's answers for the DST
  gap and overlap, but it is wrong for text that carries an offset: the scan types `2024-01-01T03:00:00+02:00` and
  `2024-01-01T03:00:00Z` as zone-less after moving them to UTC, so main's two right rows would be lost.

## Delta 1 (2026-11-19): the cost misses Fable's bars

**Method.** Unit 0's runner and data, lane B: every sample alone under `flock -x <measure lock> flock <link lock>
flock <opus-cargo lock> flock <opus2-cargo lock> nice -n 10 taskset -c 48-57`, the clock inside the locks; five runs,
each in the order main, head, main again. Head: `S/ddu-work/pkg-csvts-1ee847b7` (`BUILT_FROM` `1ee847b7`).

**Against main `pkg-main-4c365814`** (the brief's series; K0, K2 and C-012):

| case | main / main again (median) | head (median) | ratio | control band (main again / main) | wall ratio | head runs | bar | holds | unit |
|---|---|---|---|---|---|---|---|---|---|
| k0_count | 0.0219 / 0.0212 | 0.0240 | **1.115x** | 0.970 | 1.027x | 5 | 1.05x | no | s per repetition, in process |
| k0_hour | 0.0551 / 0.0543 | 0.0881 | **1.618x** | 0.986 | 1.313x | 5 | 1.45x | no | s per repetition, in process |
| k2 | 0.0553 / 0.0538 | 0.0923 | **1.692x** | 0.972 | 1.353x | 5 | 1.45x | no | s per repetition, in process |
| c012p6 | 40.2100 / 39.3500 | 39.5300 | **0.993x** | 0.979 | 0.993x | 5 | 1.05x | yes | s wall |
| c012rest | 42.7400 / 42.7800 | 42.6400 | **0.997x** | 1.001 | 0.997x | 5 | 1.05x | yes | s wall |

**Against `pkg-sum-845766fd`** (its product tree equals main `2900efde`, which this branch merged, and it was built in
the same target directory as the head; K0 and K2 only):

| case | main / main again (median) | head (median) | ratio | control band (main again / main) | wall ratio | head runs | bar | holds | unit |
|---|---|---|---|---|---|---|---|---|---|
| k0_count | 0.0220 / 0.0222 | 0.0237 | **1.070x** | 1.007 | 1.054x | 5 | 1.05x | no | s per repetition, in process |
| k0_hour | 0.0556 / 0.0548 | 0.0849 | **1.548x** | 0.985 | 1.259x | 5 | 1.45x | no | s per repetition, in process |
| k2 | 0.0525 / 0.0549 | 0.0893 | **1.655x** | 1.046 | 1.358x | 5 | 1.45x | no | s per repetition, in process |

**Reading.** C-012 holds. `k0_count` is over 1.05x and `k0_hour` and K2 are over Fable's 1.45x ceiling, in both
series. The verifier measured the old head on the same cores at 1.342x (`k0_hour`) and 1.352x (K2). **[M]** K2's plan
is the same on the old head and delta 1 (`to_timestamp(column_2)` over the same ten-group scan, projection
`[column_2]`); only the scan's file schema differs (`column_1` `Int64`, not `Utf8`), and K2 never reads `n`. **[M]**
One-process control timings under the shared lock (`<scratchpad>/csv-ts-delta1/cost2/controls.txt`) were too noisy to
split the cause: a pure `to_timestamp` over `range(2000000)` read 0.062 to 0.088 s across the same packages. The cause
is not found; this is a bar miss for the orchestrator to rule on. Samples: `<scratchpad>/csv-ts-delta1/cost/` and
`cost2/` (`samples.jsonl`, `raw-csv.jsonl`).

Up: [map.md](map.md).
