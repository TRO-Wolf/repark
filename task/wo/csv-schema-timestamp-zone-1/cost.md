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

## Delta 2 (2026-11-19): the cost, isolated and removed

**The call.** The orchestrator's call under the owner's delegation of 2026-11-19 (about 01:28 UTC, verbatim in
[map.md](map.md)), not an owner ruling: a second delta, past the one-delta cap, for cost only. The owner sees it in the
morning read and may stop it. Fable's bars stand unchanged: 1.05x on `k0_count` and C-012, a 1.45x ceiling on
`k0_hour` and K2, in process.

**The orchestrator's isolation** (before this delta; `<scratchpad>/costiso/`; medians of seconds per repetition against
main `pkg-main-4c365814`):

| case | first head `186c34b0` | delta 1, `target-sum` | delta 1 rebuilt in `target-head` | delta 1's `.so` + first head's `reader.py` | first head's `.so` + delta 1's `reader.py` | bar |
|---|---|---|---|---|---|---|
| `k0_count` | 1.030x | 1.084x | 1.120x | 1.077x | 0.939x | 1.05x |
| `k0_hour` | 1.399x | 1.562x | 1.581x | 1.587x | 1.378x | 1.45x |
| K2 | 1.318x | 1.643x | 1.743x | 1.724x | 1.475x | 1.45x |

It showed that the cost travels with delta 1's native library, not with its `reader.py`, and that K2's plans are the
same on both heads.

**The cause, measured** ([evidence/raw/delta2-diag.txt](evidence/raw/delta2-diag.txt); one process per build per
round, builds alternating, every process alone under the exclusive lock on cores 48-57; medians of 36 timed
repetitions; `*_planned` plans the frame once outside the clock):

1. **Not delta 1's logic [M].** Inside delta 1's own build, K2 and `k0_hour` over delta 1's named-only scan
   (`[n Int64, ts Utf8]`) and over the all-text scan (`[Utf8, Utf8]`, by forcing the `inferSchema` pre-scan) run alike:
   `k2_planned` 78.2 and 85.6 ms, `k0_hour_planned` 75.2 and 76.4 ms. A decode of `column_2` alone costs the same
   under either file schema (25.1 ms).
2. **Delta 1's build [M].** One executed path (delta 1's `reader.py`, the all-text scan, the same plan) on main
   `2900efde`'s build and on delta 1's: `k2_planned` 77.7 → 88.9 ms (1.144x), `k0_hour_planned` 67.1 → 74.0 ms
   (1.103x). The first head's `reader.py` on main `2900efde`'s build (exactly `c427b3b4`) runs as the first head:
   `k2_planned` 0.889x, `k0_hour` 0.996x. Per-process minimums agree: K2's twin bottoms out at 52-53 ms on main
   `4c365814`'s build, 57-59 ms on main `2900efde`'s, 63-70 ms on both delta 1 builds. No one kernel carries it
   (over the same scan: the cast alone 1.115x, `max(hour())` over it 0.926x); `EXPLAIN ANALYZE` puts K2's extra time
   in the scan's stream (`time_elapsed_processing` 472 → 563 ms summed over partitions), not in the cast
   (`elapsed_compute` 147 → 151 ms). **[R]** `read_csv_path` lives in `repark-core`, one codegen unit under thin
   LTO, so any change to it rebuilds that unit and moves the hot code it links with; the K2 twin's minimums spread
   52-64 ms across five builds whose change never touches this path (two mains, two sum builds, an avg build).
3. **A second inference [M].** Delta 1's re-read lets DataFusion infer again before it retypes: 0.08 → 0.55 ms per
   native read, and the facade's read 2.0-2.2 → 2.4-2.8 ms. `k0_count` pays it on every repetition.

**The remedy.** `read_csv_path` is main's again: the delta 2 build's loaded code is main `2900efde`'s byte for byte
(against `pkg-sum-845766fd`, only `.strtab` and the build id differ). `_load_csv` re-reads an existing local path
through its `file://` URL, which main's code re-reads named-only (bar.md "Delta 2").

**The cost** (delta 1's runner and data, lane B, every sample alone under the exclusive lock, five runs in the order
main, head, main again; head `S/ddu-work/pkg-csvts-468ba436`, `BUILT_FROM` `468ba436`; against main
`pkg-main-4c365814`):

| case | main / main again (median) | head (median) | ratio | control band (main again / main) | wall ratio | head runs | bar | holds | unit |
|---|---|---|---|---|---|---|---|---|---|
| k0_count | 0.0212 / 0.0209 | 0.0228 | **1.083x** | 0.988 | 1.042x | 5 | 1.05x | no | s per repetition, in process |
| k0_hour | 0.0557 / 0.0532 | 0.0786 | **1.443x** | 0.955 | 1.237x | 5 | 1.45x | yes | s per repetition, in process |
| k2 | 0.0526 / 0.0561 | 0.0771 | **1.415x** | 1.066 | 1.215x | 5 | 1.45x | yes | s per repetition, in process |
| c012p6 | 39.1300 / 39.2300 | 39.3600 | **1.005x** | 1.003 | 1.005x | 5 | 1.05x | yes | s wall |
| c012rest | 42.6900 / 42.5400 | 42.6800 | **1.000x** | 0.996 | 1.000x | 5 | 1.05x | yes | s wall |

**Side by side** (a copy of the orchestrator's `run2.sh`, delta 2 in place of `newth`; five runs, each sample alone
under the exclusive lock; medians of seconds per repetition against main `pkg-main-4c365814`):

| case | main | first head `186c34b0` | delta 1 `1ee847b7` | delta 2 `468ba436` | bar | runs |
|---|---|---|---|---|---|---|
| `k0_count` | 0.0215 s | 0.0208 s, **0.966x** | 0.0221 s, **1.024x** | 0.0233 s, **1.082x** | 1.05x | 5 |
| `k0_hour` | 0.0521 s | 0.0758 s, **1.453x** | 0.0827 s, **1.586x** | 0.0745 s, **1.428x** | 1.45x | 5 |
| `k2` | 0.0537 s | 0.0765 s, **1.425x** | 0.0854 s, **1.590x** | 0.0783 s, **1.458x** | 1.45x | 5 |

**Reading.** C-012 holds. K2 and `k0_hour` are back at the first head's cost: 1.415x and 1.443x in delta 1's runner,
1.458x and 1.428x beside the first head's 1.425x and 1.453x in the same series, so delta 1's excess is gone and what is
left is the text-timestamp parse of Fable's named exception, at the 1.45x ceiling. **`k0_count` misses 1.05x** (1.083x
and 1.082x). **[M]** The named-only re-read needs a second DataFusion inference on main's code: the read is 1.00 ms
on the first head and 1.52 ms on delta 2 in a warm loop (the native calls 0.58 → 1.04 ms per read), and 2.04 → 2.96 ms
between queries. That is the price Fable put on option (b) when declining it ("about 1 ms of 20"). **[R]** No
`reader.py` route avoids it on main's code: a local path's `utf8_columns` read makes every column text (F1 and F2
again), and a URL's infers again. A Rust route that skips the second inference changes `repark-core`, which re-rolls
the build (the 10-15 percent above) for about half a millisecond. This is a cost miss for the orchestrator: a ceiling
ruling on `k0_count` (about 1.08x), or a Rust unit that hands the first read's types to the re-read, measured
on its own build; TEXT-TIMESTAMP-PARSE-KERNEL-1 stays the way out for K2 and `k0_hour`. Samples: `<scratchpad>/csv-ts-delta2/cost/` and
`costiso/`, and [evidence/raw/](evidence/raw/map.md) (`delta2-cost-*.jsonl`).


## Delta 3 (2026-11-19): no series; the cost lines as ruled

**The rulings.** Owner, 2026-11-19, about 14:45 UTC, verbatim: "go with delta 3". Owner, 2026-11-19, about 14:00
UTC, verbatim: "go with recommendations", which adopts §9 of Fable's reply on the reader-parse units, cost noise and
the next queue; two of its lines bind this file.

**No series.** Delta 3 (bar.md "Delta 3": the verifier's G1, its cause at
`datafusion-datasource-54.1.0/src/url.rs:115`, `:125-141` and `:483`, the fix and the residual) changes only which
spellings take the `file://` route: one that holds `*`, `?` or `[` now keeps itself. The timed queries read `k.csv`
by a local path with no glob character, so they take delta 2's route, and the native library is delta 2's byte for
byte (every section equal). **[M]** `EXPLAIN` of K2, `k0_hour` and `k0_count` on delta 3's package equals delta 2's.

**`k0_count`, recorded by its cause** (Fable's line in §1.2, as ruled): "a CSV read whose schema holds a `TIMESTAMP`
field pays one extra inference, at most 1 ms per read; way out: CSV-STRING-FIELD-TEXT-1".
- The re-check verifier's C1, against `pkg-sum-845766fd` (main `2900efde`'s build) over 3 rounds: +1.64 ms per read
  against the mean of two base medians, while the base itself moved 1.37 ms between rounds; against one base round
  alone, +0.96 ms. Inside the noise.
- Fable, like for like against the same build: 16.24 → 17.10 ms, 0.86 ms (1.053x); over a nine-row file, 4.90 → 5.54
  ms, 0.64 ms.
- The orchestrator's instruction-count trial, its `d2 / main` `k0_count` row: instructions 1.0053, cycles 0.9893,
  wall 1.0188 (medians per repetition 1,018.82M → 1,024.21M instructions, 23.55 → 23.99 ms).

**K2 and `k0_hour`, restated** as "no dearer than the first head", in place of the 1.45x ratio:
- the re-check verifier: 1.007x on `k0_hour` and 0.994x on K2, against the first head's package;
- Fable: 1.015x on `k0_hour` and 1.007x on K2;
- against `pkg-main-4c365814` the ratio moves with the core count: 1.436x on 10 cores, 1.47x to 1.50x on 16.

Up: [map.md](map.md).
