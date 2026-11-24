# TEMP-VIEW-CACHE-LAZY-PARSE-1 A1 — cost (ten rounds, with the two rows attributed)

**The method.** Each sample ran under the measure lock's exclusive mode (with its turnstile), with the link and both
cargo locks held, on cores 48-57, one sample at a time, with the clock inside the lock. Each round ran main
(`pkg-rp1d-52f468ce`), the head (`pkg-a1-860c57c3`), then main again.
- **The probe** is [evidence/probes/a1_cost.py](evidence/probes/a1_cost.py), on a 2M-row JSON file under Los Angeles.
- **The ratio** is the head's median over the mean of main's and main again's medians. The table is
  [evidence/probes/a1_cost_table.py](evidence/probes/a1_cost_table.py)'s output.
- **The sample count:** an interrupted round 5 left 11 samples of `count_collect` and `collect_first`, 11, 11 and 10 of
  `view_count` (main, head, main again), and 10 of the two child cases.

| row | n | main ms | head ms | main again ms | ratio | head range | main range |
|---|---|---|---|---|---|---|---|
| cached read, `count` (it fills) | 11 | 306.5 | 346.3 | 294.6 | **1.152** | 244-393 | 233-324 |
| cached read, then `collect` | 11 | 1,782.4 | 1,772.0 | 1,788.1 | 0.993 | 1,743-1,779 | 1,750-1,849 |
| cached read, `count` then `collect`, total | 11 | 2,074.0 | 2,113.4 | 2,099.8 | 1.013 | 2,025-2,188 | 1,989-2,176 |
| cached read, `collect` first, total | 11 | 2,100.3 | 2,062.0 | 2,076.4 | 0.987 | 2,003-2,129 | 2,002-2,160 |
| a view of the cached frame: registration | 11 | 284.3 | 9.3 | 307.5 | 0.031 | 9-22 | 241-325 |
| the view's first `count` | 11 | 3.7 | 342.6 | 3.7 | — | 243-417 | 3-4 |
| the view, total | 11 | 288.0 | 351.9 | 311.3 | **1.174** | 252-427 | 244-329 |
| the selective child's first `count` | 10 | 62.6 | 295.1 | 62.1 | **4.735** | 241-363 | 54-74 |
| a second child's `count` | 10 | 56.1 | 3.0 | 57.0 | 0.052 | 3-4 | 51-67 |
| C-012 `c012p6` (s) | 5 | 39.7 | 39.6 | 39.7 | 0.998 | | |
| C-012 `c012rest` (s) | 5 | 43.1 | 43.6 | 43.1 | 1.010 | | |

- **C-012 is level** (0.998x and 1.010x over five runs), under the hard 1.05x.
- **The selective child (about 5x)** is Spark's contract for `cache()`: the child fills the parent. The owner accepted
  it on 2026-11-20, with a release-note line, and the second child's 0.05x is the paired win.
- **The view's registration moved its fill to the first read.** Main fills at `createOrReplaceTempView`; the head
  fills at the view's first `count`. The view's total is the row to compare.
- **Two rows are over 1.15x:** the first `count` (1.152x) and the view's total (1.174x). The owner asked for them to be
  attributed with instruction counts before ruling. The next section does that.

## The two rows, attributed: code layout, not A1's code

**The method.** `perf stat -D -1 --control fifo:…` counted only the action itself (`instructions:u`, `cycles:u`, and
later the branch, instruction-cache and iTLB misses), five rounds in alternating order. Every sample ran under the
exclusive lock on cores 48-57.
- **The probe** is [evidence/probes/a1_perf.py](evidence/probes/a1_perf.py), and the table is
  [evidence/probes/a1_perf_table.py](evidence/probes/a1_perf_table.py).
- **The cases:** `fill_count` is a cached frame's first `count`; `view_total` is `cache`, then
  `createOrReplaceTempView`, then the view's `count`.

**1. The head does the same work as main** (the first counting run).

| case | build | instructions | cycles |
|---|---|---|---|
| `fill_count` | main | 23,202 M | 1.000x |
| `fill_count` | head | 23,204 M (1.000x) | 1.068x |
| `fill_count` | head's native with main's Python (the swap package) | 23,202 M (1.000x) | 1.073x |
| `view_total` | main | 23,225 M | 1.000x |
| `view_total` | head | 23,236 M (1.000x) | 1.054x |
| `view_total` | the swap package | 23,225 M (1.000x) | 1.055x |
| a `count` with no cache (control) | head | 2,740 M (1.000x) | 0.998x |

The instructions match to 0.05%. The extra cycles come with the head's native library: the swap package carries them,
so the Python side contributes none.

**2. Where the extra cycles land.** A `perf record` profile, three rounds per build, was taken in user space.
- **The functions:** `repark_functions::spark_string_timestamp::string_to_timestamp_micros` took 27% more samples on
  the head (8,637 against 6,811 for `fill_count`), and the `chrono_tz` offset lookup took 12% to 34% more. A1 does not
  change either function; both parse the source file's timestamps, on both builds.
- **The addresses:** the function starts at different offsets in the two binaries (`…970` and `…1e0`).

**3. Placebo builds: builds that do not touch this path differ as much.** Three packages built from later commits
leave this read path alone, and run the same instructions as one another:
- main `9a48932f`;
- `pkg-wire-3bde1153` (CSV-STRING-FIELD-TEXT-1, a CSV reader change);
- `pkg-ice1a-58c7e307` (ICE-HELD-FRAME-SNAPSHOT-1 1a, an Iceberg catalog change).

Five rounds each, in a second counting run:

| build | instructions (`fill_count`) | cycles `fill_count` | cycles `view_total` |
|---|---|---|---|
| main (`52f468ce`) | 23,202 M | 1.000x | 1.000x |
| A1's head | 23,203 M | 1.071x | 1.071x |
| main `9a48932f` | 23,376 M | 1.000x | 1.010x |
| the CSV wiring build | 23,376 M | **1.094x** | **1.101x** |
| the ICE-HELD 1a build | 23,376 M | 1.060x | 1.071x |

The three later builds execute identical instructions to 0.001%, yet their cycles spread from 1.000x to 1.10x. A1's
1.07x sits inside that spread, and the CSV build, which never touches this path, is slower than A1. The
instruction-cache misses differ between builds too (A1 1.8x, 1a 1.4x, the CSV build 1.1x, main 0.9x), as code
placement would make them.

**4. Not parallelism, not the kernel.**
- **One CPU** (`taskset -c 48`, five rounds): the head's wall is 2,049 ms against main's 1,933 ms and later main's
  1,942 ms, so 1.057x. That equals its cycle ratio (1.061x), and the spread within a build is under 0.7% (1.8% for
  main with one outlier sample).
- **Ten CPUs:**
  - the CPUs used per sample (CPU time over wall) are 5 to 9 for every build, with no difference between them;
  - page faults are 109 to 751 per action, and the builds' medians differ by at most 63;
  - the same build's wall swings by 15% to 65% from sample to sample, which is why the ten-round medians read 1.15x
    and 1.17x against a 1.07x cycle gap.

**The attribution.** A1's instructions are within 0.05% of main's on both rows. The same unchanged timestamp-parsing code runs 6% to 7%
slower in A1's binary, a code-layout effect of the build. Builds that do not touch the path show the same effect, up to
10%. It cannot be fixed inside A1, because no A1 code causes it, and the next unrelated merge can move it either way. A
build-level fix (function and loop alignment, or profile-guided optimisation of the release build) would be its own
unit and would move every cost row. **The owner rules on the two rows,** as the cost rule requires for rows over
1.15x.
