# CSV-FIELD-KERNEL-1 — the cost (Fable §10 item 12), before any wiring

Head package `S/ddu-work/pkg-kernel1-ff85b57e`; C-012's base is main's `S/ddu-work/pkg-rp1base-8bc26cba`.

## The kernel against arrow's `CAST`

- **The case.** Step 0's 2M-row `INT` file (`is.csv`, `n,s` rows `0,s000` …), read all-text through the local
  pre-scan (`read_csv` with `utf8_columns`), then `sum(n)`, `max(s)`, `count(n)` with `n` either
  `CAST(column_1 AS INT)` (arrow's cast, the reader's cast today) or the kernel through the Python builder
  (`csv_field("int")`). Both answer `sum(n)=1999999000000, max(s)='s999', count(n)=2000000` in every sample.
- **The method** ([probes/cost.py](probes/cost.py)): `N/perf-trial/probe.py`'s, five rounds, the order alternating
  by round, each sample one process under `flock -x` on the measure lock, `nice -n 10`, cores 32-47: a warm-up,
  then ten timed queries inside a `perf stat -D -1 --control fifo` window (`instructions:u`, `cycles:u`). The row is
  the median over the rounds of each sample's median query.

| variant | median query, ms | rounds' medians, ms | `instructions:u` per query | `cycles:u` per query |
|---|---|---|---|---|
| arrow `CAST` | 23.40 | 26.11, 21.25, 23.77, 23.34, 23.40 | 1,573.3 M | 481.0 M |
| the kernel | 23.47 | 25.78, 23.64, 23.33, 23.11, 23.47 | 1,619.2 M | 498.3 M |
| **kernel / `CAST`** | **1.003x** | | **1.029x** | **1.036x** |

- **Attribution.** The instruction counts are steady to 0.01 % across rounds on both variants. The kernel spends
  about 46 M more user instructions per query, about 23 per row. By reading (no swap was needed under the rule),
  that is Java's grammar: the sign, the per-digit check and the overflow checks through `i64`, where arrow's cast
  parses a plain `i32`. The wall time does not see it.
- **The ASCII fast path.** The kernel is ASCII throughout: the integer, float and decimal grammars are byte loops,
  so no row reaches a Unicode path (Unicode digits are the declared difference).
- **Against the short-row rule:** the whole query is under 100 ms and under 1.05x on every measure, so no flag.

## C-012

`replay_work.py`'s two halves (`c012p6`, `c012rest`), process wall, five runs, each run main, head, main again, one
sample at a time under `flock -x` on the measure lock, cores 32-47. All 30 samples exit 0.

| case | main | head | main again | head / main | band (main again / main) | bar | verdict |
|---|---|---|---|---|---|---|---|
| `c012p6` | 40.62 s | 39.93 s | 39.99 s | **0.983** | 0.984 | 1.05x hard | pass |
| `c012rest` | 44.20 s | 43.46 s | 43.37 s | **0.983** | 0.981 | 1.05x hard | pass |

Nothing in C-012 reads CSV, and nothing reads through the kernel in this unit; the rows sit inside main's own band.

Raw: [raw/cost-samples.jsonl](raw/cost-samples.jsonl), [raw/cost-perf.txt](raw/cost-perf.txt),
[raw/c012-samples.jsonl](raw/c012-samples.jsonl).

Up: [map.md](map.md).
