# CSV-STRING-FIELD-TEXT-1, the wiring unit — the cost (2026-11-20)

Head package `S/ddu-work/pkg-wire-3bde1153`, base `S/ddu-work/pkg-main-9a48932f`. The rule (owner, 2026-11-19,
about 14:00 UTC): C-012 is a hard 1.05x; a short row over 1.05x is a flag, attributed by swap or instruction counts;
nothing over 1.15x passes without the owner.

## The short rows

- **The method** ([probes/cost.py](probes/cost.py), [probes/cost_table.py](probes/cost_table.py)): `N/perf-trial/probe.py`'s.
  Each sample is one process under `flock -x` on the measure lock, `nice -n 10`, cores 32-47: a warm-up, then ten
  timed queries inside a `perf stat -D -1 --control fifo` window (`instructions:u`, `cycles:u`). Base and head
  alternate in order by round. The row is the median over the rounds of each sample's median query. Session zone Los
  Angeles, as step 0's. Files: step 0's 2M-row `k.csv` (`n INT, ts TIMESTAMP`) and `is.csv` (`n,s` rows `0,s000` …).
- **Every sample gives the same answer on both builds** (`count` 2,000,000; `sum(n)` 1,999,999,000,000;
  `max(s)` `'s999'`; `max(hour(ts))` 23).

| case | base ms | head ms | head / base | rounds | base spread | instr base | instr head | instr ratio | cycles ratio | answers equal |
|---|---|---|---|---|---|---|---|---|---|---|
| `k0_count` | 18.63 | 17.50 | **0.939x** | 7/7 | 17.45-19.96 | 1,029.3 M | 1,021.8 M | 0.9927 | 1.043 | True |
| `k0_hour` | 60.63 | 61.33 | **1.012x** | 7/7 | 58.91-63.34 | 4,071.2 M | 4,191.2 M | 1.0295 | 1.113 | True |
| `is_count` | 12.58 | 12.57 | **0.999x** | 7/7 | 12.27-14.06 | 705.9 M | 704.3 M | 0.9977 | 0.993 | True |
| `is_sum` | 23.97 | 25.32 | **1.056x** | 7/7 | 22.14-25.35 | 1,500.9 M | 1,619.3 M | 1.0789 | 1.100 | True |
| `is_none` | 27.65 | 21.71 | **0.785x** | 7/7 | 26.10-28.18 | 1,853.4 M | 1,256.5 M | 0.6779 | 0.697 | True |
| `is_infer` | 56.73 | 54.76 | **0.965x** | 7/7 | 53.76-62.66 | 3,958.4 M | 3,958.4 M | 1.0000 | 1.014 | True |

- **`is_sum` is a flag, attributed to the unit.** Its instructions move with it, so it is not the build. The head
  scans `n` as text and parses it with the kernel, where main's scan parsed `Int64` inside arrow-csv and cast it to
  `Int32`. The kernel unit priced the parse alone at 46 M instructions per query over the same text
  ([../kernel/cost.md](../kernel/cost.md)); the rest of the gap is building the text column the scan used to skip.
  It is Spark's rule (a field is converted from its text), under 1.15x, and step 0 predicted it (1.055x with
  arrow's `CAST`). It does not touch C-012.
- **`k0_hour`** stays under 1.05x in wall time; its 2.9 % more instructions are the kernel's Spark timestamp grammar
  in place of `CAST` under the zone, against main's second inference and re-read, which are gone.
- **The gains.** `k0_count` drops the second inference and read; `is_none` drops the integer scan and its cast to
  text. `is_infer` (`inferSchema=true`, no schema) runs unchanged code: 1.0000x in instructions.

## The retired cost line

Unit 0's "a CSV read whose schema holds a `TIMESTAMP` field pays one extra schema inference, at most 1 ms per read"
retires: the head reads once, with a header-only inference. `k0_count` (unit 0's row for it) reads 17.50 ms against main's 18.63 ms (**0.939x**).

## C-012

**Not yet measured** (the builder's turn budget ran out under lock contention; the hand-back names the exact
command). C-012 reads no CSV ([R]: `replay_work.py` and its directory).

## Rounds

Seven complete rounds of ten: the exclusive samples starved behind the other lane's cargo tests and C-012 series
(one sample waited about 20 minutes), so the eighth round's partial samples were discarded and the series stopped.

Raw: [raw/cost-samples.jsonl](raw/cost-samples.jsonl), [raw/cost-perf.txt](raw/cost-perf.txt).

Up: [map.md](map.md).
