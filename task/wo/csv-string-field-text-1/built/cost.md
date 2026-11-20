# CSV-STRING-FIELD-TEXT-1, the wiring unit — the cost (2026-11-20)

Head package `S/ddu-work/pkg-wire-3bde1153`, base `S/ddu-work/pkg-main-9a48932f`. The rule (owner, 2026-11-19,
about 14:00 UTC): C-012 is a hard 1.05x; a short row over 1.05x is a flag, attributed by swap or instruction counts;
nothing over 1.15x passes without the owner.

## The short rows

- **The method** ([probes/cost.py](probes/cost.py), [probes/cost_table.py](probes/cost_table.py)): `N/perf-trial/probe.py`'s.
  Each sample is one process under the exclusive measure lock, `nice -n 10`, cores 32-47: a warm-up, then ten
  timed queries inside a `perf stat -D -1 --control fifo` window (`instructions:u`, `cycles:u`). Base and head
  alternate in order by round. The row is the median over the rounds of each sample's median query. Session zone Los
  Angeles, as step 0's. Files: step 0's 2M-row `k.csv` (`n INT, ts TIMESTAMP`) and `is.csv` (`n,s` rows `0,s000` …).
- **Every sample gives the same answer on both builds** (`count` 2,000,000; `sum(n)` 1,999,999,000,000;
  `max(s)` `'s999'`; `max(hour(ts))` 23).

| case | base ms | head ms | head / base | rounds | base spread | instr base | instr head | instr ratio | cycles ratio | answers equal |
|---|---|---|---|---|---|---|---|---|---|---|
| `k0_count` | 18.12 | 17.20 | **0.950x** | 10/10 | 17.21-19.96 | 1,029.3 M | 1,021.8 M | 0.9927 | 1.035 | True |
| `k0_hour` | 60.59 | 62.03 | **1.024x** | 10/10 | 58.91-63.67 | 4,071.2 M | 4,191.2 M | 1.0295 | 1.113 | True |
| `is_count` | 12.58 | 12.53 | **0.997x** | 10/10 | 12.17-14.06 | 705.9 M | 704.3 M | 0.9976 | 0.992 | True |
| `is_sum` | 23.62 | 24.99 | **1.058x** | 10/10 | 22.14-25.35 | 1,500.9 M | 1,619.4 M | 1.0789 | 1.102 | True |
| `is_none` | 27.60 | 21.15 | **0.767x** | 10/10 | 26.10-28.18 | 1,853.5 M | 1,256.5 M | 0.6779 | 0.698 | True |
| `is_infer` | 56.65 | 55.22 | **0.975x** | 10/10 | 52.33-64.72 | 3,958.4 M | 3,958.4 M | 1.0000 | 1.008 | True |

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
retires: the head reads once, with a header-only inference. `k0_count` (unit 0's row for it) reads 17.20 ms against main's 18.12 ms (**0.950x**).

## C-012

| case | main | head | head / main | main's band (max / min) | runs | bar | verdict |
|---|---|---|---|---|---|---|---|
| `c012p6` | 40.05 s | 39.93 s | **0.997x** | 1.099 | 10/10 | 1.05x hard | pass |
| `c012rest` | 43.39 s | 43.40 s | **1.000x** | 1.121 | 10/10 | 1.05x hard | pass |

Process wall of `replay_work.py`'s two halves, main and head alternating by run, each sample alone under the
exclusive measure lock, cores 32-47; 40 samples, none with a nonzero exit
([raw/c012-samples.jsonl](raw/c012-samples.jsonl)). C-012 reads no CSV.

## Rounds

Ten rounds: rounds 1 to 7 ran first; rounds 8 to 10 ran after the gates, with the order still alternating by
round.

Raw: [raw/cost-samples.jsonl](raw/cost-samples.jsonl), [raw/cost-perf.txt](raw/cost-perf.txt).

Up: [map.md](map.md).
