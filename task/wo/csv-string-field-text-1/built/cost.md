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

## Fix round 1 (head `8ac58878`, package `S/ddu-work/pkg-wire-8ac58878`)

Round 0's two scripts, copied unchanged apart from their output directories: five short rounds (base and head
alternating by round, each sample alone under the exclusive measure lock, cores 32-47, `perf stat` instruction
counts) and five C-012 runs.

| case | base ms | head ms | head / base | rounds | base spread | instr base | instr head | instr ratio | cycles ratio | answers equal |
|---|---|---|---|---|---|---|---|---|---|---|
| `k0_count` | 17.91 | 16.54 | **0.923x** | 5/5 | 16.82-18.52 | 1,029.4 M | 1,021.9 M | 0.9928 | 1.019 | True |
| `k0_hour` | 59.20 | 61.30 | **1.035x** | 5/5 | 56.86-62.47 | 4,071.2 M | 4,191.4 M | 1.0295 | 1.038 | True |
| `is_count` | 12.85 | 12.18 | **0.948x** | 5/5 | 11.74-13.54 | 705.9 M | 704.4 M | 0.9978 | 1.005 | True |
| `is_sum` | 22.30 | 25.05 | **1.123x** | 5/5 | 21.26-22.80 | 1,500.9 M | 1,619.4 M | 1.0790 | 1.105 | True |
| `is_none` | 25.99 | 20.19 | **0.777x** | 5/5 | 24.01-27.19 | 1,853.6 M | 1,256.5 M | 0.6779 | 0.702 | True |
| `is_infer` | 53.39 | 54.39 | **1.019x** | 5/5 | 52.39-57.31 | 3,958.4 M | 3,958.4 M | 1.0000 | 0.996 | True |

| case | main | head | head / main | main's band (max / min) | runs | bar | verdict |
|---|---|---|---|---|---|---|---|
| `c012p6` | 41.86 s | 39.79 s | **0.951x** | 1.129 | 5/5 | 1.05x hard | pass |
| `c012rest` | 42.89 s | 43.16 s | **1.006x** | 1.014 | 5/5 | 1.05x hard | pass |

- **`is_sum` is the flag, attributed by instruction counts to round 0's owner-accepted row, not to the fix.** Its
  head executes 1,619.4 M instructions per query in both rounds (ratio 1.0790 both times) and its cycles ratio is
  1.105 against round 0's 1.102; the head's wall time is 25.05 ms against round 0's 24.99 ms. The wall ratio moved
  from 1.058x to 1.123x because this round's base samples ran faster (22.30 ms against 23.62 ms, band 21.26-22.80).
  Under 1.15x; the fix round changes no field parse, only the scan's column names and two facade checks.
- **The other rows** are within 1.05x, and `is_infer` (unchanged code) is 1.0000x in instructions.
- **C-012** passes the hard 1.05x in both halves (20 samples, none with a nonzero exit). C-012 reads no CSV.

Raw: [raw/fix1-cost-samples.jsonl](raw/fix1-cost-samples.jsonl), [raw/fix1-cost-perf.txt](raw/fix1-cost-perf.txt),
[raw/fix1-c012-samples.jsonl](raw/fix1-c012-samples.jsonl).

Round 0's raw: [raw/cost-samples.jsonl](raw/cost-samples.jsonl), [raw/cost-perf.txt](raw/cost-perf.txt).

## Delta 2 (head `ef5abef1`, five rounds and five C-012 runs, run by the orchestrator)

The same scripts as fix round 1, copied into the delta's folder. The exclusive lock was used, on cores 32-47, base
against head in alternating order.

| case | base ms | head ms | head / base | instructions ratio | cycles ratio |
|---|---|---|---|---|---|
| `k0_count` | 17.76 | 16.51 | 0.929x | 0.9928 | 1.001 |
| `k0_hour` | 58.54 | 60.73 | 1.037x | 1.0295 | 1.045 |
| `is_count` | 12.58 | 12.21 | 0.970x | 0.9978 | 0.986 |
| `is_sum` | 22.49 | 24.08 | 1.070x | 1.0790 | 1.085 |
| `is_none` | 26.87 | 19.77 | 0.736x | 0.6779 | 0.693 |
| `is_infer` | 55.87 | 56.48 | 1.011x | 1.0000 | 1.002 |
| C-012 `c012p6` (s) | 39.87 | 39.84 | **0.999x** | | |
| C-012 `c012rest` (s) | 43.75 | 43.23 | **0.988x** | | |

- **C-012 passes** the hard 1.05x in both halves (20 samples, none with a nonzero exit).
- **`is_sum`** is round 0's owner-accepted row: the instruction ratio is 1.0790, as in round 0 and fix round 1. It is
  under 1.15x.
- **No cost row holds a Σ,** so `string_lowered` takes its unchanged path on every row.

Raw: [raw/delta2-cost-samples.jsonl](raw/delta2-cost-samples.jsonl), [raw/delta2-cost-perf.txt](raw/delta2-cost-perf.txt),
[raw/delta2-c012-samples.jsonl](raw/delta2-c012-samples.jsonl).

Up: [map.md](map.md).
