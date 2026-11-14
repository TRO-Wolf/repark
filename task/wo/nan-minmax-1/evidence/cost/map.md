# task/wo/nan-minmax-1/evidence/cost/ — the cost rows

NAN-MINMAX-1 part 1, 2026-11-13, model `claude-opus-5-5`. Main `631d283c` (module `3f603240…`) against the head
(module `6eafceab…`), both release wheels, the Spark door, no Spark JVM.

**Data** (`cost_probe.py data <root>`): `many`, 100 Parquet files of 100,000 rows (`id BIGINT`, `d DOUBLE = id`),
two row groups each, disjoint ranges; `single`, one file of 10,000,000 rows (`d = id * 0.5`), ten row groups.

**Method** (`cost_probe.py <build> <root> <out>`). One process per sample: three warm-ups, then 15 timed
`collect()` runs per query, the median kept, and one `EXPLAIN ANALYZE` for the scan metrics. Six alternating
main/head pairs. Every sample held `flock -x measure.lock`, then both cargo locks, on cores 48–57 with a 48 GiB
address-space limit. `cost_probe.py report <raw> <base> <head>` prints the tables below.

- `count_all`: `SELECT count(*) FROM many` (held to 1.05x).
- `count_selective`: `SELECT count(*) FROM many WHERE d > 9949999.5`, one row group of 200 (held to 1.05x; the row
  groups scanned must equal main's).
- `max_stats`: `SELECT max(d) FROM single` (reported, not held: main answers it from the footer).

## The head

Pairs: 6 (base `base`, head `head2`), 15 timed runs per query per sample.

| query | answer | main median (ms) | head median (ms) | head / main (median of pair ratios) |
|---|---|---|---|---|
| `count_all` | [[10000000]] | 4.26 | 4.43 | 1.040 |
| `count_selective` | [[50000]] | 11.25 | 10.80 | 0.958 |
| `max_stats` | [[4999999.5]] | 2.26 | 21.19 | 9.048 |

| query | build | row groups (pruned by statistics) | file ranges (pruned by statistics) |
|---|---|---|---|
| `count_all` | base | none | none |
| `count_all` | head2 | none | none |
| `count_selective` | base | 2 total → 1 matched -> 1 fully matched | 109 total → 1 matched |
| `count_selective` | head2 | 2 total → 1 matched -> 1 fully matched | 109 total → 1 matched |
| `max_stats` | base | none | none |
| `max_stats` | head2 | 10 total → 10 matched | 10 total → 7 matched |

**Read:** `count(*)` and the selective filter hold (1.040x, 0.958x), and the selective filter prunes exactly as main
does: 108 of 109 file ranges by file statistics, then 1 of 2 row groups. `max(d)` now reads the column (about 9x: 21 ms
over ten million rows, from 2 ms); DataFusion's aggregate dynamic filter skips 3 of its 10 file ranges.

## The first head (replaced)

The first head demoted every float bound per file. Its six pairs (`cost-raw-first-head.jsonl`, the same method):

| query | answer | main median (ms) | head median (ms) | head / main (median of pair ratios) |
|---|---|---|---|---|
| `count_all` | [[10000000]] | 4.07 | 4.40 | 1.059 |
| `count_selective` | [[50000]] | 10.38 | 11.88 | 1.166 |
| `max_stats` | [[4999999.5]] | 1.89 | 33.00 | 17.187 |

It lost file-level pruning (109 file ranges opened, 1 of 200 row groups scanned): the row groups scanned stayed equal,
but the selective filter ran 1.166x main, over the hold. DataFusion's file pruner reads only exact bounds, so the head
keeps a spread file's bounds exact and demotes only equal ones, plus every float bound at the scan.

- `cost_probe.py` — data, the per-sample runner, the report.
- `cost-raw.jsonl` — the head's twelve samples (one JSON record per sample; `build` `base` or `head2`).
- `cost-raw-first-head.jsonl` — the first head's twelve samples (`build` `base` or `head`).

Up: [../map.md](../map.md).
