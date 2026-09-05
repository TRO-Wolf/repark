# map — python/repark-parity/bench/facade

## Purpose

**PERF-FACADE-1** measurement harness for the facade boundary: `collect()` row
materialization and `withColumn` chain building. The fixture generator is checked in;
parquet lands under `/tmp/oc-facade-bed/` (never the repo). One session, one process, no JVM
— every cell is repark-only, so this battery does not contend for the box's single Spark JVM.

pins: perf-facade-1/C-001, C-006, C-007, C-009

Docstrings here are one line each: `check_docstring_presence` (D101/D102/D103/D105/D107)
requires one, and nothing may say more. Reasons live in this map, not in the source.

## Why this harness exists

The unit's first baseline named probe scripts under an untracked `scratch/` directory. Numbers
whose runner is not in the tree cannot be re-derived by anyone else, so they are not a baseline
(the same rule `docs/perf/map.md` states as H-3). Everything the
`docs/perf/facade-boundary-baseline.md` tables report is produced by `run_facade.py` here.

## The before/after contract

The pre-unit numbers are **not** a stale column from another hour or another module. Both legs
run in one process on one release module, and the harness reconstructs the pre-unit code path
rather than quoting it:

| cell pair | old leg | new leg |
|---|---|---|
| `collect_old/N` vs `collect/N` | `DataFrame._rows_from_arrow_table` swapped back to the pure-Python converter — end to end, result list held | shipped |
| `rows_old/N` vs `rows_new/N` | `rows_export.rows_from_arrow_table_python`, the untouched pure-Python converter | `rows_export.rows_from_arrow_table`, the shipped path |
| `chain_old/D` vs `chain/D` | `_old_columns` / `_old_iter_bound_columns` in `cells.py`, the pre-unit bodies, swapped onto `DataFrame` for the timed region and restored in a `finally` | the shipped bodies |
| `create_old/N/*` vs `create/N/*` | the rows-module dispatcher swapped back to `_arrow_table_from_raw_tuples_legacy` — end to end, `count()` held | shipped |

Both legs see the same fixture, the same load and the same native module, so a difference is
the code path and nothing else. The reconstruction is faithful only while those two functions
still mirror what `PERF-FACADE-1` replaced; if `columns`, `_iter_bound_columns` or
`_rows_from_arrow_table` changes again, the old leg stops being the right comparison and must
be re-derived or dropped. The `create_old` leg is not a reconstruction — it calls the kept
legacy path itself, so it stays the right comparison until the legacy path is removed.

pins: perf-facade-cdf-1/C-001, C-005, C-006, C-008

## The createDataFrame cells (PERF-FACADE-CDF-1)

`tuples_count` reuses the seed-42 seven-column rows as in-memory tuples with a name-list
schema — the path the unit rewrites. `nested_count` builds 10,000 deterministic nested rows in
memory (`id`, a two-int list, a two-key dict, a two-field tuple) with a name-list schema, so
the delegated slow arm is measured on both legs. `explicit_count` runs the same 1e5 tuples
under a DDL schema, so the legacy-dispatched path is measured on both legs and must read ~1×.
Input construction stays outside every timed region; the timed region is `createDataFrame`
plus `count()`, exactly as the pre-unit control cells timed it.

`collect_old/N` and `rows_old/N` measure different things on purpose: the first holds a million
`Row` objects as `collect()` must, the second releases each batch's rows. The gap between them
on the shipped path is the cost of holding the result, which is where the remaining headroom
is; on the pre-unit path the two are indistinguishable because it was collector-bound anyway.

## Cell contract

| rule | what it means |
|---|---|
| release | `release_proof()` raises rather than measure a debug native module |
| thread parity | `spark.sql.shuffle.partitions = 8`; no all-cores column, no Spark leg |
| iterations | 5 timed after 1 warm-up; `collect`-family cells cap at 3 because each materializes a million rows |
| floor | `collect/100000` repeated `--floor-repeats` times; the floor is the spread of those medians, re-measured every run because it is a property of the box that hour |
| reported | median, min, spread (max − min of the samples) and the 1-minute load at both ends of every cell |
| never | a sum across cells, or a cost read against a floor from a different run |

## The collapsed-chain cell

`chain_collapsed/D/build_only` exists to bound the option `PERF-FACADE-CHAIN-2` defers, so its
definition has to be exact or the deferral rests on a number nobody can reproduce:

- one expression list is built **once** and appended to, never rebuilt per step — a real
  collapse would keep its list, and rebuilding it is O(depth²) Python the design does not pay;
- each step projects the 7 base columns plus the `i+1` computed expressions **directly onto the
  base frame**, discarding the previous projection, so the child schema stays 7 fields wide;
- the timed region is the whole loop, expression construction included, exactly as
  `chain/D/build_only` times the whole `withColumn` loop. Timing only the `select` calls
  measures a different thing and is not comparable to the stacked cell.

It is a *shape*, not an implementation: it shows what a perfect collapse would cost, and
deliberately skips every correctness obligation a real one carries (plan lineage,
`_origin_plan_id`, `MISSING_ATTRIBUTES`, the adjacent-window-layer merge). Read it as an upper
bound on the prize, never as evidence that the change is safe.

## Contents

- `fixture.py` — the seed-42 seven-column parquet (`id`, `ts`, `v`, `vi`, `s`, `cat`, `part`),
  written once per size under the bed and reused.
- `cells.py` — release proof, load sampling, `time_cell`, the chain builders and the three
  pre-unit reconstructions (`_old_columns`, `_old_iter_bound_columns`, `collect_with_old_converter`).
- `measure.py` — orchestrator, floor, markdown renderer.
- `run_facade.py` — CLI (`--cells`, `--iterations`, `--floor-repeats`, `--out`, `--json`,
  `--report`).
- `__init__.py` — package marker.
- `map.md` — this file.

## I want to…

| I want to… | Go to |
|---|---|
| Run the whole battery | `.venv/bin/python python/repark-parity/bench/facade/run_facade.py --out /tmp/oc-facade-bed` |
| Run one group | `… run_facade.py --cells chain` (groups: `export`, `collect`, `rows`, `create`, `chain`) |
| Run it the way `make` does | `PYTHON=.venv/bin/python make facade-bench` |
| Read the numbers | [../../../../docs/perf/facade-boundary-baseline.md](../../../../docs/perf/facade-boundary-baseline.md) |

The `make` target resolves its interpreter through `$(PYTHON)`, default `python`. The runner
imports `repark`, `pyarrow` and `numpy`, so it needs the project venv: inside an activated venv
the default works, and outside one pass `PYTHON=.venv/bin/python`. The neighbouring
`dynflatten-bench` target hard-codes bare `python` and carries the same requirement without the
override.

## Debug

| Symptom | First check |
|---|---|
| `refusing to measure on a debug native build` | `maturin develop --release`, then re-check `_native.__debug_assertions__` |
| `python: not found` from `make facade-bench` | pass `PYTHON=.venv/bin/python`, or activate the venv first |
| medians drift between runs | read the 1-minute load columns; sibling lanes building Rust move every cell, which is why both legs of each pair run in one process |
| `chain_old` is not ~6× `chain` | the reconstructed pre-unit bodies no longer mirror the shipped ones; see "The before/after contract" |
| fixture looks stale | delete the bed directory; `ensure_fixture` only writes when the file is absent |

## Pointers

- Up: [../map.md](../map.md)
- Numbers: [../../../../docs/perf/facade-boundary-baseline.md](../../../../docs/perf/facade-boundary-baseline.md)
- Ledger: [../../../../task/ledgers/staging/perf-facade-1-ledger.md](../../../../task/ledgers/staging/perf-facade-1-ledger.md)
