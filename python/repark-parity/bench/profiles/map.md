# map — python/repark-parity/bench/profiles

## Purpose

PROFILES-1 step 1: the measurement bed step 2 sweeps alone on the box on a release
build. Datasets, query shapes, and a knob × value timing harness writing one CSV row
per (dataset, query, knob, value, repetition, seconds). No profiles, no timings as
results, no `docs/perf/config-profiles-*.md` — those are steps 2–3.

## Contents

- [datasets.py](datasets.py) — three D-2 datasets under a scratch root: the owner's
  futures parquet (referenced in place at full scale; a tiny same-schema stand-in for
  `--smoke`), TPC-H through the shared `tpch.datagen.ensure_parquet_sf` dbgen path
  (SF10 full, SF0.01 smoke — the smallest scale that path accepts), and a local
  memory-catalog Iceberg v2 table (`PARTITIONED BY (g)`, CTAS plus one append per file:
  200 files full, 4 smoke). pins: profiles-1/C-001
- [queries.py](queries.py) — five reads (`scan_filter`, `group_by` on futures+tpch;
  `hash_join`, `sort_merge_join` on tpch; `window` on futures) and three writes
  (`append_files` × 8, `overwrite_partition` one `PARTITION (g = '7')`,
  `merge_updates` at `MERGE_UPDATE_FRACTION = 0.10`). Every query forces execution
  through the Arrow path (`to_arrow`), never only `show`.
  pins: profiles-1/C-002
- [harness.py](harness.py) — `require_quiet_box` (`pgrep -f java` empty or the timed
  run refuses), `measure` (quiet check before every repetition), `write_cell_rows`
  (header once, one row per repetition), `median_seconds`, `smoke_config`.
  pins: profiles-1/C-003, C-004, C-005
- [run_profiles.py](run_profiles.py) — CLI entry: `--scratch` (required), `--out`,
  `--knob` + `--values`, `--repeats`, `--smoke`, `--futures`. One session per value
  (stopped after, as the step-0 probe did); the write table is rebuilt before every
  timed write repetition so appends never see another cell's rows. Non-smoke runs
  refuse unless `--repeats 3` (D-3); values without a knob refuse. `write.*` knobs
  are Iceberg table properties the session conf only stores, so step 2's fix lands
  them on the rebuilt bed table with `ALTER TABLE … SET TBLPROPERTIES` before the
  timed write (`table_property_alter`), leaving every bed identical across cells.
  pins: profiles-1/C-007
- `map.md` — this file.

## The accepted-but-unread nine

Step 0 left 9 of 20 D-1 keys `ACCEPTED BUT UNREAD`
(`docs/perf/profiles-1-passthrough-probe-2026-09-09.md` §§2–3): no plan text shows
them, so the probe could not confirm they reach the engine. This bed sweeps all 20
keys anyway and expects a flat line on those nine. The bed measures wall time, which
is exactly the execution-level signal the probe lacked (its open question (b)), so a
flat line is itself the measurement step 2 records — not a reason to skip the knob.

## I want to…

| I want to… | Go to |
|---|---|
| Prove the harness at tiny scale | `python python/repark-parity/bench/profiles/run_profiles.py --smoke --scratch <dir>` |
| Sweep one knob (release build, idle box, step 2 only) | `…/run_profiles.py --scratch <dir> --knob datafusion.execution.batch_size --values 1024,4096,8192` |
| Sweep the baseline (no knob) | `…/run_profiles.py --scratch <dir>` |
| Read the step-0 pass-through verdicts | [../../../../docs/perf/profiles-1-passthrough-probe-2026-09-09.md](../../../../docs/perf/profiles-1-passthrough-probe-2026-09-09.md) |

## Constraints

- Never run a sweep while another lane builds: `require_quiet_box` refuses on any JVM,
  and step 2 runs alone on the box. `--smoke` proves the harness, not a knob.
- `pgrep -f java` matches full command lines, so a shell whose own command contains
  that string self-matches; the runner itself is immune (its command line never
  contains it). A refusal names the PIDs, so a false positive is checkable.
- Debug native is enough for `--smoke`; step 2 uses `maturin develop --release`.
- Never commit the scratch root, the CSV, or the warehouse.
