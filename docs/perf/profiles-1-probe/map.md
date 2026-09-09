# map — docs/perf/profiles-1-probe

## Purpose

The two runnable probe scripts behind
[profiles-1-passthrough-probe-2026-09-09.md](../profiles-1-passthrough-probe-2026-09-09.md)
(PROFILES-1 step 0). Each script generates its own data directory beside itself, so no
fixture setup precedes a run. Kept verbatim from the measuring worker's originals except
for the script-relative `WORK` directory.

## Contents

- [profiles1_probe.py](profiles1_probe.py) — twenty-key pass-through probe: one session
  per D-1 key via `builder.config`, `conf.get` / `conf.getAll` read-back, runtime
  `conf.set`, join / agg / scan explains, `range(10)` batch counts, written-file facts,
  validation and runtime probes. Produces the document's §§2–4.
- [profiles1_probe2.py](profiles1_probe2.py) — multi-file follow-up on an 8-file
  directory subject: `pushdown_filters=false`, `repartition_file_scans=false`,
  `coalesce_batches=false` (alone and with `target_partitions=1`) against a
  same-process baseline. Produces the probe-2 rows the document cites.

## Pointers

- Up: [../map.md](../map.md)
