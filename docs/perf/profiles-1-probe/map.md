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
  **REVIEW-FIX-8 (2026-09-11):** each run builds its subjects in a unique temporary
  directory (never under `docs/perf/`), sets `REPARK_CONFIG=""` before opening any
  session so no discovered file reaches a measurement, and scrubs the work prefix to
  `<work>` in every plan so two runs agree byte for byte. Run data stays under the
  system temporary root for the operator to delete. Pinned by
  `python/repark/tests/test_profiles1_probe_rerun.py`.
- [profiles1_probe2.py](profiles1_probe2.py) — multi-file follow-up on an 8-file
  directory subject: `pushdown_filters=false`, `repartition_file_scans=false`,
  `coalesce_batches=false` (alone and with `target_partitions=1`) against a
  same-process baseline. Produces the probe-2 rows the document cites. Same
  REVIEW-FIX-8 guards as the main probe (temporary directory, `REPARK_CONFIG=""`,
  `<work>` scrub).

## Pointers

- Up: [../map.md](../map.md)

The two scripts were run by the measuring worker in their original layout; the copies here
are `ruff format`ed (the repository's `py-lint` gate reads every tracked `*.py`, including these),
which moved line breaks only. No statement, value or captured output changed.

