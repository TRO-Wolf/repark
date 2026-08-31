# map — docs/examples/

## Purpose

Executable worked examples of the public surface. The v0.7 drift gate
(`scripts/check_example_coverage.py`, `make check-example-coverage`) walks the
facade, reads each script's `COVERS` list, and fails when a public name is
neither covered nor listed in the backlog ratchet or the cloud exceptions file.

Examples run against local filesystem and a memory catalog only. They are not a
substitute for pins in `python/repark/tests/` — they teach the public name.
Each `COVERS` name must appear as a real use in that script (an `F.*` / `ta.*`
call or attribute, or a method/property access). `exceptions.txt` is an exact
count ratchet (`EXCEPTIONS_BASELINE`); a new row is a visible baseline bump.

This file closes when the v0.7 example backfill is complete and the backlog
file is empty.

## Contents

- [inventory.txt](inventory.txt) — checked-in snapshot of the enumerator
  (`family<TAB>name`). Must match the AST walk.
- [backlog.txt](backlog.txt) — uncovered public names. Count ratchets down only
  (`BACKLOG_BASELINE` in the gate script).
- [exceptions.txt](exceptions.txt) — public names whose only honest example
  needs a cloud service, each with a one-line reason.
- [functions/](functions/map.md) — `F.*` examples.
- [dataframe/](dataframe/map.md) — DataFrame / GroupedData / na / stat examples.
- [ta/](ta/map.md) — TA kernel examples.
- [io/](io/map.md) — reader / writer examples.
- [session/](session/map.md) — `repark.sql` and SparkSession door examples.

## I want to...

| I want to... | go to |
|---|---|
| See which public names still need an example | [backlog.txt](backlog.txt) |
| Add an example for a name | a new `*.py` under the family dir with `COVERS`, then drop the name from the backlog and ratchet `BACKLOG_BASELINE` |
| Record a cloud-only name | [exceptions.txt](exceptions.txt) |
| Run the gate | `make check-example-coverage` |

## Pointers

- Up: [../map.md](../map.md)
- Gate: [../../scripts/check_example_coverage.py](../../scripts/check_example_coverage.py)
- Ruling: [../../task/roadmap/epic-term/release-roadmap-2026-08-29.md](../../task/roadmap/epic-term/release-roadmap-2026-08-29.md)
- Ledger: [../../task/ledgers/staging/ex-0-example-drift-gate-ledger.md](../../task/ledgers/staging/ex-0-example-drift-gate-ledger.md)

## Debug

| Symptom | First check |
|---|---|
| `public name … has no example` | Add `COVERS` or a backlog row (new names cannot join the backlog) |
| `backlog still lists` | Remove the name from `backlog.txt` and lower `BACKLOG_BASELINE` |
| `skipping example execution` | Native module is not importable — `make develop` |
