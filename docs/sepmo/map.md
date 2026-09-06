# map — docs/sepmo/

## Purpose

SEPMO efficiency-pilot documents that live in the repository: telemetry
inventory, the usage-record schema, and proposed worker-wrapper patches. Process
canon stays under [../../.agents/skills/sepmo/](../../.agents/skills/sepmo/map.md).
This directory is campaign class. It closes when the efficiency pilot's E-7
outcome is recorded.

## Contents

- [telemetry/](telemetry/map.md) — E-0 inventory, the usage-record schema, and
  wrapper-patch proposals for the four worker adapters.

## I want to...

| ...do this | go to |
|---|---|
| See which usage fields each worker actually reports | [telemetry/inventory.md](telemetry/inventory.md) |
| Read the normalized usage-record schema | [telemetry/usage-record.schema.json](telemetry/usage-record.schema.json) |
| Collect a run directory | [../../scripts/sepmo_usage.py](../../scripts/sepmo_usage.py) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../../.agents/skills/sepmo/map.md](../../.agents/skills/sepmo/map.md)

## Debug

| Symptom | First check |
|---|---|
| A usage field is null | [telemetry/inventory.md](telemetry/inventory.md) §2 — many adapter fields are unavailable |
| Collector rejects a run dir | `cmd.txt` missing, or the event stream is malformed; see [telemetry/map.md](telemetry/map.md) |
