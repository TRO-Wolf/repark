# map — docs/sepmo/

## Purpose

SEPMO efficiency-pilot documents that live in the repository: telemetry
inventory, compact worker packets, and proposed worker-wrapper patches. Process
canon stays under [../../.agents/skills/sepmo/](../../.agents/skills/sepmo/map.md).
This directory is campaign class. It closes when the efficiency pilot's E-7
outcome is recorded.

## Contents

- [telemetry/](telemetry/map.md) — E-0 inventory, the usage-record schema, and
  wrapper-patch proposals for the four worker adapters.
- [packets/](packets/map.md) — E-2 compact worker packet format v1, schema,
  baseline comparison, and wrapper-consumption proposal.
  pins: sepmo-e2/C-001, C-006, C-007

## I want to...

| ...do this | go to |
|---|---|
| See which usage fields each worker actually reports | [telemetry/inventory.md](telemetry/inventory.md) |
| Read the normalized usage-record schema | [telemetry/usage-record.schema.json](telemetry/usage-record.schema.json) |
| Collect a run directory | [../../scripts/sepmo_usage.py](../../scripts/sepmo_usage.py) |
| Read the compact worker packet format | [packets/packet-format.md](packets/packet-format.md) |
| Build or check a packet | [../../scripts/sepmo_packet.py](../../scripts/sepmo_packet.py) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../../.agents/skills/sepmo/map.md](../../.agents/skills/sepmo/map.md)

## Debug

| Symptom | First check |
|---|---|
| A usage field is null | [telemetry/inventory.md](telemetry/inventory.md) §2 — Muse cost and Claude remain unavailable |
| Collector rejects a run dir | `cmd.txt` missing, majority-bad JSONL, or a `://` URI; minority truncated JSONL is a degraded record; see [telemetry/map.md](telemetry/map.md) |
