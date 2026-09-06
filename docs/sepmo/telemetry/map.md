# map — docs/sepmo/telemetry/

## Purpose

E-0 / E-1 telemetry homes: the measured adapter inventory, the usage-record
schema the collector emits, and wrapper-patch proposals (the wrappers themselves
live outside the repo and are not edited here).

This file closes when the SEPMO efficiency pilot records its E-7 outcome.

## Contents

- [inventory.md](inventory.md) — capability and telemetry inventory for Muse,
  opencode/kilo, Grok, and Claude sub-agents; frozen 2026-09-05/06 Muse baseline;
  pilot-strata mapping.
  pins: sepmo-e0-e1/C-001, C-002, C-003
- [usage-record.schema.json](usage-record.schema.json) — normalized record
  emitted by `scripts/sepmo_usage.py collect`. Every payload field is nullable;
  `missing_reason` names why a field is absent; `units` states the unit of each
  numeric field.
  pins: sepmo-e0-e1/C-004
- [wrapper-patches/](wrapper-patches/map.md) — proposed patches for the
  out-of-repo worker wrappers. The orchestrator applies them; this unit does not
  edit `$HOME/.claude/`.

## I want to...

| ...do this | go to |
|---|---|
| See what a Muse / Grok / OpenCode / Claude run actually records | [inventory.md](inventory.md) |
| Validate a collector record | [usage-record.schema.json](usage-record.schema.json) |
| Propose a wrapper change | [wrapper-patches/](wrapper-patches/map.md) |
| Run the collector | [../../../scripts/sepmo_usage.py](../../../scripts/sepmo_usage.py) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../../../scripts/map.md](../../../scripts/map.md)

## Debug

| Symptom | First check |
|---|---|
| Tokens are null on a Muse record | Expected — [inventory.md](inventory.md) Muse row |
| OpenCode tokens missing | No `out.ndjson` in the run dir; sqlite is documented, not opened |
| Grok tokens missing | `out.json` empty or has no `usage` object — [inventory.md](inventory.md) Grok row |
