# map — python/repark-parity/tests/fixtures/sepmo_usage/

## Purpose

Sanitized worker run-dir shapes for `scripts/sepmo_usage.py`. Each child is one
adapter or one failure class. No home paths, no secrets, no live token dumps.

## Contents

- [muse/](muse/map.md) — Muse JSONL run dir (happy path and session-store join).
- [grok/](grok/map.md) — Grok `out.json` shapes (cost-only, live usage keys, empty).
- [opencode/](opencode/map.md) — OpenCode NDJSON with step-finish tokens.
- [claude/](claude/map.md) — Claude marker with no transcript.
- [malformed/](malformed/map.md) — truncated JSON / JSONL that must fail loudly.

## Pointers

- Up: [../map.md](../map.md)
- Related: [../../../../../scripts/sepmo_usage.py](../../../../../scripts/sepmo_usage.py)

## Debug

| Symptom | First check |
|---|---|
| Collector cannot detect the adapter | `cmd.txt` must name muse/grok/opencode/claude |
