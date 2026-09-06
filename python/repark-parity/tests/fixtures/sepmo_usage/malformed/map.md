# map — malformed fixtures

## Purpose

Inputs the collector must reject (majority-bad JSON/JSONL) or emit as a
degraded record (minority truncated JSONL).

## Contents

- [bad-json/](bad-json/map.md) — truncated Grok `out.json` (loud failure).
- [bad-jsonl/](bad-jsonl/map.md) — Muse JSONL that is not JSON (majority-bad).
- [truncated-tail/](truncated-tail/map.md) — JSONL last line cut; degraded record.

## Pointers

- Up: [../map.md](../map.md)
