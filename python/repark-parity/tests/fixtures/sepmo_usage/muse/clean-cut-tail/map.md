# map — muse/clean-cut-tail

## Purpose

Every JSONL line parses, `exit` is present, and `run.terminal.completed` is
absent. Collect must emit a degraded record (`truncated: true`) and keep the
prefix step/tool counts.

## Contents

- [cmd.txt](cmd.txt) — muse argv.
- [prompt.md](prompt.md) — build-lane header.
- [out.jsonl](out.jsonl) — three tasks and three tools; no terminal event.
- [exit](exit) — `0`.

## Pointers

- Up: [../map.md](../map.md)
