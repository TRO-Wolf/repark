# map — malformed/truncated-tail

## Purpose

Muse JSONL whose last line is cut mid-string and which has no
`run.terminal.completed` event. Collect must fail loudly.

## Contents

- [cmd.txt](cmd.txt) — muse argv.
- [out.jsonl](out.jsonl) — two valid events plus a truncated tail.

## Pointers

- Up: [../map.md](../map.md)
