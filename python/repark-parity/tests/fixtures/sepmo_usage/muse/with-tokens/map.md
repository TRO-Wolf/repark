# map — muse/with-tokens

## Purpose

Muse run dir joined to a sanitized session-store excerpt. Tokens come from
`session.jsonl` per-turn `model_completed.usage`, cross-checked against the
snapshot cumulative. Cost stays null.

## Contents

- [cmd.txt](cmd.txt) — `muse exec` argv.
- [prompt.md](prompt.md) — build-lane header (role stays null).
- [out.jsonl](out.jsonl) — two tasks, two tools, `stream.id` `sess-muse-tokens`.
- [exit](exit) — `0`.
- [runs.tsv](runs.tsv) — lane/stamp → session id join.
- [sessions/](sessions/map.md) — sanitized session store.

## Pointers

- Up: [../map.md](../map.md)
