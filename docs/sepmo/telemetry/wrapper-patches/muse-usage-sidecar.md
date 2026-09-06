# Proposed Muse wrapper patch — usage sidecar

**Target (outside the repo):** `muse-worker.sh` in the muse-worker skill.
**Do not apply from this unit.**

The 2026-09-05/06 `out.jsonl` files carry no model token or cost fields. A
wrapper cannot invent them. It can still persist the fields it already computes
so a later collector does not have to re-parse a multi-megabyte JSONL for wall
time, tools, session id, and terminal.

## Change

1. At run-dir creation, write `started` as a UTC ISO timestamp (one line).
2. After the process exits, write `usage.json` next to `exit`:

```json
{
  "adapter": "muse",
  "model": "<--model>",
  "effort": "<--reasoning-effort>",
  "session": "<stream.id>",
  "tool_calls": 0,
  "terminal": "",
  "exit": 0,
  "started_utc": "",
  "finished_utc": "",
  "wall_s": 0
}
```

3. Keep `runs.tsv` as it is. Add `wall_s` as a last column only if existing
   readers are updated in the same change.

`tokens_in` / `tokens_out` / `cost_usd` stay absent until Muse emits them.
Do not write zeros for those keys.

pins: sepmo-e0-e1/C-001
