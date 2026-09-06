# Proposed Grok wrapper patch — persist usage keys

**Target (outside the repo):** `grok-worker.sh` in the grok-worker skill.
**Do not apply from this unit.**

The wrapper already appends `sessionId`, `stopReason`, `num_turns`, and
`total_cost_usd` to `runs.tsv`. A completed `out.json` was not on disk at
inventory time, so token keys are unconfirmed. If the CLI adds a `usage`
object, persist it.

## Change

After `out.json` is written, copy these keys when present into `usage.json`:

- `sessionId`, `stopReason`, `num_turns`, `total_cost_usd`
- `usage.input_tokens` / `output_tokens` / `cache_read_tokens` / `reasoning_tokens`
  (or the names the live object actually uses)
- `num_tool_calls` if present

Do not write a token key as `0` when the object omitted it. Write `exit` even
when `out.json` is empty so wall time has a finish mark.

pins: sepmo-e0-e1/C-001
