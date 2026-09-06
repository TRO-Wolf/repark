# Proposed OpenCode wrapper patch — token sums on the TSV row

**Target (outside the repo):** `oc-worker.sh` in the oc-worker skill.
**Do not apply from this unit.**

The wrapper already counts `step-finish` events and sums their `cost` into
`runs.tsv`. The same events carry `tokens.input` / `output` / `reasoning` /
`cache.read`. Those sums are not in the TSV today.

## Change

In the existing Python footer that reads `out.ndjson`, also sum:

- `tokens.input` → `tokens_in`
- `tokens.output` → `tokens_out`
- `tokens.cache.read` → `tokens_cached`
- `tokens.reasoning` → `tokens_reasoning` (keep separate; do not add to output)

Append those four integers to the `runs.tsv` row. Write `usage.json` with the
same numbers plus `cost` and `steps`. Do not open the OpenCode sqlite from the
wrapper (it holds workspace paths).

pins: sepmo-e0-e1/C-001
