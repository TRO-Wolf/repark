# Proposed Muse wrapper patch — wall-time sidecar (cost still absent)

**Target (outside the repo):** `muse-worker.sh` in the muse-worker skill.
**Do not apply from this unit.**

Round-1 premise was false: Muse **does** report tokens, outside the run dir.
`$HOME/.local/share/muse/sessions/<yyyy>/<mm>/<dd>/<session-id>/session.jsonl`
carries per-turn usage, and `.msp-view-v1/<session-id>/snapshot-*.json`
carries `tokenUsage.cumulative`. The session id is already `runs.tsv`
column 6. The collector joins those files read-only.

Cost is genuinely absent. This patch does **not** invent token fields and
does **not** write zeros for `cost_usd`.

## Change

1. At run-dir creation, write `started` as a UTC ISO timestamp (one line).
2. After the process exits, write `usage.json` next to `exit` with wall time,
   session id, tool-call count, terminal, and exit. Omit token and cost keys
   (the collector already reads tokens from the session store).
3. Keep `runs.tsv` as it is. Add `wall_s` as a last column only if existing
   readers are updated in the same change.

Do not copy session-store token objects into the run dir. The join is enough.

pins: sepmo-e0-e1/C-001
