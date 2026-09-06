# Proposed wrapper patch — packet write point and Muse prefix order

**Target (outside the repo):** `muse-worker.sh`, `grok-worker.sh`,
`oc-worker.sh`. **Do not apply from this unit.**

Packet format v1 requires the stable prefix to be the first bytes of the
worker prompt. Muse currently prepends the persona ahead of `--brief`, so a
packet's prefix is not first. Muse and OpenCode also generate `$run/prompt.md`
from persona + brief + HANDBACK, so a packet written there is overwritten.
Grok already reads `--brief` / `--followup` (`prompt=${followup:-$brief}`) and
only copies that file to `$run/prompt.md` as an archive.

## Change

1. Treat `--brief` / `--followup` as the orchestrator write point on every
   adapter. Do not document `$run/prompt.md` as the input.
2. Muse: keep the packet (stable prefix first) at the start of the generated
   prompt. Append the persona and the HANDBACK block **after** the packet, or
   move the persona into wrapper config so it is not prepended.
3. OpenCode: keep generating `$run/prompt.md` from `--brief` plus HANDBACK;
   do not prepend a persona to that file (persona already lives in
   `config.json`).
4. Grok: no prompt-order change. Keep `grok --prompt-file` pointed at
   `prompt=${followup:-$brief}`.

pins: sepmo-e2/C-007
