# Proposed packet consumption by worker wrappers

**Opened:** 2026-09-06. **Class:** campaign. **State:** proposal. This unit
does not edit wrappers.

**Retires:** freeze when a wrapper change lands or E-7 records the decision to
decline it.

The assembler already emits Markdown with the stable prefix first. Each wrapper
already takes a prompt file. The change is to write the packet Markdown to that
file instead of the 4–8 KB prose brief, and to keep the JSON sidecar next to it
for `sepmo_packet.py check`. No wrapper body is patched here.

## Muse Spark

`muse-worker.sh` launches `muse exec --json --prompt-file <run-dir>/prompt.md`.
The file the wrapper already reads is `prompt.md` in the run directory
(`/tmp/muse-worker/<lane>/<utc>/prompt.md`). After an accepted E-4 pilot, the
orchestrator would write the packet Markdown to that `prompt.md` (stable prefix
first) and copy the JSON sidecar to `packet.json` in the same directory for
mechanical `check`, not as model input.

## Grok

`grok-worker.sh` launches `grok --prompt-file <run-dir>/prompt.md`. The file
the wrapper already reads is `prompt.md` in the run directory
(`/tmp/grok-worker/<lane>/<utc>/prompt.md`). The orchestrator would write the
packet Markdown to that same `prompt.md` and place `packet.json` beside it.

## GLM / opencode (kilo)

`oc-worker.sh` launches `opencode run --dir <clone> --agent worker|critic
--format json --auto <prompt>`. The prompt text is the contents of
`<run-dir>/prompt.md` (`/tmp/oc-worker/<lane>/<utc>/prompt.md`). The
orchestrator would fill that `prompt.md` from the packet Markdown. The sidecar
would be `packet.json` in the same run directory.

## Opus / Claude sub-agents

Claude sub-agents have no worker run-dir prompt file (E-0 inventory). The
orchestrator would paste the packet Markdown as the sub-agent prompt text. There
is no `prompt.md` path to name until a run-dir layout exists.

pins: sepmo-e2/C-007
