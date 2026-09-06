# Proposed packet consumption by worker wrappers

**Opened:** 2026-09-06. **Class:** campaign. **State:** proposal. This unit
does not edit wrappers.

**Retires:** freeze when a wrapper change lands or E-7 records the decision to
decline it.

The assembler already emits Markdown with the stable prefix first. The write
point is the file each wrapper takes as `--brief` (and `--followup` on
resume). `$run/prompt.md` is not that write point for any adapter: Grok copies
it as an archive, and Muse / OpenCode generate it. No wrapper body is patched
here. The Muse persona-prepend change lives under
[../telemetry/wrapper-patches/packet-brief-input.md](../telemetry/wrapper-patches/packet-brief-input.md).

## Muse Spark

`muse-worker.sh` takes `--brief FILE` and `--followup FILE` (resume). That
file is the orchestrator write point. The wrapper then **generates**
`$run/prompt.md` by concatenating the persona, the brief, and a HANDBACK
block, and launches `muse exec --json --prompt-file <run-dir>/prompt.md`. A
packet written to `$run/prompt.md` is overwritten. The persona currently
precedes the brief, so it sits ahead of the stable prefix (contrary to
[packet-format.md](packet-format.md)). After an accepted E-4 pilot, write the
packet Markdown to `--brief` / `--followup` and apply the wrapper patch so
the prefix stays first. Place `packet.json` beside the brief.

## Grok

`grok-worker.sh` takes `--brief FILE` and `--followup FILE`. The prompt it
passes to `grok --prompt-file` is `prompt=${followup:-$brief}`. `$run/prompt.md`
is an archive copy (`cp "$prompt" "$run/prompt.md"`), not the input. Write the
packet Markdown to the `--brief` / `--followup` file and place `packet.json`
beside it.

## GLM / opencode (kilo)

`oc-worker.sh` takes `--brief FILE` and `--followup FILE`. It **generates**
`$run/prompt.md` from the brief plus a HANDBACK block, then launches
`opencode run --dir <clone> --agent worker|critic --format json --auto` with
that generated text. A packet written to `$run/prompt.md` is overwritten. The
persona lives in `config.json` agent prompt, not in the brief file. Write the
packet to `--brief` / `--followup`. The sidecar would be `packet.json` next
to the brief.

## Opus / Claude sub-agents

Claude sub-agents have no worker run-dir prompt file (E-0 inventory). The
orchestrator would paste the packet Markdown as the sub-agent prompt text. There
is no `prompt.md` path to name until a run-dir layout exists.

pins: sepmo-e2/C-007
