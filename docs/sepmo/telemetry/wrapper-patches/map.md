# map — docs/sepmo/telemetry/wrapper-patches/

## Purpose

Proposed edits to the out-of-repo worker wrappers. This unit must not edit
`$HOME/.claude/` or `$HOME/.Codex/`. The orchestrator applies a patch from here
if it accepts it.

This directory closes with the parent telemetry campaign.

## Contents

- [muse-usage-sidecar.md](muse-usage-sidecar.md) — wall-time sidecar only.
  Tokens already live in the session store; cost is still absent.
- [grok-usage-fields.md](grok-usage-fields.md) — persist the live `usage` keys
  (`cache_read_input_tokens`, `cache_creation_input_tokens`, `modelUsage`).
- [oc-usage-tokens.md](oc-usage-tokens.md) — add token sums from `step-finish`
  events to the OpenCode `runs.tsv` row.

## I want to...

| ...do this | go to |
|---|---|
| Apply a Muse wrapper change | [muse-usage-sidecar.md](muse-usage-sidecar.md) |
| Apply a Grok wrapper change | [grok-usage-fields.md](grok-usage-fields.md) |
| Apply an OpenCode wrapper change | [oc-usage-tokens.md](oc-usage-tokens.md) |

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| A patch mentions a home path | Reject it — patches here must stay path-generic |
