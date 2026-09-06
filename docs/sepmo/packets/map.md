# map — docs/sepmo/packets/

## Purpose

SEPMO compact worker packet format v1: the field contract, the JSON schema,
the baseline comparison against E-0 usage, and the wrapper-consumption
proposal. Assembler: [../../../scripts/sepmo_packet.py](../../../scripts/sepmo_packet.py).

This directory is campaign class. It closes when the efficiency pilot's E-7
outcome is recorded.

## Contents

- [packet-format.md](packet-format.md) — packet format v1: eight field groups,
  stable prefix versus dynamic section, source-identity and refresh, assembler
  commands, trailer/re-render/`bash -n` checks, sidecar `authority.constraints`
  equality with `STABLE_RULES`, and the dynamic-versus-prefix phrase-scan limit.
  pins: sepmo-e2/C-001, C-002, C-003
- [packet.schema.json](packet.schema.json) — machine schema for the JSON
  sidecar. `additionalProperties` is false. `packet_version` is `"1"`.
  pins: sepmo-e2/C-001
- [baseline.md](baseline.md) — three converted briefs versus prefix and
  dynamic size, plus E-0 cached/uncached ratios. No token-savings claim.
  pins: sepmo-e2/C-006
- [adoption.md](adoption.md) — how each adapter would read a packet. Names
  `--brief` / `--followup` as the write point; `$run/prompt.md` is generated
  or an archive copy. No wrapper edits.
  pins: sepmo-e2/C-007

## I want to...

| ...do this | go to |
|---|---|
| See the eight field groups and the prefix split | [packet-format.md](packet-format.md) |
| Validate a JSON sidecar | [packet.schema.json](packet.schema.json) |
| Compare packet size to today's briefs | [baseline.md](baseline.md) |
| See which wrapper file would consume a packet | [adoption.md](adoption.md) |
| Build or check a packet | [../../../scripts/sepmo_packet.py](../../../scripts/sepmo_packet.py) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../telemetry/inventory.md](../telemetry/inventory.md)

## Debug

| Symptom | First check |
|---|---|
| `check` says the prefix is not byte-identical | The markdown must start with the assembler `STABLE_PREFIX` |
| `check` reports a missing stable rule | A mutation dropped a constraint; restore the prefix |
| `check` reports `authority.constraints` mismatch | The JSON list must equal `STABLE_RULES` in order and text |
| `check` reports a trailer finding | The rendered trailer must equal the adapter `AUTHORED_BY` entry |
| `check` reports a re-render mismatch | Sidecar fields and markdown dynamic section disagree |
| Packet contains `/home/` | Sanitizer missed a path; `build` must rewrite it to `$HOME` |
| Token savings claimed from this directory | Invalid — [baseline.md](baseline.md) forbids it until E-4 |
