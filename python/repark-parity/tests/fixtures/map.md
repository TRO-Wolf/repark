# map — python/repark-parity/tests/fixtures/

## Purpose

Checked-in test fixtures for the parity harness. Generated goldens stay under
`tests/goldens/` when those exist. This directory holds small, sanitized
inputs. No home-directory paths, no secrets.

## Contents

- [sepmo_usage/](sepmo_usage/map.md) — sanitized worker run directories for
  `test_sepmo_usage.py`.
  pins: sepmo-e0-e1/C-007
- [sepmo_packets/](sepmo_packets/map.md) — sanitized campaign briefs and v1
  packets for `test_sepmo_packet.py`.
  pins: sepmo-e2/C-005

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| A fixture contains `$HOME` or `/home/` | Strip it; collector tests fail closed on secrets in fixtures |
