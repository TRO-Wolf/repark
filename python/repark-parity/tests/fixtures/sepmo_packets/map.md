# map — python/repark-parity/tests/fixtures/sepmo_packets/

## Purpose

Sanitized campaign briefs and the v1 packets assembled from them. No home
directory paths. Used by `test_sepmo_packet.py`.

## Contents

- `brief-ex25.md` — sanitized EX-25 campaign brief.
- `brief-cdf1.md` — sanitized PERF-FACADE-CDF-1 campaign brief.
- `brief-icescan.md` — sanitized PERF-ICE-SCAN-1 campaign brief.
- `ex25-actor.md` / `ex25-actor.json` — actor packet for EX-25.
- `cdf1-actor.md` / `cdf1-actor.json` — actor packet for PERF-FACADE-CDF-1.
- `icescan-actor.md` / `icescan-actor.json` — actor packet for PERF-ICE-SCAN-1.

pins: sepmo-e2/C-005

## Pointers

- Up: [../map.md](../map.md)
- Assembler: [../../../../../scripts/sepmo_packet.py](../../../../../scripts/sepmo_packet.py)

## Debug

| Symptom | First check |
|---|---|
| Fixture contains `/home/` | Rebuild with `sepmo_packet.py build`; the sanitizer must rewrite it to `$HOME` |
| Prefix differs across the three `.md` packets | Assembler defect; prefixes must be byte-identical |
