# Packet size versus today's briefs (E-2 baseline)

**Opened:** 2026-09-06. **Class:** campaign. **State:** live. Sizes measured
from the three converted fixtures in
`python/repark-parity/tests/fixtures/sepmo_packets/`. Token columns are the
E-0 inventory numbers for the matching Muse actor runs, not a packet-fed
re-run.

**Retires:** freeze when E-4 records a packet-fed versus brief-fed measurement,
or when E-7 closes the pilot.

E-2 does **not** claim token savings. Document bytes are not billed tokens.
The E-0 collector already shows cached input dominating uncached input on
every adapter that reports both. A stable prefix is the layout that prompt
caching needs. Whether that layout changes billed tokens is an E-4
measurement.

## Converted briefs

Sanitized copies of the campaign briefs for EX-25, PERF-FACADE-CDF-1, and
PERF-ICE-SCAN-1. Word counts use whitespace split (`str.split`), matching
`wc -w`. Byte counts are UTF-8.

| Brief | Original bytes | Original words | Prefix bytes | Prefix words | Dynamic bytes | Dynamic words | Packet bytes | Packet words |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ex25 | 5774 | 598 | 1505 | 232 | 5426 | 538 | 6942 | 772 |
| cdf1 | 8004 | 952 | 1505 | 232 | 6291 | 695 | 7807 | 929 |
| icescan | 8864 | 1155 | 1505 | 232 | 4932 | 539 | 6448 | 773 |

The prefix is byte-identical across the three (1505 bytes, 232 words). The
packet total is not always smaller than the brief: ex25 grows 5774 → 6942
bytes because the structured dynamic section still carries the deliverable
and the prefix is added. icescan shrinks 8864 → 6448 bytes because the
repeated rules block is not copied into the dynamic section. Size reduction
is not the claim.

## E-0 cached / uncached input (Muse actor runs)

From [../telemetry/inventory.md](../telemetry/inventory.md) §3. `tokens_in` is
uncached input. `tokens_cached` is cached input and is not included in
`tokens_in`. Ratio is `tokens_cached / tokens_in`.

| Lane | Stamp | tokens_in | tokens_cached | cached / uncached | tokens_out |
|---|---|---:|---:|---:|---:|
| ex25 | 20260905T214117Z | 269827 | 24171581 | 89.6 | 95642 |
| cdf1 | 20260905T113405Z | 689231 | 75010034 | 108.8 | 194648 |
| icescan | 20260905T183353Z | 1106400 | 122053590 | 110.3 | 302942 |

Grok on this machine (inventory §2, unit
`/tmp/grok-worker/sepmoe0/20260906T010334Z`): `tokens_in=275354`,
`tokens_cached=10007936` (36.3× uncached; 97% of `total_tokens=10352603`).
Cache reads dominate that adapter too.

What E-2 can claim: the three packets share one 1505-byte prefix; `check`
detects a dropped stable rule; `diff` shows only the dynamic section. What
E-2 cannot claim: a change in billed tokens, cache-hit rate, or elapsed
time. Those need a packet-fed run measured by `scripts/sepmo_usage.py`.

pins: sepmo-e2/C-006
