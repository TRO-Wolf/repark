# STATUS.md "Current milestone": the sequence after milestone one

> **ARCHIVED 2026-11-09** — the text below stood in [STATUS.md](../../../STATUS.md) under
> "Current milestone" until that date and is kept for provenance, **not as a source of live
> rules or state**. It was moved word for word; relative links were repaired for this location and
> nothing else changed. The standing decision (the v1 predecessor is bugfix-only) and the API-freeze
> sentence stay in STATUS.md. Current state: [STATUS.md](../../../STATUS.md).

**Milestone one is COMPLETE** and there is no in-flight *port* work; its record is under
[Delivered capabilities](../../../STATUS.md#delivered-capabilities) above.

**Standing decision: the private v1 predecessor is bugfix-only, and this repository is the sole
forward target.** New engine work happens here; a defect both engines share is fixed there and
re-ported rather than patched only here.

What happens next, in order:

1. **Agent-Agnostic Front-Door** — **DONE (2026-08-10):** [docs/history/frontdoor/](../frontdoor/README.md), [task/metrics.md](../../../task/metrics.md).
2. **V2 Engine Hardening** — the active engine campaign; its state is the H-2 entry in [Active workstreams](../../../STATUS.md#active-workstreams). The Y/Z/W/V/S wave records are indexed in [task/ledgers/archive/2026-08/map.md](../../../task/ledgers/archive/2026-08/map.md).
3. **Production-pipeline cutover inventory** — **Filed 2026-09-04:** [docs/cutover/inventory.md](../../cutover/inventory.md), under **single-writer-per-table**, with each rollback story; its §7 carries the four owner rulings of the same day.
4. **The first tagged release** — **DONE**: see [Release state](../../../STATUS.md#release-state). The API freeze is pinned at 888 names in [docs/design/v1-0-api-freeze.json](../../design/v1-0-api-freeze.json) (policy: [docs/release.md](../../release.md) "Versioning policy"); v1.0.0 was cut on the north-star gate line V1-GATE wrote the same day.

Owner-side actions that rode this sequence are **DISCHARGED — no owner-side tier-2 action
remains** (aws-acceptance green 2026-08-10; the parity-live half on first-run evidence; three
stale always-PASS Apache smoke pins are known-FAIL meta pins). Pre-scrub content stays reachable
in [p3e-facade-ledger.md](p3e-facade-ledger.md) by explicit decision.
