# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [plan-1-northstar-fnp-sequence-ledger.md](plan-1-northstar-fnp-sequence-ledger.md) — **PLAN-1
  (2026-08-28):** truth up the v1.0 critical path, add fork F-17 shared-Puffin closure, narrow
  RP-2 to its safe guarded increment, and align FNP on per-unit delivery and one remaining order.
  Six clauses PROVEN; plan-contract, lifecycle, verify, and preflight gates green.
- [rp-2-fork-repin-ledger.md](rp-2-fork-repin-ledger.md) — **RP-2 (2026-08-27; narrowed and
  salvaged 2026-08-28):** the fork repin `5e7b2e4` → `ce92a7bf` (F-13, F-7 U1+U2, F-3). A first
  plain-`WHERE` DELETE on a DV-free v3 table runs on both modes Spark-clean; any table carrying a
  live DV refuses before a write (second engine DELETE and the Spark shared-Puffin fixture
  pinned per door); UPDATE / MERGE refuse; `rewrite_data_files` re-measured RED (guard stays);
  F-3's true `removed_delete_files_count` taken. Finding F-rp2-1 (shared-Puffin sibling loss)
  became fork F-17. Two Critic cycles; the #254 clauses moved to RP-3.
- [v3e-5-nightly-v3-oracle-ledger.md](v3e-5-nightly-v3-oracle-ledger.md) — **V3E-5 (2026-08-27):** the nightly v3 live-oracle leg — `REPARK_PARITY_LIVE=1` repark == Spark on the V3E-3 fixtures (`v3-spark-part-dv` / `v3-spark-eq-dv`), dual-wired `parity-live` leg green, northstar nightly row dated. Scoped `.github/` grant only.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
