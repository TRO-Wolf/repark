# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [v3e-5-nightly-v3-oracle-ledger.md](v3e-5-nightly-v3-oracle-ledger.md) — **V3E-5 (2026-08-27):** the nightly v3 live-oracle leg — `REPARK_PARITY_LIVE=1` repark == Spark on the V3E-3 fixtures (`v3-spark-part-dv` / `v3-spark-eq-dv`), dual-wired `parity-live` leg green, northstar nightly row dated. Scoped `.github/` grant only.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
