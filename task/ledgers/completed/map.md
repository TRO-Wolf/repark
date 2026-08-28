# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [rp-2-fork-repin-ledger.md](rp-2-fork-repin-ledger.md) — **RP-2 (2026-08-27), drafted for the
  owner's charter:** the second fork repin, `5e7b2e4` → fork `main` `ce92a7b`, which takes the
  fork's landed F-13 (deletion-vector write path), F-7 U1+U2 (lineage through rewrites) and F-3
  (`remove-dangling-deletes`). Eight clauses, all OPEN: the pin move, the two standing repin
  duties, three measure-first clauses that decide whether the `R113` v3 arm, `V3-LINEAGE-1` and
  `V3-COW-1` guards lift, F-3 taken, the documents trued up, the gates. Not in it: V3-3's new
  surfaces, F-14/F-15/F-16, any DataFusion family move.
- [v3e-5-nightly-v3-oracle-ledger.md](v3e-5-nightly-v3-oracle-ledger.md) — **V3E-5 (2026-08-27):** the nightly v3 live-oracle leg — `REPARK_PARITY_LIVE=1` repark == Spark on the V3E-3 fixtures (`v3-spark-part-dv` / `v3-spark-eq-dv`), dual-wired `parity-live` leg green, northstar nightly row dated. Scoped `.github/` grant only.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
