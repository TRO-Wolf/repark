# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [mw-7-scale-measurement-ledger.md](mw-7-scale-measurement-ledger.md) — **MW-7 (2026-08-24):**
  Iceberg scale measurement, measure-only. 1e7 rows x 50 MERGEs (substituted from the charter's
  100 — the projection arithmetic is §1), merge-on-read against a copy-on-write control. The
  merge-on-read scans reach 4.18x/4.58x the control by merge 50 and cross 2x at ~19 merges;
  the control is flat. **MW-9 is urgent**; MW-8's defaults are §6. Two OPEN findings:
  `rewrite_data_files` removes no delete files (F-MW7-1), position-delete compaction grows the
  delete bytes 31 % (F-MW7-2).
- [mw-6-rewrite-manifests-ledger.md](mw-6-rewrite-manifests-ledger.md) — **MW-6 (2026-08-23):**
  `CALL system.rewrite_manifests` over the fork's `RewriteManifestsAction`; counts read from the
  new snapshot's summary; registry rows `MANIFEST-1` / `MANIFEST-2`.
- [rp-1-fork-repin-ledger.md](rp-1-fork-repin-ledger.md) — **RP-1 (2026-08-23):** re-pin
  `iceberg*` to fork `main` `5e7b2e4` (F-0/F-1/F-2/F-8a; T6 name-directory freeze;
  Spark `position_deletes` rewrite). First row of the post-MW sequence.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
