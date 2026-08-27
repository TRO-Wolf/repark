# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [cap-1-source-file-line-cap-ledger.md](cap-1-source-file-line-cap-ledger.md) — **CAP-1
  (2026-08-26):** ratchet Rust and Python source files to the new source-line default with exact,
  no-slack baselines for the measured offenders; keep narrow line-neutral fixes legal and preserve
  the facade no-stub rule.
- [pr-245-revalidation-ledger.md](pr-245-revalidation-ledger.md) — **PR-245 revalidation
  (2026-08-26):** remeasure SQP-1 after current-main integration, preserve both SQL-door and facade
  controls, preserve original parser locations after canonicalization, pin the enumerable Python
  helper guard and inventory, and clear exact source-size ratchets. Five Actor–Critic remediation
  cycles converged on 2026-08-27; the completed SQP-1 ledger remains frozen.
- [sqp-1-spark-string-literals-ledger.md](sqp-1-spark-string-literals-ledger.md) — **SQP-1
  (2026-08-25):** Spark string-literal escapes on the SQL door + `CAST … AS BINARY`. Twelve
  clauses PROVEN and pinned; the completed ledger carries the filed Critic attestation.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
