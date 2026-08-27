# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [pr-245-revalidation-ledger.md](pr-245-revalidation-ledger.md) — **PR #245 revalidation
  (2026-08-26):** revalidate SQP-1 against current `main`, including exact source-size ratchets,
  separate regression pins, and the final Actor–Critic evidence.
- [sqp-1-spark-string-literals-ledger.md](sqp-1-spark-string-literals-ledger.md) — **SQP-1
  (2026-08-25):** Spark string-literal escapes on the Spark SQL door and Spark-compatible
  `CAST` and `TRY_CAST AS BINARY` behavior.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
