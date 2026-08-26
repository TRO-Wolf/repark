# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [reg-1-registry-truth-up-ledger.md](reg-1-registry-truth-up-ledger.md) — **REG-1 (2026-08-26):**
  the docs-only truth-up of the divergence registry and STATUS — DEC-2 / DEC-6 / DEC-7 / DEC-8
  become dated FIXED notes (#94 / #99), TZ-8 splits its FIXED `CAST(ts AS DATE)` / `to_date` /
  `datediff` half (#100) from the `last_day` / `date_add` / `date_sub` residual, and G3-E8 states
  the delivered spellings and the true remainder. Six clauses, each pinned by
  [../../../python/repark-parity/tests/test_reg_1_registry_truth_up.py](../../../python/repark-parity/tests/test_reg_1_registry_truth_up.py).
- [sqp-1-spark-string-literals-ledger.md](sqp-1-spark-string-literals-ledger.md) — **SQP-1
  (2026-08-25):** Spark string-literal escapes on the SQL door + `CAST … AS BINARY`. Twelve
  clauses PROVEN and pinned; Actor done, awaiting the Critic (attestation deferred via the
  grammar gate's EXCEPTIONS).
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
