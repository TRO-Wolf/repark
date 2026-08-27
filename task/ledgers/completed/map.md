# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [dl-5-contract-compaction-ledger.md](dl-5-contract-compaction-ledger.md) — **DL-5
  (2026-08-25):** compact the live STATUS remainder and the contributor contract;
  `engineering-method` loses restated project rules; `check_docs_compaction` ceilings
  extend to `AGENTS.md` and the method skill. Host-injection measurement and
  `.agents/roles/` are out of scope.
- [proc-1-tiered-review-ledger.md](proc-1-tiered-review-ledger.md) — **PROC-1 (2026-08-25):**
  review effort by tier (the `review_profile` tunable, `light_thresholds` re-bind, `critic_engine`
  amended to bind CCC at HIGH), the new pointer-only `unit-runbook.md`, the MW-6 Critic-evidence
  home, and two runbook truth-ups (disk headroom, the iceberg-rust F-7 handoff). Eleven clauses,
  all pinned by `test_proc_1_tiered_review.py`; the Critic's attestation is pending.
- [cap-1-source-file-line-cap-ledger.md](cap-1-source-file-line-cap-ledger.md) — **CAP-1
  (2026-08-26):** ratchet Rust and Python source files to the new source-line default with exact,
  no-slack baselines for the measured offenders; keep narrow line-neutral fixes legal and preserve
  the facade no-stub rule.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
