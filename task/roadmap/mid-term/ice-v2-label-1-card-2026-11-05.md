# Card ICE-V2-LABEL-1: the format-v2 label text from the corpus (U-13)

**Date:** 2026-11-05. **Filed by:** the records clerk (`claude-haiku-5-5`), from the label brief.

**Status:** re-audit 2026-11-05: LABEL DOES NOT STAND, five blocking findings; AUD-024 ruled a kept divergence; three fix units dispatched; a narrow second re-audit then re-dates the label (ledger C-097 to C-115; [report](../../../docs/audits/iceberg-v2-label-corpus-audit-2026-11-05.md)). The label text stays PROPOSED for PRODUCTION (2026-11-05) and is not re-dated; the maturity stays WITHDRAWN 2026-10-29 (ledger C-072). The fix cards are [DQ-IDENTIFIERS-CONFIG-REFUSE-1](dq-identifiers-config-refuse-1-card-2026-11-05.md), [ANSI-SUBSCRIPT-AND-AMBIGUOUS-FIELD-1](ansi-subscript-and-ambiguous-field-1-card-2026-11-05.md) and [CTAS-QUERY-FAILURE-CLEANUP-1](ctas-query-failure-cleanup-1-card-2026-11-05.md); the kept divergence is [NAN-UNDER-ICEBERG-SCAN-FILTERS-1](nan-under-iceberg-scan-filters-1-card-2026-11-05.md).

**Why:** The label section of the parity registry ([format-v2 label](../../../docs/spark-sql-iceberg-parity.md#format-v2-label-supported-behaviour-2026-11-05)) states the per-family claims, the evidence and the exclusions from the corpus. Its status is PROPOSED for PRODUCTION, pending the audited sign-off. The label was WITHDRAWN on 2026-10-29 (ledger [ice-v2-label-1-ledger.md](../../ledgers/staging/ice-v2-label-1-ledger.md) C-072).

**Open before the re-audit (answered by the re-audit of 2026-11-05):** C-092 is closed for the two fixes and C-085 and C-090 are closed by the owner's rulings; the five blocking findings are the open items. Before the re-audit, the open list read: the re-audit of the AUD-015 and AUD-016 fixes (ledger C-092); four CREATE TABLE names cells outside the 2026-11-03 ruling and the nested expression answers D10 and D13 (C-090). The map-key DROP arm is a fork-side lead only (C-091, superseded by C-095).

**Close:** when the re-audit passes and the owner signs on the label PR, the ledger moves to `completed/` in the same PR.
