# Card ICE-HADOOP-HINT-WINDOW-1: on `type=hadoop`, RePark's path read stops at `version-hint.text` while a newer metadata file is the commit

**Date:** 2026-10-22. **Filed by:** Claude (claude-opus-5-5) in fix round 1 of ICE-INTEGRITY-PINS-1
(PR #1142), from the §7 halt of that unit's verifier round 1.

**Status:** closed 2026-10-29 by PR #1205 (`9ab67d7d`). Dispatched 2026-10-28 on an Opus 5.5 builder lane; Opus 5.5 verifier PASS and Muse MAX critic PASS on the code at `564285cb`. Owner ruled 2026-10-22: option 1, the product change. The path read walks past
the hint as Java's `HadoopTableOperations.refresh` does, in its own Opus unit after ZONE-SEAM-1;
ruling 83 stands as written. This is existing product behaviour, not code from PR #1142. The independent audit of 2026-10-28 measured the same window as its AUD-001 (the second measurement): after a committed `DELETE`, the path read returns the deleted row, ids 1, 2, 3 where the catalog answers 2, 3 ([the audit report](../../../docs/audits/iceberg-v2-label-independent-audit-2026-10-28.md), section 3). The v2 PRODUCTION label is withdrawn pending this unit and re-dates on its merge. Pin K runs on uuid naming, so it never takes this path. The registry row
[ICE-LOCAL-COMMIT-POINT-1](../../../docs/spark-sql-iceberg-parity.md) is narrowed to
`register_table` until that unit lands.

**Retires:** when the unit that carries option 1 lands and the registry row widens again.

## What was measured

On `type=hadoop` (the memory catalog with Hadoop file names) the fork commits in two steps:

1. It writes `v(N+1).metadata.json`: fork `d80a2884`, `crates/iceberg/src/catalog/memory/catalog.rs:584`.
2. It then advances `version-hint.text`, best effort, at `:598-600`.

RePark's path read, `crates/repark-core/src/iceberg_path.rs:99-108`, returns the version the
hint names and never looks past it. Ruling 83 says the newest metadata file is the commit on the
local catalog. In the window between the two steps, the reopen door decides whether a crashed
commit counts as committed.

**Setup** (the verifier's probe, uncommitted): a `type=hadoop` table with v1 (create), v2 (insert
1) and v3 (insert 2), then `version-hint.text` reset to `2`. That is the on-disk state a kill
between `:584` and `:600` leaves. The probe measured RePark through the facade and Spark live
(PySpark with iceberg-spark-runtime-4.1_2.13-1.11.0). The table below is verbatim from the
verifier's hand-back:

| Reopen door | Rows read |
|---|---|
| RePark path read `format("iceberg").load(<location>)` | `[1]`: not committed |
| RePark `register_table` on the newest file (v3) | `[1, 2]`: committed |
| Spark `HadoopCatalog` `SELECT` | `[1, 2]` |
| Spark path load | `[1, 2]` |
| Control (hint intact): every RePark door | `[1, 2]` |

**A crash is not needed.** A failed hint write leaves the same state: `advance_version_hint` only
logs a warning, so a live process can commit `v(N+1)` and keep the hint at `N`.

## The owner's two options

1. **Product change:** the path read walks past the hint, as Java's `HadoopTableOperations.refresh`
   does.
2. **Narrow ruling 83:** ruling 83, and registry row ICE-LOCAL-COMMIT-POINT-1, cover
   `register_table` on the newest metadata file only.

This card named no fix when it was filed; the owner picked option 1 on 2026-10-22, in the morning
read of the overnight watch, with the orchestrator's and the verifier's lean.
