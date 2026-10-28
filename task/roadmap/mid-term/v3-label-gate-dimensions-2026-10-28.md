# Format-v3 label gate dimensions: the input to the v3 label order (2026-10-28)

**Filed:** 2026-10-28. **Filed by:** the audit clerk round (`claude-haiku-5-5`), copied from section 5 of the independent audit ([iceberg-v2-label-independent-audit-2026-10-28.md](../../../docs/audits/iceberg-v2-label-independent-audit-2026-10-28.md)).

**Status:** filed. It is the gate input to the format-v3 label order (V3-LABEL-1), which is not written yet; the label's date of record is 2026-11-17. Nothing here is dispatched or scheduled.

## What this is

Section 5 of the audit names fifteen dimensions that the format-v2 gate did not establish, and the evidence each one requires for the next label. Each row below is the audit's own text, unchanged. A dimension is satisfied only by a row whose actual result and exact revision are recorded, not by a checked assertion or a green upstream run. The v2 label's 842-cell statement matrix (705 EQUAL, 132 SPARK-CANNOT, 5 REFUSED-REGISTERED, 0 DIFFERENT, as the audit read it from the signed records) is statement-level evidence and does not establish these products of dimensions.

## The fifteen dimensions

| # | Dimension | Required discriminating evidence |
|---|---|---|
| 1 | Public routing | Equivalent predicates (`BETWEEN`, comparisons, `IN`, subquery), native SQL, Spark SQL and writer APIs; verify which writer actually executes. |
| 2 | File format composition | Parquet/Avro/ORC data **and delete** combinations; read exact surviving values after DML and reopen, not just suffix/magic. |
| 3 | Evolution history | Equality deletes followed by drop/rename/promotion/re-add with a new id; partition evolution; V2→V3 upgrade with old deletes retained. |
| 4 | Volume boundaries | Row count, total UTF-8 path bytes, maximum value width, data/delete/manifest count, manifests per snapshot and metadata history length. Cross the real boundary, not only a smaller configured row limit. |
| 5 | Resource ownership | Pool bytes versus total RSS, disk, open handles, live encoders and retained Arrow buffers; multiplied partitions and simultaneous statements. |
| 6 | Commit outcome | Definite failure, proved success and unresolved outcome; also failure **after** catalog publication but before returning to the caller. |
| 7 | Failure schedule | Data writes, writer close, manifest/list/metadata creation, publication, hint write, provider refresh and reopen; truncated files and failed reads as distinct cases. |
| 8 | Concurrency | Two writers, maintenance under a writer, readers across commits, schema/partition changes while a write is planned; exact outcomes and no lost acknowledged rows. |
| 9 | Reader authority | Catalog pointer, Hadoop current-version walk, explicit historical metadata, cached plans and fresh sessions; no accidental mixing of those contracts. |
| 10 | Retention and recovery | Reachability of every retained snapshot/ref after failure and cleanup; no deletion of possibly committed files; bounded or operationally managed orphan growth. |
| 11 | Independent oracle | Required row/schema comparisons must execute. Infrastructure failures cannot become an allowed unsupported-feature result. |
| 12 | Exception boundaries | For each carve-out, a minimal trigger, affected entry points and proof that neighboring supported cases remain safe. Silent wrong results cannot be excused under a definition that forbids them. |
| 13 | Refusal guidance | Error class and actionable hint; no blind destructive remediation; assertions on persisted state after a refusal. |
| 14 | Runtime/platform/artifact | Packaged-wheel behavior, supported platforms and memory detection, cross-release reads/writes, and fault behavior after a real process restart. |
| 15 | Evidence freshness | Bind suite selection, dependency resolution and results to the exact candidate revision; distinguish weekly interop, nightly product acceptance and manual-only scale runs. |

## Use

The v3 label order cites each dimension by its number. A dimension with no evidence row at the label's main sha stays OPEN on the label, and a silent wrong value in any dimension disqualifies the label under the production definition, as it did for AUD-001. The resource scale workflow is `workflow_dispatch` only at the audited revision; it is not a scheduled regression guarantee and cannot satisfy the volume dimension by itself.

Up: [map.md](map.md).
