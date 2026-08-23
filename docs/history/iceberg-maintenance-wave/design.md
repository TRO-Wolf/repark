# Design — the Iceberg write-path maintenance wave (MW)

> **ARCHIVED 2026-08-23 (MW-5).** Present state:
> [STATUS.md](../../../STATUS.md) Iceberg maintenance wave. Dated correction:
> §5's "MW-5 lands them" (expire-funnel / omitted-column registry rows) did
> not happen — MW-1 and MW-2 closed those as Spark columns, not registry rows.
> Remaining rows: MOR-1, MOR-2, ORPHAN-1, ORPHAN-2, B-MOR-3.

**Chartered:** 2026-08-21 · **Base:** `1a36b72` (`main`, post-#194) ·
**Charter ledger:** [../../../task/ledgers/completed/mw-0-charter-ledger.md](../../../task/ledgers/completed/mw-0-charter-ledger.md) ·
**Slate:** [slate.md](slate.md)

## 1. The problem, stated as narrowly as it actually is

Merge-on-read (MOR) writes are correct. Three independent evaluations of the MERGE executor found
no correctness defect, and the tier-2 live-AWS acceptance already runs a real upsert loop —
bronze read → dedup → CTAS-if-fresh / MERGE-if-exists → a second identical MERGE asserted
idempotent — against both remote catalogs.

What is missing is not a write capability. It is the **operational half**: every MOR merge adds
position-delete files, read amplification grows monotonically until compaction runs, and every
`CALL system.*` maintenance procedure refuses on exactly the catalogs where production data
lives. The abort path compounds it: `CommitStateUnknown` deliberately leaves files behind and
defers reclamation to orphan maintenance — which is not wired at all.

So MOR on a production catalog today means either unbounded scan degradation, or running a JVM
engine alongside purely for maintenance, which forfeits the no-JVM thesis for exactly this
workload.

**This is a scope problem, not a quality problem** — and smaller than it looks, for the reason
in §2.

## 2. Why the campaign is small: the fence is policy, not capability

Two facts, both verified on this tree rather than assumed:

1. **The fork already ships every action.** At the current pin, `crates/iceberg/src/maintenance/`
   carries `delete_orphan_files`, `rewrite_position_delete_files`, `remove_dangling_delete_files`,
   `delete_reachable_files`, `convert_equality_delete_files`, `rewrite_manifests`,
   `rewrite_table_path`, the stats actions, and an actions provider. **None of them is
   local-only.**
2. **The engine's refusal carries no local-path assumption either.** `refuse_non_local_catalog`
   inspects a `LocationPolicy` and returns an error; the execute paths below it take a
   `&dyn Catalog` and reach object storage through the same FileIO everything else uses. Nothing
   downstream of the fence needs a local filesystem.

The fence was a deliberate blast-radius decision for v1, taken when the surface was new. Lifting
it is a policy change plus tests. **No fork work is on the critical path.**

## 3. The baseline this campaign has to move

Measured on `1a36b72` against the built wheel: a format-v2 table at
`write.merge.mode = 'merge-on-read'`, 1,000 rows, then ten sequential MERGEs each touching the
same 200 ids.

| MERGE | rows | delete files | `COUNT(*)` scan |
|---:|---:|---:|---:|
| 1 | 1000 | 1 | 153.6 ms *(cold)* |
| 2 | 1000 | 2 | 60.1 ms |
| 3 | 1000 | 3 | 78.7 ms |
| 4 | 1000 | 4 | 80.9 ms |
| 5 | 1000 | 5 | 94.6 ms |
| 6 | 1000 | 6 | 108.1 ms |
| 7 | 1000 | 7 | 125.3 ms |
| 8 | 1000 | 8 | 123.9 ms |
| 9 | 1000 | 9 | 124.8 ms |
| 10 | 1000 | 10 | 127.9 ms |

Read it as three separate facts:

- **Delete files grow one per merge, strictly, and are never reclaimed.** This is the campaign's
  whole thesis in one column.
- **Scan cost tracks that growth on a table whose contents never change.** Merge 1 is a cold-start
  outlier (first scan of the session); the honest figure is merge 2 → 10, **60.1 ms → 127.9 ms, a
  2.1× degradation while the answer stays 1,000 rows**.
- **Correctness holds throughout.** Every scan returns exactly 1,000 rows. That is the
  "write path is production-grade" half, measured rather than asserted.

MW-5 re-runs this identical demo and records the delta.

## 4. The units

| Unit | Scope |
|---|---|
| **MW-0** | This design, the slate, the charter, the measured floor above, and the procedure-result schemas in §5. No product change. |
| **MW-1** | Lift the maintenance fence on `expire_snapshots` / `rewrite_data_files` / `rollback_to_snapshot` for **both** remote catalog policies. Refusal-preservation pins: unknown procedures still refuse, error text pinned. |
| **MW-2** | Wire `CALL system.rewrite_position_delete_files` — the one procedure that exists specifically to serve MOR. Scope floor matches `rewrite_data_files` austerity (no filter, no sort); deferrals documented loud. |
| **MW-3** | Wire `CALL system.remove_orphan_files`. Dry-run default, an `older_than` floor, and the resulting divergence from Spark declared as a registry row. Trues up the stale comment, the supported-procedure list, the crate maps, and the guide. |
| **MW-4** | The MOR leg in the tier-2 AWS acceptance. Gated on the role gaining scoped delete. |
| **MW-5** | Registry rows, STATUS scorecard, guide and map lockstep, the re-measured baseline delta, campaign close. |

## 5. Procedure result schemas, measured

Every `CALL` result schema a pin will assert, measured before any of it is written down.

**The oracle runs.** An Iceberg Spark-4.0/Scala-2.13 runtime (1.10.0) loads into the pinned
PySpark 4.1.2 oracle and executes procedures against a Hadoop catalog. Two of the four
procedures execute cleanly. `expire_snapshots` and `remove_orphan_files` die on a Spark 4.0→4.1
binary break — `DataSourceV2Relation.create`'s signature moved — so their schemas come from the
Iceberg jar's own `OUTPUT_TYPE` constant, read out of the class file. That is still a
measurement of the shipping artifact, not a reading of documentation.

| Procedure | Spark result schema | Source |
|---|---|---|
| `rewrite_data_files` | `rewritten_data_files_count:int`, `added_data_files_count:int`, `rewritten_bytes_count:bigint`, `failed_data_files_count:int`, `removed_delete_files_count:int` — all **non-nullable** | Executed |
| `rewrite_position_delete_files` | `rewritten_delete_files_count:int`, `added_delete_files_count:int`, `rewritten_bytes_count:bigint`, `added_bytes_count:bigint` — all **non-nullable** | Executed |
| `expire_snapshots` | `deleted_data_files_count`, `deleted_position_delete_files_count`, `deleted_equality_delete_files_count`, `deleted_manifest_files_count`, `deleted_manifest_lists_count`, `deleted_statistics_files_count` — all `bigint`, all **nullable** | `OUTPUT_TYPE` |
| `remove_orphan_files` | `orphan_file_location:string` — **one row per orphan**, not a summary count | `OUTPUT_TYPE` |

Three consequences the units inherit:

1. **`rewrite_position_delete_files` gets parity for free.** The measured Spark schema matches the
   fork result type's four accessors exactly — names, order, and the int/bigint split. MW-2 does
   not have to choose anything.
2. **`remove_orphan_files` returns a row per orphan.** The fork's action returns
   `orphan_file_locations: Vec<String>`, so the shapes line up directly — and it means the
   dry-run listing IS the Spark-shaped result, not a second surface.
3. **The two existing result-schema divergences are disclosed in code but absent from the
   registry.** `call.rs` documents both in its own doc tables, with reasoning: the fork's
   `CleanupReport.deleted_content_files` funnels data, position-delete, equality-delete and DV
   puffin files into one number, so `expire_snapshots` reports four columns under Spark's names
   and omits the two it cannot honestly split; and `rewrite_data_files` omits
   `removed_delete_files_count` because the fork does not expose dangling-delete removal there.
   Both are principled — counts are never fabricated. But the divergence registry is where parity
   divergences live **with pins**, and neither has a row or a pin. MW-5 lands them.

One item is genuinely undisclosed, found while measuring: **Spark declares `expire_snapshots`'s
result columns nullable; the engine pins them non-nullable.** For the other two procedures Spark
pins non-nullable and the engine agrees. Small, and real. MW-1 fixes or registers it.

## 6. What the lifted fence actually exposes

The intake recommended keeping the service-managed catalog fenced. **The owner ruled on
2026-08-21 to lift for both**, on the grounds that the service-managed surface is arguably the
more important of the two. This campaign implements that ruling.

The recommendation to keep the fence was made without reading the primary source. Having read
it, the fork's engine contract §8 says this:

> S3 Tables runs **service-side maintenance** (compaction, snapshot expiry) that commits
> concurrently with the engine — treat `CommitFailed` requirement mismatches as routine there,
> and expect `validate_data_files_exist` trips when service compaction rewrites files referenced
> by in-flight position deletes.

**That is a commit-conflict hazard, not a corruption hazard**, and the difference decides how much
MW-1 has to build. `validate_data_files_exist` lives in the fork's `row_delta` transaction path
and is already implemented. When the service rewrites a file that an in-flight position delete
refers to, the validation trips and the commit fails. The failure is loud, and the table is not
damaged. Iceberg's optimistic concurrency is doing exactly the job it exists to do.

So the fork already handles the unsafe part. What the fence was actually buying was not safety
but the absence of a confusing failure mode: an operator running maintenance against S3 Tables
can see a commit fail for a reason that has nothing to do with their command.

**MW-1's obligation is therefore documentation, not machinery.** The procedure surface and the
guide have to say that maintenance against a service-managed catalog can fail on a conflict, that
this is routine rather than a sign of damage, and that the response is to retry. The refusal text
that exists today should point at that guidance instead of refusing.

What MW-1 may not do is lift the fence and say nothing at all. A conflict failure with no
explanation anywhere is how a safe, correct refusal turns into a support question.

## 7. Excluded, with the reason

- **`WHEN NOT MATCHED BY SOURCE`** — a statement-surface gap, not a maintenance one. Schedule
  separately if a target pattern needs full-sync or SCD-2.
- **Format v3 and deletion vectors** — a different write encoding; it would change what
  maintenance operates on, and belongs after MOR is operable at v2.
- **REST / Hive / Nessie catalogs** — no catalog policy exists for them yet.
- **Branch-targeted writes (`REF-1`)** — fork API work, on the critical path of nothing here.
- **Sort and z-order rewrite strategies** — the existing `rewrite_data_files` is binpack-only and
  MW-2 matches that austerity deliberately. Widening both at once hides which one regressed.
- **`remove_dangling_delete_files` as its own procedure** — fold into MW-2 only if it is free.

## 8. The oracle

Live PySpark 4.1.2 + JDK 17, plus the Iceberg Spark-4.0/2.13 1.10.0 runtime for the procedure
surface. It is not a build dependency and CI cannot reach it. **Every value any pin asserts is
transcribed from it — including the incidental controls**, which is this repo's standing lesson
from the SEM campaign: a green pin that asserts a divergence as parity is the most expensive
wrong test, and the way it gets written is by reading the engine's own answer back as if it were
Spark's.

Where the oracle cannot execute (the two procedures in §5), the schema comes from the shipping
jar's own constant and the ledger says so. It is never inferred from documentation.
