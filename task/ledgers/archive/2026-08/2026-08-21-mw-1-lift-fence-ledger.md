# MW-1 — the fence comes down, and expire tells the truth about delete files

**Date:** 2026-08-21 · **Branch:** `feat/mw-1-lift-fence` · **Base:** `53c8987` (`main`,
post-#195) · **Charter:** [mw-0-charter-ledger.md](../../completed/mw-0-charter-ledger.md) · **Design:**
[../docs/design/iceberg-maintenance-wave.md](../../../../docs/history/iceberg-maintenance-wave/design.md)

## What changed, and why it grew

Chartered as three things: lift the fence, fix the nullability, document the conflict. The owner
then ruled on the campaign's target — **as close to 1:1 with Spark as possible, at least in end
results**, because the point of the project is replacing Glue and Spark jobs, and Iceberg
maintenance is part of those jobs. That ruling turned the disclosed `expire_snapshots` column gap
from something to register into something to close, so it landed here rather than in MW-5.

## 1. The fence

`refuse_non_local_catalog` is gone, along with its call. `catalog_handle` already refuses an
unknown catalog one line earlier, so nothing was left holding the door.

The refusal was never a capability gap. Nothing downstream of the gate assumes a local
filesystem: the execute paths take `&dyn Catalog` and reach storage through the same FileIO
everything else uses. It was a v1 blast-radius decision, and lifting it is policy.

## 2. What the fence was guarding, read from the source

The recommendation to keep the service-managed catalog fenced was made from a secondhand
citation. The fork's `ENGINE_CONTRACT` §8 actually says the service commits its own compaction
and expiry concurrently, that `CommitFailed` requirement mismatches are **routine** there, and
that `validate_data_files_exist` trips when service compaction rewrites a file an in-flight
position delete references.

That is a commit conflict, not corruption. The validation is already implemented fork-side. The
commit fails loudly and the table is undamaged.

So there is no mitigation to build, and one thing to say clearly — which the module docs and the
guide now do: on S3 Tables, expect an occasional conflict and retry it, because it is the
concurrency control working rather than a sign of damage.

## 3. `expire_snapshots` returns Spark's six columns

Measured on a live **Spark 4.0.1** + Iceberg 1.10.0 oracle. The 4.1.2 oracle cannot execute this
procedure (a Spark 4.0→4.1 `DataSourceV2Relation.create` signature break), so this unit stood up
a second oracle rather than pinning values it could not measure.

Spark, on three merge-on-read MERGEs plus a compaction:

```
deleted_data_files_count=4   deleted_position_delete_files_count=2
deleted_equality_delete_files_count=0   deleted_manifest_files_count=6
deleted_manifest_lists_count=4   deleted_statistics_files_count=0
```

This engine reported `deleted_data_files_count = 6` and omitted the other two columns. A job
migrating off Spark got a wrong number where it looked and an error where it read.

The fix does not fabricate anything. The fork's `CleanupReport.deleted_content_files` is one
funnel of paths because `expire_cleanup` collects `entry.file_path()` and drops the
classification — but every `ManifestEntry` carries `content_type()` with exactly Spark's
three-way split. `classify_content_files` walks the table **before** cleanup runs, builds
path → content type, and `ExpireCounts::tally` splits the funnel against it. A path that cannot
be classified is counted in **no** column; it is never folded in to make the arithmetic tidy.

Nullability came with it. Spark declares all six of these columns nullable (`iconst_1` per
`StructField` in the jar's `OUTPUT_TYPE`) while declaring its two rewrite procedures'
non-nullable. This engine had pinned all of them non-nullable. Matched per procedure, not by one
blanket rule.

## 4. The pins, and what each one is for

| Pin | Asserts | Red before |
|---|---|---|
| `call_runs_against_both_remote_catalog_policies` | Both remote policies execute `expire_snapshots` and `rewrite_data_files` | Refused with the LOCAL-only message |
| `call_still_refuses_an_unknown_catalog` | Lifting the fence did not turn a typo into a silent no-op | (new; guards a regression the lift could introduce) |
| `call_expire_snapshots_keeps_tag_reachable` | Six columns, Spark's order, all nullable | 4 columns |
| `call_expire_splits_content_files_like_spark` | Stranded position deletes land in the position column, with an independent non-zero data count beside them | Column absent |

## 5. Two scenarios that looked like bugs and were not

Recorded because each cost a diagnostic pass, and both are facts a later unit will meet again.

**Compaction below the binpack floor is a silent no-op.** The first draft of the split pin made
three data files and compacted. It succeeded, rewrote nothing, and every column read `0` — a
green-looking no-op. Java's binpack defaults to a minimum of 5 input files. The pin now writes
enough files to cross it, and asserts a non-zero delete-file count *before* the split so the
evidence cannot silently evaporate again.

**This engine's `rewrite_data_files` keeps position deletes.** After compaction the delete files
were still present, where Spark's compaction had orphaned two. That is not a correctness defect:
the compacted table was verified to read correctly with the deletes still applied. Orphaning
delete files through compaction is what `rewrite_position_delete_files` does, and that is
**MW-2**. The pin strands its delete files by rolling back past the MERGEs instead, which does
not wait on MW-2 and does not pretend the two behaviours are the same.

## 6. Collateral

`docs/guide/iceberg-guide.md` gains the six-column result and a maintenance-on-Glue-and-S3-Tables
section carrying the retry guidance. `crates/repark-spark/src/map.md` in lockstep. The module doc
table now describes six real sources instead of a divergence.

## What MW-5 no longer inherits

One of the three registry rows the charter queued. The `expire_snapshots` column funnel is
**closed**, not registered — a divergence that no longer exists does not get a row. The
`rewrite_data_files` omitted `removed_delete_files_count` still stands, and belongs to MW-2,
which is where the fork's dangling-delete surface comes into scope.
