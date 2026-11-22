# Card ICE-HELD-FRAME-SNAPSHOT-1: a frame or view held across the session's own Iceberg write answers the snapshot bound at the hold, silently, and stores old rows

**Date:** 2026-11-20. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's morning ruling 2 of 2026-11-20 and Fable's guidance of the same date (the owner's delegation, about 00:22 UTC). **Sources:** the [step 0 sketch](../../../wo/ice-held-frame-snapshot-1/sketch.md) and its [domain table](../../../wo/ice-held-frame-snapshot-1/domain-table.md); the [morning record](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-20-am.md), §3; and Fable's reply titled `Fable reply — ICE-HELD-FRAME-SNAPSHOT-1 step 0 2026-11-20`, kept outside the repository.

**Status:** filed 2026-11-20. Unit 1a merged as #1390 (`347eff83`). D1, the view re-bind, merged as #1391 (`dac2af61`); its two verifiers were pending at the 2026-11-21 rulings. The live Glue and S3 Tables cells merged as #1393 (`5a3db151`). Unit 2 (the cached cells) is not merged; its PR, #1394, has no record in the tree yet. Unit 1b is not built. Live acceptance run 44955277129 (2026-11-21, main `5a3db151`): the same-session insert, delete and copy cells equal Spark on Glue and S3 Tables; the four external-writer and refresh cells failed before measuring, because `spark.newSession()` could not find the table (card [SESSION-CATALOG-TABLE-VISIBILITY-1](../cards-2026-11-21/session-catalog-table-visibility-1-card-2026-11-21.md), filed in #1396; cells in [live-aws.md](../../../wo/ice-held-frame-snapshot-1/built/live-aws.md)). Q10 and Q2 stay unmeasured.

**Severity: S1, label-blocking, confirmed by the owner on 2026-11-20** (ruling 2 of the morning record).

## Finding

A frame or view held across a same-session write answers from the snapshot the table had when its name was resolved, where Spark re-binds the table once per query. Per build and mode, step 0 measured 974 cells on a scalar Iceberg v2 table: 785 are WRONG, all silent, and 509 of those store the old rows into an Iceberg table.

Per build and mode, 252 more answer old rows where Spark raises, and 7 raise where Spark answers (sketch §0.2).

## Units

- **1a: the re-bind and the command scope.** Merged as #1390 (`347eff83`), from branch `fix/ice-held-frame-snapshot-1a`. 640 cells per build and mode. The sketch's split (§0.6) had 11 files and put the command scope in 1b. Fable's Q6 moves the command scope into 1a, making 12 files. That is Fable's guidance, not an owner ruling.
- **1b: the bare `writeTo` re-plan.** 118 cells per build and mode. Not built. Fable reads Spark's source (from memory, not yet from the jar) as re-planning a bare frame's `writeTo` on every write, and 1b measures that on Spark before it pins anything.
- **2: the cached cells after A1.** Not merged. 4 cells per build and mode. They need A1's lazy table to drop its fill on a write.
- The path read (27 cells per build and mode) is not a unit here. It is card ICE-HELD-PATH-READ-1, at S2.

## Order and cost

- Owner ruling 5: a live Glue and S3 Tables run after 1a merges and before 1b builds. Fable's eight live cells are in the morning record, §3 (Fable §14).
- Owner ruling 6: no new build until A1's series and 1a's cost sample are in.
- Cost: step 0 measured none, and Glue and S3 Tables were read, not measured. Fable §12 has the build take main's baseline under the exclusive lock, C-012 at the hard 1.05x, before product code. No cost claim is made without a like-for-like pair.

## Links

- [sketch.md](../../../wo/ice-held-frame-snapshot-1/sketch.md): §0 in short; §7 the questions Q1 to Q11.
- [domain-table.md](../../../wo/ice-held-frame-snapshot-1/domain-table.md): §7, the family table.
- [morning record](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-20-am.md), §3: Fable's guidance on Q1 to Q11 and on §12 to §14.

Up: [map.md](map.md).
