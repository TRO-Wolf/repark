# CASE-DISTINCT-ASSIGN-TWIN-WRITE-1 — an assignment to one of two case-distinct columns also writes the other

**Rating: proposed S1 (the replay, 2026-11-21); the owner has not rated it.**

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`).
**Source:** an outside gap-map audit of 2026-11-21 (finding id `NEW-write-1`), replayed on main `dac2af61` by an Opus 5.5 agent on 2026-11-21. The replay widened the audit's scope; the wider scope is in the Finding.

**Status:** filed 2026-11-21; not dispatched.

**Severity: S1 (proposed).** A silent write to a column the statement never named.

## Finding

Setup for the `[M]` cells: Spark 4.1.2 with the Iceberg 1.11.0 jar, Java 17, session zone UTC, ANSI on; RePark built from main `dac2af61`. The table has two columns that differ only by case, `score` and `Score`.

- **[M] Spark, MERGE.** `SET Score=99` gives (1,99,20) in all five cells (plain; CoW/MoR × matched / not-matched-by-source).
- **[M] RePark, MERGE.** The same statement gives (1,99,99) in all five cells. A cold Spark read of RePark's files agrees.
- **[M] Controls.** `SET score=77`: Spark (1,10,77), RePark (1,77,77). Plain `UPDATE … SET Score=55`: Spark (1,55,77), RePark (1,55,55).
- **[M] Wider scope.** The twin write happens in both directions, and outside MERGE as well: a plain UPDATE also twin-writes, and `SET score=` also writes `Score`.

## Why S1

A statement writes a column it never named, with no error, and the stored data is wrong. The audit reported the MERGE case with the upper-case name; the replay found the same write on plain UPDATE and in both name directions.

## Scope

The fix belongs at shared assignment and column resolution, not only in `merge_fragments.rs` (the replay's note). The card covers MERGE and plain UPDATE, in both name directions.

**Label path:** owner to decide (depends on the workload's reach).

Up: [map.md](map.md).
