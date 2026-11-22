# CASE-SENSITIVE-STATIC-PARTITION-1 — a static partition clause with the wrong case replaces the partition where Spark refuses

**Rating: proposed S1 (the replay, 2026-11-21); the owner has not rated it.**

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`).
**Source:** an outside gap-map audit of 2026-11-21 (finding id `NEW-write-2`), replayed on main `dac2af61` by an Opus 5.5 agent on 2026-11-21.

**Status:** filed 2026-11-21; not dispatched.

**Severity: S1 (proposed).** A silent replacement where Spark refuses.

## Finding

Setup for the `[M]` cells: Spark 4.1.2 with the Iceberg 1.11.0 jar, Java 17, session zone UTC, ANSI on; RePark built from main `dac2af61`.

- **[M] Spark.** `PARTITION (P=1)` raises NON_PARTITION_COLUMN. The rows stay (10,1),(20,2).
- **[M] RePark.** The same statement commits. The rows are (99,1),(20,2). A cold Spark read agrees with RePark's files.
- **[M] Control.** A lower-case `PARTITION (p=1)` gives (99,1),(20,2) on both engines.

## Why S1

A silent replacement of partition rows where Spark refuses the statement. The trigger needs `caseSensitive=true` and static mode (the replay's note).

## Scope

The static `PARTITION (…)` clause on a write, under case-sensitive resolution, where the clause's name differs in case from the partition column. The lower-case control already matches Spark.

**Label path:** owner to decide (depends on the workload's reach).

Up: [map.md](map.md).
