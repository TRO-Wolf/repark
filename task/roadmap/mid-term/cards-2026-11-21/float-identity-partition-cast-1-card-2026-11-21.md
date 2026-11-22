# FLOAT-IDENTITY-PARTITION-CAST-1 — an identity partition on a double refuses 100.1, where Spark commits it

**Rating: proposed S3 (the replay, 2026-11-21); the owner has not rated it.**

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`).
**Source:** an outside gap-map audit of 2026-11-21 (finding id `NEW-fork-7`), replayed on main `dac2af61` by an Opus 5.5 agent on 2026-11-21.

**Status:** filed 2026-11-21; not dispatched.

**Severity: S3 (proposed).** A loud over-refusal.

## Finding

Setup for the `[M]` cells: Spark 4.1.2 with the Iceberg 1.11.0 jar, Java 17, session zone UTC, ANSI on; RePark built from main `dac2af61`.

- **[M] Spark.** 100.1 commits: (1,100.0999984741211).
- **[M] RePark.** Refused: "DataInvalid => Unable to convert raw literal (double)…". The table is empty.
- **[M] Control.** 100.0 commits on both engines.

## Why S3

A loud refusal of a value that Spark commits.

## Scope

The double identity-partition literal on write. The exact control (100.0) commits on both engines.

**Label path:** owner to decide (depends on the workload's reach).

Up: [map.md](map.md).
