# STRUCT-NESTED-INITIAL-DEFAULT-1 — a struct with a nested initial default reads NULL where the spec gives the struct

**Rating: proposed S3 (the replay, 2026-11-21); the owner has not rated it.**

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`).
**Source:** an outside gap-map audit of 2026-11-21 (finding id `NEW-V3-1`), replayed on main `dac2af61` by an Opus 5.5 agent on 2026-11-21. There is no Spark oracle for this card; the expected value comes from the spec.

**Status:** filed 2026-11-21; not dispatched.

**Severity: S3 (proposed).** Only a hand-made fixture reaches it.

## Finding

Setup for the `[M]` cells: Spark 4.1.2 with the Iceberg 1.11.0 jar, Java 17, session zone UTC, ANSI on; RePark built from main `dac2af61`.

- **[M] Spark side.** An error: `Cannot create expression literal from GenericRecord: Record(null)`.
- **[M] RePark.** (1, NULL). The spec says (1,{x:7}).
- **Oracle.** There is no executable oracle. The expected value comes only from the spec, and Java cannot read the fixture (the replay's note).
- **Control.** None for the compound case. The primitive initial-default projection (1,7) agrees on both engines.

## Why S3

Only a hand-made fixture reaches it, and Java cannot read that fixture either.

## Scope

A struct (compound) initial default, reached only through a hand-made fixture. Without an oracle, the spec alone decides the expected value.

**Label path:** owner to decide (depends on the workload's reach).

Up: [map.md](map.md).
