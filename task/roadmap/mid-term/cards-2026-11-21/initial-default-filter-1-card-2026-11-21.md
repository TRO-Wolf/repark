# INITIAL-DEFAULT-FILTER-1 — a filter on a column added with an initial default finds no row, on both engines

**Rating: proposed S3 against parity, S2 if the spec is the bar (the replay, 2026-11-21); the owner has not rated it.**

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`).
**Source:** an outside gap-map audit of 2026-11-21 (finding id `NEW-fork-1`), replayed on main `dac2af61` by an Opus 5.5 agent on 2026-11-21.

**Status:** filed 2026-11-21; not dispatched.

**Severity: S3 against Spark parity (proposed); S2 if the spec is the bar.** A spec gap, not a parity gap. Which of the two applies is the owner's call.

## Finding

Setup for the `[M]` cells: Spark 4.1.2 with the Iceberg 1.11.0 jar, Java 17, session zone UTC, ANSI on; RePark built from main `dac2af61`.

- **[M] Spark.** An old row reads projection (1,7). `WHERE d=7` gives []; `IS NULL` gives []. After the append, `d=7` gives only (2,7).
- **[M] RePark.** The same answers as Spark.
- **[M] Controls.** The projection controls agree on both engines.
- **[M] Reference.** Spark with Iceberg 1.11.0 (Java) gives the same empty answer, so both engines break the spec in the same way.

## Why S3

Both engines match, so this is a spec-conformance gap that the reference shares, not a RePark-vs-Spark divergence. The audit's own refutation note says the same, but its card still carries S2 against parity. The severity is the owner's call: S3 against Spark parity, or S2 if the spec is the bar.

## Scope

Filter semantics over a column's initial default. Whether it counts as a gap depends on whether the spec is the bar.

**Label path:** owner to decide (depends on the workload's reach).

Up: [map.md](map.md).
