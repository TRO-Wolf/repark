# NS-BUCKET-PRE-EPOCH-1 — the `timestamp_ns` bucket transform gives a wrong bucket below the epoch

**Rating: proposed S2 (the replay, 2026-11-21); the owner has not rated it.**

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`).
**Source:** an outside gap-map audit of 2026-11-21 (finding id `NEW-fork-5`), replayed on main `dac2af61` by an Opus 5.5 agent on 2026-11-21.

**Status:** filed 2026-11-21; not dispatched.

**Severity: S2 (proposed).** A wrong bucket below the epoch, limited to `timestamp_ns` (v3, EXPERIMENTAL).

## Finding

Setup for the `[M]` cells: Spark 4.1.2 with the Iceberg 1.11.0 jar, Java 17, session zone UTC, ANSI on; RePark built from main `dac2af61`.

- **[M] Java, `Transforms.bucket(128)`.** −1000→104, −1→104, 0→124, 1→124.
- **[M] RePark.** −1000→104, −1→124, 0→124, 1→124.
- **[M] Controls.** −1000, 0 and 1 agree with Java. The stored raw values (−1000, −1, 0, 1) are intact.
- **The replay's reading of the cause.** The nanosecond-to-microsecond step truncates where it should floor. The verdict gives no file:line for this.

## Why S2

A wrong bucket below the epoch. No Java read miss was measured.

## Scope

The `timestamp_ns` bucket transform for negative values, in v3 (EXPERIMENTAL).

**Label path:** owner to decide (depends on the workload's reach).

Up: [map.md](map.md).
