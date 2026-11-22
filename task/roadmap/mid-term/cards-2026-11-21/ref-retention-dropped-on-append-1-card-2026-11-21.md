# REF-RETENTION-DROPPED-ON-APPEND-1 — an append drops the main branch's retention values from the newest metadata file

**Rating: proposed S2 (the replay, 2026-11-21); the owner has not rated it.**

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`).
**Source:** an outside gap-map audit of 2026-11-21 (finding id `NEW-fork-3`), replayed on main `dac2af61` by an Opus 5.5 agent on 2026-11-21.

**Status:** filed 2026-11-21; not dispatched.

**Severity: S2 (proposed).** Silently loses the retention policy; no data loss.

## Finding

Setup for the `[M]` cells: Spark 4.1.2 with the Iceberg 1.11.0 jar, Java 17, session zone UTC, ANSI on; RePark built from main `dac2af61`.

- **[M] Spark.** After the append, the main ref keeps min-snapshots 5 and max-age 1555200000 (in its newest metadata file, `v4.metadata.json`).
- **[M] RePark.** The newest file, `v4.metadata.json`, has the main ref `{"type":"branch"}` only, so both values are dropped. The `.refs` view shows null.
- **[M] The probe's gap, now closed.** The probe's RePark side read only the `.refs` metadata view. The replay added a direct read of the newest metadata file, and that read confirms the loss.
- **[M] Controls.** Before the append, both engines show the same Java-made ref. Rows (1),(2) are the same on both.

## Why S2

The retention policy is lost silently. The verdict says a later expire could keep fewer snapshots; the replay did not measure that consequence.

## Scope

The retention values of the main ref across an append's metadata write. The consequence at expiry is not measured.

**Label path:** owner to decide (depends on the workload's reach).

Up: [map.md](map.md).
