# SESSION-CATALOG-TABLE-VISIBILITY-1 — a table one session created is not found by another session on the same catalog; the root is not diagnosed

**Rating: proposed S2, to be confirmed by a diagnosis (the replay and the live run, 2026-11-21); the owner has not rated it.**

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`).
**Source:** two observations, one card, no audit finding id. Observation 1 is the replay's incidental note (an Opus 5.5 agent, main `dac2af61`, 2026-11-21). Observation 2 is the live acceptance run `44955277129` on main `5a3db151` (2026-11-21).

**Status:** filed 2026-11-21; not dispatched.

**Severity: S2 (proposed), to be confirmed by a diagnosis.** A session cannot see a table another session wrote, and the root is not diagnosed.

## Finding

- **[M] Observation 1, the replay's incidental note.** A fresh RePark session, configured with the same Hadoop catalog (`lc`) and the same warehouse, could not find `lc.db.day_t`, which an earlier RePark session had written: "table 'lc.db.day_t' not found". The replay saw the same with two catalogs. The replay read through `register_table` instead. The verdict says this may be the handoff's open item on native Hadoop namespace durability across sessions, and that it is undiagnosed.
- **[M] Observation 2, the live run `44955277129`.** In `test_held_frame_external_writer_against_glue`, `test_held_frame_external_writer_against_s3tables`, `test_held_frame_refresh_against_glue` and `test_held_frame_refresh_against_s3tables`, `spark.newSession()` used as the second writer raised `AnalysisException: Error during planning: table '<catalog>.<namespace>.<table>' n…` (message cut in the log) for the table the first session had just created.
- **Reading recorded with the live run.** Spark's `newSession()` sees such a table. The four cells therefore did not measure the held-frame cache question they were written for.

## Why S2

The proposal is S2 pending a diagnosis. The sources give no impact measure beyond the two observations: a fresh session cannot find a table another session wrote, and four live cells did not measure their question.

## Scope

Two observations, one card, root not diagnosed. The verdict says this should be checked before anyone relies on it. The first step is a diagnosis of cross-session catalog visibility on the Hadoop catalog and on the Glue and S3 Tables live runs.

**Label path:** owner to decide (depends on the workload's reach).

Up: [map.md](map.md).
