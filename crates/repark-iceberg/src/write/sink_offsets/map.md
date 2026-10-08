# map — repark-iceberg/src/write/sink_offsets

## Purpose

Children of the `write::sink_offsets` module (`write/sink_offsets.rs` declares them). The sink
offsets claim, the epoch check and the stamped commit arms live in the parent file; this
directory holds the piece split out of it. The test files of the module stay beside the parent in
`write/`, as `#[cfg(test)]` `#[path]` children of its probe module.

## Contents

- `append_fence.rs` — **MB-2c closing slice (2026-10-07, owner ruling ~20:55 EDT:
  `F-APPEND-PIN-BASE-1` is not built in the fork now, and the RePark-side stopgap is allowed):**
  `AppendFence`, a catalog wrapper installed for one commit only, and only when the commit holds a
  claimed stamp (the append arm and the stamp-only door). At `update_table` it reads the
  refreshed base the fork hands in `TableCommit::base_table` and refuses with a non-retryable
  `DataInvalid` when this query's `repark.cdc.query-id` was stamped on `main` above
  `ClaimedStamp::base`, or when the base is no longer an ancestor of `main`. When the walk finds
  neither, it still runs the epoch check on the refreshed table and refuses on its error (an
  expired stamp the offsets property still names, C-012). `refusal_of` reads
  the typed `MicroBatchError` back at the two commit sites. Every other `Catalog` method forwards
  to the inner catalog. The full rule, the pins and the retirement list are in
  [the parent map](../map.md) under the MB-2c closing slice. Stopgap for
  [F-APPEND-PIN-BASE-1](../../../../../task/roadmap/mid-term/f-append-pin-base-1-2026-10-07.md);
  the file is deleted when the fork lands it and RP-N repins.
  pins: mb-2c/C-008, C-009, C-010, C-011, C-012, C-013, C-014
