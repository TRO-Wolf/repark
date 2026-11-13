# map — repark-sql/tests/staged_publish_faults

## Purpose

The fault helpers of the native door's staged-publish suite (`../staged_publish_cleanup.rs`), kept
here so that suite stays under the Rust file-size ceiling.

## Contents

- `mod.rs` — CORPUS-CHECKER-REPAIR-1 (2026-11-13): the two commit-table failure points the suite
  used to skip. `inject` writes the next `v<N>.metadata.json` at publish time (the
  `metadata_rename` point, a version file that already exists); `run_until_killed` drops the
  statement once the fault catalog parks it at publish, which stands in for the
  `kill_before_publish` SIGKILL in process; `recorded_refusal` reads RePark's recorded failure text
  out of the cells row.

## Pointers

- Up: [../map.md](../map.md)
