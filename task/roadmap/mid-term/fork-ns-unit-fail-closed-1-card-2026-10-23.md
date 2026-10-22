# Card FORK-NS-UNIT-FAIL-CLOSED-1: the fork's record-batch writer refuses an Arrow timestamp unit that is not the column's unit

**Date:** 2026-10-23. **Filed by:** Claude (claude-fable-5-1), orchestrator, on the owner's ruling of
2026-10-23 ("rule it") after the ZONE-SEAM-1 verifier's round-2 finding S1-3.

**Status:** open, clerk grade, to ride with the next fork repin (RP-60 or later). Not a RePark
unit; the RePark half is WRITE-SPINE-1 step (2)
([write-spine-1-card-2026-10-22.md](write-spine-1-card-2026-10-22.md)).

**Retires:** when the fork's writer refuses a unit mismatch with one error text, a RePark pin stores
a nanosecond Arrow batch into a microsecond column through the Rust append API and reads that
refusal, and the registry row R-024 names the pin.

## Why

Every nanosecond defect since R-007 (R-009, R-017, R-019, R-020, R-024, the round-2 S1-3 of
ZONE-SEAM-1) is a Spark store-assignment rule applied door by door in RePark and a feed shape one
door's copy did not see. Those rules stay in RePark's plan-time conform stage. The fork's
record-batch writer is the last place that sees the Arrow unit before Parquet, so a nanosecond array
meeting a microsecond column is cast somewhere on the way down today (R-024's silent truncation). A
format-layer invariant there makes the silent class impossible whatever the planner misses.

## The ask

- In the fork's Arrow-to-Iceberg write path, an Arrow `Timestamp` whose unit differs from the Iceberg
  column's unit (microsecond for `timestamp`/`timestamptz`, nanosecond for `timestamp_ns`/
  `timestamptz_ns`) is refused before any file is written, with one error text that names the column,
  the Arrow unit and the column unit. No cast, no floor: conversion is the engine's job.
- A fork pin per unit pair; the fork's nightly-interop suite carries it.
- RePark: one pin through the Rust append API (the only RePark door that reaches the writer without
  the plan-time stage), citing R-024, and a dated line on R-024.

## What this card does not do

It does not move Spark's floor rule into the fork, and it does not change what RePark stores on any
door today; the engine-side rules land in WRITE-SPINE-1 step (2).
