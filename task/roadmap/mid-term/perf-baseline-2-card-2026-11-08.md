# Card PERF-BASELINE-2 — ETL jobs against Spark 4.1.2, with Polars and DuckDB targets

**Date:** 2026-11-08 · **Filed by:** orchestrator, on the owner's ruling B · **Source:** owner
ruling "Go with B" (2026-11-08): performance numbers live in cards, not in PROJECT.md; the
north-star redraft of 2026-11-08, its performance rows "ETL job vs Spark" and "Connector read".
**Status:** filed, not scheduled. Protocol, baseline and evidence are PENDING; the unit that
runs the measurement sets them.

## Why

PROJECT.md "Performance posture" names the two reference surfaces and points here. The gate, the
published targets, the protocol and the measured baseline live in this card. The PROJECT.md
sentence "its acceptance target lives in the approved performance card, never here" is the rule
this card applies.

## Reference shapes

The ETL shapes, each run end to end on the same box and the same data as its Spark run:

1. read Iceberg, join, aggregate, write Iceberg;
2. read Postgres, transform, write Iceberg.

## Gate (blocks a merge)

At or better than Spark 4.1.2 local mode on the same box and data.

## Published targets (published and re-measured; not a merge gate)

Polars and DuckDB at 2.0× for the first baseline, tightened each release; a target is published
and re-measured, a gate blocks a merge. Where a shape has no equivalent in Polars or DuckDB, that
target is not stated for the shape.

## Protocol

PENDING, set by the unit: data sizes, run count, the Spark 4.1.2 local-mode configuration, the
settle and load rules, and how "the same box" is recorded.

## Baseline

PENDING: the first measured ratios against Spark 4.1.2, Polars and DuckDB, recorded by the unit
that runs the protocol, with the date and the box.

## Evidence

PENDING: the ledger row per shape, with the verbatim timing output.

## Pointers

- Up: [map.md](map.md)
- Reads: [Connector read companion, the C-3 work order](../../wo/c-3-partitioned-reads.md) (the
  connector factor, a separate gate).
- Intent: [PROJECT.md](../../../PROJECT.md) "Performance posture".
