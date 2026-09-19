# map — repark-spark/benches

## Purpose

Benchmarks that drive the real product path: a `ReparkSession` with `SparkExtension` and
`SparkDialect`, SQL through `session.sql(...)`, results streamed. A number from here is a
baseline only with its recorded environment (H-3, [docs/perf/map.md](../../../docs/perf/map.md)).

This directory closes when its last bench is retired; `ice_read_perf` retires when the v1.5.0
re-measure gate (slate unit 4) closes and the owner drops the AWS bench tables.

## Contents

- [ice_read_perf/](ice_read_perf/map.md) — **ICE-READ-PERF-0 (2026-09-19):** the Iceberg read
  bench bed: `setup` writes the local bed (or, with `--catalog glue|s3tables --phase
  create|write`, the AWS bench tables), `run --mode cold|warm|concurrent|concurrent-cold
  [--repeat N]` measures seven queries (round 2 added Q7, the footer / page split, concurrent-cold
  and repeats), and the R-3 size flag stops a run before its first scan.

## Pointers

- Up: [../map.md](../map.md)
- Slate: [task/roadmap/mid-term/ice-read-perf-slate-2026-09-18.md](../../../task/roadmap/mid-term/ice-read-perf-slate-2026-09-18.md)
