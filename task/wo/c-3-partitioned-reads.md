# C-3 — partitioned parallel reads and the ConnectorX benchmark · grade-B skeleton · standard tier · 1.6

> **North Star briefing (owner, 2026-10-05).** Every executor and verifier on this order reads [the CDC and micro-batch North Star](../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md) first: NS-1…NS-19, the authority order in §2 (Flink governs guarantees, Spark the surface, Iceberg's own sinks the commits, else refuse with a dated row), the Rust placement in §4, and the production-grade lists in §5. The North Star sits below every ruled O/D/CC/ES/CL row and above the agent's judgment. For a question no ruled row answers, write the four lines (the question, Flink's answer, Spark's answer, the NS default), act on the default, and record the four lines as a dated ledger row. Halt only for the three §8 cases.


## 0. Why, and what is out

C-3 parallelizes C-2's read path on a partition column and proves it against the
external bars: the same query benchmarked against ConnectorX and pandas+SQLAlchemy,
the factor recorded (card 1.6). Out: writes (C-4), SQL Server (C-5), conveniences
(C-6).

## 1. Rulings already made

- R-1 (engine): the **standard tier** with one verifier per stack and DIFF-PROBE
  (card 1.6: "C-3…C-6 run on the standard tier with one verifier per stack and
  DIFF-PROBE on each").
- R-2 (shape): partitioned parallel reads on a partition column, straight into
  Arrow (card 1.6; the 1.10 row carries the same bar for Trino later).
- R-3 (benchmark): the same query vs ConnectorX and pandas+SQLAlchemy; the factor
  is recorded (card 1.6). The pins require "the benchmark within the stated
  factor" (card 1.6 pins) but no factor is stated in any source: the acceptance
  factor is **PENDING** — C-3 records the measured factor and the ruling follows.
- R-4 (ledger): cites ConnectorX (the partitioned-read design and the benchmark
  bar) and ADBC per the 1.6 row's standing instruction (owner, 2026-09-13).

## 2. Files

`crates/repark-connect/src/read/postgres.rs` (partitioning),
`provider/table.rs` (partition-aware scan); the benchmark harness beside the C-0
live cells; `map.md` lockstep. **PENDING on C-2**: the provider trait the
partitioning plugs into, the partition-column selection surface, the exact
benchmark queries.

## 4. Steps

1. Partition the C-2 scan on the partition column; pins stay green.
2. Run the benchmark (ConnectorX, pandas+SQLAlchemy, repark); record the factor.
3. Ledger with the citations (R-4) and the measured factor; one commit per slice.

## 5. Gates and the line that means green

`cargo test -p` on touched crates; live cells on the C-0 container; DIFF-PROBE
(R-1); `bash scripts/check_map_md.sh --base origin/main`; clippy/fmt clean. Pin:
partitioned results equal the unpartitioned C-2 answers row for row. **PENDING**:
the benchmark-acceptance factor (R-3).

## 6. Halt rules

- **H-C2** Anything in §2 marked PENDING on C-2 is still open: halt; do not guess
  the provider shape.
- **H-SKEW** A partition plan changes an answer vs the C-2 scan: halt with the
  diverging rows; do not absorb it.

## 7. Hand-back

```json
{"unit":"C-3","tests":"","benchmark_factor":"","halt":null}
```

Plus the ledger (R-4).
