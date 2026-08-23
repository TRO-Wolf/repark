# The Iceberg write-path maintenance wave — the archived record

**Archived 2026-08-23** (MW-5 campaign close). History, not law. Current state:
[STATUS.md](../../../STATUS.md); rules: [AGENTS.md](../../../AGENTS.md).

Merge-on-read writes were production-grade and maintenance was fenced off on the
catalogs that hold production data. This campaign lifted that fence, wired
`rewrite_position_delete_files` and `remove_orphan_files`, proved compact+expire
on Glue, and re-measured the MW-0 1,000-row / ten-MERGE growth demo.

Unit ledgers live in
[task/ledgers/archive/2026-08/](../../../task/ledgers/archive/2026-08/map.md)
(MW-0/MW-5 sit in `completed/` until the next pickup archives them).

## What lives here

| File | What it records |
|---|---|
| [design.md](design.md) | The 2026-08-21 design, with a dated correction that MW-5 did not land the two schema registry rows (MW-1/MW-2 closed them as columns). |
| [slate.md](slate.md) | The MW-0…MW-5 unit contract and sequencing. |

## Promotion check

| Claim in the design/slate | Live home after archival |
|---|---|
| Fence is policy, both catalog policies | STATUS MW; `call.rs`; guide "Maintenance on Glue and S3 Tables" |
| Five CALL procedures, no omitted Spark column | STATUS; `crates/repark-spark/src/call.rs` |
| MW-0 2.1× growth demo | STATUS scorecard; `python/repark/tests/test_mw5_baseline_delta.py` |
| Remaining divergences | registry MOR-1, MOR-2, ORPHAN-1, ORPHAN-2, B-MOR-3 |
| S3 Tables MOR compact+expire out | STATUS; OD-3 Glue prefix only |
| Live Glue proof | aws-acceptance run 32640855145 on `d3c248c` |

## Rules for this directory

Immutable except link repair and dated corrections. Status claims carry an
effective date. [STATUS.md](../../../STATUS.md) wins on any conflict.
