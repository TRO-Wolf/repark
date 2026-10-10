# Card MB-SINK-MAINTENANCE-PATH-1: a sanctioned maintenance path against a live `foreachBatch` sink

**Date:** 2026-10-10 · **Filed by:** claude-opus-5-5 (opus-worker build lane), for the orchestrator · **Source:** owner ruling D3 of 2026-10-10 on MB-4 fold 2's question Q2 (PR #1011).

**Status:** open. **A MUST before MB-5 closes.** This card retires when a maintenance run
against a live sink no longer ends the query, or the owner declines it in writing.

## What refuses today

Since MB-4 fold 2 the `foreachBatch` door holds a lineage invariant: every snapshot on the
sink's `main` above the query's newest stamped batch must be the running batch's one stamped
snapshot. The declared sink is therefore exclusive for as long as a query name lives, between
runs included. Anything else that adds a snapshot ends the next start, or the running batch,
with `RecoveryRequiredException` (`UnstampedSinkCommit` or `UnstampedSinkChange`):

- `CALL <catalog>.system.rewrite_data_files` and `rewrite_manifests` on the sink;
- `expire_snapshots` while a batch is in flight (a removed snapshot is a change);
- a batch `INSERT`, `MERGE`, `UPDATE` or `DELETE` from any other writer;
- `rollback_to_snapshot`, a branch or tag change, a table-property change during a batch.

The two remedies the error names are a rollback of the sink to its newest stamped snapshot and
a new query name. Neither keeps a compaction: the rollback undoes it and the new name starts a
new query.

The `toTable` door does not hold this invariant and is not affected.

## Why it must be closed

Compaction against a live sink is routine in production. A table that receives one small file
per micro-batch needs it, and an operator cannot be asked to stop the stream, rename the query
or lose the compaction. Flink's Iceberg sink walks past foreign snapshots and keeps its
guarantee; the track cannot claim Flink-grade guarantees while it refuses the same run. The
exclusive sink is the right rule for this slice because it is what makes the guarantee
checkable; it is not the end state.

## Shapes to weigh (none is chosen here)

1. **A stamped maintenance commit.** The maintenance procedures take the query's stamp when
   they are run through a door that names the query, so their `replace` snapshot is the
   query's own and the invariant holds unchanged.
2. **An acknowledged snapshot.** The operator names the foreign snapshot to the query (an
   option at start, or a procedure), and the driver moves its anchor past it once. This is
   also the missing recovery for a foreign batch write the operator wants to keep.
3. **Data-neutral operations pass.** A snapshot whose operation is `replace` changes no row;
   the walk could pass it. This trusts the summary's operation field and does not cover
   `expire_snapshots` during a batch.

Each needs its own crash matrix: a kill between the maintenance commit and the next stamp, a
maintenance run racing a batch's stamped commit, and a restart after either.

## Pointers

- The invariant: `crates/repark-iceberg/src/write/sink_offsets/lineage.rs` and its map; the
  driver's two checks in `crates/repark-core/src/microbatch/run.rs`.
- Registry row `MB-4-FOREACH-EO-1` in
  [spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).
- The ledger: [mb-4-foreach-eo-ledger.md](../../ledgers/staging/mb-4-foreach-eo-ledger.md),
  fold 2 "Where the invariant stops" and fold 3.
- Q10 of the micro-batch packet rules that the driver never runs maintenance by itself; this
  card is about a run the operator starts.
