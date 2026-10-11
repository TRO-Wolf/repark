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

The remedies the error names (fold 4, 2026-10-10) are a rollback of the sink to the snapshot
the text names, and a new query name started from the printed
`repark.cdc.start-after-snapshot-id` position. The rollback undoes a compaction. The new name
keeps it, at the price of a new query identity; it is the only way today to keep a foreign
commit, and it is not a maintenance path.

**No back door (fold 4).** Until fold 4 a batch write that set the summary key
`repark.cdc.query-id` passed the check, because "stamped" is read as "carries a query id"; the
third verify measured it and noted it was the only way to run maintenance against a live sink.
That write is now refused by name (registry row `MB-4-RESERVED-SUMMARY-KEYS-1`), so no
maintenance path exists against a live `foreachBatch` sink, sanctioned or not. Another
streaming query's stamped batches still pass, by design.

**A MUST before MB-5 closes (owner ruling D3, 2026-10-10, MB-4 fold 5): expiry on a sink
with a live `foreachBatch` query retains the newest two stamps and everything between.** The
lineage walk needs the stretch under the newest stamp to tell a stray from a clean sink. A
stray left under a stamp, a process death before the driver's check, and an expiry that keeps
only the newest snapshot leave nothing to read: the next start runs on (measured: source 32
rows, sink 36). Fold 5 stops this build from committing a stamp over a stray, so the state is
no longer produced on a catalog that checks a commit's requirements; stamps written before
that rule, and a catalog that applies a commit without checking them, are still exposed. The
sanctioned maintenance path must therefore own expiry for such a sink: `expire_snapshots`
through it keeps this query's two newest stamped snapshots and every snapshot between them,
whatever `retain_last` and `older_than` say, or refuses. Recording the expected parent in
every stamp was weighed as the durable alternative and not adopted: another query's stamp
that lands inside a batch from a second process mismatches without a stray (ledger, fold 5).

**"Ever ran through `foreachBatch`" ends at retention (MB-4 fold 6, 2026-10-10; the fifth
verify's `healthy.py d2_ever_expired`).** The `toTable` door walks a name for as long as one
of the name's `foreachBatch` stamps or its mark is on the sink. After an expiry that leaves
only `toTable` stamps, a foreign `INSERT` between `toTable` runs is accepted, and a later
`foreachBatch` start under the same name runs on with the foreign row kept. The maintenance
path owns expiry for such a sink and so can close it: either keep one `foreachBatch` stamp
of the name, or leave a durable marker in the offsets property that the name ran through
`foreachBatch`.

**What the check does not look at**, measured in the third verify and kept: between batches, a
property-only commit, a schema change, a branch or tag, a staged or branch write,
`compute_table_stats`, and `expire_snapshots` that keeps the newest stamp. A shape for this
card must say whether those stay outside the rule.

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
