# ICE-STREAMING — structured streaming over Iceberg tables, a v1.6.0 card

**Filed:** 2026-09-19 by the orchestrating session on the owner's ruling C-1
([ice-parity-inventory-2026-09-19.md §0](ice-parity-inventory-2026-09-19.md#0-owner-ruling-2026-09-19--this-slate-gates-v150)):
streaming read and write of Iceberg tables (inventory unit IPI-47) leaves the v1.5.0 gate and is scheduled for
**v1.6.0**, the connectors minor. Nothing here is measured yet; the first step below is the measurement.

## Why it is its own release item

RePark has no streaming runtime: `readStream` and `writeStream` refuse under registry rows SES-DECL-readStream and
SES-DECL-streams. Parity on the three inventory cells (`R-STREAM-READ`, `R-STREAM-READ-SKIP`,
`W-STREAM-WRITE-FILESRC`) cannot be measured until a runtime exists. The Iceberg half is delivered by v1.5.0:
incremental append reads between snapshots and the changelog scan (IPI-22), which Spark's streaming read loops over,
and snapshot-summary properties on write (ICE-WRITE-OPTIONS-1), which carry a streaming write's idempotency marker.

## Step 0 — the oracle (before any design is ruled)

Record on Spark 4.1.2 + Iceberg 1.11.0, local catalogs, the behaviour of: a streaming read from a table with appends,
with an overwrite snapshot and with a delete snapshot (`streaming-skip-overwrite-snapshots`,
`streaming-skip-delete-snapshots`, `stream-from-timestamp`, `streaming-max-files-per-micro-batch`,
`streaming-max-rows-per-micro-batch`); a streaming write in `append` and `complete` output modes with `fanout-enabled`
on and off; a restart from a checkpoint after a committed and after an uncommitted batch; what the snapshot summary
carries per micro-batch. Every design question below is answered from these cells, not from the documentation.

## Design questions to rule, in order

1. **Execution model.** Micro-batch only (Spark's default and what Iceberg's source implements), or continuous
   processing too. Recommendation to test against the oracle: micro-batch only; Iceberg's Spark source has no
   continuous mode.
2. **Triggers.** `ProcessingTime`, `AvailableNow`, `Once`; which are in the first cut.
3. **Checkpoints.** RePark's own format, or one that can also read a Spark checkpoint directory so a running Spark
   job can be migrated without a replay. The offset the Iceberg source stores is a snapshot id plus a position within
   the snapshot's added files.
4. **Exactly-once on write.** A replayed micro-batch must not commit twice; the contract is the query id and epoch in
   the snapshot summary, checked before the commit.
5. **Sources and sinks beyond Iceberg.** v1.6.0 is the connectors minor; the owner's connector reference rule
   (ConnectorX and Arrow ADBC as design and benchmark references) applies to any source this card adds. File and
   rate sources are enough to pin the Iceberg cells; Kafka is a separate decision.
6. **Where it lives.** Rust first: the runtime, the offset log and the commit protocol are Rust; the Python facade
   forwards `readStream` / `writeStream` builders only.

## Rules that bind it

The fork rules, the comment ban on every actor, measure before ruling, recorded-oracle pins with a live leg, a
verification critic before every product-Rust merge. The v1.5.0 gate does not wait on this card.
