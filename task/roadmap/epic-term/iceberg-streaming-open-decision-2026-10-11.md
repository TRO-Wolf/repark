# Iceberg streaming: the open decision (owner-held), 2026-10-11

**Status:** OPEN. Owner-held. Nothing in this file is ruled. The owner wants to understand the
streaming architecture in full before any further ruling on it, so the streaming track takes no
new product work until the owner closes this file. The orchestrator pivots to the v1.5.5 items.
Filed by the Frontier delegate from the MB-4 Q1 evaluation of 2026-10-11 and the owner's
follow-up questions the same day.

**Authority order while this is open:** ruled rows (D1 to D5 on MB-4, O-1 to O-10 on the packet)
stand as written; the North Star
([cdc-microbatch-north-star-2026-10-05.md](cdc-microbatch-north-star-2026-10-05.md)) answers what
they do not; every proposal below is a lean, not a rule.

## 1. The question that opened this: MB-4 Q1

The eighth verify of PR #1011 (`feat/mb-4-facade`, head `48e961ef`) passed with no S1 and no
S2 and left one owner question (fold 9 of
[mb-4-foreach-eo-ledger.md](../../ledgers/staging/mb-4-foreach-eo-ledger.md) on that branch).

A query name that ever ran through `foreachBatch` is walked on the `toTable` door too (ruling
D2). The trace that says so is one marker on one snapshot: the first `toTable` stamp written over
the starting mark. Later `toTable` stamps do not record it. Routine `expire_snapshots` deletes
that one snapshot while the newer stamps remain, and from then on the name follows the plain
`toTable` rule: a foreign commit under its stamp passes silently. Measured on the verify's
release build: silent 96 of 96 deterministic restarts over a stray, 277 of 480 foreign commits
under a stamp in a raced run. Not worse than main, which has no check on that door. Nobody
chose the lapse. An expiry that obeys the maintenance MUST of ruling D3 (keep the newest two
stamps) still triggers it, because the two newest stamps are not the one that carries the trace.

| Option | What it is | Frontier reading |
|---|---|---|
| A. Keep as built | The limit is one snapshot; the prose states it | Flink-equivalent after the first clean stamp, but the exclusive-sink ruling D3 lapses by accident |
| B1. Carry, narrow | Later `toTable` stamps of a mark-only name record the key | Two kinds of carried name keep two different limits |
| B2. Carry, full | Every stamp of a carried name records a presence key; fenced on both doors while any stamp of the name remains | One rule; closes the silent path; the lean |

**Frontier lean, not adopted:** B2 with a separate presence-only key written at the one site
that builds stamp entries (no walk at stamp time, no new catalog read), as its own unit after
#1011 and before the release that ships MB-4; the operator's exit is a new query name, which
the stray remedies already print with its start-after position (C-033). Pins the unit would
need: the step-5 expiry case refused on both restart doors, 0 foreign commits under a stamp in
a raced run after the expiry, the `toTable`-only battery still 0 of N, another name's key not
carrying this name, every stamp expired releases the name, the forge battery re-run with the
new key, and the D5 timing record standing because no read is added.

## 2. What the architecture does today, in plain words

- A streaming query writes to an Iceberg sink in batches. Every batch's commit carries a stamp
  in the snapshot summary: query name, run, epoch, generation, and the source offsets, in the
  same atomic commit as the data (NS-1, NS-2). On restart the driver reads the sink and nothing
  else (NS-4).
- **`toTable`:** the engine writes the batch itself, so it knows what it wrote. It skips an
  epoch it already committed and ignores other writers, as Spark does.
- **`foreachBatch`:** the user's code writes the batch, so after a crash the engine cannot tell
  its own half-finished batch from another writer. Before stamping, it walks the sink's main
  lineage back to a floor; any snapshot without a stamp is a stray and the commit is refused
  with remedies (roll back to the named stamp, or restart under a new name at a printed
  start-after position). The declared sink is therefore exclusive for as long as the name lives
  (ruling D3, the maintenance card on the branch).
- **The starting mark:** written at a `foreachBatch` name's first start, before any batch,
  inside the offsets property. It records the head at first start, which is the floor under the
  first stamp, so a table that held rows before the query started is not refused (fold 5).
- **Retention:** the newest stamp is the resume point; expiring it refuses loudly. The floor
  under the newest stamp is the previous stamp or the recorded head; expiring both runs the
  walk into pre-stream history and refuses a healthy restart. Hence the D3 MUST: keep the
  newest two stamps and everything between.
- **Another name's stamp** is not a stray. It only marks the stretch as shared, which makes the
  discard remedy unproven.
- **Forgery:** caller-supplied keys under `repark.cdc.` and `spark.sql.streaming.` are refused
  at the stamp site.

## 3. Edge cases catalogued (reading of the branch, not measured)

**Taking over a table Spark already wrote**

- Old Spark history is below the floor and never a stray. Spark's own streaming stamps are not
  RePark stamps.
- Once a fenced name starts, any Spark commit (batch, streaming, compaction, manifest rewrite)
  is a stray. A gradual cutover with both engines writing one sink is only possible through a
  `toTable`-only name.
- Spark's nightly `expire_snapshots` is the hazard: it adds no snapshot, but it can delete the
  stamps the walk floors on. Expiry for a fenced sink must move to the sanctioned path, or
  Spark's retention must keep two RePark stamps alive.

**A source vendor's errors**

- Corrections that arrive through capture into Bronze as appends flow through normally.
- A delete or overwrite snapshot inside an unconsumed Bronze window refuses on restart (NS-6).
- A hand repair of Silver is a stray on a fenced name: stop, fix, restart under a new name.
- Semantic duplicates from the vendor are the MERGE's business keys, not the pipeline's.

**Stop, fix Bronze and Silver, restart**

- Works, under three conditions: restart under a new query name (a fenced name refuses its old
  name over the hand fix, by design); the last batch must have ended on a source snapshot
  boundary, or the driver prints no start-after position and the only printed remedy is a
  rollback that undoes the fix; Bronze fixes must be appends or sit behind the restart position.

**Not verified:** whether a rewrite of Bronze history already consumed is ignored or refused;
what happens when Bronze expiry removes the snapshot the offsets point at; every takeover claim
above. Each is one probe.

## 4. Hardening proposals (not carded; each needs a measured acceptance)

1. **Every legitimate write is stamped.** Compaction, manifest rewrite, expiry and operator
   repairs run through the engine as stamped operations under the live name, so every
   unstamped write is foreign by definition. Extends the maintenance card's direction.
2. **Tag-protected floors.** Iceberg expiry keeps snapshots a tag or branch references, in every
   engine. The engine tags the newest two stamps and the starting head of each live name, so a
   foreign maintenance job cannot erase the query's memory. Verify first that the Rust Iceberg
   crate can write refs in a transaction.
3. **A planned stop drains to a snapshot boundary** (NS-16 names the draining state), so the
   restart recipe is always printable.
4. **Source-range identity in the stamp:** Bronze table UUID, snapshot range, sequence numbers
   of the files read, so a rewrite of consumed history is detectable on restart. Declared
   Bronze tables refuse non-append DML through RePark's own doors.
5. **Cross-process lease** with a heartbeat, so a second driver or a zombie refuses before its
   first batch; generation fencing (NS-5) keeps covering the commit.
6. **Doctor and repair commands:** one read-only command that prints a sink's state (live
   names, floors, strays, protected snapshots, what retention would break) and one that applies
   a printed remedy exactly.
7. **A standing crash matrix:** the MB-4 race scripts as a scheduled battery that kills the
   driver at every commit point, races a foreign writer at every point, expires at every
   retention value and runs the takeover scenario against the real Spark on the build box.
   Every incident adds a cell that never leaves.

**Out of reach:** another engine's writes can be detected, never prevented, unless the catalog
restricts the sink to the stream's credentials (deployment, one sentence in the docs). Wrong
vendor data is semantics. Multi-sink atomicity does not exist in Iceberg; per-sink epochs with
idempotent replay is the honest answer, and Flink's.

**Suggested order if adopted:** the carry unit, tag-protected floors inside the maintenance
card, the draining stop, the takeover battery as a scheduled gate, doctor and repair, then the
lease and source-range identity.

## 5. Open decisions for the owner

| Id | Decision | Frontier lean |
|---|---|---|
| OD-1 | MB-4 Q1: keep, carry narrow or carry full | carry full, presence key, own unit after #1011 |
| OD-2 | #1011 while this file is open: hold as draft, or merge on its passed verify | hold; it is built on D2 and D3, which the owner wants to re-read |
| OD-3 | MB-5 and any further streaming slice | not started until this file closes |
| OD-4 | Which of the seven hardening proposals become cards, and in what order | the order in section 4 |
| OD-5 | The takeover battery as a card beside the maintenance card | yes, measured against the real Spark on the box |
| OD-6 | The draining stop as a card | yes, it removes the one sequence that loses an operator's fix |
| OD-7 | Expiry ownership for fenced sinks: sanctioned path only, or tag-protected floors | tags, after the refs check |
| OD-8 | The stricter-than-Flink exclusive sink on the `toTable` door of a carried name | keep; it is D3, and the question is only whether it may lapse |

## 6. Reading path for the owner

In this order: the North Star
([cdc-microbatch-north-star-2026-10-05.md](cdc-microbatch-north-star-2026-10-05.md)); the packet
([../../wo/microbatch/packet.md](../../wo/microbatch/packet.md)) and the design sketch
([../../wo/microbatch/mb-design-2026-10-06.md](../../wo/microbatch/mb-design-2026-10-06.md));
the MB-4 order ([../../wo/microbatch/mb-4-facade.md](../../wo/microbatch/mb-4-facade.md)); on
the branch of #1011: the ledger's folds 2, 3, 5, 6, 7, 8 and 9, the `sink_offsets` map
(`crates/repark-iceberg/src/write/sink_offsets/map.md`), the walk in
`crates/repark-iceberg/src/write/sink_offsets/lineage.rs`, and the maintenance card
(`task/roadmap/mid-term/mb-sink-maintenance-path-1-card-2026-10-10.md`). Section 2 above is the
short form of all of it.

## 7. Closing this file

The owner closes it with dated rulings on OD-1 to OD-8, written here or in the ledger, and the
map row changes from open to ruled. Until then the streaming track is paused and nothing in
sections 1, 4 or 5 is built.
