# Slate — the next sequence of work (opened 2026-08-21)

**What this is.** One ordered queue across three open tracks, written because the tracks now
interleave and the order between them is a decision rather than an accident. [../STATUS.md](../STATUS.md)
stays the SSOT for state; this file states **sequence and reasoning**, and each unit still earns its
own `task/<unit>-ledger.md` when it starts.

Rolling slate: a unit leaves this file when it merges — mechanically and whole. Its row and its
reasoning carry a `<!-- unit id=… -->` marker and `scripts/ledger_lifecycle.py compact` (run by
`make ledger-archive` and by the departure `move`) removes both once its ledger is filed; nothing
is written here about a unit that has left. The file closes when the queue empties.

## Standing rules for every unit below

Restated for a mixed queue:

1. **Reproduce first.** The behaviour is demonstrated on this tree before anything is edited.
2. **Write the pin and watch it go red.** A pin that was never red proves nothing.
3. **Measure against the oracle, including the incidental controls.** A green pin asserting a
   divergence as parity is the most expensive wrong test in the repo.
4. **Gate alone.** `make preflight` runs by itself and its own exit code is read immediately.
5. **`map.md` in lockstep, in the same commit.** Not a follow-up.
6. **One group at a time**, manual PR, owner merges.
7. **Pickup ritual first, departure edit last.** Per
   [../.agents/skills/compact-context-docs/SKILL.md](../.agents/skills/compact-context-docs/SKILL.md).
   Last commit: STATUS trued up for this unit alone, the ledger `move`d to `completed/` (which
   removes it from this file), `map.md` in lockstep. No departure line for the unit, here or anywhere.

---

## The order, and why it is this order

| # | Unit | Track | Blocked by | Size |
|---|---|---|---|---|
| 1 | **EX batches** — backfill from the 578-name backlog (bounded parallel lane) | Examples | none | STANDARD <!-- unit id=ex-batches --> |
| 2 | **Cutover canary C2–C6** — the shadow week on `<ns>_silver_repark`, then the writer flip | Cutover | CUTOVER-SCHEMA-1, pipeline-side SHADOW-1 | STANDARD <!-- unit id=cutover-inventory --> |
| 3 | **H3-SPILL residue** — `H3-SPILL-NLJ-1` (a caught DataFusion panic where a refusal belongs) and `H3-SPILL-COLLECT-1` (`collect()` past the address space panics, not `MemoryError`) | Hardening | H3-SPILL-1 (measured) | STANDARD <!-- unit id=h-3-spill --> |
| 4 | **DBT-GATES** — M0b/M1b/M2b AWS gates on the 1.0.1 wheel (owner-scheduled) | dbt | — | STANDARD <!-- unit id=dbt-gates --> |

<!-- unit id=ex-batches -->
**Why EX batches are a parallel lane.** 578 names remain; the 1.0.1 wheel-execute gate is
green. Bounded parallel.
<!-- /unit -->

<!-- unit id=cutover-inventory -->
**Why the canary waits.** The inventory is filed ([../docs/cutover/inventory.md](../docs/cutover/inventory.md))
and the four rulings are taken; C2 starts when the schema unit and the shadow DAG both exist.
<!-- /unit -->

<!-- unit id=h-3-spill -->
**Why the H-3 residue.** The matrix is measured (180 cells, 0 aborts, 0 wrong); the two
failure shapes it filed are the remaining Never-OOM work.
<!-- /unit -->

**Not in this queue (owner-sequenced or owner-gated):** V3-4 and the engine units after it
(V3-3 delivered 2026-08-30 as a measured keep-refusal; its ledger is in `completed/`); DML-A/B/C and Track A W-0. A merged unit leaves this file with no record
here — its ledger is in
[../task/ledgers/archive/](../task/ledgers/archive/map.md) and its PR on `main`.

