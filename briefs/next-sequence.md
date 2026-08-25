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

Restated because a mixed queue makes it easy to assume the previous campaign's contract carried:

1. **Reproduce first.** The behaviour is demonstrated on this tree before anything is edited.
2. **Write the pin and watch it go red.** A pin that was never red proves nothing.
3. **Measure against the oracle, including the incidental controls.** A green pin asserting a
   divergence as parity is the most expensive wrong test in the repo.
4. **Gate alone.** `make preflight` runs by itself and its own exit code is read immediately.
5. **`map.md` in lockstep, in the same commit.** Not a follow-up.
6. **One group at a time**, manual PR, owner merges.
7. **Pickup ritual first, departure edit last.** First act of a unit: fetch, confirm the prior
   unit's PR merged and that the local base carries its departure edit, `make ledger-archive`
   (files the prior unit's ledger, takes it off this file, files any closed campaign, runs the
   gate; zero tokens), run the drift checks (`make check-map-sync`, `make check-ledgers`), and
   compact the context docs **against the just-merged delta only**, as a docs-only first commit.
   Last commit of the unit: STATUS trued up for what this unit changed and nothing else (a
   campaign the owner ruled closed gets `state=closed` on its marker, no prose), the unit's
   ledger `move`d from `task/ledgers/staging/` to `completed/` — which removes it from this
   file — `map.md` in lockstep. No departure line for the unit, here or anywhere.

---

## The order, and why it is this order

| # | Unit | Track | Blocked by | Size |
|---|---|---|---|---|
| 1 | **V3E-4** — refs + time travel on v3; expiry/orphans with real work | v3 evidence | — (oracle named: pyspark-4.1.2+iceberg-1.11.0) | M <!-- unit id=v3e-4 --> |
| 2 | **V3E-5** — the nightly-oracle v3 leg | v3 evidence | the scoped `.github/` grant (below) | S <!-- unit id=v3e-5 --> |

<!-- unit id=v3e-5 ledger=v3e-5- -->
**Lane A — the v3 evidence intake (owner-chartered 2026-08-24).** Five measure-first units
against the north-star matrix
([../task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md](../task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md)
§3); none depends on fork work. The owner's three rulings, all dated 2026-08-24:

1. **Lane A is the next sequence.** V3E-1 + V3E-2 merged as
   [#235](https://github.com/TRO-Wolf/repark/pull/235): COW DML on an adopted v3
   table committed and **reassigned** lineage (registry `V3-COW-1`); Spark preserves
   `_row_id` on DELETE. **Ruled 2026-08-25: guarded** — the row now refuses on both doors. The v3
   maintenance oracle is PySpark 4.1.2 + Iceberg 1.11.0. **V3E-4 is Lane A's next unit** and
   #1 on the queue (DL-4 delivered 2026-08-25).
2. **Table encryption keys are a dated DECLARED exclusion from the v1.0 gate.** Registry
   `ENC-1` ([#235](https://github.com/TRO-Wolf/repark/pull/235)).
3. **A one-time scoped `.github/` grant** for V3E-5 only: add the v3 fixture leg to the nightly
   parity workflow, in its own reviewable PR. No other workflow edit rides it.

The **fork lane runs in parallel and is owner-run** via
[../task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md](../task/roadmap/mid-term/iceberg-rust-handoff-2026-08-23.md)
— suggested order F-16 (small; repark's C-011 pin flipping red is the acceptance signal) →
F-13 (the DV write path, gates V3-3) → F-14. Each fork landing returns here as a repin unit
(RP-2, …). V3-3 and later engine units stay owner-sequenced and are **not** in this queue.
<!-- /unit -->

**Not in this queue (owner-sequenced or owner-gated):** V3-3 (deletion-vector writes; gated on
fork F-13) and the engine units after it; S3 Tables MOR (intake "MW-4b", gated on OD-3b); DML-A/B/C
and Track A W-0. A merged unit leaves this file with no record here — its ledger is in
[../task/ledgers/archive/](../task/ledgers/archive/map.md) and its PR on `main`.
