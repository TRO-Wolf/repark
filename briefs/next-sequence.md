# Slate — the next sequence of work (opened 2026-08-21)

**What this is.** One ordered queue across three open tracks, written because the tracks now
interleave and the order between them is a decision rather than an accident. [../STATUS.md](../STATUS.md)
stays the SSOT for state; this file states **sequence and reasoning**, and each unit still earns its
own `task/<unit>-ledger.md` when it starts.

Rolling slate: a unit leaves this file when it merges, and the file closes when the queue empties.

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
   (files the prior unit's ledger; zero tokens), run the drift checks (`make check-map-sync`,
   `make check-ledgers`), and compact the context docs **against the just-merged delta only**,
   as a docs-only first commit. Last commit of the unit: the departure edit to this file, STATUS
   trued up for what this unit changed and nothing else, the unit's ledger `move`d from
   `task/ledgers/staging/` to `completed/`, `map.md` in lockstep.

---

## The order, and why it is this order

| # | Unit | Track | Blocked by | Size |
|---|---|---|---|---|
| 1 | **DL-4** — the live documents carry only live state ([charter](../task/ledgers/staging/dl-4-live-doc-compaction-charter-ledger.md)) | document lifecycle | — | M <!-- unit id=dl-4 --> |
| 2 | **V3E-4** — refs + time travel on v3; expiry/orphans with real work | v3 evidence | — (oracle named: pyspark-4.1.2+iceberg-1.11.0) | M <!-- unit id=v3e-4 --> |
| 3 | **V3E-5** — the nightly-oracle v3 leg | v3 evidence | the scoped `.github/` grant (below) | S <!-- unit id=v3e-5 --> |

<!-- unit id=dl-4 -->
**Why DL-4 goes ahead of V3E-4 (chartered 2026-08-25).** A faithful walk of the read path
for a fresh work group costs ~97k tokens before a ledger exists, ~35k of it live signal; the
deficit is closed-campaign diary on `STATUS.md` (a 36 kB "Active workstreams") and merged-unit
obituaries in this file, and it is paid by **every Actor and Critic a unit spawns**, not once
per session. DL-4 moves the diaries to `docs/history/`, makes merged units leave this file with
no residue, and arms a byte ratchet so the two files cannot regrow unnoticed. Landing it first
means V3E-4's agents onboard on the compacted files. No engine code; one script, one gate.
<!-- /unit -->

<!-- unit id=v3e-5 ledger=v3e-5- -->
**Lane A — the v3 evidence intake (owner-chartered 2026-08-24).** Five measure-first units
against the north-star matrix
([../task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md](../task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md)
§3); none depends on fork work. The owner's three rulings, all dated 2026-08-24:

1. **Lane A is the next sequence.** V3E-1 + V3E-2 merged as
   [#235](https://github.com/TRO-Wolf/repark/pull/235): COW DML on an adopted v3
   table commits and **reassigns** lineage (registry `V3-COW-1`, BACKLOG); Spark preserves
   `_row_id` on DELETE. **Guard-or-not is a second owner ruling on those numbers.** The v3
   maintenance oracle is PySpark 4.1.2 + Iceberg 1.11.0. **V3E-4 is Lane A's next unit** —
   queue #2 behind DL-4 since 2026-08-25.
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
