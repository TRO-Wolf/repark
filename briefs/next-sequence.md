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
<!-- unit id=v3-cov -->
| 1 | **V3-COV** — full v3 statement coverage against PySpark | v3 (v1.0 gate) | nothing | L |
<!-- /unit -->

<!-- unit id=v3-cov -->
**Why V3-COV is first, and why it is here at all.** V1-GATE (2026-09-03) audited every §3 row of
the north star and found one v1.0 requirement with no row and no discharge: §2 pillar 4's *full
statement-coverage comparison against PySpark on v3 tables*. The nightly v3 leg is ten cells over
two fixtures, not the statement matrix, and the tree carries no statement-coverage harness at any
format version — so the unit builds the matrix (every DML and DDL statement and every
`CALL system.*` procedure, on v3 tables, compared against the pinned PySpark 4.1.2 +
Iceberg 1.11.0 oracle) and lands each divergence as a registry row rather than prose. It is the
last engineering item between the audited gate and the tag; everything else the gate owes is an
owner line.
<!-- /unit -->

**Not in this queue (owner-sequenced or owner-gated):** V3-4 and the engine units after it
(V3-3 delivered 2026-08-30 as a measured keep-refusal; its ledger is in `completed/`); DML-A/B/C and Track A W-0. A merged unit leaves this file with no record
here — its ledger is in
[../task/ledgers/archive/](../task/ledgers/archive/map.md) and its PR on `main`.
