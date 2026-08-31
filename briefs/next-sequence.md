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
| 1 | **FNP-15/16** — register every unreachable and declared-deferred family | Function parity | RP-3 departure, or a fork-wait window | STANDARD <!-- unit id=fnp-15-16 --> |
| 2 | **F-Y10-1** — integer overflow raises where Spark raises | Function parity / hardening | the owner's charter gate | STANDARD <!-- unit id=f-y10-1 --> |

<!-- unit id=fnp-15-16 ledger=fnp-15-16- -->
**Why FNP-15/16 follows.** This is the highest-value fork-independent honesty unit: it turns 62
missing or ambiguous names into explicit refusing surfaces with exact registry reasons. It does
not gate v1.0 and yields to a ready v3 unit.
<!-- /unit -->

<!-- unit id=f-y10-1 ledger=f-y10-1- -->
**Why F-Y10-1 is second.** A wrong answer on ordinary addition outranks any missing function,
and FNP-7b's `try_*` family is blocked on it (§7.1 of the parity design). Chartered 2026-08-30
at [../task/ledgers/staging/f-y10-1-int-overflow-ledger.md](../task/ledgers/staging/f-y10-1-int-overflow-ledger.md);
opens on the owner's confirm, fork-independent, any window.
<!-- /unit -->

**Not in this queue (owner-sequenced or owner-gated):** V3-3 (chartered; opens after RP-3
merges) and the engine units after it; DML-A/B/C and Track A W-0. A merged unit leaves this file with no record
here — its ledger is in
[../task/ledgers/archive/](../task/ledgers/archive/map.md) and its PR on `main`.
