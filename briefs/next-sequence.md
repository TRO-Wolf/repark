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
| 1 | **CUTOVER-SCHEMA-1** — nullability derived Spark's way (`CUTOVER-CTAS-REQ-1`, `CUTOVER-DEDUP-SCHEMA-1`); owner ruling 2026-09-04 | Cutover | none (in flight, Muse) | STANDARD <!-- unit id=cutover-schema-1 --> |
| 2 | **DBT-1** — a dbt path for RePark: design ledger, then the thinnest adapter that runs the two gold models | Cutover / dbt | none | STANDARD <!-- unit id=dbt-1 --> |
| 3 | **PERF-DYNFLATTEN-2 residue** — `DYNFLATTEN-LISTNULL-1` / `DYNFLATTEN-READNULL-1`, the two null rows left | Performance | PERF-DYNFLATTEN-2 (built) | STANDARD <!-- unit id=perf-dynflatten-2 --> |
| 4 | **EX batches** — backfill from the 578-name backlog (bounded parallel lane) | Examples | none | STANDARD <!-- unit id=ex-batches --> |
| 5 | **Cutover canary C2–C6** — the shadow week on `<ns>_silver_repark`, then the writer flip | Cutover | CUTOVER-SCHEMA-1, pipeline-side SHADOW-1 | STANDARD <!-- unit id=cutover-inventory --> |
| 6 | **H-3 spill matrix** — Never-OOM truth: which operators spill, and how each fails past the pool | Hardening | none (measure-only) | STANDARD <!-- unit id=h-3-spill --> |
| 7 | **FNP-9/10** — remaining function-parity units after FN-FIX-2 | Function parity | FN-FIX-2 | STANDARD <!-- unit id=fnp-9-10 --> |
| 8 | **DBT-GATES** — M0b/M1b/M2b AWS gates on the 1.0.1 wheel (owner-scheduled) | dbt | — | STANDARD <!-- unit id=dbt-gates --> |

<!-- unit id=cutover-schema-1 -->
**Why CUTOVER-SCHEMA-1 is first.** The owner ruled the two metadata rows are not accepted
differences: readers nullable-by-default, CTAS `required: false`, Spark's `coalesce`/`cast`
nullability. Blast radius is every schema pin; measured first, flipped to Spark's answer.
<!-- /unit -->

<!-- unit id=dbt-1 -->
**Why DBT-1 is queued.** Gold is two dbt models; RePark has no dbt path, so gold cannot move.
Design first (in-process adapter expected), then the thinnest adapter; acceptance = canary C6.
<!-- /unit -->

<!-- unit id=perf-dynflatten-2 -->
**Why PERF-DYNFLATTEN-2 was one candidate, and what it returned.** Only null-mask struct
extract ever cleared 3x, on `struct_d6` alone; Cartesian and the optimizer walks stay closed.
Built and re-measured: `struct_d6`'s isolated null cost is 64.83 ms → 0.01 ms, 0.1x its run's
floor, every bed row set byte-identical, and `DYNFLATTEN-QUALNAME-1` closed as a side effect.
Numbers, controls and do-not list:
[../docs/perf/dynamic-flatten-baseline.md](../docs/perf/dynamic-flatten-baseline.md).
<!-- /unit -->

<!-- unit id=ex-batches -->
**Why EX batches are a parallel lane.** 578 names remain; the 1.0.1 wheel-execute gate is
green. Bounded parallel.
<!-- /unit -->

<!-- unit id=cutover-inventory -->
**Why the canary waits.** The inventory is filed ([../docs/cutover/inventory.md](../docs/cutover/inventory.md))
and the four rulings are taken; C2 starts when the schema unit and the shadow DAG both exist.
<!-- /unit -->

<!-- unit id=h-3-spill -->
**Why the H-3 spill matrix.** Never-OOM is pending this measurement. Pins only; no product
change.
<!-- /unit -->

<!-- unit id=fnp-9-10 ledger=fnp-9- -->
**Why FNP-9/10 is last.** The 2026-08-31 remaining order starts here after FN-FIX-2.
<!-- /unit -->

**Not in this queue (owner-sequenced or owner-gated):** V3-4 and the engine units after it
(V3-3 delivered 2026-08-30 as a measured keep-refusal; its ledger is in `completed/`); DML-A/B/C and Track A W-0. A merged unit leaves this file with no record
here — its ledger is in
[../task/ledgers/archive/](../task/ledgers/archive/map.md) and its PR on `main`.
