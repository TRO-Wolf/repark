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
| 1 | **F-24** — min-input-files floor on the fork (`B-MOR-3-FLOOR-1`) | Format-v3 / fork | none | STANDARD <!-- unit id=f-24 --> |
| 2 | **F-25 → RP-10** — stop `validate_fresh_dvs_only` once every DV key is known (`PERF-DVCLOSE-STMT-1`) | Format-v3 / fork | F-24 (fork lane may overlap) | STANDARD <!-- unit id=rp-10 --> |
| 3 | **PERF-SCAN-1** — collapse the 3× `plan_files` identity-DELETE scan (`PERF-SCAN-3PASS-1`) | Performance | none | STANDARD <!-- unit id=perf-scan-1 --> |
| 4 | **SQL-HARDEN-1** — cutover SQL shapes vs Spark on Glue + S3 Tables | Hardening / H-2 | none | STANDARD <!-- unit id=sql-harden-1 --> |
| 5 | **FN-FIX-2** — `FN-INITCAP-1`, `FN-CHR-1`, `FN-TRIM-CHARS-1`, `FN-ELT-1`, `FN-REGEX-POSIX-1`, `FN-LIKE-ESCEND-1` | Function parity | none | STANDARD <!-- unit id=fn-fix-2 --> |
| 6 | **dynamicFlatten measure** — the three H-3 candidates in the 2026-08-31 intake | Performance | none (measure-only) | STANDARD <!-- unit id=dynamic-flatten-measure --> |
| 7 | **EX batches** — backfill from the 578-name backlog (bounded parallel lane) | Examples | none | STANDARD <!-- unit id=ex-batches --> |
| 8 | **Cutover inventory** — which workloads move, in what order, under single-writer-per-table | Cutover | SQL-HARDEN-1 | STANDARD <!-- unit id=cutover-inventory --> |
| 9 | **H-3 spill matrix** — Never-OOM truth: which operators spill, and how each fails past the pool | Hardening | none (measure-only) | STANDARD <!-- unit id=h-3-spill --> |
| 10 | **FNP-9/10** — remaining function-parity units after FN-FIX-2 | Function parity | FN-FIX-2 | STANDARD <!-- unit id=fnp-9-10 --> |

<!-- unit id=f-24 -->
**Why F-24 is first.** Spark's `MIN_INPUT_FILES_DEFAULT = 5` is a fork floor, not a RePark
planner patch; it unblocks F-25 → RP-10.
<!-- /unit -->

<!-- unit id=rp-10 ledger=rp-10- -->
**Why F-25 → RP-10 follows.** `PERF-DVCLOSE-STMT-1`: F-25 stops the commit walk once every DV
key is known; RP-10 consumes that pin.
<!-- /unit -->

<!-- unit id=perf-scan-1 -->
**Why PERF-SCAN-1.** The identity DELETE still runs `plan_files` three times. Independent of
the fork lane.
<!-- /unit -->

<!-- unit id=sql-harden-1 -->
**Why SQL-HARDEN-1.** H-2 scoped to cutover SQL vs Spark on Glue and S3 Tables; feeds the
cutover inventory.
<!-- /unit -->

<!-- unit id=fn-fix-2 -->
**Why FN-FIX-2 before FNP-9/10.** Six filed rows the example campaign left on the backlog.
<!-- /unit -->

<!-- unit id=dynamic-flatten-measure -->
**Why measure dynamicFlatten now.** Three H-3 candidates in the 2026-08-31 intake; measure
before any implementation unit.
<!-- /unit -->

<!-- unit id=ex-batches -->
**Why EX batches are a parallel lane.** 578 names remain; the 1.0.1 wheel-execute gate is
green. Bounded parallel.
<!-- /unit -->

<!-- unit id=cutover-inventory -->
**Why the cutover inventory waits on SQL-HARDEN-1.** Workloads and rollback need the measured
SQL shapes first.
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
