# map — task/

## Purpose

Working state for **current** work: the rules in force, the ledger of each unit in flight, and the
acceptance inputs that gates still read. Finished **phases and campaigns** do not accumulate here —
they are archived under [../docs/history/](../docs/history/map.md) once their rules have been
promoted to a current document (mid-campaign phase promotions are allowed; see hardening-h1).

Current state (release, delivered surface, what happens next) is **[../STATUS.md](../STATUS.md)**,
not this directory.

## Contents

- [g7b-decimal-rust-ledger.md](g7b-decimal-rust-ledger.md) — **G-7b / W-1 in flight:** 10
  bit-exact `Decimal128` i128 pins on the Spark door + 2 cross-door ANSI/Spark rows (Python
  corpus cited, not edited). Continues archived
  [../docs/history/hardening-h1/g7-decimal-ledger.md](../docs/history/hardening-h1/g7-decimal-ledger.md)
  §9.
- [todo.md](todo.md) — a **pointer only**: the live backlog is [../STATUS.md](../STATUS.md), and a
  unit's working plan is its own ledger. The file keeps its name because live code, docs and one
  runtime error message cite this path.
- [lessons.md](lessons.md) — DO / DO-NOT rules in force (append date-stamped; supersede, don't
  delete). Seeded 2026-08-06 from the private v1 repository.
- [g3e8-guard-ledger.md](g3e8-guard-ledger.md) — unit ledger for **G3-E8 (guard-first half)**:
  the DELETE/UPDATE **subquery-predicate valve**. Localizes a silent-data-loss defect (a subquery
  in a DML `WHERE` was lost at DataFusion's DML planning boundary and matched EVERY row), closes
  the window with a refuse-loud valve in BOTH SQL doors, and records the live-Spark oracle the
  future fix will need (`python/repark/tests/test_dml_subquery_parity.py`, 10 rows). Carries the
  statement-form matrix, the guard decisions (incl. the deliberate over-refusal of uncorrelated
  scalar subqueries), provocation transcripts, and §6 registry rows READY TO PASTE but not landed.
  Recon lives planning-side (it names fork + DataFusion internals).
- [metrics.md](metrics.md) — the **process metrics ledger**: one section per retrospective, the
  eight-metric set the SEPMO retrospective contract fixes (findings per cycle, cycles to
  convergence, noise ratio, coverage misses, escaped defects by origin, LIGHT-path escapes, flags
  shipped, environment drift events). Append a section per campaign; never rewrite an earlier one.
  Created 2026-08-10 with the Front-Door campaign's numbers.
- [w3-joins-ledger.md](w3-joins-ledger.md) — **in flight (W-3 / H-2 gap G4):** joins differential
  corpus vs live Spark (value AND Arrow type AND nullability); record driver; §6 paste-true
  registry rows. Does **not** edit `docs/spark-sql-iceberg-parity.md`.
- [w4-windows-ledger.md](w4-windows-ledger.md) — **live** W-4 window-function corpus (gap G5) unit ledger
- [port/](port/map.md) — **live acceptance inputs**: the deferred-test manifest and its
  reconciliation rule ([port/deferred-tests.md](port/deferred-tests.md)), the machine-readable
  deferral allowlist ([port/deferred-python-tests.txt](port/deferred-python-tests.txt)) and its
  mirror additions ledger ([port/added-python-tests.txt](port/added-python-tests.txt)). The census
  comparator still subtracts these, so they are not history.
- [census/](census/map.md) — **evidence**: the recorded census runs, `baseline-fc3f48102/` (the port
  pin) and `v2-a5be8a7/` (the acceptance run). Never hand-edited; a re-run replaces a whole
  directory in one commit.

- [wc-check-lib-rs-stale-ledger.md](wc-check-lib-rs-stale-ledger.md) — WC: `check_lib_rs` stale-EXCEPTIONS crate-key fail-closed (G-8 mold backport).
- [xc-product-statements-ledger.md](xc-product-statements-ledger.md) — **XC (docs):** G3-E3/E4/E7
  product statements → [`docs/design/product-contract.md`](../docs/design/product-contract.md)
  + design/docs map lockstep; cite inventory; B6 proposals only.

## Where the closed campaigns' ledgers went

The seventeen `p1*` / `p2*` / `p3*` unit ledgers, the four phase briefs and the port's `todo.md`
execution log moved to [../docs/history/port-v2/](../docs/history/port-v2/map.md) on **2026-08-09**
(Front-Door FD-4), keeping their basenames. A citation of `task/p3e-facade-ledger.md` — a few
survive in Rust doc comments — means
[../docs/history/port-v2/p3e-facade-ledger.md](../docs/history/port-v2/p3e-facade-ledger.md), and so
on. Nothing was lost in the move; the audit is
[../docs/history/port-v2/promotion-ledger.md](../docs/history/port-v2/promotion-ledger.md).

`fd3-ledger.md` left the same way on **2026-08-10**, at the Front-Door campaign's close-out: it is
[../docs/history/frontdoor/fd3-ledger.md](../docs/history/frontdoor/fd3-ledger.md), alongside that
campaign's design, slate and retrospective. Its audit is the retrospective's "Promotion check"
section, and the one rule it stranded — set a repo-local git identity before the first commit — was
promoted into [lessons.md](lessons.md) (2026-08-10) **before** the move.

**H-1 phase ledgers** (and the parallel G/N corpus units delivered through the H-1 close gate,
repark #35–#46) moved on **2026-08-11** by **G-9** — a **mid-campaign** phase promotion, not a
campaign close-out. Basenames kept under
[../docs/history/hardening-h1/](../docs/history/hardening-h1/map.md):

| Former `task/` path | Now |
|---|---|
| `task/h1d-ledger.md` | [../docs/history/hardening-h1/h1d-ledger.md](../docs/history/hardening-h1/h1d-ledger.md) |
| `task/h1a-ledger.md` | [../docs/history/hardening-h1/h1a-ledger.md](../docs/history/hardening-h1/h1a-ledger.md) |
| `task/h1c-ledger.md` | [../docs/history/hardening-h1/h1c-ledger.md](../docs/history/hardening-h1/h1c-ledger.md) |
| `task/h1b-ledger.md` | [../docs/history/hardening-h1/h1b-ledger.md](../docs/history/hardening-h1/h1b-ledger.md) |
| `task/g4-tests-split-ledger.md` | [../docs/history/hardening-h1/g4-tests-split-ledger.md](../docs/history/hardening-h1/g4-tests-split-ledger.md) |
| `task/g4-artifacts/` | [../docs/history/hardening-h1/g4-artifacts/](../docs/history/hardening-h1/g4-artifacts/map.md) |
| `task/g5-sweep-ledger.md` | [../docs/history/hardening-h1/g5-sweep-ledger.md](../docs/history/hardening-h1/g5-sweep-ledger.md) |
| `task/g6-chores-ledger.md` | [../docs/history/hardening-h1/g6-chores-ledger.md](../docs/history/hardening-h1/g6-chores-ledger.md) |
| `task/g7-decimal-ledger.md` | [../docs/history/hardening-h1/g7-decimal-ledger.md](../docs/history/hardening-h1/g7-decimal-ledger.md) |
| `task/n2-merge-ledger.md` | [../docs/history/hardening-h1/n2-merge-ledger.md](../docs/history/hardening-h1/n2-merge-ledger.md) |
| `task/g8-file-size-ledger.md` | [../docs/history/hardening-h1/g8-file-size-ledger.md](../docs/history/hardening-h1/g8-file-size-ledger.md) |

A citation of `task/h1d-ledger.md` (or any row above) means the matching file under
`docs/history/hardening-h1/`. The audit is
[../docs/history/hardening-h1/promotion-ledger.md](../docs/history/hardening-h1/promotion-ledger.md).

## Live unit ledgers

| Ledger | Unit |
|---|---|
| [n2b-merge-followup-ledger.md](n2b-merge-followup-ledger.md) | **N-2b / W-2** MERGE follow-up — items 1+4 in PR #50; items 2+3 (lifecycle live + 13 tz live scenarios) in the second PR. Full N-2b closed only when **both** PRs land. |

## I want to...

| ...do this | go to |
|---|---|
| See the live backlog / what happens next | [../STATUS.md](../STATUS.md) |
| Check a rule before acting | [lessons.md](lessons.md) |
| See how a data-loss defect is localized, valved and oracled before it is fixed | [g3e8-guard-ledger.md](g3e8-guard-ledger.md) |
| Find out why a `DELETE`/`UPDATE` with a subquery `WHERE` is refused | [g3e8-guard-ledger.md](g3e8-guard-ledger.md) §2 (the matrix) + §3 (D-3, the deliberate over-refusal) |
| Start a new unit's ledger | copy the shape of the archived [h1d-ledger.md](../docs/history/hardening-h1/h1d-ledger.md) (or [fd3-ledger.md](../docs/history/frontdoor/fd3-ledger.md)); link it from this map in the same commit |
| See how a divergence gets declared, pinned and mirrored | [../docs/history/hardening-h1/h1d-ledger.md](../docs/history/hardening-h1/h1d-ledger.md), then [../docs/spark-sql-iceberg-parity.md](../docs/spark-sql-iceberg-parity.md) §6 |
| Read why the session timezone is a build-time knob with one spelling | [../docs/history/hardening-h1/h1a-ledger.md](../docs/history/hardening-h1/h1a-ledger.md) |
| Read how timestamp extraction came to honor it, and what the fix deliberately did NOT close | [../docs/history/hardening-h1/h1a-ledger.md](../docs/history/hardening-h1/h1a-ledger.md) "§ Split B" |
| See how an open question gets FIXED instead of declared (and why a fixed defect gets no row) | [../docs/history/hardening-h1/h1c-ledger.md](../docs/history/hardening-h1/h1c-ledger.md) + [../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md](../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md) |
| Find out why a `__repark_tt_*` name is on a session, and which of its three producers put it there | [../docs/history/hardening-h1/h1b-ledger.md](../docs/history/hardening-h1/h1b-ledger.md), then [../crates/repark-spark/src/map.md](../crates/repark-spark/src/map.md) `## Debug` |
| See what a two-mutation acceptance looks like (and why the second mutation is the one that matters) | [../docs/history/hardening-h1/h1b-ledger.md](../docs/history/hardening-h1/h1b-ledger.md) §7c/§7d |
| Read the MERGE INTO differential corpus (gap G3) ledger + registry paste rows | [../docs/history/hardening-h1/n2-merge-ledger.md](../docs/history/hardening-h1/n2-merge-ledger.md) |
| Read the decimal128 differential corpus ledger (Python half) | [../docs/history/hardening-h1/g7-decimal-ledger.md](../docs/history/hardening-h1/g7-decimal-ledger.md) |
| Read the decimal128 Rust half (G-7b pins + cross-door) | [g7b-decimal-rust-ledger.md](g7b-decimal-rust-ledger.md) |
| Read the window-function differential corpus (gap G5) ledger | [w4-windows-ledger.md](w4-windows-ledger.md) |
| See why a dependency edge or a manifest field is gated, and the proofs it fires | [../docs/history/frontdoor/fd3-ledger.md](../docs/history/frontdoor/fd3-ledger.md) |
| File a retrospective's metrics | [metrics.md](metrics.md) — append a section, never rewrite one |
| See which v1 tests are deferred, and why | [port/deferred-tests.md](port/deferred-tests.md) |
| Feed the census comparator its allowlists | [port/map.md](port/map.md) |
| Run or compare a census | [../docs/port/census.md](../docs/port/census.md) |
| Read the port's record (briefs, unit ledgers, retrospectives) | [../docs/history/port-v2/README.md](../docs/history/port-v2/README.md) |
| Read the H-1 phase archive (mid-campaign) | [../docs/history/hardening-h1/README.md](../docs/history/hardening-h1/README.md) |
| Read the port plan the phases executed | [../docs/port/PLAN.md](../docs/port/PLAN.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../AGENTS.md](../AGENTS.md) (the durable contract) and [../STATUS.md](../STATUS.md)
  (current state); this directory holds the moving parts of work in flight.
- Unit ledgers: one `<unit>-ledger.md` per delivered unit, with gate evidence and provocation proofs
  per [../docs/testing.md](../docs/testing.md), linked from this map in the same commit. When a
  **phase or campaign** closes (or is deliberately mid-campaign-promoted), its ledgers are archived
  under [../docs/history/](../docs/history/map.md) after a promotion audit — never deleted.

## Debug

- `pg-integration-report.md` may appear here untracked: `python/repark/tests/test_pg_acceptance.py`
  writes it (CWD-relative) on every facade run. It is gitignored on purpose — a run output, not a
  record. Do not `git add` it.
- If work and trackers disagree, the code is truth — update the tracker.
- A link into `task/p*-ledger.md` or `task/fd3-ledger.md` fails: see "Where the closed campaigns'
  ledgers went" above — same basename, under [../docs/history/](../docs/history/map.md).
- A link into `task/h1*-ledger.md`, `task/g4-*-ledger.md`, `task/g5-sweep-ledger.md`,
  `task/g6-chores-ledger.md`, `task/g7-decimal-ledger.md`, `task/n2-merge-ledger.md`,
  `task/g8-file-size-ledger.md`, or `task/g4-artifacts/` fails the same way: those moved to
  [../docs/history/hardening-h1/](../docs/history/hardening-h1/map.md) on **2026-08-11** (G-9).
- **H-1 phase ledgers were promoted mid-campaign** to
  [../docs/history/hardening-h1/](../docs/history/hardening-h1/map.md) (2026-08-11). **H-2+ unit
  ledgers re-accumulate here** until the next promotion. Empty-of-ledgers is again a valid steady
  state between units (the campaign continues; only the closed H-1 phase record left).
- Looking for a backlog item that is not in [../STATUS.md](../STATUS.md)? Check
  [../docs/history/port-v2/promotion-ledger.md](../docs/history/port-v2/promotion-ledger.md) or
  [../docs/history/hardening-h1/promotion-ledger.md](../docs/history/hardening-h1/promotion-ledger.md)
  — if it was live at archival, that table says where it went.
