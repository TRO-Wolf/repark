# map — task/

## Purpose

Working state for **current** work: the rules in force, the ledger of each unit in flight, and the
acceptance inputs that gates still read. Finished campaigns do not accumulate here — they are
archived under [../docs/history/](../docs/history/map.md) once their rules have been promoted to a
current document.

Current state (release, delivered surface, what happens next) is **[../STATUS.md](../STATUS.md)**,
not this directory.

## Contents

- [todo.md](todo.md) — a **pointer only**: the live backlog is [../STATUS.md](../STATUS.md), and a
  unit's working plan is its own ledger. The file keeps its name because live code, docs and one
  runtime error message cite this path.
- [lessons.md](lessons.md) — DO / DO-NOT rules in force (append date-stamped; supersede, don't
  delete). Seeded 2026-08-06 from the private v1 repository.
- [h1d-ledger.md](h1d-ledger.md) — **V2 Engine Hardening H-1d** (the divergence registry): the
  re-verified citation inventory, the eight design decisions (including D-6, the ruling that the
  live-tier `DISCLOSURES` list becomes a machine-checked mirror of the registry's live-mirrored
  rows), the gate results, and the mirror check's provocation proofs. Sweep queue historically
  seeded here; **closed by G-5** (see [g5-sweep-ledger.md](g5-sweep-ledger.md)).
- [g5-sweep-ledger.md](g5-sweep-ledger.md) — **G-5 registry sweep**: full inventory triage,
  dispositions, rows added (NS-1/NS-2/ST-1/ID-3/TY-4/TY-5/FA-2/FA-3), open-item rulings, gate
  evidence.
- [h1c-ledger.md](h1c-ledger.md) — **V2 Engine Hardening H-1c** (the `$`-metadata introspection
  rider): the evidence that ended the open question (what the fork's schema provider actually
  synthesizes, what Spark and Trino do, and why the live oracle tier cannot observe either), the
  ruling — **filter, at the catalog layer** — the five in-unit decisions, the four flagged
  deviations, the gate results, and the dated **fix pass** that corrected a falsely-green facade
  gate (a nondeterministic assertion), a 2×-wrong perf number, an overclaimed "drops exactly", a
  duplicated description across STATUS/registry and a false test-site citation. The decision itself
  is
  [../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md](../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md);
  the ledger records how it was reached, not what it says.
- [g6-chores-ledger.md](g6-chores-ledger.md) — **G-6 hardening chores** (four items, one PR):
  parity-runner markdown default → `target/census-reports/`; four `session.rs` rustdoc
  intra-link fixes; Glue acceptance location-mismatch fail-loud guard (+ DESCRIBE probe); and
  the `make parity-live` ↔ `parity-live.yml` dual-wire checker with must-FAIL/must-PASS
  provocation proofs.
- [g4-tests-split-ledger.md](g4-tests-split-ledger.md) — **G-4** declared-rename: split
  `crates/repark-spark/src/tests.rs` → `src/tests/` by production-module alignment; identity
  gate + name map under `task/g4-artifacts/`.
- [metrics.md](metrics.md) — the **process metrics ledger**: one section per retrospective, the
  eight-metric set the SEPMO retrospective contract fixes (findings per cycle, cycles to
  convergence, noise ratio, coverage misses, escaped defects by origin, LIGHT-path escapes, flags
  shipped, environment drift events). Append a section per campaign; never rewrite an earlier one.
  Created 2026-08-10 with the Front-Door campaign's numbers.
- [h1a-ledger.md](h1a-ledger.md) — unit ledger for **H-1a, BOTH splits**, of the V2 Engine
  Hardening campaign: split A (session-timezone conf surface + the live registry's per-scenario
  session-conf override + the recorded G1/G16 differential rows) and **§ Split B** (the extraction
  fix itself — the coercion path, the invoke-time carrier, the four-entry-point matrix, the eight
  Rust extractor-family pins, and the flip of thirteen recorded disclosures into equality rows,
  which is the fix's revert-red evidence). Carries the split's decisions with rationale, the
  acceptance-gate evidence, verbatim gate output, the both-ways provocations for its detection
  claims, and the ready-to-paste divergence-registry rows this unit produced for H-1d
  (which owns `docs/spark-sql-iceberg-parity.md` and merges first). **§8 is the adversarial
  panel's fix pass** — every MAJOR/NIT with the action taken, the provocations added or re-run,
  the post-fix gate output, and the deviations from the dispositions stated rather than absorbed.
  **§ Split B** (2026-08-10) is the extraction fix: its decisions (why
  `datafusion.execution.time_zone` was measured and rejected; why the zone is read at invoke and
  not baked in at registration; why TZ-4 split again), the matrix table, the flip inventory, and
  both provocations — including the one that replays a real over-reach the DATE negative caught
  mid-fix.
- [n2-merge-ledger.md](n2-merge-ledger.md) — unit ledger for **N-2 / H-2 gap G3** (MERGE INTO
  differential corpus, record-side). 10 recorded rows against live PySpark 4.1.2 +
  `iceberg-spark-runtime-4.1_2.13:1.11.0`, lifecycle helper cleanup proof, ready-to-paste registry
  rows, deferred Rust pins + live-tier scenarios (declared), octo/overload evidence.
- [g7-decimal-ledger.md](g7-decimal-ledger.md) — unit ledger for **G-7** (decimal128 differential
  corpus, Python half; G13 folded). Gap G2 (20-26 rows) + G13 (6-8) + 3 CTAS write-back +
  committed record driver; budget pin + CONVERGED-flip-don't-delete disclosures; G-7b deferred
  (Rust bit-exact pins + cross-door rows). Ready-to-paste registry rows live here, never in the
  registry file (conductor ban).
- [port/](port/map.md) — **live acceptance inputs**: the deferred-test manifest and its
  reconciliation rule ([port/deferred-tests.md](port/deferred-tests.md)), the machine-readable
  deferral allowlist ([port/deferred-python-tests.txt](port/deferred-python-tests.txt)) and its
  mirror additions ledger ([port/added-python-tests.txt](port/added-python-tests.txt)). The census
  comparator still subtracts these, so they are not history.
- [census/](census/map.md) — **evidence**: the recorded census runs, `baseline-fc3f48102/` (the port
  pin) and `v2-a5be8a7/` (the acceptance run). Never hand-edited; a re-run replaces a whole
  directory in one commit.

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

## I want to...

| ...do this | go to |
|---|---|
| See the live backlog / what happens next | [../STATUS.md](../STATUS.md) |
| Check a rule before acting | [lessons.md](lessons.md) |
| Start a new unit's ledger | copy the shape of [h1d-ledger.md](h1d-ledger.md) (or the archived [fd3-ledger.md](../docs/history/frontdoor/fd3-ledger.md)); link it from this map in the same commit |
| See how a divergence gets declared, pinned and mirrored | [h1d-ledger.md](h1d-ledger.md), then [../docs/spark-sql-iceberg-parity.md](../docs/spark-sql-iceberg-parity.md) §6 |
| Read why the session timezone is a build-time knob with one spelling | [h1a-ledger.md](h1a-ledger.md) |
| Read how timestamp extraction came to honor it, and what the fix deliberately did NOT close | [h1a-ledger.md](h1a-ledger.md) "§ Split B" |
| See how an open question gets FIXED instead of declared (and why a fixed defect gets no row) | [h1c-ledger.md](h1c-ledger.md) + [../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md](../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md) |
| Read the MERGE INTO differential corpus (gap G3) ledger + registry paste rows | [n2-merge-ledger.md](n2-merge-ledger.md) |
| See why a dependency edge or a manifest field is gated, and the proofs it fires | [../docs/history/frontdoor/fd3-ledger.md](../docs/history/frontdoor/fd3-ledger.md) |
| File a retrospective's metrics | [metrics.md](metrics.md) — append a section, never rewrite one |
| See which v1 tests are deferred, and why | [port/deferred-tests.md](port/deferred-tests.md) |
| Feed the census comparator its allowlists | [port/map.md](port/map.md) |
| Run or compare a census | [../docs/port/census.md](../docs/port/census.md) |
| Read the port's record (briefs, unit ledgers, retrospectives) | [../docs/history/port-v2/README.md](../docs/history/port-v2/README.md) |
| Read the port plan the phases executed | [../docs/port/PLAN.md](../docs/port/PLAN.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../AGENTS.md](../AGENTS.md) (the durable contract) and [../STATUS.md](../STATUS.md)
  (current state); this directory holds the moving parts of work in flight.
- Unit ledgers: one `<unit>-ledger.md` per delivered unit, with gate evidence and provocation proofs
  per [../docs/testing.md](../docs/testing.md), linked from this map in the same commit. When a
  campaign closes, its ledgers are archived under [../docs/history/](../docs/history/map.md) after a
  promotion audit — never deleted.

## Debug

- `pg-integration-report.md` may appear here untracked: `python/repark/tests/test_pg_acceptance.py`
  writes it (CWD-relative) on every facade run. It is gitignored on purpose — a run output, not a
  record. Do not `git add` it.
- If work and trackers disagree, the code is truth — update the tracker.
- A link into `task/p*-ledger.md` or `task/fd3-ledger.md` fails: see "Where the closed campaigns'
  ledgers went" above — same basename, under [../docs/history/](../docs/history/map.md).
- No `<unit>-ledger.md` in this directory is the steady state between campaigns, not a missing
  file; the ledger of a closed campaign lives with that campaign's archive. A campaign IS running
  (V2 Engine Hardening), so its delivered units' ledgers accumulate here until close-out.
- Looking for a backlog item that is not in [../STATUS.md](../STATUS.md)? Check
  [../docs/history/port-v2/promotion-ledger.md](../docs/history/port-v2/promotion-ledger.md) — if it
  was live at archival, that table says where it went.
