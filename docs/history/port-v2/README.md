# The v1 → v2 port — the archived record

**Archived 2026-08-09** (Front-Door FD-4). Everything in this directory is **history**: it records
how this repository's engine came to exist, between 2026-08-06 and 2026-08-08. It is deliberately
**off the normal read path** and is **not a source of live rules** — see
[promotion-ledger.md](promotion-ledger.md) for the audit that proves it.

**Current state lives in [STATUS.md](../../../STATUS.md); the rules live in
[AGENTS.md](../../../AGENTS.md).** If this directory and a current document disagree, the current
document wins and the disagreement is a bug in the current document, not a rule hidden here.

## What the port was

RePark v1 was a private, working engine. V2 (this repository) is its public successor, and it was
built by **copy-then-re-home**, not by rewriting: each phase started from a literal copy of the
relevant v1 crates or packages at one frozen commit, then re-homed them commit by commit — renames,
re-layering and the deliberate V2 design passes landing as reviewable increments on top of a faithful
copy. Every intermediate commit built and passed the gates, and tests moved **with their names** so
the move could be proved mechanically rather than argued.

- **Port source pin:** the private v1 repository at **`fc3f48102`** (its `main` at the freeze).
  Every copy came from `git show <pin>:<path>`, never from a working tree.
- **Four phases:** 0 bootstrap (governance + gates before any code) · 1 engine core
  (`repark-common`, `repark-iceberg`, `repark-core`) · 2 the two SQL doors (`repark-functions`,
  `repark-ta`, `repark-spark`, `repark-sql`) · 3 the Python facade + parity (`repark-ml`,
  `repark-python`, `python/repark`, `python/repark-parity`) — **phase 3 = milestone one**.
- **Closed 2026-08-08** with milestone one reached (PRs #16, #18–#23 for phase 3; the earlier phases
  are #3–#15).

The plan of record — the copy-then-re-home rules, the four phases and the acceptance gate — is still
live at [docs/port/PLAN.md](../../port/PLAN.md), because it is what the census gate and the
relocation discipline are defined against.

## How parity was verified

The completion claim was mechanical, not narrative: the same pyspark-compat census, the same module
cohorts, run on both repositories and compared **byte-flat as multisets** by a purpose-built
comparator, with two checked-in ledgers (deferred, added) as the only permitted subtractions.

| Cohort | Result at acceptance |
|---|---|
| classic | 142/345 — identical, exit 0 |
| expand | 44/171 — identical, exit 0 |
| expand2 | 87/167 — identical, exit 0 |
| full-extras facade | `(2,499 collected − 2 added) ∪ 12 deferred = pin 2,509` — identical, exit 0 |

- **The procedure** is [docs/port/census.md](../../port/census.md) (live — it is still the recipe for
  running one).
- **The evidence** is committed under [task/census/](../../../task/census/map.md):
  `baseline-fc3f48102/` (the pin) and `v2-a5be8a7/` (the acceptance run). It is evidence, never
  hand-edited; a re-run replaces a whole directory in one commit.
- **The ledgers** are live acceptance inputs: [task/port/](../../../task/port/map.md).

## What lives here

| File(s) | What it records |
|---|---|
| [phase-0-bootstrap.md](phase-0-bootstrap.md) … [phase-3-python-facade.md](phase-3-python-facade.md) | The four execution briefs — what each phase was **asked** to do. |
| `p1a`…`p1c`, `p2a`…`p2g`, `p3a`…`p3g` `-ledger.md` (17 files) | One unit ledger per delivered PR — scope, declared edit classes, census arithmetic, gate results, provocation proofs, verify-panel findings and their dispositions. |
| [port-execution-log.md](port-execution-log.md) | The port's live tracker (was `task/todo.md`), including the three SEPMO phase retrospectives. |
| [promotion-ledger.md](promotion-ledger.md) | The lossless-archival audit: every rule in every file above, classified, with its current home. |

**Where the ledgers used to live.** The unit ledgers were `task/p*-ledger.md` and the briefs were
`briefs/phase-*.md` until 2026-08-09. A prose citation of an old path (a few survive in Rust doc
comments) means the file with that basename, here.

## Which decisions are still current

Four ADRs made during the port are **live decisions**, not history, and are the authoritative "why"
for how the engine is built today:

- [docs/adr/0001-own-iceberg-fork.md](../../adr/0001-own-iceberg-fork.md) — the owned `iceberg-rust`
  fork, rev-pinned via `[patch.crates-io]`, never vendored; DataFusion is never forked.
- [docs/adr/0002-two-sql-doors.md](../../adr/0002-two-sql-doors.md) — two honest SQL doors, no
  blended parser; new surface lands with both spellings and one test row per door.
- [docs/adr/0003-copy-then-rehome-port.md](../../adr/0003-copy-then-rehome-port.md) — the port shape
  itself, and the census-multiset acceptance gate.
- [docs/adr/0004-server-prep-disciplines.md](../../adr/0004-server-prep-disciplines.md) —
  everything-through-Session and bindings-as-thin-adapter.

The settled designs the phases implemented are also still live, because the engine still obeys them:
[session-api.md](../../design/session-api.md) (the frozen `SqlDialect` / `SessionExtension` seams),
[sql-doors.md](../../design/sql-doors.md) (the ANSI rulings the surface matrices cite) and
[python-facade.md](../../design/python-facade.md) (the binding/facade edit classes and the census
definition).

## Rules for this directory

1. **Immutable**, with exactly two exceptions: link repair, and a **dated** correction that is
   labelled as one. Nothing here is silently rewritten to match today.
2. **Every status claim carries its effective date.** A ledger that said "IN FLIGHT" when it was
   written now says what it actually became, with the date it became it.
3. **Current documents link here only where provenance matters** — never to state a rule.
4. The universal `map.md` discipline holds inside the archive: this directory carries
   [map.md](map.md), as does [docs/history/](../map.md).
