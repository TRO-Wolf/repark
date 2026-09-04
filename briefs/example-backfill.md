# BRIEF — the v0.7 example backfill campaign (execution slate)

**Roadmap ruling:** [task/roadmap/epic-term/release-roadmap-2026-08-29.md](../task/roadmap/epic-term/release-roadmap-2026-08-29.md)
§"v1.1 — Full example documentation (was v0.7)".
**Gate:** `make check-example-coverage`, delivered by EX-0
(`task/ledgers/completed/ex-0-example-drift-gate-ledger.md`, archived at the next pickup). No batch opens before that gate is
in `make ci` on main.
**Delivery shape:** one pull request per family, batches landing as commits on that branch. The
owner merges; review cost, not worker cost, sets the batch size.

## The contract

v0.7 is done when every public name RePark exposes has an executable worked example, and the drift
gate proves it on every CI run. **Each family PR carries its own charter ledger** (owner ruling,
2026-08-31): `task/ledgers/staging/ex-<n>-<family-slug>-ledger.md`, one clause per batch with
red-first gate evidence, archived to `completed/` when the family lands. The backlog file EX-0
seeded is the campaign's shared scoreboard beside those ledgers: it starts holding every
uncovered name and ends empty but for the documented exceptions. **Its count may only go down.**

The gate is the whole acceptance bar and it is not negotiable in either direction:

- A name is covered when a runnable example names it in `COVERS` **and exercises it**. A `COVERS`
  entry the script does not demonstrate is the one defect the gate cannot see and the one the
  campaign will not tolerate — it is a review duty on every PR.
- Every example runs green under `python <path>`, locally, with no network, no cloud service and
  no JVM. The gate executes them; a nonzero exit is a red CI.
- A name that cannot be honestly demonstrated locally goes in the exceptions file with a one-line
  reason, written by the orchestrator. Declared-absent names (FNP-15/16) are exceptions by
  construction — a refusal is documented as a refusal, never as an example that swallows it.
- Examples are documentation. An example that exists to satisfy the gate rather than to teach the
  name is a review rejection, not a merge.
- Sessions are constructed `repark = ReparkSession.builder…` (owner ruling, 2026-09-01); the
  reference example is [../docs/examples/session/sql.py](../docs/examples/session/sql.py).

## Batch roster

EX-0 seeded 763 names; EX-1 widened to **913** (892 on the backlog, 2 exceptions). Counts below are the uncovered names at campaign open (2026-09-01).

| # | Family | Branch | Names | Tier | Status |
|---|---|---|---|---|---|
| 1 | `F.*` math + bitwise (pilot) | `feat/ex-2-functions-math-bitwise` | 23 | GLM | dispatched 2026-09-01 |
| 2 | TA kernels | `docs/ex-3-ta-kernels` | 86 | GLM | |
| 3 | `F.*` collections | `docs/ex-4-functions-collections` | — | GLM | |
| 4 | `F.*` datetime | `docs/ex-5-functions-datetime` | — | GLM | |
| 5 | `F.*` agg + window | `docs/ex-6-functions-agg-window` | — | Grok | |
| 6 | `F.*` lambda + try + url | `docs/ex-7-functions-lambda-try` | — | mixed | |
| 7 | `F.*` expressions (long tail) | `docs/ex-8-functions-expr-<n>` | — | GLM | |
| 8 | DataFrame + Column methods | `docs/ex-9-dataframe-column` | 190 | mixed | |
| 9 | Session / reader / writer / Catalog | `docs/ex-10-io-session` | 111 | Grok | |
| 10 | Window/WindowSpec, types + Row, ml | `docs/ex-11-class-remainder` | 82 | GLM | |
| — | Declared-absent (FNP-15/16) | — | — | exceptions | |

The `F.*` sub-family splits (444 uncovered functions names total, 441 on the backlog today) are
chartered exactly at each lane's dispatch — the family ledger's first clause records its batch
roster against the live backlog, so the numbers here stay directional, not authoritative.

Order is dispatch order: the pilot proves the template on the cheapest surface (pure scalar
expressions — no catalog, no filesystem), the TA kernels then move the count fastest at the same
per-name judgement, and the IO surfaces run last because they are the ones most likely to expose a
product bug. Up to four lanes run in parallel; never two on one family branch.

## Standing rules

These narrow [AGENTS.md](../AGENTS.md) "Delegated-agent standing rules"; they never relax it.

- **Tiering.** Batches are mechanical work and run on the GLM tier (`oc-worker`). A name needing
  judgement — non-obvious semantics, a fixture that must be designed, a demonstration spanning two
  families — escalates to a Grok lane, batched per family rather than dispatched one-off. Escalation
  is the campaign's entire budget; the GLM tier is effectively free.
- **The orchestrator never merges and workers never push.** The orchestrator reads every commit's
  full diff, re-runs `make ci` and the gate from a clean checkout of the branch, and confirms the
  backlog delta is exactly the batch's names before pushing. A worker's own green is directional.
- **Writable paths, every batch:** `docs/examples/`, the backlog file, map.md in lockstep, and
  the family's own staging ledger. Everything else is closed — `crates/`,
  `python/repark/src/`, `scripts/`, `.github/`, STATUS.md, other ledgers,
  `briefs/next-sequence.md`.
- **This campaign changes no product behaviour.** An example that exposes a bug files it in the
  handback and drops the name back to the backlog. Nobody fixes engine code inside a docs PR.
- **No comments in code** ([the owner's ruling](../CLAUDE.md)); module docstrings are required.
- **Green is an exit condition.** Real exit codes, never a pipe's.

## What "done" means for the campaign

The backlog file holds no names; the exceptions file holds only names with a stated reason the
owner has read; `make check-example-coverage` is green in `make ci` on main; and every family PR
is merged. STATUS.md records the final counts — covered, excepted — as the v0.7 close-out.
