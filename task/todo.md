# todo — pointer only

**The live backlog is [STATUS.md](../STATUS.md)** (release state, active workstreams, known issues,
deferred capabilities). This file is a pointer, not a tracker; do not accumulate a second backlog
here.

- **Working plan for a unit in flight** → its `task/<unit>-ledger.md` (see [map.md](map.md)); the
  ledger is where scope, decisions, gate results and provocation proofs go.
- **The port's own execution history** (phase checklists + the three retrospectives) →
  [docs/history/port-v2/port-execution-log.md](../docs/history/port-v2/port-execution-log.md).

## Post-milestone-one (BACKLOG)

This heading is load-bearing: the Python binding's deferred-reader refusal cites it at runtime
(`crates/repark-python/src/session.rs`), and tests pin the citation.

- **`repark-postgres` / `repark-excel` read connectors** (the surfaces the binding's refuse-arms
  name) → [STATUS.md](../STATUS.md) "Deferred capabilities", with the test rows in
  [port/deferred-tests.md](port/deferred-tests.md).

_Condensed 2026-08-09 (Front-Door FD-4): the port closed, so its tracker became history. The file
keeps its name because live code, docs and error messages cite this path._
