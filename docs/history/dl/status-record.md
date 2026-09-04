# dl — STATUS record

## Cut from STATUS.md — closed 2026-09-04 by #343

- **Document lifecycle (DL)** (chartered 2026-08-23; DL-1..DL-5 delivered). Unit ledgers live in
  [task/ledgers/](../../../task/ledgers/map.md) by state; `scripts/ledger_lifecycle.py` is the only mover.
  Three gates in `make ci` hold the class: `check-ledgers`, `check-ledger-grammar`,
  `check-docs-compaction`. Policy: [AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle".
  Records: [task/ledgers/archive/2026-08/](../../../task/ledgers/archive/2026-08/map.md).
