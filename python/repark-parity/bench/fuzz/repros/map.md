# map — python/repark-parity/bench/fuzz/repros

## Purpose

Banked minimal SQL repros from the R-SQL-FUZZER differential harness. Each file is
``<seed>-<n>.sql`` with header comments (seed, query index, compare message, fixture
rows) and the minimized SQL body. **Empty corpus is a valid outcome** — do not pad.

## Contents

- `map.md` — this file.
- `*.sql` — banked repros (if any); see ledger for the live census.
- `corpus_index.json` — optional index written by a long pass (gitignored if present
  under local runs; committed only when the unit banks a non-empty corpus and the
  actor elects to keep the index).

## I want to…

| I want to… | Go to |
|---|---|
| See whether the corpus is empty | this dir (no `*.sql` → empty) + `task/d3-sql-fuzzer-ledger.md` |
| Pin a banked repro as xfail | `python/repark/tests/test_fuzz_smoke.py` *(facade path — arrives with the facade package in the phase-3 facade PR)* |

## Debug

| Symptom | Check |
|---|---|
| Repro no longer diverges | engine fix-forward may have landed — flip xfail → OK pin + ledger note |
| Repro missing fixture | header `-- TABLE` comments carry the minimized row set |

## Constraints

- Engine product fixes are out of scope for D3 — bank + pin only.
- Never commit AWS artifacts or credentials.

- 2026-08-01: fuzz-42-1/2 RESOLVED (DF54.1 Sort-loss; session flag guard) — corpus empty,
  resolutions recorded in corpus_index.json.

<!-- Phase-3 PR-4 (V2 port), declared: the `task/…-report-*.md` scoreboards and unit
     ledgers named above are port-source measurement artifacts and were NOT ported —
     they are historical evidence of runs made in the source repository. Re-running a
     bench here writes a fresh report under `task/`. The row text is kept verbatim so
     the invocation recipes stay accurate; only the report files are absent. -->
