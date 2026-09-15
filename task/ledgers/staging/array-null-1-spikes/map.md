# map — task/ledgers/staging/array-null-1-spikes/

## Purpose
Measurement harness for ARRAY-NULL-1's before/after release table. The step-0
oracle/`repark_today`/route-measure/rss-table scripts were superseded by the
committed product arm and pins in step 1 and removed; their outputs stay frozen
in the ledger. Branch `fix/array-null-1`.
pins: array-null-1/C-002

## Contents
- [before_after.py](before_after.py) — `before_after.py <label> <d1,d2,...>` runs
  append and prepend chains at each depth in subprocess workers under
  `RLIMIT_AS = VmSize + 3 × 8 GB` with a 300 s per-worker cap, printing RSS
  build/total deltas and build/collect wall per row. Produces the ledger's
  "Before/after (release)" table against whatever native `maturin develop`
  installed last.

## Pointers
- Up: [../map.md](../map.md)
- Ledger: [../array-null-1-ledger.md](../array-null-1-ledger.md)
