# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [ctas-view-1-conform-stream-ledger.md](ctas-view-1-conform-stream-ledger.md) —
  **CTAS-VIEW-1 (2026-09-03), in flight:** unpartitioned CTAS stream writer conforms
  Utf8View/BinaryView batches to the Iceberg table schema. Branch
  `fix/ctas-view-1-conform-stream`. `risk_tier: standard`.
- [ex-10-functions-null-cond-misc-ledger.md](ex-10-functions-null-cond-misc-ledger.md) —
  **EX-10 (2026-09-03), in flight:** the v0.7 example backfill's `F.*` null-handling,
  conditional, ordering, bit and session batch — 33 names landed in seven examples, the
  backlog ratchet 842 → 809; the 12 names the live oracle measured divergent (`F.isnan`
  `[False,False]` vs `[False,None]`, the session-identity four `repark` vs OS user) or
  refused (`F.expr` literals Spark-equal `[2,2]`/`['AB','AB']`, column ref `AnalysisException`
  vs Spark `[2.0,None]`; `F.raise_error` `USER_RAISED_EXCEPTION`; the input/partition five
  `UnsupportedOperationException`) stay on the backlog with both values recorded. `risk_tier: standard`. Branch
  `feat/ex-10-functions-null-conditional`. Slate:
  [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md).
- [ex-11-functions-hash-url-random-ledger.md](ex-11-functions-hash-url-random-ledger.md) —
- [ex-12-functions-aggregates-a-ledger.md](ex-12-functions-aggregates-a-ledger.md) —
- [ex-13-functions-aggregates-b-stats-ledger.md](ex-13-functions-aggregates-b-stats-ledger.md) —
- [ex-14-functions-window-ledger.md](ex-14-functions-window-ledger.md) — **EX-14 (2026-09-03), in flight:** the v0.7 example backfill's `F.*` window batch — nine names land in four examples, the backlog ratchet 777 → 768, all nine measured Spark-equal on the live oracle. Branch `feat/ex-14-functions-window`. Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md).
- [ex-2-functions-math-bitwise-ledger.md](ex-2-functions-math-bitwise-ledger.md) —
  **EX-2 (2026-09-01), in flight:** the v0.7 example backfill's `F.*` math +
  bitwise family — the campaign pilot. One clause per batch; batch 1 covers
  eleven roots / exponential / power / sign / rounding names and moves the
  backlog ratchet 892 → 881; the twelfth, `F.expm1`, is measured, reported and
  left on the backlog rather than taught by an example that omits its reason
  for existing. `risk_tier: standard`. Branch
  `feat/ex-2-functions-math-bitwise`. Slate:
  [../briefs/example-backfill.md](../../../briefs/example-backfill.md).
- [ex-4-functions-strings-a-ledger.md](ex-4-functions-strings-a-ledger.md) — **EX-4 (2026-09-03),
- [ex-5-functions-strings-b-regex-ledger.md](ex-5-functions-strings-b-regex-ledger.md) —
- [ex-6-functions-datetime-a-ledger.md](ex-6-functions-datetime-a-ledger.md) —
- [ex-7-functions-datetime-b-ledger.md](ex-7-functions-datetime-b-ledger.md) —
- [ex-8-functions-arrays-ledger.md](ex-8-functions-arrays-ledger.md) —
- [ex-9-functions-maps-structs-json-ledger.md](ex-9-functions-maps-structs-json-ledger.md) —
  **EX-9 (2026-09-03), in flight:** the v0.7 example backfill's `F.*` map,
  struct and JSON family. Twelve names land in four files and the backlog
  ratchet moves 842 → 830; the other 24 roster names (json_tuple, csv, xml,
  xpath, variant) are measured against the live oracle and stay on the backlog —
  the engine refuses each (E1-disclosed deferrals). `risk_tier: standard`.
  Branch `feat/ex-9-functions-maps-structs-json`. Slate:
  [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md).
- [fn-fix-1-registry-rows-ledger.md](fn-fix-1-registry-rows-ledger.md) —
  **FN-FIX-1 (2026-09-03), in flight:** ten filed function-parity divergences plus
  NaN ingest become Spark-equal. `risk_tier: standard`. Branch
  `feat/fn-fix-1-registry-rows`. pins: fn-fix-1-registry-rows/C-001
- [rp-9-repin-f23-ledger.md](rp-9-repin-f23-ledger.md) — **RP-9 (2026-09-03), in flight:**
  the fork repin `c1d6c9de` → `594bdbe5` (F-23). The DV close skips the data-manifest walk
  when there are no legacy deletes and `known_partitions` covers every touched path;
  `PERF-DVCLOSE-WALK-1` FIXED. `risk_tier: standard`. Branch `feat/rp-9-repin-f23`.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
