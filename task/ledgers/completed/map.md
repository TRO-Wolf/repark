# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [ctas-view-1-conform-stream-ledger.md](ctas-view-1-conform-stream-ledger.md) —
  **CTAS-VIEW-1 (2026-09-03), complete:** unpartitioned CTAS stream writer conforms Utf8View
  batches. Merged #341.
- [ex-10-functions-null-cond-misc-ledger.md](ex-10-functions-null-cond-misc-ledger.md) —
  **EX-10 (2026-09-03), complete:** `F.*` null / conditional / misc.
- [ex-11-functions-hash-url-random-ledger.md](ex-11-functions-hash-url-random-ledger.md) —
  **EX-11 (2026-09-03), complete:** `F.*` hash / URL / random.
- [ex-12-functions-aggregates-a-ledger.md](ex-12-functions-aggregates-a-ledger.md) —
  **EX-12 (2026-09-03), complete:** `F.*` aggregates (a).
- [ex-13-functions-aggregates-b-stats-ledger.md](ex-13-functions-aggregates-b-stats-ledger.md) —
  **EX-13 (2026-09-03), complete:** `F.*` aggregates (b) / stats.
- [ex-14-functions-window-ledger.md](ex-14-functions-window-ledger.md) —
  **EX-14 (2026-09-03), complete:** `F.*` window.
- [ex-2-functions-math-bitwise-ledger.md](ex-2-functions-math-bitwise-ledger.md) —
  **EX-2 (2026-09-01), complete:** `F.*` math + bitwise pilot.
- [ex-4-functions-strings-a-ledger.md](ex-4-functions-strings-a-ledger.md) —
  **EX-4 (2026-09-03), complete:** `F.*` string basics.
- [ex-5-functions-strings-b-regex-ledger.md](ex-5-functions-strings-b-regex-ledger.md) —
  **EX-5 (2026-09-03), complete:** `F.*` string search / regex.
- [ex-6-functions-datetime-a-ledger.md](ex-6-functions-datetime-a-ledger.md) —
  **EX-6 (2026-09-03), complete:** `F.*` datetime arithmetic.
- [ex-7-functions-datetime-b-ledger.md](ex-7-functions-datetime-b-ledger.md) —
  **EX-7 (2026-09-03), complete:** `F.*` datetime remainder.
- [ex-8-functions-arrays-ledger.md](ex-8-functions-arrays-ledger.md) —
  **EX-8 (2026-09-03), complete:** `F.*` arrays.
- [ex-9-functions-maps-structs-json-ledger.md](ex-9-functions-maps-structs-json-ledger.md) —
  **EX-9 (2026-09-03), complete:** `F.*` map / struct / JSON.
- [fn-fix-1-registry-rows-ledger.md](fn-fix-1-registry-rows-ledger.md) —
  **FN-FIX-1 (2026-09-03), complete:** ten filed function-parity divergences plus NaN ingest.
  pins: fn-fix-1-registry-rows/C-001
- [nullability-2-ledger.md](nullability-2-ledger.md) —
  **NULLABILITY-2 (2026-09-05), complete:** the analyzer's remaining nullability
  and cast residues, Spark-equal — eight registry rows FIXED, live roster 13 → 10.
  pins: nullability-2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [perf-ice-scan-1-ledger.md](perf-ice-scan-1-ledger.md) —
  **PERF-ICE-SCAN-1 (2026-09-05), complete:** Iceberg `count(*)` folds (86.5 → 2.0 ms)
  and small tables scan N=8 (sum 89.5 → 36.2 ms); the 1.5×-of-parquet target is an
  honest miss (1.8–3.6×, decomposed in the baseline). Registry rows
  `PERF-ICE-COUNTSTAR-1` and `PERF-ICE-SCANPART-1` FIXED-PENDING-PIN behind the RP-14
  fork bump. `risk_tier: standard`. Branch `perf/ice-scan-1`.
  pins: perf-ice-scan-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011
- [rp-11-repin-f24-ledger.md](rp-11-repin-f24-ledger.md) —
  **RP-11 (2026-09-04), complete:** fork repin `85a4aaf0` → `189a73ed` (F-24);
  `B-MOR-3-FLOOR-1` FIXED.
- [rp-9-repin-f23-ledger.md](rp-9-repin-f23-ledger.md) —
  **RP-9 (2026-09-03), complete:** fork repin `c1d6c9de` → `594bdbe5` (F-23);
  `PERF-DVCLOSE-WALK-1` FIXED.
- [sem-0-charter-ledger.md](sem-0-charter-ledger.md) — **SEM-0 (2026-08-21), complete:** the
  scope audit for `RE-1` and `LOG-1`; both closed to Spark by SEM-1. Campaign closed 2026-09-04.
- [types-1-ledger.md](types-1-ledger.md) — **TYPES-1 (2026-09-05), complete:** the SQL
  door's Arrow types follow Spark — INT literals, BIGINT count-likes, the INT rank family,
  session-zone STRING `from_unixtime`. 9 clauses PROVEN, 7 live legs on Spark 4.1.2.
  `V3-COV-8`, `BL-8`, `G5-RANK-TYPE-1/2/3`, `UNIX-1` FIXED; `TY-3` narrowed; `TY-6`,
  `BL-18`, `TY-7`, `TY-8`, `TY-9`, `TY-10` filed (round-4 §13 corrects §7.1/§11;
  round-5 §14 corrects §6/C-008 and pads negative years).
  pins: types-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
