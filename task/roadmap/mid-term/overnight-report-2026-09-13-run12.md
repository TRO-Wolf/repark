# Run 12 report — the FACADE half (2026-09-13, afternoon into evening)

**Unit:** overnight-12 · **Orchestrator:** Opus 5 (named by the owner) · **Beside:** run 12b (overnight-12b:
EAGER-BUDGET-1, REPLACE-LINEAR-1, ARRAY-NULL-1) on the same box · **Grants:** G-1 yes, G-2 yes, G-3 stop by 21:30
or when the list is exhausted, G-4 Devin actor + Grok critic/reviewers, G-5 yes · **Runbook:**
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md)

## 1. Outcome

| # | Unit | State | PR | Merged |
|---|---|---|---|---|
| 1 | FACADE-3 step 3 (F-FUNNEL + F-TIMETUPLE); the ledger departs to `completed/` | Merged, tree-equal | #574 | `4a9eb35a` |
| 2 | FACADE-4 (type conversions in Rust) | Not opened: the stop time came first | — | — |
| 3 | FACADE-5 (display renderer in Rust) | Not opened | — | — |

FACADE-3 took the whole window: one actor round, three reviewer rounds that each sent real findings back, three
follow-ups, and a critic re-check. FACADE-4's step-0 brief is written and ready (§5).

## 2. FACADE-3 step 3 — before and after

Release native, the step-1 runner's `measure_cell` (warmup + 3, medians, idle box, one fresh process per shape under
`systemd-run --user --scope -p MemoryMax=8G`), 1e5 rows × 7 columns, `createDataFrame` only.

| shape | step 2 | step 3 | Δ |
|---|---:|---:|---:|
| rows | 793.48 ms | 371.00 ms | −53.2 % |
| dicts | 641.20 ms | 286.78 ms | −55.3 % |
| tuples | 437.50 ms | 230.00 ms | −47.4 % |
| tuples + DDL | 448.31 ms | 242.15 ms | −46.0 % |
| tuples + StructType | 450.21 ms | 239.57 ms | −46.8 % |
| nested | 206.96 ms (under `prlimit --as`) | 236.54 ms | same-box A/B with the step-2 binary: 243.04 → 236.54 (−2.7 %) |
| pandas (control) | 445.99 ms | 447.51 ms | +0.3 % |
| polars (control) | not measurable under `prlimit` | 436.18 ms | +2.9 % against step 1 |

From step 1's baseline to step 3, the unit took rows 1,060.81 → 371.00 ms (−65 %), dicts 947.00 → 286.78 ms
(−70 %), tuples 735.61 → 230.00 ms (−69 %), the explicit-schema pair 2,057–2,089 → 240–242 ms (−88 %), and
nested 2,852.45 → 236.54 ms (−92 %).

The F-TIMETUPLE route was measured per cell (1e5 cells, the timing that chose it) before any product change:
`timetuple()` 98.4 / 125.3 / 194.7 ms against 10.2 / 25.6 / 31.9 ms for date / naive / UTC-aware cells. The Rust
reviewer re-measured the finished route against `main`: date −81.9 %, naive −72.0 %, UTC-aware −79.7 %, zoneinfo −59.1 %.

Fallback shapes at row 90 000, measured against the real `main` release tree:

| fallback shape | main | branch | Δ |
|---|---:|---:|---:|
| `object()` cell in a dict list | 570.05 ms | 578.63 ms | +1.5 % |
| int→float in a tuple list | 358.25 ms | 360.01 ms | +0.5 % |
| non-`Row` element in a `Row` list | 85.24 ms | 71.74 ms | −15.8 % |
| non-dict element in a dict list | 9.20 ms | 4.48 ms | −51.3 % |
| strict key-set mismatch in a `Row` list | 209.00 ms | 197.61 ms | −5.4 % |
| `object()` cell in a `Row` list | 637.95 ms | 635.38 ms | −0.4 % |

After the critic-logic remediation (exact-type gates), a same-box A/B put the gate at +0.1 % on `tuples` (the pre-gate binary measured 245.00 ms vs 245.24 ms; the 230.00 ms in the table was not reproducible for either binary on the loaded box), `rows` −3.4 %, and the date / naive / UTC-aware micro cells +3.1 % / +3.7 % / +0.9 % (ledger C-027).

## 3. Reviewer verdicts (S2-21 + critic-logic)

| round | tier, cost | verdict | what came back | outcome |
|---|---|---|---|---|
| Rust perf | Grok 4.6, $0.81 | no P1 | P2-1 `Row` built an `asDict()` dict per row; P2-2 homogeneous dict lists sorted every row's keys; P3 F-SLOTS, non-interned timedelta names | Devin follow-up 1: index route over `_Row__field_values`, seen-key fast path, interned names; F-SLOTS stays in the ledger |
| Python perf | Grok 4.6, $0.83 | no P1 | F-PY-2: a dict-list fallback paid the named probe, the whole Python funnel, then a tuple-door native retry (+11.5 % vs real `main`) | Devin follow-up 2: no retry after a named decline, cheaper doomed-path probes; fallbacks re-measured against the real base tree |
| Critic-logic, round 1 | Grok 4.6, $0.78 (incl. a turn-1 stall, $0.02) | NEEDS_REMEDIATION | L-001..L-003 (P1): a `date` subclass overriding `__sub__` → 1970-01-01; a `datetime` subclass overriding `year` → 1999; a `utcoffset` cache keyed by a raw pointer returned stale offsets. L-004 (P2): the subclass pin used empty subclasses | Devin follow-up 3: exact-type gates (subclasses keep main's `timetuple()` path natively), an owned-reference cache, pins on all three doors |
| Critic-logic re-check | Grok 4.6, $0.51 | PASS | `repro_p1.py` matches `main` on every case; nested subclass cells, `zoneinfo`, mixed fixed offsets, pandas `Timestamp`/`NaT` match; removing the exact-type gate reds the new pins | none |

## 4. Decisions taken under G-2 (runbook §6)

- R12-D-1: fallback bars are measured against a real `main` release tree. The brief's "patch one export to `None`"
  simulation stopped matching `main` once a second native door existed, and it had hidden a +11.5 % case.
- R12-D-2: the L-001/L-002 fix routes subclasses to main's native `timetuple()` path rather than a whole-frame Python
  fallback, so pandas `Timestamp` rows keep native speed.
- R12-D-3: the `#[allow(clippy::…)]` attributes on the new `named.rs` entry point follow step 2's merged precedent in
  the same module; no convention change.
- R12-D-4: the orchestrator repaired the `Authored-By` trailer on the actor commits whose messages carried the Devin
  CLI's own footer (a `Generated with` line plus a bot co-author trailer). The rewrite was message-only, and the tree
  equality was checked.

## 5. What remains of the Rust switchover (measured on `main` at `4a9eb35a`)

`python/repark/src/repark/spark/` holds 51,701 Python lines after this run.

| next unit | files | lines | state |
|---|---|---:|---|
| FACADE-3 residue | `session/create_dataframe_{rows,inference,schema,tuples,values,arrow,columns}.py` | 4,223 | The native path covers rows, tuples, dicts, nested cells and explicit schemas. What remains is the per-cell refusal/fallback path and the pandas/polars legs. No measured wall remains in the unit's scope |
| FACADE-4 | `types.py` 1,834 · `session/timestamp_type.py` 97 · DDL writers in `create_dataframe_schema.py` 692 · `_csv_smart.py` 899 (rungs) | ≈3,522 | UNMEASURED; step 0 is written (`facade4-step-0.md`: baseline cells, the three-table agreement census, DDL round-trip goldens, an isinstance pin) |
| FACADE-5 | `dataframe/display.py` 514 · `dataframe/plan_collapse.py` 1,057 (formatters) · `dataframe/eager.py` 129 (run 12b owns it tonight) | 1,700 | UNMEASURED; fetch and format legs to be timed separately first |
| FACADE-6+ roll-call | `joins_columns.py` 1,238 · `writer_readwriter.py` 1,111 · `reader.py` 1,022 + `reader_support.py` 492 · `functions_udf.py` 1,300 · `udf_bridge.py` 471 + `udf_projection.py` 349 + `udf_schema.py` 64 + `grouped_udf.py` 145 · `session/sql_udf*.py` 2,633 | 8,825 | Opened measure-first in the order the isolating measurements rank them (owner charter 2026-09-12) |

## 6. Owner questions, with recommendations

- **Q-R12-1 — pandas `Timestamp` and user `date`/`datetime` subclasses keep the slower `timetuple()` route.**
  Recommendation: keep it. The fast route reads attributes a subclass can override, and the critic proved that gives
  wrong values. Exact `datetime` rows are already the fast path. A later card could add a measured
  `Timestamp`-specific route (`value` nanoseconds) if a pandas-rows workload shows the cost.
- **Q-R12-2 — the default for the per-round fallback bar.** Recommendation: every perf card measures fallback shapes
  against a real base tree built in the review bed, not an in-process patch (R12-D-1); add it to the slate preamble.
- **Q-R12-3 — the Devin CLI appends its own `Generated with` line and bot co-author trailer on some commits.** It did so on
  resumed rounds, even with the brief's trailer rule. Recommendation: the launcher checks every new commit's
  `Authored-By` trailer after each round and repairs or refuses before the orchestrator reads the hand-back.

## 7. Spend

| tier | rounds | cost |
|---|---|---|
| Devin SWE-2 (free) | 4 on session `dull-buckaroo` (1 actor + 3 follow-ups) | $0 |
| Grok 4.6 | Rust perf 1, Python perf 1, critic-logic 3 (stall + round + re-check) | $2.93 |
| Muse / GLM | 0 | — |

### Incidents

- INC-R12-1: critic-logic's first launch ended after one turn with a placeholder summary. It was resumed on the final
  tip with a proceed mandate (runbook §3 turn-1 stall).
- INC-R12-2: the first ship run was stopped mid-preflight when critic-logic returned three P1s, so no ungated commit
  could reach the push. The gates re-ran on the remediated tip.
- INC-R12-3: Devin measured with a temporary Rust probe and rebuilt the step-2 native in a worktree at `/tmp/s2-wt`,
  outside the lane. The probe never reached a commit; the orchestrator removed the worktree (2.3 GB).

## Pointers

- Up: [map.md](map.md) · Ledger: [../../ledgers/completed/facade-3-ledger.md](../../ledgers/completed/facade-3-ledger.md)
- Perf: [../../../docs/perf/facade-3-cdf-step3-2026-09-13.md](../../../docs/perf/facade-3-cdf-step3-2026-09-13.md)
- Audit: [../epic-term/facade-audit-2026-09-10.md](../epic-term/facade-audit-2026-09-10.md)
