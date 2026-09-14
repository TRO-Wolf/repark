# Run 13 report — the FACADE half (2026-09-14, early morning)

**Unit:** overnight-13 · **Orchestrator:** Opus 5 (named by the owner) · **Beside:** run 13b (overnight-13b: ARRAY-NULL-1,
ANSI-DOOR-1, REPLACE-LINEAR-1 step 1) on the same box · **Grants:** G-1 yes, G-2 yes, G-3 stop by 06:30 or when the list
is exhausted, G-4 Devin actor + Grok critic/reviewers, G-5 yes · **Runbook:**
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md)

## 1. Outcome

The session started at 05:19, which left a 71-minute window before the 06:30 stop.

| # | Unit | State | PR | Merged |
|---|---|---|---|---|
| 1 | FACADE-4 step 0: baseline, three-table census, DDL/Arrow goldens; no product change | PR open, not merged: the stop time came before the per-PR critic-logic round | #579 | — |
| 2 | FACADE-4 step 1 (the consolidation) | Not opened: it waits on the owner rulings Q-R13-1..10 (§4) and the stop time | — | — |
| 3 | FACADE-5 (display renderer in Rust) | Not opened: the stop time. The step-0 brief is written (§5) | — | — |

## 2. FACADE-4 step 0 — the baseline

Release native (`__debug_assertions__` False), warmup + 5 reps, medians, idle-box wait before each cell, every cell under
`systemd-run --user --scope -p MemoryMax=8G`. Doc: `docs/perf/facade-4-types-baseline-2026-09-14.md`.

| cell | measured | wall bar | wall? |
|---|---|---|---|
| (a) `repark_type_to_arrow` per call | 7.1–59.9 µs (flat 7 → wide 50) | ≥ 1 ms per user call | no |
| (a) calls per 1e5 op (spy) | 0 on `createDataFrame(pandas)`, `collect`, `to_arrow`, `show`, `df.schema`; 16 on nested `createDataFrame(rows, StructType)` ≈ 0.16 ms of 2,089 ms | ≥ 5 % of the wall | no (0.008 %) |
| (b) `struct_type_from_arrow` round-trip | 8.2–99.3 µs; wide 50 round-trip 134 µs | ≥ 1 ms | no |
| (c) DDL parse (`fromDDL` / `_parse_datatype_string`) | ≤ 125 µs (wide 50); atoms 1.65–8.65 µs | ≥ 1 ms | no |
| (c) DDL write (`simpleString` / `toDDL`) | ≤ 25 µs | ≥ 1 ms | no |
| (d) cProfile share, `df.schema` | 0.033 of 0.066 ms | ≥ 5 % | ratio yes; absolute 33 µs per call |
| (d) cProfile share, `createDataFrame(pandas)` 1e5 | 0 of 1,650 ms | ≥ 5 % | no |
| (d) cProfile share, `read.csv(inferSchema)` 1e5 | 0.14 of 2,270 ms (0.006 %) | ≥ 5 % | no |

**Verdict: no wall.** Step 1 is option (B): ship FACADE-4 as the correctness consolidation, with no perf claim. There is no
before/after table because step 0 changes no product code. The only ratio trip is `df.schema`, where 33 µs of a 66 µs
call is conversion. A Rust table cannot move a user-visible number there.

Pins: `python/repark/tests/test_facade_4_ddl_round_trip.py` + `facade_4_type_goldens.json` cover 47 golden cases (29
atomic, 16 complex, a raw-Arrow probe and the session conf) and an `isinstance` pin. Record mode is refused under CI. The
mutation proof: dropping the tz from `pa.timestamp("us", tz="UTC")` in `repark_type_to_arrow` reds four cases, and the
restore turns them green (ledger C-004).

### The three-table census (measured, none fixed)

8 agree rows, 21 disagreement rows (ledger `task/ledgers/staging/facade-4-ledger.md`, D1–D21). By family:

| family | rows | the split |
|---|---|---|
| timestamps | D1, D2 | the facade and `_csv_smart` distinguish `timestamp_ntz`; `read.csv(inferSchema)` answers `timestamp` even under `spark.sql.timestampType=TIMESTAMP_NTZ` |
| CSV literal lattice | D3, D4, D5 | `_csv_smart` rungs give `decimal(3,2)` / `decimal(18,18)` / `int`, but the served infer gives `double` / `double` / `bigint`, so the precise rung table does not serve `read.csv` |
| decimals > 38 | D6 | `decimal256(76,10)`: the facade reads it in, then `repark_type_to_arrow` refuses it; `_csv_smart` → `double`; the reader → `decimal(76,10)` |
| binary, float32 | D7, D8, D18, D19 | the reader is two answers: the scan says `string` / `double`, `DESCRIBE` says `binary` / `float` |
| small and unsigned ints | D9, D10 | the facade `tinyint` / `smallint` and `string` for `uint*`; the reader collapses all of them to `int` |
| intervals and unsupported Arrow | D11–D14, D20, D21 | the facade degrades to `string`; `fromDDL` refuses `interval day to second`; `type_key` leaks raw Arrow names |
| nested nullability | D15, D16, D17 | `toDDL` emits `NOT NULL`, which `fromDDL` refuses; `containsNull=False` and `valueContainsNull=False` are dropped silently in both directions |

Step 1 routes 11 facade and 2 Rust entry points through one Rust table (the ledger's step-1 target list).

## 3. Reviewer verdicts

| round | tier, cost | verdict | outcome |
|---|---|---|---|
| S2-21 Rust / Python perf reviewers | — | not run | Step 0 changes no product code and makes no perf claim, so there is nothing to review for speed |
| critic-logic (one per PR) | — | **not run** | The stop time came first. The PR stays unmerged until the owner has it run or waives it for a measurement-only step |
| orchestrator audit | — | pass | 5 commits (+1 orchestrator attestation commit), files inside Home, 0 added comment lines, every actor commit carries the Devin `Authored-By` trailer and none carries a forbidden form, 0 lines under `python/repark/src` or `crates/` |
| orchestrator gates, rebased on `e147685b` | — | pass: 184 passed, 7 skipped; `make verify` exit 0 on head `7b423721` | nine pin files + `test_production_file_size.py`, `make verify` |

## 4. Owner questions, with recommendations

FACADE-4 step 1 cannot unify the three tables until these are ruled. Each question comes from the census with the actor's
lean, and I agree with every lean unless a line says otherwise.

- **Q-R13-1 — timestamps (D1/D2).** Should `read.csv(inferSchema)` honor `spark.sql.timestampType=TIMESTAMP_NTZ`?
  Recommendation: yes. Spark does, and both other tables already distinguish NTZ.
- **Q-R13-2 — binary (D7/D19).** Is `string` on the scan surface a contract or a bug? Recommendation: measure against
  the Spark oracle before ruling. Spark reports `binary` on both surfaces, so the per-surface split the actor leans to
  would keep a known divergence; I lean to `binary` everywhere, pending the oracle.
- **Q-R13-3 — float32 (D8/D18).** `float` or `double`? Recommendation: `float` on every Spark surface (Spark parity);
  the widening stays a physical detail.
- **Q-R13-4 — unsigned Arrow ints (D10).** Recommendation: widen to the signed family (`uint8`/`uint16` → `int`,
  `uint32`/`uint64` → `bigint`, which avoids overflow), not the `string` degradation. This differs from the actor's lean
  on `uint32`.
- **Q-R13-5 — small signed ints (D9).** Recommendation: `tinyint`/`smallint` (the actor's lean); Iceberg's widening to
  `int` is a storage fact.
- **Q-R13-6 — decimal precision > 38 (D6).** Recommendation: cap at 38 on the facade surface and refuse beyond it the way
  Spark does; the reader keeps its internal spelling.
- **Q-R13-7 — the CSV literal lattice (D3–D5).** Recommendation: adopt Spark's ladder (the actor's lean) and retire the
  `_csv_smart` rung table into the Rust lattice. This is the largest behavior change in the unit, so it should be its own
  pinned slice.
- **Q-R13-8 — intervals (D14).** Recommendation: keep the `string` degradation and the `fromDDL` refusal for now (the F1
  freeze pins them); file the Spark-shaped interval classes as their own card.
- **Q-R13-9 — nested nullability (D15–D17).** Recommendation: yes, preserve `containsNull` and `valueContainsNull` and
  teach `fromDDL` `NOT NULL`. Today's loss is silent, and PySpark accepts both.
- **Q-R13-10 — unsupported Arrow types (D11–D13, D20).** Recommendation: keep `string` on the facade surface; `type_key`
  keeps its internal spelling.
- **Q-R13-11 — critic-logic on a measurement-only step.** The per-PR critic round did not run on #579. Recommendation:
  run it anyway (Grok, about $0.5) before the merge, because the goldens become binding for step 1.

## 5. What remains of the Rust switchover (measured on `main` at `e147685b`)

`python/repark/src/repark/spark/` holds 51,790 Python lines.

| next unit | files | lines | state |
|---|---|---:|---|
| FACADE-4 step 1 | `types.py` 1,834 · `session/timestamp_type.py` 97 · DDL writers in `create_dataframe_schema.py` 692 · `_csv_smart.py` 899 | ≈3,522 | MEASURED tonight: no wall, so it ships as a correctness consolidation; blocked on Q-R13-1..10 |
| FACADE-5 | `dataframe/display.py` 514 · `dataframe/plan_collapse.py` 1,057 (formatters ≈250 of it) · `dataframe/eager.py` 129 | 1,700 | UNMEASURED; step-0 brief written (fetch and format legs timed apart, census of unbound renderer × truncation pairs, goldens only for the unbound pairs) |
| FACADE-3 residue | `session/create_dataframe_{rows,inference,schema,tuples,values,arrow,columns}.py` | 4,308 | no measured wall in scope (run 12) |
| FACADE-6+ roll-call | `joins_columns.py` 1,238 · `writer_readwriter.py` 1,111 · `reader.py` 1,022 + `reader_support.py` 492 · `functions_udf.py` 1,300 · `udf_bridge.py` 471 + `udf_projection.py` 349 + `udf_schema.py` 64 + `grouped_udf.py` 145 · `session/sql_udf*.py` 2,633 | 8,825 | measure-first, unopened |

## 6. Decisions taken under G-2

- R13-D-1: the step-0 brief from run 12 was reused with the date moved to 2026-09-14, a time box (commit each deliverable
  as it completes), and the attribution and comment fences restated.
- R13-D-2: no S2-21 perf reviewers on step 0. It changes no product code and makes no perf claim, so the reviewers
  would have nothing to review.
- R13-D-3: with no time left for the critic-logic round, FACADE-5 was not opened and the step-0 PR is left unmerged
  (runbook §8: finish or park open lanes).
- R13-D-4: the orchestrator added the COVERAGE_ATTESTATION block (all eight clauses PROVEN) in its own commit, signed
  with its own `Authored-By` line.

## 7. Spend

| tier | rounds | cost |
|---|---|---|
| Devin SWE-2 (free) | 1 on session `amused-albacore` (86 agent steps, 93 tool calls, 05:27 → 06:20) | $0 |
| Grok 4.6 | 0 | $0 |
| Muse / GLM | 0 | — |

No incidents. The brief named the FACADE-3 ledger at its old `staging/` path (it moved to `completed/`); the actor found
it.

## Pointers

- Up: [map.md](map.md) · Ledger: [../../ledgers/staging/facade-4-ledger.md](../../ledgers/staging/facade-4-ledger.md)
- Perf: [../../../docs/perf/facade-4-types-baseline-2026-09-14.md](../../../docs/perf/facade-4-types-baseline-2026-09-14.md)
- Audit: [../epic-term/facade-audit-2026-09-10.md](../epic-term/facade-audit-2026-09-10.md) · Previous: [overnight-report-2026-09-13-run12.md](overnight-report-2026-09-13-run12.md)
