# Run 13 report — the FACADE half (2026-09-14, early morning)

**Unit:** overnight-13 · **Orchestrator:** Opus 5 (named by the owner) · **Beside:** run 13b (overnight-13b: ARRAY-NULL-1,
ANSI-DOOR-1, REPLACE-LINEAR-1 step 1) on the same box · **Grants:** G-1 yes, G-2 yes, G-3 stop by 06:30 or when the list
is exhausted, G-4 Devin actor + Grok critic/reviewers, G-5 yes · **Runbook:**
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md)

## 1. Outcome

The session started at 05:19, which left a 71-minute window before the 06:30 stop. **That stop was set in error; the run
continued as a day run to 12:00. See [Day continuation](#day-continuation-operator-correction-2026-09-14),
which supersedes the states in this table.**

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

## Day continuation (operator correction, 2026-09-14)

The 06:30 stop was set in error: the run was launched at 05:18, so the night window was about 70 minutes. The
operator reset G-3 to "stop at 12:00 local or when the list is exhausted" and kept every other grant and rule. The
run resumed at 06:27 from where §1 stopped.

### D-1. FACADE-4 step 0 — merged

| event | when | result |
|---|---|---|
| Grok critic-logic, round 1 | 06:29–06:42 | **NEEDS_REMEDIATION.** P1: L-001 D10 put `uint64` under `int` (the reader answers `long`/`bigint`); L-002 D2 said `_csv_smart` has no NTZ answer (it honors the session and answers `TimestampNTZType`); L-003 D21 missed the reader's `void`/`Null`; L-004 two agree rows mixed inputs and hid an offset-literal disagreement. P2: L-005..L-009, goldens that did not bind `containsNull`/`valueContainsNull`, inner-struct nullability, inbound `int8`/`int16` or the session zone, plus a spy on a name `collect`/`show`/`df.schema` never call. P3: L-010, L-011 |
| Devin follow-up (session `amused-albacore`) | 06:49–07:46 | 4 commits: census D1–D24 re-measured with 18 single-input agree rows; goldens re-recorded (45 cases) with each critic mutation proven red; six-name spy; CI refusal for any non-empty `CI` |
| Grok critic-logic, re-check | 07:47–07:51 | **PASS.** Five P3 notes filed in the ledger (R2-P3-1..5) |
| orchestrator gates | 07:48 | nine pin files 184 passed / 7 skipped, `make verify` exit 0 |
| merge | 08:24 | #579 squash `30ca2ba1`, tree-equal |

What changed in the census after the critic: the unsigned-int row splits (D10 `uint8`/`uint16`/`uint32` → `int`, D22
`uint64` → `bigint`), and two new rows D23 and D24 appear. Q-R13-1 now reads "CSV infer disagrees with both other
tables under NTZ". The no-wall verdict stands. The spy correction shows `df.schema` makes one `fromDDL` and one
`_parse_datatype_string` call per op, 33 µs of a 66 µs call, which is still under the bar.

### D-2. FACADE-4 step 1 — the consolidation

**State: parked as draft PR #582** (branch `perf/facade-4-s1`, rebased on `main` `c9b03c67`). Devin session
`coherent-sheet`: one round 06:37–10:29, then a follow-up on the rebased scratch lane from 10:38, still running at the
stop.

What the round built:
- One Rust conversion table: `crates/repark-spark/src/type_table.rs` plus `type_table/parse.rs`, split at the
  1,000-line Rust ceiling rather than taking an exception row.
- `spark_type_names.rs` and `arrow_type_key` delegate to it, and `crates/repark-python/src/type_bridge.rs` is its only
  FFI consumer.
- `types.py`, `_csv_smart.py`, `timestamp_type.py`, `create_dataframe_values.py` and `create_dataframe_inference.py`
  thin to checks plus native calls.
- All 25 public classes, their signatures and `isinstance` are unchanged, and no D-row is unified.
- Five entry points stay on Python as residue rows (C-015): JSON round-trip, foreign `DataType` subtypes, decimals
  outside the Arrow FFI scale envelope, the evaluated-collation refusal, and `_sql_type_to_arrow`'s decimal/nested
  spellings.
- The actor's gates passed on the pre-rebase tree: whole facade suite 6,098 passed / 365 skipped, `cargo test -p
  repark-spark -p repark-python` green, `make verify` exit 0.
- Ceilings moved down: `types.py` 1,834 → 1,833, `dataframe.rs` 1,084 → 1,019.

Before and after, with the base `main` release native and the branch timed back to back
(`docs/perf/facade-4-types-step1-2026-09-14.md`):

| cell | base | branch | Δ |
|---|---:|---:|---:|
| `createDataFrame(pandas)` 1e5 | 478.2 ms | 479.8 ms | +0.3 % |
| `createDataFrame(rows, StructType)` nested 1e5 | 2,149.9 ms | 2,098.1 ms | −2.4 % (conversion calls 16 → 6) |
| `collect` 1e5 | 349.7 ms | 350.2 ms | +0.1 % |
| `show` 1e5 | 4.02 ms | 3.36 ms | −16.2 % |
| `repark_type_to_arrow` wide50 | 60.7 µs | 170.2 µs | +109.5 µs |
| `struct_type_from_arrow` round-trip wide50 | 133.9 µs | 330.5 µs | +196.5 µs |
| `fromDDL` wide50 | 125.7 µs | 83.9 µs | −41.7 µs |
| `toDDL` wide50 | 82.0 µs | 6.1 µs | −76.0 µs |
| `simpleString` wide50 | 19.5 µs | 107.3 µs | +87.8 µs |

The round also fixed its own first-pass `fromDDL` regression, 100–300× slower from a per-call `Regex::new`, before
committing.

| review | tier, cost | verdict | outcome |
|---|---|---|---|
| S2-21 Rust perf | Grok 4.6, $0.88 | **P1** `DataFrame.dtypes` +106 % on wide50 (60.5 → 124.7 µs), +80 % flat7: every atomic `simpleString()` now crosses PyO3. P2 un-interned `PyDict` descriptors. P3 collation `to_string`, no parse depth cap (base has none either), refusals parse twice | Devin follow-up running at the stop: constant atomic tokens back in Python with an agreement pin, `intern!` keys, a static collation. The depth cap is not added (it would change a refusal) and is filed as an owner question |
| S2-21 Python perf | Grok 4.6, $0.92 | no P1; every official 1e5 cell within ±5 %. P2 constants rebuilt through the native path (`IntegerType.simpleString` 0.055 → 1.30 µs), class trees rebuilt after Rust has them, a throwaway `DataType` in the CSV rung tokens | the first P2 is in the running follow-up; the other two go to the next round |
| critic-logic | — | not run | stopped deliberately: the follow-up changes the code under review, and the review lane went to FACADE-5 so it could merge today |

Not mergeable yet. What remains: land the follow-up, re-check both perf reviews, run critic-logic, then run the gates on
the rebased head.

### D-3. FACADE-5 step 0

**State: merged as `e5cc10e1` (#583), tree-equal**, after CI and the critic PASS. Devin session `sugar-sauce`: step 0 08:22–09:24, follow-up 09:55–11:17. There was no second build:
the lane ran on the release native of `main`'s product in `/tmp/f-types4/.venv` (R13-D-7).

Fetch and format timed separately, over 96 cells: six renderers, n = 20 and n = 1000, truncate on and off, and flat,
wide-50 and nested frames (`docs/perf/facade-5-display-baseline-2026-09-14.md`). Every split door's bytes equal the
public call's.

| cell family | fetch | format | format share | wall? |
|---|---:|---:|---:|---|
| n=20, spark doors (ASCII, vertical, eager `repr`, HTML) | ≈0.4–2 ms | 0.4–3.2 ms | 37–57 % | yes (share) |
| n=20, polars `wide50` `mr=10` | — | 1.6–1.7 ms | 14–15 % | yes (≥ 1 ms) |
| n=1000, flat | 0.4–0.8 ms | 17–25 ms | 92–98 % | yes |
| n=1000, 50-column | 2.8–9.5 ms | 59–107 ms | 92–98 % | yes |
| n=1000, nested | 0.4–2.0 ms | 22–33 ms | 92–98 % | yes |

**Verdict: a measured format wall.** Format cost grows with rows × columns (the per-cell `str` / `_cell_text` /
`to_pylist` work), while fetch is a `limit` plus Arrow export and stays nearly flat. So unlike FACADE-4, step 1 of
FACADE-5 is a real perf move:
- **Moves to Rust, byte-identical:** the five grid formatters in `plan_collapse.py`, the per-cell pipeline in
  `polars_cells.py`, and the `_repr_html` body.
- **Stays in Python:** the fetch leg, the conf reads, and every `eager.py` door, which becomes a thin wrapper.

Census: 6 renderers × 6 truncation rules; 34 golden cases now bind the pairs that no §8 pin bound, each proven with a
mutation (M1–M16).

| review | tier, cost | verdict | outcome |
|---|---|---|---|
| critic-logic, round 1 | Grok 4.6, $2.05 | NEEDS_REMEDIATION. P2 L-001/L-002: zero-column vertical and HTML doors unbound, and they print phantom rows from the `Table.slice` pad. P2 L-003: the ledger overclaimed that the HTML footer pin counts rows. P2 L-004: polars nested × truncate unbound. P3 L-005/L-006 | follow-up: phantom-row bytes pinned and filed as owner question F-L2 (Q-R13-12); a `<tr>` count test; polars nested truncate cases; a timer-scope note; record mode refused for any non-empty `CI` |
| critic-logic, re-check | Grok 4.6, $1.46 | **PASS** | every finding closed; the new goldens bite the slice-pad and nested-truncate mutations |
| orchestrator audit and gates | — | pass | 11 commits with the Devin trailer, 0 comments, 0 product lines, `.shim/` untracked; §8 pins unedited + goldens + file size 113 passed; `make verify` on the rebased head |

S2-21 reviewers do not apply, because step 0 changes no product code.

### D-4. Decisions taken under G-2 (day)

- R13-D-5: step 1 keeps every surface's current answer byte-for-byte, and the table carries a per-surface column
  wherever the census disagrees. Unifying any D-row changes a Spark-visible answer, so it waits on Q-R13-1..10
  (runbook §6: a semantic change parks).
- R13-D-6: the critic's findings on a measurement-only step went back to the actor before the merge. The census is
  step 1's pin list, so a wrong row there would have become a wrong unification target.
- R13-D-7: FACADE-5 step 0 runs without a second build, on `/tmp/f-types4/.venv`: a release native whose product
  files equal `main`'s. That kept one build lane (FACADE-4 step 1) while both units progressed.
- R13-D-8: the round-2 P3 notes on #579 were filed in the ledger rather than fixed, and the step-1 pins close three of
  them.

### D-5. Spend (day)

| tier | rounds | cost |
|---|---|---|
| Devin SWE-2 (free) | 5 finished: `amused-albacore` step-0 follow-up 1; `coherent-sheet` step 1 (106 steps) plus a follow-up still running at the stop; `sugar-sauce` FACADE-5 step 0 (53 steps) and follow-up 1 | $0 |
| Grok 4.6 | critic-logic on #579: round 1 $0.83, re-check $1.25; S2-21 Rust perf on step 1 $0.88; S2-21 Python perf on step 1 $0.92; critic-logic on FACADE-5 step 0: round 1 $2.05, re-check $1.46 | $7.39 |
| Muse / GLM | 0 | — |

Incidents, all recovered:
- INC-R13-1: `git push --force-with-lease=<branch>` from a clone that pushes by URL was rejected as "stale info"
  (there is no remote-tracking ref). A plain fast-forward push went through.
- INC-R13-2: the first FACADE-5 rebase stopped on `python/repark/tests/map.md` and the second on
  `task/ledgers/staging/map.md`, after run 13b's #577 moved its ledger. Both were resolved by keeping both entries
  and dropping the moved ledger's row, then grepping for conflict markers.
- INC-R13-3: step 1's trial rebase first replayed step 0's pre-squash commits. It was redone as
  `rebase --onto origin/main 7b423721`.
- INC-R13-4: the step-1 review chain was stopped after the two perf reviewers so the single review lane could run
  FACADE-5's re-check before the stop. The Grok rounds themselves were not interrupted.

### D-6. Owner questions added (day)

- **Q-R13-12 — FACADE-5 zero-column phantom rows (critic L-001/L-002, ledger F-L2).** On a frame with no columns,
  vertical `show` prints `n` `-RECORD` blocks and `_repr_html_` prints `maxNumRows` body rows, because
  `pyarrow.Table.slice(0, n)` pads a zero-column table. ASCII `show` prints the real row count. The new goldens pin
  today's bytes. Recommendation: fix the bound in FACADE-5 step 1 as its own pinned slice, with a changelog line (the
  row is B2-unfrozen), rather than port a pyarrow artefact into Rust.
- **Q-R13-13 — DDL parse depth cap (step-1 Rust review P3).** Neither base nor branch caps DDL nesting; both raise
  `RecursionError` around depth 1,000. Recommendation: cap at the existing `SPARK_TYPE_NAME_MAX_DEPTH` (32) with a
  loud, typed refusal in a follow-up card. It changes a refusal, so it needs your yes.
- **Q-R13-14 — step-1 per-call cost.** The consolidation makes µs-scale conversions 2–3× slower. Every end-to-end cell
  stays within 5 %, and `dtypes` is being fixed. Recommendation: accept a µs-scale per-call cost for one table, as
  long as every named surface (`dtypes`, `df.schema`, `printSchema`, DESCRIBE) stays within 5 % of base; the
  reviewers' remaining P2s go into the next round rather than blocking.
- **Q-R13-1..10 still open.** The FACADE-4 unification (NTZ in CSV infer, binary/float32 per surface, unsigned and
  small ints, decimal > 38, the CSV literal lattice, intervals, nested nullability, unsupported Arrow types) is the
  follow-on to step 1. The table's per-surface columns make each ruling a one-row change with its census pin.

## Pointers

- Up: [map.md](map.md) · Ledger: [../../ledgers/staging/facade-4-ledger.md](../../ledgers/staging/facade-4-ledger.md)
- Perf: [../../../docs/perf/facade-4-types-baseline-2026-09-14.md](../../../docs/perf/facade-4-types-baseline-2026-09-14.md)
- FACADE-5: [../../ledgers/staging/facade-5-ledger.md](../../ledgers/staging/facade-5-ledger.md) · [../../../docs/perf/facade-5-display-baseline-2026-09-14.md](../../../docs/perf/facade-5-display-baseline-2026-09-14.md)
- Audit: [../epic-term/facade-audit-2026-09-10.md](../epic-term/facade-audit-2026-09-10.md) · Previous: [overnight-report-2026-09-13-run12.md](overnight-report-2026-09-13-run12.md)
