# Sketch ICE-TSNS-TYPE-PLANNER-1: the SQL type name decides the unit, so a nanosecond cast means nanoseconds

**Date:** 2026-10-13. **Author:** Fable 5.1 (architecture guidance under the major-blocker rule). **Status:** sketch, halts before code; builds only on the owner's adopt. **Parity rows:** R-017 (primary); R-010, R-013, R-018(1) measured against the same head. **Closes unmerged:** #1027 (ICE-TSNS-NARROW-REFUSE-1), #1042 (ICE-TSNS-COERCION-1). **Grade:** A. **Target:** 1.5.6.

## 1. The defect in one paragraph

DataFusion 54.1 maps the SQL type name `TIMESTAMP` to `Timestamp(Nanosecond, None)`. RePark's session rule, `rewrite_cast` in `crates/repark-functions/src/instant_ts.rs`, therefore reads every `Expr::Cast` whose target is a nanosecond timestamp as Spark's `TIMESTAMP` and rewrites it to a microsecond instant in the session zone (`wrap_as_ltz`), and reads a nanosecond source cast to a naive nanosecond target as a user narrowing (`narrow_timestamp_ns_expr`). When the planner coerces `coalesce(ns, NULL)` it inserts `CAST(NULL AS Timestamp(ns))`; the rule cannot tell that cast from one the user wrote, turns it into microseconds, and the nanosecond sibling narrows to match. Two designs tried to tell the planner's cast from the user's by walking the plan (#1027 marker functions, #1042 feed rewrite); seven verifier rounds each found a new plan shape. The set of plan shapes is open. The set of SQL type names is closed.

## 2. The seam

DataFusion 54.1 consults a registered `TypePlanner` before its own type mapping, in `convert_data_type_to_field` (datafusion-sql `planner.rs` 648–657), and that function is on the path of both `CAST(x AS T)` (`expr/mod.rs` 303, 321) and typed literals `T '…'` (`expr/mod.rs` 1050). RePark registers none today. The session state is built once, in `crates/repark-core/src/session/df_guards.rs` line 73 (`SessionStateBuilder::new()`), and a second builder in `crates/repark-python/src/column/expr_build.rs` line 286 plans DataFrame expression strings; both take `.with_type_planner(…)`.

RePark registers one planner, `SparkTypePlanner`:

| SQL type name (both doors) | Arrow type today | Arrow type with the planner | Spark type |
|---|---|---|---|
| `TIMESTAMP`, `TIMESTAMP_LTZ` | `Timestamp(ns, None)` | `Timestamp(µs, Some(session zone))` | TIMESTAMP |
| `TIMESTAMP_NTZ` | `Timestamp(ns, None)` | `Timestamp(µs, None)` | TIMESTAMP_NTZ |
| `timestamp_ns` | `Custom`, lowered to a UDF call by `keyword_lower.rs`, only from `spark_ast.rs:193` | `Timestamp(ns, None)` | none (Iceberg v3) |
| `timestamptz_ns` | same | `Timestamp(ns, Some("UTC"))` | none (Iceberg v3) |
| every other name | DataFusion default | unchanged (planner returns `None`) | — |

After the planner, a `Timestamp(ns, _)` target in any `Expr::Cast` means nanoseconds, because no SQL name maps to it except the two nanosecond names. These arms of `rewrite_cast` come out: `targeting_ns && is_ltz_timestamp(source) => peel`, `targeting_ns && (int or null or seconds) => wrap_as_ltz`, and `source ns && target ns None => narrow`. `keyword_lower::lower_timestamp_ns_casts` and the by-name kernel registration behind R-013 retire; the wall kernels (`timestamp_ns_cast_expr`) are applied by the analyzer from the *type* of a cast target, not from a spelled name, so `UPDATE … SET`, `DELETE … WHERE` and the ANSI door get them without lowering (R-010, R-013).

The owner's rules stay exactly as ruled: a cast the user wrote to `TIMESTAMP` over a nanosecond value is now an ordinary microsecond cast and floors (10-10 narrowing rule, Q-21c-8 floor); a NULL beside a nanosecond value takes the nanosecond type by DataFusion's own coercion (10-12 widen rule); out of range raises in both ANSI modes; nothing falls back silently.

## 3. Step 0, the inventory: every nanosecond producer that is not a `timestamp_ns` column

Run on main before any code, on both doors, and committed as a table in the unit ledger. A producer with no disposition halts the unit. The starting list below is measured from main `f8f1d252`; step 0 completes it mechanically.

| # | Producer | Where (main) | Measured today | Disposition |
|---|---|---|---|---|
| P1 | `CAST(x AS TIMESTAMP)`, `CAST(x AS TIMESTAMP_NTZ)` | DataFusion default mapping | `Timestamp(ns, None)` target, rewritten by `instant_ts::rewrite_cast` | Planner maps to µs; the rewrite arms for ns targets are deleted |
| P2 | Typed literals `TIMESTAMP '…'`, `TIMESTAMP_NTZ '…'` | same path, `expr/mod.rs:1050`; today caught by `wrap_ns_literal` | ns literal | Planner maps to µs; `wrap_ns_literal` shrinks to the cases step 0 still finds |
| P3 | `CAST(x AS timestamp_ns)` / `timestamptz_ns` | `keyword_lower.rs`, called only from `spark_ast.rs:193` | lowered to a UDF call; UPDATE/DELETE and ANSI door miss it (R-010, R-013) | Planner maps to ns types; the analyzer wraps the wall kernel by target type |
| P4 | Planner-inserted coercion casts (`coalesce`, `nvl`, `array`, `CASE`, `if`, UNION, `array_append` coerce) | DataFusion type coercion | `CAST(NULL AS Timestamp(ns))` beside a ns value; today read as TIMESTAMP | Stays as DataFusion emits it; means ns. Pinned by the 9,600-cell census |
| P5 | DataFusion built-ins RePark does not override | `datafusion::functions::datetime` still registered under RePark's overrides | step 0 enumerates: for every registered scalar UDF, call `return_type` over `[Timestamp(µs,tz)]`, `[Timestamp(µs,None)]`, `[Utf8]`, `[Int64]`, `[Date32]`, `[Null]`; list every ns return | Each by name: override with the Spark function if Spark has the name; refuse by name if Spark does not (`to_timestamp_nanos`, `to_local_time`, `date_bin` with ns inputs); keep if it returns its input unit (`date_trunc` override already does) |
| P6 | `now()`, `current_timestamp`, `localtimestamp` | `instant_ts` overrides `now`/`current_timestamp`/`to_timestamp`; `bare_nullary.rs` refuses bare `now`/`localtimestamp` | µs | Stays; step 0 confirms the override wins registration order |
| P7 | Interval and date arithmetic | Arrow kernels | unit of the timestamp operand | Stays; ns only when the operand is ns (correct) |
| P8 | DataFrame door type names (`"timestamp"`, `TimestampType()`, `.cast("timestamp")`) | `python/repark/src/repark/spark/types.py:1377,1379` → µs; expression strings planned through `expr_build.rs:286` | µs on the type map; strings go through DataFusion's SQL mapping | The second `SessionStateBuilder` takes the same planner; one pin per spelling |
| P9 | Arrow ingest: pandas/polars ns | frame door | floored to µs (R-019, FIXED 10-13) | Stays |
| P10 | Arrow ingest: Parquet, Iceberg v3 ns columns, CSV/JSON inference, Postgres reads | readers | real ns from Iceberg v3; others step 0 measures | Real ns stays ns; any inferred ns from text readers is a finding with its own row |
| P11 | Python UDF returns (`pandas_udf`, `applyInPandas`, `mapInPandas`, `mapInArrow`) | pandas → Arrow conversion | datetime64[ns] arrays copied tick for tick (NS-ARRAY-FLOOR-1, 1.5.6) | Out of this unit; the card's floor stays its own unit |
| P12 | `VALUES` rows and `INSERT … VALUES` conform | `timestamp_ns_cast::conform_values_timestamp_columns` | rewrites rows to the column type | Re-measured; expected to shrink (R-018(1)) |
| P13 | The ANSI door (`repark-sql`) | shares `instant_ts` through `repark-functions` registration | same rule, same defect | Same planner, registered once in `df_guards.rs`; the ANSI door's own type names checked by step 0 |
| P14 | Cached frames (`cache()`, the 420 cells of ICE-TSNS-CACHE-1) | cache materialization | cut on main by the cache alone | Re-measured; the planner should make the cache a pure pass-through; any residue keeps the absorbed card's row |

The mechanical enumeration for P5 is a Rust test that walks `ctx.state().scalar_functions()` and prints `(name, inputs, return)` for every ns return; its output is committed as the inventory's evidence file.

## 4. Halt criteria (the sketch stops and reports; no code)

- H1: a P5 built-in that returns ns, has a Spark name, and has no Spark-semantics replacement in `datafusion_spark` or `repark-functions`.
- H2: `SessionStateBuilder::with_type_planner` is not on the path of the Spark door's planning (step 0 proves it with a planner that maps `TIMESTAMP` to a marker type and a `SELECT CAST(1 AS TIMESTAMP)` that must fail).
- H3: DataFusion's coercion of `(Timestamp(µs, tz), Timestamp(ns, None))` narrows to µs anywhere the owner's widen rule says ns; measured on `coalesce`, `UNION`, `array`, `CASE`, comparison and join keys. If it narrows, the unit needs one custom coercion rule for the timestamp lattice, by type, and the sketch names it before the build starts.
- H4: any P10 reader infers ns from text; that is a separate row, not a reason to keep the rewrite arms.

## 5. Pins and the verifier

- The fixture, committed before the first code commit: the 9,600-cell census of #1042 (categories derived by the harness from the rule "NULL beside ns is ns; a written TIMESTAMP cast floors"), the 117 reads, the 90 consumer cells, the seven rounds' repros (#1027 verify 1–3, #1042 verify 1–4) as named cells, the 420 cache cells, the R-010/R-013/R-018(1) cells.
- Blast-radius gates: the full facade suite, the 842-cell Spark + Iceberg matrix live, `make verify`, the parity-live job on the branch, write-cost medians within the owner's 1.05 gate on the write set, read-cost medians within 1.05 on the TPC-style set under `docs/perf/`.
- Mutants, each must die: planner returns `None` for `TIMESTAMP`; planner maps `timestamp_ns` to µs; the deleted `wrap_as_ltz` arm restored; the deleted narrow arm restored; the ANSI door built without the planner.
- One Opus 5.5 high verifier, fresh, scoped to the fixture plus the blast-radius gates; FAIL parks the unit with no second fold.

## 6. What the executor may not do

No plan-walk, marker function, feed rewrite or reader accounting of any kind; the unit is refused at review if one appears. No comments in Rust or Python (owner ruling 2026-08-26; relocated code sheds its comments). No edits on `#1027` or `#1042`; they close by pointer. No change to any numeric gate. No new SQL type names. No `unsafe`.

---

# Brief for the orchestrator: ICE-TSNS-TYPE-PLANNER-1

Card: `task/roadmap/mid-term/ice-tsns-type-planner-1-card-2026-10-13.md` (clerk files it from this sketch, docs PR, no verifier). Work order grade A under `task/wo/ice-tsns-type-planner-1/`.

**Sequence.**
1. Clerk docs PR (Muse medium): file the sketch under `task/wo/ice-tsns-type-planner-1/sketch.md`, the card, the R-017 park record on main's registry naming this unit as next step, the closing comments on #1027 and #1042, the lessons entries (provisional anchors, rounds per finding class, no unit from an integer). Trailer on every commit as the attribution rule says.
2. Step 0 (Muse xhigh, one lane, no product code): the inventory of §3 as a committed table plus the P5 enumeration test output; the fixture of §5 committed red-first; the H2 and H3 probes with their printed results. Hand back the table. The orchestrator checks H1–H4 and pastes the owner the one-paragraph result before step 1 opens.
3. Step 1 (one Opus 5.5 high executor, CARGO_BUILD_JOBS=6, inside repark.slice): `SparkTypePlanner` in `repark-functions`, registered in `df_guards.rs` and `expr_build.rs`; delete the three `rewrite_cast` arms; retire `keyword_lower` nanosecond lowering and the by-name registration; wall kernels applied by target type in the analyzer; P5 dispositions by name. Commit order: planner, deletions, analyzer, P5, in four commits, each green on the fixture slice it owns.
4. CI green on the branch before any verifier (owner rule). Then one scoped Opus 5.5 high verifier per §5. PASS merges through the queue for 1.5.6; FAIL parks, and the owner hears before anything else happens.

**Lane rules in force.** Muse lane cap 4; non-Opus lanes on Muse until Wed 10-14 08:00 EDT, then standing tiers; one cargo build per orchestrator; verifier probes inside the slice or under `ulimit -v 67108864`; forbidden-pattern scan before every commit and PR body; CI green before any verifier; the proportionality rule counts rounds against R-017, which is already at its bar, so this unit gets exactly one fold if the verifier names a seam-consistent fix and zero if it names a new reader shape.

**Hand-back from every stage:** head sha, the fixture counts (red on main, green on head), the gate list with exit codes, cost medians against 1.05, any halt hit by letter, nothing else.
