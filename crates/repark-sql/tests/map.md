# map — repark-sql/tests

## Purpose

Integration tests for the ANSI door. The end-to-end door battery lives IN the crate
(`../src/tests.rs`) because it drives `crate::execute` against an `EngineContext` directly; what
belongs out here is what must be observed from outside the crate.

## Contents

- `parser_productions.rs` — the R1 spike, kept as assertions. Pins that every production M1
  depends on parses on the stock DataFusion-re-exported sqlparser (Generic dialect), and that the
  three PR-6 productions (`ALTER … SET PROPERTIES`, `ALTER … EXECUTE`, `FOR … AS OF`) still do
  NOT — so PR-6's recognizer obligations stay honest, and an upstream parser change is a visible
  signal rather than a silent redundancy.

- `session_wiring.rs` — the door's REACHABILITY: `AnsiDialect` installed on a real
  `ReparkSession` through `ReparkSessionBuilder::with_sql_dialect`, driving schema DDL, CTAS,
  INSERT and a typed read through `session.sql`, plus a refusal that must survive the session
  boundary. It lives out here because "reachable through a session" is precisely what a unit
  test calling `AnsiDialect.execute(...)` on a bare `SessionContext` cannot show
  (`surfaces::SQL_DIALECT_SEAM`). **TZ-4 PR-1 A11:**
  `ansi_column_def_timestamp_still_rejects_ns_on_v2` — native ANSI
  `CREATE TABLE (ts timestamp)` still derives `timestamp_ns` (v2 reject); grant to
  `repark-sql/src/create_table.rs` not taken (wrote `timestamp_ns`, not `timestamp`).

- `introspection.rs` (PR-6, Q8) — `SHOW TABLES` / `DESCRIBE` / `information_schema` DELEGATED
  through the door, on a session whose `information_schema` was enabled the product way
  (`.config("datafusion.catalog.information_schema", "true")` — the repark-core R2 fix PR-6
  landed). Carries the negative half (without the conf the same door refuses, so the delivery is
  attributable to the fix) and — since **2026-08-10 (unit H-1c,
  [ADR-0006](../../../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md))** — the
  ANSI-door half of the metadata-table enumeration claim: `$`-suffixed metadata tables are hidden
  from `information_schema.tables` **and** from its twin `SHOW TABLES`, while staying queryable as
  `ns."t$snapshots"`. That row was formerly the opposite assertion
  (`metadata_tables_currently_enumerate_alongside_the_real_table`), left red-on-purpose so the
  decision could not be made silently; it was flipped in the same diff as the behavior. Also
  carries the leak pin: a `FOR … AS OF` read must leave no `__repark_ansi_tt_*` relation behind,
  which only became observable once this PR turned `information_schema` on. **Broadened in H-1b
  (2026-08-11):** the pin now asserts the `'__repark_tt%'` half too — the name
  `repark_core::read_table_at` registers under this door's view, which the ANSI-prefix filter was
  blind to by construction and which was still leaking (three pinned reads →
  `__repark_tt_1|2|3`). Added RED, then fixed in
  `../src/time_travel.rs::register_pinned_view`; the two `LIKE` patterns are disjoint, so neither
  half can go quiet again.

- `ta_toll.rs` (PR-6, Q11) — `TaExtension` on a **native** session, one kernel driven through
  ANSI-door SQL as a window function and compared `f64::to_bits` against the recorded C TA-Lib
  golden, plus the non-literal-period refuse and the "absent until you opt in" row. Needs the
  `repark-ta` dev-dep (feature `datafusion`).

- `cross_door.rs` (PR-6, Q13 / graft G5) — the **two-session** cross-door protocol: a native
  `AnsiDialect` session and a Spark-extended `SparkDialect` session, each over its OWN in-memory
  catalog, compared on the Arrow path (value AND type). Rows: CTAS, INSERT, ALTER (schema
  evolution + table rename), MERGE, time travel, identifier case folding, the single-session
  legality boundary (pure catalog DDL), the session-scope guard rail that explains why one
  session cannot do this job, **G-7b decimal128** (`cross_door_decimal_add_same_precision_scale_bit_exact`,
  `cross_door_decimal_mul_money_by_quantity_bit_exact` — same SQL through both doors, schema +
  nullability + raw i128 equal; corpus rows `add_same_precision_scale` /
  `mul_money_by_quantity`), **G12 three-valued logic** (`cross_door_tvl_true_and_null_is_null`,
  `cross_door_tvl_case_when_null_predicate` — portable SQL, Boolean/Int32 type + nullability +
  value equal across doors; corpus rows `and_true_null_is_null` / `case_when_null_predicate`;
  no Spark-only `<=>`), and — added 2026-08-11 —
  `cross_door_g3e8_refusals_render_identically` (ROW 9, restated 2026-08-13 over still-refused
  correlated IN / UPDATE IN / nested / scalar — IN / NOT IN / `[NOT] EXISTS` now execute) plus
  `cross_door_g3e8_not_in_delete_executes_identically` and
  `cross_door_g3e8_exists_delete_executes_identically` (executed columns), which compares a
  **rendered refusal string** rather than a result: the G3-E8 valve is implemented twice (no
  door→door product edge), and this is the only pin that can see the two copies drift, including
  the rendered TARGET that the per-door message pins cannot. **G11 (2026-08-12):** six **INTENDED** door-vs-door value
  divergences (correctness, not parity — Spark is not the ANSI oracle): integer `/` (truncate
  vs float), integer `/ 0` (raise vs NULL), float `/ 0` (IEEE +Inf vs NULL), decimal `/ 0`
  (raise vs NULL), default `ORDER BY ASC` (NULLS LAST vs FIRST), default `ORDER BY DESC`
  (NULLS FIRST vs LAST). Each row asserts both doors' actual Arrow outputs side by side; the
  one-sentence reason is the test's doc comment. Needs the `repark-spark` dev-dep — the ONLY
  place either door may name the other, and legal because the crate-DAG guard scopes layering
  to normal edges.

  Its case-folding row (`cross_door_identifier_case_folding_agrees_unquoted_and_diverges_quoted`)
  is a **declared-divergence test** as of H-1d (2026-08-10): it names the registry row it defends
  — [`../../../docs/spark-sql-iceberg-parity.md`](../../../docs/spark-sql-iceberg-parity.md) §3
  row ID-1, the registry's first declared row (decision D3) — and asserts the refusal *text*, not
  merely that an error occurred (`No field named` **and** the quoted `"ID"`: the class of failure
  and the identifier, because a bare `contains("ID")` also matches `INVALID` / `UUID` and would
  attribute nothing), so the row stays attributable to identifier resolution. It reds
  if the divergence silently disappears, which is the point: the registry holds the semantics and
  this test holds the registry honest.

- `session_timezone_ansi_door.rs` — **H-1a split B (2026-08-10):** the ANSI-door cell of the
  session-timezone matrix. On ONE Spark-extended session at a non-UTC zone, `sql_with(AnsiDialect)`
  and the Spark door return the same calendar fields, value AND Arrow type — a legal
  single-session row, because what is measured is that the DOOR does not change the answer
  (extensions are session-scoped). It also pins the honest negative: an extension-free session is
  stock DataFusion and reads the stored zone, which is a property of that profile rather than a
  Spark divergence. Needs the same `repark-spark` dev-dep as `cross_door.rs`, for the same
  reason — the reverse edge does not exist, so this cell can only be built here.

- `timestamp_cast_ansi_door.rs` — **TZ-5 (2026-08-12):** the ANSI-door cell of the
  `CAST(TIMESTAMP AS <numeric>)` epoch-seconds matrix, built here for the same crate-DAG reason as
  `session_timezone_ansi_door.rs`. On ONE Spark-extended session both doors scale a timestamp cast
  to epoch SECONDS, value AND Arrow type, including the negative FRACTIONAL second where Spark
  floors (`-0.5 s → -1`). Its second pin is the honest negative AND the class's revert-red
  evidence: a bare, extension-free session still returns the raw nanosecond tick
  (`-1800000000000`) — correct for a non-Spark session, and exactly what the Spark door returned
  before the fix. Ledger: `../../../task/tz5-cast-seconds-ledger.md`.

- `ansi_door_values.rs` — **G11 / Y-10 (2026-08-12):** ANSI-door-only value pins. Standard SQL
  is the oracle (ruling: correctness, not Spark parity). Six rows on a native `AnsiDialect`
  session, Arrow path, value AND type: CAST overflow raises, integer `/` truncates, integer
  `/ 0` raises, `SUM` skips NULLs, default `ORDER BY ASC` is `NULLS LAST`, implicit
  string→number coercion refuses. Does **not** edit `timestamp_cast_ansi_door.rs` or
  `session_timezone_ansi_door.rs`. Identifier case folding is registry ID-1 (cited, not
  duplicated). Ledger: `../../../task/y10-ansi-door-ledger.md`. Cargo wires this file as its
  own integration-test binary — this crate has no `tests/mod.rs`.

## Pointers

- Up: [../map.md](../map.md). Spike record: `../../../docs/history/port-v2/p2f-ansi-m1-ledger.md`;
  PR-6 record: `../../../docs/history/port-v2/p2g-ansi-m2-ledger.md`. Seam freeze + the session-scope rule:
  `docs/design/session-api.md`.

## Debug

| Symptom | First check |
|---|---|
| `m2_productions_still_need_a_pre_parse_recognizer` RED | Good news — upstream learned the form. Revisit the PR-6 plan, then update the pin |
| An M1 production stopped parsing | The matching handler is now unreachable; check the DataFusion/sqlparser version bump |
| `session_wiring` RED on the catalog-visible read | The dialect is probably not installed (session default fell back to `DataFusionDialect`, whose CTAS makes a `MemTable`) |
| `introspection` RED with "not supported unless information_schema is enabled" | The repark-core builder→`SessionConfig` plumbing (`apply_datafusion_config_keys`) regressed; check `cargo test -p repark-core --lib builder_datafusion` first |
| `cross_door` RED on ONE door only | The doors' lowerings drifted — that is the row doing its job (design §6 R3). Compare the two handlers, do not relax the assertion |
| `cross_door_g3e8_refusals_render_identically` RED | The duplicated G3-E8 valve drifted: one door's message text or its target derivation changed without the other's. Both copies are named in `task/g3e8-guard-ledger.md` D-1; fix the copy, never the assertion (this pin IS the mitigation D-1 accepted the duplication on) |
| `cross_door_identifier_case_folding_*` RED because a quoted wrong-case identifier now RESOLVES | repark has CONVERGED on Apache Spark. Retire `docs/spark-sql-iceberg-parity.md` §3 row ID-1 in the same change (a new dated decision supersedes D3) — never relax the assertion |
| a G11 `cross_door_integer_division_*` / `cross_door_order_by_*` / `cross_door_*_div_by_zero_*` row RED | an INTENDED door-vs-door split moved. Re-read `task/y10-ansi-door-ledger.md` §2 — do not silently retarget the ANSI half at Spark |
| `ansi_door_cast_overflow_int_to_tinyint_raises` RED because the CAST wraps or nulls | CAST overflow stopped raising. That is a correctness regression on the ANSI door; do not absorb it into F-Y10-1 (that finding is *arithmetic* wrap, not CAST) |
| `extensions_are_session_scoped_not_dialect_scoped` RED | Extension scoping changed. Every `TwoSession` matrix row in BOTH doors needs re-reading before anything else |
| `ansi_door_and_spark_door_agree_under_a_non_utc_session` RED on ONE door | The session timezone stopped being session-scoped (it rides `ConfigOptions`, which every door on the session shares). Check `repark_functions::session_time_zone` and `SparkExtension::configure` before touching the row |
| `a_native_session_without_the_spark_extension_reads_the_stored_zone` RED | A zone-aware extractor leaked into the extension-less profile — check what registers UDFs on a bare session |
| `ta_toll` RED on bit-exactness | Compare against `crates/repark-ta/tests/goldens.rs` first — if THAT is green, the divergence is in the window-UDF wrapper or the door, not the kernel |

First checks: `cargo test -p repark-sql --test parser_productions`,
`cargo test -p repark-sql --test session_wiring`, `--test introspection`, `--test ta_toll`,
`--test cross_door`, `--test ansi_door_values`.
Escalate to: [../map.md#debug](../map.md).
