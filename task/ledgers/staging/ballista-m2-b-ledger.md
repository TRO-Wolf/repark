# Unit ledger — BALLISTA-M2-B · typed IcebergTableScan encode (step 1)

**Retires:** this ledger moves to `../completed/` in this unit's last commit.
This file closes when BALLISTA-M2-B merges, or when the owner closes the slate row.

**Unit:** BALLISTA-M2-B step 1 · **Date:** 2026-09-11 · **Executor:** Grok (grok-4.6), Actor, step 1 ·
**Branch:** `feat/ballista-m2-b` · **Base:** `65d0db9b`
**Model:** grok-4.6
**risk_tier:** standard.
**Path:** STANDARD.

Step 1 landed typed encode of `IcebergTableScan` (downcast + fork accessors), typed
`Predicate` → `Expr` on the `RPIC` v2 wire, inverted string-literal and bracket pins
that now travel, retirement of the encode-time rebuild-and-compare guard, and the
dated close of design-doc open questions 1 and 4 (BALLISTA-M2-A-R-001 closed).

**Not in this unit:** `STATUS.md`, `briefs/next-sequence.md`, `Cargo.toml`,
`Cargo.lock`, `.github/`, `arrow_flight`.

## Proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Encode downcasts the node to `iceberg_datafusion::IcebergTableScan` and reads table identifier, frozen snapshot, projection and predicate from typed accessors. No `Debug`/`Verbose` text is parsed for those node fields. A node whose downcast fails refuses loud naming the node. Catalog spec recovery stays the session-catalog Debug probe — no typed catalog accessor exists at this pin. | `iceberg_table_scan_round_trips_through_the_wrapper` and `unowned_node_refuses_encode_and_passes_the_rewrite_untouched` in `crates/repark-distributed/tests/codec.rs`; `from_scan_node` in `src/iceberg_provider.rs`. | **PROVEN** | Measured 2026-09-11: `IcebergTableScan` exposes `table()`, `resolved_snapshot_id()`, `projection()`, `predicates()`; `Table` exposes `identifier()` (namespace + name). Neither type exposes a catalog spec (`CatalogKind` / warehouse / props) — `session_catalog_spec` / `catalog_spec_from_debug` stay. Dead text parsers (`scan_node_table_ident`, `scan_node_projection`, `scan_node_filters`, `reject_reserved_text`) deleted. Downcast miss names the node (`node {} is not IcebergTableScan`). pins: ballista-m2-b/C-001 |
| C-002 | The spec carries the predicate as a `datafusion-proto` `Expr`. Conversion covers And/Or/Not, unary IsNull/NotNull/IsNan/NotNan, binary comparisons including StartsWith/NotStartsWith, In/NotIn, and Datum → ScalarValue for bool/int/long/float/double/date/time/timestamp/timestamptz/string/uuid/binary/fixed/decimal. An inexpressible shape refuses loud at encode naming the shape. Wire `MAGIC`/version bumped so an old-format payload refuses on decode. | `predicate_expr::tests::always_true_predicate_shape_refuses_loud`, `always_false_predicate_shape_refuses_loud`; `iceberg_scan_spec_old_format_payload_refuses_loud` in `tests/iceberg_scan.rs`. | **PROVEN** | `src/predicate_expr.rs` converts the listed shapes; AlwaysTrue/AlwaysFalse refuse naming the shape (`IcebergTableScan predicate shape AlwaysTrue cannot be expressed as a DataFusion Expr`). `RPIC` version 2; a payload with version byte 1 refuses `version 1 is not 2`. pins: ballista-m2-b/C-002 |
| C-003 | String-literal predicates (`name = 'alpha'`, `name = 'x] snapshot_id=1'`) and `]`-carrying projection / table identifier travel exactly: decoded `predicates()` equals the original's, and the two-executor cluster answer equals `LocalDataFusionExecutor`. `in_list_predicate_travels_exactly_or_refuses` and the date/timestamp pin stay passing. Red first on the inverted pins. | `string_literal_predicate_travels_exactly`, `string_literal_injection_predicate_travels_exactly`, `bracket_column_projection_travels_exactly`, `bracket_table_identifier_travels_exactly`, `in_list_predicate_travels_exactly_or_refuses`, `date_and_timestamp_predicates_measure_the_pushdown_surface` in `tests/codec.rs`. | **PROVEN** | Red first on seed `65d0db9b` (see below): all four inverted pins refused. Green after typed encode: all four travel; `iceberg_scan_predicates_match` holds; cluster rows equal local. `IN` list travels. DATE still drops at pushdown (`predicate:[]`); timestamp literal still cannot form a node. The fork accepted `we]col` / `we]t` at table creation, so those names travel rather than refuse. pins: ballista-m2-b/C-003 |
| C-004 | `encode_iceberg_scan` no longer rebuilds the scan and no longer calls `verify_scan_identity`. Decode keeps the frozen-snapshot check and the session-authority rule. | `wrapper_refuses_the_scan_without_the_session_catalog`; decode snapshot check in `src/codec.rs` `rebuild_iceberg_scan`; encode path no longer calls `verify_scan_identity`. | **PROVEN** | `verify_scan_identity` deleted. Decode still compares rebuilt `resolved_snapshot_id()` (typed) to the spec's frozen id. Vanilla session encode/decode still refuse naming the missing catalog / `ReparkSessionProvider`. pins: ballista-m2-b/C-004 |
| C-005 | `docs/design/distributed-m1.md` open questions 1 and 4 each gain a dated **Typed rebuild 2026-09-11 (BALLISTA-M2-B, S2-17)** paragraph; question 2 stays OPEN; `docs/design/map.md` one-line description updated; crate maps list `predicate_expr.rs`. | Reading: those files. | **PROVEN** | Questions 1 and 4 keep their history and append the typed-rebuild close. Question 2 is unchanged. `docs/design/map.md` no longer names the guard or R-001 as live. pins: ballista-m2-b/C-005 |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

## Red-first evidence (step 1, 2026-09-11)

Command: `cargo test -p repark-distributed --features cluster --test codec` on the seed
(`65d0db9b`, inverted pins, no production change).

```
---- bracket_column_projection_travels_exactly stdout ----
thread 'bracket_column_projection_travels_exactly' panicked at
crates/repark-distributed/tests/codec.rs:736:25:
bracket-column predicate must travel, got refusal: External error: datafusion
engine error: IcebergTableScan projection field: datafusion engine error:
IcebergTableScan debug text has an unterminated string in "\"we"

---- string_literal_injection_predicate_travels_exactly stdout ----
thread 'string_literal_injection_predicate_travels_exactly' panicked at
crates/repark-distributed/tests/codec.rs:736:25:
literal-injection predicate must travel, got refusal: Execution error:
IcebergTableScan encode refused: the spec could not be rebuilt into an
equivalent scan — the predicate or table identifier field did not re-parse
under session authority: External error: SQL error: TokenizerError("Expected
close delimiter '\"' before EOF. at Line: 1, Column: 8")

---- bracket_table_identifier_travels_exactly stdout ----
thread 'bracket_table_identifier_travels_exactly' panicked at
crates/repark-distributed/tests/codec.rs:736:25:
bracket-table predicate must travel, got refusal: External error: datafusion
engine error: IcebergTableScan table identifier "we]t" carries a character the
codec's debug surface cannot round-trip ('"', '\', '[' or ']')

---- string_literal_predicate_travels_exactly stdout ----
thread 'string_literal_predicate_travels_exactly' panicked at
crates/repark-distributed/tests/codec.rs:736:25:
string-literal predicate must travel, got refusal: Execution error:
IcebergTableScan encode refused: the spec could not be rebuilt into an
equivalent scan — the predicate or table identifier field did not re-parse
under session authority: External error: Schema error: No field named alpha.
Valid fields are id, name.

test result: FAILED. 0 passed; 4 failed; 0 ignored; 0 measured; 7 filtered out
```

Green after typed encode (`src/predicate_expr.rs`, `from_scan_node` downcast, `RPIC` v2,
guard retired):

```
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

## Mechanism notes for the Critic

- Encode downcasts `Arc<dyn ExecutionPlan>` to `iceberg_datafusion::IcebergTableScan`.
  Identifier is `table().identifier()` (namespace + name). Snapshot is
  `resolved_snapshot_id()`. Projection is `projection()`. Predicate is
  `predicates()` converted by `predicate_to_expr` and serialized with
  `datafusion_proto::bytes` `to_bytes` / `from_bytes`.
- Catalog spec has no typed accessor on `IcebergTableScan` or `Table` at fork rev
  `85db42f2` (measured 2026-09-11). `session_catalog_spec` /
  `catalog_spec_from_debug` stay.
- Decode still rebuilds through `IcebergScanSpec::scan` on the codec-carried
  `SessionContext`, on a joined `std::thread` with a fresh `current_thread`
  runtime. The rebuilt node's typed `resolved_snapshot_id()` must equal the
  spec's frozen value.
- `RPIC` version is 2. Filters on the wire are a bytes list: codec path writes
  serialized `Expr`s; the M1-D SQL constructor still writes UTF-8 SQL when no
  Expr bytes are present. Decode classifies each item by `Expr::from_bytes`.

## Residue

None. BALLISTA-M2-A-R-001 is closed.

## Gates

| Command | Result |
|---|---|
| `cargo test -p repark-distributed --features cluster --test codec` | 11 passed; 0 failed |
| `cargo test -p repark-distributed --features cluster` | 30 passed; 0 failed (lib 2, cluster_two_executors 4, codec 11, iceberg_scan 5, local_executor 3, multi_stage 5) |
| `cargo test -p repark-distributed` | 3 passed; 0 failed (default features; cluster tests cfg-out) |
| `cargo clippy -p repark-distributed --features cluster --all-targets -- -D warnings` | clean |
| `make verify` | green (fmt, clippy workspace, panic-ban, crate-dag, file-size, maps, ledgers, docs, rust-check, py-lint, spell-check, `cargo test --workspace --locked`) |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ballista-m2-b
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every inverted string-literal and bracket pin red-first on the seed (they refused via Debug-text parse / rebuild-compare), then green after typed encode. C-002 wire fields asserted cell by cell on the scan round-trip (catalog name/kind, table identifier, frozen snapshot id, projection) plus decoded-node name/schema/partition count, typed predicates() equality, and byte-identical re-encode.
      artifacts: [crates/repark-distributed/tests/codec.rs, crates/repark-distributed/src/codec.rs, crates/repark-distributed/src/iceberg_provider.rs, crates/repark-distributed/src/predicate_expr.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary inputs exercised — vanilla session (no catalog), unowned custom ExecutionPlan, five-node delegation sweep, memory-catalog bed, string-literal and injection predicates, bracket projection and table names, dropped DATE predicate, timestamp literal that cannot form a node, IN list, AlwaysTrue/AlwaysFalse inexpressible shapes, v1 RPIC payload.
      artifacts: [crates/repark-distributed/tests/codec.rs, crates/repark-distributed/tests/iceberg_scan.rs, crates/repark-distributed/src/predicate_expr.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Failure paths pinned loud — vanilla decode refuses naming the session catalog, vanilla encode refuses, unowned-node encode refuses with Unsupported plan node, AlwaysTrue/AlwaysFalse refuse naming the shape, v1 payload refuses naming the version, decode refuses when the rebuilt resolved_snapshot_id differs from the spec's frozen value.
      artifacts: [crates/repark-distributed/tests/codec.rs, crates/repark-distributed/tests/iceberg_scan.rs, crates/repark-distributed/src/codec.rs, crates/repark-distributed/src/predicate_expr.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The decode bridge still spawns a joined std::thread with a fresh current_thread runtime rather than Runtime::block_on, because try_decode runs inside the executor's tokio worker where ambient block_on panics; the handle is always joined, errors map to DataFusionError, no detached task survives.
      artifacts: [crates/repark-distributed/src/codec.rs]
    - id: AT-5
      status: ATTACKED
      evidence: Decode resolves catalogs only through the codec-carried SessionContext built from ReparkSessionProvider::session_state — ambient authority is unreachable by construction and the vanilla-session refusal is pinned. Catalog spec on the wire still comes from the session-catalog Debug probe; IcebergTableScan/Table have no catalog-spec accessor at this pin (measured).
      artifacts: [crates/repark-distributed/tests/codec.rs, crates/repark-distributed/src/iceberg_provider.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Wire format bumped to RPIC v2 (magic/version checks, length bounds, trailing-byte refusal kept); a v1 version byte refuses on decode. Filters travel as Expr protobuf bytes on the codec path. The scan's frozen resolved_snapshot_id is what travels and is re-verified on the rebuilt node via the typed accessor.
      artifacts: [crates/repark-distributed/src/iceberg_provider.rs, crates/repark-distributed/tests/iceberg_scan.rs, crates/repark-distributed/tests/codec.rs]
    - id: AT-7
      status: N/A
      justification: Encode is a bounded walk over session catalogs plus a bounded Predicate walk (depth 64); decode spawns one joined thread per task decode — no unbounded growth, no hot-path change.
    - id: AT-8
      status: ATTACKED
      evidence: The wrapper still delegates every non-owned PhysicalExtensionCodec method to BallistaPhysicalExtensionCodec. No Cargo.toml/Cargo.lock/arrow_flight change. iceberg-datafusion and iceberg were already optional behind cluster at the seed.
      artifacts: [crates/repark-distributed/src/codec.rs, crates/repark-distributed/tests/codec.rs]
    - id: AT-9
      status: N/A
      justification: Every new refusal is a DataFusionError/repark Error naming the node, the missing catalog, the snapshot mismatch, the inexpressible predicate shape, or the old wire version — DataFusion surfaces it on the task as today; no new silent path exists to alarm on.
    - id: AT-10
      status: ATTACKED
      evidence: Pins cover every new branch by name — install pin, five-node delegation, scan round-trip including frozen-snapshot assert, vanilla encode/decode refusals, unowned-node encode refusal plus rewrite pass-through, inverted string-literal and bracket travel pins with typed predicates() equality and two-executor cluster vs local, IN-list travel, dropped DATE predicate, AlwaysTrue/AlwaysFalse shape refusal, v1 payload refusal. A mutation flipping the downcast or the MAGIC/version guard would red the suite.
      artifacts: [crates/repark-distributed/tests/codec.rs, crates/repark-distributed/tests/iceberg_scan.rs, crates/repark-distributed/src/codec.rs, crates/repark-distributed/src/predicate_expr.rs]
  complete: true
```
