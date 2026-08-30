# map — repark-core/src/session

CC-3 (2026-08-30): comments condensed to one line; banners removed.

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

File-backed modules of `../session.rs` (`ReparkSession`): the behavior modules (`temp_views.rs`,
`spill.rs`, `df_guards.rs`) plus the test cohorts under `tests/` (`session.rs`,
`session/catalog_registration.rs`, `df_guard.rs`, `aws_gate.rs`, `namespace_create.rs`, `a13.rs`). Test cohorts are two: the E-2 gate tests
(new, additive) and — landing with the PR-C test-audit commit — the ported v1 session unit-test
battery (names under the declared-rename map; the not-yet-ported subset is listed in
`task/port/deferred-tests.md`).

## Contents

- `temp_views.rs` — **SQM round 6 (R6-1):** the temp-view family, split out of `session.rs` when
  the choke-point fix pushed that file past its ceiling. The old exception then retired under the
  prior default; CAP-1 records the file again at its exact source-size baseline. Holds
  `create_or_replace_temp_view`
  (batches) / `create_or_replace_temp_view_from` (a plan),
  `register_record_batches_as_temp_view`, `materialize_dataframe_as_temp_view` /
  `materialize_dataframe_as_cache_view` and their shared `register_collected_memtable`,
  `declare_temp_view_sorted`, the shared `replace_view` registration, `drop_temp_view`, and
  `temp_view_ref` / `temp_view_ref_from_segment` — the wrappers over [`crate::temp_view`] that
  EVERY member resolves names through, so a qualified name cannot register into a catalog and a
  one-part name is immune to `SET datafusion.catalog.default_catalog`. They also re-check the
  home PROVIDER live (`assert_home_intact`): a session built with
  `datafusion.catalog.default_catalog = <a name a catalog is later registered under>` has no
  session-local home at all, and the whole family refuses loud rather than write that catalog
  (round-6 critic S1, MEASURED).
- `df_guards.rs` — the DataFusion **54.1 regression guards**, CORE (never door-extension —
  design G8). Guard 1 is a configurable scalar-subquery default; guard 2 wraps
  `push_down_leaf_projections`, declining failed rewrites only on an `Unnest` path while keeping
  unrelated siblings loud. The wrapper preserves DataFusion's rule name and order.
  The two guards sit at **different altitudes**,
  because the two bugs are:
  * **Guard 1, a config default:** `optimizer.enable_physical_uncorrelated_scalar_subquery =
    false` — the 54.1 physical scalar-subquery path drops a top-level Sort (fuzz-42-1/2). The
    whole planning mode is bad, so the flag is the switch; it stays a *default*, not a lock (the
    builder's `datafusion.*` keys apply after it).
  * **Guard 2, a scoped optimizer RULE** (since **DEFECT-2, 2026-08-18**):
    `unnest_safe_optimizer_rules()` hands `SessionStateBuilder` DataFusion's own rule list with
    `push_down_leaf_projections` wrapped in `UnnestSafeLeafProjectionPushdown`. That rule cannot
    rewrite an `Unnest`-over-`Unnest` chain carrying a `get_field` leaf — the shape every
    multi-pass `dynamicFlatten` / repeated `explode` builds — either asserting inside
    `Unnest::with_new_exprs` or landing a qualified and an unqualified spelling of one name in
    one `DFSchema`. The wrapper's `apply_order` is `None` so it owns the TopDown walk
    (a reconstruction error on a `Projection` under `Unnest` otherwise bypasses
    per-node decline). Swallow is the Unnest *path*: this node is `Unnest`, has an
    `Unnest` ancestor, or `carries_unnest` in this subtree — a mixed plan's
    non-`Unnest` sibling stays loud. `enable_leaf_expression_pushdown` therefore
    stays at DataFusion's default: the flag
    would have cost every nested-column query in the engine (measured up to ~8x in one run, load-sensitive ratio, on a filtered
    wide-struct parquet scan), and declining by shape alone would have cost 11.8x on a
    wide-struct scan that merely has an unnest nearby. Recorded trade: within an
    `Unnest`-carrying subtree the rule's error is swallowed (repark-core has no logging dep), so
    a genuinely-failing shape silently keeps the slower, correct plan.
  Pins: all seven live in `tests/df_guard.rs` (below), not in `tests/session.rs`;
  ledger `task/c25-bugfix-ledger.md` → DEFECT-2.
- `tests/df_guard.rs` — the seven `df_guards.rs` pins, split out of `tests.rs` when the DEFECT-2
  cohort pushed that file past the 1500-line ceiling (the sanctioned "split the module" out, not
  an EXCEPTIONS row). Guard 1: a bare no-extension session carries the scalar-subquery config
  default. Guard 2, six pins: the `enable_leaf_expression_pushdown` flag stays ENABLED (the
  anti-blanket-skip pin), the wrapper is installed under DataFusion's own rule name in
  DataFusion's own rule order, a no-`Unnest` plan optimizes byte-identically to stock DataFusion,
  an `Unnest` plan the rule CAN rewrite still gets the optimization (this is what makes the scope
  "by failure", not "by shape"), an explicit conf can still disable the optimization, and a mixed
  plan's non-`Unnest` inner-rule error stays loud (`mixed_plan_non_unnest_inner_error_stays_loud`).
- `spill.rs` — **S-1:** FairSpillPool install + runtime `SET datafusion.runtime.memory_limit`
  intercept (R1). DataFusion 54.1 has no in-place resize (`pool_size` lives outside the mutex),
  so SET **swaps** a new `FairSpillPool` (in-flight reservations stay on the old pool).
  Dual `repark.memory.limit.gb` + the DF key refuses. **R2:** runtime
  `SET datafusion.runtime.temp_directory` refuses loud (names `TMPDIR`); build-time key
  applies `RuntimeEnvBuilder::with_temp_file_path`. `max_temp_directory_size` residual.
  **R3:** RAM-relative default `clamp(0.6 × cgroup-or-MemTotal, MIN, 8 GiB)` at `build()`
  only; `builder_default_installs_eight_gib_fair_spill_pool` asserts Finite / floor / cap /
  equals helper.
- `tests/aws_gate.rs` — E-2 gate pins, AWS-free by construction: an offline session's finalize
  never resolves the AWS SDK chain (no IMDS probe); an S3-path read on a session that never
  resolved fails loud naming `register_configured_catalogs` and the `repark.aws.enable` opt-in;
  opt-in without finalize still refuses (no lazy query-time resolution); the late config map's
  region-conf signal class is consulted (dual-spelling conflict fails loud pre-resolution).
- `tests/namespace_create.rs` — **R-6 / G-6 Q1 (2026-08-14):** session
  `create_namespace` location-guard pins on a memory catalog: create-new, re-create
  same location (idempotent), re-create conflicting (Analysis, both paths named,
  stored location unchanged), re-create without request location (idempotent),
  trailing-slash-only difference (idempotent).
- `tests/` — session test modules; see [tests/map.md](tests/map.md).
  Registration pins cover `rust-catalog-registration/C-001` through `C-004`.
- `tests/session.rs` — the ported v1 session test battery (38 port-now tests, v1 order; the deferred
  subset is in `task/port/deferred-tests.md`), plus the P2G R2 cohort at the tail: the
  builder→`SessionConfig` `datafusion.*` plumbing (key lands / unprefixed key still ignored /
  unknown key fails loud / explicit conf overrides a core default / unset `batch_size` lands
  `DEFAULT_BATCH_SIZE` 65536 with the conf key still winning), the DF-54.1 guard pins
  and the Q8 enumeration pair
  (a registered Iceberg catalog enumerates through `information_schema` + `SHOW TABLES` +
  `DESCRIBE` on the PRODUCT path; the negative half proves the conf is what enables it).
  **A13:** `register_memory_catalog` and the `spark.sql.catalog.*.type=memory` config path set
  `TempFallbackAllowed.root` to the warehouse, not `$TMPDIR`. Since
  **2026-08-10 (unit H-1c,
  [ADR-0006](../../../../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md))** the
  `$`-metadata rows are the **bare-session** half of the enumeration claim — the synthesized names
  do not enumerate, the base table still does, and a hidden name still resolves and executes. They
  replace the former single row that pinned the opposite
  (`information_schema_still_exposes_the_dollar_metadata_tables`), flipped in the same diff as the
  behavior.
- `tests/a13.rs` — **A13:** `file://` / `FILE://` / `file://localhost` warehouses become a
  filesystem fallback root on the product path (skipping the helper reds this pin).

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| `S3 read … refused: this session never resolved its AWS SDK config` | Call `register_configured_catalogs()` after signaling AWS use (AWS-backed catalog spec, S3-region conf, or `repark.aws.enable=true`). |
| Uncorrelated scalar subquery misplans on a bare session | The DF-54.1 guard (`enable_physical_uncorrelated_scalar_subquery = false`) is a core session default (G8), pinned by `bare_session_without_extension_carries_df_54_1_subquery_guard`. |
| A builder `datafusion.*` key seems ignored | It is not (P2G R2) — `apply_datafusion_config_keys` applies it and an unknown key is a build error, with exact-key exclusions in `REPARK_OWNED_DATAFUSION_PSEUDO_KEYS` (`datafusion.runtime.memory_limit` is applied to a **FairSpillPool** at `build()` / runtime SET, never swept into `ConfigOptions`). The typo pin carries TWO fixtures — truncated (catches a namespace-prefix exclusion) and extended (catches a `starts_with(pseudo_key)` exclusion). If the value did not take, check ordering: the extension `configure` hook runs AFTER, so an extension can still overwrite. Pin: `builder_datafusion_config_key_reaches_session_config`. |
| Runtime `SET datafusion.runtime.memory_limit` OOMs with `greedy(` | Intercept lives in `spill.rs` (`maybe_apply_runtime_set`) and must run **before** `dialect.execute`. Pin: `runtime_set_memory_limit_oom_is_fair_not_greedy`. |
| Runtime `SET datafusion.runtime.temp_directory` succeeds silently | R2: must refuse and name `TMPDIR`. Pin: `runtime_set_temp_directory_refuses_loud_naming_tmpdir`. Build-time key: `builder_temp_directory_wires_disk_manager`. |

First checks: `cargo test -p repark-core session`. Escalate to: [../map.md#debug](../map.md).

- **Neutral-fixture scrub (2026-08-10, hardening-prep):** an owner-approved, forward-only,
  comment-and-fixture-only pass moved this directory's doc text and example literals to
  neutral placeholders — the upstream job the acceptance shape mirrors is named generically
  ("the source publish job"), and example table/view/entity names are placeholders carrying no
  domain vocabulary. Outcome-neutral: every renamed fixture moved together with the assertions
  that read it. Sites here: `tests/session.rs` — the doc comment on
  `late_catalog_registration_adds_new_names_and_skips_existing`.
**SQM round 7 (R7-1):** `temp_views.rs` also owns the READ spelling — `temp_view_home` and
`resolve_temp_view_home_ref`, the two lookups the Python facade uses so a product read path never
emits a BARE reference for a session-local view (a bare one is re-resolved against the LIVE
`datafusion.catalog.default_catalog`). Both go through `assert_home_intact` first.
