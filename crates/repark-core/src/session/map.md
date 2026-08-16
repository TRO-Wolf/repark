# map — repark-core/src/session

## Purpose

File-backed test modules of `../session.rs` (`ReparkSession`). Two cohorts: the E-2 gate tests
(new, additive) and — landing with the PR-C test-audit commit — the ported v1 session unit-test
battery (names under the declared-rename map; the not-yet-ported subset is listed in
`task/port/deferred-tests.md`).

## Contents

- `spill.rs` — **S-1:** FairSpillPool install + runtime `SET datafusion.runtime.memory_limit`
  intercept (R1). DataFusion 54.1 has no in-place resize (`pool_size` lives outside the mutex),
  so SET **swaps** a new `FairSpillPool` (in-flight reservations stay on the old pool).
  Dual `repark.memory.limit.gb` + the DF key refuses. **R2:** runtime
  `SET datafusion.runtime.temp_directory` refuses loud (names `TMPDIR`); build-time key
  applies `RuntimeEnvBuilder::with_temp_file_path`. `max_temp_directory_size` residual.
  **R3:** RAM-relative default `clamp(0.6 × cgroup-or-MemTotal, MIN, 8 GiB)` at `build()`
  only; `builder_default_installs_eight_gib_fair_spill_pool` asserts Finite / floor / cap /
  equals helper.
- `aws_gate_tests.rs` — E-2 gate pins, AWS-free by construction: an offline session's finalize
  never resolves the AWS SDK chain (no IMDS probe); an S3-path read on a session that never
  resolved fails loud naming `register_configured_catalogs` and the `repark.aws.enable` opt-in;
  opt-in without finalize still refuses (no lazy query-time resolution); the late config map's
  region-conf signal class is consulted (dual-spelling conflict fails loud pre-resolution).
- `namespace_create_tests.rs` — **R-6 / G-6 Q1 (2026-08-14):** session
  `create_namespace` location-guard pins on a memory catalog: create-new, re-create
  same location (idempotent), re-create conflicting (Analysis, both paths named,
  stored location unchanged), re-create without request location (idempotent),
  trailing-slash-only difference (idempotent).
- `tests.rs` — the ported v1 session test battery (38 port-now tests, v1 order; the deferred
  subset is in `task/port/deferred-tests.md`), plus the P2G R2 cohort at the tail: the
  builder→`SessionConfig` `datafusion.*` plumbing (key lands / unprefixed key still ignored /
  unknown key fails loud / explicit conf overrides a core default / unset `batch_size` lands
  `DEFAULT_BATCH_SIZE` 65536 with the conf key still winning) and the Q8 enumeration pair
  (a registered Iceberg catalog enumerates through `information_schema` + `SHOW TABLES` +
  `DESCRIBE` on the PRODUCT path; the negative half proves the conf is what enables it). Since
  **2026-08-10 (unit H-1c,
  [ADR-0006](../../../../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md))** the
  `$`-metadata rows are the **bare-session** half of the enumeration claim — the synthesized names
  do not enumerate, the base table still does, and a hidden name still resolves and executes. They
  replace the former single row that pinned the opposite
  (`information_schema_still_exposes_the_dollar_metadata_tables`), flipped in the same diff as the
  behavior.

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
  that read it. Sites here: `tests.rs` — the doc comment on
  `late_catalog_registration_adds_new_names_and_skips_existing`.
