# Unit ledger — C-1 · the repark-connect skeleton, the CC-2 identity move, settings and the Postgres type map

**Date:** 2026-10-05 · **Branch:** `feat/c-1-connect-skeleton` · **Base:** `02ce2acc` (`origin/main`)
· **Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Order:** `task/wo/c-1-connect-skeleton.md` on PR #957 (branch `docs/connect-1-6-orders`,
`9197b2dd`), grade B, rulings R-1…R-13. Card 1.6 of
[the design plan](../../roadmap/epic-term/roadmap-design-plan-2026-08-29.md); contracts CC-1…CC-6
and ES-1 of [contracts-ahead-of-code](../../roadmap/epic-term/contracts-ahead-of-code-2026-10-01.md);
layout [crate-layout-1-8](../../roadmap/epic-term/crate-layout-1-8-2026-10-01.md) §1 and CL-8.

**Out:** any provider, read path, pool or driver dependency; the `repark-core → repark-connect`
edge (R-3, C-2 adds it with the mount); endpoint keys (C-2, CC-3); generation assignment and
re-pointing detection (1.7). Both stubs stay: `refuse_source_ddl` and the `read_postgres`
refusal retire in C-2.

## PROPOSITION LEDGER — C-1 — 2026-10-05

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `crates/repark-connect` exists on its pre-declared footprint: a workspace member, a crate-root `map.md` naming tier 1, the `repo-manifest.toml` row flipped `planned` → `delivered` with layer `table service`, and exactly one internal edge, `repark-connect → repark-common` (`normal`). No `repark-core → repark-connect` edge (R-3); per R-14 its pre-declared row leaves `ALLOWED_EDGES` until C-2. | `./scripts/check_crate_dag.sh`, `./scripts/check_manifest.sh`; `cargo tree -p repark-connect --depth 1 -e normal`, `cargo tree -i repark-connect`. | PROVEN | Both gates clean after R-14 (§3). `cargo tree` shows `arrow`, `repark-common` and `thiserror` as the only normal dependencies, and no crate depends on `repark-connect`. Before R-14 the gate exited 1 with `ERROR: stale policy row — ALLOWED_EDGES declares repark-core -> repark-connect but no such dependency exists`. |
| C-002 | The CC-2 move: `SourceKind` (`Postgres`, `SqlServer`, `Trino`, `spelling()`) moves to `repark-common` unchanged with `SourceIdentity { name, kind, generation: Option<NonZeroU64> }`, `None` = unassigned (R-12 as amended, owner, 2026-10-05: "generation u64/0-unassigned accepted, with 0 only in the serialized form and an Option or NonZero type in Rust"). C-1 serializes no identity (no serde, string form, config key or fixture carries a generation), so the serialized `0` ↔ `None` mapping has no boundary to live at yet and no `0` appears in Rust; the unit that first serializes the identity pins both directions there. `SourceSpec` keeps `profile`, `auto_register`, `props` and carries the identity; `key_path()` renders unchanged; loader behavior unchanged. | The CFG-2 suite (`config_file::tests`, `named_sources::tests`) green with assertions untouched, and `a_source_identity_round_trips_through_the_loader`; mutation: the loader assigns generation 1. | PROVEN | §2: `cargo test -p repark-core` green. Red under the mutation (re-measured after the amendment, the loader assigning `NonZeroU64::new(1)`): `config_file/tests/mod.rs:597`, `left: SourceIdentity { name: "company_db", kind: Postgres, generation: Some(1) }`, `right: … generation: None }`. |
| C-003 | `ConnectionSettings::from_props`: absent `auth_method` and `password` both give `AuthMethod::Password`; every other prop is carried untouched, and the interpreted `auth_method` key leaves the carried map (R-5). | `an_absent_auth_method_is_password`, `an_explicit_password_auth_method_is_accepted_and_carries_the_other_props`; mutation m4: keep `auth_method` in the carried map. | PROVEN | Green; m4 red at `tests/it/settings.rs:36`. |
| C-004 | `iam_token` and `kerberos` refuse with `SettingsError::DeclaredAuthMethod` naming their dated registry rows (`CONNECT-DECL-auth-iam_token`, `CONNECT-DECL-auth-kerberos`) and fold to the Unsupported class (R-6, R-13, ES-1). | `iam_token_is_a_declared_refusal`, `kerberos_is_a_declared_refusal`; mutation m3: skip the declared-refusal check. | PROVEN | Green; m3 red in both pins (`settings.rs:42`, `:60`). |
| C-005 | Any other `auth_method` value (empty, wrong case, hyphenated, padded, unknown) is CC-4's invalid specification, lists the three spellings, and folds to the IllegalArgument class (R-13). | `any_other_auth_method_is_an_invalid_specification`, `every_auth_method_spelling_round_trips`; mutation m8: accept `Password`. | PROVEN | Green; m8 red at `settings.rs:87`. |
| C-006 | `ConnectionSettings`'s `Debug` never renders a prop value (a `password` value cannot reach a log). | `connection_settings_debug_never_renders_a_prop_value`; mutation m5: print the prop map. | PROVEN | Green; m5 red at `settings.rs:120`. |
| C-007 | `POSTGRES_TYPES` has one row per Postgres type, each naming a live pin; every mapped row (`bool`, `int2`, `int4`, `int8`, `float4`, `float8`, `text`, `varchar`, `bpchar`, `bytea`) round-trips Arrow → Postgres binary wire → Arrow, equal, over NULLs and boundary values (R-7). | The ten `<type>_round_trips` pins and `type_map_has_one_row_per_type_and_a_live_pin_per_row`; mutations m1 (`bool` decode as `== 1`), m2 (`int4` little-endian), m7 (a row names a missing pin). | PROVEN | Green; m1 red at `postgres_types.rs:44`, m2 at `:32`, m7 at `:308`. Floats compare by bit pattern (NaN, `-0.0`, the infinities). |
| C-008 | The nine declared rows (`numeric`, `date`, `time`, `timestamp`, `timestamptz`, `interval`, `uuid`, `json`, `jsonb`) refuse encode and decode with `TypeMapError::Declared` naming their registry row; malformed input refuses typed (wrong wire length with its row index, invalid UTF-8, an Arrow array of another type). | `declared_types_refuse_naming_their_row`, `wire_values_of_the_wrong_length_refuse`, `text_refuses_invalid_utf8`, `encode_refuses_an_arrow_array_of_another_type`; mutation m6: lossy text decode. | PROVEN | Green; m6 red in `text_refuses_invalid_utf8` and the three text round trips. |
| C-009 | Eleven dated registry rows land in [the registry](../../../docs/spark-sql-iceberg-parity.md) §5 beside `SES-DECL`, each with the repark / Apache Spark / Pin / Rationale shape: `CONNECT-DECL-auth-iam_token`, `CONNECT-DECL-auth-kerberos`, `CONNECT-DECL-pg-numeric`, `-pg-date`, `-pg-time`, `-pg-timestamp`, `-pg-timestamptz`, `-pg-interval`, `-pg-uuid`, `-pg-json`, `-pg-jsonb` (R-6, R-7). | The rows; `python3 scripts/check_docs_links.py`; each row's pin exists. | PROVEN | §2: links clean; every pin names a live test in `crates/repark-connect/tests/it/`. The Spark halves are *documented* with no value claim; C-2's live cells attach the first measurement. |
| C-010 | R-10: ADBC shaped the type map's Arrow contract (each mapped row's Arrow type is the one the ADBC PostgreSQL driver's documented type-mapping table names; a disagreement or an unholdable value declares the row). ConnectorX shapes the partitioned reads and is recorded as not yet (C-2, C-3). | The crate map's "Design bars" section and this clause. | PROVEN | `crates/repark-connect/map.md` "Design bars (R-10)". The ADBC basis is its document, not a run of the driver. |
| C-011 | Gates of order §5 green; dependencies are `repark-common`, `arrow`, `thiserror` at workspace versions (R-8); `Cargo.lock` gains only the new member; no code comments (R-11). | The §5 commands. | PROVEN | §2: every gate green after R-14, the commit made with hooks on. `Cargo.lock` gains 9 lines, the new package entry only. |

## 1. Executor readings (no halt)

- **The call-site edits.** R-4 calls the core edits "re-import only", while the sketch puts the
  identity inside `SourceSpec`. Carrying the identity makes `spec.name` / `spec.kind` read as
  `spec.identity.name` / `spec.identity.kind`; those field-path edits (`named_sources.rs`,
  `catalog_state.rs`, `config_file/tests/mod.rs`) are the mechanical consequence of the
  ruled shape, with no assertion changed. `session.rs` and `config_file/wiring.rs` import
  `SourceSpec`, which stays in core, so they needed no edit. `SourceSpec`'s manual `Debug` prints
  the identity where it printed `name` and `kind`; secret masking is unchanged.
- **`auth_method` leaves the carried map.** R-5 says C-1 "interprets only `auth_method` and
  carries every other prop through untouched"; the parsed value lives in `auth_method()`.
- **Spellings are exact.** `Password`, ` password` and `iam-token` are invalid, as R-13 says
  "any other value", matching the loader's case-sensitive kind spellings.
- **Map files.** AGENTS.md's lockstep rule (and the §5 `check_map_md.sh` gate) require a map
  update in every directory with changed code, so besides the §2 list the root `map.md`,
  `crates/repark-common/map.md`, `crates/repark-common/src/map.md`,
  `crates/repark-core/src/map.md`, `config_file/map.md` and `config_file/tests/map.md` carry
  the C-1 notes.
- **Declared rather than mapped.** R-7 makes an undecided row a declared row. `date`,
  `timestamp`, `timestamptz` (`±infinity`), `time` (`24:00:00`), `interval` (nanosecond
  overflow, infinite intervals), `numeric`, `uuid`, `json`, `jsonb` (two plausible mappings)
  are declared; C-2 flips them with live cells.

## 2. Gates

Re-run after R-14 on the committed tree, exit codes verbatim. The pre-commit hook passed.

| command | exit | output |
|---|---|---|
| `cargo test -p repark-connect -p repark-common -p repark-core` | 0 | core 1016 passed (1 ignored, as on `main`), connect 22, common 33, zero failures |
| `./scripts/check_crate_dag.sh` | 0 | clean |
| `./scripts/check_manifest.sh` | 0 | 20 components (11 delivered, 9 planned) agree |
| `cargo clippy -p repark-connect -p repark-common -- -D warnings` | 0 | no diagnostics |
| `cargo fmt --check -p repark-connect -p repark-common` | 0 | no output |
| `python3 scripts/check_rust_file_size.py` | 0 | clean |
| `bash scripts/check_map_md.sh --base origin/main` | 0 | no output |
| `python3 scripts/check_docs_links.py` | 0 | clean |
| `cargo clippy --workspace --all-targets -- -D warnings -A clippy::disallowed_methods` | 0 | no diagnostics |
| `make rust-panic-ban` | 0 | clean |
| `./scripts/check_lib_rs.sh` | 0 | crate roots clean |
| `python3 scripts/sync_map_md.py --check` | 0 | maps clean |
| `python3 scripts/check_ledger_grammar.py` | 0 | live ledgers clean |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xc1 origin/main HEAD` | 0 | `hits=0` |

## 3. Halt — Q-1 (H-GATE), ruled R-14

**Premise.** R-3 keeps the `repark-core → repark-connect` edge out of C-1 because "the gate bans
edges but never requires one". That holds only while `repark-connect` is pre-declared. The
gate's drift rule (`scripts/check_crate_dag.py`, the loop after the observed-edge pass: "Only
reported when BOTH endpoints are present in the workspace, so rows for pre-declared crates
stay legal") reds a declared edge once both endpoints are members. C-1 makes `repark-connect`
a member, so the pre-declared row now counts as stale. The pre-commit hook runs this gate, so
the commit is blocked, and `--no-verify` is banned.

**Question.** Which out-of-list change makes the gate green?

- **A (lean).** Remove the `("repark-core", "repark-connect")` row from `ALLOWED_EDGES` in C-1.
  C-2 re-adds it, with its reason string, in the same change that adds the dependency and the
  mount. This keeps R-3 (no edge without a caller) and the gate's own rule ("the table
  describes the workspace"), and it is a one-row edit to the DAG SSOT.
- **B.** Add `repark-connect` to `repark-core`'s `[dependencies]` (plus a root
  `[workspace.dependencies]` entry) now, with no `use`. This reverses R-3 and adds a dependency
  with no caller, and `Cargo.lock` gains a line in `repark-core`'s entry beyond the new member.

Either way, a file outside §2 changes: `scripts/check_crate_dag.py` (A), or
`crates/repark-core/Cargo.toml` and the root `[workspace.dependencies]` (B). Per H-GATE, neither
is made here.

**Ruling R-14 (orchestrator, 2026-10-05): option A.** The `repark-core → repark-connect` row
leaves `ALLOWED_EDGES` in `scripts/check_crate_dag.py`. `repark-connect` keeps its `TIERS` and
`ROLES` entries. **C-2 restores the row** (`normal`, its reason string kept verbatim in
[scripts/map.md](../../../scripts/map.md)) in the same change that adds the dependency and mounts
the providers. Option B (a dependency with no caller) is refused. The executor's lean was A.

```
COVERAGE_ATTESTATION:
  pr_unit: c-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: R-2..R-13 each map to a clause; the §2 file list, the sketch's types and signatures, the generation as Option<NonZeroU64> with None unassigned (R-12 as amended by the owner, 2026-10-05) and the three auth spellings are checked against the diff.
      artifacts: [crates/repark-connect/src/settings.rs, crates/repark-connect/src/types/postgres.rs, crates/repark-common/src/source.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Empty, wrong-case, padded and hyphenated auth values; integer MIN/MAX, signed zero, infinities and NaN by bits, empty and multibyte text, invalid UTF-8 in bytea, short and empty wire values, NULLs in every mapped type.
      artifacts: [crates/repark-connect/tests/it/settings.rs, crates/repark-connect/tests/it/postgres_types.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every failure is a typed error with its row index or key; the settings errors fold into the common taxonomy by class; no panic path in product code (panic-ban gate).
      artifacts: [crates/repark-connect/src/types/postgres.rs]
    - id: AT-4
      status: N/A
      justification: Stateless value types and a const table; no shared state, ordering or concurrency.
    - id: AT-5
      status: ATTACKED
      evidence: ConnectionSettings Debug prints keys only, pinned with a password value; the core SourceSpec Debug still masks through redact_value; error messages echo only the auth_method value, which is not a secret.
      artifacts: [crates/repark-connect/tests/it/settings.rs]
    - id: AT-6
      status: ATTACKED
      evidence: The CC-2 move keeps loader behavior byte-identical (CFG-2 suite green, key_path unchanged); text decode refuses rather than replaces invalid UTF-8; values with no Arrow counterpart declare their type instead of approximating.
      artifacts: [crates/repark-core/src/config_file/tests/mod.rs]
    - id: AT-7
      status: N/A
      justification: No read path; the codec allocates one buffer per value, which C-2 revisits with COPY decoding.
    - id: AT-8
      status: ATTACKED
      evidence: Only the pre-declared connect to common edge; dependencies limited to repark-common, arrow and thiserror at workspace versions; Cargo.lock gains only the new package entry.
      artifacts: [crates/repark-connect/Cargo.toml, scripts/check_crate_dag.py]
    - id: AT-9
      status: ATTACKED
      evidence: Refusals name the registry row and the registry path; invalid specifications list the accepted spellings; wire errors carry the Postgres type and row index.
      artifacts: [crates/repark-connect/src/settings.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Nine mutations (m1..m8 in repark-connect, generation 1 in the loader), each red in its own pin and the tree restored; the pin-liveness test reads the test file so a row cannot cite a missing pin.
      artifacts: [crates/repark-connect/tests/it/postgres_types.rs, crates/repark-core/src/config_file/tests/mod.rs]
  complete: true
```
