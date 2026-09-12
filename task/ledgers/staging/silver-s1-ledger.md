# Unit ledger — SILVER-S1 · typed SilverPlan: models, strict parsing, canonical identity, deterministic explanation

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when SILVER-S1 merges, or when the owner closes the slate row.

**Unit:** SILVER-S1 · **Date:** 2026-09-12 · **Model:** grok-4.6 · **Branch:** `feat/silver-s1`
**Slate:** card SILVER-S1 (silver §17 S-1, run 9) — the whole S-1 in one round plus remediation.
**Path:** STANDARD. `risk_tier: standard`.

**Owner crate:** `crates/repark-core` (`src/silver.rs` + `src/silver/`). Orchestrator placement
for S-1 only; SIL-8 remains open. The module is `pub` from repark-core and is not bound into
`repark-python` and is not named in STATUS.md. The contract is unstable until SIL-1..SIL-10.

**Not in this unit:** DataFusion `Expr` lowering, Iceberg calls, Python bindings, a cryptographic
digest (owner question below), STATUS.md, briefs/next-sequence.md, any dependency file.

## Proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | D-1: TOML authoring parsed with `toml` + `serde`; snake_case keys and `kind` tags as in §8; unknown key, unknown kind, missing required field, or a disallowed parameter combination is a typed refusal naming the key path. | `unknown_key_names_the_root_path`, `unknown_column_key_names_the_column_path`, `unknown_transform_kind_names_the_kind_path`, `missing_dataset_id_names_the_field`, `invalid_parameter_type_refuses`, `toml_syntax_refuses` | **PROVEN** | 31 `silver` pins green (`cargo test -p repark-core silver`, 2026-09-12). Paths: `nonesuch`, `columns[0].nonesuch`, `columns[0].transforms[0].kind` / `regex_replace`, `dataset_id`, `columns[0].source_field_id`. |
| C-002 | D-2: operations are closed enums (`Trim`, `EmptyToNull`, `ParseTimestamp`, `RequireNonNull`, `CheckAllowedValues`); selection `latest_source_version` or `disabled`; quality `exact_disposition_accounting`, `accepted_key_uniqueness`, `ratio_at_most`; publication `single_classified_table`; target types the finite set; `iso8601_seconds_offset_v1` is the only `format_id`. | `positive_fixtures_parse`; negatives `unsupported_format_id_refuses`, `unsupported_timezone_refuses`, `unsupported_timestamp_unit_refuses`, `unknown_ratio_population_refuses`, `unknown_conflicting_tie_refuses`, `unknown_empty_input_refuses` | **PROVEN** | Ten positive fixtures parse, including the §8 example, row-snapshot/disabled selection, decimal/boolean/date, int32, `check_allowed_values`, and `on_empty=fail`. Closed-set refusals name `rfc3339`, `America/New_York`, `fortnight`, `magic`, `pick_first`, `drop_table`. |
| C-003 | D-3: parse-time structural validation before any data — duplicate target names, duplicate source field ids, unmapped selection keys, unmapped order fields, illegal type-position transforms, non-nullable target without `require_non_null`, threshold outside `[0, 1]`, NaN threshold — each its own refusal variant and negative fixture. | `duplicate_target_name_refuses`, `duplicate_source_field_id_refuses`, `unmapped_selection_key_refuses`, `unmapped_order_field_refuses`, `illegal_transform_position_refuses_trim_after_parse_timestamp`, `non_nullable_without_require_non_null_refuses`, `threshold_out_of_range_refuses`, `threshold_nan_refuses`, `empty_columns_refuses`, `duplicate_rule_id_refuses`, `decimal_scale_invalid_refuses`, `plan_format_version_2_refuses` | **PROVEN** | Each variant matched. Trim after `parse_timestamp` refuses at `columns[0].transforms[1]`. Field 91 is mapped as `source_version` in the positive §8 fixture so D-3's unmapped-order-field check holds. |
| C-004 | D-4: `canonical()` bytes are independent of key order, whitespace, comments, equivalent numeric spellings (`0.01` vs `1e-2`) and table-vs-inline layout, and change when any semantic field changes. No digest. | `permutation_whitespace_and_inline_spellings_share_canonical_bytes`, `scientific_threshold_matches_decimal_canonical_bytes`, `a_semantic_field_change_changes_canonical_bytes` | **PROVEN** | Permuted keys, extra blank lines, inline tables, and a TOML comment suffix share bytes with `crm_contacts.toml`. `1e-2` matches `0.01`. Changing `dataset_id` or trim characters changes bytes. Identity is `SilverPlanIdentity` over those bytes. |
| C-005 | D-5: `explain()` is a stable human-readable text of identity, input, columns (ordered transforms and validators), selection, quality gates, and publication; the same plan in any spelling explains byte-identically. | `equivalent_spellings_explain_byte_identically`; golden `crm_contacts.explain.txt` | **PROVEN** | Five spellings of the §8 plan explain byte-identically and match the golden. |
| C-006 | D-6: S-1 has no data execution, no DataFusion `Expr` lowering, no Iceberg calls, and no Python binding. | `silver_rust_sources_do_not_name_datafusion_iceberg_or_pyo3`; `lib.rs` has `pub mod silver` and no crate-root `pub use` of silver names | **PROVEN** | The pin reads `silver.rs` and `silver/{plan,policy,identity,explain,refusal}.rs` and asserts those needles are absent. |
| C-007 | D-7: fixtures live as `.toml` files under `silver/fixtures/`; at least 6 positive and 20 negative, one negative per refusal variant. | `fixture_counts_meet_the_floor`; 23 `SilverRefusal` variants, 24 negative files (23 `.toml` plus the nested unknown-key path), 10 positive `.toml` | **PROVEN** | Count pin: 10 positive, 24 negative `.toml`. `toml_syntax.toml` is excluded from taplo because it is intentionally invalid. |
| C-008 | Gates: `cargo test -p repark-core silver` green; `make verify` green; parity suite green; no code comments; maps in lockstep; no dependency or STATUS.md edits. | Commands in Gates. | **PROVEN** | See Gates. |

VERDICT: 8 clauses, 8 PROVEN, 0 OPEN, 0 REJECTED.

## Owner question (logged per D-4, not a halt)

A cryptographic digest of the canonical bytes needs a dependency seed (`sha2` / `blake3` is
not in `repark-core` today). S-1 uses the canonical bytes as the identity; `SilverPlanIdentity`
is named so a digest can be added without changing callers.

## Red first

The pins import `SilverPlan` / `SilverRefusal` from `crate::silver`. The module file existed
only as a test-mod declaration, so the test target did not build. Honest red — the pins
cannot pass without the types.

`cargo test -p repark-core silver --offline`, 2026-09-12:

```text
error[E0432]: unresolved import `super::SilverPlan`
 --> crates/repark-core/src/silver/tests/mod.rs:7:5
  |
7 | use super::SilverPlan;
  |     ^^^^^^^^^^^^^^^^^ no `SilverPlan` in `silver`

error[E0432]: unresolved import `super::SilverRefusal`
 --> crates/repark-core/src/silver/tests/mod.rs:8:5
  |
8 | use super::SilverRefusal;
  |     ^^^^^^^^^^^^^^^^^^^^ no `SilverRefusal` in `silver`

error[E0433]: cannot find `SilverPlan` in `silver`
  --> crates/repark-core/src/silver/tests/identity.rs:18:36
   |
18 |     let commented = crate::silver::SilverPlan::parse(&with_comment).expect("commented");
   |                                    ^^^^^^^^^^ could not find `SilverPlan` in `silver`

error: could not compile `repark-core` (lib test) due to 5 previous errors
```

After the types landed: `test result: ok. 31 passed; 0 failed`.

## Gates

| Command | Result |
|---|---|
| `cargo test -p repark-core silver` | 31 passed; 0 failed |
| `make verify` | exit 0 (clippy workspace `-D warnings`, panic-ban, rust-test workspace, lib-rs 10 roots, rust-file-size 484 files, ledger-grammar 117 live ledgers / 743 clauses) |
| parity `pytest python/repark-parity/tests -q` | 749 passed, 1 skipped, 12 xfailed |
| comment fence `git diff --cached … grep` | no hits |

```
COVERAGE_ATTESTATION:
  pr_unit: silver-s1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: 31 silver pins cover parse refusals with key paths, six-plus positive plans, canonical identity across spelling, explain goldens, fixture floors, and the no-engine-import pin.
      artifacts: [crates/repark-core/src/silver/tests/parse.rs, crates/repark-core/src/silver/tests/identity.rs, crates/repark-core/src/silver/tests/mod.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Structural refusals include trim-after-parse_timestamp, NaN and out-of-range thresholds, unmapped keys, duplicate ids/names, non-nullable without require_non_null.
      artifacts: [crates/repark-core/src/silver/plan.rs, crates/repark-core/src/silver/fixtures/negative/map.md]
    - id: AT-3
      status: N/A
      justification: No AWS, credential, IAM, or .github surface.
    - id: AT-4
      status: N/A
      justification: No performance claim; S-1 is parse/identity only.
    - id: AT-5
      status: ATTACKED
      evidence: Cargo.toml and Cargo.lock untouched. toml + serde already in repark-core.
      artifacts: [crates/repark-core/Cargo.toml]
    - id: AT-6
      status: N/A
      justification: No data path, no row compute, no execution.
    - id: AT-7
      status: ATTACKED
      evidence: Red-first compile refusal pasted above; each negative fixture maps to one SilverRefusal variant.
      artifacts: [task/ledgers/staging/silver-s1-ledger.md]
    - id: AT-8
      status: N/A
      justification: No dependency, lockfile, or workspace-manifest change.
    - id: AT-9
      status: N/A
      justification: No Spark-visible surface and no STATUS.md mention.
    - id: AT-10
      status: ATTACKED
      evidence: Eight clauses cited from crates/repark-core/src/silver/map.md.
      artifacts: [crates/repark-core/src/silver/map.md]
  complete: true
```
