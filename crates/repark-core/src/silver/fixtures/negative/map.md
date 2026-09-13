# map — repark-core/src/silver/fixtures/negative

## Purpose

Negative SilverPlan TOML fixtures: one file per `SilverRefusal` variant. See [../map.md](../map.md).

## Contents

| File | Variant |
|---|---|
| `unknown_key.toml` | `UnknownKey` at the document root |
| `unknown_column_key.toml` | `UnknownKey` at `columns[0].nonesuch` |
| `unknown_transform_kind.toml` | `UnknownKind` at `columns[0].transforms[0].kind` |
| `missing_dataset_id.toml` | `MissingField` |
| `plan_format_version_2.toml` | `UnsupportedPlanFormatVersion` |
| `unsupported_format_id.toml` | `UnsupportedFormatId` |
| `duplicate_target_name.toml` | `DuplicateTargetName` |
| `duplicate_source_field_id.toml` | `DuplicateSourceFieldId` |
| `unmapped_selection_key.toml` | `UnmappedSelectionKey` |
| `unmapped_order_field.toml` | `UnmappedOrderField` |
| `illegal_transform_position.toml` | `IllegalTransformPosition` (trim after parse_timestamp) |
| `non_nullable_without_require_non_null.toml` | `NonNullableWithoutRequireNonNull` |
| `threshold_out_of_range.toml` | `ThresholdOutOfRange` |
| `threshold_nan.toml` | `ThresholdNan` |
| `unsupported_timezone.toml` | `UnsupportedTimezone` |
| `unsupported_timestamp_unit.toml` | `UnsupportedTimestampUnit` |
| `decimal_scale_invalid.toml` | `DecimalScaleInvalid` |
| `empty_columns.toml` | `EmptyColumns` |
| `duplicate_rule_id.toml` | `DuplicateRuleId` |
| `unknown_ratio_population.toml` | `UnknownRatioPopulation` |
| `unknown_conflicting_tie.toml` | `UnknownConflictingTie` |
| `unknown_empty_input.toml` | `UnknownEmptyInput` |
| `invalid_parameter_type.toml` | `InvalidParameter` |

`unknown_column_key.toml` is a second `UnknownKey` path pin, not a new variant.
`TomlSyntax` is pinned in `tests/parse.rs` with inline invalid text so taplo never sees a `.toml` file.

pins: silver-s1/C-001, C-002, C-003, C-007

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-core silver`.
