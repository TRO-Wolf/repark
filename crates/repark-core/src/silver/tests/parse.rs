use super::{SilverRefusal, parse_fixture};

fn assert_ok(relative: &str) {
    parse_fixture(relative).unwrap_or_else(|error| panic!("{relative} must parse: {error}"));
}

fn refuse(relative: &str) -> SilverRefusal {
    parse_fixture(relative).expect_err(&format!("{relative} must refuse"))
}

#[test]
fn positive_fixtures_parse() {
    assert_ok("positive/crm_contacts.toml");
    assert_ok("positive/row_snapshot_disabled_selection.toml");
    assert_ok("positive/decimal_boolean_date.toml");
    assert_ok("positive/int32_passthrough.toml");
    assert_ok("positive/allowed_values.toml");
    assert_ok("positive/quality_zero_threshold.toml");
    assert_ok("positive/crm_contacts_permuted_keys.toml");
    assert_ok("positive/crm_contacts_whitespace_comments.toml");
    assert_ok("positive/crm_contacts_inline.toml");
    assert_ok("positive/crm_contacts_threshold_scientific.toml");
}

#[test]
fn unknown_key_names_the_root_path() {
    let error = refuse("negative/unknown_key.toml");
    match error {
        SilverRefusal::UnknownKey { path } => assert_eq!(path, "nonesuch"),
        other => panic!("expected UnknownKey, got {other:?}"),
    }
}

#[test]
fn unknown_column_key_names_the_column_path() {
    let error = refuse("negative/unknown_column_key.toml");
    match error {
        SilverRefusal::UnknownKey { path } => assert_eq!(path, "columns[0].nonesuch"),
        other => panic!("expected UnknownKey, got {other:?}"),
    }
}

#[test]
fn unknown_transform_kind_names_the_kind_path() {
    let error = refuse("negative/unknown_transform_kind.toml");
    match error {
        SilverRefusal::UnknownKind { path, value } => {
            assert_eq!(path, "columns[0].transforms[0].kind");
            assert_eq!(value, "regex_replace");
        }
        other => panic!("expected UnknownKind, got {other:?}"),
    }
}

#[test]
fn missing_dataset_id_names_the_field() {
    let error = refuse("negative/missing_dataset_id.toml");
    match error {
        SilverRefusal::MissingField { path } => assert_eq!(path, "dataset_id"),
        other => panic!("expected MissingField, got {other:?}"),
    }
}

#[test]
fn toml_syntax_refuses() {
    let error = crate::silver::SilverPlan::parse("this is { not valid toml")
        .expect_err("invalid TOML must refuse");
    assert!(
        matches!(error, SilverRefusal::TomlSyntax { .. }),
        "{error:?}"
    );
}

#[test]
fn plan_format_version_2_refuses() {
    let error = refuse("negative/plan_format_version_2.toml");
    match error {
        SilverRefusal::UnsupportedPlanFormatVersion { version } => assert_eq!(version, 2),
        other => panic!("expected UnsupportedPlanFormatVersion, got {other:?}"),
    }
}

#[test]
fn unsupported_format_id_refuses() {
    let error = refuse("negative/unsupported_format_id.toml");
    match error {
        SilverRefusal::UnsupportedFormatId { path, value } => {
            assert!(path.contains("format_id"), "{path}");
            assert_eq!(value, "rfc3339");
        }
        other => panic!("expected UnsupportedFormatId, got {other:?}"),
    }
}

#[test]
fn duplicate_target_name_refuses() {
    let error = refuse("negative/duplicate_target_name.toml");
    match error {
        SilverRefusal::DuplicateTargetName { name } => assert_eq!(name, "code"),
        other => panic!("expected DuplicateTargetName, got {other:?}"),
    }
}

#[test]
fn duplicate_source_field_id_refuses() {
    let error = refuse("negative/duplicate_source_field_id.toml");
    match error {
        SilverRefusal::DuplicateSourceFieldId { source_field_id } => {
            assert_eq!(source_field_id, 1);
        }
        other => panic!("expected DuplicateSourceFieldId, got {other:?}"),
    }
}

#[test]
fn unmapped_selection_key_refuses() {
    let error = refuse("negative/unmapped_selection_key.toml");
    match error {
        SilverRefusal::UnmappedSelectionKey { source_field_id } => {
            assert_eq!(source_field_id, 999);
        }
        other => panic!("expected UnmappedSelectionKey, got {other:?}"),
    }
}

#[test]
fn unmapped_order_field_refuses() {
    let error = refuse("negative/unmapped_order_field.toml");
    match error {
        SilverRefusal::UnmappedOrderField { source_field_id } => {
            assert_eq!(source_field_id, 777);
        }
        other => panic!("expected UnmappedOrderField, got {other:?}"),
    }
}

#[test]
fn illegal_transform_position_refuses_trim_after_parse_timestamp() {
    let error = refuse("negative/illegal_transform_position.toml");
    match error {
        SilverRefusal::IllegalTransformPosition { path } => {
            assert_eq!(path, "columns[0].transforms[1]");
        }
        other => panic!("expected IllegalTransformPosition, got {other:?}"),
    }
}

#[test]
fn non_nullable_without_require_non_null_refuses() {
    let error = refuse("negative/non_nullable_without_require_non_null.toml");
    match error {
        SilverRefusal::NonNullableWithoutRequireNonNull { target_name } => {
            assert_eq!(target_name, "code");
        }
        other => panic!("expected NonNullableWithoutRequireNonNull, got {other:?}"),
    }
}

#[test]
fn threshold_out_of_range_refuses() {
    let error = refuse("negative/threshold_out_of_range.toml");
    assert!(
        matches!(error, SilverRefusal::ThresholdOutOfRange { .. }),
        "{error:?}"
    );
}

#[test]
fn threshold_nan_refuses() {
    let error = refuse("negative/threshold_nan.toml");
    assert!(
        matches!(error, SilverRefusal::ThresholdNan { .. }),
        "{error:?}"
    );
}

#[test]
fn unsupported_timezone_refuses() {
    let error = refuse("negative/unsupported_timezone.toml");
    match error {
        SilverRefusal::UnsupportedTimezone { value, .. } => {
            assert_eq!(value, "America/New_York");
        }
        other => panic!("expected UnsupportedTimezone, got {other:?}"),
    }
}

#[test]
fn unsupported_timestamp_unit_refuses() {
    let error = refuse("negative/unsupported_timestamp_unit.toml");
    match error {
        SilverRefusal::UnsupportedTimestampUnit { value, .. } => {
            assert_eq!(value, "fortnight");
        }
        other => panic!("expected UnsupportedTimestampUnit, got {other:?}"),
    }
}

#[test]
fn decimal_scale_invalid_refuses() {
    let error = refuse("negative/decimal_scale_invalid.toml");
    assert!(
        matches!(error, SilverRefusal::DecimalScaleInvalid { .. }),
        "{error:?}"
    );
}

#[test]
fn empty_columns_refuses() {
    let error = refuse("negative/empty_columns.toml");
    assert!(matches!(error, SilverRefusal::EmptyColumns), "{error:?}");
}

#[test]
fn duplicate_rule_id_refuses() {
    let error = refuse("negative/duplicate_rule_id.toml");
    match error {
        SilverRefusal::DuplicateRuleId { rule_id } => assert_eq!(rule_id, "row_accounting"),
        other => panic!("expected DuplicateRuleId, got {other:?}"),
    }
}

#[test]
fn unknown_ratio_population_refuses() {
    let error = refuse("negative/unknown_ratio_population.toml");
    match error {
        SilverRefusal::UnknownRatioPopulation { value, .. } => assert_eq!(value, "magic"),
        other => panic!("expected UnknownRatioPopulation, got {other:?}"),
    }
}

#[test]
fn unknown_conflicting_tie_refuses() {
    let error = refuse("negative/unknown_conflicting_tie.toml");
    match error {
        SilverRefusal::UnknownConflictingTie { value, .. } => assert_eq!(value, "pick_first"),
        other => panic!("expected UnknownConflictingTie, got {other:?}"),
    }
}

#[test]
fn unknown_empty_input_refuses() {
    let error = refuse("negative/unknown_empty_input.toml");
    match error {
        SilverRefusal::UnknownEmptyInput { value, .. } => assert_eq!(value, "drop_table"),
        other => panic!("expected UnknownEmptyInput, got {other:?}"),
    }
}

#[test]
fn invalid_parameter_type_refuses() {
    let error = refuse("negative/invalid_parameter_type.toml");
    match error {
        SilverRefusal::InvalidParameter { path, .. } => {
            assert_eq!(path, "columns[0].source_field_id");
        }
        other => panic!("expected InvalidParameter, got {other:?}"),
    }
}
