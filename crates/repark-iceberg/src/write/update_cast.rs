use std::borrow::Cow;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use repark_common::spark_error;

use super::store_assign::{
    ansi_store_assignable, normalize_for_assignment, without_field_metadata,
};

#[must_use]
pub fn store_assignment_cast_sql(
    expr: &str,
    source: Option<&DataType>,
    target: &DataType,
) -> String {
    if let Some(nested) = nested_micro_ntz_wall_sql(expr, source, target) {
        return nested;
    }
    match super::ntz_store::store_kernel_sql(expr, target) {
        Some(stored) => stored,
        None => nested_leaf_cast_sql(expr, target),
    }
}

#[must_use]
pub fn nested_leaf_cast_sql(expr: &str, target: &DataType) -> String {
    if let Some(wall) = super::ntz_store::wall_cast_sql(expr, target) {
        return wall;
    }
    let type_name = without_field_metadata(target)
        .to_string()
        .replace('\'', "''");
    if super::ntz_store::is_ltz_instant_target(target) {
        let instant = super::ntz_store::ltz_instant_cast_sql(expr);
        return format!("arrow_cast(({instant}), '{type_name}')");
    }
    format!("arrow_cast(({expr}), '{type_name}')")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NtzDecision {
    Ineligible,
    Keep,
    Rebuild(Vec<Vec<String>>),
}

#[must_use]
pub fn nested_micro_ntz_decision(source: Option<&DataType>, target: &DataType) -> NtzDecision {
    if !target_has_nested_micro_ntz(target, false) {
        return NtzDecision::Ineligible;
    }
    let Some(source) = source else {
        return NtzDecision::Keep;
    };
    let paths = source_paths(source, target, false);
    if paths.is_empty() {
        NtzDecision::Keep
    } else {
        NtzDecision::Rebuild(paths)
    }
}

#[must_use]
pub fn nested_micro_ntz_selected_paths(
    source: Option<&DataType>,
    target: &DataType,
) -> Vec<Vec<String>> {
    match nested_micro_ntz_decision(source, target) {
        NtzDecision::Rebuild(paths) => paths,
        NtzDecision::Ineligible | NtzDecision::Keep => Vec::new(),
    }
}

#[must_use]
pub fn nested_ntz_paths_literal(paths: &[Vec<String>]) -> String {
    let body = paths
        .iter()
        .map(|path| path.join("."))
        .collect::<Vec<_>>()
        .join(",");
    format!("'{}'", body.replace('\'', "''"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeafKind {
    MicroNtz,
    ZonedInstant,
    Other,
}

struct LeafRow {
    target: LeafKind,
    source: LeafKind,
    wrap: bool,
}

const LEAF_ROWS: &[LeafRow] = &[
    LeafRow {
        target: LeafKind::MicroNtz,
        source: LeafKind::ZonedInstant,
        wrap: true,
    },
    LeafRow {
        target: LeafKind::MicroNtz,
        source: LeafKind::MicroNtz,
        wrap: false,
    },
    LeafRow {
        target: LeafKind::MicroNtz,
        source: LeafKind::Other,
        wrap: false,
    },
    LeafRow {
        target: LeafKind::ZonedInstant,
        source: LeafKind::MicroNtz,
        wrap: false,
    },
    LeafRow {
        target: LeafKind::ZonedInstant,
        source: LeafKind::ZonedInstant,
        wrap: false,
    },
    LeafRow {
        target: LeafKind::ZonedInstant,
        source: LeafKind::Other,
        wrap: false,
    },
    LeafRow {
        target: LeafKind::Other,
        source: LeafKind::MicroNtz,
        wrap: false,
    },
    LeafRow {
        target: LeafKind::Other,
        source: LeafKind::ZonedInstant,
        wrap: false,
    },
    LeafRow {
        target: LeafKind::Other,
        source: LeafKind::Other,
        wrap: false,
    },
];

fn leaf_kind(data_type: &DataType) -> LeafKind {
    if is_micro_ntz(data_type) {
        LeafKind::MicroNtz
    } else if is_zoned_instant(data_type) {
        LeafKind::ZonedInstant
    } else {
        LeafKind::Other
    }
}

fn leaf_wraps(source: &DataType, target: &DataType) -> bool {
    let target = leaf_kind(target);
    let source = leaf_kind(source);
    LEAF_ROWS
        .iter()
        .find(|row| row.target == target && row.source == source)
        .is_some_and(|row| row.wrap)
}

fn members_align(source: &DataType, target: &DataType) -> bool {
    let (DataType::Struct(target_fields), DataType::Struct(source_fields)) = (target, source)
    else {
        return true;
    };
    target_fields.len() == source_fields.len()
        && target_fields
            .iter()
            .zip(source_fields.iter())
            .all(|(target_field, source_field)| {
                target_field.name() == source_field.name()
                    && members_align(source_field.data_type(), target_field.data_type())
            })
}

fn source_paths(source: &DataType, target: &DataType, below: bool) -> Vec<Vec<String>> {
    if below && leaf_wraps(source, target) {
        return vec![Vec::new()];
    }
    let (DataType::Struct(target_fields), DataType::Struct(source_fields)) = (target, source)
    else {
        return Vec::new();
    };
    if !members_align(source, target) {
        return Vec::new();
    }
    let mut paths = Vec::new();
    for (target_field, source_field) in target_fields.iter().zip(source_fields.iter()) {
        if name_breaks_paths(source_field.name()) || name_breaks_paths(target_field.name()) {
            continue;
        }
        for mut path in source_paths(source_field.data_type(), target_field.data_type(), true) {
            if path.is_empty() {
                paths.push(vec![source_field.name().clone()]);
            } else {
                path.insert(0, source_field.name().clone());
                paths.push(path);
            }
        }
    }
    paths
}

fn name_breaks_paths(name: &str) -> bool {
    name.contains('.') || name.contains(',')
}

fn target_has_nested_micro_ntz(target: &DataType, below: bool) -> bool {
    match target {
        DataType::Timestamp(TimeUnit::Microsecond, None) if below => true,
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| target_has_nested_micro_ntz(field.data_type(), true)),
        _ => false,
    }
}

fn is_micro_ntz(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(TimeUnit::Microsecond, None))
}

#[must_use]
pub fn nested_micro_ntz_kernel_sql(
    expr_sql: &str,
    source: Option<&DataType>,
    target: &DataType,
) -> Option<String> {
    let NtzDecision::Rebuild(paths) = nested_micro_ntz_decision(source, target) else {
        return None;
    };
    Some(format!(
        "{}(({expr_sql}), {})",
        super::ntz_store::NESTED_NTZ_WALL_CAST_UDF_NAME,
        nested_ntz_paths_literal(&paths)
    ))
}

#[must_use]
pub fn arrow_cast_rejects_not_null_member(source: &DataType, target: &DataType) -> bool {
    let (DataType::Struct(target_fields), DataType::Struct(source_fields)) = (target, source)
    else {
        return false;
    };
    if !members_align(source, target) {
        return false;
    }
    target_fields
        .iter()
        .zip(source_fields.iter())
        .any(|(target_field, source_field)| {
            (source_field.is_nullable() && !target_field.is_nullable())
                || arrow_cast_rejects_not_null_member(
                    source_field.data_type(),
                    target_field.data_type(),
                )
        })
}

#[must_use]
pub fn nested_micro_ntz_wall_sql(
    expr_sql: &str,
    source: Option<&DataType>,
    target: &DataType,
) -> Option<String> {
    let call = nested_micro_ntz_kernel_sql(expr_sql, source, target)?;
    if source.is_some_and(|found| arrow_cast_rejects_not_null_member(found, target)) {
        return None;
    }
    Some(nested_leaf_cast_sql(&call, target))
}

fn is_zoned_instant(source: &DataType) -> bool {
    matches!(source, DataType::Timestamp(_, Some(_)))
}

#[must_use]
pub fn incompatible_update_message(
    table: &str,
    column: &str,
    source: &DataType,
    target: &DataType,
) -> Option<String> {
    incompatible_message(table, column, source, target, false)
}

#[must_use]
pub fn incompatible_store_message(
    table: &str,
    column: &str,
    source: &DataType,
    target: &DataType,
) -> Option<String> {
    incompatible_message(table, column, source, target, true)
}

fn incompatible_message(
    table: &str,
    column: &str,
    source: &DataType,
    target: &DataType,
    name_decimals: bool,
) -> Option<String> {
    let source = normalize_for_assignment(source);
    let target = normalize_for_assignment(target);
    if ansi_store_assignable(source, target) {
        return None;
    }
    if matches!(source, DataType::Map(_, _)) || matches!(target, DataType::Map(_, _)) {
        return incompatible_nested_message(column, source, target);
    }
    let from = store_type_name(source, name_decimals)?;
    let to = store_type_name(target, name_decimals)?;
    Some(spark_error::message(
        spark_error::INCOMPATIBLE_DATA_FOR_TABLE_CANNOT_SAFELY_CAST,
        &[
            ("tableName", table),
            ("columnName", column),
            ("fromType", &from),
            ("toType", &to),
        ],
    ))
}

fn store_type_name(data_type: &DataType, name_decimals: bool) -> Option<Cow<'static, str>> {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale)
            if name_decimals =>
        {
            Some(Cow::Owned(format!("DECIMAL({precision},{scale})")))
        }
        other => spark_update_type_name(other).map(Cow::Borrowed),
    }
}

#[must_use]
pub fn incompatible_nested_message(
    column: &str,
    source: &DataType,
    target: &DataType,
) -> Option<String> {
    let source = normalize_for_assignment(source);
    let target = normalize_for_assignment(target);
    match (source, target) {
        (DataType::Map(from, _), DataType::Map(to, _)) => {
            let (DataType::Struct(from), DataType::Struct(to)) = (from.data_type(), to.data_type())
            else {
                return None;
            };
            from.iter().zip(to.iter()).zip(["key", "value"]).find_map(
                |((source, target), label)| {
                    incompatible_nested_message(
                        &format!("{column}.`{label}`"),
                        source.data_type(),
                        target.data_type(),
                    )
                },
            )
        }
        (DataType::Struct(from), DataType::Struct(to)) => to.iter().find_map(|target| {
            let child = format!("{column}.`{}`", target.name());
            match struct_member(from, target.name()) {
                Some(source) => {
                    incompatible_nested_message(&child, source.data_type(), target.data_type())
                }
                None => Some(format!(
                    "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write incompatible \
                     data for the table ``: Cannot find data for the output column {child}. \
                     SQLSTATE: KD000"
                )),
            }
        }),
        _ if ansi_store_assignable(source, target) => None,
        _ => Some(spark_error::message(
            spark_error::INCOMPATIBLE_DATA_FOR_TABLE_CANNOT_SAFELY_CAST,
            &[
                ("tableName", "``"),
                ("columnName", column),
                ("fromType", spark_update_type_name(source)?),
                ("toType", spark_update_type_name(target)?),
            ],
        )),
    }
}

fn struct_member<'a>(fields: &'a Fields, name: &str) -> Option<&'a Arc<Field>> {
    fields
        .iter()
        .find(|field| field.name() == name)
        .or_else(|| {
            fields
                .iter()
                .find(|field| field.name().eq_ignore_ascii_case(name))
        })
}

fn spark_update_type_name(data_type: &DataType) -> Option<&'static str> {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some("STRING"),
        DataType::Int8 => Some("TINYINT"),
        DataType::Int16 => Some("SMALLINT"),
        DataType::Int32 => Some("INT"),
        DataType::Int64 => Some("BIGINT"),
        DataType::Float32 => Some("FLOAT"),
        DataType::Float64 => Some("DOUBLE"),
        DataType::Boolean => Some("BOOLEAN"),
        DataType::Date32 | DataType::Date64 => Some("DATE"),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Some("TIMESTAMP_NTZ"),
        DataType::Timestamp(_, _) => Some("TIMESTAMP"),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Some("BINARY"),
        DataType::Null => Some("VOID"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
    use std::sync::Arc;

    use super::{
        incompatible_nested_message, incompatible_update_message, nested_micro_ntz_kernel_sql,
        nested_micro_ntz_wall_sql, store_assignment_cast_sql,
    };

    fn map_of(key: DataType, value: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", key, false),
                        Field::new("value", value, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        )
    }

    fn struct_of(names: &[&str]) -> DataType {
        DataType::Struct(
            names
                .iter()
                .map(|name| Field::new(*name, DataType::Int32, true))
                .collect::<Vec<_>>()
                .into(),
        )
    }

    #[test]
    fn a_map_key_or_value_that_cannot_store_assign_names_its_path_as_spark_does() {
        let key = incompatible_update_message(
            "`sc`.`ns`.`t`",
            "`c`",
            &map_of(DataType::Utf8, DataType::Utf8),
            &map_of(DataType::Int32, DataType::Utf8),
        );
        assert_eq!(
            key.as_deref(),
            Some(
                "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data \
                 for the table ``: Cannot safely cast `c`.`key` \"STRING\" to \"INT\". SQLSTATE: KD000"
            )
        );
        let value = incompatible_update_message(
            "`sc`.`ns`.`t`",
            "`c`",
            &map_of(DataType::Utf8, DataType::Date32),
            &map_of(DataType::Utf8, DataType::Int32),
        );
        assert_eq!(
            value.as_deref(),
            Some(
                "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data \
                 for the table ``: Cannot safely cast `c`.`value` \"DATE\" to \"INT\". SQLSTATE: KD000"
            )
        );
    }

    #[test]
    fn a_map_struct_value_missing_a_field_cannot_find_its_data() {
        let text = incompatible_nested_message(
            "`c`",
            &map_of(DataType::Utf8, struct_of(&["a"])),
            &map_of(DataType::Utf8, struct_of(&["a", "b"])),
        );
        assert_eq!(
            text.as_deref(),
            Some(
                "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write incompatible data for \
                 the table ``: Cannot find data for the output column `c`.`value`.`b`. SQLSTATE: \
                 KD000"
            )
        );
    }

    #[test]
    fn a_map_whose_key_and_value_store_assign_has_no_message() {
        assert!(
            incompatible_update_message(
                "``",
                "`c`",
                &map_of(DataType::Utf8, DataType::Int64),
                &map_of(DataType::Utf8, DataType::Int32),
            )
            .is_none()
        );
        assert!(
            incompatible_nested_message(
                "`c`",
                &map_of(DataType::Int32, struct_of(&["a", "b"])),
                &map_of(DataType::Utf8, struct_of(&["b", "a"])),
            )
            .is_none()
        );
    }

    #[test]
    fn the_cast_names_the_target_type_without_field_ids() {
        let target = DataType::Struct(
            vec![Field::new("a", DataType::Int32, true).with_metadata(
                std::collections::HashMap::from([(
                    "PARQUET:field_id".to_string(),
                    "3".to_string(),
                )]),
            )]
            .into(),
        );
        assert_eq!(
            store_assignment_cast_sql("s.v", None, &target),
            "arrow_cast((s.v), 'Struct(\"a\": Int32)')"
        );
        assert_eq!(
            store_assignment_cast_sql("'it''s'", None, &DataType::Utf8),
            "arrow_cast(('it''s'), 'Utf8')"
        );
    }

    #[test]
    fn a_wall_target_casts_through_the_kernel_of_its_unit() {
        let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
        let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let zoned_nanos = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
        assert_eq!(
            store_assignment_cast_sql("s.v", None, &micros),
            "__repark_cast_timestamp_ntz__((s.v))"
        );
        assert_eq!(
            store_assignment_cast_sql("s.v", None, &nanos),
            "__repark_cast_timestamp_ns__((s.v))"
        );
        assert_eq!(
            store_assignment_cast_sql("s.v", None, &zoned_nanos),
            "__repark_cast_timestamptz_ns__((s.v))"
        );
        assert_eq!(
            super::nested_leaf_cast_sql("s.v", &zoned_nanos),
            "arrow_cast((s.v), 'Timestamp(ns, \"UTC\")')"
        );
        assert_eq!(
            super::nested_leaf_cast_sql("s.v", &micros),
            "__repark_cast_timestamp_ntz__((s.v))"
        );
    }

    #[test]
    fn utf8_to_int64_stamps_the_condition() {
        let text = incompatible_update_message(
            "`ice`.`sales`.`t`",
            "`id`",
            &DataType::Utf8,
            &DataType::Int64,
        )
        .expect("Utf8 to Int64 must stamp");
        assert!(
            text.contains("[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]"),
            "{text}"
        );
        assert!(
            text.contains("Cannot write incompatible data for the table"),
            "{text}"
        );
        assert!(text.contains("SQLSTATE: KD000"), "{text}");
        assert!(
            text.contains("Cannot safely cast `id` \"STRING\" to \"BIGINT\""),
            "{text}"
        );
    }

    #[test]
    fn int32_to_int64_returns_none() {
        assert!(
            incompatible_update_message(
                "`ice`.`sales`.`t`",
                "`id`",
                &DataType::Int32,
                &DataType::Int64
            )
            .is_none()
        );
    }

    #[test]
    fn type_outside_the_name_list_returns_none() {
        let list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        assert!(
            incompatible_update_message("`ice`.`sales`.`t`", "`id`", &list, &DataType::Int64)
                .is_none()
        );
    }

    fn ntz() -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, None)
    }

    fn instant(unit: TimeUnit) -> DataType {
        DataType::Timestamp(unit, Some(Arc::from("UTC")))
    }

    fn struct_type(pairs: &[(&str, DataType)]) -> DataType {
        DataType::Struct(
            pairs
                .iter()
                .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
                .collect(),
        )
    }

    #[test]
    fn a_not_null_member_the_cast_cannot_produce_skips_the_wall_only() {
        let source = struct_type(&[
            ("v", instant(TimeUnit::Microsecond)),
            ("n", DataType::Int32),
        ]);
        let target = DataType::Struct(
            vec![
                Field::new("v", ntz(), false),
                Field::new("n", DataType::Int32, true),
            ]
            .into(),
        );
        assert!(nested_micro_ntz_wall_sql("`st`", Some(&source), &target).is_none());
        assert!(nested_micro_ntz_kernel_sql("`st`", Some(&source), &target).is_some());
        let required_source = DataType::Struct(
            vec![
                Field::new("v", instant(TimeUnit::Microsecond), false),
                Field::new("n", DataType::Int32, false),
            ]
            .into(),
        );
        let required_target = DataType::Struct(
            vec![
                Field::new("v", ntz(), false),
                Field::new("n", DataType::Int32, false),
            ]
            .into(),
        );
        let sql = nested_micro_ntz_wall_sql("`st`", Some(&required_source), &required_target)
            .expect("wraps");
        assert!(sql.contains("__repark_cast_nested_ntz__"), "{sql}");
    }

    #[test]
    fn the_leaf_table_wraps_only_a_zoned_instant_into_micro_ntz() {
        use super::{LEAF_ROWS, LeafKind};
        assert_eq!(LEAF_ROWS.len(), 9);
        let wrapping: Vec<_> = LEAF_ROWS.iter().filter(|row| row.wrap).collect();
        assert_eq!(wrapping.len(), 1);
        assert_eq!(wrapping[0].target, LeafKind::MicroNtz);
        assert_eq!(wrapping[0].source, LeafKind::ZonedInstant);
        let mirror = LEAF_ROWS
            .iter()
            .find(|row| row.target == LeafKind::ZonedInstant && row.source == LeafKind::MicroNtz)
            .expect("mirror row");
        assert!(!mirror.wrap);
    }

    #[test]
    fn a_misaligned_struct_stays_main_and_the_two_sites_can_emit_a_bare_kernel() {
        let target = struct_type(&[("v", ntz()), ("n", DataType::Int32)]);
        let cased = struct_type(&[
            ("V", instant(TimeUnit::Microsecond)),
            ("n", DataType::Int32),
        ]);
        let extra = struct_type(&[
            ("v", instant(TimeUnit::Microsecond)),
            ("n", DataType::Int32),
            ("x", DataType::Int32),
        ]);
        let reordered = struct_type(&[
            ("n", DataType::Int32),
            ("v", instant(TimeUnit::Microsecond)),
        ]);
        for source in [&cased, &extra, &reordered] {
            assert!(nested_micro_ntz_wall_sql("`st`", Some(source), &target).is_none());
        }
        let aligned = struct_type(&[
            ("v", instant(TimeUnit::Microsecond)),
            ("n", DataType::Int32),
        ]);
        let kernel = nested_micro_ntz_kernel_sql("`st`", Some(&aligned), &target).expect("kernel");
        assert!(!kernel.contains("arrow_cast"), "{kernel}");
        assert!(
            kernel.contains("__repark_cast_nested_ntz__((`st`), 'v')"),
            "{kernel}"
        );
    }

    #[test]
    fn an_instant_leaf_below_a_struct_is_wrapped_and_its_sibling_is_not() {
        let source = struct_type(&[
            ("v", instant(TimeUnit::Microsecond)),
            ("n", DataType::Int32),
        ]);
        let target = struct_type(&[("v", ntz()), ("n", DataType::Int32)]);
        let sql = nested_micro_ntz_wall_sql("`st`", Some(&source), &target).expect("wraps");
        assert!(sql.starts_with("arrow_cast(("), "{sql}");
        assert!(
            sql.contains("__repark_cast_nested_ntz__((`st`), 'v')"),
            "{sql}"
        );
        assert!(!sql.contains("rand("), "{sql}");
        assert!(!sql.contains("get_field"), "{sql}");
        assert!(!sql.contains("named_struct"), "{sql}");
    }

    #[test]
    fn a_wall_string_date_integer_or_unknown_source_wraps_nothing() {
        let target = struct_type(&[("v", ntz()), ("n", DataType::Int32)]);
        for source in [
            struct_type(&[("v", ntz()), ("n", DataType::Int32)]),
            struct_type(&[("v", DataType::Utf8), ("n", DataType::Int32)]),
            struct_type(&[("v", DataType::Date32), ("n", DataType::Int32)]),
            struct_type(&[("v", DataType::Int64), ("n", DataType::Int32)]),
        ] {
            assert_eq!(
                nested_micro_ntz_wall_sql("`st`", Some(&source), &target),
                None,
                "{source}"
            );
        }
        assert_eq!(nested_micro_ntz_wall_sql("`st`", None, &target), None);
    }

    #[test]
    fn a_nanosecond_wall_leaf_and_a_nested_timestamp_leaf_wrap_nothing() {
        let instant_source = struct_type(&[("v", instant(TimeUnit::Microsecond))]);
        let nanos = struct_type(&[("v", DataType::Timestamp(TimeUnit::Nanosecond, None))]);
        let zoned = struct_type(&[(
            "v",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+00:00"))),
        )]);
        assert_eq!(
            nested_micro_ntz_wall_sql("`st`", Some(&instant_source), &nanos),
            None
        );
        assert_eq!(
            nested_micro_ntz_wall_sql("`st`", Some(&instant_source), &zoned),
            None
        );
        assert_eq!(
            nested_micro_ntz_wall_sql("`v`", Some(&instant(TimeUnit::Nanosecond)), &ntz()),
            None
        );
    }

    #[test]
    fn a_struct_inside_a_struct_wraps_only_the_inner_instant_leaf() {
        let inner_source =
            struct_type(&[("v", instant(TimeUnit::Nanosecond)), ("n", DataType::Int32)]);
        let source = struct_type(&[("i", inner_source), ("m", DataType::Int32)]);
        let inner_target = struct_type(&[("v", ntz()), ("n", DataType::Int32)]);
        let target = struct_type(&[("i", inner_target), ("m", DataType::Int32)]);
        let sql = nested_micro_ntz_wall_sql("s", Some(&source), &target).expect("wraps");
        assert_eq!(
            sql.matches("__repark_cast_nested_ntz__").count(),
            1,
            "{sql}"
        );
        assert!(sql.contains("'i.v'"), "{sql}");
        assert!(!sql.contains("get_field"), "{sql}");
        assert!(!sql.contains("rand("), "{sql}");
    }

    #[test]
    fn a_list_or_map_is_left_as_it_is() {
        let inner = struct_type(&[("v", ntz())]);
        let source_inner = struct_type(&[("v", instant(TimeUnit::Microsecond))]);
        let list_target = DataType::List(Arc::new(Field::new("item", inner.clone(), true)));
        let list_source = DataType::List(Arc::new(Field::new("item", source_inner.clone(), true)));
        assert_eq!(
            nested_micro_ntz_wall_sql("a", Some(&list_source), &list_target),
            None
        );
        let entry = |value: DataType| {
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", value, true),
                ]
                .into(),
            )
        };
        let map_target = DataType::Map(Arc::new(Field::new("entries", entry(inner), false)), false);
        let map_source = DataType::Map(
            Arc::new(Field::new("entries", entry(source_inner), false)),
            false,
        );
        assert_eq!(
            nested_micro_ntz_wall_sql("m", Some(&map_source), &map_target),
            None
        );
    }

    #[test]
    fn store_assignment_wraps_an_instant_leaf_and_an_unknown_source_stays_main() {
        let source = struct_type(&[
            ("v", instant(TimeUnit::Microsecond)),
            ("n", DataType::Int32),
        ]);
        let target = struct_type(&[("v", ntz()), ("n", DataType::Int32)]);
        let wrapped = store_assignment_cast_sql("s", Some(&source), &target);
        assert!(wrapped.contains("__repark_cast_nested_ntz__"), "{wrapped}");
        assert!(wrapped.contains("arrow_cast(("), "{wrapped}");
        assert!(!wrapped.contains("get_field"), "{wrapped}");
        assert_eq!(
            store_assignment_cast_sql("s", None, &target),
            super::nested_leaf_cast_sql("s", &target)
        );
    }

    #[test]
    fn the_four_sites_keep_the_sql_the_walk_emitted() {
        let source = struct_type(&[
            ("v", instant(TimeUnit::Microsecond)),
            ("n", DataType::Int32),
        ]);
        let target = struct_type(&[("v", ntz()), ("n", DataType::Int32)]);
        let sql = nested_micro_ntz_wall_sql("`st`", Some(&source), &target).expect("wraps");
        assert_eq!(
            store_assignment_cast_sql("`st`", Some(&source), &target),
            sql
        );
        assert!(
            sql.contains("__repark_cast_nested_ntz__((`st`), 'v')"),
            "{sql}"
        );
        assert!(sql.starts_with("arrow_cast(("), "{sql}");
    }

    #[test]
    fn a_scalar_subquery_is_bound_once_and_a_column_is_not() {
        let mut source_fields = Vec::new();
        let mut target_fields = Vec::new();
        for index in 0..19 {
            let name = format!("f{index:02}");
            source_fields.push(Field::new(&name, DataType::Int32, true));
            target_fields.push(Field::new(&name, DataType::Int32, true));
        }
        source_fields.push(Field::new("v", instant(TimeUnit::Microsecond), true));
        target_fields.push(Field::new("v", ntz(), true));
        let source = DataType::Struct(source_fields.into());
        let target = DataType::Struct(target_fields.into());
        let expr = "(SELECT st FROM ice.ns.src)";
        let sql = nested_micro_ntz_wall_sql(expr, Some(&source), &target).expect("wraps");
        assert_eq!(sql.matches(expr).count(), 1, "{sql}");
        assert!(!sql.contains("rand("), "{sql}");
        assert!(!sql.contains("get_field"), "{sql}");
        assert!(
            sql.contains("__repark_cast_nested_ntz__(((SELECT st FROM ice.ns.src)), 'v')"),
            "{sql}"
        );
        let column = nested_micro_ntz_wall_sql("`ice`.`ns`.`src`.`st`", Some(&source), &target)
            .expect("column");
        assert!(!column.contains("rand("), "{column}");
        assert!(
            column.contains("__repark_cast_nested_ntz__((`ice`.`ns`.`src`.`st`), 'v')"),
            "{column}"
        );
    }
}
