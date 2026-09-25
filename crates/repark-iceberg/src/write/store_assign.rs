//! Shared Spark ANSI store-assignment matrix for every write path.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::error::{DataFusionError, Result};

/// Spark's error class for the MERGE store-assignment refusals.
pub(crate) const MERGE_SPARK_CLASS: &str = "INCOMPATIBLE_DATA_FOR_TABLE";

/// Spark's sub-class for the plain INSERT / append store-assignment refusals.
pub(crate) const WRITE_SPARK_CLASS: &str = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST";

/// Strip wrappers that do not change assignability (dictionary encoding).
pub(crate) fn normalize_for_assignment(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value) => normalize_for_assignment(value),
        other => other,
    }
}

/// Spark `Cast.canANSIStoreAssign`, translated to Arrow types.
pub(crate) fn ansi_store_assignable(src: &DataType, dst: &DataType) -> bool {
    use DataType::{
        Binary, BinaryView, Boolean, Date32, Date64, LargeBinary, LargeUtf8, Null, Timestamp, Utf8,
        Utf8View,
    };
    if src == dst {
        return true;
    }
    if let (DataType::Struct(from), DataType::Struct(to)) = (src, dst) {
        return struct_fields_store_assignable(from, to);
    }
    // NullType → anything (the projection NULL-fills nullable columns as untyped NULL).
    if matches!(src, Null) {
        return true;
    }
    // NumericType → NumericType.
    if src.is_numeric() && dst.is_numeric() {
        return true;
    }
    // AtomicType → StringType.
    let is_string = |t: &DataType| matches!(t, Utf8 | LargeUtf8 | Utf8View);
    let is_atomic = |t: &DataType| {
        t.is_numeric()
            || matches!(
                t,
                Boolean
                    | Utf8
                    | LargeUtf8
                    | Utf8View
                    | Binary
                    | BinaryView
                    | LargeBinary
                    | Date32
                    | Date64
            )
            || matches!(t, Timestamp(_, _))
    };
    if is_string(dst) && is_atomic(src) {
        return true;
    }
    // String width variants among themselves.
    if is_string(src) && is_string(dst) {
        return true;
    }
    // Date ↔ Timestamp, both directions; timestamp unit/annotation changes within timestamps.
    let is_date = |t: &DataType| matches!(t, Date32 | Date64);
    let is_ts = |t: &DataType| matches!(t, Timestamp(_, _));
    if (is_date(src) && (is_date(dst) || is_ts(dst)))
        || (is_ts(src) && (is_ts(dst) || is_date(dst)))
    {
        return true;
    }
    if matches!(src, Binary | BinaryView | LargeBinary)
        && matches!(dst, Binary | BinaryView | LargeBinary)
    {
        return true;
    }
    false
}

fn struct_fields_store_assignable(from: &Fields, to: &Fields) -> bool {
    from.len() == to.len()
        && from.iter().zip(to.iter()).all(|(source, target)| {
            source.name().eq_ignore_ascii_case(target.name())
                && ansi_store_assignable(
                    normalize_for_assignment(source.data_type()),
                    normalize_for_assignment(target.data_type()),
                )
        })
}

pub(crate) fn without_field_metadata(data_type: &DataType) -> DataType {
    let strip = |field: &Arc<Field>| {
        Arc::new(Field::new(
            field.name(),
            without_field_metadata(field.data_type()),
            field.is_nullable(),
        ))
    };
    match data_type {
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(strip).collect()),
        DataType::List(field) => DataType::List(strip(field)),
        DataType::LargeList(field) => DataType::LargeList(strip(field)),
        DataType::Map(field, sorted) => DataType::Map(strip(field), *sorted),
        other => other.clone(),
    }
}

/// The shared refusal.
/// # Errors
/// Returns `Plan` with the `not ANSI-store-assignable` needle, the column name, and both types.
pub(crate) fn refuse_unless_ansi_store_assignable(
    op: &str,
    spark_class: &str,
    column: &str,
    source_type: &DataType,
    target_type: &DataType,
) -> Result<()> {
    let src = normalize_for_assignment(source_type);
    let dst = normalize_for_assignment(target_type);
    if !ansi_store_assignable(src, dst) {
        return Err(DataFusionError::Plan(format!(
            "{op} cannot store-assign column `{column}`: source type {source_type} is not \
             ANSI-store-assignable to target type {target_type} (Spark {spark_class}; \
             add an explicit CAST only if the reinterpretation is intended semantics)"
        )));
    }
    Ok(())
}

/// The non-MERGE write-path gate (WI-1): same matrix, with nested pairs not judged.
/// # Errors
/// Same as [`refuse_unless_ansi_store_assignable`], for flat `(source, target)` pairs.
pub(crate) fn refuse_unless_write_store_assignable(
    op: &str,
    column: &str,
    source_type: &DataType,
    target_type: &DataType,
) -> Result<()> {
    let src = normalize_for_assignment(source_type);
    let dst = normalize_for_assignment(target_type);
    if !is_flat(src) || !is_flat(dst) {
        return Ok(());
    }
    refuse_unless_ansi_store_assignable(op, WRITE_SPARK_CLASS, column, source_type, target_type)
}

/// Whether `data_type` is a leaf Arrow type the v1 matrix can judge.
fn is_flat(data_type: &DataType) -> bool {
    !matches!(
        data_type,
        DataType::List(_)
            | DataType::ListView(_)
            | DataType::LargeList(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Map(_, _)
            | DataType::Union(_, _)
            | DataType::RunEndEncoded(_, _)
    )
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
    use std::sync::Arc;

    use super::{
        MERGE_SPARK_CLASS, WRITE_SPARK_CLASS, ansi_store_assignable,
        refuse_unless_ansi_store_assignable, refuse_unless_write_store_assignable,
        without_field_metadata,
    };

    /// WI-1: `Date32 → Int32|Int64` is the silently-wrong pair every plain INSERT persisted before.
    #[test]
    fn date_and_int_are_not_store_assignable_in_either_direction() {
        use DataType::{Date32, Date64, Int32, Int64};
        assert!(!ansi_store_assignable(&Date32, &Int32));
        assert!(!ansi_store_assignable(&Date32, &Int64));
        assert!(!ansi_store_assignable(&Date64, &Int32));
        assert!(!ansi_store_assignable(&Int32, &Date32));
        assert!(!ansi_store_assignable(&Int64, &Date32));
    }

    /// The positive controls the non-MERGE gate must NOT break.
    #[test]
    fn widening_null_fill_and_atomic_to_string_stay_assignable() {
        use DataType::{
            Binary, BinaryView, Date32, Float64, Int32, Int64, LargeBinary, Null, Utf8, Utf8View,
        };
        assert!(ansi_store_assignable(&Int32, &Int64));
        assert!(ansi_store_assignable(&Int64, &Float64));
        assert!(ansi_store_assignable(&Null, &Int32));
        assert!(ansi_store_assignable(&Date32, &Utf8));
        assert!(ansi_store_assignable(&Utf8View, &Utf8));
        assert!(ansi_store_assignable(&BinaryView, &Binary));
        assert!(ansi_store_assignable(&BinaryView, &LargeBinary));
        assert!(ansi_store_assignable(
            &Date32,
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        ));
    }

    /// Both entry points carry the shared needle, the column, both types, and their own class.
    #[test]
    fn refusals_carry_the_needle_the_column_both_types_and_the_class() {
        let merge = refuse_unless_ansi_store_assignable(
            "MERGE INSERT",
            MERGE_SPARK_CLASS,
            "v",
            &DataType::Date32,
            &DataType::Int32,
        )
        .expect_err("date→int must refuse")
        .to_string();
        assert!(merge.starts_with("Error during planning: MERGE INSERT cannot store-assign"));
        assert!(merge.contains("not ANSI-store-assignable"), "{merge}");
        assert!(
            merge.contains("Spark INCOMPATIBLE_DATA_FOR_TABLE;"),
            "{merge}"
        );

        let write = refuse_unless_write_store_assignable(
            "INSERT OVERWRITE",
            "v",
            &DataType::Date32,
            &DataType::Int32,
        )
        .expect_err("date→int must refuse")
        .to_string();
        assert!(
            write.contains("INSERT OVERWRITE cannot store-assign column `v`"),
            "{write}"
        );
        assert!(write.contains("not ANSI-store-assignable"), "{write}");
        assert!(write.contains("Date32"), "{write}");
        assert!(write.contains("Int32"), "{write}");
        assert!(write.contains(WRITE_SPARK_CLASS), "{write}");
    }

    /// Dictionary wrappers are transparent on BOTH sides.
    #[test]
    fn dictionary_encoding_is_transparent() {
        let dict_date = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Date32));
        assert!(
            refuse_unless_write_store_assignable("append", "v", &dict_date, &DataType::Int32)
                .is_err()
        );
        let dict_utf8 = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        assert!(
            refuse_unless_write_store_assignable("append", "v", &dict_date, &dict_utf8).is_ok()
        );
    }

    /// Nested pairs fall through to the strict arrow cast rather than gaining a NEW refusal.
    #[test]
    fn nested_pairs_are_excused_by_the_write_gate() {
        let list_view = DataType::List(Arc::new(Field::new("item", DataType::Utf8View, true)));
        let list_utf8 = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        assert!(
            !ansi_store_assignable(&list_view, &list_utf8),
            "the v1 matrix judges nested pairs by identity"
        );
        assert!(
            refuse_unless_write_store_assignable("append", "v", &list_view, &list_utf8).is_ok(),
            "the write gate must not manufacture a nested refusal"
        );
        // …but the flat half of a nested/flat pair is excused too (nothing to judge).
        assert!(
            refuse_unless_write_store_assignable("append", "v", &list_view, &DataType::Int32)
                .is_ok()
        );
    }

    fn field_id(field: Field, id: &str) -> Field {
        field.with_metadata(std::collections::HashMap::from([(
            "PARQUET:field_id".to_string(),
            id.to_string(),
        )]))
    }

    #[test]
    fn structs_are_judged_field_by_field_without_field_ids() {
        let target = DataType::Struct(
            vec![
                field_id(Field::new("a", DataType::Int32, true), "3"),
                field_id(Field::new("b", DataType::Utf8, true), "4"),
            ]
            .into(),
        );
        let widened = DataType::Struct(
            vec![
                Field::new("A", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
            ]
            .into(),
        );
        assert!(ansi_store_assignable(&widened, &target));
        let string_leaf = DataType::Struct(
            vec![
                Field::new("a", DataType::Utf8, true),
                Field::new("b", DataType::Utf8, true),
            ]
            .into(),
        );
        assert!(!ansi_store_assignable(&string_leaf, &target));
        let renamed = DataType::Struct(
            vec![
                Field::new("q", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ]
            .into(),
        );
        assert!(!ansi_store_assignable(&renamed, &target));
        let short = DataType::Struct(vec![Field::new("a", DataType::Int32, true)].into());
        assert!(!ansi_store_assignable(&short, &target));
    }

    #[test]
    fn field_ids_are_stripped_at_every_depth() {
        let inner =
            DataType::Struct(vec![field_id(Field::new("x", DataType::Int32, false), "7")].into());
        let nested = DataType::List(Arc::new(field_id(Field::new("element", inner, true), "6")));
        assert_eq!(
            without_field_metadata(&nested).to_string(),
            "List(Struct(\"x\": non-null Int32), field: 'element')"
        );
    }
}
