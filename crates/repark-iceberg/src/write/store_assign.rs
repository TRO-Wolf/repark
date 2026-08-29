//! Shared Spark ANSI store-assignment matrix for every write path.
//!
//! The matrix rejects pairs such as boolean→integer, timestamp→bigint, string→numeric, and
//! date→integer before Arrow casts can reinterpret values. It is distinct from CAST legality:
//! callers must not use this predicate to validate explicit casts.

use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};

/// Spark's error class for the MERGE store-assignment refusals (#111 / #135 text — byte-stable;
/// changing it moves a shipped needle).
pub(crate) const MERGE_SPARK_CLASS: &str = "INCOMPATIBLE_DATA_FOR_TABLE";

/// Spark's sub-class for the plain INSERT / append store-assignment refusals. Measured against
/// live PySpark 4.1.2 ANSI on every non-MERGE write door (`INSERT INTO … SELECT`,
/// `INSERT INTO … VALUES`, `INSERT OVERWRITE`, `writeTo().append()`, `write.insertInto()`):
/// `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`. The coarse class above is a prefix of it, so
/// a pin on either engine can use the coarse needle and a stronger pin can use the sub-class.
pub(crate) const WRITE_SPARK_CLASS: &str = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST";

/// Strip wrappers that do not change assignability (dictionary encoding).
pub(crate) fn normalize_for_assignment(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value) => normalize_for_assignment(value),
        other => other,
    }
}

/// Spark `Cast.canANSIStoreAssign`, translated to Arrow types (v1: nested types must be
/// identical — Spark's per-field recursion is a named residual).
pub(crate) fn ansi_store_assignable(src: &DataType, dst: &DataType) -> bool {
    use DataType::{
        Binary, Boolean, Date32, Date64, LargeBinary, LargeUtf8, Null, Timestamp, Utf8, Utf8View,
    };
    if src == dst {
        return true;
    }
    // NullType → anything (the projection NULL-fills nullable columns as untyped NULL).
    if matches!(src, Null) {
        return true;
    }
    // NumericType → NumericType (widening AND narrowing — overflow is the strict runtime cast's
    // ANSI error, exactly Spark's split between analysis-legal and runtime-failing).
    if src.is_numeric() && dst.is_numeric() {
        return true;
    }
    // AtomicType → StringType.
    let is_string = |t: &DataType| matches!(t, Utf8 | LargeUtf8 | Utf8View);
    let is_atomic = |t: &DataType| {
        t.is_numeric()
            || matches!(
                t,
                Boolean | Utf8 | LargeUtf8 | Utf8View | Binary | LargeBinary | Date32 | Date64
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
    // Binary width variants.
    if matches!(src, Binary | LargeBinary) && matches!(dst, Binary | LargeBinary) {
        return true;
    }
    false
}

/// ===========================================================================================
/// The shared refusal. `op` is the whole path label the message opens with — `MERGE INSERT`,
/// `MERGE UPDATE SET`, `INSERT OVERWRITE`, `append` — and `spark_class` is the class citation.
/// The MERGE callers pass [`MERGE_SPARK_CLASS`] so their shipped text stays byte-identical; the
/// non-MERGE write paths pass [`WRITE_SPARK_CLASS`], the sub-class Spark actually raises there.
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::Plan`] carrying the `not ANSI-store-assignable` needle, the column name and
/// BOTH types, when `source_type` is not ANSI-store-assignable to `target_type`.
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

/// ===========================================================================================
/// The non-MERGE write-path gate (WI-1): same matrix, [`WRITE_SPARK_CLASS`] citation, and one
/// deliberate narrowing — **nested pairs are not judged**.
///
/// [`ansi_store_assignable`] answers nested types by identity only (`src == dst`), which is a
/// named v1 residual rather than Spark's per-field recursion. On the MERGE path that strictness
/// is already shipped behaviour; on the append / overwrite conform paths it would be a NEW
/// refusal for pairs this unit never measured — e.g. a `List<Utf8View>` plan column landing in a
/// `List<Utf8>` Iceberg column, which conforms correctly today through the strict arrow cast.
/// A nested pair therefore falls through to that cast (loud on failure, never silent), exactly as
/// before. Flat pairs — where every measured silently-wrong reinterpretation lives — are gated.
/// ===========================================================================================
///
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

/// Whether `data_type` is a leaf Arrow type the v1 matrix can judge (see
/// [`refuse_unless_write_store_assignable`] for why nested pairs are excused).
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
    };

    /// The WI-1 row the hoist exists for: `Date32 → Int32|Int64` is the silently-wrong pair every
    /// plain INSERT door persisted before the gate (Spark refuses
    /// `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`), and the reverse `Int → Date` is refused
    /// in the same stroke because the matrix was always right about it — only the call sites were
    /// missing.
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
        use DataType::{Date32, Float64, Int32, Int64, Null, Utf8, Utf8View};
        assert!(ansi_store_assignable(&Int32, &Int64)); // widening
        assert!(ansi_store_assignable(&Int64, &Float64));
        assert!(ansi_store_assignable(&Null, &Int32)); // NULL-fill
        assert!(ansi_store_assignable(&Date32, &Utf8)); // atomic → string
        assert!(ansi_store_assignable(&Utf8View, &Utf8)); // string width variant
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

    /// Nested pairs fall through to the strict arrow cast rather than gaining a NEW refusal
    /// (the documented WI-1 narrowing) — while the flat matrix would have refused them.
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
}
