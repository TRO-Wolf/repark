use super::super::{MergeSql, row_lineage};
use super::merge::{spec, update};

use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use iceberg::metadata_columns::{
    RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_COL_NAME_ROW_ID,
};

fn arrow_schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ])
}

#[test]
fn rewrite_projection_carries_stored_row_id_and_nulls_last_updated_on_change() {
    let _: &str = "pins: v3-7-merge-lineage/C-001";
    let merge_spec = spec(vec![update(None, &[("name", "s.name")])], vec![]);
    let sql = MergeSql {
        spec: &merge_spec,
        target_name: "scratch",
        match_flag: "__repark_matched_t",
        carry_lineage: true,
    };
    let projection = sql.rewrite_projection(&arrow_schema());
    assert!(
        projection.contains("t.\"_row_id\" AS \"_row_id\""),
        "survivors must keep stored _row_id, got: {projection}"
    );
    assert!(
        projection.contains("CAST(NULL AS BIGINT)")
            && projection.contains("_last_updated_sequence_number"),
        "changed rows must write null last-updated, got: {projection}"
    );
    assert!(
        !projection.contains(&format!("WHEN 0 THEN (t.\"{RESERVED_COL_NAME_ROW_ID}\")")),
        "UPDATE SET must not rewrite _row_id, got: {projection}"
    );
}

#[test]
fn rewrite_projection_without_lineage_flag_stays_user_columns() {
    let _: &str = "pins: v3-7-merge-lineage/C-001";
    let merge_spec = spec(vec![update(None, &[("name", "s.name")])], vec![]);
    let sql = MergeSql {
        spec: &merge_spec,
        target_name: "scratch",
        match_flag: "__repark_matched_t",
        carry_lineage: false,
    };
    let projection = sql.rewrite_projection(&arrow_schema());
    assert!(
        !projection.contains(RESERVED_COL_NAME_ROW_ID)
            && !projection.contains(RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER),
        "v2 rewrite must not project lineage columns, got: {projection}"
    );
}

#[test]
fn scratch_schema_with_lineage_appends_reserved_pair_before_identity() {
    let _: &str = "pins: v3-7-merge-lineage/C-001";
    let schema = std::sync::Arc::new(arrow_schema());
    let scratch = row_lineage::scratch_schema_with_lineage(&schema, true);
    let names: Vec<String> = scratch
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(
        names,
        vec![
            "id".to_string(),
            "name".to_string(),
            RESERVED_COL_NAME_ROW_ID.to_string(),
            RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER.to_string(),
            "_file".to_string(),
            "_pos".to_string(),
        ]
    );
}
