//! Model: Claude Fable 5
//! CodeQuality:S
//!
//! ANSI-door pins for the v3 type rulings (V3R-1, owner rulings 2026-08-25): `geometry` /
//! `geography` are DECLARED out of v1.0 (registry `V3-GEO-1`); `variant` stays V3-6 work with
//! shredded-Parquet variant DECLARED out of the gate (queued `V3-VARIANT-SHRED-1`). No
//! ANSI-door surface reaches any of the three: CREATE refuses at type resolution, naming the
//! type, and leaves no table behind. A later landing reds this on purpose.
//!
//! pins: v3r-1-rulings/C-008, C-009

use iceberg::{NamespaceIdent, TableIdent};

use crate::v3_cow::door_with_v3_opt_in;

#[tokio::test]
async fn v3_type_columns_geometry_geography_variant_refuse_naming_the_type() {
    let door = door_with_v3_opt_in().await;
    for type_name in ["GEOMETRY", "GEOGRAPHY", "VARIANT"] {
        let table = format!("t_{}", type_name.to_ascii_lowercase());
        let err = door
            .err(&format!(
                "CREATE TABLE ice.sales.{table} (id INT, v {type_name}) WITH (format_version = 3)"
            ))
            .await;
        assert!(
            err.to_ascii_uppercase().contains(type_name),
            "CREATE with a `{type_name}` column must refuse naming the type: {err}"
        );
        let exists = door
            .catalog
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                table.clone(),
            ))
            .await
            .expect("table_exists");
        assert!(!exists, "a refused CREATE must leave no `{table}` behind");
    }
}
