//! Model: Claude Fable 5
//! ANSI-door pins declare `geometry`, `geography`, and `variant` out at CREATE (`V3-GEO-1`).
//! pins: v3r-1-rulings/C-008, C-009

use iceberg::{NamespaceIdent, TableIdent};

use super::cow::door_with_v3_opt_in;

#[tokio::test]
async fn v3_type_columns_geometry_geography_variant_refuse_naming_the_type() {
    let door = door_with_v3_opt_in().await;
    for type_name in ["GEOMETRY", "GEOGRAPHY"] {
        let table = format!("t_{}", type_name.to_ascii_lowercase());
        let err = door
            .err(&format!(
                "CREATE TABLE ice.sales.{table} (id INT, v {type_name}) WITH (format_version = 3)"
            ))
            .await;
        assert!(
            err.contains("[UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED]")
                && err.contains("SQLSTATE: 0A000"),
            "CREATE with a `{type_name}` column must carry condition and SQLSTATE: {err}"
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
    let variant_err = door
        .err("CREATE TABLE ice.sales.t_variant (id INT, v VARIANT) WITH (format_version = 3)")
        .await;
    assert!(
        variant_err.to_ascii_uppercase().contains("VARIANT"),
        "CREATE with a `VARIANT` column must refuse naming the type: {variant_err}"
    );
    let variant_exists = door
        .catalog
        .table_exists(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "t_variant".to_string(),
        ))
        .await
        .expect("table_exists");
    assert!(
        !variant_exists,
        "a refused CREATE must leave no `t_variant` behind"
    );
    let srid_err = door
        .err(
            "CREATE TABLE ice.sales.t_geometry_4326 (id INT, g GEOMETRY(4326)) WITH (format_version = 3)",
        )
        .await;
    assert!(
        srid_err.contains("[UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED]")
            && srid_err.contains("SQLSTATE: 0A000"),
        "CREATE with a `GEOMETRY(4326)` column must carry condition and SQLSTATE: {srid_err}"
    );
    let srid_exists = door
        .catalog
        .table_exists(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "t_geometry_4326".to_string(),
        ))
        .await
        .expect("table_exists");
    assert!(
        !srid_exists,
        "a refused CREATE must leave no `t_geometry_4326` behind"
    );
}

#[tokio::test]
async fn v3_type_column_named_geometry_with_int_succeeds() {
    let door = door_with_v3_opt_in().await;
    door.ok(
        "CREATE TABLE ice.sales.t_named_geometry (id INT, geometry INT) WITH (format_version = 3)",
    )
    .await;
    let exists = door
        .catalog
        .table_exists(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "t_named_geometry".to_string(),
        ))
        .await
        .expect("table_exists");
    assert!(
        exists,
        "the named-geometry CREATE must leave its table behind"
    );
}

/// pins: v3-6-v3-types/C-004
#[tokio::test]
async fn v3_type_column_unknown_refuses_naming_the_type() {
    let door = door_with_v3_opt_in().await;
    let err = door
        .err("CREATE TABLE ice.sales.t_unknown (id INT, u UNKNOWN) WITH (format_version = 3)")
        .await;
    assert!(
        err.to_ascii_uppercase().contains("UNKNOWN"),
        "CREATE with an `UNKNOWN` column must refuse naming the type: {err}"
    );
    let exists = door
        .catalog
        .table_exists(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "t_unknown".to_string(),
        ))
        .await
        .expect("table_exists");
    assert!(!exists, "a refused CREATE must leave no table behind");
}
