/// Column-def CREATE TABLE schema-equals a CTAS twin; empty row count; **no data write**.
use super::super::*;
use super::common::*;

#[tokio::test]
#[allow(clippy::too_many_lines)] // one flat pin battery over the CREATE/CTAS twin matrix
async fn column_def_create_schema_equals_ctas_twin() {
    use iceberg::spec::PrimitiveType;

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.col_def (id BIGINT NOT NULL, name STRING, active BOOLEAN) \
             USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet')",
    )
    .await
    .expect("column-def CREATE");

    // CTAS twin: same names/types (nullable — CTAS NULL casts), zero rows (WHERE false).
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ctas_twin USING iceberg AS \
             SELECT CAST(NULL AS BIGINT) AS id, CAST(NULL AS VARCHAR) AS name, \
                    CAST(NULL AS BOOLEAN) AS active WHERE false",
    )
    .await
    .expect("CTAS twin");

    let col_def = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "col_def".to_string(),
        ))
        .await
        .unwrap();
    let twin = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "ctas_twin".to_string(),
        ))
        .await
        .unwrap();

    // Attack focus: accidental data write on schema-only CREATE.
    assert!(
        col_def.metadata().current_snapshot_id().is_none(),
        "schema-only CREATE must not stamp a current snapshot"
    );
    let location = col_def.metadata().location().to_string();
    let mut parquet_count = 0usize;
    walk_parquet(std::path::Path::new(&location), &mut parquet_count);
    assert_eq!(
        parquet_count, 0,
        "schema-only CREATE must write zero parquet data files under {location}"
    );
    // NOT NULL → required.
    assert!(
        col_def.metadata().current_schema().as_struct().fields()[0].required,
        "NOT NULL must map to Iceberg required"
    );
    assert!(!col_def.metadata().current_schema().as_struct().fields()[1].required);

    let col_fields: Vec<_> = col_def
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.name.clone(), field.field_type.to_string()))
        .collect();
    let twin_fields: Vec<_> = twin
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.name.clone(), field.field_type.to_string()))
        .collect();
    assert_eq!(
        col_fields, twin_fields,
        "column-def schema must equal CTAS twin (name, type)"
    );
    // Explicit type pins (oracle min: schema equality class).
    assert!(matches!(
        col_def.metadata().current_schema().as_struct().fields()[0]
            .field_type
            .as_ref(),
        iceberg::spec::Type::Primitive(PrimitiveType::Long)
    ));
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.col_def").await,
        0
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.ctas_twin").await,
        0
    );

    // DEFAULT column option must refuse loud (not silent ignore).
    let default_err = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.with_def (id BIGINT DEFAULT 0) USING iceberg",
    )
    .await
    .expect_err("DEFAULT must refuse");
    assert!(
        default_err.to_string().contains("not supported"),
        "got: {default_err}"
    );
}

/// Column-def CREATE with PARTITIONED BY identity + TBLPROPERTIES.
#[tokio::test]
async fn column_def_create_partitioned_by_identity() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.parted (id BIGINT, category STRING) \
             USING iceberg PARTITIONED BY (category)",
    )
    .await
    .expect("partitioned column-def CREATE");
    let table = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "parted".to_string(),
        ))
        .await
        .unwrap();
    let spec = table.metadata().default_partition_spec();
    assert!(
        !spec.is_unpartitioned(),
        "must carry an identity partition on category"
    );
    assert_eq!(spec.fields().len(), 1);
    assert_eq!(spec.fields()[0].name, "category");
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.parted").await,
        0
    );
}

/// LOCATION + Hive ROW FORMAT refuse; CTAS TEMPORARY refuse.
#[tokio::test]
#[allow(clippy::too_many_lines)] // one flat refuse-clause pin battery
async fn column_def_location_and_ctas_temporary_refuse() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let location_err = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.loc (id BIGINT) USING iceberg LOCATION '/tmp/should_not'",
    )
    .await
    .expect_err("LOCATION must refuse");
    assert!(
        location_err.to_string().contains("LOCATION")
            && location_err.to_string().contains("not supported"),
        "got: {location_err}"
    );
    assert!(
        !catalogs["ice"]
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                "loc".to_string(),
            ))
            .await
            .unwrap()
    );

    let row_format_err = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.rf (id BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','",
    )
    .await
    .expect_err("ROW FORMAT must refuse");
    assert!(
        row_format_err.to_string().contains("not supported")
            && (row_format_err.to_string().contains("ROW FORMAT")
                || row_format_err.to_string().contains("Hive")),
        "got: {row_format_err}"
    );
    // STORED AS lands in hive_formats.storage.
    let stored_as = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.stored (id BIGINT) STORED AS PARQUET",
    )
    .await
    .expect_err("STORED AS must refuse");
    assert!(
        stored_as.to_string().contains("not supported"),
        "got: {stored_as}"
    );

    let temp_ctas = execute(
        &ctx,
        &catalogs,
        "CREATE TEMPORARY TABLE ice.sales.ctmp AS SELECT * FROM src",
    )
    .await
    .expect_err("CTAS TEMPORARY must refuse");
    assert!(
        temp_ctas.to_string().contains("TEMPORARY")
            && temp_ctas.to_string().contains("not supported"),
        "got: {temp_ctas}"
    );
    assert!(
        !catalogs["ice"]
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                "ctmp".to_string(),
            ))
            .await
            .unwrap(),
        "refused CTAS TEMPORARY must not leave a durable table"
    );

    let ctas_location = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.cloc LOCATION '/tmp/x' AS SELECT * FROM src",
    )
    .await
    .expect_err("CTAS LOCATION must refuse");
    assert!(
        ctas_location.to_string().contains("LOCATION")
            && ctas_location.to_string().contains("not supported"),
        "got: {ctas_location}"
    );
    assert!(
        !catalogs["ice"]
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                "cloc".to_string(),
            ))
            .await
            .unwrap()
    );

    // Table COMMENT must refuse.
    let comment_err = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.cm (id BIGINT) COMMENT 'hello'",
    )
    .await
    .expect_err("COMMENT must refuse");
    assert!(
        comment_err.to_string().contains("COMMENT")
            && comment_err.to_string().contains("not supported"),
        "got: {comment_err}"
    );

    // pins: v3-2-create-v3-opt-in/C-007
    // format-version=1 refuse on column-def.
    let fv1 = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.fv1 (id BIGINT) USING iceberg \
             TBLPROPERTIES ('format-version' = '1')",
    )
    .await
    .expect_err("format-version=1");
    assert!(
        fv1.to_string().contains("format-version") && fv1.to_string().contains("not supported"),
        "got: {fv1}"
    );

    // pins: v3-2-create-v3-opt-in/C-004
    let fv3 = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.fv3 (id BIGINT) USING iceberg \
             TBLPROPERTIES ('format-version' = '3')",
    )
    .await
    .expect_err("format-version=3 without opt-in");
    assert!(
        fv3.to_string()
            .contains("repark.sql.allowCreateFormatVersion3")
            && fv3.to_string().contains("format-version"),
        "opt-in refuse must name conf and property: {fv3}"
    );
    assert!(
        !catalogs["ice"]
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                "fv3".to_string(),
            ))
            .await
            .unwrap()
    );

    // Schema-only → INSERT → CREATE BRANCH default.
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.so_branch (id INT, name STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.so_branch SELECT 1 AS id, 'a' AS name",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.so_branch CREATE BRANCH after_insert",
    )
    .await;
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.so_branch VERSION AS OF 'after_insert' ORDER BY id"
        )
        .await,
        vec![1]
    );
}

/// TEMPORARY refuse; `testing_create_ref` seam still works; typed cols.
#[tokio::test]
#[allow(clippy::too_many_lines)] // one flat refuse + seam + typed-cols pin battery
async fn column_def_temporary_refuse_testing_create_ref_and_types() {
    use iceberg::spec::PrimitiveType;
    use repark_iceberg::write::{SnapshotRefKind, testing_create_ref};

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let temp_err = execute(
        &ctx,
        &catalogs,
        "CREATE TEMPORARY TABLE ice.sales.tmp (id BIGINT) USING iceberg",
    )
    .await
    .expect_err("TEMPORARY must refuse");
    assert!(
        temp_err.to_string().contains("TEMPORARY")
            && temp_err.to_string().contains("not supported"),
        "got: {temp_err}"
    );
    assert!(
        !catalogs["ice"]
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                "tmp".to_string(),
            ))
            .await
            .unwrap(),
        "refused TEMPORARY must not leave a durable table"
    );

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.typed (\
             d DECIMAL(10,2), ts TIMESTAMP, dt DATE, f FLOAT, bin BINARY, s VARCHAR(10)\
             ) USING iceberg",
    )
    .await;
    let typed = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "typed".to_string(),
        ))
        .await
        .unwrap();
    let fields = typed.metadata().current_schema().as_struct().fields();
    assert!(matches!(
        fields[0].field_type.as_ref(),
        iceberg::spec::Type::Primitive(PrimitiveType::Decimal {
            precision: 10,
            scale: 2
        })
    ));
    assert!(matches!(
        fields[1].field_type.as_ref(),
        iceberg::spec::Type::Primitive(PrimitiveType::Timestamptz)
    ));
    assert!(matches!(
        fields[2].field_type.as_ref(),
        iceberg::spec::Type::Primitive(PrimitiveType::Date)
    ));
    assert!(matches!(
        fields[3].field_type.as_ref(),
        iceberg::spec::Type::Primitive(PrimitiveType::Float)
    ));
    assert!(matches!(
        fields[4].field_type.as_ref(),
        iceberg::spec::Type::Primitive(PrimitiveType::Binary)
    ));
    assert!(matches!(
        fields[5].field_type.as_ref(),
        iceberg::spec::Type::Primitive(PrimitiveType::String)
    ));
    assert!(typed.metadata().current_snapshot_id().is_none());

    // testing_create_ref seam must remain (I5 charter).
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.tref AS SELECT * FROM src",
    )
    .await;
    let s1 = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "tref".to_string(),
        ))
        .await
        .unwrap()
        .metadata()
        .current_snapshot_id()
        .expect("snapshot");
    testing_create_ref(
        catalogs["ice"].as_ref(),
        &TableIdent::new(NamespaceIdent::new("sales".to_string()), "tref".to_string()),
        SnapshotRefKind::Tag,
        "via_testing",
        s1,
    )
    .await
    .expect("testing_create_ref must stay");
    assert_eq!(
        time_travel_id_multiset(
            &ctx,
            &catalogs,
            "SELECT id FROM ice.sales.tref VERSION AS OF 'via_testing' ORDER BY id"
        )
        .await,
        vec![1, 2, 3]
    );

    let path_ref =
        ref_ddl::try_parse_ref_ddl("ALTER TABLE ice.sales.tref CREATE BRANCH `..` AS OF VERSION 1")
            .expect("recognized")
            .expect_err("path-escape ref name");
    assert!(
        path_ref.to_string().contains("path") || path_ref.to_string().contains(".."),
        "got: {path_ref}"
    );
}

/// OR REPLACE column-def wipes prior rows.
#[tokio::test]
async fn column_def_or_replace_wipe_if_not_exists_and_like() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.repl AS SELECT * FROM src",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.repl").await,
        3
    );

    run(
        &ctx,
        &catalogs,
        "CREATE OR REPLACE TABLE ice.sales.repl (id BIGINT, name STRING) USING iceberg",
    )
    .await;
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.repl").await,
        0,
        "OR REPLACE schema-only must wipe prior data files/rows"
    );

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.keep_schema (id BIGINT) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE IF NOT EXISTS ice.sales.keep_schema (id INT, extra STRING) USING iceberg",
    )
    .await;
    let keep_schema = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "keep_schema".to_string(),
        ))
        .await
        .unwrap();
    let fields = keep_schema.metadata().current_schema().as_struct().fields();
    assert_eq!(fields.len(), 1, "IF NOT EXISTS must not replace schema");
    assert!(matches!(
        fields[0].field_type.as_ref(),
        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long)
    ));

    let like_err = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.like_t LIKE ice.sales.repl",
    )
    .await
    .expect_err("LIKE must refuse");
    let like_message = like_err.to_string();
    assert!(
        like_message.contains("LIKE") && like_message.contains("not supported"),
        "LIKE must surface NotImplemented class, got: {like_message}"
    );
    assert!(
        !like_message.contains("requires a column list"),
        "empty-column message must not mask LIKE: {like_message}"
    );
}

/// One-row CTAS type smoke.
#[tokio::test]
async fn ctas_of_instant_producers_stores_timestamptz() {
    use iceberg::spec::PrimitiveType;

    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    repark_functions::register_all(&ctx);

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ts_now USING iceberg AS SELECT current_timestamp() AS ts",
    )
    .await;
    let now_table = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "ts_now".to_string(),
        ))
        .await
        .unwrap();
    assert!(
        matches!(
            now_table.metadata().current_schema().as_struct().fields()[0]
                .field_type
                .as_ref(),
            iceberg::spec::Type::Primitive(PrimitiveType::Timestamptz)
        ),
        "SQL current_timestamp CTAS must be timestamptz, not timestamp_ns / timestamp"
    );

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ts_z USING iceberg AS \
         SELECT to_timestamp('2024-06-15T12:00:00Z') AS ts",
    )
    .await;
    let zoned = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "ts_z".to_string(),
        ))
        .await
        .unwrap();
    assert!(matches!(
        zoned.metadata().current_schema().as_struct().fields()[0]
            .field_type
            .as_ref(),
        iceberg::spec::Type::Primitive(PrimitiveType::Timestamptz)
    ));

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ts_part USING iceberg PARTITIONED BY (ts) AS \
         SELECT to_timestamp('2024-06-15T12:00:00Z') AS ts",
    )
    .await;
    let partitioned = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "ts_part".to_string(),
        ))
        .await
        .unwrap();
    assert!(
        matches!(
            partitioned.metadata().current_schema().as_struct().fields()[0]
                .field_type
                .as_ref(),
            iceberg::spec::Type::Primitive(PrimitiveType::Timestamptz)
        ),
        "identity-partition key type follows the column: timestamptz"
    );
}

/// pins: v3-2-create-v3-opt-in/C-002, C-005
#[tokio::test]
async fn column_def_create_format_version_three_needs_opt_in() {
    use iceberg::spec::FormatVersion;

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.v3c (id BIGINT) USING iceberg \
         TBLPROPERTIES ('format-version' = '3')",
    )
    .await;
    let v3 = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "v3c".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(v3.metadata().format_version(), FormatVersion::V3);
    assert!(!v3.metadata().properties().contains_key("format-version"));

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.still_v2 (id BIGINT) USING iceberg",
    )
    .await;
    let v2 = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "still_v2".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(
        v2.metadata().format_version(),
        FormatVersion::V2,
        "opt-in must not change the unspecified default"
    );
}

/// pins: v3-2-create-v3-opt-in/C-005, C-008
#[tokio::test]
async fn or_replace_applies_requested_v3_and_alter_still_refuses_with_opt_in() {
    use iceberg::spec::FormatVersion;

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.up (id BIGINT) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE OR REPLACE TABLE ice.sales.up (id BIGINT) USING iceberg \
         TBLPROPERTIES ('format-version' = '3')",
    )
    .await;
    let upgraded = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "up".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(upgraded.metadata().format_version(), FormatVersion::V3);
    assert!(
        !upgraded
            .metadata()
            .properties()
            .contains_key("format-version")
    );

    run(
        &ctx,
        &catalogs,
        "CREATE OR REPLACE TABLE ice.sales.up (id BIGINT) USING iceberg",
    )
    .await;
    let kept = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "up".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(
        kept.metadata().format_version(),
        FormatVersion::V3,
        "unspecified OR REPLACE must not force v2 onto an existing v3 table"
    );

    let alter = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.up SET TBLPROPERTIES ('format-version' = '3')",
    )
    .await
    .expect_err("ALTER format-version=3 must still refuse with the opt-in on");
    assert!(
        alter.to_string().contains("format-version") || alter.to_string().contains("reserved"),
        "ALTER must name the reserved key: {alter}"
    );
}

/// pins: v3r-1-rulings/C-008, C-009
/// V3R-1: `geometry` / `geography` DECLARED out (`V3-GEO-1`), `variant` stays V3-6; all refuse.
#[tokio::test]
async fn v3_type_columns_geometry_geography_variant_refuse_naming_the_type() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&wh).await;
    for type_name in ["GEOMETRY", "GEOGRAPHY", "VARIANT"] {
        let table = format!("t_{}", type_name.to_ascii_lowercase());
        let err = execute(
            &ctx,
            &catalogs,
            &format!(
                "CREATE TABLE ice.sales.{table} (id INT, v {type_name}) USING iceberg \
                 TBLPROPERTIES ('format-version' = '3')"
            ),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(
            err.to_ascii_uppercase().contains(type_name),
            "CREATE with a `{type_name}` column must refuse naming the type: {err}"
        );
        let exists = catalogs["ice"]
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                table.clone(),
            ))
            .await
            .unwrap();
        assert!(!exists, "a refused CREATE must leave no `{table}` behind");
    }
}
