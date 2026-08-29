/// Create a table via CTAS so the ALTER tests have a target to mutate.
use super::super::*;
use super::common::*;

async fn create_alter_target(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    execute(
        ctx,
        catalogs,
        &format!("CREATE TABLE ice.sales.{table} AS SELECT * FROM src"),
    )
    .await
    .unwrap();
}

async fn table_props(catalogs: &CatalogRegistry, table: &str) -> HashMap<String, String> {
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), table.to_string());
    catalogs["ice"]
        .load_table(&ident)
        .await
        .unwrap()
        .metadata()
        .properties()
        .clone()
}

/// `ALTER TABLE … SET TBLPROPERTIES (…)` routes through `execute` to the write path and lands the
/// properties in the table metadata.
#[tokio::test]
async fn alter_set_tblproperties() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_alter_target(&ctx, &catalogs, "t").await;

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t SET TBLPROPERTIES('owner' = 'example-team', 'pii' = 'false')",
    )
    .await
    .unwrap();

    let props = table_props(&catalogs, "t").await;
    assert_eq!(props.get("owner").map(String::as_str), Some("example-team"));
    assert_eq!(props.get("pii").map(String::as_str), Some("false"));
}

/// `ALTER TABLE … UNSET TBLPROPERTIES (…)` — exercises the token rewrite (sqlparser 0.59 cannot
/// parse `UNSET`) and removes only the named keys.
#[tokio::test]
async fn alter_unset_tblproperties() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_alter_target(&ctx, &catalogs, "t").await;

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t SET TBLPROPERTIES('owner' = 'example-team', 'pii' = 'false')",
    )
    .await
    .unwrap();
    // UNSET one of the two keys.
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t UNSET TBLPROPERTIES('owner', 'pii')",
    )
    .await
    .unwrap();

    let props = table_props(&catalogs, "t").await;
    assert!(!props.contains_key("owner"));
    assert!(!props.contains_key("pii"));
}

/// `ALTER TABLE … RENAME TO …` moves the table: the new ident loads, the old one is gone, and the
/// renamed table is queryable through the re-registered provider.
#[tokio::test]
async fn alter_rename_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_alter_target(&ctx, &catalogs, "orders").await;

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.orders RENAME TO ice.sales.orders_v2",
    )
    .await
    .unwrap();

    let old = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "orders".to_string(),
    );
    let new = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "orders_v2".to_string(),
    );
    assert!(!catalogs["ice"].table_exists(&old).await.unwrap());
    assert!(catalogs["ice"].table_exists(&new).await.unwrap());
    // Queryable under the new name via the re-registered provider.
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.orders_v2").await,
        3
    );
}

/// I6 READY — ADD COLUMN (with COMMENT + AFTER), RENAME COLUMN (field-id stable), DROP COLUMN;
/// schema-equality pin + read-after (added → NULL, rename keeps data).
#[tokio::test]
#[allow(clippy::too_many_lines)] // flat pin battery: schema + read-after + field-id + drop
async fn alter_add_rename_drop_column_schema_and_read_after() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_alter_target(&ctx, &catalogs, "ev").await;

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ev ADD COLUMN note STRING COMMENT 'free text' AFTER id",
    )
    .await
    .unwrap();

    // Schema pin: name + Arrow type via SELECT * (value AND type — never only show).
    let after_add = execute(&ctx, &catalogs, "SELECT * FROM ice.sales.ev")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let schema = after_add[0].schema();
    let names: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    // `src` is id + name; AFTER id puts note between id and name.
    assert_eq!(
        names,
        vec!["id".to_string(), "note".to_string(), "name".to_string()],
        "schema names: {names:?}"
    );
    assert_eq!(
        schema.field_with_name("note").unwrap().data_type(),
        &DataType::Utf8
    );
    // Added column reads as NULL for existing rows.
    let note_index = schema.index_of("note").unwrap();
    let id_index = schema.index_of("id").unwrap();
    for batch in &after_add {
        let note = batch
            .column(note_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..note.len() {
            assert!(
                note.is_null(row),
                "added column must be NULL on existing rows"
            );
        }
    }

    // RENAME keeps data under the new name (field-id preserved in Iceberg metadata).
    let ids_before: Vec<i32> = after_add
        .iter()
        .flat_map(|batch| {
            let col = batch
                .column(id_index)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
        })
        .collect();
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ev RENAME COLUMN id TO event_id",
    )
    .await
    .unwrap();
    let after_rename = execute(&ctx, &catalogs, "SELECT event_id FROM ice.sales.ev")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(after_rename[0].schema().field_with_name("event_id").is_ok());
    let ids_after: Vec<i32> = after_rename
        .iter()
        .flat_map(|batch| {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(ids_before, ids_after, "rename must keep column data");

    // Iceberg field-id stability on rename.
    let table = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".into()),
            "ev".into(),
        ))
        .await
        .unwrap();
    let fields = table.metadata().current_schema().as_struct().fields();
    let event_id = fields
        .iter()
        .find(|field| field.name == "event_id")
        .unwrap();
    // Original CTAS schema assigns id as field-id 1 (first column).
    assert_eq!(event_id.id, 1, "rename must preserve field-id");

    execute(&ctx, &catalogs, "ALTER TABLE ice.sales.ev DROP COLUMN note")
        .await
        .unwrap();
    let after_drop = execute(&ctx, &catalogs, "SELECT * FROM ice.sales.ev")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let drop_names: Vec<String> = after_drop[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(drop_names, vec!["event_id".to_string(), "name".to_string()]);
}

/// I6 READY — ADD COLUMNS plural (parenthesised) rewrites to multi ADD COLUMN.
#[tokio::test]
async fn alter_add_columns_plural_form() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_alter_target(&ctx, &catalogs, "plural").await;
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.plural ADD COLUMNS (a INT, b STRING)",
    )
    .await
    .unwrap();
    let batches = execute(&ctx, &catalogs, "SELECT * FROM ice.sales.plural")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let schema = batches[0].schema();
    let names: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert!(
        names.iter().any(|name| name == "a") && names.iter().any(|name| name == "b"),
        "got {names:?}"
    );
}

/// I6 READY — ADD COLUMN FIRST lands the column at the front.
#[tokio::test]
async fn alter_add_column_first() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_alter_target(&ctx, &catalogs, "first_t").await;
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.first_t ADD COLUMN lead BOOLEAN FIRST",
    )
    .await
    .unwrap();
    let batches = execute(&ctx, &catalogs, "SELECT * FROM ice.sales.first_t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches[0].schema().field(0).name(), "lead");
}

/// I6 stretch — TYPE widen int→long lands; narrow long→int refuses (twin pin).
#[tokio::test]
async fn alter_column_type_widen_and_narrow_refuse() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    // Column-def CREATE so `n` is INT (CTAS from src may widen).
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.widen (n INT, label STRING) USING iceberg",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.widen VALUES (1, 'a'), (2, 'b')",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.widen ALTER COLUMN n TYPE BIGINT",
    )
    .await
    .unwrap();
    let batches = execute(&ctx, &catalogs, "SELECT n FROM ice.sales.widen")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &DataType::Int64,
        "widen must land as Arrow int64"
    );
    // Values intact after widen.
    let values: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(values, vec![1, 2]);

    let error = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.widen ALTER COLUMN n TYPE INT",
    )
    .await
    .expect_err("narrow long→int must refuse");
    let message = error.to_string().to_lowercase();
    assert!(
        message.contains("cannot change column type")
            || message.contains("promote")
            || message.contains("cannot"),
        "narrow refusal must be loud, got: {error}"
    );
}

/// I6 stretch — DROP NOT NULL makes a required column optional.
#[tokio::test]
async fn alter_column_drop_not_null() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.req (id BIGINT NOT NULL, name STRING) USING iceberg",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.req ALTER COLUMN id DROP NOT NULL",
    )
    .await
    .unwrap();
    let table = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".into()),
            "req".into(),
        ))
        .await
        .unwrap();
    let id = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .find(|f| f.name == "id")
        .unwrap();
    assert!(!id.required, "DROP NOT NULL must make the column optional");
}

/// I6 residual + I7 identity-trap — ADD NOT NULL / SET NOT NULL refuse; REPLACE COLUMNS
/// identity trap (same-name incompatible type) refuses; WRITE ORDERED BY still loud.
#[tokio::test]
#[allow(clippy::too_many_lines)] // flat refuse battery: ORDERED/DISTRIBUTED/LHS/width=0 (octo C2)
async fn alter_unsupported_forms_refuse_loud() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_alter_target(&ctx, &catalogs, "loud").await;

    // I7 identity trap: table has `id INT` + `name STRING`; REPLACE with id STRING refuses.
    let replace_err = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.loud REPLACE COLUMNS (id STRING, name STRING)",
    )
    .await
    .expect_err("REPLACE COLUMNS identity trap must refuse");
    assert!(
        replace_err
            .to_string()
            .to_lowercase()
            .contains("identity trap")
            || replace_err.to_string().contains("REPLACE COLUMNS"),
        "got: {replace_err}"
    );

    let not_null_err = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.loud ADD COLUMN flag BOOLEAN NOT NULL",
    )
    .await
    .expect_err("ADD NOT NULL must refuse");
    assert!(
        not_null_err.to_string().contains("NOT NULL"),
        "got: {not_null_err}"
    );

    let set_nn_err = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.loud ALTER COLUMN id SET NOT NULL",
    )
    .await
    .expect_err("SET NOT NULL must refuse");
    assert!(
        set_nn_err.to_string().contains("SET NOT NULL")
            || set_nn_err.to_string().contains("not supported"),
        "got: {set_nn_err}"
    );

    let write_order_err = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.loud WRITE ORDERED BY id",
    )
    .await
    .expect_err("WRITE ORDERED BY must refuse");
    assert!(
        write_order_err
            .to_string()
            .to_lowercase()
            .contains("write ordered")
            || write_order_err
                .to_string()
                .to_lowercase()
                .contains("not supported"),
        "got: {write_order_err}"
    );

    // WRITE DISTRIBUTED BY uses the same residual refusal path as ORDERED.
    let write_dist_err = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.loud WRITE DISTRIBUTED BY PARTITION",
    )
    .await
    .expect_err("WRITE DISTRIBUTED BY must refuse");
    assert!(
        write_dist_err
            .to_string()
            .to_lowercase()
            .contains("write distributed")
            || write_dist_err
                .to_string()
                .to_lowercase()
                .contains("not supported"),
        "got: {write_dist_err}"
    );

    // REPLACE PARTITION FIELD rejects a transform expression on the left side.
    let lhs_err = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.loud REPLACE PARTITION FIELD bucket(8, id) WITH bucket(16, id)",
    )
    .await
    .expect_err("REPLACE PF transform LHS must refuse loud");
    assert!(
        lhs_err.to_string().to_lowercase().contains("not supported")
            || lhs_err.to_string().to_lowercase().contains("left-hand")
            || lhs_err.to_string().to_lowercase().contains("transform"),
        "got: {lhs_err}"
    );

    // Invalid bucket and truncate widths refuse on the ALTER path.
    let bucket_zero = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.loud ADD PARTITION FIELD bucket(0, id)",
    )
    .await
    .expect_err("bucket(0) must refuse");
    assert!(
        bucket_zero.to_string().contains("> 0")
            || bucket_zero
                .to_string()
                .to_lowercase()
                .contains("numbuckets")
            || bucket_zero.to_string().to_lowercase().contains("must be"),
        "got: {bucket_zero}"
    );
    let trunc_zero = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.loud ADD PARTITION FIELD truncate(0, name)",
    )
    .await
    .expect_err("truncate(0) must refuse");
    assert!(
        trunc_zero.to_string().contains("> 0")
            || trunc_zero.to_string().to_lowercase().contains("width")
            || trunc_zero.to_string().to_lowercase().contains("must be"),
        "got: {trunc_zero}"
    );
}

/// I7 READY — ADD/DROP PARTITION FIELD; write-after-evolution pins (new writes NEW spec;
/// old files keep prior spec-id; mixed-spec read correct).
#[tokio::test]
async fn alter_add_drop_partition_field_and_write_after_evolution() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pevo (id INT, category STRING) USING iceberg",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.pevo VALUES (1, 'a'), (2, 'b')",
    )
    .await
    .unwrap();

    let table = load_sales_table(&catalogs, "pevo").await;
    let pre_snap = table
        .metadata()
        .current_snapshot()
        .expect("seed insert must create a snapshot")
        .snapshot_id();
    let pre_default_spec = table.metadata().default_partition_spec_id();

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.pevo ADD PARTITION FIELD category",
    )
    .await
    .unwrap();
    let table = load_sales_table(&catalogs, "pevo").await;
    let after_add_spec = table.metadata().default_partition_spec_id();
    let field_names: Vec<_> = table
        .metadata()
        .default_partition_spec()
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect();
    assert_ne!(after_add_spec, pre_default_spec);
    assert_eq!(field_names, vec!["category".to_string()]);

    let specs = live_data_file_spec_ids(&catalogs, "pevo").await;
    assert!(
        specs.iter().all(|spec_id| *spec_id == pre_default_spec),
        "pre-evolution files keep old spec-id {pre_default_spec}, got {specs:?}"
    );

    execute(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.pevo VALUES (3, 'c')",
    )
    .await
    .unwrap();
    let specs = live_data_file_spec_ids(&catalogs, "pevo").await;
    assert!(specs.contains(&pre_default_spec) && specs.contains(&after_add_spec));
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM ice.sales.pevo").await,
        3
    );
    assert_eq!(
        rows(
            &ctx,
            &catalogs,
            &format!("SELECT * FROM ice.sales.pevo VERSION AS OF {pre_snap}"),
        )
        .await,
        2
    );

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.pevo DROP PARTITION FIELD category",
    )
    .await
    .unwrap();
    let table = load_sales_table(&catalogs, "pevo").await;
    assert!(
        table.metadata().default_partition_spec().is_unpartitioned()
            || table
                .metadata()
                .default_partition_spec()
                .fields()
                .is_empty()
    );
}

/// I7 stretch — REPLACE PARTITION FIELD; bucket transform + AS name; unsupported transform.
#[tokio::test]
async fn alter_replace_partition_field_and_transforms() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.prepl (id INT, label STRING) USING iceberg",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.prepl ADD PARTITION FIELD bucket(8, id) AS id_b8",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.prepl REPLACE PARTITION FIELD id_b8 WITH bucket(16, id) AS id_b16",
    )
    .await
    .unwrap();
    let names = {
        let handle = catalog_handle(&catalogs, "ice").unwrap();
        let table = handle
            .load_table(&TableIdent::new(
                NamespaceIdent::new("sales".into()),
                "prepl".into(),
            ))
            .await
            .unwrap();
        table
            .metadata()
            .default_partition_spec()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(names, vec!["id_b16".to_string()]);

    let bad = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.prepl ADD PARTITION FIELD unknown_xform(id)",
    )
    .await
    .expect_err("unknown transform must refuse");
    assert!(
        bad.to_string().to_lowercase().contains("not a supported")
            || bad.to_string().to_lowercase().contains("not supported")
            || bad.to_string().to_lowercase().contains("unknown"),
        "got: {bad}"
    );
}

/// I7 stretch — REPLACE COLUMNS happy path (drop unused + promote int→long) + identity-trap twin.
#[tokio::test]
async fn alter_replace_columns_promote_and_identity_trap() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.rcols (id INT, name STRING, junk INT) USING iceberg",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.rcols VALUES (1, 'a', 9), (2, 'b', 8)",
    )
    .await
    .unwrap();

    // Happy: drop junk, promote id INT→BIGINT, keep name.
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rcols REPLACE COLUMNS (id BIGINT, name STRING)",
    )
    .await
    .unwrap();
    let names = {
        let batches = execute(&ctx, &catalogs, "SELECT * FROM ice.sales.rcols")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        batches[0]
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(names, vec!["id".to_string(), "name".to_string()]);
    // Read-after: data intact under promoted type.
    let count = rows(&ctx, &catalogs, "SELECT * FROM ice.sales.rcols").await;
    assert_eq!(count, 2);

    // Identity-trap twin: same name, incompatible type (BIGINT → STRING).
    let trap = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rcols REPLACE COLUMNS (id STRING, name STRING)",
    )
    .await
    .expect_err("identity trap must refuse");
    assert!(
        trap.to_string().to_lowercase().contains("identity trap"),
        "got: {trap}"
    );

    // Field-id stability on promote (identity trap exists so field-ids are not recycled
    // under an incompatible type; the happy path must keep the id field-id).
    let table = load_sales_table(&catalogs, "rcols").await;
    let id_field = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .find(|field| field.name == "id")
        .expect("id column");
    // CREATE TABLE column-def assigns sequential ids starting at 1 for `id`.
    assert_eq!(
        id_field.id, 1,
        "REPLACE COLUMNS promote int→long must preserve field-id"
    );
    assert!(
        matches!(
            id_field.field_type.as_ref(),
            iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long)
        ),
        "id must be long after promote, got {:?}",
        id_field.field_type
    );
}

/// Cover truncate and temporal partition fields, transform drops, required-column refusal, and
/// case-insensitive column drops.
#[tokio::test]
#[allow(clippy::too_many_lines)] // flat pin battery: truncate/year/drop-by-transform/required
async fn alter_partition_transforms_drop_by_transform_and_replace_required_refuse() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    // truncate[W]
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ptrunc (id INT, label STRING) USING iceberg",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ptrunc ADD PARTITION FIELD truncate(2, label) AS lab_t2",
    )
    .await
    .unwrap();
    let names = {
        let table = load_sales_table(&catalogs, "ptrunc").await;
        table
            .metadata()
            .default_partition_spec()
            .fields()
            .iter()
            .map(|field| (field.name.clone(), format!("{}", field.transform)))
            .collect::<Vec<_>>()
    };
    assert_eq!(names.len(), 1);
    assert_eq!(names[0].0, "lab_t2");
    assert!(
        names[0].1.contains("trunc") || names[0].1.contains('2'),
        "expected truncate transform, got {}",
        names[0].1
    );

    // DROP by transform form (not bare name).
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ptrunc DROP PARTITION FIELD truncate(2, label)",
    )
    .await
    .unwrap();
    let table = load_sales_table(&catalogs, "ptrunc").await;
    assert!(
        table.metadata().default_partition_spec().is_unpartitioned()
            || table
                .metadata()
                .default_partition_spec()
                .fields()
                .is_empty()
    );

    // year(ts) temporal
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pyear (id INT, ts TIMESTAMP) USING iceberg",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.pyear ADD PARTITION FIELD year(ts)",
    )
    .await
    .unwrap();
    let year_fields = {
        let table = load_sales_table(&catalogs, "pyear").await;
        table
            .metadata()
            .default_partition_spec()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>()
    };
    assert!(
        year_fields
            .iter()
            .any(|name| name.contains("year") || name == "ts_year"),
        "year partition field auto-name expected, got {year_fields:?}"
    );

    // Case-insensitive DROP of partition field name via SQL.
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pcase (id INT, region STRING) USING iceberg",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.pcase ADD PARTITION FIELD region AS reg",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.pcase DROP PARTITION FIELD REG",
    )
    .await
    .expect("DROP PARTITION FIELD name must be case-insensitive at SQL");

    // REPLACE COLUMNS required-new refuse twin.
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.rreq (id INT) USING iceberg",
    )
    .await
    .unwrap();
    let required_err = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rreq REPLACE COLUMNS (id INT, flag BOOLEAN NOT NULL)",
    )
    .await
    .expect_err("REPLACE COLUMNS ADD required must refuse");
    assert!(
        required_err.to_string().to_lowercase().contains("required")
            || required_err.to_string().to_lowercase().contains("not null"),
        "got: {required_err}"
    );

    // Identity transform form and optional-to-required refusal.
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.pid (id INT, k STRING) USING iceberg",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.pid ADD PARTITION FIELD identity(k) AS k_id",
    )
    .await
    .unwrap();
    let id_names = {
        let table = load_sales_table(&catalogs, "pid").await;
        table
            .metadata()
            .default_partition_spec()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(id_names, vec!["k_id".to_string()]);

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.ropt (id INT) USING iceberg",
    )
    .await
    .unwrap();
    let opt_req = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ropt REPLACE COLUMNS (id INT NOT NULL)",
    )
    .await
    .expect_err("optional→required via REPLACE COLUMNS must refuse");
    assert!(
        opt_req.to_string().to_lowercase().contains("not null")
            || opt_req.to_string().to_lowercase().contains("required"),
        "got: {opt_req}"
    );
}

/// REPLACE COLUMNS widens float and decimal types and rejects unsafe identity changes.
#[tokio::test]
async fn alter_replace_columns_float_decimal_promote_and_traps() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.rfd (measure FLOAT, amount DECIMAL(5,2), junk INT) USING iceberg",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.rfd VALUES (1.5, 12.34, 9)",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rfd REPLACE COLUMNS (measure DOUBLE, amount DECIMAL(10,2))",
    )
    .await
    .unwrap();
    let table = load_sales_table(&catalogs, "rfd").await;
    let fields = table.metadata().current_schema().as_struct().fields();
    let measure = fields.iter().find(|f| f.name == "measure").unwrap();
    let amount = fields.iter().find(|f| f.name == "amount").unwrap();
    assert!(
        matches!(
            measure.field_type.as_ref(),
            iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Double)
        ),
        "float→double via REPLACE COLUMNS, got {:?}",
        measure.field_type
    );
    match amount.field_type.as_ref() {
        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Decimal {
            precision,
            scale,
        }) => {
            assert_eq!((*precision, *scale), (10, 2));
        }
        other => panic!("expected decimal(10,2), got {other:?}"),
    }
    assert!(
        fields.iter().all(|f| f.name != "junk"),
        "junk must be dropped by REPLACE COLUMNS"
    );
    // Read-after value integrity (Arrow path).
    let batches = execute(&ctx, &catalogs, "SELECT measure, amount FROM ice.sales.rfd")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(!batches.is_empty());
    assert_eq!(batches[0].schema().field(0).data_type(), &DataType::Float64);

    // Identity-trap twins on REPLACE COLUMNS (double→string, decimal→int).
    let trap_double = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rfd REPLACE COLUMNS (measure STRING, amount DECIMAL(10,2))",
    )
    .await
    .expect_err("double→string identity trap");
    assert!(
        trap_double
            .to_string()
            .to_lowercase()
            .contains("identity trap"),
        "got: {trap_double}"
    );
    let trap_dec = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rfd REPLACE COLUMNS (measure DOUBLE, amount INT)",
    )
    .await
    .expect_err("decimal→int identity trap");
    assert!(
        trap_dec
            .to_string()
            .to_lowercase()
            .contains("identity trap"),
        "got: {trap_dec}"
    );
}

/// ALTER COLUMN TYPE widens float and decimal types and refuses narrowing.
#[tokio::test]
async fn alter_column_type_float_double_and_decimal_widen_twins() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.fd (measure FLOAT, amount DECIMAL(5,2)) USING iceberg",
    )
    .await
    .unwrap();
    // Seed a row so SELECT * returns a batch (empty tables can yield zero batches).
    execute(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.fd VALUES (1.5, 12.34)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.fd ALTER COLUMN measure TYPE DOUBLE",
    )
    .await
    .unwrap();
    let measure = execute(&ctx, &catalogs, "SELECT measure FROM ice.sales.fd")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(
        !measure.is_empty(),
        "read-after widen must yield at least one batch"
    );
    assert_eq!(
        measure[0].schema().field(0).data_type(),
        &DataType::Float64,
        "float→double must land as Arrow float64"
    );
    let narrow_float = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.fd ALTER COLUMN measure TYPE FLOAT",
    )
    .await
    .expect_err("double→float must refuse");
    assert!(
        narrow_float.to_string().to_lowercase().contains("cannot")
            || narrow_float.to_string().to_lowercase().contains("promote"),
        "got: {narrow_float}"
    );

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.fd ALTER COLUMN amount TYPE DECIMAL(10,2)",
    )
    .await
    .unwrap();
    let amount = execute(&ctx, &catalogs, "SELECT amount FROM ice.sales.fd")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    match amount[0].schema().field(0).data_type() {
        DataType::Decimal128(precision, scale) => {
            assert_eq!((*precision, *scale), (10, 2));
        }
        other => panic!("expected decimal128(10,2), got {other:?}"),
    }
    let narrow_decimal = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.fd ALTER COLUMN amount TYPE DECIMAL(5,2)",
    )
    .await
    .expect_err("decimal precision narrow must refuse");
    assert!(
        narrow_decimal.to_string().to_lowercase().contains("cannot")
            || narrow_decimal
                .to_string()
                .to_lowercase()
                .contains("promote")
            || narrow_decimal
                .to_string()
                .to_lowercase()
                .contains("decimal"),
        "got: {narrow_decimal}"
    );
}

/// Column rename and drop resolve names case-insensitively.
#[tokio::test]
async fn alter_column_case_insensitive_rename_and_drop() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_alter_target(&ctx, &catalogs, "cased").await;

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.cased RENAME COLUMN ID TO event_id",
    )
    .await
    .unwrap();
    let batches = execute(&ctx, &catalogs, "SELECT event_id FROM ice.sales.cased")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(batches[0].schema().field_with_name("event_id").is_ok());

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.cased ADD COLUMN note STRING",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.cased DROP COLUMN NOTE",
    )
    .await
    .unwrap();
    let after = execute(&ctx, &catalogs, "SELECT * FROM ice.sales.cased")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let names: Vec<String> = after[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert!(
        !names.iter().any(|name| name.eq_ignore_ascii_case("note")),
        "DROP COLUMN NOTE must remove note, got {names:?}"
    );
}

/// COMMENT, MOVE FIRST/AFTER, and missing AFTER siblings refuse loudly.
#[tokio::test]
async fn alter_unsupported_comment_move_and_after_missing_refuse() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_alter_target(&ctx, &catalogs, "refuse2").await;

    let comment_err = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.refuse2 ALTER COLUMN id COMMENT 'docs'",
    )
    .await
    .expect_err("ALTER COLUMN COMMENT must refuse loud");
    assert!(
        comment_err.to_string().to_uppercase().contains("COMMENT")
            || comment_err
                .to_string()
                .to_lowercase()
                .contains("not supported"),
        "got: {comment_err}"
    );

    let move_err = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.refuse2 ALTER COLUMN id FIRST",
    )
    .await
    .expect_err("ALTER COLUMN MOVE must refuse loud");
    assert!(
        move_err
            .to_string()
            .to_lowercase()
            .contains("not supported")
            || move_err.to_string().to_uppercase().contains("FIRST")
            || move_err.to_string().to_lowercase().contains("move"),
        "got: {move_err}"
    );

    let after_err = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.refuse2 ADD COLUMN ghost STRING AFTER no_such_col",
    )
    .await
    .expect_err("AFTER missing sibling must refuse");
    assert!(
        after_err.to_string().to_lowercase().contains("no_such_col")
            || after_err.to_string().to_lowercase().contains("missing")
            || after_err.to_string().to_lowercase().contains("cannot")
            || after_err.to_string().to_lowercase().contains("not found"),
        "got: {after_err}"
    );
}

/// Bare plural DROP COLUMNS updates the schema and remains readable.
#[tokio::test]
async fn alter_drop_columns_bare_plural_form() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_alter_target(&ctx, &catalogs, "drop_bare").await;
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.drop_bare ADD COLUMNS (a INT, b STRING)",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.drop_bare DROP COLUMNS a, b",
    )
    .await
    .unwrap();
    let batches = execute(&ctx, &catalogs, "SELECT * FROM ice.sales.drop_bare")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let names: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert!(
        !names.iter().any(|name| name == "a" || name == "b"),
        "bare DROP COLUMNS must remove both, got {names:?}"
    );
    assert!(names.iter().any(|name| name == "id"));
}

/// ADD COLUMN IF NOT EXISTS skips existing columns; AFTER sibling is
/// case-insensitive; multi-op SET TBLPROPERTIES after RENAME TO targets new ident.
#[tokio::test]
async fn alter_if_not_exists_after_case_and_rename_then_set_props() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_alter_target(&ctx, &catalogs, "c8").await;

    // IF NOT EXISTS: first add lands; second soft-skips (no error).
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.c8 ADD COLUMN IF NOT EXISTS note STRING",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.c8 ADD COLUMN IF NOT EXISTS note STRING",
    )
    .await
    .unwrap();
    let after_if = execute(&ctx, &catalogs, "SELECT * FROM ice.sales.c8")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let note_count = after_if[0]
        .schema()
        .fields()
        .iter()
        .filter(|field| field.name() == "note")
        .count();
    assert_eq!(note_count, 1, "IF NOT EXISTS must not duplicate column");

    // AFTER with different-case sibling reference.
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.c8 ADD COLUMN tag STRING AFTER ID",
    )
    .await
    .unwrap();
    let after_pos = execute(&ctx, &catalogs, "SELECT * FROM ice.sales.c8")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let names: Vec<String> = after_pos[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let id_index = names.iter().position(|name| name == "id").unwrap();
    let tag_index = names.iter().position(|name| name == "tag").unwrap();
    assert_eq!(
        tag_index,
        id_index + 1,
        "AFTER ID (case-insensitive) must place tag after id, got {names:?}"
    );

    // RENAME TO then SET props in one statement — C2 ident update must cover props too.
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.c8 RENAME TO ice.sales.c8_v2, \
             SET TBLPROPERTIES('owner'='octo')",
    )
    .await
    .unwrap();
    let props = table_props(&catalogs, "c8_v2").await;
    assert_eq!(props.get("owner").map(String::as_str), Some("octo"));
    let old = TableIdent::new(NamespaceIdent::new("sales".into()), "c8".into());
    assert!(!catalogs["ice"].table_exists(&old).await.unwrap());
}

/// A multi-operation rename then add applies the ADD against the new identifier atomically.
#[tokio::test]
async fn alter_rename_table_then_add_column_same_statement() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_alter_target(&ctx, &catalogs, "ren_add").await;

    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.ren_add RENAME TO ice.sales.ren_add_v2, ADD COLUMN extra STRING",
    )
    .await
    .unwrap();

    let old = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "ren_add".to_string(),
    );
    let new = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "ren_add_v2".to_string(),
    );
    assert!(!catalogs["ice"].table_exists(&old).await.unwrap());
    assert!(catalogs["ice"].table_exists(&new).await.unwrap());

    let batches = execute(&ctx, &catalogs, "SELECT * FROM ice.sales.ren_add_v2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let names: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert!(
        names.iter().any(|name| name == "extra"),
        "ADD after RENAME TO must land on the renamed table, got {names:?}"
    );
}

/// O2-C4-L-002: ALTER TABLE SET TBLPROPERTIES must not be false-positived as BRANCH/TAG sniff.
#[tokio::test]
async fn alter_set_tblproperties_not_misclassified_as_branch_ddl() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.t SET TBLPROPERTIES ('x' = 'y')",
    )
    .await;
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 3);
}
