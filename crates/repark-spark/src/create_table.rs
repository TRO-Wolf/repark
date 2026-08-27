//! Column-def `CREATE TABLE … (cols) USING iceberg [PARTITIONED BY …] [TBLPROPERTIES …]`.
//!
//! Schema-only create via the **same staged path as CTAS** (`StagedTableTransaction::begin_create`
//! / `begin_replace` → commit with **no data files**). No SELECT is planned or executed.
//!
//! CTAS with an explicit column list stays OUT — that reject lives in [`crate::build_ctas`]
//! (Group Q pins). This module handles non-CTAS `Statement::CreateTable` only.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    ColumnDef, ColumnOption, CreateTable, CreateTableOptions, DataType as SqlDataType,
    ExactNumberInfo, SqlOption, TimezoneInfo,
};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type, UnboundPartitionSpec};
use iceberg::transaction::StagedTableTransaction;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};

use repark_core::{CatalogRegistry, LocationPolicy};
use repark_functions::timestamp_type::{SparkTimestampType, spark_timestamp_type_from_options};

use crate::{
    CreatePlan, PartitionFieldSpec, PartitionedByElement, build_partition_spec,
    build_transform_field, catalog_handle, iceberg_err, name_parts, namespace_schema_name,
    property_value, reject_path_escape_ident, reregister, resolve_create_plan_for,
};

/// A resolved column-def `CREATE TABLE` (no `AS SELECT`).
struct SchemaCreate {
    catalog: String,
    namespace: NamespaceIdent,
    table: String,
    full_name: String,
    if_not_exists: bool,
    or_replace: bool,
    properties: HashMap<String, String>,
    partition_fields: Vec<PartitionFieldSpec>,
    schema: Schema,
    /// Requested `TBLPROPERTIES ('format-version' = …)`, consumed at execute (session opt-in).
    format_version: Option<String>,
}

/// ===========================================================================================
/// Execute `CREATE TABLE catalog.namespace.table (cols) [USING iceberg] [PARTITIONED BY …]
/// [TBLPROPERTIES …]` as a schema-only staged create (or replace).
/// ===========================================================================================
///
/// # Errors
/// Name resolution, type mapping, partition resolution, location, or catalog publish failures.
pub(crate) async fn execute_create_table(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    create: &CreateTable,
    partitioning: &[PartitionedByElement],
) -> Result<DataFrame> {
    let timestamp_type = spark_timestamp_type_from_options(ctx.copied_config().options());
    let schema_create = build_schema_create(create, partitioning, timestamp_type)?;
    execute_schema_create(ctx, catalogs, schema_create).await
}

#[allow(clippy::too_many_lines)] // one clause-by-clause walk of the CREATE TABLE AST — splitting would scatter the refuse rules
/// Extract a [`SchemaCreate`] from a non-CTAS `CREATE TABLE` AST + token-extracted partitioning.
fn build_schema_create(
    create: &CreateTable,
    partitioning: &[PartitionedByElement],
    timestamp_type: SparkTimestampType,
) -> Result<SchemaCreate> {
    if create.query.is_some() {
        return Err(DataFusionError::Internal(
            "build_schema_create requires a non-CTAS CREATE TABLE".into(),
        ));
    }
    // TEMPORARY / EXTERNAL / TRANSIENT / VOLATILE must not silently create durable Iceberg
    // tables (I5 octo C3-F1).
    if create.temporary {
        return Err(DataFusionError::NotImplemented(
            "CREATE TEMPORARY TABLE is not supported for Iceberg tables yet — omit TEMPORARY \
             for a durable catalog table, or use a temp view (CREATE TEMP VIEW)"
                .into(),
        ));
    }
    if create.external {
        return Err(DataFusionError::NotImplemented(
            "CREATE EXTERNAL TABLE is not supported for Iceberg column-def CREATE yet".into(),
        ));
    }
    if create.transient || create.volatile {
        return Err(DataFusionError::NotImplemented(
            "CREATE TRANSIENT/VOLATILE TABLE is not supported for Iceberg column-def CREATE yet"
                .into(),
        ));
    }
    // LOCATION / Hive ROW FORMAT etc. must not be silently dropped (I5 octo C4-F1 / C5-F2).
    crate::refuse_unsupported_create_table_clauses(create, "column-def CREATE")?;
    // LIKE / CLONE before empty-column check so the honest NotImplemented surfaces
    // (I5 octo C2-F2 — empty-column message must not mask LIKE/CLONE).
    if create.like.is_some() || create.clone.is_some() {
        return Err(DataFusionError::NotImplemented(
            "CREATE TABLE … LIKE / CLONE is not supported yet".into(),
        ));
    }
    if !create.constraints.is_empty() {
        return Err(DataFusionError::NotImplemented(
            "CREATE TABLE table constraints (PRIMARY KEY / UNIQUE / …) are not supported yet"
                .into(),
        ));
    }
    if create.columns.is_empty() {
        return Err(DataFusionError::Plan(
            "CREATE TABLE without AS SELECT requires a column list \
             (e.g. CREATE TABLE c.ns.t (id BIGINT, name STRING) USING iceberg)"
                .into(),
        ));
    }

    let mut partition_fields = Vec::with_capacity(partitioning.len());
    for element in partitioning {
        match element {
            PartitionedByElement::Identity(column) => {
                partition_fields.push(PartitionFieldSpec::Identity(column.clone()));
            }
            PartitionedByElement::Transform { name, args } => {
                partition_fields.push(build_transform_field(name, args)?);
            }
            PartitionedByElement::Typed(column) => {
                return Err(DataFusionError::Plan(format!(
                    "PARTITIONED BY typed column `{column}` is not supported for Iceberg CREATE \
                     TABLE — reference a table column (or a transform over one), e.g. \
                     PARTITIONED BY ({column})"
                )));
            }
            PartitionedByElement::Nested(path) => {
                return Err(DataFusionError::NotImplemented(format!(
                    "CREATE TABLE PARTITIONED BY nested-field reference `{path}` is not \
                     supported yet — partition by a top-level column (v1)"
                )));
            }
        }
    }

    let parts = name_parts(&create.name);
    let [catalog, namespace, table] = parts.as_slice() else {
        return Err(DataFusionError::Plan(format!(
            "CREATE TABLE target must be a three-part `catalog.namespace.table` name, got `{}`",
            create.name
        )));
    };

    let mut properties = HashMap::new();
    if let CreateTableOptions::TableProperties(options) = &create.table_options {
        for option in options {
            if let SqlOption::KeyValue { key, value } = option {
                properties.insert(key.value.clone(), property_value(value));
            }
        }
    }
    // Reserved Iceberg key — consumed here, applied as `TableCreation.format_version` at execute
    // (V3-2: v3 needs `repark.sql.allowCreateFormatVersion3`).
    let format_version = properties.remove("format-version");

    let schema = schema_from_column_defs(&create.columns, timestamp_type)?;
    let partition_spec = build_partition_spec(&schema, &partition_fields)?;
    // Bind partition validation early (unknown column fails before catalog I/O).
    let _ = partition_spec;

    Ok(SchemaCreate {
        catalog: catalog.clone(),
        namespace: NamespaceIdent::new(namespace.clone()),
        table: table.clone(),
        full_name: create.name.to_string(),
        if_not_exists: create.if_not_exists,
        or_replace: create.or_replace,
        properties,
        partition_fields,
        schema,
        format_version,
    })
}

/// Map sqlparser column defs → Iceberg [`Schema`] (1-based field ids, Spark nullable default).
fn schema_from_column_defs(
    columns: &[ColumnDef],
    timestamp_type: SparkTimestampType,
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        let field_id = i32::try_from(index + 1).map_err(|_| {
            DataFusionError::Plan("CREATE TABLE exceeds Iceberg field-id range".into())
        })?;
        let iceberg_type =
            sql_type_to_iceberg_with_timestamp_type(&column.data_type, timestamp_type)?;
        // Only NULL / NOT NULL are handled. DEFAULT / UNIQUE / CHECK / COMMENT / … must not be
        // silently dropped (I5 octo C1-F2 — same fail-open class as historical CTAS column lists).
        let mut required = false;
        for option in &column.options {
            match &option.option {
                ColumnOption::NotNull => required = true,
                ColumnOption::Null => {}
                other => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "CREATE TABLE column option `{other}` on `{}` is not supported yet — \
                         only NULL / NOT NULL are accepted (defaults, constraints, generated \
                         columns stay out of I5 schema-only CREATE)",
                        column.name.value
                    )));
                }
            }
        }
        let nested = if required {
            NestedField::required(field_id, column.name.value.clone(), iceberg_type)
        } else {
            NestedField::optional(field_id, column.name.value.clone(), iceberg_type)
        };
        fields.push(Arc::new(nested));
    }
    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(iceberg_err)
}

/// Map a Spark/SQL column type to an Iceberg primitive (loud on nested / unsupported).
///
/// Shared with `ALTER TABLE ADD/ALTER COLUMN` (I6) so CREATE and ALTER cannot drift.
/// Bare `TIMESTAMP` (`TimezoneInfo::None`) follows [`SparkTimestampType`] — default
/// LTZ → `timestamptz`. The no-arg wrapper keeps existing call sites on that default.
pub(crate) fn sql_type_to_iceberg(data_type: &SqlDataType) -> Result<Type> {
    sql_type_to_iceberg_with_timestamp_type(data_type, SparkTimestampType::Ltz)
}

/// Same mapping as [`sql_type_to_iceberg`], with the session default for bare `TIMESTAMP`.
pub(crate) fn sql_type_to_iceberg_with_timestamp_type(
    data_type: &SqlDataType,
    timestamp_type: SparkTimestampType,
) -> Result<Type> {
    let primitive = match data_type {
        SqlDataType::Boolean | SqlDataType::Bool => PrimitiveType::Boolean,
        SqlDataType::TinyInt(_)
        | SqlDataType::SmallInt(_)
        | SqlDataType::Int2(_)
        | SqlDataType::Int(_)
        | SqlDataType::Int4(_)
        | SqlDataType::Integer(_) => PrimitiveType::Int,
        SqlDataType::BigInt(_) | SqlDataType::Int8(_) => PrimitiveType::Long,
        SqlDataType::Float(_) | SqlDataType::Float4 | SqlDataType::Float32 | SqlDataType::Real => {
            PrimitiveType::Float
        }
        SqlDataType::Double(_)
        | SqlDataType::DoublePrecision
        | SqlDataType::Float8
        | SqlDataType::Float64 => PrimitiveType::Double,
        SqlDataType::Decimal(info) | SqlDataType::Numeric(info) | SqlDataType::Dec(info) => {
            decimal_from_info(info)?
        }
        // Spark STRING / VARCHAR / CHAR / TEXT → Iceberg string.
        SqlDataType::String(_)
        | SqlDataType::Text
        | SqlDataType::TinyText
        | SqlDataType::MediumText
        | SqlDataType::LongText
        | SqlDataType::Varchar(_)
        | SqlDataType::Nvarchar(_)
        | SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::CharacterVarying(_)
        | SqlDataType::CharVarying(_) => PrimitiveType::String,
        SqlDataType::Date => PrimitiveType::Date,
        // TIMESTAMP_NTZ / TIMESTAMP WITHOUT TIME ZONE stay naive Iceberg timestamp,
        // independent of the session default.
        SqlDataType::TimestampNtz(_) | SqlDataType::Timestamp(_, TimezoneInfo::WithoutTimeZone) => {
            PrimitiveType::Timestamp
        }
        // Bare TIMESTAMP follows spark.sql.timestampType. Default LTZ → timestamptz
        // (live Spark 4.1.2 + iceberg-spark-runtime CREATE probe, Z-2 A7).
        SqlDataType::Timestamp(_, TimezoneInfo::None) => match timestamp_type {
            SparkTimestampType::Ltz => PrimitiveType::Timestamptz,
            SparkTimestampType::Ntz => PrimitiveType::Timestamp,
        },
        // WITH TIME ZONE / TIMESTAMPTZ stay instants.
        SqlDataType::Timestamp(_, _) => PrimitiveType::Timestamptz,
        SqlDataType::Binary(_) | SqlDataType::Varbinary(_) => PrimitiveType::Binary,
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "column type `{other}` is not supported yet for Iceberg tables"
            )));
        }
    };
    Ok(Type::Primitive(primitive))
}

fn decimal_from_info(info: &ExactNumberInfo) -> Result<PrimitiveType> {
    let (precision, scale) = match info {
        ExactNumberInfo::None => (38_u32, 18_u32),
        ExactNumberInfo::Precision(precision) => {
            let precision = u32::try_from(*precision).map_err(|_| {
                DataFusionError::Plan(format!("DECIMAL precision {precision} out of range"))
            })?;
            (precision, 0_u32)
        }
        ExactNumberInfo::PrecisionAndScale(precision, scale) => {
            let precision = u32::try_from(*precision).map_err(|_| {
                DataFusionError::Plan(format!("DECIMAL precision {precision} out of range"))
            })?;
            if *scale < 0 {
                return Err(DataFusionError::Plan(format!(
                    "DECIMAL scale {scale} must be non-negative"
                )));
            }
            let scale = u32::try_from(*scale).map_err(|_| {
                DataFusionError::Plan(format!("DECIMAL scale {scale} out of range"))
            })?;
            (precision, scale)
        }
    };
    if precision == 0 || precision > 38 {
        return Err(DataFusionError::Plan(format!(
            "DECIMAL precision {precision} must be in 1..=38"
        )));
    }
    if scale > precision {
        return Err(DataFusionError::Plan(format!(
            "DECIMAL scale {scale} cannot exceed precision {precision}"
        )));
    }
    Ok(PrimitiveType::Decimal { precision, scale })
}

async fn execute_schema_create(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    create: SchemaCreate,
) -> Result<DataFrame> {
    let catalog = catalog_handle(catalogs, &create.catalog)?;
    let table_ident = TableIdent::new(create.namespace.clone(), create.table.clone());

    let existed = catalog
        .table_exists(&table_ident)
        .await
        .map_err(iceberg_err)?;
    if existed {
        if create.if_not_exists {
            return ctx.read_empty();
        } else if !create.or_replace {
            return Err(DataFusionError::Plan(format!(
                "table `{}` already exists",
                create.full_name
            )));
        }
    }

    let partition_spec = build_partition_spec(&create.schema, &create.partition_fields)?;
    let format_version = iceberg_create_format_version(ctx, create.format_version.as_deref())?;
    if existed {
        // OR REPLACE: stage against the existing table (same path as CTAS replace).
        // The fork upgrades format version from the reserved property, not
        // `TableCreation.format_version` — stamp only on replace (create rejects reserved keys).
        let existing = catalog
            .load_table(&table_ident)
            .await
            .map_err(iceberg_err)?;
        let mut properties = create.properties.clone();
        stamp_requested_format_version(
            &mut properties,
            create.format_version.as_deref(),
            format_version,
        );
        let creation = TableCreation::builder()
            .name(create.table.clone())
            .schema(create.schema)
            .partition_spec_opt(partition_spec)
            .format_version(format_version)
            .properties(properties)
            .build();
        let staged = StagedTableTransaction::begin_replace(&existing, creation)
            .await
            .map_err(iceberg_err)?;
        staged
            .add_data_files(Vec::new())
            .commit(catalog.as_ref())
            .await
            .map_err(iceberg_err)?;
    } else if catalogs.location_policy(&create.catalog)
        == Some(LocationPolicy::ServiceManagedLocation)
    {
        validate_service_managed_create(catalog.as_ref(), &create).await?;
        let creation = TableCreation::builder()
            .name(create.table.clone())
            .schema(create.schema)
            .partition_spec_opt(partition_spec)
            .format_version(format_version)
            .properties(create.properties.clone())
            .build();
        catalog
            .create_table(&create.namespace, creation)
            .await
            .map_err(iceberg_err)?;
    } else {
        let plan = resolve_create_plan_for(
            catalog.as_ref(),
            catalogs,
            &create.catalog,
            &create.namespace,
            &create.table,
            &create.full_name,
        )
        .await?;
        commit_staged_schema_only(
            catalog.as_ref(),
            plan,
            &table_ident,
            &create.table,
            create.schema,
            partition_spec,
            create.properties.clone(),
            format_version,
        )
        .await?;
    }

    let namespace = namespace_schema_name(&create.namespace);
    reregister(ctx, catalog.clone(), &create.catalog, &namespace).await?;
    ctx.read_empty()
}

/// Resolve CREATE/CTAS `TBLPROPERTIES ('format-version')` against the session opt-in.
/// pins: v3-2-create-v3-opt-in/C-001, C-005
///
pub(crate) fn iceberg_create_format_version(
    ctx: &SessionContext,
    requested: Option<&str>,
) -> Result<iceberg::spec::FormatVersion> {
    use iceberg::spec::FormatVersion;
    use repark_functions::cardinality::{
        repark_sql_settings_from_options, resolve_create_format_version,
    };
    let allow = repark_sql_settings_from_options(ctx.copied_config().options())
        .allow_create_format_version_3;
    let number =
        resolve_create_format_version(requested, allow, "format-version", "TBLPROPERTIES")?;
    Ok(if number == 3 {
        FormatVersion::V3
    } else {
        FormatVersion::V2
    })
}

/// The fork's replace path upgrades format version from the reserved `format-version`
/// property, not `TableCreation.format_version`. Stamp only when SQL requested a version
/// so an unspecified OR REPLACE of a v3 table cannot force v2.
pub(crate) fn stamp_requested_format_version(
    properties: &mut HashMap<String, String>,
    requested: Option<&str>,
    format_version: iceberg::spec::FormatVersion,
) {
    if requested.map(str::trim).is_none_or(str::is_empty) {
        return;
    }
    let number = if format_version == iceberg::spec::FormatVersion::V3 {
        "3"
    } else {
        "2"
    };
    properties.insert("format-version".to_string(), number.to_string());
}

async fn validate_service_managed_create(
    catalog: &dyn Catalog,
    create: &SchemaCreate,
) -> Result<()> {
    catalog
        .get_namespace(&create.namespace)
        .await
        .map_err(iceberg_err)?;
    reject_path_escape_ident(create.catalog.as_str(), "catalog")?;
    for part in create.namespace.as_ref() {
        reject_path_escape_ident(part.as_str(), "namespace")?;
    }
    reject_path_escape_ident(create.table.as_str(), "table")
}

#[allow(clippy::too_many_arguments)] // schema-only staged create carries location plan + V3-2 version
async fn commit_staged_schema_only(
    catalog: &dyn Catalog,
    plan: CreatePlan,
    table_ident: &TableIdent,
    table_name: &str,
    schema: Schema,
    partition_spec: Option<UnboundPartitionSpec>,
    properties: HashMap<String, String>,
    format_version: iceberg::spec::FormatVersion,
) -> Result<()> {
    let creation = TableCreation::builder()
        .name(table_name.to_string())
        .location(plan.location)
        .schema(schema)
        .partition_spec_opt(partition_spec)
        .format_version(format_version)
        .properties(properties)
        .build();
    let staged = StagedTableTransaction::begin_create(plan.file_io, table_ident.clone(), creation)
        .await
        .map_err(iceberg_err)?;
    // Schema-only: no data write — empty pending files publish metadata only (fork
    // `StagedTableTransaction::materialize_pending` short-circuits on empty).
    staged
        .add_data_files(Vec::new())
        .commit(catalog)
        .await
        .map_err(iceberg_err)?;
    Ok(())
}

#[cfg(test)]
mod type_mapping_tests {
    use super::*;
    use datafusion::sql::sqlparser::ast::DataType as SqlDataType;
    use datafusion::sql::sqlparser::ast::TimezoneInfo;

    #[test]
    fn maps_spark_core_types() {
        assert!(matches!(
            sql_type_to_iceberg(&SqlDataType::BigInt(None)).unwrap(),
            Type::Primitive(PrimitiveType::Long)
        ));
        assert!(matches!(
            sql_type_to_iceberg(&SqlDataType::Int(None)).unwrap(),
            Type::Primitive(PrimitiveType::Int)
        ));
        assert!(matches!(
            sql_type_to_iceberg(&SqlDataType::String(None)).unwrap(),
            Type::Primitive(PrimitiveType::String)
        ));
        assert!(matches!(
            sql_type_to_iceberg(&SqlDataType::Boolean).unwrap(),
            Type::Primitive(PrimitiveType::Boolean)
        ));
        assert!(matches!(
            sql_type_to_iceberg(&SqlDataType::Double(ExactNumberInfo::None)).unwrap(),
            Type::Primitive(PrimitiveType::Double)
        ));
        assert!(matches!(
            sql_type_to_iceberg(&SqlDataType::Date).unwrap(),
            Type::Primitive(PrimitiveType::Date)
        ));
        assert!(matches!(
            sql_type_to_iceberg(&SqlDataType::Timestamp(None, TimezoneInfo::None)).unwrap(),
            Type::Primitive(PrimitiveType::Timestamptz)
        ));
        assert!(matches!(
            sql_type_to_iceberg(&SqlDataType::TimestampNtz(None)).unwrap(),
            Type::Primitive(PrimitiveType::Timestamp)
        ));
        match sql_type_to_iceberg(&SqlDataType::Decimal(ExactNumberInfo::PrecisionAndScale(
            10, 2,
        )))
        .unwrap()
        {
            Type::Primitive(PrimitiveType::Decimal { precision, scale }) => {
                assert_eq!(precision, 10);
                assert_eq!(scale, 2);
            }
            other => panic!("expected decimal, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unsupported_array() {
        use datafusion::sql::sqlparser::ast::ArrayElemTypeDef;
        let err = sql_type_to_iceberg(&SqlDataType::Array(ArrayElemTypeDef::None)).unwrap_err();
        assert!(err.to_string().contains("not supported"), "got: {err}");
    }

    #[test]
    fn bare_timestamp_follows_session_timestamp_type() {
        assert!(matches!(
            sql_type_to_iceberg_with_timestamp_type(
                &SqlDataType::Timestamp(None, TimezoneInfo::None),
                SparkTimestampType::Ltz,
            )
            .unwrap(),
            Type::Primitive(PrimitiveType::Timestamptz)
        ));
        assert!(matches!(
            sql_type_to_iceberg_with_timestamp_type(
                &SqlDataType::Timestamp(None, TimezoneInfo::None),
                SparkTimestampType::Ntz,
            )
            .unwrap(),
            Type::Primitive(PrimitiveType::Timestamp)
        ));
        assert!(matches!(
            sql_type_to_iceberg_with_timestamp_type(
                &SqlDataType::TimestampNtz(None),
                SparkTimestampType::Ltz,
            )
            .unwrap(),
            Type::Primitive(PrimitiveType::Timestamp)
        ));
        assert!(matches!(
            sql_type_to_iceberg_with_timestamp_type(
                &SqlDataType::Timestamp(None, TimezoneInfo::WithTimeZone),
                SparkTimestampType::Ntz,
            )
            .unwrap(),
            Type::Primitive(PrimitiveType::Timestamptz)
        ));
    }

    #[test]
    fn not_null_maps_required_and_default_option_refused() {
        use datafusion::sql::sqlparser::ast::{
            ColumnDef, ColumnOption, ColumnOptionDef, Ident, Statement,
        };
        use datafusion::sql::sqlparser::dialect::DatabricksDialect;
        use datafusion::sql::sqlparser::parser::Parser;

        let not_null = ColumnDef {
            name: Ident::new("id"),
            data_type: SqlDataType::BigInt(None),
            options: vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::NotNull,
            }],
        };
        let schema =
            schema_from_column_defs(std::slice::from_ref(&not_null), SparkTimestampType::Ltz)
                .unwrap();
        assert!(schema.as_struct().fields()[0].required);

        // Parse a real DEFAULT form so the option variant stays accurate across sqlparser bumps.
        let statements = Parser::parse_sql(
            &DatabricksDialect {},
            "CREATE TABLE t (id BIGINT DEFAULT 0)",
        )
        .expect("parse");
        let Statement::CreateTable(create) = &statements[0] else {
            panic!("expected CreateTable");
        };
        let err = schema_from_column_defs(&create.columns, SparkTimestampType::Ltz).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("not supported")
                && (message.contains("DEFAULT") || message.contains("default")),
            "got: {message}"
        );
    }
}
