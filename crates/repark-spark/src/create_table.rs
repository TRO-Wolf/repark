//! Column-def `CREATE TABLE … (cols) USING iceberg [PARTITIONED BY …] [TBLPROPERTIES …]`.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    ArrayElemTypeDef, ColumnDef, ColumnOption, CreateTable, CreateTableOptions,
    DataType as SqlDataType, ExactNumberInfo, SqlOption, StructField, TimezoneInfo,
};
use datafusion::sql::sqlparser::parser::ParserError;
use iceberg::spec::{
    ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type, UnboundPartitionSpec,
};
use iceberg::transaction::StagedTableTransaction;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};

use repark_common::spark_error;
use repark_core::{CatalogRegistry, LocationPolicy};
use repark_functions::timestamp_type::{SparkTimestampType, spark_timestamp_type_from_options};
use repark_iceberg::write::nested_type_sql::struct_field_required;

use crate::catalog_ops::table_or_view_already_exists;
use crate::normalize::create_clauses::CreateClauses;
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
    location: Option<String>,
    /// Requested `TBLPROPERTIES ('format-version' = …)`, consumed at execute (session opt-in).
    format_version: Option<String>,
}

/// Execute schema-only staged `CREATE TABLE` with optional partition spec and table properties.
/// # Errors
/// Name resolution, type mapping, partition resolution, location, or catalog publish failures.
pub(crate) async fn execute_create_table(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    create: &CreateTable,
    partitioning: &[PartitionedByElement],
    clauses: &CreateClauses,
) -> Result<DataFrame> {
    let options = ctx.copied_config();
    let timestamp_type = spark_timestamp_type_from_options(options.options());
    let case_sensitive =
        repark_functions::case_sensitive::spark_case_sensitive_from_options(options.options());
    let schema_create = build_schema_create(
        catalogs,
        create,
        partitioning,
        timestamp_type,
        case_sensitive,
        clauses,
    )?;
    execute_schema_create(ctx, catalogs, schema_create).await
}

#[allow(clippy::too_many_lines)] // one clause-by-clause walk of the CREATE TABLE AST — splitting would scatter the refuse rules
/// Extract a [`SchemaCreate`] from a non-CTAS `CREATE TABLE` AST + token-extracted partitioning.
fn build_schema_create(
    catalogs: &CatalogRegistry,
    create: &CreateTable,
    partitioning: &[PartitionedByElement],
    timestamp_type: SparkTimestampType,
    case_sensitive: bool,
    clauses: &CreateClauses,
) -> Result<SchemaCreate> {
    if create.query.is_some() {
        return Err(DataFusionError::Internal(
            "build_schema_create requires a non-CTAS CREATE TABLE".into(),
        ));
    }
    // TEMPORARY / EXTERNAL / TRANSIENT / VOLATILE must not silently create durable Iceberg tables.
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
    crate::refuse_unsupported_create_table_clauses(create, "column-def CREATE")?;
    // LIKE / CLONE before empty-column check so the honest NotImplemented surfaces.
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
    let typed_columns = typed_partition_columns(partitioning)?;
    if create.columns.is_empty() && typed_columns.is_empty() {
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
                partition_fields.push(PartitionFieldSpec::Identity(column.name.value.clone()));
            }
            PartitionedByElement::Nested(path) => {
                return Err(DataFusionError::NotImplemented(format!(
                    "CREATE TABLE PARTITIONED BY nested-field reference `{path}` is not \
                     supported yet — partition by a top-level column (v1)"
                )));
            }
        }
    }

    let parts = crate::use_ddl::complete_name(catalogs, &name_parts(&create.name))?;
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
    refuse_reserved_owner_property(&properties)?;
    // Reserved Iceberg key — consumed here, applied as `TableCreation.format_version` at execute.
    let format_version = properties.remove("format-version");
    if let Some(comment) = clauses.comment.clone() {
        properties.insert("comment".to_string(), comment);
    }

    let table_name = format!("`{catalog}`.`{namespace}`.`{table}`");
    refuse_duplicate_partition_columns(&create.columns, &typed_columns, case_sensitive)?;
    let columns: Vec<ColumnDef> = create
        .columns
        .iter()
        .chain(&typed_columns)
        .cloned()
        .collect();
    let schema = schema_from_column_defs(&columns, timestamp_type, &table_name)?;
    let partition_spec = build_partition_spec(&schema, &partition_fields, true)?;
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
        location: clauses.location.clone(),
        format_version,
    })
}

pub(crate) fn typed_partition_columns(
    partitioning: &[PartitionedByElement],
) -> Result<Vec<ColumnDef>> {
    let mut columns = Vec::new();
    let mut expressions = Vec::new();
    for element in partitioning {
        match element {
            PartitionedByElement::Typed(column) => columns.push(column.clone()),
            PartitionedByElement::Identity(column) | PartitionedByElement::Nested(column) => {
                expressions.push(column.clone());
            }
            PartitionedByElement::Transform { name, args } => {
                expressions.push(format!("{name}({})", args.join(", ")));
            }
        }
    }
    if columns.is_empty() || expressions.is_empty() {
        return Ok(columns);
    }
    let rendered = columns
        .iter()
        .map(|column| {
            let data_type = column.data_type.to_string().to_lowercase();
            format!("{} {data_type}", column.name.value)
        })
        .collect::<Vec<_>>();
    Err(DataFusionError::SQL(
        Box::new(ParserError::ParserError(format!(
            "Operation not allowed: PARTITION BY: Cannot mix partition expressions and \
             partition columns:\nExpressions: {}\nColumns: {}.",
            expressions.join(", "),
            rendered.join(", ")
        ))),
        None,
    ))
}

fn refuse_duplicate_partition_columns(
    declared: &[ColumnDef],
    typed: &[ColumnDef],
    case_sensitive: bool,
) -> Result<()> {
    for (index, column) in typed.iter().enumerate() {
        let name = &column.name.value;
        if declared.iter().chain(&typed[..index]).any(|earlier| {
            if case_sensitive {
                earlier.name.value == *name
            } else {
                earlier.name.value.eq_ignore_ascii_case(name)
            }
        }) {
            return Err(DataFusionError::Plan(format!(
                "[COLUMN_ALREADY_EXISTS] The column `{}` already exists. Choose another name or \
                 rename the existing column. SQLSTATE: 42711",
                name.to_lowercase()
            )));
        }
    }
    Ok(())
}

/// Map sqlparser column defs → Iceberg [`Schema`] (1-based field ids, Spark nullable default).
fn schema_from_column_defs(
    columns: &[ColumnDef],
    timestamp_type: SparkTimestampType,
    table_name: &str,
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(columns.len());
    let mut next_id = 1i32;
    for column in columns {
        let field_id = alloc_field_id(&mut next_id)?;
        let iceberg_type =
            sql_type_to_iceberg_nested(&column.data_type, timestamp_type, &mut next_id)?;
        let mut required = false;
        let mut doc: Option<String> = None;
        for option in &column.options {
            match &option.option {
                ColumnOption::NotNull => required = true,
                ColumnOption::Null => {}
                ColumnOption::Comment(text) => doc = Some(text.clone()),
                _ => {
                    return Err(DataFusionError::Plan(spark_error::message(
                        spark_error::UNSUPPORTED_FEATURE_TABLE_OPERATION,
                        &[("tableName", table_name)],
                    )));
                }
            }
        }
        let mut nested = if required {
            NestedField::required(field_id, column.name.value.clone(), iceberg_type)
        } else {
            NestedField::optional(field_id, column.name.value.clone(), iceberg_type)
        };
        if let Some(text) = doc {
            nested = nested.with_doc(text);
        }
        fields.push(Arc::new(nested));
    }
    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(iceberg_err)
}

/// Map a Spark/SQL column type to an Iceberg primitive (loud on nested / unsupported).
#[cfg(test)]
pub(crate) fn sql_type_to_iceberg(data_type: &SqlDataType) -> Result<Type> {
    sql_type_to_iceberg_with_timestamp_type(data_type, SparkTimestampType::Ltz)
}

/// Same mapping as [`sql_type_to_iceberg`], with the session default for bare `TIMESTAMP`.
pub(crate) fn sql_type_to_iceberg_with_timestamp_type(
    data_type: &SqlDataType,
    timestamp_type: SparkTimestampType,
) -> Result<Type> {
    let mut next_id = 1i32;
    sql_type_to_iceberg_nested(data_type, timestamp_type, &mut next_id)
}

fn alloc_field_id(next_id: &mut i32) -> Result<i32> {
    let allocated = *next_id;
    *next_id = next_id.checked_add(1).ok_or_else(|| {
        DataFusionError::Plan("CREATE TABLE exceeds Iceberg field-id range".into())
    })?;
    Ok(allocated)
}

fn sql_type_to_iceberg_nested(
    data_type: &SqlDataType,
    timestamp_type: SparkTimestampType,
    next_id: &mut i32,
) -> Result<Type> {
    if let SqlDataType::Array(ArrayElemTypeDef::AngleBracket(element)) = data_type {
        let element_id = alloc_field_id(next_id)?;
        let element_type = sql_type_to_iceberg_nested(element, timestamp_type, next_id)?;
        let field = NestedField::optional(element_id, "element", element_type);
        return Ok(Type::List(ListType::new(Arc::new(field))));
    }
    if let SqlDataType::Struct(fields, _) = data_type {
        return struct_type_to_iceberg(fields, timestamp_type, next_id);
    }
    if let SqlDataType::Map(key, value) = data_type {
        let key_id = alloc_field_id(next_id)?;
        let key_type = sql_type_to_iceberg_nested(key, timestamp_type, next_id)?;
        let value_id = alloc_field_id(next_id)?;
        let value_type = sql_type_to_iceberg_nested(value, timestamp_type, next_id)?;
        return Ok(Type::Map(MapType::new(
            Arc::new(NestedField::map_key_element(key_id, key_type)),
            Arc::new(NestedField::map_value_element(value_id, value_type, false)),
        )));
    }
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
        // TIMESTAMP_NTZ stays a naive Iceberg timestamp, independent of the session default.
        SqlDataType::TimestampNtz(_) | SqlDataType::Timestamp(_, TimezoneInfo::WithoutTimeZone) => {
            PrimitiveType::Timestamp
        }
        // Bare TIMESTAMP follows spark.sql.timestampType.
        SqlDataType::Timestamp(_, TimezoneInfo::None) => match timestamp_type {
            SparkTimestampType::Ltz => PrimitiveType::Timestamptz,
            SparkTimestampType::Ntz => PrimitiveType::Timestamp,
        },
        // WITH TIME ZONE / TIMESTAMPTZ stay instants.
        SqlDataType::Timestamp(_, _) => PrimitiveType::Timestamptz,
        SqlDataType::Binary(_) | SqlDataType::Varbinary(_) => PrimitiveType::Binary,
        other => {
            if let Some(primitive) = iceberg_named_primitive(other) {
                primitive
            } else {
                if geospatial_sql_type(other) {
                    return Err(DataFusionError::Plan(spark_error::message(
                        spark_error::UNSUPPORTED_FEATURE_GEOSPATIAL_DISABLED,
                        &[],
                    )));
                }
                return Err(DataFusionError::NotImplemented(format!(
                    "column type `{other}` is not supported yet for Iceberg tables"
                )));
            }
        }
    };
    Ok(Type::Primitive(primitive))
}

fn struct_type_to_iceberg(
    fields: &[StructField],
    timestamp_type: SparkTimestampType,
    next_id: &mut i32,
) -> Result<Type> {
    let mut children = Vec::with_capacity(fields.len());
    for field in fields {
        let name = field
            .field_name
            .as_ref()
            .ok_or_else(|| DataFusionError::Plan(format!("STRUCT field `{field}` needs a name")))?;
        let field_id = alloc_field_id(next_id)?;
        let field_type = sql_type_to_iceberg_nested(&field.field_type, timestamp_type, next_id)?;
        let required = struct_field_required(field).map_err(|message| {
            DataFusionError::SQL(Box::new(ParserError::ParserError(message)), None)
        })?;
        let child = if required {
            NestedField::required(field_id, name.value.clone(), field_type)
        } else {
            NestedField::optional(field_id, name.value.clone(), field_type)
        };
        children.push(Arc::new(child));
    }
    Ok(Type::Struct(StructType::new(children)))
}

fn geospatial_sql_type(data_type: &SqlDataType) -> bool {
    let upper = data_type.to_string().to_ascii_uppercase();
    let head = upper.split(['(', ' ', ',']).next().unwrap_or("");
    head == "GEOMETRY" || head == "GEOGRAPHY"
}

fn iceberg_named_primitive(data_type: &SqlDataType) -> Option<PrimitiveType> {
    match data_type.to_string().to_ascii_lowercase().as_str() {
        "timestamp_ltz" => Some(PrimitiveType::Timestamptz),
        "timestamp_ns" => Some(PrimitiveType::TimestampNs),
        "timestamptz_ns" => Some(PrimitiveType::TimestamptzNs),
        _ => None,
    }
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

async fn replaced_table(
    catalog: &dyn Catalog,
    table_ident: &TableIdent,
    existed: bool,
    create: &mut SchemaCreate,
) -> Result<Option<iceberg::table::Table>> {
    if !existed {
        return Ok(None);
    }
    let table = catalog.load_table(table_ident).await.map_err(iceberg_err)?;
    create.schema = repark_iceberg::write::replacement_schema(table.metadata(), &create.schema)?;
    Ok(Some(table))
}

async fn execute_schema_create(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    mut create: SchemaCreate,
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
            return Err(table_or_view_already_exists(
                &create.catalog,
                create.namespace.to_string().as_str(),
                &create.table,
            ));
        }
    }
    crate::ctas::check_custom_location(
        create.location.as_deref(),
        existed,
        catalogs.location_policy(&create.catalog) == Some(LocationPolicy::ServiceManagedLocation),
        "column-def CREATE",
    )?;

    let existing = replaced_table(catalog.as_ref(), &table_ident, existed, &mut create).await?;
    let partition_spec = build_partition_spec(
        &create.schema,
        &create.partition_fields,
        crate::spark_door_case_insensitive(ctx.state().config().options()),
    )?;
    let format_version = iceberg_create_format_version(ctx, create.format_version.as_deref())?;
    if let Some(existing) = existing {
        let mut properties = stamped_creation_properties(ctx, catalogs, &create);
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
        let properties = stamped_creation_properties(ctx, catalogs, &create);
        let creation = TableCreation::builder()
            .name(create.table.clone())
            .schema(create.schema)
            .partition_spec_opt(partition_spec)
            .format_version(format_version)
            .properties(properties)
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
            create.location.as_deref(),
        )
        .await?;
        let properties = stamped_creation_properties(ctx, catalogs, &create);
        commit_staged_schema_only(
            catalog.as_ref(),
            plan,
            &table_ident,
            &create.table,
            create.schema,
            partition_spec,
            properties,
            format_version,
        )
        .await?;
    }

    let namespace = namespace_schema_name(&create.namespace);
    reregister(ctx, catalog.clone(), &create.catalog, &namespace).await?;
    ctx.read_empty()
}

/// pins: v3-2-create-v3-opt-in/C-001, C-005
/// Model: Grok 4.6 xHigh
/// Resolve CREATE/CTAS `TBLPROPERTIES ('format-version')` against the session opt-in.
pub(crate) fn iceberg_create_format_version(
    ctx: &SessionContext,
    requested: Option<&str>,
) -> Result<iceberg::spec::FormatVersion> {
    use iceberg::spec::FormatVersion;
    use repark_functions::cardinality::{
        repark_sql_settings_from_options, resolve_create_format_version,
    };
    refuse_format_version_spark_rejects(requested)?;
    let allow = repark_sql_settings_from_options(ctx.copied_config().options())
        .allow_create_format_version_3;
    let number =
        resolve_create_format_version(requested, allow, "format-version", "TBLPROPERTIES")?;
    Ok(match number {
        1 => FormatVersion::V1,
        3 => FormatVersion::V3,
        _ => FormatVersion::V2,
    })
}

const SPARK_MAX_FORMAT_VERSION: i32 = 4;

fn refuse_format_version_spark_rejects(requested: Option<&str>) -> Result<()> {
    let Some(raw) = requested.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    match raw.parse::<i32>() {
        Err(_) => Err(repark_core::illegal_argument_error(format!(
            "For input string: \"{raw}\""
        ))),
        Ok(number) if number > SPARK_MAX_FORMAT_VERSION => {
            Err(repark_core::illegal_argument_error(format!(
                "Unsupported format version: v{number} (supported: v{SPARK_MAX_FORMAT_VERSION})"
            )))
        }
        Ok(_) => Ok(()),
    }
}

/// The fork replace path upgrades format version from `format-version`, not `TableCreation`.
pub(crate) fn stamp_requested_format_version(
    properties: &mut HashMap<String, String>,
    requested: Option<&str>,
    format_version: iceberg::spec::FormatVersion,
) {
    if requested.map(str::trim).is_none_or(str::is_empty) {
        return;
    }
    let number = format_version as u8;
    properties.insert("format-version".to_string(), number.to_string());
}

pub(crate) fn stamp_owner(ctx: &SessionContext, properties: &mut HashMap<String, String>) {
    properties.insert(
        "owner".to_string(),
        crate::describe_show::describe_table_owner(ctx),
    );
}

fn stamped_creation_properties(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    create: &SchemaCreate,
) -> HashMap<String, String> {
    let mut properties = catalogs.table_creation_properties(&create.catalog, &create.properties);
    stamp_owner(ctx, &mut properties);
    properties
}

pub(crate) const RESERVED_OWNER_PROPERTY_ERROR: &str = concat!(
    "[UNSUPPORTED_FEATURE.SET_TABLE_PROPERTY] The feature is not supported: ",
    "owner is a reserved table property, it will be set to the current user. ",
    "SQLSTATE: 0A000"
);

pub(crate) fn refuse_reserved_owner_property(properties: &HashMap<String, String>) -> Result<()> {
    if properties.contains_key("owner") {
        return Err(DataFusionError::SQL(
            Box::new(ParserError::ParserError(
                RESERVED_OWNER_PROPERTY_ERROR.to_string(),
            )),
            None,
        ));
    }
    Ok(())
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
    // Schema-only: no data write — empty pending files publish metadata only.
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
    fn maps_angle_bracket_array_to_nullable_element_list() {
        use datafusion::sql::sqlparser::ast::ArrayElemTypeDef;
        let angle = |inner: SqlDataType| {
            SqlDataType::Array(ArrayElemTypeDef::AngleBracket(Box::new(inner)))
        };
        let Type::List(list) = sql_type_to_iceberg(&angle(SqlDataType::Int(None))).unwrap() else {
            panic!("expected list");
        };
        assert_eq!(list.element_field.name, "element");
        assert!(!list.element_field.required);
        assert!(matches!(
            list.element_field.field_type.as_ref(),
            Type::Primitive(PrimitiveType::Int)
        ));
        let Type::List(outer) =
            sql_type_to_iceberg(&angle(angle(SqlDataType::String(None)))).unwrap()
        else {
            panic!("expected nested list");
        };
        let Type::List(inner) = outer.element_field.field_type.as_ref() else {
            panic!("expected inner list");
        };
        assert!(matches!(
            inner.element_field.field_type.as_ref(),
            Type::Primitive(PrimitiveType::String)
        ));
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
    }

    #[test]
    fn maps_iceberg_v3_nanosecond_timestamp_names() {
        use datafusion::sql::sqlparser::ast::{Ident, ObjectName};
        let timestamp_ns =
            SqlDataType::Custom(ObjectName::from(Ident::new("timestamp_ns")), Vec::new());
        let timestamptz_ns =
            SqlDataType::Custom(ObjectName::from(Ident::new("timestamptz_ns")), Vec::new());
        assert!(matches!(
            sql_type_to_iceberg(&timestamp_ns).unwrap(),
            Type::Primitive(PrimitiveType::TimestampNs)
        ));
        assert!(matches!(
            sql_type_to_iceberg(&timestamptz_ns).unwrap(),
            Type::Primitive(PrimitiveType::TimestamptzNs)
        ));
        assert!(matches!(
            sql_type_to_iceberg(&SqlDataType::TimestampNtz(None)).unwrap(),
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
        let schema = schema_from_column_defs(
            std::slice::from_ref(&not_null),
            SparkTimestampType::Ltz,
            "`t`",
        )
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
        let err =
            schema_from_column_defs(&create.columns, SparkTimestampType::Ltz, "`t`").unwrap_err();
        assert!(
            matches!(err, DataFusionError::Plan(_)),
            "DEFAULT refuse must be Plan, got: {err:?}"
        );
        let message = err.to_string();
        assert!(
            message.contains("[UNSUPPORTED_FEATURE.TABLE_OPERATION]")
                && message.contains("SQLSTATE: 0A000")
                && message.contains("column default value"),
            "got: {message}"
        );
    }

    #[test]
    fn column_comment_maps_to_field_doc() {
        use datafusion::sql::sqlparser::ast::{ColumnDef, ColumnOption, ColumnOptionDef, Ident};

        let commented = ColumnDef {
            name: Ident::new("id"),
            data_type: SqlDataType::BigInt(None),
            options: vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::Comment("the id".to_string()),
            }],
        };
        let schema = schema_from_column_defs(
            std::slice::from_ref(&commented),
            SparkTimestampType::Ltz,
            "`t`",
        )
        .unwrap();
        assert_eq!(
            schema.as_struct().fields()[0].doc.as_deref(),
            Some("the id")
        );
    }
}
