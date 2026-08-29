//! `CREATE TABLE` — CTAS and the column-def form — with the Q15 routing ruling.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    ColumnOption, CreateTable, CreateTableOptions, ObjectName, SqlOption,
};
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::io::FileIO;
use iceberg::spec::{FormatVersion, UnboundPartitionSpec};
use iceberg::transaction::StagedTableTransaction;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};

use crate::partitioning::build_partition_spec;
use crate::properties::{TableProperties, parse_with_options};
use crate::schema_ddl::{iceberg_err, name_parts, reject_path_escape_ident};

/// Plan and guard the CTAS body before target creation or publication, then return its lazy frame.
async fn derive_ctas_query(
    cx: &EngineContext<'_>,
    query: &datafusion::sql::sqlparser::ast::Query,
) -> Result<DataFrame> {
    let belt = repark_core::PreExecute::new(cx.ctx, cx.catalogs);
    let plan = belt.plan(&query.to_string()).await?;
    belt.guard(&plan)?;
    repark_core::refuse_iceberg_create_of_tightened_plan(&plan)
        .map_err(|error| DataFusionError::Plan(error.to_string()))?;
    belt.execute(plan).await
}

/// A create target that resolved to a registered Iceberg catalog.
pub(crate) struct CreateTarget {
    pub(crate) catalog_name: String,
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) namespace: NamespaceIdent,
    pub(crate) table: String,
    /// The name as the user wrote it, for messages.
    pub(crate) full_name: String,
}

impl CreateTarget {
    pub(crate) fn ident(&self) -> TableIdent {
        TableIdent::new(self.namespace.clone(), self.table.clone())
    }

    pub(crate) fn schema_name(&self) -> String {
        self.namespace
            .as_ref()
            .last()
            .cloned()
            .unwrap_or_else(|| self.namespace.to_url_string())
    }
}

/// ===========================================================================================
/// Route and execute a `CREATE TABLE` (with or without `AS SELECT`).
/// ===========================================================================================
pub(crate) async fn execute_create_table(
    cx: &EngineContext<'_>,
    create: &CreateTable,
) -> Result<DataFrame> {
    let form = if create.query.is_some() {
        "CREATE TABLE AS SELECT"
    } else {
        "CREATE TABLE"
    };
    refuse_unsupported_clauses(create, form)?;
    let options = with_options(create, form)?;
    let properties = parse_with_options(&options, form)?;
    let target = resolve_target(cx, &create.name, form)?;

    let existed = target
        .catalog
        .table_exists(&target.ident())
        .await
        .map_err(iceberg_err)?;
    if existed {
        if create.if_not_exists {
            return cx.ctx.read_empty();
        }
        if !create.or_replace {
            return Err(DataFusionError::Plan(format!(
                "table `{}` already exists — use CREATE OR REPLACE TABLE to replace it, or \
                 CREATE TABLE IF NOT EXISTS to make this a no-op",
                target.full_name
            )));
        }
    }

    // Derive the table's Arrow schema. CTAS uses the SELECT's logical plan without publishing it.
    let (arrow_schema, query) = if let Some(query) = create.query.as_ref() {
        let frame = derive_ctas_query(cx, query).await?;
        let schema = Arc::new(frame.schema().as_arrow().clone());
        (schema, Some(frame))
    } else {
        let schema = column_def_schema(cx.ctx, create, form).await?;
        refuse_nanosecond_timestamp_columns(&schema, form)?;
        (schema, None)
    };
    repark_core::refuse_iceberg_create_of_tightened_schema(arrow_schema.as_ref())
        .map_err(|error| DataFusionError::Plan(error.to_string()))?;
    let iceberg_schema =
        arrow_schema_to_schema_auto_assign_ids(arrow_schema.as_ref()).map_err(iceberg_err)?;
    let partition_spec = build_partition_spec(&iceberg_schema, &properties.partitioning)?;
    let format_version =
        iceberg_create_format_version(cx.ctx, properties.format_version.as_deref())?;

    // Resolve the placement before running the SELECT so target errors fail before writes.
    let placement = resolve_placement(&target, &properties, cx.catalogs, existed).await?;

    match placement {
        Placement::ServiceManaged => {
            create_first_service_managed(
                cx,
                &target,
                &properties,
                iceberg_schema,
                partition_spec,
                format_version,
                query,
            )
            .await
        }
        Placement::StagedReplace | Placement::StagedCreate { .. } => {
            execute_staged_create(
                cx,
                &target,
                &properties,
                iceberg_schema,
                partition_spec,
                format_version,
                query,
                placement,
            )
            .await
        }
    }
}

/// Model: Grok 4.6 xHigh
#[allow(clippy::too_many_arguments)] // target + schema + V3-2 version + placement travel together
async fn execute_staged_create(
    cx: &EngineContext<'_>,
    target: &CreateTarget,
    properties: &TableProperties,
    iceberg_schema: iceberg::spec::Schema,
    partition_spec: Option<UnboundPartitionSpec>,
    format_version: FormatVersion,
    query: Option<DataFrame>,
    placement: Placement,
) -> Result<DataFrame> {
    let staged = if let Placement::StagedCreate { location, file_io } = placement {
        let creation = iceberg_table_creation(
            &target.table,
            iceberg_schema,
            partition_spec,
            format_version,
            properties.extra_properties.clone(),
            Some(location),
            None,
        );
        StagedTableTransaction::begin_create(*file_io, target.ident(), creation)
            .await
            .map_err(iceberg_err)?
    } else {
        // Replace stages keep the existing table location and metadata contract.
        let existing = target
            .catalog
            .load_table(&target.ident())
            .await
            .map_err(iceberg_err)?;
        let creation = iceberg_table_creation(
            &target.table,
            iceberg_schema,
            partition_spec,
            format_version,
            properties.extra_properties.clone(),
            None,
            properties.format_version.as_deref(),
        );
        StagedTableTransaction::begin_replace(&existing, creation)
            .await
            .map_err(iceberg_err)?
    };

    // Streaming bounds memory by batch size and open writers. A failure leaves the staged
    // transaction unpublished; one commit publishes it.
    let data_files = match query {
        Some(frame) => {
            let stream = frame.execute_stream().await?;
            write_stream(cx.ctx, staged.table(), stream).await?
        }
        None => Vec::new(),
    };
    staged
        .add_data_files(data_files)
        .commit(target.catalog.as_ref())
        .await
        .map_err(iceberg_err)?;
    finish(cx.ctx, target).await
}

/// Where a create's data will live, resolved before the SELECT runs.
enum Placement {
    /// Stage a REPLACE against the existing table, which carries its own location and `FileIO`.
    StagedReplace,
    /// Stage a CREATE at `location`, write, publish the catalog pointer last.
    StagedCreate {
        location: String,
        file_io: Box<FileIO>,
    },
    /// The service assigns the location at create time (S3 Tables) — staging is impossible.
    ServiceManaged,
}

/// Resolve create placement. REPLACE reuses the existing table's location.
async fn resolve_placement(
    target: &CreateTarget,
    properties: &TableProperties,
    catalogs: &CatalogRegistry,
    existed: bool,
) -> Result<Placement> {
    if existed {
        return Ok(Placement::StagedReplace);
    }
    let policy = catalogs
        .location_policy(&target.catalog_name)
        // Default to the STRICT policy when a catalog somehow carries none: an unknown policy
        // must never silently mean "temp directory is fine".
        .unwrap_or(LocationPolicy::RequireExplicitLocation);

    if matches!(policy, LocationPolicy::ServiceManagedLocation) {
        if properties.location.is_some() {
            return Err(DataFusionError::Plan(format!(
                "catalog `{}` assigns table locations itself (service-managed) — \
                 WITH (location = …) is not accepted for `{}`",
                target.catalog_name, target.full_name
            )));
        }
        // Fail early: the namespace must exist and identifiers must be safe.
        target
            .catalog
            .get_namespace(&target.namespace)
            .await
            .map_err(iceberg_err)?;
        validate_identifiers(target)?;
        return Ok(Placement::ServiceManaged);
    }

    let location = resolve_location(target, properties, policy).await?;
    let file_io =
        repark_iceberg::catalog::file_io_for_location(&location, target.catalog.properties())?;
    Ok(Placement::StagedCreate {
        location,
        file_io: Box::new(file_io),
    })
}

/// The table's absolute location: explicit property → namespace property → policy fallback.
async fn resolve_location(
    target: &CreateTarget,
    properties: &TableProperties,
    policy: LocationPolicy,
) -> Result<String> {
    if let Some(location) = &properties.location {
        return Ok(location.trim_end_matches('/').to_string());
    }
    let namespace = target
        .catalog
        .get_namespace(&target.namespace)
        .await
        .map_err(iceberg_err)?;
    validate_identifiers(target)?;

    if let Some(prefix) =
        repark_iceberg::catalog::resolve_namespace_location(namespace.properties())
    {
        return Ok(format!("{}/{}", prefix.trim_end_matches('/'), target.table));
    }
    match policy {
        LocationPolicy::TempFallbackAllowed { root } => {
            let mut path = root;
            path.push("repark_ansi_ctas");
            path.push(&target.catalog_name);
            for part in target.namespace.as_ref() {
                path.push(part.as_str());
            }
            path.push(&target.table);
            Ok(path.to_string_lossy().into_owned())
        }
        LocationPolicy::ServiceManagedLocation => Err(DataFusionError::Internal(format!(
            "location resolver reached for service-managed catalog `{}` — routing bug",
            target.catalog_name
        ))),
        LocationPolicy::RequireExplicitLocation => Err(DataFusionError::Plan(format!(
            "cannot resolve a storage location for `{}`: schema `{}` in catalog `{}` has no \
             `location` property. Set it on the schema — CREATE SCHEMA {}.{} WITH (location = \
             's3://…/{}') — or give this table an explicit location: CREATE TABLE {} WITH \
             (location = 's3://…'). RePark will not place a real warehouse's data in a temporary \
             directory.",
            target.full_name,
            target.namespace,
            target.catalog_name,
            target.catalog_name,
            target.namespace,
            target.namespace,
            target.full_name
        ))),
    }
}

/// Reject identifier segments that could escape a warehouse root once composed into a path.
fn validate_identifiers(target: &CreateTarget) -> Result<()> {
    reject_path_escape_ident(&target.catalog_name, "catalog")?;
    for part in target.namespace.as_ref() {
        reject_path_escape_ident(part.as_str(), "schema")?;
    }
    reject_path_escape_ident(&target.table, "table")
}

/// Canonical conf key (Spark-style camelCase). Default **false**.
pub(crate) const ALLOW_CREATE_FORMAT_VERSION_3_KEY: &str = "repark.sql.allowCreateFormatVersion3";

/// The `snake_case` spelling DataFusion's `extensions_options!` macro registers the field under.
const ALLOW_CREATE_FORMAT_VERSION_3_OPTION: &str = "repark.sql.allow_create_format_version_3";

/// Resolve CREATE/CTAS `WITH (format_version)` against the session opt-in.
/// pins: v3-2-create-v3-opt-in/C-006, C-013
/// Model: Grok 4.6 xHigh
#[allow(clippy::too_many_arguments)] // schema + location + requested format-version travel together
fn iceberg_table_creation(
    name: &str,
    schema: iceberg::spec::Schema,
    partition_spec: Option<UnboundPartitionSpec>,
    format_version: FormatVersion,
    extra_properties: HashMap<String, String>,
    location: Option<String>,
    requested_format_version: Option<&str>,
) -> TableCreation {
    let mut extra_properties = extra_properties;
    if requested_format_version
        .map(str::trim)
        .is_some_and(|value| !value.is_empty())
    {
        let number = if format_version == FormatVersion::V3 {
            "3"
        } else {
            "2"
        };
        extra_properties.insert("format-version".to_string(), number.to_string());
    }
    let builder = TableCreation::builder()
        .name(name.to_string())
        .schema(schema)
        .partition_spec_opt(partition_spec)
        .format_version(format_version)
        .properties(extra_properties);
    match location {
        Some(location) => builder.location(location).build(),
        None => builder.build(),
    }
}

/// Model: Grok 4.6 xHigh
fn iceberg_create_format_version(
    ctx: &SessionContext,
    requested: Option<&str>,
) -> Result<FormatVersion> {
    let allow_v3 = ctx
        .copied_config()
        .options()
        .entries()
        .into_iter()
        .find(|entry| entry.key == ALLOW_CREATE_FORMAT_VERSION_3_OPTION)
        .and_then(|entry| entry.value)
        .is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "true" | "1" | "yes"
            )
        });
    let Some(raw) = requested.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(FormatVersion::V2);
    };
    match raw {
        "2" => Ok(FormatVersion::V2),
        "3" if allow_v3 => Ok(FormatVersion::V3),
        "3" => Err(DataFusionError::NotImplemented(format!(
            "WITH 'format_version' = '3' is not enabled — set `{ALLOW_CREATE_FORMAT_VERSION_3_KEY}` \
             = true (v3 tables cannot yet do merge-on-read row-level writes; default create stays \
             format v2)"
        ))),
        other => Err(DataFusionError::NotImplemented(format!(
            "WITH 'format_version' = '{other}' is not supported (tables are created as Iceberg \
             format v2)"
        ))),
    }
}

/// Create first because the service assigns storage during create. Drop after any query, file-write,
/// or append-commit failure. A later catalog-refresh failure leaves the created table intact.
#[allow(clippy::too_many_arguments)] // placement + schema + the V3-2 format version travel together
async fn create_first_service_managed(
    cx: &EngineContext<'_>,
    target: &CreateTarget,
    properties: &TableProperties,
    schema: iceberg::spec::Schema,
    partition_spec: Option<UnboundPartitionSpec>,
    format_version: FormatVersion,
    query: Option<DataFrame>,
) -> Result<DataFrame> {
    let creation = iceberg_table_creation(
        &target.table,
        schema,
        partition_spec,
        format_version,
        properties.extra_properties.clone(),
        None,
        None,
    );
    let table = target
        .catalog
        .create_table(&target.namespace, creation)
        .await
        .map_err(iceberg_err)?;

    let write: Result<()> = async {
        if let Some(frame) = query {
            let stream = frame.execute_stream().await?;
            let data_files = write_stream(cx.ctx, &table, stream).await?;
            if !data_files.is_empty() {
                repark_iceberg::write::commit_append(&target.catalog, &table, data_files).await?;
            }
        }
        Ok(())
    }
    .await;

    if let Err(write_err) = write {
        return Err(match target.catalog.drop_table(&target.ident()).await {
            Ok(()) => DataFusionError::Execution(format!(
                "CREATE TABLE `{}` failed after the table was created (service-managed \
                 location); the incomplete table was dropped. Original error: {write_err}",
                target.full_name
            )),
            Err(drop_err) => DataFusionError::Execution(format!(
                "CREATE TABLE `{}` failed after the table was created (service-managed \
                 location), AND the abort drop ALSO failed — an incomplete table may remain. \
                 Original error: {write_err}; drop error: {drop_err}",
                target.full_name
            )),
        });
    }
    finish(cx.ctx, target).await
}

/// Stream a plan's batches into Iceberg data files, honouring the session's write concurrency.
async fn write_stream(
    ctx: &SessionContext,
    table: &iceberg::table::Table,
    stream: datafusion::physical_plan::SendableRecordBatchStream,
) -> Result<Vec<iceberg::spec::DataFile>> {
    let concurrency = repark_iceberg::write::concurrency_from_ctx(ctx);
    if table.metadata().default_partition_spec().is_unpartitioned() {
        repark_iceberg::write::write_data_files_from_stream_with_concurrency(
            table,
            stream,
            concurrency,
        )
        .await
    } else {
        repark_iceberg::write::write_partitioned_data_files_from_stream_with_concurrency(
            table,
            stream,
            concurrency,
        )
        .await
    }
}

/// Refresh the touched schema's name directory, then return an empty frame; refresh errors do not roll back.
async fn finish(ctx: &SessionContext, target: &CreateTarget) -> Result<DataFrame> {
    repark_iceberg::catalog::invalidate_catalog_namespaces(
        ctx,
        Arc::clone(&target.catalog),
        &target.catalog_name,
        &[&target.schema_name()],
    )
    .await?;
    ctx.read_empty()
}

// === Q15 routing ============================================================================

/// ===========================================================================================
/// Resolve a create target's leading segment against the registered Iceberg catalogs (Q15/G1).
/// ===========================================================================================
pub(crate) fn resolve_target(
    cx: &EngineContext<'_>,
    name: &ObjectName,
    form: &str,
) -> Result<CreateTarget> {
    let catalogs = cx.catalogs;
    let parts = name_parts(name);
    let full_name = name.to_string();
    let Some(leading) = parts.first() else {
        return Err(refuse_unqualified(cx.ctx, catalogs, &full_name, form));
    };
    if catalogs.is_read_only_catalog(leading) && catalogs.get(leading).is_none() {
        return Err(DataFusionError::Plan(
            crate::guards::read_only_catalog_message(leading, form),
        ));
    }
    let Some(catalog) = catalogs.get(leading) else {
        return Err(refuse_unqualified(cx.ctx, catalogs, &full_name, form));
    };
    let [_, namespace @ .., table] = parts.as_slice() else {
        return Err(refuse_unqualified(cx.ctx, catalogs, &full_name, form));
    };
    if namespace.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "{form} target `{full_name}` names catalog `{leading}` but no schema — write \
             `{leading}.<schema>.{table}`"
        )));
    }
    Ok(CreateTarget {
        catalog_name: leading.clone(),
        catalog: Arc::clone(catalog),
        namespace: NamespaceIdent::from_strs(namespace).map_err(iceberg_err)?,
        table: table.clone(),
        full_name,
    })
}

/// The Q15 refusal lists registered catalogs because the common cause is an unqualified target.
fn refuse_unqualified(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    full_name: &str,
    form: &str,
) -> DataFusionError {
    let registered: Vec<String> = ctx
        .catalog_names()
        .into_iter()
        .filter(|name| catalogs.get(name).is_some())
        .collect();
    let available = if registered.is_empty() {
        "none are registered on this session".to_string()
    } else {
        registered
            .iter()
            .map(|name| format!("`{name}`"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    DataFusionError::Plan(format!(
        "{form} target `{full_name}` is not a qualified Iceberg table name. Write \
         `<catalog>.<schema>.<table>`, where <catalog> is a registered Iceberg catalog \
         (registered here: {available}). This door does not create session-local in-memory \
         tables: an unqualified CREATE TABLE that appeared to succeed would vanish when the \
         session ends, so it refuses instead."
    ))
}

// === Clause refusals ========================================================================

/// Return the `WITH (…)` option list. Reject other option syntaxes.
fn with_options(create: &CreateTable, form: &str) -> Result<Vec<SqlOption>> {
    match &create.table_options {
        CreateTableOptions::None => Ok(Vec::new()),
        CreateTableOptions::With(options) => Ok(options.clone()),
        CreateTableOptions::TableProperties(_) => Err(DataFusionError::NotImplemented(format!(
            "{form}: TBLPROPERTIES is Spark syntax — set table properties with WITH (…) in this \
             door (see WITH (extra_properties = MAP(ARRAY[…], ARRAY[…])) for raw Iceberg keys)"
        ))),
        CreateTableOptions::Plain(_) | CreateTableOptions::Options(_) => {
            Err(DataFusionError::NotImplemented(format!(
                "{form}: only the WITH (…) property syntax is supported"
            )))
        }
    }
}

/// Refuse clauses that this door cannot apply, because each would otherwise be silently dropped.
fn refuse_unsupported_clauses(create: &CreateTable, form: &str) -> Result<()> {
    if create.temporary {
        return Err(DataFusionError::NotImplemented(format!(
            "{form}: TEMPORARY tables are not supported — omit TEMPORARY for a durable Iceberg \
             table, or create a temporary VIEW"
        )));
    }
    if create.external {
        return Err(DataFusionError::NotImplemented(format!(
            "{form}: EXTERNAL tables are not supported for Iceberg targets"
        )));
    }
    if create.transient || create.volatile {
        return Err(DataFusionError::NotImplemented(format!(
            "{form}: TRANSIENT / VOLATILE tables are not supported for Iceberg targets"
        )));
    }
    if create.location.is_some()
        || create
            .hive_formats
            .as_ref()
            .and_then(|formats| formats.location.as_deref())
            .is_some_and(|location| !location.trim().is_empty())
    {
        return Err(DataFusionError::NotImplemented(format!(
            "{form}: the bare LOCATION clause is not supported — set the location as a table \
             property: WITH (location = 's3://…')"
        )));
    }
    if let Some(formats) = create.hive_formats.as_ref()
        && (formats.row_format.is_some()
            || formats.serde_properties.is_some()
            || formats.storage.is_some())
    {
        return Err(DataFusionError::NotImplemented(format!(
            "{form}: Hive storage clauses (ROW FORMAT / SERDE / STORED AS) are not supported — \
             Iceberg tables written by this engine are Parquet"
        )));
    }
    if create.file_format.is_some() {
        return Err(DataFusionError::NotImplemented(format!(
            "{form}: the file-format clause is not supported — use WITH (format = 'PARQUET')"
        )));
    }
    // sqlparser stores `COMMENT '…'` on `create.comment` or inside an option.
    let commented_option = match &create.table_options {
        CreateTableOptions::None | CreateTableOptions::TableProperties(_) => false,
        CreateTableOptions::Plain(options)
        | CreateTableOptions::With(options)
        | CreateTableOptions::Options(options) => options
            .iter()
            .any(|option| matches!(option, SqlOption::Comment(_))),
    };
    if create.comment.is_some() || commented_option {
        return Err(DataFusionError::NotImplemented(format!(
            "{form}: table COMMENT is not supported yet"
        )));
    }
    if create.query.is_some() && !create.columns.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "{form}: a column list may not be combined with AS SELECT — the table's schema comes \
             from the SELECT"
        )));
    }
    if create.query.is_none() && create.columns.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "{form}: a CREATE TABLE without AS SELECT needs a column list"
        )));
    }
    Ok(())
}

// === Column-def schema derivation ===========================================================

/// Derive a column-def `CREATE TABLE` Arrow schema by planning a zero-row projection.
async fn column_def_schema(
    ctx: &SessionContext,
    create: &CreateTable,
    form: &str,
) -> Result<Arc<ArrowSchema>> {
    let projection = create
        .columns
        .iter()
        .map(|column| {
            format!(
                "CAST(NULL AS {}) AS \"{}\"",
                column.data_type,
                column.name.value.replace('"', "\"\"")
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let plan = ctx
        .state()
        .create_logical_plan(&format!("SELECT {projection}"))
        .await
        .map_err(|err| {
            DataFusionError::Plan(format!(
                "{form}: could not resolve the declared column types ({err})"
            ))
        })?;

    let fields = plan
        .schema()
        .fields()
        .iter()
        .zip(&create.columns)
        .map(|(field, column)| {
            let nullable = !column
                .options
                .iter()
                .any(|option| matches!(option.option, ColumnOption::NotNull));
            Field::new(field.name(), field.data_type().clone(), nullable)
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(ArrowSchema::new(fields)))
}

/// SQL / Iceberg precision this engine can honor on a column-def CREATE (microseconds).
const SUPPORTED_TIMESTAMP_PRECISION: u8 = 6;

/// DataFusion's default / `TIMESTAMP(9)` precision (nanoseconds).
const NANOSECOND_TIMESTAMP_PRECISION: u8 = 9;

/// ===========================================================================================
/// Refuse column-def timestamps the write path cannot honor (Arrow nanoseconds).
/// ===========================================================================================
fn refuse_nanosecond_timestamp_columns(schema: &ArrowSchema, form: &str) -> Result<()> {
    for field in schema.fields() {
        if matches!(
            field.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, _)
        ) {
            return Err(DataFusionError::Plan(format!(
                "{form}: column `{}` is TIMESTAMP with nanosecond precision ({ns}), which this \
                 engine cannot honor (`timestamp_ns` is not a supported write type). Supported \
                 precisions: {us} (microseconds). Declare TIMESTAMP({us}).",
                field.name(),
                ns = NANOSECOND_TIMESTAMP_PRECISION,
                us = SUPPORTED_TIMESTAMP_PRECISION,
            )));
        }
    }
    Ok(())
}

/// ===========================================================================================
/// Resolve one declared SQL type to its Iceberg type through the shared planner.
/// ===========================================================================================
pub(crate) async fn sql_type_to_iceberg(
    ctx: &SessionContext,
    data_type: &datafusion::sql::sqlparser::ast::DataType,
    form: &str,
) -> Result<iceberg::spec::Type> {
    let plan = ctx
        .state()
        .create_logical_plan(&format!("SELECT CAST(NULL AS {data_type}) AS c"))
        .await
        .map_err(|err| {
            DataFusionError::Plan(format!(
                "{form}: could not resolve type `{data_type}` ({err})"
            ))
        })?;
    let arrow_type = plan.schema().field(0).data_type().clone();
    let schema = ArrowSchema::new(vec![Field::new("c", arrow_type, true)]);
    let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&schema).map_err(iceberg_err)?;
    let field = iceberg_schema
        .as_struct()
        .fields()
        .first()
        .ok_or_else(|| DataFusionError::Plan(format!("{form}: type `{data_type}` is empty")))?;
    Ok(field.field_type.as_ref().clone())
}

#[cfg(test)]
mod tests;
