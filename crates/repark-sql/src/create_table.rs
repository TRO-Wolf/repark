//! `CREATE TABLE` — CTAS and the column-def form — with the Q15 routing ruling.
//!
//! ## Q15 (graft G1): resolve the target, or refuse. Never a silent `MemTable`.
//!
//! The target's leading name segment is resolved against the session's registered Iceberg
//! catalogs. A registered catalog means a staged Iceberg create/replace. **Anything else — an
//! unqualified name, a two-part name, an unregistered catalog — refuses loud and asks for
//! qualification.**
//!
//! It is worth being explicit about what the refusal prevents, because "just fall through to
//! DataFusion" is the tempting alternative. DataFusion's own CTAS creates an in-memory table that
//! disappears when the session ends. Compose that with the error-path wrong-door sniff and a
//! dbt-style two-part name (`analytics.orders`), and a user's model "succeeds", reads back
//! correctly all session, and is simply gone tomorrow — with no error anywhere. That is silent
//! data loss produced by a helpful-looking fallthrough, so there is no fallthrough.
//!
//! Temp views never shadow a create target: resolution consults the catalog registry only.
//! Default-catalog resolution (making a two-part name mean `<default>.ns.t`) is a deliberate
//! future relaxation — it is non-breaking to add later, and impossible to take back.
//!
//! ## Location: the three-way `LocationPolicy`, same as the Spark door
//!
//! An explicit `WITH (location = …)` wins. Otherwise the namespace's `location` property is used.
//! Otherwise the catalog's [`LocationPolicy`] decides: `TempFallbackAllowed { root }` falls back
//! to the registration-time temp root (offline development), `RequireExplicitLocation` fails LOUD
//! (a real warehouse must never have data placed under `$TMPDIR`), and `ServiceManagedLocation`
//! (S3 Tables) cannot stage at all — the service assigns the location at create — so it routes to
//! create-first + append + drop-on-abort.

use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    ColumnOption, CreateTable, CreateTableOptions, ObjectName, SqlOption,
};
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::io::FileIO;
use iceberg::spec::UnboundPartitionSpec;
use iceberg::transaction::StagedTableTransaction;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};

use crate::partitioning::build_partition_spec;
use crate::properties::{TableProperties, parse_with_options};
use crate::schema_ddl::{iceberg_err, name_parts, reject_path_escape_ident};

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
    fn ident(&self) -> TableIdent {
        TableIdent::new(self.namespace.clone(), self.table.clone())
    }

    fn schema_name(&self) -> String {
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
///
/// # Errors
/// The Q15 routing refusal, any unsupported-clause refusal, a `WITH (…)` validation failure, or
/// any planning / iceberg / execution error from the staged create.
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

    // Derive the table's Arrow schema. CTAS derives it from the SELECT's logical plan (no
    // execution needed); the column-def form derives it from the declared columns.
    let (arrow_schema, query) = match create.query.as_ref() {
        Some(query) => {
            let frame = cx.ctx.sql(&query.to_string()).await?;
            let schema = Arc::new(frame.schema().as_arrow().clone());
            (schema, Some(frame))
        }
        None => (column_def_schema(cx.ctx, create, form).await?, None),
    };
    let iceberg_schema =
        arrow_schema_to_schema_auto_assign_ids(arrow_schema.as_ref()).map_err(iceberg_err)?;
    let partition_spec = build_partition_spec(&iceberg_schema, &properties.partitioning)?;

    // Resolve WHERE the table goes BEFORE running the SELECT: a misconfigured target must fail
    // loud without burning the source query.
    let placement = resolve_placement(&target, &properties, cx.catalogs, existed).await?;

    match placement {
        Placement::ServiceManaged => {
            create_first_service_managed(
                cx,
                &target,
                &properties,
                iceberg_schema,
                partition_spec,
                query,
            )
            .await
        }
        Placement::StagedReplace | Placement::StagedCreate { .. } => {
            let staged = if let Placement::StagedCreate { location, file_io } = placement {
                let creation = TableCreation::builder()
                    .name(target.table.clone())
                    .location(location)
                    .schema(iceberg_schema)
                    .partition_spec_opt(partition_spec)
                    .properties(properties.extra_properties.clone())
                    .build();
                StagedTableTransaction::begin_create(*file_io, target.ident(), creation)
                    .await
                    .map_err(iceberg_err)?
            } else {
                // Replace stages against the existing table: it keeps its own location and
                // FileIO, and the NEW definition is authoritative (Spark/Java
                // `buildReplacement` semantics — no clause resets the table to unpartitioned).
                let existing = target
                    .catalog
                    .load_table(&target.ident())
                    .await
                    .map_err(iceberg_err)?;
                let creation = TableCreation::builder()
                    .name(target.table.clone())
                    .schema(iceberg_schema)
                    .partition_spec_opt(partition_spec)
                    .properties(properties.extra_properties.clone())
                    .build();
                StagedTableTransaction::begin_replace(&existing, creation)
                    .await
                    .map_err(iceberg_err)?
            };

            // STREAM the SELECT into the staged table: peak memory is O(batch × open writers),
            // not O(result), and a mid-stream failure drops the staged transaction unpublished —
            // the catalog pointer is never touched. One `commit` publishes.
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
            finish(cx.ctx, &target).await
        }
    }
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

/// Resolve the create placement. A REPLACE reuses the existing table's own location, so it needs
/// no resolution at all (the staged replace reads it from the loaded table).
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
        // The cheap half of fail-early still applies: the namespace must exist and identifiers
        // must be clean before anything is sent to the service.
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

/// Create-first on a service-managed-location catalog (S3 Tables): the service generates the
/// table's storage at create, and rejects a caller-supplied location, so a staged
/// "pick location → write → register" is structurally impossible. Create, stream into the created
/// table, commit ONE append, and on ANY post-create failure drop the table (Spark's non-staging
/// `BasicStagedTable.abortStagedChanges`). An empty SELECT commits no snapshot — the created empty
/// table IS the correct result.
async fn create_first_service_managed(
    cx: &EngineContext<'_>,
    target: &CreateTarget,
    properties: &TableProperties,
    schema: iceberg::spec::Schema,
    partition_spec: Option<UnboundPartitionSpec>,
    query: Option<DataFrame>,
) -> Result<DataFrame> {
    let creation = TableCreation::builder()
        .name(target.table.clone())
        .schema(schema)
        .partition_spec_opt(partition_spec)
        .properties(properties.extra_properties.clone())
        .build();
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

/// Refresh the DataFusion name directory for the touched schema, then return an empty frame
/// (DDL has no rows — the same shape the Spark door returns).
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
///
/// # Errors
/// A loud refusal requiring qualification for anything that is not a three-part name whose
/// leading segment is a registered Iceberg catalog.
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

/// The Q15 refusal. It lists the registered catalogs because the overwhelmingly common cause is
/// a name that is one segment short, and the fix is mechanical once the user can see the names.
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

/// The `WITH (…)` option list, or an empty list. Any OTHER option syntax (Hive `OPTIONS`, plain
/// options, Spark `TBLPROPERTIES`) refuses rather than being dropped.
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

/// Refuse the clauses sqlparser accepts but this door does not apply. Every one of these would
/// otherwise be SILENTLY DROPPED, which is the failure mode worth the most refusals: a table that
/// exists but does not match what was asked for is far worse than a statement that failed.
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
    // sqlparser parks `COMMENT '…'` on `create.comment` for some shapes and inside
    // `table_options` for others, so both are checked — a comment that reached the table
    // silently would be a lie about what the table declares.
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

/// Derive the Arrow schema of a column-def `CREATE TABLE` by planning a zero-row projection of
/// `CAST(NULL AS <declared type>)`.
///
/// Deliberately NOT a hand-written sqlparser-type → Arrow-type table: DataFusion already owns
/// that mapping, and a second copy of it in this door would drift from the one the rest of the
/// engine plans with. Nullability comes from the column's own `NOT NULL` option, since the cast
/// expression is always nullable.
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

#[cfg(test)]
mod tests;
