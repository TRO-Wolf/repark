//! CTAS staged create/replace, service-managed path, and create-clause refuse helpers.
//!
//! Extracted MOVE-ONLY from `lib.rs` (r25 T0 DataFusion-style reorg). Zero behavior change.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{CreateTable, CreateTableOptions, SqlOption};
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::io::FileIO;
use iceberg::spec::UnboundPartitionSpec;
use iceberg::transaction::StagedTableTransaction;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};

use repark_core::{CatalogRegistry, LocationPolicy};

use crate::catalog_ops::{
    catalog_handle, iceberg_err, name_parts, namespace_schema_name, reject_path_escape_ident,
    reregister,
};
use crate::normalize::{
    PartitionFieldSpec, PartitionedByElement, build_partition_spec, build_transform_field,
    property_value,
};
use crate::spark_ast;

/// A `CREATE TABLE … AS SELECT` resolved to a three-part `catalog.namespace.table` target.
pub(crate) struct Ctas {
    catalog: String,
    namespace: NamespaceIdent,
    table: String,
    /// The original three-part name, rendered for the `INSERT INTO` target.
    full_name: String,
    /// The `SELECT …` re-rendered to SQL.
    query_sql: String,
    if_not_exists: bool,
    or_replace: bool,
    /// `TBLPROPERTIES (...)` (e.g. `format-version`), applied at table creation.
    properties: HashMap<String, String>,
    /// The `PARTITIONED BY` fields, in clause order (empty = unpartitioned) — identity columns
    /// and non-identity transforms (`bucket`/`truncate`/`year[s]`/`month[s]`/`day[s]`/`hour[s]`).
    /// Extracted by the token pre-pass and validated in [`build_ctas`] (arity + width `> 0`);
    /// resolved against the derived iceberg schema in [`build_partition_spec`].
    partition_fields: Vec<PartitionFieldSpec>,
}

/// Extract a [`Ctas`] from a parsed `CREATE TABLE … AS SELECT` plus the `PARTITIONED BY` elements
/// the token pre-pass extracted. Errors if the target is not a three-part
/// `catalog.namespace.table` name (default-catalog resolution is a follow-up), or if the
/// partitioning uses an unsupported form:
/// - a transform call (`bucket(4, c)` / `days(ts)` / `truncate(3, c)` …) is parsed into a typed
///   [`PartitionFieldSpec`] (Spark parses these as named transforms — design record D2); an
///   UNKNOWN transform name or a non-positive bucket/truncate width is a loud typed error;
/// - a TYPED column (`PARTITIONED BY (c STRING)`) → the Spark parse error class "Partition column
///   types may not be specified in Create Table As Select (CTAS)" (Spark v3.5.1
///   `AstBuilder.scala` L3884-3888) — before this unit the typed form was silently DROPPED
///   (audit BUG-008/OTH-001);
/// - a multipart/nested reference (`s.f`) → `NotImplemented` (top-level output columns only, v1).
pub(crate) fn build_ctas(
    create: &CreateTable,
    partitioning: &[PartitionedByElement],
) -> Result<Ctas> {
    let query = create.query.as_ref().ok_or_else(|| {
        DataFusionError::Plan("build_ctas requires a CTAS (query must be Some)".into())
    })?;
    // TEMPORARY / EXTERNAL / TRANSIENT / VOLATILE must not silently create durable Iceberg
    // tables via CTAS (I5 octo C4-F2 — column-def path refused in C3-F1).
    if create.temporary {
        return Err(DataFusionError::NotImplemented(
            "CREATE TEMPORARY TABLE … AS SELECT is not supported for Iceberg tables yet — omit \
             TEMPORARY for a durable catalog table, or use a temp view (CREATE TEMP VIEW)"
                .into(),
        ));
    }
    if create.external {
        return Err(DataFusionError::NotImplemented(
            "CREATE EXTERNAL TABLE … AS SELECT is not supported for Iceberg CTAS yet".into(),
        ));
    }
    if create.transient || create.volatile {
        return Err(DataFusionError::NotImplemented(
            "CREATE TRANSIENT/VOLATILE TABLE … AS SELECT is not supported for Iceberg CTAS yet"
                .into(),
        ));
    }
    // LOCATION / Hive ROW FORMAT etc. must not be silently dropped (I5 octo C5-F1).
    refuse_unsupported_create_table_clauses(create, "CTAS")?;
    // An explicit column list (`CREATE TABLE t (a INT, …) AS SELECT …`) alongside the query is a
    // loud Spark parse error, NOT a silently-dropped schema. Spark's `AstBuilder` checks this
    // FIRST — before the partition-column-types check below — so a CTAS carrying BOTH a column
    // list and a typed `PARTITIONED BY` reports the schema error (Spark v3.5.1 `AstBuilder.scala`
    // L3878-3888; RTAS at L3947-3956). Before this guard `create.columns` was never read, so the
    // typed column list was silently ignored and the table took the SELECT's names/types instead
    // (the same fail-open class as the typed-`PARTITIONED BY` bug, U1 / BUG-008). A bare
    // name-only list (`(a, b)`) never reaches here — stock sqlparser's `parse_column_def` requires
    // a data type and parse-rejects it upstream (still fail-loud, a different message).
    if !create.columns.is_empty() {
        let statement = if create.or_replace {
            "Replace Table As Select (RTAS)"
        } else {
            "Create Table As Select (CTAS)"
        };
        return Err(DataFusionError::Plan(format!(
            "Schema may not be specified in a {statement} statement"
        )));
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
                    "Partition column types may not be specified in Create Table As Select \
                     (CTAS): partition column `{column}` carries a data type. Reference an \
                     output column of the SELECT instead, e.g. PARTITIONED BY ({column})"
                )));
            }
            PartitionedByElement::Nested(path) => {
                return Err(DataFusionError::NotImplemented(format!(
                    "CTAS PARTITIONED BY nested-field reference `{path}` is not supported yet — \
                     partition by a top-level output column of the SELECT (v1)"
                )));
            }
        }
    }
    let parts = name_parts(&create.name);
    let [catalog, namespace, table] = parts.as_slice() else {
        return Err(DataFusionError::Plan(format!(
            "CTAS target must be a three-part `catalog.namespace.table` name, got `{}`",
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
    // `format-version` is an Iceberg RESERVED property — iceberg-rust rejects it as a plain
    // table property at creation. The engine creates format v2 tables (the iceberg-rust
    // default), so requesting '2' is satisfied by consuming the key; any other version is a
    // deterministic reject, never a silently ignored request.
    if let Some(version) = properties.remove("format-version")
        && version.trim() != "2"
    {
        return Err(DataFusionError::NotImplemented(format!(
            "TBLPROPERTIES 'format-version' = '{version}' is not supported (tables are created \
             as Iceberg format v2)"
        )));
    }

    Ok(Ctas {
        catalog: catalog.clone(),
        namespace: NamespaceIdent::new(namespace.clone()),
        table: table.clone(),
        full_name: create.name.to_string(),
        query_sql: query.to_string(),
        if_not_exists: create.if_not_exists,
        or_replace: create.or_replace,
        properties,
        partition_fields,
    })
}

/// CTAS via fork staged create/replace (`ENGINE_CONTRACT` §8a).
///
/// 1. For the create path, resolve the table location + its scheme-selected `FileIO` **before**
///    running the `SELECT` (ADV-3) — a misconfigured target fails loud without executing the query.
/// 2. Stage a create or replace transaction (schema + partition spec are derived from the SELECT's
///    logical schema, which needs no execution).
/// 3. STREAM the `SELECT` (`execute_stream`) incrementally into the staged write path (WG-2,
///    audit SAF-002): batches are written as produced, peak memory O(batch × open writers) rather
///    than O(result). A mid-stream source failure aborts the write, drops the staged transaction
///    unpublished, and never touches the catalog pointer.
/// 4. `commit` publishes once. Failure between write and publish leaves create absent /
///    replace original current — no drop-then-insert hole, no orphan-cleanup path.
pub(crate) async fn execute_ctas(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    ctas: Ctas,
) -> Result<DataFrame> {
    // Use catalog_handle so postgres (read-only) targets get P11, not a bare "unknown catalog".
    let catalog = catalog_handle(catalogs, &ctas.catalog)?;
    let table_ident = TableIdent::new(ctas.namespace.clone(), ctas.table.clone());

    let existed = catalog
        .table_exists(&table_ident)
        .await
        .map_err(iceberg_err)?;
    if existed {
        if ctas.if_not_exists {
            return ctx.read_empty();
        } else if !ctas.or_replace {
            return Err(DataFusionError::Plan(format!(
                "table `{}` already exists",
                ctas.full_name
            )));
        }
    }

    // ADV-3: resolve the create-path location + scheme-selected FileIO BEFORE the SELECT, so a
    // misconfigured target (a location-less namespace on a RequireExplicitLocation catalog) fails
    // loud without executing the source query. After the early returns above, `existed` implies OR
    // REPLACE (a replace, which reuses the existing table's location); `!existed` is the create
    // path (OR REPLACE of a missing table is create — Spark). A create on a
    // [`LocationPolicy::ServiceManagedLocation`] catalog (S3 Tables) cannot stage — the service
    // assigns the location at create — so it routes to the create-first executor below; only the
    // cheap target validation (namespace exists, identifier hygiene) runs pre-SELECT.
    let mode = if existed {
        CtasMode::Replace
    } else if catalogs.location_policy(&ctas.catalog)
        == Some(LocationPolicy::ServiceManagedLocation)
    {
        validate_service_managed_target(catalog.as_ref(), &ctas).await?;
        CtasMode::ServiceManagedCreate
    } else {
        CtasMode::StagedCreate(resolve_create_plan(catalog.as_ref(), catalogs, &ctas).await?)
    };

    // Plan the SELECT via the Spark passthrough so schema + rows carry Spark expression
    // semantics (int `/` → double). Derive the iceberg schema and resolve the PARTITIONED BY
    // columns against it BEFORE the query executes (design record D6, the ADV-3 class): an
    // unknown partition column fails loud without running the source.
    let query = spark_ast::execute_passthrough(ctx, catalogs, &ctas.query_sql).await?;
    // The write path both DERIVES the table schema from this plan and, below, EXECUTES it —
    // and execution re-runs the analyzer at physical planning (a second pass). For most
    // expressions one analyze is already a fixpoint, but an integer `/` under a set operation is
    // not: the passthrough's single eager analyze rewrites each `int / int` branch to `Float64`
    // *after* `TypeCoercion` has already coerced the parent `UNION` against the pre-rewrite
    // `Int64` branches, so the once-analyzed `UNION` schema stays `Int64` while the executed data
    // is `Float64`. Deriving the write schema straight from `query.schema()` would then hand the
    // parquet writer an `Int64` field for a `Float64` array (`Field q has type Int64, array has
    // type Float64`). Re-analyze to the fixpoint — pass 2's `TypeCoercion` propagates `Float64` up
    // through the `UNION` — and rebuild the query from that plan, so the derived schema and the
    // executed batches come from the identical fixed-point plan by construction (Group L-write;
    // the analyzer's own idempotency makes the physical planner's third analyze a genuine no-op).
    let analyzed_plan =
        repark_functions::analyze_eagerly(&ctx.state(), query.logical_plan().clone())?;
    let query = ctx.execute_logical_plan(analyzed_plan).await?;
    let arrow_schema = Arc::new(query.schema().as_arrow().clone());
    let iceberg_schema =
        arrow_schema_to_schema_auto_assign_ids(arrow_schema.as_ref()).map_err(iceberg_err)?;
    let partition_spec = build_partition_spec(&iceberg_schema, &ctas.partition_fields)?;

    if matches!(mode, CtasMode::ServiceManagedCreate) {
        return execute_ctas_service_managed(
            ctx,
            catalog,
            &ctas,
            table_ident,
            iceberg_schema,
            partition_spec,
            query,
        )
        .await;
    }
    let staged = if let CtasMode::StagedCreate(plan) = mode {
        // Create: location + FileIO were resolved above the SELECT (ADV-3). The FileIO's backend
        // matches the location scheme (LocalFs for a local warehouse, S3 for an `s3://`/`s3a://`
        // warehouse) so `publish_create_table` reloads the metadata the stage wrote from the same
        // storage. The partition spec (`None` = unpartitioned) binds inside
        // `from_table_creation` (fork pin `fe30d7d4`, `table_metadata_builder.rs:167-197`).
        let creation = TableCreation::builder()
            .name(ctas.table.clone())
            .location(plan.location)
            .schema(iceberg_schema)
            .partition_spec_opt(partition_spec)
            .properties(ctas.properties.clone())
            .build();
        StagedTableTransaction::begin_create(plan.file_io, table_ident.clone(), creation)
            .await
            .map_err(iceberg_err)?
    } else {
        // Replace: stage against the existing table (its own location + FileIO). The NEW
        // definition is authoritative, Spark/Java `buildReplacement` semantics: a clause sets
        // the new default spec, no clause (`None`) resets the table to unpartitioned (the
        // fork's replace arm destructures `TableCreation.partition_spec` into
        // `add_default_partition_spec` — design record D4).
        let existing = catalog
            .load_table(&table_ident)
            .await
            .map_err(iceberg_err)?;
        let creation = TableCreation::builder()
            .name(ctas.table.clone())
            .schema(iceberg_schema)
            .partition_spec_opt(partition_spec)
            .properties(ctas.properties.clone())
            .build();
        StagedTableTransaction::begin_replace(&existing, creation)
            .await
            .map_err(iceberg_err)?
    };

    // STREAM the SELECT into the staged table (WG-2 bounded memory). Mid-stream error drops the
    // staged transaction unpublished.
    let stream = query.execute_stream().await?;
    let data_files = write_ctas_stream(ctx, staged.table(), stream).await?;
    staged
        .add_data_files(data_files)
        .commit(catalog.as_ref())
        .await
        .map_err(iceberg_err)?;

    let namespace = namespace_schema_name(&ctas.namespace);
    reregister(ctx, catalog.clone(), &ctas.catalog, &namespace).await?;
    ctx.read_empty()
}

/// ===========================================================================================
/// Stream a CTAS SELECT into Iceberg data files, honouring session write concurrency.
/// ===========================================================================================
///
/// # Errors
/// Propagates stream, conform, or writer errors from `repark-write`.
pub(crate) async fn write_ctas_stream(
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

/// The table location + scheme-selected `FileIO` for a staged CTAS / column-def **create**,
/// resolved *before* any data write (ADV-3) so a misconfigured target fails loud early.
pub(crate) struct CreatePlan {
    pub(crate) location: String,
    pub(crate) file_io: FileIO,
}

/// Resolve the [`CreatePlan`] for a staged CTAS create: the table location (the namespace `location`
/// property, or the [`LocationPolicy`] fallback) and a `FileIO` whose backend matches that location's
/// scheme — `LocalFs` for a local warehouse, the fork's OpenDAL S3 storage for an `s3://`/`s3a://`
/// (Glue) warehouse — so the create arm can write to and reload from the intended storage. The
/// `Catalog` does not expose its own `FileIO`, so the scheme is derived here from the resolved
/// location via [`repark_iceberg::catalog::file_io_for_location`] (threading the catalog's load-time
/// properties for S3 region/credentials).
///
/// # Errors
/// Returns a plan error if the location cannot be resolved (fail-loud policy) or carries an
/// unsupported storage scheme.
pub(crate) async fn resolve_create_plan(
    catalog: &dyn Catalog,
    catalogs: &CatalogRegistry,
    ctas: &Ctas,
) -> Result<CreatePlan> {
    resolve_create_plan_for(
        catalog,
        catalogs,
        &ctas.catalog,
        &ctas.namespace,
        &ctas.table,
        &ctas.full_name,
    )
    .await
}

/// Shared staged-create location + `FileIO` resolution for CTAS and column-def CREATE (I5).
///
/// # Errors
/// Location-policy / `FileIO` failures as [`DataFusionError`].
pub(crate) async fn resolve_create_plan_for(
    catalog: &dyn Catalog,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    namespace: &NamespaceIdent,
    table: &str,
    full_name: &str,
) -> Result<CreatePlan> {
    // The policy is registered alongside the handle; default to the strict (fail-loud) policy if it
    // is somehow absent, never the silent temp fallback.
    let location_policy = catalogs
        .location_policy(catalog_name)
        .unwrap_or(LocationPolicy::RequireExplicitLocation);
    let location = resolve_table_create_location(
        catalog,
        catalog_name,
        namespace,
        table,
        full_name,
        location_policy,
    )
    .await?;
    let file_io = repark_iceberg::catalog::file_io_for_location(&location, catalog.properties())?;
    Ok(CreatePlan { location, file_io })
}

/// Resolve an absolute table location for a staged CTAS create.
///
/// The namespace location property is always preferred, read via
/// `repark_iceberg::catalog::resolve_namespace_location` — `location` (the Java-canonical key `RePark`
/// documents) first, `location_uri` (the key the fork's Glue catalog fills from a real Glue
/// database's `locationUri` — audit BUG-001 / U2) as the deterministic fallback, so a
/// pre-existing Glue database resolves without any `RePark`-written property. When NEITHER key is
/// present the behaviour depends on the catalog's [`LocationPolicy`]:
/// - [`LocationPolicy::TempFallbackAllowed`] (in-memory / `LocalFs`): fall back to a process-temp
///   root so offline CTAS runs without a configured warehouse.
/// - [`LocationPolicy::RequireExplicitLocation`] (Glue / S3 Tables): **fail loud**. Silently placing
///   a real warehouse's data under `$TMPDIR` is the audit's BUG-002 / SEC-003 mis-placement hole; the
///   error names the namespace and points at BOTH ways to set the location — SQL
///   `CREATE NAMESPACE … LOCATION '…'` (or `WITH DBPROPERTIES ('location' = '…')`, WG-5) and the
///   programmatic `create_namespace(..., location=…)` (ADV-2 wording, updated by WG-5 now that SQL
///   `CREATE NAMESPACE` can carry the property).
///
/// # Errors
/// Returns a plan error if the namespace cannot be loaded, or if it has neither a `location` nor a
/// `location_uri` property and the catalog's policy is
/// [`LocationPolicy::RequireExplicitLocation`].
/// Resolve an absolute table location for staged create (CTAS or column-def CREATE).
///
/// # Errors
/// Namespace load / path-escape / missing location under strict policy.
pub(crate) async fn resolve_table_create_location(
    catalog: &dyn Catalog,
    catalog_name: &str,
    namespace_ident: &NamespaceIdent,
    table: &str,
    full_name: &str,
    policy: LocationPolicy,
) -> Result<String> {
    let namespace = catalog
        .get_namespace(namespace_ident)
        .await
        .map_err(iceberg_err)?;
    // Reject path-escape segments in identifiers before composing a warehouse path (C2-SEC-003).
    reject_path_escape_ident(catalog_name, "catalog")?;
    for part in namespace_ident.as_ref() {
        reject_path_escape_ident(part.as_str(), "namespace")?;
    }
    reject_path_escape_ident(table, "table")?;

    if let Some(prefix) =
        repark_iceberg::catalog::resolve_namespace_location(namespace.properties())
    {
        let prefix = prefix.trim_end_matches('/');
        return Ok(format!("{prefix}/{table}"));
    }
    match policy {
        // E-4 (phase-1 forced edit): the fallback root is resolved ONCE at catalog-registration
        // time (`ReparkSession::register_memory_catalog`) and carried on the policy — no
        // `std::env::temp_dir()` read at query time (v1 read the env here).
        LocationPolicy::TempFallbackAllowed { root } => {
            let mut path = root;
            path.push("repark_ctas");
            path.push(catalog_name);
            for part in namespace_ident.as_ref() {
                path.push(part.as_str());
            }
            path.push(table);
            Ok(path.to_string_lossy().into_owned())
        }
        // Routed to service-managed create-first before this resolver is consulted.
        LocationPolicy::ServiceManagedLocation => Err(DataFusionError::Internal(format!(
            "create location resolver reached for service-managed catalog `{catalog_name}` — \
             create-first routing bug"
        ))),
        LocationPolicy::RequireExplicitLocation => Err(DataFusionError::Plan(format!(
            "cannot resolve a storage location for create target `{full_name}`: namespace \
             `{namespace_ident}` in catalog `{catalog_name}` has no `location` (or \
             `location_uri`) property. Create the namespace with its warehouse path first, using \
             EITHER SQL — `CREATE NAMESPACE {catalog_name}.{namespace_ident} LOCATION \
             's3://.../{namespace_ident}'` (or `CREATE NAMESPACE {catalog_name}.{namespace_ident} \
             WITH DBPROPERTIES ('location' = 's3://.../{namespace_ident}')`) — OR the programmatic \
             API — `spark.create_namespace(\"{catalog_name}\", \"{namespace_ident}\", \
             location=\"s3://.../{namespace_ident}\")` — so RePark writes to the intended warehouse \
             instead of a temporary directory."
        ))),
    }
}

/// How this CTAS reaches its target, resolved BEFORE the SELECT runs (ADV-3).
pub(crate) enum CtasMode {
    /// Create on a catalog whose locations `RePark` chooses: stage at the resolved location, write,
    /// publish the catalog pointer last (full catalog-pointer atomicity).
    StagedCreate(CreatePlan),
    /// Create on a [`LocationPolicy::ServiceManagedLocation`] catalog (S3 Tables): the service
    /// assigns the location at create, so staging is impossible — create-first + append +
    /// drop-on-abort ([`execute_ctas_service_managed`]).
    ServiceManagedCreate,
    /// OR REPLACE of an existing table: stage against the existing table's own location.
    Replace,
}

/// Pre-SELECT validation for a service-managed CTAS create (the ADV-3 fail-early half that is
/// still possible when no location can be resolved up front): the target namespace must exist,
/// and the identifier-hygiene checks the staged path runs are applied for consistency (the
/// service composes its own paths, but a hostile identifier should fail here, not at AWS).
///
/// # Errors
/// Returns a plan error if the namespace cannot be loaded or an identifier fails hygiene.
pub(crate) async fn validate_service_managed_target(
    catalog: &dyn Catalog,
    ctas: &Ctas,
) -> Result<()> {
    catalog
        .get_namespace(&ctas.namespace)
        .await
        .map_err(iceberg_err)?;
    reject_path_escape_ident(ctas.catalog.as_str(), "catalog")?;
    for part in ctas.namespace.as_ref() {
        reject_path_escape_ident(part.as_str(), "namespace")?;
    }
    reject_path_escape_ident(ctas.table.as_str(), "table")
}

/// ===========================================================================================
/// CTAS create on a service-managed-location catalog (S3 Tables): **create-first + append +
/// drop-on-abort**, because staging is structurally impossible — S3 Tables namespaces carry no
/// `location`, the service generates each table's `warehouseLocation` at create, and the fork's
/// `S3TablesCatalog::create_table` REJECTS a caller-supplied location. Semantics mirror Spark's
/// non-staging catalog CTAS (`BasicStagedTable`): create the table through the catalog (location
/// deliberately ABSENT from the `TableCreation`), stream the SELECT into the created table's own
/// storage, commit the data as ONE fast-append (the sanctioned append commit,
/// `repark_iceberg::write::commit_append` — operation-id stamped), and on ANY failure after the create,
/// abort by dropping the just-created table (best-effort, loud when the drop itself fails).
/// An empty SELECT commits NO snapshot — the freshly created empty table is the correct result,
/// and a zero-file fast-append would stamp a pointless empty snapshot.
/// ===========================================================================================
///
/// # Errors
/// Returns the create error verbatim; a post-create failure is returned as an execution error
/// naming both the original failure and the abort outcome (dropped cleanly vs drop also failed).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_ctas_service_managed(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    ctas: &Ctas,
    table_ident: TableIdent,
    iceberg_schema: iceberg::spec::Schema,
    partition_spec: Option<UnboundPartitionSpec>,
    query: DataFrame,
) -> Result<DataFrame> {
    // Location deliberately not set: the service assigns it (a user-supplied one is rejected by
    // the fork's S3 Tables catalog).
    let creation = TableCreation::builder()
        .name(ctas.table.clone())
        .schema(iceberg_schema)
        .partition_spec_opt(partition_spec)
        .properties(ctas.properties.clone())
        .build();
    let table = catalog
        .create_table(&ctas.namespace, creation)
        .await
        .map_err(iceberg_err)?;

    // From here the table EXISTS in the catalog: any failure below aborts by dropping it.
    let write_result: Result<()> = async {
        let stream = query.execute_stream().await?;
        let data_files = write_ctas_stream(ctx, &table, stream).await?;
        if !data_files.is_empty() {
            repark_iceberg::write::commit_append(catalog, &table, data_files).await?;
        }
        Ok(())
    }
    .await;

    if let Err(write_err) = write_result {
        return Err(match catalog.drop_table(&table_ident).await {
            Ok(()) => DataFusionError::Execution(format!(
                "CTAS into `{}` failed after the table was created (service-managed location); \
                 the incomplete table was dropped (create-first abort). Original error: \
                 {write_err}",
                ctas.full_name
            )),
            Err(drop_err) => DataFusionError::Execution(format!(
                "CTAS into `{}` failed after the table was created (service-managed location), \
                 AND the abort `drop_table` ALSO failed — an incomplete table may remain. \
                 Original error: {write_err}; drop error: {drop_err}",
                ctas.full_name
            )),
        });
    }

    let namespace = namespace_schema_name(&ctas.namespace);
    reregister(ctx, Arc::clone(catalog), &ctas.catalog, &namespace).await?;
    ctx.read_empty()
}

/// Refuse LOCATION / Hive ROW FORMAT / SERDE / STORED AS clauses that sqlparser accepts but
/// `RePark` does not apply — silent drop would mis-place tables or imply Hive storage semantics
/// (I5 octo C4-F1 / C5-F1 / C5-F2). Used by column-def CREATE and CTAS.
pub(crate) fn refuse_unsupported_create_table_clauses(
    create: &CreateTable,
    form: &str,
) -> Result<()> {
    let hive_location = create
        .hive_formats
        .as_ref()
        .and_then(|formats| formats.location.as_deref());
    let bare_location = create.location.as_deref();
    let has_location = [hive_location, bare_location]
        .into_iter()
        .flatten()
        .any(|location| !location.trim().is_empty());
    if has_location {
        return Err(DataFusionError::NotImplemented(format!(
            "CREATE TABLE … LOCATION is not supported for Iceberg {form} yet — table location \
             is derived from the namespace warehouse (or service-managed catalog)"
        )));
    }
    if let Some(formats) = create.hive_formats.as_ref() {
        let has_hive_shape = formats.row_format.is_some()
            || formats.serde_properties.is_some()
            || formats.storage.is_some();
        if has_hive_shape {
            return Err(DataFusionError::NotImplemented(format!(
                "CREATE TABLE Hive storage clauses (ROW FORMAT / SERDE / STORED AS) are not \
                 supported for Iceberg {form} yet — Iceberg tables use parquet (or the \
                 write.format.default table property)"
            )));
        }
    }
    if create.file_format.is_some() {
        return Err(DataFusionError::NotImplemented(format!(
            "CREATE TABLE file-format clause is not supported for Iceberg {form} yet — use \
             TBLPROPERTIES ('write.format.default' = 'parquet') when needed"
        )));
    }
    // Table COMMENT is not mapped to Iceberg properties yet — refuse rather than silent drop
    // (I5 octo C6-F1). sqlparser parks `COMMENT '…'` in `table_options: Plain([Comment(…)])`
    // (and sometimes `create.comment`).
    if create.comment.is_some() {
        return Err(DataFusionError::NotImplemented(format!(
            "CREATE TABLE … COMMENT is not supported for Iceberg {form} yet — use TBLPROPERTIES \
             or ALTER TABLE when comment support lands"
        )));
    }
    match &create.table_options {
        CreateTableOptions::None | CreateTableOptions::TableProperties(_) => {}
        CreateTableOptions::Plain(options)
        | CreateTableOptions::With(options)
        | CreateTableOptions::Options(options) => {
            let has_comment = options
                .iter()
                .any(|option| matches!(option, SqlOption::Comment(_)));
            if has_comment {
                return Err(DataFusionError::NotImplemented(format!(
                    "CREATE TABLE … COMMENT is not supported for Iceberg {form} yet — use \
                     TBLPROPERTIES or ALTER TABLE when comment support lands"
                )));
            }
            if !options.is_empty() {
                return Err(DataFusionError::NotImplemented(format!(
                    "CREATE TABLE WITH/OPTIONS/plain options are not supported for Iceberg \
                     {form} yet — use TBLPROPERTIES for Iceberg table properties"
                )));
            }
        }
    }
    Ok(())
}
