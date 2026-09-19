use std::collections::HashMap;
use std::sync::Arc;

use iceberg::spec::{PrimitiveType, Type};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::ReparkSession;
use repark_iceberg::catalog::resolve_namespace_location;

use crate::BoxError;
use crate::bed::{self, CATALOG};
use crate::cli::{Outcome, boxed};
use crate::r3::{R3Verdict, StepSummary, r3_gate, table_footprint};
use crate::run::CatalogChoice;

pub const GLUE_CATALOG_PROP_WAREHOUSE: &str = "warehouse";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Create,
    Write,
}

impl Phase {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "create" => Ok(Self::Create),
            "write" => Ok(Self::Write),
            other => Err(format!("unknown phase `{other}` (create|write)")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteSetupOptions {
    pub catalog: CatalogChoice,
    pub props: Vec<(String, String)>,
    pub table: String,
    pub phase: Phase,
    pub files: usize,
    pub rows_per_file: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceRule {
    Located(String),
    Unlocated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateOutcome {
    Created,
    Existed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    Wrote,
    Skipped,
}

pub fn split_table(table: &str) -> Result<(String, String), BoxError> {
    match table.split_once('.') {
        Some((namespace, name))
            if !namespace.is_empty() && !name.is_empty() && !name.contains('.') =>
        {
            Ok((namespace.to_string(), name.to_string()))
        }
        _ => Err(boxed(format!(
            "--table needs <namespace>.<table>, got `{table}`"
        ))),
    }
}

pub fn namespace_rule(
    catalog: CatalogChoice,
    props: &[(String, String)],
    namespace: &str,
) -> Result<NamespaceRule, BoxError> {
    match catalog {
        CatalogChoice::Glue => {
            let warehouse = props
                .iter()
                .find(|(key, _)| key == GLUE_CATALOG_PROP_WAREHOUSE)
                .map(|(_, value)| value.trim_end_matches('/'))
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    boxed(format!(
                        "setup --catalog glue needs --prop {GLUE_CATALOG_PROP_WAREHOUSE}=<s3 uri>"
                    ))
                })?;
            Ok(NamespaceRule::Located(format!("{warehouse}/{namespace}")))
        }
        CatalogChoice::S3Tables => Ok(NamespaceRule::Unlocated),
        CatalogChoice::Local => Err(boxed(
            "setup --phase is for --catalog glue|s3tables; a local bed uses --warehouse",
        )),
    }
}

pub async fn register_remote_catalog(
    session: &ReparkSession,
    catalog: CatalogChoice,
    props: &[(String, String)],
) -> Result<(), BoxError> {
    let (kind, required) = match catalog {
        CatalogChoice::Glue => ("glue", GLUE_CATALOG_PROP_WAREHOUSE),
        CatalogChoice::S3Tables => ("s3tables", "table_bucket_arn"),
        CatalogChoice::Local => {
            return Err(boxed("a local catalog is registered from its warehouse"));
        }
    };
    if !props
        .iter()
        .any(|(key, value)| key == required && !value.trim().is_empty())
    {
        return Err(boxed(format!(
            "--catalog {kind} needs a non-empty `{required}` property (--prop {required}=…)"
        )));
    }
    let config = remote_catalog_config(kind, props);
    let (added, skipped) = session.register_late_configured_catalogs(&config).await?;
    if added != [CATALOG.to_string()] || !skipped.is_empty() {
        return Err(boxed(format!(
            "registering the bench catalog added {added:?} and skipped {skipped:?}"
        )));
    }
    Ok(())
}

#[must_use]
pub fn remote_catalog_config(kind: &str, props: &[(String, String)]) -> HashMap<String, String> {
    let mut config = HashMap::from([(
        format!("repark.sql.catalog.{CATALOG}.type"),
        kind.to_string(),
    )]);
    for (key, value) in props {
        config.insert(format!("repark.sql.catalog.{CATALOG}.{key}"), value.clone());
    }
    config
}

fn catalog_handle(session: &ReparkSession) -> Result<Arc<dyn Catalog>, BoxError> {
    session
        .catalogs_snapshot()
        .get(CATALOG)
        .cloned()
        .ok_or_else(|| boxed("the bench catalog is not registered"))
}

fn bed_columns() -> [(&'static str, Type); 5] {
    [
        ("id", Type::Primitive(PrimitiveType::Long)),
        ("ts", Type::Primitive(PrimitiveType::Timestamptz)),
        ("category", Type::Primitive(PrimitiveType::String)),
        ("value", Type::Primitive(PrimitiveType::Double)),
        ("payload", Type::Primitive(PrimitiveType::String)),
    ]
}

async fn require_bed_schema(
    handle: &Arc<dyn Catalog>,
    ident: &TableIdent,
    table: &str,
) -> Result<(), BoxError> {
    let loaded = handle.load_table(ident).await?;
    let schema = loaded.metadata().current_schema();
    let found: Vec<(String, Type)> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.name.clone(), (*field.field_type).clone()))
        .collect();
    let expected: Vec<(String, Type)> = bed_columns()
        .into_iter()
        .map(|(name, kind)| (name.to_string(), kind))
        .collect();
    if found != expected {
        let names: Vec<String> = found
            .iter()
            .map(|(name, kind)| format!("{name} {kind}"))
            .collect();
        return Err(boxed(format!(
            "{table} exists but its schema is not the bed schema (found: {})",
            names.join(", ")
        )));
    }
    Ok(())
}

async fn ensure_namespace(
    session: &ReparkSession,
    namespace: &str,
    rule: &NamespaceRule,
) -> Result<(), BoxError> {
    match rule {
        NamespaceRule::Unlocated => {
            session
                .create_namespace(CATALOG, namespace, HashMap::new())
                .await?;
        }
        NamespaceRule::Located(expected) => {
            session
                .create_namespace(
                    CATALOG,
                    namespace,
                    HashMap::from([("location".to_string(), expected.clone())]),
                )
                .await?;
            let stored = catalog_handle(session)?
                .get_namespace(&NamespaceIdent::new(namespace.to_string()))
                .await?;
            let actual = resolve_namespace_location(stored.properties())
                .map(|location| location.trim_end_matches('/').to_string());
            if actual.as_deref() != Some(expected.trim_end_matches('/')) {
                return Err(boxed(format!(
                    "namespace `{namespace}` has location {actual:?}, expected `{expected}`; \
                     refusing to adopt it (docs/tier2-aws.md §5)"
                )));
            }
        }
    }
    Ok(())
}

pub async fn create_phase(
    session: &ReparkSession,
    namespace: &str,
    name: &str,
    rule: &NamespaceRule,
) -> Result<CreateOutcome, BoxError> {
    ensure_namespace(session, namespace, rule).await?;
    let handle = catalog_handle(session)?;
    let ident = TableIdent::new(NamespaceIdent::new(namespace.to_string()), name.to_string());
    let table = format!("{CATALOG}.{namespace}.{name}");
    if handle.table_exists(&ident).await? {
        require_bed_schema(&handle, &ident, &table).await?;
        println!("create: {table} already exists with the bed schema; nothing created");
        return Ok(CreateOutcome::Existed);
    }
    session
        .sql(&bed::create_table_sql(&table))
        .await?
        .collect()
        .await?;
    println!("create: created the empty table {table}");
    Ok(CreateOutcome::Created)
}

pub async fn write_phase(
    session: &ReparkSession,
    table: &str,
    files: usize,
    rows_per_file: u64,
) -> Result<WriteOutcome, BoxError> {
    let expected_files = u64::try_from(files)?;
    let expected_rows = expected_files.saturating_mul(rows_per_file);
    let before = table_footprint(session, table).await?;
    if before.files == 0 {
        write_and_verify(session, table, files, rows_per_file).await?;
        return Ok(WriteOutcome::Wrote);
    }
    if before.data_files() == expected_files
        && before.delete_files == 0
        && before.data_rows == expected_rows
    {
        println!(
            "write: {table} already holds exactly {expected_files} data files, \
             {expected_rows} rows and no delete file; nothing written"
        );
        return Ok(WriteOutcome::Skipped);
    }
    Err(boxed(format!(
        "refusing to write {table}: it holds {} data files ({} rows) and {} delete files; the \
         bed writes only into an empty table and skips only at exactly {expected_files} data \
         files, {expected_rows} rows and no delete file (an owner drops a wedged bench table, \
         docs/tier2-aws.md)",
        before.data_files(),
        before.data_rows,
        before.delete_files
    )))
}

async fn write_and_verify(
    session: &ReparkSession,
    table: &str,
    files: usize,
    rows_per_file: u64,
) -> Result<(), BoxError> {
    let seconds = bed::write_files(session, table, files, rows_per_file).await?;
    let after = table_footprint(session, table).await?;
    let expected_files = u64::try_from(files)?;
    if after.data_files() != expected_files || after.delete_files != 0 {
        return Err(boxed(format!(
            "{table} holds {} data files and {} delete files after the write; the bed promises \
             exactly {expected_files} data files",
            after.data_files(),
            after.delete_files
        )));
    }
    println!("write: {expected_files} files written into {table} in {seconds:.1}s");
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhaseRequest {
    pub namespace: String,
    pub name: String,
    pub phase: Phase,
    pub rule: NamespaceRule,
    pub files: usize,
    pub rows_per_file: u64,
}

pub async fn run_phase(
    session: &ReparkSession,
    request: &PhaseRequest,
    summary: &StepSummary,
    size_override: Option<u64>,
) -> Result<Outcome, BoxError> {
    let table = format!("{CATALOG}.{}.{}", request.namespace, request.name);
    match request.phase {
        Phase::Create => {
            create_phase(session, &request.namespace, &request.name, &request.rule).await?;
        }
        Phase::Write => {
            write_phase(session, &table, request.files, request.rows_per_file).await?;
        }
    }
    let (_, verdict) = r3_gate(session, &table, summary, size_override).await?;
    Ok(match verdict {
        R3Verdict::WithinLimit => Outcome::Done,
        R3Verdict::Flagged => Outcome::SizeFlagged(Box::new(session.iceberg_io_stats())),
    })
}

pub async fn setup_remote(
    options: &RemoteSetupOptions,
    summary: &StepSummary,
) -> Result<Outcome, BoxError> {
    let (namespace, name) = split_table(&options.table)?;
    let rule = namespace_rule(options.catalog, &options.props, &namespace)?;
    let session = bed::spark_session()?;
    register_remote_catalog(&session, options.catalog, &options.props).await?;
    let request = PhaseRequest {
        namespace,
        name,
        phase: options.phase,
        rule,
        files: options.files,
        rows_per_file: options.rows_per_file,
    };
    run_phase(&session, &request, summary, None).await
}
