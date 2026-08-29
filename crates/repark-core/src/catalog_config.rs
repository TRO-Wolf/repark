//! Parse Spark and repark catalog configuration into typed registration specifications.
//!
//! Both prefixes share one normalized keyspace. Kind indicators resolve Glue, S3 Tables, memory,
//! or Postgres catalogs; implementation-only keys are consumed, properties pass through, and
//! ambiguous or incomplete blocks fail with key-only diagnostics.

use std::collections::{BTreeMap, HashMap};
use std::hash::BuildHasher;

use repark_common::{Error, Result};

/// The config-key prefix Spark uses for a per-catalog configuration block. Kept accepted
/// verbatim forever — it is the near-drop-in contract (an existing PySpark script's
/// `.config("spark.sql.catalog.…", …)` block must register unchanged).
const CATALOG_PREFIX: &str = "spark.sql.catalog.";

/// The repark-native spelling of the same block, accepted as a synonym for new code
/// (2026-07-12 naming decision). Both prefixes land in one keyspace; the same key configured
/// under both spellings with *different* values is a fail-loud conflict, never a silent pick.
const REPARK_CATALOG_PREFIX: &str = "repark.sql.catalog.";

/// The property carrying the catalog warehouse location (an `s3://` path for Glue, a local
/// directory for the in-memory catalog). Passed through to the builder verbatim.
pub(crate) const WAREHOUSE_PROP: &str = "warehouse";

/// The property the `repark-catalog` S3 Tables builder requires: the table-bucket ARN. Spark's S3
/// Tables convention passes this ARN as the catalog `warehouse`, so an absent `table_bucket_arn` is
/// filled from `warehouse` (an explicit `table_bucket_arn` always wins).
pub(crate) const TABLE_BUCKET_ARN_PROP: &str = "table_bucket_arn";

/// The Iceberg catalog implementation this catalog block resolves to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogKind {
    /// AWS Glue Data Catalog (the primary product surface).
    Glue,
    /// AWS S3 Tables catalog (the secondary product surface).
    S3Tables,
    /// The AWS-free in-memory catalog over a local-filesystem warehouse (`RePark` extension —
    /// local development and tests).
    Memory,
    /// PostgreSQL read catalog (DataFusion `CatalogProvider` of `TableProvider`s — **not** an
    /// Iceberg `Catalog`; never enters `CatalogRegistry` — P2 / PG3).
    Postgres,
}

/// ===========================================================================================
/// A single configured catalog, parsed from a `spark.sql.catalog.<name>.*` block: its registered
/// `name`, resolved `kind`, and the `props` to hand the matching `repark-catalog` builder
/// (`io-impl` dropped and the kind indicators consumed; `warehouse` and all other props kept).
///
/// [`Debug`] redacts values for secret-like property keys (C1-SEC-002) so a logged `CatalogSpec`
/// never leaks credentials. Keys are always named; only the values are replaced with `***`.
/// ===========================================================================================
#[derive(Clone, PartialEq, Eq)]
pub struct CatalogSpec {
    /// The catalog name (the `<name>` in `spark.sql.catalog.<name>`), used as the registration key.
    pub name: String,
    /// The resolved catalog implementation.
    pub kind: CatalogKind,
    /// Builder properties, passed through verbatim (`warehouse` included). May carry credentials —
    /// see the redacting [`Debug`] impl.
    pub props: HashMap<String, String>,
}

impl std::fmt::Debug for CatalogSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Sort keys so Debug is deterministic regardless of HashMap iteration order.
        let mut props: Vec<(&str, &str)> = self
            .props
            .iter()
            .map(|(key, value)| {
                let shown = if prop_key_is_secret(key) {
                    "***"
                } else {
                    value.as_str()
                };
                (key.as_str(), shown)
            })
            .collect();
        props.sort_by(|left, right| left.0.cmp(right.0));
        f.debug_struct("CatalogSpec")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("props", &props)
            .finish()
    }
}

/// Whether a catalog property key's **value** should be redacted in Debug output (C1-SEC-002).
///
/// Matches common AWS / secret key spellings case-insensitively. Key names are always shown;
/// only the associated values are replaced with `***`.
fn prop_key_is_secret(key: &str) -> bool {
    // Hyphens and dots → underscore so `basic.auth.user.info` / `s3.access-key-id` share needles
    // with snake_case (C2-SEC-002).
    let lower = key.to_ascii_lowercase().replace(['-', '.'], "_");
    // Underscores stripped so camelCase `accessKey` / `privateKey` / one-word `apikey` share
    // needles with snake_case (residual of C1-SEC-002 / C2-SEC-002).
    let compact = lower.replace('_', "");
    // Substring match covers `aws_secret_access_key`, `s3.access-key-id`, `session_token`, etc.
    // Hyphens normalized to underscores so OpenDAL / Spark spellings share one needle set (C2-SEC-002).
    lower.contains("aws_secret")
        || lower.contains("secret")
        || lower.contains("password")
        || lower.contains("token")
        || lower.contains("credential")
        || lower.contains("connection_string")
        || lower.ends_with("access_key_id")
        || lower.ends_with("access_key")
        || compact.contains("accesskey")
        || compact.contains("apikey")
        || compact.contains("privatekey")
        || compact == "bearer"
        || compact.ends_with("bearer")
        // Kafka / Spark JDBC often embed `user:password` under this key.
        || lower.contains("user_info")
        || compact.contains("userinfo")
        || lower == "key"
        // `.key` needle is unreachable after the dot→underscore fold above; `foo.key` → `foo_key`
        // is caught by the `_key` arm (review 2026-07-23).
        || lower.ends_with("_key") && !lower.contains("bucket") && !lower.contains("arn")
}

/// The in-progress state accumulated for one catalog name while scanning the config map.
#[derive(Default)]
struct Block {
    /// The kind resolved from a `catalog-impl` value, if that key was present.
    kind_from_impl: Option<CatalogKind>,
    /// The kind resolved from a `type` value, if that key was present.
    kind_from_type: Option<CatalogKind>,
    /// Passthrough builder properties (kind indicators consumed, `io-impl` dropped).
    props: HashMap<String, String>,
}

/// ===========================================================================================
/// Parse a Spark / repark config map into one [`CatalogSpec`] per configured catalog.
///
/// Both accepted prefixes normalize into one keyspace. Equal duplicates collapse; conflicting
/// values fail naming both keys without exposing secrets. Results are sorted by catalog name.
/// ===========================================================================================
///
/// # Errors
/// Returns [`Error::Config`] — naming the config key(s) at fault under both accepted
/// spellings where applicable — when a `catalog-impl` / `type` value is unrecognized, when a
/// catalog resolves to no kind, when `catalog-impl` and `type` disagree, when a `memory`
/// catalog omits its required `warehouse`, when an empty catalog name appears in a key, when
/// an S3 Tables ARN is missing or malformed, or when the same property is set under both
/// prefixes with different values.
pub fn parse_catalog_specs<S: BuildHasher>(
    config: &HashMap<String, String, S>,
) -> Result<Vec<CatalogSpec>> {
    let mut blocks: BTreeMap<String, Block> = BTreeMap::new();

    // Normalize the two accepted spellings (`spark.sql.catalog.*` — the drop-in contract — and
    // `repark.sql.catalog.*` — the native synonym) into one keyspace first, so the block
    // building below is spelling-blind and a cross-spelling duplicate is deterministic:
    // identical values collapse, different values fail loudly naming both keys (HashMap
    // iteration order must never decide which spelling wins).
    let mut normalized: BTreeMap<&str, (&String, &String)> = BTreeMap::new();
    for (key, value) in config {
        let Some(rest) = key
            .strip_prefix(CATALOG_PREFIX)
            .or_else(|| key.strip_prefix(REPARK_CATALOG_PREFIX))
        else {
            continue;
        };
        if let Some((prior_key, prior_value)) = normalized.get(rest) {
            if *prior_value != value {
                // Name keys only — never interpolate raw values (props can carry credentials).
                return Err(Error::Config(format!(
                    "conflicting catalog config: `{prior_key}` and `{key}` set different \
                     values for the same property"
                )));
            }
            continue;
        }
        normalized.insert(rest, (key, value));
    }

    for (rest, (source_key, value)) in normalized {
        match rest.split_once('.') {
            // `spark.sql.catalog.<name>` — Spark catalog class / short kind. Iceberg class names stay
            // inert (kind still from catalog-impl/type). `jdbc` / `postgres` / JDBCTableCatalog
            // set kind Postgres (PG3).
            None => {
                if rest.is_empty() {
                    return Err(Error::Config(format!(
                        "catalog config key `{source_key}` has an empty catalog name — expected \
                         `{CATALOG_PREFIX}<name>` or `{REPARK_CATALOG_PREFIX}<name>`"
                    )));
                }
                let block = blocks.entry(rest.to_string()).or_default();
                if let Some(kind) = kind_from_bare_catalog_value(value) {
                    block.kind_from_type = Some(kind);
                }
            }
            // `spark.sql.catalog.<name>.<prop>` — a catalog property.
            Some((name, prop)) if !name.is_empty() => {
                let block = blocks.entry(name.to_string()).or_default();
                apply_prop(block, name, prop, value)?;
            }
            // Empty catalog name: `spark.sql.catalog..warehouse` (double-dot) etc.
            Some((_, _)) => {
                return Err(Error::Config(format!(
                    "catalog config key `{source_key}` has an empty catalog name — expected \
                     `{CATALOG_PREFIX}<name>.<prop>` or `{REPARK_CATALOG_PREFIX}<name>.<prop>`"
                )));
            }
        }
    }

    blocks
        .into_iter()
        .map(|(name, block)| block.into_spec(name))
        .collect()
}

/// Render a catalog property key under both accepted spellings for fail-loud errors.
fn dual_catalog_key(name: &str, prop: &str) -> String {
    format!("{CATALOG_PREFIX}{name}.{prop} / {REPARK_CATALOG_PREFIX}{name}.{prop}")
}

/// True when `value` looks like an S3 Tables table-bucket ARN.
fn is_s3tables_arn_shape(value: &str) -> bool {
    value.trim().starts_with("arn:aws:s3tables:")
}

/// Fold one `<name>.<prop> = value` pair into `block`, resolving kind indicators, dropping
/// `io-impl`, and keeping every other property as passthrough.
fn apply_prop(block: &mut Block, name: &str, prop: &str, value: &str) -> Result<()> {
    match prop {
        "catalog-impl" => {
            block.kind_from_impl = Some(kind_from_catalog_impl(value).ok_or_else(|| {
                Error::Config(format!(
                    "{} has an unrecognized value '{value}' \
                     (expected a class ending in 'GlueCatalog', 'S3TablesCatalog', or \
                     'JDBCTableCatalog')",
                    dual_catalog_key(name, "catalog-impl")
                ))
            })?);
        }
        "type" => {
            block.kind_from_type = Some(kind_from_type(value).ok_or_else(|| {
                Error::Config(format!(
                    "{} has an unrecognized value '{value}' \
                     (expected 'glue', 's3tables', 'memory', 'postgres', or 'jdbc')",
                    dual_catalog_key(name, "type")
                ))
            })?);
        }
        // iceberg-rust FileIO is not pluggable by Java class name — the Spark io-impl is inert.
        "io-impl" => {}
        _ => {
            block.props.insert(prop.to_string(), value.to_string());
        }
    }
    Ok(())
}

/// Resolve a `catalog-impl` Java class name to a [`CatalogKind`] by its suffix.
fn kind_from_catalog_impl(value: &str) -> Option<CatalogKind> {
    let value = value.trim();
    if value.ends_with("GlueCatalog") {
        Some(CatalogKind::Glue)
    } else if value.ends_with("S3TablesCatalog") {
        Some(CatalogKind::S3Tables)
    } else if value.ends_with("JDBCTableCatalog") {
        Some(CatalogKind::Postgres)
    } else {
        None
    }
}

/// Resolve Spark's short-form `type` value to a [`CatalogKind`].
fn kind_from_type(value: &str) -> Option<CatalogKind> {
    match value.trim().to_ascii_lowercase().as_str() {
        "glue" => Some(CatalogKind::Glue),
        "s3tables" => Some(CatalogKind::S3Tables),
        "memory" => Some(CatalogKind::Memory),
        "postgres" | "postgresql" | "jdbc" => Some(CatalogKind::Postgres),
        _ => None,
    }
}

/// Bare `spark.sql.catalog.<name> = <value>` kind resolution (jdbc/postgres class spellings).
fn kind_from_bare_catalog_value(value: &str) -> Option<CatalogKind> {
    kind_from_type(value).or_else(|| kind_from_catalog_impl(value))
}

impl Block {
    /// Finalize the accumulated block into a [`CatalogSpec`], resolving the kind and enforcing the
    /// per-kind requirements (a resolvable, agreeing kind; a `warehouse` for `memory`).
    fn into_spec(self, name: String) -> Result<CatalogSpec> {
        let kind = match (self.kind_from_impl, self.kind_from_type) {
            (Some(from_impl), Some(from_type)) if from_impl != from_type => {
                return Err(Error::Config(format!(
                    "{} and {} resolve to different catalog kinds ({from_impl:?} vs \
                     {from_type:?}) — set only one, or make them agree",
                    dual_catalog_key(&name, "catalog-impl"),
                    dual_catalog_key(&name, "type"),
                )));
            }
            (Some(kind), _) | (_, Some(kind)) => kind,
            (None, None) => {
                return Err(Error::Config(format!(
                    "catalog '{name}' has no kind — set {} \
                     (e.g. org.apache.iceberg.aws.glue.GlueCatalog) or {} \
                     (glue / s3tables / memory)",
                    dual_catalog_key(&name, "catalog-impl"),
                    dual_catalog_key(&name, "type"),
                )));
            }
        };

        if kind == CatalogKind::Memory
            && self
                .props
                .get(WAREHOUSE_PROP)
                .is_none_or(|w| w.trim().is_empty())
        {
            return Err(Error::Config(format!(
                "memory catalog '{name}' requires a non-empty {}",
                dual_catalog_key(&name, WAREHOUSE_PROP)
            )));
        }
        if kind == CatalogKind::Postgres
            && self
                .props
                .get("url")
                .is_none_or(|url| url.trim().is_empty())
        {
            return Err(Error::Config(format!(
                "postgres catalog '{name}' requires a non-empty {} (url-only v1; no host/port/database split)",
                dual_catalog_key(&name, "url")
            )));
        }

        let mut props = self.props;
        if kind == CatalogKind::S3Tables {
            translate_s3tables_arn(&name, &mut props)?;
        }

        Ok(CatalogSpec { name, kind, props })
    }
}

/// Fill an S3 Tables catalog's required `table_bucket_arn` from its `warehouse` when the ARN is
/// absent — Spark's S3 Tables convention passes the ARN as the `warehouse`. An explicit
/// `table_bucket_arn` always wins (never overwritten). Both sources must have
/// `arn:aws:s3tables:` shape. Errors, naming both keys, when neither yields a usable ARN.
fn translate_s3tables_arn(name: &str, props: &mut HashMap<String, String>) -> Result<()> {
    let arn_key = dual_catalog_key(name, TABLE_BUCKET_ARN_PROP);
    let warehouse_key = dual_catalog_key(name, WAREHOUSE_PROP);

    if let Some(arn) = props.get(TABLE_BUCKET_ARN_PROP).map(String::as_str) {
        let arn = arn.trim();
        if !arn.is_empty() {
            if !is_s3tables_arn_shape(arn) {
                return Err(Error::Config(format!(
                    "s3tables catalog '{name}': `{arn_key}` must be an S3 Tables table-bucket \
                     ARN starting with 'arn:aws:s3tables:' (got a non-ARN value; do not pass \
                     an s3:// warehouse path as the ARN)"
                )));
            }
            return Ok(()); // Explicit ARN wins; leave `warehouse` as a harmless passthrough.
        }
    }
    match props.get(WAREHOUSE_PROP) {
        Some(warehouse) if !warehouse.trim().is_empty() => {
            if !is_s3tables_arn_shape(warehouse) {
                return Err(Error::Config(format!(
                    "s3tables catalog '{name}': `{warehouse_key}` (used as table-bucket ARN \
                     when `{arn_key}` is unset) must start with 'arn:aws:s3tables:' — a Glue-style \
                     s3:// warehouse path is not valid for S3 Tables"
                )));
            }
            let arn = warehouse.clone();
            props.insert(TABLE_BUCKET_ARN_PROP.to_string(), arn);
            Ok(())
        }
        _ => Err(Error::Config(format!(
            "s3tables catalog '{name}' requires a table-bucket ARN — set \
             `{arn_key}` or `{warehouse_key}` \
             (Spark's S3 Tables convention passes the ARN as the warehouse)"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The verbatim measured source publish job catalog block.
    fn measured_glue_block() -> HashMap<String, String> {
        HashMap::from([
            (
                "spark.sql.catalog.glue_alt".to_string(),
                "org.apache.iceberg.spark.SparkCatalog".to_string(),
            ),
            (
                "spark.sql.catalog.glue_alt.catalog-impl".to_string(),
                "org.apache.iceberg.aws.glue.GlueCatalog".to_string(),
            ),
            (
                "spark.sql.catalog.glue_alt.warehouse".to_string(),
                "s3://example-team-spark-iceberg-glue-v1/".to_string(),
            ),
            (
                "spark.sql.catalog.glue_alt.io-impl".to_string(),
                "org.apache.iceberg.aws.s3.S3FileIO".to_string(),
            ),
        ])
    }

    /// The measured block parses to one Glue catalog: `warehouse` passes through, `io-impl` and the
    /// consumed `catalog-impl` do not, and the bare `SparkCatalog` class key is tolerated.
    #[test]
    fn parses_the_measured_glue_block() {
        let specs = parse_catalog_specs(&measured_glue_block()).unwrap();
        assert_eq!(specs.len(), 1);
        let spec = &specs[0];
        assert_eq!(spec.name, "glue_alt");
        assert_eq!(spec.kind, CatalogKind::Glue);
        assert_eq!(
            spec.props.get(WAREHOUSE_PROP).map(String::as_str),
            Some("s3://example-team-spark-iceberg-glue-v1/"),
            "warehouse passes through verbatim"
        );
        assert!(
            !spec.props.contains_key("io-impl"),
            "io-impl is dropped (iceberg-rust FileIO is not pluggable by class name)"
        );
        assert!(
            !spec.props.contains_key("catalog-impl"),
            "the catalog-impl kind indicator is consumed, not passed through"
        );
    }

    /// The repark-native prefix parses identically to the Spark spelling (2026-07-12 naming
    /// decision): the measured block with every key re-prefixed `repark.sql.catalog.` produces
    /// the same spec.
    #[test]
    fn repark_prefix_parses_identically() {
        let repark_block: HashMap<String, String> = measured_glue_block()
            .into_iter()
            .map(|(key, value)| (key.replacen("spark.", "repark.", 1), value))
            .collect();
        let specs = parse_catalog_specs(&repark_block).unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].name, "glue_alt");
        assert_eq!(specs[0].kind, CatalogKind::Glue);
        assert_eq!(
            specs[0].props.get(WAREHOUSE_PROP).map(String::as_str),
            Some("s3://example-team-spark-iceberg-glue-v1/"),
        );
    }

    /// The two spellings share one keyspace: consistent duplicates collapse; a cross-spelling
    /// duplicate with *different* values fails loudly naming both keys — never a silent,
    /// iteration-order-dependent pick.
    #[test]
    fn cross_prefix_duplicates_merge_or_fail_loud() {
        let consistent = HashMap::from([
            ("spark.sql.catalog.c.type".to_string(), "memory".to_string()),
            (
                "repark.sql.catalog.c.type".to_string(),
                "memory".to_string(),
            ),
            (
                "repark.sql.catalog.c.warehouse".to_string(),
                "/tmp/wh".to_string(),
            ),
        ]);
        let specs = parse_catalog_specs(&consistent).unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].kind, CatalogKind::Memory);

        let conflicting = HashMap::from([
            (
                "spark.sql.catalog.c.warehouse".to_string(),
                "/tmp/a".to_string(),
            ),
            (
                "repark.sql.catalog.c.warehouse".to_string(),
                "/tmp/b".to_string(),
            ),
        ]);
        let err = parse_catalog_specs(&conflicting).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("spark.sql.catalog.c.warehouse")
                && message.contains("repark.sql.catalog.c.warehouse"),
            "the conflict error must name both spellings, got: {message}"
        );
        assert!(
            !message.contains("/tmp/a") && !message.contains("/tmp/b"),
            "conflict errors must not echo raw config values (credentials risk), got: {message}"
        );
    }

    /// Empty catalog names (`spark.sql.catalog.` / `spark.sql.catalog..prop`) fail loud naming
    /// the malformed key — never silently discarded.
    #[test]
    fn empty_catalog_name_fails_loud() {
        let bare = HashMap::from([("spark.sql.catalog.".to_string(), "x".to_string())]);
        let err = parse_catalog_specs(&bare).unwrap_err().to_string();
        assert!(
            err.contains("spark.sql.catalog.") && err.contains("empty catalog name"),
            "{err}"
        );

        let prop = HashMap::from([(
            "repark.sql.catalog..warehouse".to_string(),
            "s3://bucket/".to_string(),
        )]);
        let err = parse_catalog_specs(&prop).unwrap_err().to_string();
        assert!(
            err.contains("repark.sql.catalog..warehouse") && err.contains("empty catalog name"),
            "{err}"
        );
    }

    /// Spark's short-form `type` resolves each kind; `memory` requires a warehouse;
    /// `s3tables` requires an ARN-shaped warehouse / `table_bucket_arn`.
    #[test]
    fn type_short_forms_resolve_each_kind() {
        let glue = HashMap::from([
            ("spark.sql.catalog.c.type".to_string(), "glue".to_string()),
            (
                "spark.sql.catalog.c.warehouse".to_string(),
                "s3://bucket/wh".to_string(),
            ),
        ]);
        assert_eq!(
            parse_catalog_specs(&glue).unwrap()[0].kind,
            CatalogKind::Glue
        );

        let arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket";
        let s3tables = HashMap::from([
            (
                "spark.sql.catalog.c.type".to_string(),
                "s3tables".to_string(),
            ),
            ("spark.sql.catalog.c.warehouse".to_string(), arn.to_string()),
        ]);
        assert_eq!(
            parse_catalog_specs(&s3tables).unwrap()[0].kind,
            CatalogKind::S3Tables
        );

        let memory = HashMap::from([
            ("spark.sql.catalog.c.type".to_string(), "memory".to_string()),
            (
                "spark.sql.catalog.c.warehouse".to_string(),
                "/tmp/wh".to_string(),
            ),
        ]);
        assert_eq!(
            parse_catalog_specs(&memory).unwrap()[0].kind,
            CatalogKind::Memory
        );
    }

    /// The S3 Tables `catalog-impl` suffix resolves to the S3 Tables kind. An explicit
    /// `table_bucket_arn` passes through and wins.
    #[test]
    fn s3tables_catalog_impl_resolves() {
        let arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket";
        let config = HashMap::from([
            (
                "spark.sql.catalog.tb.catalog-impl".to_string(),
                "org.apache.iceberg.aws.s3tables.S3TablesCatalog".to_string(),
            ),
            (
                "spark.sql.catalog.tb.table_bucket_arn".to_string(),
                arn.to_string(),
            ),
        ]);
        let specs = parse_catalog_specs(&config).unwrap();
        assert_eq!(specs[0].kind, CatalogKind::S3Tables);
        assert_eq!(
            specs[0]
                .props
                .get(TABLE_BUCKET_ARN_PROP)
                .map(String::as_str),
            Some(arn)
        );
    }

    /// S3 Tables convention: the ARN passed as `warehouse` is carried into `table_bucket_arn` (the
    /// key the `repark-catalog` builder requires), while `warehouse` stays as a harmless passthrough.
    #[test]
    fn s3tables_warehouse_arn_is_carried_into_table_bucket_arn() {
        let arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/example-team";
        let config = HashMap::from([
            (
                "spark.sql.catalog.tb.type".to_string(),
                "s3tables".to_string(),
            ),
            (
                "spark.sql.catalog.tb.warehouse".to_string(),
                arn.to_string(),
            ),
        ]);
        let specs = parse_catalog_specs(&config).unwrap();
        assert_eq!(specs[0].kind, CatalogKind::S3Tables);
        assert_eq!(
            specs[0]
                .props
                .get(TABLE_BUCKET_ARN_PROP)
                .map(String::as_str),
            Some(arn),
            "warehouse ARN carried into table_bucket_arn"
        );
        assert_eq!(
            specs[0].props.get(WAREHOUSE_PROP).map(String::as_str),
            Some(arn),
            "warehouse remains as a passthrough"
        );
    }

    /// An explicit `table_bucket_arn` wins over a differing `warehouse` (no overwrite).
    #[test]
    fn s3tables_explicit_arn_wins_over_warehouse() {
        let explicit = "arn:aws:s3tables:us-east-1:123456789012:bucket/explicit";
        let config = HashMap::from([
            (
                "spark.sql.catalog.tb.type".to_string(),
                "s3tables".to_string(),
            ),
            (
                "spark.sql.catalog.tb.table_bucket_arn".to_string(),
                explicit.to_string(),
            ),
            (
                "spark.sql.catalog.tb.warehouse".to_string(),
                "s3://some-other-thing/".to_string(),
            ),
        ]);
        let specs = parse_catalog_specs(&config).unwrap();
        assert_eq!(
            specs[0]
                .props
                .get(TABLE_BUCKET_ARN_PROP)
                .map(String::as_str),
            Some(explicit),
            "the explicit ARN is not overwritten by the warehouse"
        );
    }

    /// An S3 Tables catalog with neither `table_bucket_arn` nor `warehouse` fails loud, naming both
    /// keys that would fix it.
    #[test]
    fn s3tables_without_any_arn_source_errors() {
        let config = HashMap::from([(
            "spark.sql.catalog.tb.type".to_string(),
            "s3tables".to_string(),
        )]);
        let err = parse_catalog_specs(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("s3tables catalog 'tb'"), "{msg}");
        assert!(
            msg.contains("spark.sql.catalog.tb.table_bucket_arn")
                && msg.contains("spark.sql.catalog.tb.warehouse"),
            "names both fixing keys: {msg}"
        );
    }

    /// A Glue-style `s3://` warehouse is not a valid S3 Tables ARN and fails loud at parse time.
    #[test]
    fn s3tables_warehouse_s3_uri_is_rejected() {
        let config = HashMap::from([
            (
                "spark.sql.catalog.tb.type".to_string(),
                "s3tables".to_string(),
            ),
            (
                "spark.sql.catalog.tb.warehouse".to_string(),
                "s3://example-team-spark-iceberg-glue-v1/".to_string(),
            ),
        ]);
        let msg = parse_catalog_specs(&config).unwrap_err().to_string();
        assert!(
            msg.contains("arn:aws:s3tables:") && msg.contains("warehouse"),
            "{msg}"
        );
    }

    /// An explicit non-ARN `table_bucket_arn` fails loud the same way.
    #[test]
    fn s3tables_explicit_non_arn_is_rejected() {
        let config = HashMap::from([
            (
                "repark.sql.catalog.tb.type".to_string(),
                "s3tables".to_string(),
            ),
            (
                "repark.sql.catalog.tb.table_bucket_arn".to_string(),
                "s3://not-an-arn/".to_string(),
            ),
        ]);
        let msg = parse_catalog_specs(&config).unwrap_err().to_string();
        assert!(
            msg.contains("arn:aws:s3tables:")
                && msg.contains("repark.sql.catalog.tb.table_bucket_arn"),
            "{msg}"
        );
    }

    /// A repark-only malformed `type` error mentions the repark spelling (via dual-key messages).
    #[test]
    fn repark_only_malformed_type_mentions_repark_spelling() {
        let config = HashMap::from([("repark.sql.catalog.x.type".to_string(), "hive".to_string())]);
        let msg = parse_catalog_specs(&config).unwrap_err().to_string();
        assert!(
            msg.contains("repark.sql.catalog.x.type"),
            "must mention repark spelling, got: {msg}"
        );
        assert!(msg.contains("spark.sql.catalog.x.type"), "{msg}");
    }

    /// An unrecognized `catalog-impl` value fails loud, naming the full key and the value.
    #[test]
    fn unknown_catalog_impl_names_the_key_and_value() {
        let config = HashMap::from([(
            "spark.sql.catalog.x.catalog-impl".to_string(),
            "com.example.MysteryCatalog".to_string(),
        )]);
        let err = parse_catalog_specs(&config).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("spark.sql.catalog.x.catalog-impl"),
            "names the key: {msg}"
        );
        assert!(
            msg.contains("com.example.MysteryCatalog"),
            "names the offending value: {msg}"
        );
    }

    /// An unrecognized `type` value fails loud the same way.
    #[test]
    fn unknown_type_names_the_key_and_value() {
        let config = HashMap::from([("spark.sql.catalog.x.type".to_string(), "hive".to_string())]);
        let err = parse_catalog_specs(&config).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("spark.sql.catalog.x.type"),
            "names the key: {msg}"
        );
        assert!(msg.contains("hive"), "names the value: {msg}");
    }

    /// A catalog block with props but no kind indicator names the key that would fix it.
    #[test]
    fn missing_kind_names_the_fixing_key() {
        let config = HashMap::from([(
            "spark.sql.catalog.glue_alt.warehouse".to_string(),
            "s3://bucket/".to_string(),
        )]);
        let err = parse_catalog_specs(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("glue_alt"), "names the catalog: {msg}");
        assert!(
            msg.contains("spark.sql.catalog.glue_alt.catalog-impl")
                && msg.contains("spark.sql.catalog.glue_alt.type"),
            "names both keys that would fix it: {msg}"
        );
    }

    /// The bare `SparkCatalog` class key alone (no kind indicator) is still a missing-kind error.
    #[test]
    fn bare_class_key_alone_is_missing_kind() {
        let config = HashMap::from([(
            "spark.sql.catalog.glue_alt".to_string(),
            "org.apache.iceberg.spark.SparkCatalog".to_string(),
        )]);
        let err = parse_catalog_specs(&config).unwrap_err();
        assert!(err.to_string().contains("no kind"), "{err}");
    }

    /// A `memory` catalog without a warehouse fails loud, naming the warehouse key.
    #[test]
    fn memory_without_warehouse_names_the_warehouse_key() {
        let config =
            HashMap::from([("spark.sql.catalog.m.type".to_string(), "memory".to_string())]);
        let err = parse_catalog_specs(&config).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("memory catalog 'm'"), "{msg}");
        assert!(msg.contains("spark.sql.catalog.m.warehouse"), "{msg}");
    }

    /// A blank warehouse is rejected the same way a missing one is.
    #[test]
    fn memory_with_blank_warehouse_is_rejected() {
        let config = HashMap::from([
            ("spark.sql.catalog.m.type".to_string(), "memory".to_string()),
            (
                "spark.sql.catalog.m.warehouse".to_string(),
                "   ".to_string(),
            ),
        ]);
        assert!(
            parse_catalog_specs(&config)
                .unwrap_err()
                .to_string()
                .contains("warehouse")
        );
    }

    /// `catalog-impl` and `type` that disagree is a conflict error naming both keys.
    #[test]
    fn conflicting_impl_and_type_errors() {
        let config = HashMap::from([
            (
                "spark.sql.catalog.c.catalog-impl".to_string(),
                "org.apache.iceberg.aws.glue.GlueCatalog".to_string(),
            ),
            (
                "spark.sql.catalog.c.type".to_string(),
                "s3tables".to_string(),
            ),
        ]);
        let err = parse_catalog_specs(&config).unwrap_err();
        assert!(err.to_string().contains("different catalog kinds"), "{err}");
    }

    /// Non-catalog `spark.*` keys (engine knobs, app name) produce no specs.
    #[test]
    fn non_catalog_keys_are_ignored() {
        let config = HashMap::from([
            ("spark.app.name".to_string(), "etl".to_string()),
            ("spark.sql.shuffle.partitions".to_string(), "8".to_string()),
            ("repark.memory.limit.gb".to_string(), "4".to_string()),
        ]);
        assert!(parse_catalog_specs(&config).unwrap().is_empty());
    }

    /// Two configured catalogs are both returned, sorted by name (deterministic order).
    #[test]
    fn multiple_catalogs_are_sorted_by_name() {
        let mut config = measured_glue_block();
        config.insert(
            "spark.sql.catalog.aaa.type".to_string(),
            "memory".to_string(),
        );
        config.insert(
            "spark.sql.catalog.aaa.warehouse".to_string(),
            "/tmp/wh".to_string(),
        );
        let specs = parse_catalog_specs(&config).unwrap();
        let names: Vec<&str> = specs.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["aaa", "glue_alt"]);
    }

    /// C1-SEC-002: Debug of a `CatalogSpec` redacts secret-like prop **values** while still
    /// naming the keys. Mutation-proof: if Debug reverts to derive(Debug), the secret appears.
    #[test]
    fn catalog_spec_debug_redacts_secret_prop_values() {
        let secret = "SUPER_SECRET_VALUE_do_not_leak";
        let spec = CatalogSpec {
            name: "glue_alt".to_string(),
            kind: CatalogKind::Glue,
            props: HashMap::from([
                ("warehouse".to_string(), "s3://bucket/wh".to_string()),
                ("aws_secret_access_key".to_string(), secret.to_string()),
                ("password".to_string(), secret.to_string()),
                ("session_token".to_string(), secret.to_string()),
                ("access_key_id".to_string(), secret.to_string()),
                ("token".to_string(), secret.to_string()),
                // C2-SEC-002: hyphenated OpenDAL / Spark spellings must redact too.
                ("s3.access-key-id".to_string(), secret.to_string()),
                ("s3.secret-access-key".to_string(), secret.to_string()),
                ("credential".to_string(), secret.to_string()),
                // camelCase / one-word spellings must redact too.
                ("accessKey".to_string(), secret.to_string()),
                ("apikey".to_string(), secret.to_string()),
                // camelCase privateKey + bearer (OAuth) must redact too.
                ("privateKey".to_string(), secret.to_string()),
                ("bearer".to_string(), secret.to_string()),
                // Kafka/JDBC user:password blob key.
                ("basic.auth.user.info".to_string(), secret.to_string()),
            ]),
        };
        let rendered = format!("{spec:?}");
        assert!(
            !rendered.contains(secret),
            "Debug must not contain the secret value: {rendered}"
        );
        assert!(
            rendered.contains("aws_secret_access_key")
                && rendered.contains("password")
                && rendered.contains("session_token")
                && rendered.contains("access_key_id")
                && rendered.contains("token")
                && rendered.contains("s3.access-key-id")
                && rendered.contains("credential")
                && rendered.contains("accessKey")
                && rendered.contains("apikey")
                && rendered.contains("privateKey")
                && rendered.contains("bearer")
                && rendered.contains("basic.auth.user.info"),
            "Debug must still name the secret keys: {rendered}"
        );
        assert!(
            rendered.contains("***"),
            "Debug must show redacted placeholders: {rendered}"
        );
        assert!(
            rendered.contains("s3://bucket/wh"),
            "non-secret warehouse value must remain visible: {rendered}"
        );
    }

    #[test]
    fn postgres_kind_from_type_and_bare_jdbc() {
        let config = HashMap::from([
            ("spark.sql.catalog.pg".to_string(), "jdbc".to_string()),
            (
                "spark.sql.catalog.pg.url".to_string(),
                "postgresql://localhost/db".to_string(),
            ),
            ("spark.sql.catalog.pg.user".to_string(), "u".to_string()),
            (
                "spark.sql.catalog.pg.password".to_string(),
                "s3cret".to_string(),
            ),
        ]);
        let specs = parse_catalog_specs(&config).unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].kind, CatalogKind::Postgres);
        assert_eq!(specs[0].name, "pg");
        let debug = format!("{:?}", specs[0]);
        assert!(!debug.contains("s3cret"), "leaked: {debug}");
        assert!(debug.contains("***"));
    }

    #[test]
    fn postgres_requires_url() {
        let config = HashMap::from([(
            "spark.sql.catalog.pg.type".to_string(),
            "postgres".to_string(),
        )]);
        let err = parse_catalog_specs(&config).unwrap_err();
        assert!(err.to_string().contains("url"), "{err}");
    }

    /// Acceptance matrix for memory, Glue, and S3 Tables kind mappings; no live AWS.
    #[test]
    fn i5_catalog_config_acceptance_matrix_ok() {
        let bare_memory = HashMap::from([
            ("spark.sql.catalog.m".to_string(), "memory".to_string()),
            (
                "spark.sql.catalog.m.warehouse".to_string(),
                "/tmp/wh".to_string(),
            ),
        ]);
        assert_eq!(
            parse_catalog_specs(&bare_memory).unwrap()[0].kind,
            CatalogKind::Memory
        );
        // repark-prefix synonym of bare `=memory`.
        let repark_bare_memory = HashMap::from([
            ("repark.sql.catalog.m".to_string(), "memory".to_string()),
            (
                "repark.sql.catalog.m.warehouse".to_string(),
                "/tmp/wh".to_string(),
            ),
        ]);
        assert_eq!(
            parse_catalog_specs(&repark_bare_memory).unwrap()[0].kind,
            CatalogKind::Memory
        );

        let spark_memory = HashMap::from([
            (
                "spark.sql.catalog.m".to_string(),
                "org.apache.iceberg.spark.SparkCatalog".to_string(),
            ),
            ("spark.sql.catalog.m.type".to_string(), "memory".to_string()),
            (
                "spark.sql.catalog.m.warehouse".to_string(),
                "/tmp/wh".to_string(),
            ),
        ]);
        assert_eq!(
            parse_catalog_specs(&spark_memory).unwrap()[0].kind,
            CatalogKind::Memory
        );

        let glue = parse_catalog_specs(&measured_glue_block()).unwrap();
        assert_eq!(glue[0].kind, CatalogKind::Glue);
        assert!(glue[0].props.contains_key(WAREHOUSE_PROP));

        let glue_type = HashMap::from([
            ("spark.sql.catalog.g.type".to_string(), "glue".to_string()),
            (
                "spark.sql.catalog.g.warehouse".to_string(),
                "s3://bucket/wh".to_string(),
            ),
        ]);
        assert_eq!(
            parse_catalog_specs(&glue_type).unwrap()[0].kind,
            CatalogKind::Glue
        );

        let arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket";
        let s3t_impl = HashMap::from([
            (
                "spark.sql.catalog.tb.catalog-impl".to_string(),
                "org.apache.iceberg.aws.s3tables.S3TablesCatalog".to_string(),
            ),
            (
                "spark.sql.catalog.tb.table_bucket_arn".to_string(),
                arn.to_string(),
            ),
        ]);
        assert_eq!(
            parse_catalog_specs(&s3t_impl).unwrap()[0].kind,
            CatalogKind::S3Tables
        );

        let s3t_type = HashMap::from([
            (
                "spark.sql.catalog.tb.type".to_string(),
                "s3tables".to_string(),
            ),
            (
                "spark.sql.catalog.tb.warehouse".to_string(),
                arn.to_string(),
            ),
        ]);
        let s3t_spec = &parse_catalog_specs(&s3t_type).unwrap()[0];
        assert_eq!(s3t_spec.kind, CatalogKind::S3Tables);
        assert_eq!(
            s3t_spec
                .props
                .get(TABLE_BUCKET_ARN_PROP)
                .map(String::as_str),
            Some(arn)
        );
    }

    /// Acceptance matrix for missing, conflicting, and unknown catalog kinds; config only.
    #[test]
    fn i5_catalog_config_acceptance_matrix_loud() {
        let bare_class = HashMap::from([(
            "spark.sql.catalog.x".to_string(),
            "org.apache.iceberg.spark.SparkCatalog".to_string(),
        )]);
        assert!(
            parse_catalog_specs(&bare_class)
                .unwrap_err()
                .to_string()
                .contains("no kind")
        );

        let mem_no_wh =
            HashMap::from([("spark.sql.catalog.m.type".to_string(), "memory".to_string())]);
        assert!(
            parse_catalog_specs(&mem_no_wh)
                .unwrap_err()
                .to_string()
                .contains("warehouse")
        );

        let conflict = HashMap::from([
            (
                "spark.sql.catalog.c.catalog-impl".to_string(),
                "org.apache.iceberg.aws.glue.GlueCatalog".to_string(),
            ),
            (
                "spark.sql.catalog.c.type".to_string(),
                "s3tables".to_string(),
            ),
        ]);
        assert!(
            parse_catalog_specs(&conflict)
                .unwrap_err()
                .to_string()
                .contains("different catalog kinds")
        );

        let unknown_impl = HashMap::from([(
            "spark.sql.catalog.x.catalog-impl".to_string(),
            "com.example.MysteryCatalog".to_string(),
        )]);
        assert!(
            parse_catalog_specs(&unknown_impl)
                .unwrap_err()
                .to_string()
                .contains("MysteryCatalog")
        );

        let unknown_type =
            HashMap::from([("spark.sql.catalog.x.type".to_string(), "hive".to_string())]);
        assert!(
            parse_catalog_specs(&unknown_type)
                .unwrap_err()
                .to_string()
                .contains("hive")
        );
    }
}
