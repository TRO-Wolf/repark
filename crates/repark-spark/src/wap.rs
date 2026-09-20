use std::any::Any;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::Arc;

use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};
use iceberg::table::Table;
use iceberg_datafusion::IcebergStaticTableProvider;
use repark_core::CatalogRegistry;
use repark_core::time_travel::next_temp_view_name;

use crate::catalog_ops::iceberg_err;
use crate::time_travel::PinnedViews;

pub const WAP_BRANCH_KEY: &str = "spark.wap.branch";
pub const WAP_ID_KEY: &str = "spark.wap.id";
pub const WAP_ENABLED_PROPERTY: &str = "write.wap.enabled";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WapSessionConfig {
    pub branch: Option<String>,
    pub id: Option<String>,
}

impl WapSessionConfig {
    #[must_use]
    pub fn value_for(&self, key: &str) -> Option<&String> {
        match key {
            WAP_BRANCH_KEY => self.branch.as_ref(),
            WAP_ID_KEY => self.id.as_ref(),
            _ => None,
        }
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn set_value(&mut self, key: &str, value: Option<String>) -> Result<()> {
        match key {
            WAP_BRANCH_KEY => self.branch = value,
            WAP_ID_KEY => self.id = value,
            _ => {
                return Err(DataFusionError::Configuration(format!(
                    "{key:?} is not a WAP session conf (served: {WAP_BRANCH_KEY:?}, {WAP_ID_KEY:?})"
                )));
            }
        }
        Ok(())
    }
}

impl ConfigExtension for WapSessionConfig {
    const PREFIX: &'static str = "repark.wap";
}

impl ExtensionOptions for WapSessionConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option: the audit branch is set with \
             `{WAP_BRANCH_KEY}` on the session builder; change it at runtime with \
             `SET {WAP_BRANCH_KEY}`",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

#[must_use]
pub fn is_wap_session_key(key: &str) -> bool {
    key == WAP_BRANCH_KEY || key == WAP_ID_KEY
}

#[must_use]
pub fn parse_runtime_wap_value(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[must_use]
pub fn wap_from_config_map<S>(config: &HashMap<String, String, S>) -> WapSessionConfig
where
    S: BuildHasher,
{
    WapSessionConfig {
        branch: config
            .get(WAP_BRANCH_KEY)
            .and_then(|raw| parse_runtime_wap_value(raw)),
        id: config
            .get(WAP_ID_KEY)
            .and_then(|raw| parse_runtime_wap_value(raw)),
    }
}

#[must_use]
pub fn with_wap_session_config(config: SessionConfig, wap: WapSessionConfig) -> SessionConfig {
    config.with_option_extension(wap)
}

#[must_use]
pub fn wap_from_options(options: &ConfigOptions) -> WapSessionConfig {
    options
        .extensions
        .get::<WapSessionConfig>()
        .cloned()
        .unwrap_or_default()
}

pub(crate) fn session_wap(ctx: &SessionContext) -> WapSessionConfig {
    wap_from_options(ctx.state().config().options())
}

#[must_use]
pub fn both_wap_keys_message(id: &str, branch: &str) -> String {
    format!("Cannot set both WAP ID and branch, but got ID [{id}] and branch [{branch}]")
}

pub(crate) fn refuse_both_wap_keys(wap: &WapSessionConfig) -> Result<()> {
    match (wap.id.as_deref(), wap.branch.as_deref()) {
        (Some(id), Some(branch)) => Err(repark_core::illegal_argument_error(
            both_wap_keys_message(id, branch),
        )),
        _ => Ok(()),
    }
}

#[must_use]
pub(crate) fn table_wap_enabled(table: &Table) -> bool {
    table
        .metadata()
        .properties()
        .get(WAP_ENABLED_PROPERTY)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

pub(crate) fn wap_branch_for_table(
    wap: &WapSessionConfig,
    table: &Table,
) -> Result<Option<String>> {
    let Some(branch) = wap.branch.as_ref() else {
        return Ok(None);
    };
    if !table_wap_enabled(table) {
        return Ok(None);
    }
    refuse_both_wap_keys(wap)?;
    Ok(Some(branch.clone()))
}

struct ReadRelation {
    start: usize,
    end: usize,
    parts: Vec<String>,
}

pub(crate) async fn apply_wap_read_redirect(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
    pinned: &mut PinnedViews,
) -> Result<Option<String>> {
    let wap = session_wap(ctx);
    if wap.branch.is_none() {
        return Ok(None);
    }
    let Ok(tokens) = Tokenizer::new(&DatabricksDialect {}, sql).tokenize() else {
        return Ok(None);
    };
    let relations = find_read_relations(&tokens);
    if relations.is_empty() {
        return Ok(None);
    }
    let mut tokens = tokens;
    let mut rewrote = false;
    for relation in relations.into_iter().rev() {
        let qualified = crate::write_to_branch::qualify_table_parts(ctx, relation.parts);
        let Some(snapshot_id) = branch_read_snapshot(catalogs, &qualified, &wap).await? else {
            continue;
        };
        let table = load_registry_table(catalogs, &qualified)
            .await
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "wap read redirect lost table {} between resolution and scan",
                    qualified.join(".")
                ))
            })?;
        let provider = IcebergStaticTableProvider::try_new_from_table_snapshot(table, snapshot_id)
            .await
            .map_err(iceberg_err)?;
        let temp_name = next_temp_view_name();
        let _ = ctx.deregister_table(temp_name.as_str());
        pinned.record(temp_name.clone());
        ctx.register_table(temp_name.as_str(), Arc::new(provider))
            .map_err(|error| {
                DataFusionError::Plan(format!(
                    "failed to register wap-branch temp view {temp_name}: {error}"
                ))
            })?;
        let replacement = Token::Word(Word {
            value: temp_name,
            quote_style: None,
            keyword: datafusion::sql::sqlparser::keywords::Keyword::NoKeyword,
        });
        tokens.splice(relation.start..relation.end, std::iter::once(replacement));
        rewrote = true;
    }
    if !rewrote {
        return Ok(None);
    }
    Ok(Some(
        tokens.iter().map(ToString::to_string).collect::<String>(),
    ))
}

async fn branch_read_snapshot(
    catalogs: &CatalogRegistry,
    qualified: &[String],
    wap: &WapSessionConfig,
) -> Result<Option<i64>> {
    let Some(table) = load_registry_table(catalogs, qualified).await else {
        return Ok(None);
    };
    let Some(branch) = wap_branch_for_table(wap, &table)? else {
        return Ok(None);
    };
    let metadata = table.metadata();
    Ok(metadata
        .snapshot_for_ref(&branch)
        .map(|snapshot| snapshot.snapshot_id())
        .or_else(|| metadata.current_snapshot_id()))
}

async fn load_registry_table(catalogs: &CatalogRegistry, qualified: &[String]) -> Option<Table> {
    let [catalog_name, namespace, table_name] = qualified else {
        return None;
    };
    let catalog = catalogs.get(catalog_name)?;
    let ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(namespace.clone()),
        table_name.clone(),
    );
    catalog.load_table(&ident).await.ok()
}

fn find_read_relations(tokens: &[Token]) -> Vec<ReadRelation> {
    let significant: Vec<(usize, &Token)> = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token, Token::Whitespace(_) | Token::EOF))
        .collect();
    let write_target = crate::write_to_branch::find_write_target_range(tokens);
    let mut relations = Vec::new();
    let mut index = 0usize;
    while index < significant.len() {
        if !opens_relation(&significant, index) {
            index += 1;
            continue;
        }
        let name_start = index + 1;
        let Some(name_end) = dotted_name_end(&significant, name_start) else {
            index += 1;
            continue;
        };
        index = name_end;
        let parts = collect_parts(&significant[name_start..name_end]);
        if parts.len() > 3 || !is_plain_table_name(&parts) {
            continue;
        }
        if glued_metadata_suffix(tokens, significant[name_end - 1].0) {
            continue;
        }
        if followed_by_time_travel(&significant, name_end) {
            continue;
        }
        let start = significant[name_start].0;
        let end = significant[name_end - 1].0 + 1;
        if write_target
            .is_some_and(|(target_start, target_end)| start < target_end && target_start < end)
        {
            continue;
        }
        relations.push(ReadRelation { start, end, parts });
    }
    relations
}

fn glued_metadata_suffix(tokens: &[Token], last_name_token: usize) -> bool {
    matches!(tokens.get(last_name_token + 1), Some(Token::Placeholder(_)))
}

fn opens_relation(significant: &[(usize, &Token)], index: usize) -> bool {
    matches!(
        significant.get(index).map(|(_, token)| *token),
        Some(Token::Word(word))
            if word.value.eq_ignore_ascii_case("FROM")
                || word.value.eq_ignore_ascii_case("JOIN")
                || word.value.eq_ignore_ascii_case("USING")
    )
}

fn is_plain_table_name(parts: &[String]) -> bool {
    let Some(last) = parts.last() else {
        return false;
    };
    if last.contains('$') || crate::metadata_tables::is_metadata_table_name(last) {
        return false;
    }
    let lowered = last.to_ascii_lowercase();
    if parts.len() >= 3 && (lowered.starts_with("branch_") || lowered.starts_with("tag_")) {
        return false;
    }
    true
}

fn followed_by_time_travel(significant: &[(usize, &Token)], name_end: usize) -> bool {
    let mut cursor = name_end;
    if word_at(significant, cursor).is_some_and(|word| word.eq_ignore_ascii_case("FOR")) {
        cursor += 1;
    }
    let is_kind = word_at(significant, cursor).is_some_and(|word| {
        matches!(
            word.to_ascii_uppercase().as_str(),
            "VERSION" | "TIMESTAMP" | "SYSTEM_VERSION" | "SYSTEM_TIME"
        )
    });
    is_kind
        && word_at(significant, cursor + 1).is_some_and(|word| word.eq_ignore_ascii_case("AS"))
        && word_at(significant, cursor + 2).is_some_and(|word| word.eq_ignore_ascii_case("OF"))
}

fn word_at<'a>(significant: &[(usize, &'a Token)], index: usize) -> Option<&'a str> {
    match significant.get(index).map(|(_, token)| *token) {
        Some(Token::Word(word)) => Some(word.value.as_str()),
        _ => None,
    }
}

fn dotted_name_end(significant: &[(usize, &Token)], start: usize) -> Option<usize> {
    if !is_ident_token(significant.get(start)?.1) {
        return None;
    }
    let mut end = start + 1;
    while matches!(
        significant.get(end).map(|(_, token)| *token),
        Some(Token::Period)
    ) && significant
        .get(end + 1)
        .is_some_and(|(_, token)| is_ident_token(token))
    {
        end += 2;
    }
    Some(end)
}

fn is_ident_token(token: &Token) -> bool {
    matches!(
        token,
        Token::Word(_) | Token::DoubleQuotedString(_) | Token::SingleQuotedString(_)
    )
}

fn collect_parts(significant_slice: &[(usize, &Token)]) -> Vec<String> {
    let mut parts = Vec::new();
    for (_, token) in significant_slice {
        match token {
            Token::Word(word) => parts.push(word.value.clone()),
            Token::DoubleQuotedString(name) | Token::SingleQuotedString(name) => {
                parts.push(name.clone());
            }
            _ => {}
        }
    }
    parts
}
