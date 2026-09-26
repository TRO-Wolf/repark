use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_datafusion::IcebergTableProvider;
use repark_core::CatalogRegistry;
use repark_core::time_travel::next_temp_view_name;

use crate::catalog_ops::{catalog_handle, iceberg_err};
use crate::ref_ddl::{WriteToBranchSniff, sniff_write_to_branch};
use crate::time_travel::PinnedViews;
use repark_iceberg::write::MorDmlKind;

pub(crate) enum RefSelectorKind {
    Branch(String),
    Tag,
}

pub(crate) fn parse_ref_selector(last: &str) -> Option<RefSelectorKind> {
    let lowered = last.to_ascii_lowercase();
    if let Some(rest) = lowered.strip_prefix("branch_") {
        if rest.is_empty() {
            return None;
        }
        let prefix_len = last.len() - rest.len();
        return Some(RefSelectorKind::Branch(last[prefix_len..].to_string()));
    }
    if let Some(rest) = lowered.strip_prefix("tag_") {
        if rest.is_empty() {
            return None;
        }
        return Some(RefSelectorKind::Tag);
    }
    Some(RefSelectorKind::Branch(last.to_string()))
}

pub(crate) fn split_write_ref_parts(parts: &[String]) -> Option<(Vec<String>, RefSelectorKind)> {
    if parts.len() < 2 {
        return None;
    }
    let last = parts.last()?;
    if crate::metadata_tables::is_metadata_table_name(last) {
        return None;
    }
    let selector = if parts.len() >= 4 {
        parse_ref_selector(last)?
    } else if parts.len() == 2 {
        let lowered = last.to_ascii_lowercase();
        if lowered.starts_with("branch_") || lowered.starts_with("tag_") {
            parse_ref_selector(last)?
        } else {
            return None;
        }
    } else {
        return None;
    };
    Some((parts[..parts.len() - 1].to_vec(), selector))
}

pub(crate) fn missing_branch_error(name: &str) -> DataFusionError {
    DataFusionError::Plan(format!("Cannot use branch (does not exist): {name}"))
}

pub(crate) fn tag_write_error(sql: &str) -> DataFusionError {
    let upper = sql.trim_start();
    let is_modify = upper
        .get(..6)
        .is_some_and(|head| head.eq_ignore_ascii_case("UPDATE"))
        || upper
            .get(..6)
            .is_some_and(|head| head.eq_ignore_ascii_case("DELETE"))
        || upper
            .get(..5)
            .is_some_and(|head| head.eq_ignore_ascii_case("MERGE"));
    if is_modify {
        DataFusionError::Plan("Cannot modify table with time travel".to_string())
    } else {
        DataFusionError::Plan("Cannot write to table with time travel".to_string())
    }
}

fn sniff_applies(ctx: &SessionContext, sniff: &WriteToBranchSniff) -> bool {
    match sniff {
        WriteToBranchSniff::MultiPart => true,
        WriteToBranchSniff::TwoPart { parts } => {
            let full =
                datafusion::sql::TableReference::partial(parts[0].as_str(), parts[1].as_str());
            let prefix = datafusion::sql::TableReference::bare(parts[0].as_str());
            !ctx.table_exist(full).unwrap_or(false) && ctx.table_exist(prefix).unwrap_or(false)
        }
    }
}

fn is_owned_write_head(ctx: &SessionContext, sql: &str) -> bool {
    if repark_iceberg::write::session_write_conf_is_set(ctx) && is_session_conf_owned_head(sql) {
        return true;
    }
    is_owned_write_head_always(sql)
}

fn is_session_conf_owned_head(sql: &str) -> bool {
    let Some(head) = statement_head(sql) else {
        return false;
    };
    matches!(head.as_str(), "INSERT" | "DELETE" | "UPDATE")
}

fn statement_head(sql: &str) -> Option<String> {
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql).tokenize().ok()?;
    let significant = tokens
        .iter()
        .find(|token| !matches!(token, Token::Whitespace(_) | Token::EOF | Token::SemiColon))?;
    match significant {
        Token::Word(word) => Some(word.value.to_ascii_uppercase()),
        _ => None,
    }
}

fn is_owned_write_head_always(sql: &str) -> bool {
    let Ok(tokens) = Tokenizer::new(&DatabricksDialect {}, sql).tokenize() else {
        return false;
    };
    let significant: Vec<&Token> = tokens
        .iter()
        .filter(|token| !matches!(token, Token::Whitespace(_) | Token::EOF | Token::SemiColon))
        .collect();
    let Some(Token::Word(word)) = significant.first() else {
        return false;
    };
    let head = word.value.to_ascii_uppercase();
    match head.as_str() {
        "MERGE" | "TRUNCATE" => true,
        "INSERT" => {
            significant.get(1).is_some_and(|token| match token {
                Token::Word(word) => word.value.eq_ignore_ascii_case("OVERWRITE"),
                _ => false,
            }) || crate::insert_by_name::sql_has_insert_by_name(sql)
                || crate::router::insert_positional::replace_where::sql_has_replace_where(sql)
                || crate::router::insert_positional::partition_append::sql_has_partition_append(sql)
        }
        _ => false,
    }
}

struct TargetSpan {
    parts: Vec<String>,
    start: usize,
    end: usize,
}

pub(crate) fn find_write_target_range(tokens: &[Token]) -> Option<(usize, usize)> {
    find_target_span(tokens, 1).map(|span| (span.start, span.end))
}

fn find_target_span(tokens: &[Token], min_parts: usize) -> Option<TargetSpan> {
    let significant: Vec<(usize, &Token)> = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token, Token::Whitespace(_) | Token::EOF | Token::SemiColon))
        .collect();
    if significant.len() < 2 {
        return None;
    }
    let start = write_target_start(&significant)?;
    let (parts, end_sig) = collect_parts(&significant, start)?;
    if parts.len() < min_parts {
        return None;
    }
    let last = parts.last()?;
    if crate::metadata_tables::is_metadata_table_name(last) {
        return None;
    }
    let orig_start = significant[start].0;
    let orig_end = significant[end_sig - 1].0 + 1;
    Some(TargetSpan {
        parts,
        start: orig_start,
        end: orig_end,
    })
}

fn write_target_start(significant: &[(usize, &Token)]) -> Option<usize> {
    let Token::Word(word) = significant.first()?.1 else {
        return None;
    };
    let head = word.value.to_ascii_uppercase();
    let mut index = match head.as_str() {
        "UPDATE" => 1,
        "DELETE" => {
            if word_eq(significant, 1, "FROM") {
                2
            } else {
                1
            }
        }
        "TRUNCATE" => {
            if word_eq(significant, 1, "TABLE") {
                2
            } else {
                1
            }
        }
        "INSERT" | "MERGE" => {
            let mut cursor = 1;
            if word_eq(significant, cursor, "INTO") || word_eq(significant, cursor, "OVERWRITE") {
                cursor += 1;
            }
            cursor
        }
        _ => return None,
    };
    if word_eq(significant, index, "TABLE") {
        index += 1;
    }
    Some(index)
}

fn word_eq(significant: &[(usize, &Token)], index: usize, expected: &str) -> bool {
    match significant.get(index).map(|(_, token)| *token) {
        Some(Token::Word(word)) => word.value.eq_ignore_ascii_case(expected),
        _ => false,
    }
}

fn collect_parts(significant: &[(usize, &Token)], start: usize) -> Option<(Vec<String>, usize)> {
    let mut parts = Vec::new();
    let mut index = start;
    loop {
        let word = match significant.get(index).map(|(_, token)| *token) {
            Some(Token::Word(word)) => unquote(&word.value),
            Some(Token::DoubleQuotedString(value)) => value.clone(),
            _ => break,
        };
        parts.push(word);
        index += 1;
        if matches!(significant.get(index).map(|(_, t)| *t), Some(Token::Period)) {
            index += 1;
            continue;
        }
        break;
    }
    if parts.is_empty() {
        None
    } else {
        Some((parts, index))
    }
}

fn unquote(value: &str) -> String {
    value.trim_matches('"').to_string()
}

fn tokens_to_sql(tokens: &[Token]) -> String {
    tokens.iter().map(ToString::to_string).collect()
}

fn session_defaults(ctx: &SessionContext) -> (String, String) {
    let state = ctx.state();
    let catalog = &state.config().options().catalog;
    (
        catalog.default_catalog.clone(),
        catalog.default_schema.clone(),
    )
}

pub(crate) fn qualify_table_parts(ctx: &SessionContext, parts: Vec<String>) -> Vec<String> {
    if parts.len() >= 3 {
        return parts;
    }
    let (catalog, schema) = session_defaults(ctx);
    match parts.as_slice() {
        [table] => vec![catalog, schema, table.clone()],
        [namespace, table] => vec![catalog, namespace.clone(), table.clone()],
        _ => parts,
    }
}

fn load_target_table(
    catalogs: &CatalogRegistry,
    table_parts: &[String],
) -> Result<(String, TableIdent, Arc<dyn iceberg::Catalog>)> {
    if table_parts.len() < 3 {
        return Err(DataFusionError::Plan(format!(
            "write-to-branch target must be a three-part catalog.namespace.table name, got `{}`",
            table_parts.join(".")
        )));
    }
    let catalog_name = table_parts[0].clone();
    let table_name = table_parts[table_parts.len() - 1].clone();
    let namespace_parts = table_parts[1..table_parts.len() - 1].to_vec();
    let namespace = NamespaceIdent::from_vec(namespace_parts).map_err(iceberg_err)?;
    let ident = TableIdent::new(namespace, table_name);
    let catalog = Arc::clone(catalog_handle(catalogs, &catalog_name)?);
    Ok((catalog_name, ident, catalog))
}

fn write_dml_kind(sql: &str) -> Option<MorDmlKind> {
    let trimmed = sql.trim_start();
    if trimmed
        .get(..6)
        .is_some_and(|head| head.eq_ignore_ascii_case("UPDATE"))
    {
        Some(MorDmlKind::Update)
    } else if trimmed
        .get(..6)
        .is_some_and(|head| head.eq_ignore_ascii_case("DELETE"))
    {
        Some(MorDmlKind::Delete)
    } else {
        None
    }
}

pub(crate) fn dotted_name_tokens(parts: &[String]) -> Vec<Token> {
    let mut tokens = Vec::new();
    for (index, part) in parts.iter().enumerate() {
        if index > 0 {
            tokens.push(Token::Period);
        }
        tokens.push(Token::Word(Word {
            value: part.clone(),
            quote_style: None,
            keyword: datafusion::sql::sqlparser::keywords::Keyword::NoKeyword,
        }));
    }
    tokens
}

struct BranchTarget {
    table_parts: Vec<String>,
    branch: String,
    selector_segment: String,
    keep_existing_selector: bool,
    require_existing_branch: bool,
}

async fn wap_branch_target(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    wap: &crate::wap::WapSessionConfig,
    parts: &[String],
) -> Result<Option<BranchTarget>> {
    if wap.branch.is_none() {
        return Ok(None);
    }
    let qualified = qualify_table_parts(ctx, parts.to_vec());
    let Ok((_catalog_name, ident, catalog)) = load_target_table(catalogs, &qualified) else {
        return Ok(None);
    };
    let Ok(table) = catalog.load_table(&ident).await else {
        return Ok(None);
    };
    let Some(branch) = crate::wap::wap_branch_for_table(wap, &table)? else {
        return Ok(None);
    };
    Ok(Some(BranchTarget {
        table_parts: parts.to_vec(),
        selector_segment: format!("branch_{branch}"),
        branch,
        keep_existing_selector: false,
        require_existing_branch: false,
    }))
}

pub(crate) async fn apply_write_to_branch<'a>(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &'a str,
    pinned: &mut PinnedViews,
    skip_wap_id_route: bool,
) -> Result<Cow<'a, str>> {
    let wap = crate::wap::session_wap(ctx);
    let explicit = sniff_write_to_branch(sql).is_some_and(|sniff| sniff_applies(ctx, &sniff));
    if !explicit && wap.branch.is_none() && wap.id.is_none() {
        return Ok(Cow::Borrowed(sql));
    }
    let dialect = DatabricksDialect {};
    let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize() else {
        return Ok(Cow::Borrowed(sql));
    };
    let Some(span) = find_target_span(&tokens, if explicit { 2 } else { 1 }) else {
        return Ok(Cow::Borrowed(sql));
    };
    let selector = explicit
        .then(|| split_write_ref_parts(&span.parts))
        .flatten();
    let target = match selector {
        Some((_, RefSelectorKind::Tag)) => return Err(tag_write_error(sql)),
        Some((table_parts, RefSelectorKind::Branch(branch))) => BranchTarget {
            table_parts,
            selector_segment: span
                .parts
                .last()
                .cloned()
                .unwrap_or_else(|| format!("branch_{branch}")),
            branch,
            keep_existing_selector: span.parts.len() >= 4,
            require_existing_branch: true,
        },
        None => {
            if let Some(target) = wap_branch_target(ctx, catalogs, &wap, &span.parts).await? {
                target
            } else if !skip_wap_id_route
                && let Some(staged) =
                    wap_id_route_target(ctx, catalogs, &wap, &span.parts, sql).await?
            {
                return commit_write_staged(ctx, catalogs, pinned, &tokens, &span, staged).await;
            } else {
                return Ok(Cow::Borrowed(sql));
            }
        }
    };
    commit_write_on_branch(ctx, catalogs, sql, pinned, &tokens, &span, target).await
}

struct WapIdTarget {
    table_parts: Vec<String>,
    wap_id: String,
}

fn is_plain_append_insert(ctx: &SessionContext, sql: &str) -> bool {
    statement_head(sql).is_some_and(|head| head == "INSERT") && !is_owned_write_head(ctx, sql)
}

async fn wap_id_route_target(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    wap: &crate::wap::WapSessionConfig,
    parts: &[String],
    sql: &str,
) -> Result<Option<WapIdTarget>> {
    if wap.id.is_none() || !is_plain_append_insert(ctx, sql) {
        return Ok(None);
    }
    let qualified = qualify_table_parts(ctx, parts.to_vec());
    let Ok((_catalog_name, ident, catalog)) = load_target_table(catalogs, &qualified) else {
        return Ok(None);
    };
    let Ok(table) = catalog.load_table(&ident).await else {
        return Ok(None);
    };
    let Some(wap_id) = crate::wap::wap_id_for_table(wap, &table)? else {
        return Ok(None);
    };
    Ok(Some(WapIdTarget {
        table_parts: parts.to_vec(),
        wap_id,
    }))
}

async fn commit_write_staged<'a>(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    pinned: &mut PinnedViews,
    tokens: &[Token],
    span: &TargetSpan,
    staged: WapIdTarget,
) -> Result<Cow<'a, str>> {
    let qualified = qualify_table_parts(ctx, staged.table_parts);
    let (_catalog_name, ident, catalog) = load_target_table(catalogs, &qualified)?;
    let provider =
        IcebergTableProvider::try_new(catalog, ident.namespace().clone(), ident.name().to_string())
            .await
            .map_err(iceberg_err)?
            .with_uuid_as_string(true)
            .with_stage_only(true)
            .with_snapshot_properties(HashMap::from([(
                crate::wap::WAP_ID_SNAPSHOT_PROPERTY.to_string(),
                staged.wap_id,
            )]));
    redirect_write_to_temp_view(ctx, pinned, tokens, span, provider, "wap-staged").map(Cow::Owned)
}

fn redirect_write_to_temp_view(
    ctx: &SessionContext,
    pinned: &mut PinnedViews,
    tokens: &[Token],
    span: &TargetSpan,
    provider: IcebergTableProvider,
    view_kind: &str,
) -> Result<String> {
    let temp_name = next_temp_view_name();
    let home_catalog = "datafusion".to_string();
    let home_schema = "public".to_string();
    let df_catalog = ctx.catalog(&home_catalog).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "no session catalog `{home_catalog}` for {view_kind} temp view (have {:?})",
            ctx.catalog_names()
        ))
    })?;
    let schema = df_catalog.schema(&home_schema).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "no schema `{home_catalog}.{home_schema}` for {view_kind} temp view"
        ))
    })?;
    let _ = schema.deregister_table(&temp_name);
    schema
        .register_table(temp_name.clone(), Arc::new(provider))
        .map_err(|error| {
            DataFusionError::Plan(format!(
                "failed to register {view_kind} temp view \
                 {home_catalog}.{home_schema}.{temp_name}: {error}"
            ))
        })?;
    pinned.record(format!("{home_catalog}.{home_schema}.{temp_name}"));
    let temp_parts = vec![home_catalog, home_schema, temp_name];
    let mut tokens = tokens.to_vec();
    tokens.splice(span.start..span.end, dotted_name_tokens(&temp_parts));
    Ok(tokens_to_sql(&tokens))
}

async fn create_wap_branch_from_main(
    catalog: &dyn iceberg::Catalog,
    ident: &TableIdent,
    table: &iceberg::table::Table,
    branch: &str,
) -> Result<()> {
    if table.metadata().snapshot_for_ref(branch).is_some() {
        return Ok(());
    }
    let Some(snapshot_id) = table.metadata().current_snapshot_id() else {
        return Ok(());
    };
    repark_iceberg::write::create_snapshot_ref(
        catalog,
        ident,
        repark_iceberg::write::SnapshotRefKind::Branch,
        branch,
        snapshot_id,
    )
    .await
}

async fn commit_write_on_branch<'a>(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &'a str,
    pinned: &mut PinnedViews,
    tokens: &[Token],
    span: &TargetSpan,
    target: BranchTarget,
) -> Result<Cow<'a, str>> {
    let qualified = qualify_table_parts(ctx, target.table_parts);
    let (_catalog_name, ident, catalog) = load_target_table(catalogs, &qualified)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;
    if target.require_existing_branch && table.metadata().snapshot_for_ref(&target.branch).is_none()
    {
        return Err(missing_branch_error(&target.branch));
    }
    if !target.require_existing_branch {
        create_wap_branch_from_main(catalog.as_ref(), &ident, &table, &target.branch).await?;
    }
    if let Some(kind) = write_dml_kind(sql) {
        repark_iceberg::write::refuse_mor_unpartitioned_multi_spec_dml(
            catalog.as_ref(),
            &ident,
            &ident.to_string(),
            kind,
        )
        .await?;
    }
    if is_owned_write_head(ctx, sql) {
        if target.keep_existing_selector {
            return Ok(Cow::Borrowed(sql));
        }
        let mut rewritten = qualified;
        rewritten.push(target.selector_segment);
        let mut tokens = tokens.to_vec();
        tokens.splice(span.start..span.end, dotted_name_tokens(&rewritten));
        return Ok(Cow::Owned(tokens_to_sql(&tokens)));
    }
    let provider =
        IcebergTableProvider::try_new(catalog, ident.namespace().clone(), ident.name().to_string())
            .await
            .map_err(iceberg_err)?
            .with_uuid_as_string(true)
            .with_commit_branch(target.branch);
    redirect_write_to_temp_view(ctx, pinned, tokens, span, provider, "branch-commit")
        .map(Cow::Owned)
}
