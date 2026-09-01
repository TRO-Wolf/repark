use std::borrow::Cow;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_datafusion::IcebergTableProvider;
use repark_core::{CatalogRegistry, next_temp_view_name};

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
    } else if parts.len() == 3 || parts.len() == 2 {
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
    let is_modify = upper.len() >= 6
        && (upper[..6].eq_ignore_ascii_case("UPDATE")
            || upper[..6].eq_ignore_ascii_case("DELETE")
            || upper[..5].eq_ignore_ascii_case("MERGE"));
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

fn is_owned_write_head(sql: &str) -> bool {
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
        "INSERT" => significant.get(1).is_some_and(|token| match token {
            Token::Word(word) => word.value.eq_ignore_ascii_case("OVERWRITE"),
            _ => false,
        }),
        _ => false,
    }
}

struct TargetSpan {
    parts: Vec<String>,
    start: usize,
    end: usize,
}

fn find_target_span(tokens: &[Token]) -> Option<TargetSpan> {
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
    if parts.len() < 2 {
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
    if trimmed.len() >= 6 && trimmed[..6].eq_ignore_ascii_case("UPDATE") {
        Some(MorDmlKind::Update)
    } else if trimmed.len() >= 6 && trimmed[..6].eq_ignore_ascii_case("DELETE") {
        Some(MorDmlKind::Delete)
    } else {
        None
    }
}

fn dotted_name_tokens(parts: &[String]) -> Vec<Token> {
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

pub(crate) async fn apply_write_to_branch<'a>(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &'a str,
    pinned: &mut PinnedViews,
) -> Result<Cow<'a, str>> {
    let Some(sniff) = sniff_write_to_branch(sql) else {
        return Ok(Cow::Borrowed(sql));
    };
    if !sniff_applies(ctx, &sniff) {
        return Ok(Cow::Borrowed(sql));
    }
    let dialect = DatabricksDialect {};
    let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize() else {
        return Ok(Cow::Borrowed(sql));
    };
    let Some(span) = find_target_span(&tokens) else {
        return Ok(Cow::Borrowed(sql));
    };
    let Some((table_parts, selector)) = split_write_ref_parts(&span.parts) else {
        return Ok(Cow::Borrowed(sql));
    };
    match selector {
        RefSelectorKind::Tag => Err(tag_write_error(sql)),
        RefSelectorKind::Branch(branch) => {
            let qualified = qualify_table_parts(ctx, table_parts);
            let (_catalog_name, ident, catalog) = load_target_table(catalogs, &qualified)?;
            let table = catalog.load_table(&ident).await.map_err(iceberg_err)?;
            if table.metadata().snapshot_for_ref(&branch).is_none() {
                return Err(missing_branch_error(&branch));
            }
            if let Some(kind) = write_dml_kind(sql) {
                repark_iceberg::write::refuse_v3_cow_dml(catalog.as_ref(), &ident, kind).await?;
                repark_iceberg::write::refuse_mor_unpartitioned_multi_spec_dml(
                    catalog.as_ref(),
                    &ident,
                    &ident.to_string(),
                    kind,
                )
                .await?;
            }
            if is_owned_write_head(sql) {
                if span.parts.len() >= 4 {
                    return Ok(Cow::Borrowed(sql));
                }
                let mut rewritten = qualified;
                rewritten.push(
                    span.parts
                        .last()
                        .cloned()
                        .unwrap_or_else(|| format!("branch_{branch}")),
                );
                let mut tokens = tokens;
                tokens.splice(span.start..span.end, dotted_name_tokens(&rewritten));
                return Ok(Cow::Owned(tokens_to_sql(&tokens)));
            }
            let provider = IcebergTableProvider::try_new(
                catalog,
                ident.namespace().clone(),
                ident.name().to_string(),
            )
            .await
            .map_err(iceberg_err)?
            .with_commit_branch(branch);
            let temp_name = next_temp_view_name();
            let home_catalog = "datafusion".to_string();
            let home_schema = "public".to_string();
            let df_catalog = ctx.catalog(&home_catalog).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "no session catalog `{home_catalog}` for branch-commit temp view (have {:?})",
                    ctx.catalog_names()
                ))
            })?;
            let schema = df_catalog.schema(&home_schema).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "no schema `{home_catalog}.{home_schema}` for branch-commit temp view"
                ))
            })?;
            let _ = schema.deregister_table(&temp_name);
            schema
                .register_table(temp_name.clone(), Arc::new(provider))
                .map_err(|error| {
                    DataFusionError::Plan(format!(
                        "failed to register branch-commit temp view {home_catalog}.{home_schema}.{temp_name}: {error}"
                    ))
                })?;
            pinned.record(format!("{home_catalog}.{home_schema}.{temp_name}"));
            let temp_parts = vec![home_catalog, home_schema, temp_name];
            let mut tokens = tokens;
            tokens.splice(span.start..span.end, dotted_name_tokens(&temp_parts));
            Ok(Cow::Owned(tokens_to_sql(&tokens)))
        }
    }
}
