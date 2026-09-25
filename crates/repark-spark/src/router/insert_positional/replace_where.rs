use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName, ObjectNamePart,
    Query, SetExpr, visit_expressions, visit_expressions_mut,
};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer, Word};
use iceberg::expr::Predicate;
use iceberg::spec::DataFile;
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;

use super::{PreparedInsert, deduplicate_source_names, reparse_insert_sql, reparsed};
use crate::catalog_ops::{
    name_parts, namespace_schema_name, refuse_read_only_dml_table_sql, reregister,
    table_or_view_not_found,
};
use crate::spark_ast;
use crate::write_options::StatementWriteOptions;

pub(crate) struct ReplaceWhere {
    table: ObjectName,
    predicate: Expr,
    source: Box<Query>,
}

pub(crate) fn sql_has_replace_where(sql: &str) -> bool {
    !matches!(parse_replace_where(sql), Ok(None))
}

pub(crate) fn parse_replace_where(sql: &str) -> Result<Option<ReplaceWhere>> {
    if !sql.to_ascii_lowercase().contains("replace") {
        return Ok(None);
    }
    let dialect = DatabricksDialect {};
    let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize_with_location() else {
        return Ok(None);
    };
    let significant: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token.token, Token::Whitespace(_)))
        .map(|(index, _)| index)
        .collect();
    let word = |position: usize| match significant.get(position).map(|index| &tokens[*index]) {
        Some(TokenWithSpan {
            token: Token::Word(word),
            ..
        }) if word.quote_style.is_none() => Some(word.keyword),
        _ => None,
    };
    if word(0) != Some(Keyword::INSERT) {
        return Ok(None);
    }
    let Some(replace_at) = replace_where_position(&tokens, &significant, word) else {
        return Ok(None);
    };
    let prefix: Vec<&Token> = significant[1..replace_at]
        .iter()
        .map(|index| &tokens[*index].token)
        .collect();
    let table = target_name(&prefix).ok_or_else(|| syntax_error("'REPLACE'"))?;
    let rest = tokens[significant[replace_at + 1] + 1..].to_vec();
    let mut parser = Parser::new(&dialect).with_tokens_with_locations(rest);
    let predicate = parser
        .parse_expr()
        .map_err(|_| syntax_error(&near(&parser.peek_token().token)))?;
    let next = parser.peek_token().token;
    if !starts_query(&next) {
        return Err(syntax_error(&near(&next)));
    }
    let source = parser
        .parse_query()
        .map_err(|_| syntax_error(&near(&next)))?;
    while parser.consume_token(&Token::SemiColon) {}
    let trailing = parser.peek_token().token;
    if trailing != Token::EOF {
        return Err(syntax_error(&near(&trailing)));
    }
    Ok(Some(ReplaceWhere {
        table,
        predicate,
        source,
    }))
}

fn replace_where_position(
    tokens: &[TokenWithSpan],
    significant: &[usize],
    word: impl Fn(usize) -> Option<Keyword>,
) -> Option<usize> {
    let mut depth = 0_usize;
    for position in 1..significant.len() {
        match &tokens[significant[position]].token {
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            _ if depth > 0 => {}
            _ => match word(position) {
                Some(Keyword::REPLACE) if word(position + 1) == Some(Keyword::WHERE) => {
                    return Some(position);
                }
                Some(Keyword::SELECT | Keyword::VALUES | Keyword::WITH | Keyword::FROM) => {
                    return None;
                }
                Some(Keyword::TABLE) if position > 2 => return None,
                _ => {}
            },
        }
    }
    None
}

fn target_name(prefix: &[&Token]) -> Option<ObjectName> {
    let keyword = |token: &Token, expected: Keyword| matches!(token, Token::Word(Word { keyword, quote_style: None, .. }) if *keyword == expected);
    let (first, mut rest) = prefix.split_first()?;
    if !keyword(first, Keyword::INTO) {
        return None;
    }
    if rest
        .first()
        .is_some_and(|token| keyword(token, Keyword::TABLE))
        && rest.len() > 1
    {
        rest = &rest[1..];
    }
    let mut parts = Vec::new();
    for (index, token) in rest.iter().enumerate() {
        match (index % 2, token) {
            (0, Token::Word(word)) => parts.push(Ident {
                value: word.value.clone(),
                quote_style: word.quote_style,
                span: datafusion::sql::sqlparser::tokenizer::Span::empty(),
            }),
            (1, Token::Period) => {}
            _ => return None,
        }
    }
    (!parts.is_empty() && rest.len() % 2 == 1).then(|| ObjectName::from(parts))
}

fn starts_query(token: &Token) -> bool {
    match token {
        Token::LParen => true,
        Token::Word(word) => matches!(
            word.keyword,
            Keyword::SELECT | Keyword::VALUES | Keyword::WITH | Keyword::TABLE | Keyword::FROM
        ),
        _ => false,
    }
}

fn near(token: &Token) -> String {
    match token {
        Token::EOF => "end of input".to_string(),
        other => format!("'{other}'"),
    }
}

fn syntax_error(near: &str) -> DataFusionError {
    DataFusionError::SQL(
        Box::new(ParserError::ParserError(format!(
            "[PARSE_SYNTAX_ERROR] Syntax error at or near {near}. SQLSTATE: 42601"
        ))),
        None,
    )
}

pub(crate) async fn execute_replace_where(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    replace: ReplaceWhere,
    options: &StatementWriteOptions,
) -> Result<DataFrame> {
    refuse_subquery(&replace.predicate)?;
    let source = unparenthesized(&replace.source);
    let parsed = reparse_insert_sql(format!("INSERT INTO {} {source}", replace.table))?;
    let PreparedInsert { sql, insert, .. } = match deduplicate_source_names(&parsed.insert) {
        Some(deduplicated) => reparsed(deduplicated)?,
        None => parsed,
    };
    crate::view_dispatch::refuse_insert_into_view(ctx, catalogs, &insert).await?;
    let table_sql = replace.table.to_string();
    if let Some(message) = refuse_read_only_dml_table_sql(catalogs, &table_sql) {
        return Err(DataFusionError::Plan(message));
    }
    let resolved = crate::insert_overwrite::try_resolve_iceberg_overwrite_target(
        ctx,
        catalogs,
        &replace.table,
    )
    .await;
    let (catalog_name, catalog, table, branch) = match resolved {
        Ok(Some(target)) => target,
        Ok(None) => {
            spark_ast::execute_insert_source(ctx, catalogs, &sql).await?;
            return Err(DataFusionError::Plan(format!(
                "INSERT INTO … REPLACE WHERE requires an Iceberg table, got `{table_sql}`"
            )));
        }
        Err(error) => {
            return Err(missing_target(ctx, catalogs, &replace.table)
                .await
                .unwrap_or(error));
        }
    };
    let planning_sql = match branch {
        Some(_) => {
            crate::append_with_options::insert_sql_without_write_ref(&insert, &replace.table)
                .unwrap_or_else(|| sql.clone())
        }
        None => sql.clone(),
    };
    let planned_source = insert.source.as_deref().unwrap_or(source);
    super::partition_append::refuse_positional_arity(
        ctx,
        catalogs,
        &catalog_name,
        &table,
        planned_source,
        source,
    )
    .await?;
    let source_df = spark_ast::execute_insert_source(ctx, catalogs, &planning_sql).await?;
    let namespace = namespace_schema_name(table.identifier().namespace());
    let base_table = format!(
        "`{catalog_name}`.`{namespace}`.`{}`",
        table.identifier().name()
    );
    refuse_non_deterministic(ctx, catalogs, &base_table, &replace.predicate).await?;
    let filter = repark_iceberg::write::spark_overwrite_filter(
        &as_written(&replace.predicate),
        table.metadata().current_schema(),
    )?;
    let staged = stage_source(ctx, &table, source_df, options).await?;
    let (snapshot_extra, _) = options.resolve_with_session(ctx)?;
    if matches!(filter, Predicate::AlwaysFalse) {
        repark_iceberg::write::commit_append_with_summary(
            &catalog,
            &table,
            staged,
            &snapshot_extra,
            branch.as_deref(),
        )
        .await?;
    } else {
        repark_iceberg::write::commit_overwrite_by_filter_with_summary(
            &catalog,
            &table,
            staged,
            filter,
            branch.as_deref(),
            &snapshot_extra,
            options.isolation.as_deref(),
        )
        .await?;
    }
    reregister(ctx, Arc::clone(&catalog), &catalog_name, &namespace).await?;
    ctx.read_empty()
}

fn unparenthesized(source: &Query) -> &Query {
    let mut current = source;
    while current.with.is_none()
        && current.order_by.is_none()
        && current.limit_clause.is_none()
        && current.fetch.is_none()
    {
        let SetExpr::Query(inner) = current.body.as_ref() else {
            break;
        };
        current = inner;
    }
    current
}

async fn missing_target(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    name: &ObjectName,
) -> Option<DataFusionError> {
    let parts = crate::write_to_branch::qualify_table_parts(ctx, name_parts(name));
    let [catalog_name, namespace, table] = parts.as_slice() else {
        return None;
    };
    let catalog = catalogs.get(catalog_name)?;
    let ident = TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone());
    let missing = match catalog.table_exists(&ident).await {
        Ok(exists) => !exists,
        Err(error) => error.kind() == iceberg::ErrorKind::NamespaceNotFound,
    };
    missing.then(|| table_or_view_not_found(catalog_name, namespace, table))
}

fn as_written(predicate: &Expr) -> Expr {
    let mut written = predicate.clone();
    let _ = visit_expressions_mut(&mut written, |expr| {
        if let Some(literal) = suffix_literal(expr) {
            *expr = literal;
        }
        ControlFlow::<()>::Continue(())
    });
    written
}

fn suffix_literal(expr: &Expr) -> Option<Expr> {
    let Expr::Function(function) = expr else {
        return None;
    };
    let [ObjectNamePart::Identifier(name)] = function.name.0.as_slice() else {
        return None;
    };
    if !name.value.eq_ignore_ascii_case(crate::SUFFIX_LITERAL_NAME) {
        return None;
    }
    let FunctionArguments::List(list) = &function.args else {
        return None;
    };
    match list.args.as_slice() {
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(literal))] => Some(literal.clone()),
        _ => None,
    }
}

fn refuse_subquery(predicate: &Expr) -> Result<()> {
    let found = visit_expressions(predicate, |expr| match expr {
        Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    });
    if found.is_break() {
        return Err(DataFusionError::Plan(
            "[UNSUPPORTED_FEATURE.OVERWRITE_BY_SUBQUERY] The feature is not supported: INSERT \
             OVERWRITE with a subquery condition. SQLSTATE: 0A000"
                .to_string(),
        ));
    }
    Ok(())
}

async fn refuse_non_deterministic(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    base_table: &str,
    predicate: &Expr,
) -> Result<()> {
    let check = spark_ast::execute_passthrough(
        ctx,
        catalogs,
        &format!("SELECT * FROM {base_table} WHERE {predicate}"),
    )
    .await?;
    let mut volatile = false;
    check.logical_plan().apply(|node: &LogicalPlan| {
        if let LogicalPlan::Filter(filter) = node
            && filter.predicate.is_volatile()
        {
            volatile = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if volatile {
        return Err(DataFusionError::Plan(format!(
            "[INVALID_NON_DETERMINISTIC_EXPRESSIONS] The operator expects a deterministic \
             expression, but the actual expression is \"({predicate})\". SQLSTATE: 42K0E"
        )));
    }
    Ok(())
}

async fn stage_source(
    ctx: &SessionContext,
    table: &iceberg::table::Table,
    source_df: DataFrame,
    options: &StatementWriteOptions,
) -> Result<Vec<DataFile>> {
    let concurrency = repark_iceberg::write::concurrency_from_ctx(ctx);
    let session = repark_iceberg::write::session_write_conf_from_ctx(ctx);
    let (_, mut staging) = options.resolve_with_session(ctx)?;
    staging.write_format.clone_from(&options.write_format);
    staging.delete_format.clone_from(&options.delete_format);
    let stream = source_df.execute_stream().await?;
    let staged = if options.is_empty() && session.is_empty() {
        repark_iceberg::write::write_overwrite_staged_files_from_stream(
            table,
            stream,
            Vec::new(),
            concurrency,
        )
        .await?
    } else {
        repark_iceberg::write::stage_overwrite_files_with(
            table,
            stream,
            Vec::new(),
            concurrency,
            &staging,
        )
        .await?
    };
    Ok(staged
        .into_iter()
        .filter(|file| file.record_count() > 0)
        .collect())
}
