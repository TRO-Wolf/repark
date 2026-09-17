use std::collections::HashSet;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{ObjectName, Query, SetExpr, Statement, TableObject};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;

use crate::catalog_ops::{name_parts, namespace_schema_name, reregister};
use crate::parse_single_normalized;

enum TargetFill {
    Source(usize),
    Null,
    Static(String),
}

struct StaticColumn {
    canonical: String,
    written: String,
    literal_sql: String,
}

fn case_sensitive_insert(ctx: &SessionContext) -> bool {
    repark_functions::case_sensitive::spark_case_sensitive_from_options(
        ctx.copied_config().options(),
    )
}

fn fold_name(value: &str, case_sensitive: bool) -> String {
    if case_sensitive {
        value.to_string()
    } else {
        value.to_ascii_lowercase()
    }
}

fn same_name(first: &str, second: &str, case_sensitive: bool) -> bool {
    fold_name(first, case_sensitive) == fold_name(second, case_sensitive)
}

pub(crate) async fn execute_insert_by_name(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    stripped_sql: &str,
) -> Result<DataFrame> {
    let parsed = parse_single_normalized(stripped_sql)?;
    let Some((statement, _)) = parsed else {
        return crate::router::execute_unparsable_fallthrough(ctx, catalogs, stripped_sql).await;
    };
    let Statement::Insert(insert) = statement else {
        return crate::router::execute_unparsable_fallthrough(ctx, catalogs, stripped_sql).await;
    };
    let table_sql = insert.table.to_string();
    let table_name = match &insert.table {
        TableObject::TableName(name) => name.clone(),
        TableObject::TableFunction(_) | TableObject::TableQuery(_) => {
            return Err(DataFusionError::Plan(format!(
                "INSERT INTO {table_sql} BY NAME needs an Iceberg table name target"
            )));
        }
    };
    let source = insert.source.as_ref().ok_or_else(|| {
        DataFusionError::Plan("INSERT INTO … BY NAME needs a SELECT or VALUES source".to_string())
    })?;
    let Some((catalog_name, catalog, table, branch)) =
        resolve_append_target(ctx, catalogs, &table_name).await?
    else {
        return Err(DataFusionError::Plan(format!(
            "INSERT INTO {table_sql} BY NAME needs a three-part Iceberg table name, got \
             `{table_sql}`"
        )));
    };
    let case_sensitive = case_sensitive_insert(ctx);
    let source_names = probe_source_names(ctx, catalogs, source, case_sensitive).await?;
    let table_display = display_table_name(&catalog_name, &table);
    let (projection_sql, static_columns) = plan_name_projection(
        &table,
        &insert,
        source,
        &source_names,
        &table_display,
        case_sensitive,
    )?;
    if insert.overwrite {
        let query = parse_projection_query(&projection_sql)?;
        if insert.partitioned.is_some() && !static_columns.is_empty() {
            let mut delegated = insert.clone();
            delegated.source = Some(query);
            delegated.columns = Vec::new();
            let partitioned = insert.partitioned.clone().unwrap_or_default();
            return crate::insert_overwrite::execute_partition_overwrite(
                ctx,
                catalogs,
                &table_name,
                &table_sql,
                &delegated,
                &partitioned,
            )
            .await;
        }
        if projection_is_empty(ctx, catalogs, &projection_sql).await? {
            let namespace = namespace_schema_name(table.identifier().namespace());
            let type_table_sql =
                format!("{catalog_name}.{namespace}.{}", table.identifier().name());
            crate::insert_overwrite::assert_empty_overwrite_types_assignment_compatible(
                ctx,
                catalogs,
                &type_table_sql,
                &query,
                &[],
            )
            .await?;
            repark_iceberg::write::commit_overwrite_replace_all_to(
                &catalog,
                &table,
                Vec::new(),
                branch.as_deref(),
            )
            .await?;
            reregister(ctx, catalog, &catalog_name, &namespace).await?;
            return ctx.read_empty();
        }
        return crate::insert_overwrite::insert_overwrite_from_staged_source(
            ctx,
            catalogs,
            &table_name,
            &table_sql,
            &query,
            &insert.columns,
        )
        .await;
    }
    append_by_name_projection(
        ctx,
        catalogs,
        &catalog_name,
        &catalog,
        &table,
        branch.as_deref(),
        &projection_sql,
    )
    .await
}

async fn append_by_name_projection(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    catalog: &Arc<dyn Catalog>,
    table: &iceberg::table::Table,
    branch: Option<&str>,
    projection_sql: &str,
) -> Result<DataFrame> {
    let source_df = crate::spark_ast::execute_passthrough(ctx, catalogs, projection_sql).await?;
    let stream = source_df.execute_stream().await?;
    let concurrency = repark_iceberg::write::concurrency_from_ctx(ctx);
    let staged = if table.metadata().default_partition_spec().is_unpartitioned() {
        repark_iceberg::write::write_data_files_from_stream_with_concurrency(
            table,
            stream,
            concurrency,
        )
        .await?
    } else {
        repark_iceberg::write::write_partitioned_data_files_from_stream_with_concurrency(
            table,
            stream,
            concurrency,
        )
        .await?
    };
    repark_iceberg::write::commit_append_to(catalog, table, staged, branch).await?;
    let namespace = namespace_schema_name(table.identifier().namespace());
    reregister(ctx, Arc::clone(catalog), catalog_name, &namespace).await?;
    ctx.read_empty()
}

async fn resolve_append_target(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table_name: &ObjectName,
) -> Result<
    Option<(
        String,
        Arc<dyn Catalog>,
        iceberg::table::Table,
        Option<String>,
    )>,
> {
    let mut parts = name_parts(table_name);
    let branch = match crate::write_to_branch::split_write_ref_parts(&parts) {
        Some((table_parts, crate::write_to_branch::RefSelectorKind::Branch(name))) => {
            parts = table_parts;
            Some(name)
        }
        Some((_, crate::write_to_branch::RefSelectorKind::Tag)) => {
            return Err(crate::write_to_branch::tag_write_error("INSERT INTO"));
        }
        None => None,
    };
    parts = crate::write_to_branch::qualify_table_parts(ctx, parts);
    if parts.len() < 3 {
        return Ok(None);
    }
    let catalog_name = parts[0].clone();
    let table_leaf = parts[parts.len() - 1].clone();
    let namespace_parts = parts[1..parts.len() - 1].to_vec();
    let Ok(namespace) = NamespaceIdent::from_vec(namespace_parts) else {
        return Ok(None);
    };
    let Some(catalog) = catalogs.get(&catalog_name) else {
        return Ok(None);
    };
    let ident = TableIdent::new(namespace, table_leaf);
    match catalog.load_table(&ident).await {
        Ok(table) => Ok(Some((catalog_name, Arc::clone(catalog), table, branch))),
        Err(error) => Err(DataFusionError::Plan(format!(
            "INSERT INTO … BY NAME target `{ident}` could not be loaded as an Iceberg table: \
             {error}"
        ))),
    }
}

fn display_table_name(catalog_name: &str, table: &iceberg::table::Table) -> String {
    let namespace = namespace_schema_name(table.identifier().namespace());
    let namespace = namespace
        .split('.')
        .map(quote_name)
        .collect::<Vec<_>>()
        .join(".");
    format!(
        "{}.{}.{}",
        quote_name(catalog_name),
        namespace,
        quote_name(table.identifier().name())
    )
}

struct SourceName {
    display: String,
    resolved: String,
}

async fn projection_is_empty(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    projection_sql: &str,
) -> Result<bool> {
    let probe_sql = format!("SELECT 1 FROM ({projection_sql}) AS _repark_by_name_empty LIMIT 1");
    let probe = crate::spark_ast::execute_passthrough(ctx, catalogs, &probe_sql).await?;
    let batches = probe.collect().await?;
    Ok(batches.iter().all(|batch| batch.num_rows() == 0))
}

fn plan_name_projection(
    table: &iceberg::table::Table,
    insert: &datafusion::sql::sqlparser::ast::Insert,
    source: &Query,
    source_names: &[SourceName],
    table_display: &str,
    case_sensitive: bool,
) -> Result<(String, Vec<StaticColumn>)> {
    let fields: Vec<(String, bool)> = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.name.clone(), field.required))
        .collect();
    let static_columns = static_partition_columns(table, insert, case_sensitive)?;
    for name in source_names {
        if let Some(found) = static_columns.iter().find(|static_column| {
            same_name(&static_column.canonical, &name.resolved, case_sensitive)
        }) {
            return Err(static_partition_in_column_list(&found.written));
        }
    }
    let match_fields: Vec<(String, bool)> = fields
        .iter()
        .filter(|(name, _)| {
            !static_columns
                .iter()
                .any(|static_column| same_name(&static_column.canonical, name, case_sensitive))
        })
        .cloned()
        .collect();
    let match_names: Vec<String> = match_fields.iter().map(|(name, _)| name.clone()).collect();
    let mapping =
        match_source_to_target(&match_names, source_names, table_display, case_sensitive)?;
    for ((name, required), slot) in match_fields.iter().zip(mapping.iter()) {
        if slot.is_none() && *required {
            return Err(cannot_find_data(table_display, name));
        }
    }
    if !insert.overwrite && !static_columns.is_empty() {
        return Ok((
            build_static_append_projection(
                source,
                &fields,
                &static_columns,
                source_names,
                &mapping,
                case_sensitive,
            ),
            static_columns,
        ));
    }
    let fills: Vec<TargetFill> = mapping
        .iter()
        .map(|slot| match slot {
            Some(index) => TargetFill::Source(*index),
            None => TargetFill::Null,
        })
        .collect();
    Ok((
        build_projection_sql(source, &match_names, source_names, &fills),
        static_columns,
    ))
}

fn static_partition_columns(
    table: &iceberg::table::Table,
    insert: &datafusion::sql::sqlparser::ast::Insert,
    case_sensitive: bool,
) -> Result<Vec<StaticColumn>> {
    let Some(partitioned) = &insert.partitioned else {
        return Ok(Vec::new());
    };
    if !insert.overwrite && table.metadata().default_partition_spec().is_unpartitioned() {
        return Err(DataFusionError::NotImplemented(
            "INSERT INTO … PARTITION (…) requires a partitioned Iceberg table; the target is \
             unpartitioned"
                .to_string(),
        ));
    }
    let request = repark_iceberg::write::partition_overwrite_request_from_exprs(partitioned)?;
    let plan = repark_iceberg::write::plan_partition_overwrite(table, &request)?;
    let repark_iceberg::write::PartitionOverwritePlan::Static(spec) = plan else {
        return Ok(Vec::new());
    };
    let fields = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    spec.equalities
        .iter()
        .map(|equality| {
            let canonical = fields
                .iter()
                .find(|name| same_name(name, &equality.name, case_sensitive))
                .cloned()
                .unwrap_or_else(|| equality.name.clone());
            Ok(StaticColumn {
                canonical,
                written: equality.name.clone(),
                literal_sql: partition_literal_sql(equality.value.as_ref()),
            })
        })
        .collect()
}

fn partition_literal_sql(value: Option<&repark_iceberg::write::PartitionLiteral>) -> String {
    match value {
        None => "NULL".to_string(),
        Some(repark_iceberg::write::PartitionLiteral::Boolean(true)) => "TRUE".to_string(),
        Some(repark_iceberg::write::PartitionLiteral::Boolean(false)) => "FALSE".to_string(),
        Some(repark_iceberg::write::PartitionLiteral::Int(value)) => value.to_string(),
        Some(repark_iceberg::write::PartitionLiteral::Long(value)) => value.to_string(),
        Some(repark_iceberg::write::PartitionLiteral::String(value)) => {
            format!("'{}'", value.replace('\'', "''"))
        }
    }
}

fn static_partition_in_column_list(column: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST] Static partition column {column} is also \
         specified in the column list. SQLSTATE: 42713"
    ))
}

fn cannot_find_data(table: &str, name: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write incompatible data for the \
         table {table}: Cannot find data for the output column {}. SQLSTATE: KD000",
        quote_name(name)
    ))
}

async fn probe_source_names(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    source: &Query,
    case_sensitive: bool,
) -> Result<Vec<SourceName>> {
    if let SetExpr::Values(values) = source.body.as_ref() {
        let Some(first) = values.rows.first() else {
            return Err(DataFusionError::Plan(
                "INSERT INTO … BY NAME needs a SELECT or VALUES source".to_string(),
            ));
        };
        return Ok((1..=first.len())
            .map(|index| SourceName {
                display: format!("col{index}"),
                resolved: format!("col{index}"),
            })
            .collect());
    }
    if let Some(names) = syntactic_source_names(source, case_sensitive) {
        return Ok(names);
    }
    let probe_sql = format!("SELECT * FROM ({source}) AS _repark_by_name_src LIMIT 0");
    let frame = crate::spark_ast::execute_passthrough(ctx, catalogs, &probe_sql).await?;
    Ok(frame
        .schema()
        .as_arrow()
        .fields()
        .iter()
        .map(|field| SourceName {
            display: field.name().clone(),
            resolved: field.name().clone(),
        })
        .collect())
}

fn normalize_ident(value: &str, quoted: bool, case_sensitive: bool) -> String {
    if quoted || case_sensitive {
        value.to_string()
    } else {
        value.to_ascii_lowercase()
    }
}

fn syntactic_source_names(source: &Query, case_sensitive: bool) -> Option<Vec<SourceName>> {
    let SetExpr::Select(select) = source.body.as_ref() else {
        return None;
    };
    select
        .projection
        .iter()
        .map(|item| match item {
            datafusion::sql::sqlparser::ast::SelectItem::ExprWithAlias { alias, .. } => {
                Some(SourceName {
                    display: alias.value.clone(),
                    resolved: normalize_ident(
                        &alias.value,
                        alias.quote_style.is_some(),
                        case_sensitive,
                    ),
                })
            }
            datafusion::sql::sqlparser::ast::SelectItem::UnnamedExpr(
                datafusion::sql::sqlparser::ast::Expr::Identifier(ident),
            ) => Some(SourceName {
                display: ident.value.clone(),
                resolved: normalize_ident(
                    &ident.value,
                    ident.quote_style.is_some(),
                    case_sensitive,
                ),
            }),
            datafusion::sql::sqlparser::ast::SelectItem::UnnamedExpr(
                datafusion::sql::sqlparser::ast::Expr::CompoundIdentifier(parts),
            ) => parts.last().map(|ident| SourceName {
                display: ident.value.clone(),
                resolved: normalize_ident(
                    &ident.value,
                    ident.quote_style.is_some(),
                    case_sensitive,
                ),
            }),
            _ => None,
        })
        .collect()
}

fn match_source_to_target(
    targets: &[String],
    sources: &[SourceName],
    table_display: &str,
    case_sensitive: bool,
) -> Result<Vec<Option<usize>>> {
    if sources.len() > targets.len() {
        let data: Vec<String> = sources.iter().map(|name| name.display.clone()).collect();
        return Err(too_many_columns(table_display, targets, &data));
    }
    let folded: Vec<String> = sources
        .iter()
        .map(|name| fold_name(&name.resolved, case_sensitive))
        .collect();
    let mut seen = HashSet::new();
    for key in &folded {
        if !seen.insert(key.clone()) {
            let first = folded.iter().position(|other| other == key);
            let spelling = first.map_or(key.as_str(), |index| sources[index].display.as_str());
            return Err(ambiguous_column(table_display, spelling));
        }
    }
    let target_folded: Vec<String> = targets
        .iter()
        .map(|name| fold_name(name, case_sensitive))
        .collect();
    let extra: Vec<&str> = sources
        .iter()
        .enumerate()
        .filter(|(index, _)| !target_folded.contains(&folded[*index]))
        .map(|(_, name)| name.display.as_str())
        .collect();
    if !extra.is_empty() {
        return Err(extra_columns(table_display, &extra));
    }
    Ok(targets
        .iter()
        .map(|target| {
            let wanted = fold_name(target, case_sensitive);
            folded.iter().position(|key| *key == wanted)
        })
        .collect())
}

fn quote_name(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn too_many_columns(table: &str, targets: &[String], sources: &[String]) -> DataFusionError {
    let table_columns = targets
        .iter()
        .map(|name| quote_name(name))
        .collect::<Vec<_>>()
        .join(", ");
    let data_columns = sources
        .iter()
        .map(|name| quote_name(name))
        .collect::<Vec<_>>()
        .join(", ");
    DataFusionError::Plan(format!(
        "[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS] Cannot write to {table}, the \
         reason is too many data columns:\nTable columns: {table_columns}.\nData columns: \
         {data_columns}. SQLSTATE: 21S01"
    ))
}

fn ambiguous_column(table: &str, name: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[INCOMPATIBLE_DATA_FOR_TABLE.AMBIGUOUS_COLUMN_NAME] Cannot write incompatible data for \
         the table {table}: Ambiguous column name in the input data {}. SQLSTATE: KD000",
        quote_name(name)
    ))
}

fn extra_columns(table: &str, names: &[&str]) -> DataFusionError {
    let columns = names
        .iter()
        .map(|name| quote_name(name))
        .collect::<Vec<_>>()
        .join(", ");
    DataFusionError::Plan(format!(
        "[INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS] Cannot write incompatible data for the \
         table {table}: Cannot write extra columns {columns}. SQLSTATE: KD000"
    ))
}

fn build_projection_sql(
    source: &Query,
    targets: &[String],
    sources: &[SourceName],
    fills: &[TargetFill],
) -> String {
    let items = targets
        .iter()
        .zip(fills.iter())
        .map(|(target, fill)| match fill {
            TargetFill::Source(index) => format!(
                "{} AS {}",
                quote_name(&sources[*index].resolved),
                quote_name(target)
            ),
            TargetFill::Null => format!("NULL AS {}", quote_name(target)),
            TargetFill::Static(literal) => format!("{literal} AS {}", quote_name(target)),
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {items} FROM ({source}) AS _repark_by_name_src")
}

fn build_static_append_projection(
    source: &Query,
    fields: &[(String, bool)],
    static_columns: &[StaticColumn],
    sources: &[SourceName],
    mapping: &[Option<usize>],
    case_sensitive: bool,
) -> String {
    let mut slots = mapping.iter();
    let mut targets = Vec::with_capacity(fields.len());
    let mut fills = Vec::with_capacity(fields.len());
    for (name, _) in fields {
        if let Some(static_column) = static_columns
            .iter()
            .find(|static_column| same_name(&static_column.canonical, name, case_sensitive))
        {
            targets.push(name.clone());
            fills.push(TargetFill::Static(static_column.literal_sql.clone()));
        } else {
            targets.push(name.clone());
            fills.push(match slots.next() {
                Some(Some(index)) => TargetFill::Source(*index),
                _ => TargetFill::Null,
            });
        }
    }
    build_projection_sql(source, &targets, sources, &fills)
}

fn parse_projection_query(sql: &str) -> Result<Box<Query>> {
    let dialect = DatabricksDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).map_err(|error| {
        DataFusionError::Plan(format!(
            "INSERT … BY NAME projection failed to plan: {error}"
        ))
    })?;
    if statements.len() != 1 {
        return Err(DataFusionError::Plan(
            "INSERT … BY NAME projection failed to plan".to_string(),
        ));
    }
    match statements.pop() {
        Some(Statement::Query(query)) => Ok(query),
        _ => Err(DataFusionError::Plan(
            "INSERT … BY NAME projection failed to plan".to_string(),
        )),
    }
}

fn parse_syntax_error(message: String) -> DataFusionError {
    DataFusionError::SQL(Box::new(ParserError::ParserError(message)), None)
}

pub(crate) fn strip_insert_by_name(sql: &str) -> Result<Option<String>> {
    match classify_insert_by_name(sql) {
        ByNameShape::Absent => Ok(None),
        ByNameShape::ColumnList => Err(parse_syntax_error(
            "[PARSE_SYNTAX_ERROR] BY NAME cannot be combined with an explicit column list. \
             SQLSTATE: 42601"
                .to_string(),
        )),
        ByNameShape::Present { sql: stripped } => Ok(Some(stripped)),
    }
}

pub(crate) fn sql_has_insert_by_name(sql: &str) -> bool {
    !matches!(classify_insert_by_name(sql), ByNameShape::Absent)
}

enum ByNameShape {
    Absent,
    ColumnList,
    Present { sql: String },
}

fn might_contain_by_name(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut index = 0;
    let mut saw_by = false;
    let mut saw_name = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte.is_ascii_alphabetic() || byte == b'_' {
            let start = index;
            while index < bytes.len()
                && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
            {
                index += 1;
            }
            match sql[start..index].to_ascii_uppercase().as_str() {
                "BY" => saw_by = true,
                "NAME" => saw_name = true,
                _ => {}
            }
            if saw_by && saw_name {
                return true;
            }
        } else {
            index += 1;
        }
    }
    false
}

fn classify_insert_by_name(sql: &str) -> ByNameShape {
    if !might_contain_by_name(sql) {
        return ByNameShape::Absent;
    }
    let dialect = DatabricksDialect {};
    let Ok(spanned) = Tokenizer::new(&dialect, sql).tokenize_with_location() else {
        return ByNameShape::Absent;
    };
    let meaningful: Vec<usize> = spanned
        .iter()
        .enumerate()
        .filter(|(_, item)| !matches!(item.token, Token::Whitespace(_)))
        .map(|(index, _)| index)
        .collect();
    if meaningful.is_empty() {
        return ByNameShape::Absent;
    }
    if !is_word_at(&spanned, meaningful[0], "INSERT") {
        return ByNameShape::Absent;
    }
    let source_start = find_source_start(&spanned, &meaningful);
    let Some((by_token, name_token)) = find_by_name_pair(&spanned, &meaningful, source_start)
    else {
        return ByNameShape::Absent;
    };
    if has_prefix_parens(&spanned, &meaningful, by_token, source_start) {
        return ByNameShape::ColumnList;
    }
    let Some((by_start, name_end)) =
        token_span_offsets(sql, &spanned[by_token], &spanned[name_token])
    else {
        return ByNameShape::Absent;
    };
    ByNameShape::Present {
        sql: format!("{}{}", &sql[..by_start], &sql[name_end..]),
    }
}

fn is_word_at(
    spanned: &[datafusion::sql::sqlparser::tokenizer::TokenWithSpan],
    index: usize,
    word: &str,
) -> bool {
    match spanned.get(index).map(|item| &item.token) {
        Some(Token::Word(found)) => {
            found.quote_style.is_none() && found.value.eq_ignore_ascii_case(word)
        }
        _ => false,
    }
}

fn find_source_start(
    spanned: &[datafusion::sql::sqlparser::tokenizer::TokenWithSpan],
    meaningful: &[usize],
) -> Option<usize> {
    let mut depth = 0usize;
    for position in 0..meaningful.len() {
        let index = meaningful[position];
        match &spanned[index].token {
            Token::LParen => {
                depth += 1;
                if depth == 1 && paren_opens_query(spanned, meaningful, position) {
                    return Some(index);
                }
            }
            Token::RParen => {
                depth = depth.saturating_sub(1);
            }
            Token::Word(found)
                if depth == 0
                    && found.quote_style.is_none()
                    && ["SELECT", "VALUES", "WITH"]
                        .iter()
                        .any(|keyword| found.value.eq_ignore_ascii_case(keyword)) =>
            {
                return Some(index);
            }
            _ => {}
        }
    }
    None
}

fn paren_opens_query(
    spanned: &[datafusion::sql::sqlparser::tokenizer::TokenWithSpan],
    meaningful: &[usize],
    position: usize,
) -> bool {
    meaningful
        .get(position + 1)
        .is_some_and(|next| is_word_at(spanned, *next, "SELECT"))
        || meaningful
            .get(position + 1)
            .is_some_and(|next| is_word_at(spanned, *next, "VALUES"))
        || meaningful
            .get(position + 1)
            .is_some_and(|next| is_word_at(spanned, *next, "WITH"))
}

fn find_by_name_pair(
    spanned: &[datafusion::sql::sqlparser::tokenizer::TokenWithSpan],
    meaningful: &[usize],
    source_start: Option<usize>,
) -> Option<(usize, usize)> {
    let mut depth = 0usize;
    for position in 0..meaningful.len() {
        let index = meaningful[position];
        match &spanned[index].token {
            Token::LParen => {
                depth += 1;
            }
            Token::RParen => {
                depth = depth.saturating_sub(1);
            }
            _ => {}
        }
        if depth != 0 {
            continue;
        }
        if source_start.is_some_and(|start| index >= start) {
            break;
        }
        if is_word_at(spanned, index, "BY")
            && let Some(after) = meaningful.get(position + 1).copied()
            && is_word_at(spanned, after, "NAME")
        {
            return Some((index, after));
        }
    }
    None
}

fn has_prefix_parens(
    spanned: &[datafusion::sql::sqlparser::tokenizer::TokenWithSpan],
    meaningful: &[usize],
    pair: usize,
    source_start: Option<usize>,
) -> bool {
    let mut depth = 0usize;
    for position in 0..meaningful.len() {
        let index = meaningful[position];
        if Some(index) == source_start || index >= pair {
            break;
        }
        match &spanned[index].token {
            Token::LParen => {
                if depth == 0 && !is_partition_paren(spanned, meaningful, position) {
                    return true;
                }
                depth += 1;
            }
            Token::RParen => {
                depth = depth.saturating_sub(1);
            }
            _ => {}
        }
    }
    false
}

fn is_partition_paren(
    spanned: &[datafusion::sql::sqlparser::tokenizer::TokenWithSpan],
    meaningful: &[usize],
    position: usize,
) -> bool {
    position > 0
        && meaningful
            .get(position - 1)
            .is_some_and(|previous| is_word_at(spanned, *previous, "PARTITION"))
}

fn token_span_offsets(
    sql: &str,
    first: &datafusion::sql::sqlparser::tokenizer::TokenWithSpan,
    second: &datafusion::sql::sqlparser::tokenizer::TokenWithSpan,
) -> Option<(usize, usize)> {
    let lines: Vec<&str> = sql.split('\n').collect();
    let offset_of = |line: u64, column: u64| -> Option<usize> {
        let before = usize::try_from(line.saturating_sub(1)).ok()?;
        let text = lines.get(before)?;
        let mut base: usize = 0;
        for earlier in lines.iter().take(before) {
            base += earlier.len() + 1;
        }
        let skip = usize::try_from(column.saturating_sub(1)).ok()?;
        let byte = text
            .char_indices()
            .nth(skip)
            .map_or(text.len(), |(index, _)| index);
        Some(base + byte)
    };
    let by_start = offset_of(first.span.start.line, first.span.start.column)?;
    let name_end = offset_of(second.span.end.line, second.span.end.column)?;
    (by_start <= name_end).then_some((by_start, name_end))
}

#[cfg(test)]
mod tests;
