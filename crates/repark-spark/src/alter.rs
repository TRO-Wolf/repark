//! `ALTER TABLE` routing and token rewrites for Spark forms sqlparser cannot model.

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    AlterTableOperation, ColumnDef, ColumnOption, MySQLColumnPosition, ObjectName,
    RenameTableNameKind,
};
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::{Token, Word};
use iceberg::spec::Transform;
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;
use repark_iceberg::write::alter::{ColumnPosition, PartitionSpecChange, SchemaChange};

use crate::create_table::sql_type_to_iceberg_with_timestamp_type;
use crate::replace_columns::ReplaceColumnDef;
use crate::{
    PartitionFieldSpec, build_transform_field, catalog_handle, iceberg_err, name_parts,
    property_value, rename_dest, reregister,
};
use repark_functions::timestamp_type::{SparkTimestampType, spark_timestamp_type_from_options};

/// The value an `UNSET TBLPROPERTIES` key carries after the token rewrite.
const UNSET_SENTINEL: &str = "__repark_unset_tblproperty_sentinel__";

/// Execute an `ALTER TABLE catalog.namespace.table <op>…` against the iceberg catalog.
/// # Errors
/// Propagates name-resolution, iceberg, and re-registration errors as [`DataFusionError`].
pub(crate) async fn execute_alter_table(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    name: &ObjectName,
    operations: &[AlterTableOperation],
) -> Result<DataFrame> {
    let (catalog_name, mut ident) = resolve_table(catalogs, name)?;
    let table_display = crate::catalog_ops::quoted_table_display(&name_parts(name));
    let handle = catalog_handle(catalogs, &catalog_name)?;
    let timestamp_type = spark_timestamp_type_from_options(ctx.copied_config().options());
    let mut schema_batch: Vec<SchemaChange> = Vec::new();
    let mut schema_dirty = false;

    for operation in operations {
        match operation {
            AlterTableOperation::SetTblProperties { table_properties } => {
                flush_schema_batch(handle.as_ref(), &ident, &mut schema_batch).await?;
                let (sets, unsets) = partition_tblproperties(table_properties);
                crate::format_version::alter_set_tblproperties(
                    ctx,
                    handle.as_ref(),
                    &ident,
                    sets,
                    &unsets,
                )
                .await?;
            }
            AlterTableOperation::RenameTable {
                table_name: RenameTableNameKind::To(dest_name),
            } => {
                flush_schema_batch(handle.as_ref(), &ident, &mut schema_batch).await?;
                // Subsequent ops must target the new ident after RENAME TO.
                ident = execute_rename_table(ctx, handle.clone(), &catalog_name, &ident, dest_name)
                    .await?;
            }
            AlterTableOperation::AddColumn {
                column_def,
                column_position,
                if_not_exists,
                ..
            } => {
                let change = schema_change_from_add_column(
                    column_def,
                    column_position.as_ref(),
                    timestamp_type,
                )?;
                if *if_not_exists {
                    // Iceberg has no IF NOT EXISTS on ADD.
                    if column_exists(handle.as_ref(), &ident, &column_def.name.value).await? {
                        continue;
                    }
                }
                schema_batch.push(change);
                schema_dirty = true;
            }
            AlterTableOperation::DropColumn {
                column_names,
                if_exists,
                ..
            } => {
                for column in column_names {
                    if *if_exists && !column_exists(handle.as_ref(), &ident, &column.value).await? {
                        continue;
                    }
                    schema_batch.push(SchemaChange::DropColumn {
                        name: column.value.clone(),
                    });
                    schema_dirty = true;
                }
            }
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                schema_batch.push(SchemaChange::RenameColumn {
                    from: old_column_name.value.clone(),
                    to: new_column_name.value.clone(),
                });
                schema_dirty = true;
            }
            AlterTableOperation::AlterColumn { column_name, op } => {
                if crate::alter_column_type::push_alter_column_change(
                    handle.as_ref(),
                    &ident,
                    column_name,
                    op,
                    timestamp_type,
                    &mut schema_batch,
                )
                .await?
                {
                    schema_dirty = true;
                }
            }
            other => {
                flush_schema_batch(handle.as_ref(), &ident, &mut schema_batch).await?;
                return Err(unsupported_alter_op(other, &table_display));
            }
        }
    }
    flush_schema_batch(handle.as_ref(), &ident, &mut schema_batch).await?;
    if schema_dirty {
        let namespace = crate::namespace_schema_name(ident.namespace());
        reregister(ctx, handle.clone(), &catalog_name, &namespace).await?;
    }
    ctx.read_empty()
}

/// Commit any pending schema changes as ONE `UpdateSchema` transaction (or no-op if empty).
async fn flush_schema_batch(
    catalog: &dyn iceberg::Catalog,
    ident: &TableIdent,
    batch: &mut Vec<SchemaChange>,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let changes = std::mem::take(batch);
    repark_iceberg::write::alter::apply_schema_changes(catalog, ident, &changes)
        .await
        .map_err(iceberg_err)
}

/// Apply RENAME TO + namespace-scoped provider invalidation; returns the new table ident.
async fn execute_rename_table(
    ctx: &SessionContext,
    handle: std::sync::Arc<dyn iceberg::Catalog>,
    catalog_name: &str,
    src_ident: &TableIdent,
    dest_name: &ObjectName,
) -> Result<TableIdent> {
    let dest_ident = rename_dest(src_ident.namespace(), dest_name)?;
    if let Err(error) =
        repark_iceberg::write::alter::rename_table(handle.as_ref(), src_ident, &dest_ident).await
    {
        return Err(
            crate::use_ddl::rename_error(handle.as_ref(), error, src_ident, &dest_ident).await,
        );
    }
    let src_namespace = crate::namespace_schema_name(src_ident.namespace());
    let dest_namespace = crate::namespace_schema_name(dest_ident.namespace());
    if src_namespace == dest_namespace {
        reregister(ctx, handle, catalog_name, &src_namespace).await?;
    } else {
        crate::reregister_namespaces(
            ctx,
            handle,
            catalog_name,
            &[&src_namespace, &dest_namespace],
        )
        .await?;
    }
    Ok(dest_ident)
}

/// Map an ADD COLUMN AST to a [`SchemaChange`], accepting NULL / NOT NULL / COMMENT only.
fn schema_change_from_add_column(
    column_def: &ColumnDef,
    column_position: Option<&MySQLColumnPosition>,
    timestamp_type: SparkTimestampType,
) -> Result<SchemaChange> {
    let field_type =
        sql_type_to_iceberg_with_timestamp_type(&column_def.data_type, timestamp_type)?;
    let mut required = false;
    let mut doc: Option<String> = None;
    for option in &column_def.options {
        match &option.option {
            ColumnOption::NotNull => required = true,
            ColumnOption::Null => {}
            ColumnOption::Comment(text) => doc = Some(text.clone()),
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "ALTER TABLE ADD COLUMN option `{other}` on `{}` is not supported yet — \
                     only NULL / NOT NULL / COMMENT are accepted",
                    column_def.name.value
                )));
            }
        }
    }
    if required {
        // Iceberg rejects required ADD without a default unless allow_incompatible_changes.
        return Err(DataFusionError::NotImplemented(format!(
            "ALTER TABLE ADD COLUMN `{}` NOT NULL is not supported yet — Iceberg treats a \
             required add without a default as an incompatible change; add the column as \
             nullable (omit NOT NULL), or use a write-default path (out of I6 READY)",
            column_def.name.value
        )));
    }
    let position = match column_position {
        Some(MySQLColumnPosition::First) => Some(ColumnPosition::First),
        Some(MySQLColumnPosition::After(ident)) => Some(ColumnPosition::After(ident.value.clone())),
        None => None,
    };
    Ok(SchemaChange::AddColumn {
        name: column_def.name.value.clone(),
        field_type,
        doc,
        required: false,
        position,
    })
}

fn unsupported_alter_op(other: &AlterTableOperation, table_display: &str) -> DataFusionError {
    let rendered = other.to_string();
    let lower = rendered.to_lowercase();
    if lower.contains("replace") && lower.contains("column") {
        return DataFusionError::NotImplemented(
            "ALTER TABLE REPLACE COLUMNS is not supported via the stock AST path — use the \
             dedicated REPLACE COLUMNS form (I7) or ADD/DROP/RENAME COLUMN"
                .into(),
        );
    }
    if lower.contains("partition") {
        return crate::catalog_ops::partition_management_unsupported(table_display);
    }
    DataFusionError::NotImplemented(format!("ALTER TABLE operation not supported yet: {other}"))
}

/// True when the current schema already has a top-level field named `column` (case-insensitive).
async fn column_exists(
    catalog: &dyn iceberg::Catalog,
    ident: &TableIdent,
    column: &str,
) -> Result<bool> {
    let table = catalog.load_table(ident).await.map_err(iceberg_err)?;
    let needle = column.to_ascii_lowercase();
    Ok(table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .any(|field| field.name.to_ascii_lowercase() == needle))
}

/// Split a parsed `SET TBLPROPERTIES` option list into real sets vs.
fn partition_tblproperties(
    table_properties: &[datafusion::sql::sqlparser::ast::SqlOption],
) -> (std::collections::HashMap<String, String>, Vec<String>) {
    use datafusion::sql::sqlparser::ast::SqlOption;
    let mut sets = std::collections::HashMap::new();
    let mut unsets = Vec::new();
    for option in table_properties {
        if let SqlOption::KeyValue { key, value } = option {
            let rendered = property_value(value);
            if rendered == UNSET_SENTINEL {
                unsets.push(key.value.clone());
            } else {
                sets.insert(key.value.clone(), rendered);
            }
        }
    }
    (sets, unsets)
}

fn resolve_table(catalogs: &CatalogRegistry, name: &ObjectName) -> Result<(String, TableIdent)> {
    let parts = crate::use_ddl::complete_name(catalogs, &name_parts(name))?;
    let [catalog, namespace, table] = parts.as_slice() else {
        return Err(DataFusionError::Plan(format!(
            "ALTER TABLE expects a three-part `catalog.namespace.table` name, got `{name}`"
        )));
    };
    Ok((
        catalog.clone(),
        TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone()),
    ))
}

pub(crate) fn rewrite_unset_tblproperties(tokens: &[Token]) -> Vec<Token> {
    let keyword_at = |kw: Keyword| {
        tokens
            .iter()
            .position(|t| matches!(t, Token::Word(w) if w.keyword == kw))
    };
    let (Some(alter), Some(table)) = (keyword_at(Keyword::ALTER), keyword_at(Keyword::TABLE))
    else {
        return tokens.to_vec();
    };
    let mut unset_index = None;
    let mut tblprops_index = None;
    for (index, token) in tokens.iter().enumerate() {
        if !matches!(token, Token::Word(word) if word.keyword == Keyword::UNSET) {
            continue;
        }
        let Some(next) = next_significant(tokens, index + 1) else {
            continue;
        };
        if is_word_keyword(&tokens[next], Keyword::TBLPROPERTIES) {
            unset_index = Some(index);
            tblprops_index = Some(next);
            break;
        }
    }
    let (Some(unset), Some(tblprops)) = (unset_index, tblprops_index) else {
        return tokens.to_vec();
    };
    if !(alter < table && table < unset && unset < tblprops) {
        return tokens.to_vec();
    }

    let mut out = Vec::with_capacity(tokens.len() + 8);
    let mut depth: i32 = 0;
    let mut seen_open = false;
    let if_exists = crate::table_props_ddl::unset_if_exists_pair(tokens, tblprops);
    for (i, token) in tokens.iter().enumerate() {
        if if_exists.is_some_and(|pair| pair.contains(&i)) {
            continue;
        }
        if i == unset {
            out.push(Token::Word(Word {
                value: "SET".to_string(),
                quote_style: None,
                keyword: Keyword::SET,
            }));
            continue;
        }
        out.push(token.clone());
        if i <= tblprops {
            continue;
        }
        match token {
            Token::LParen => {
                depth += 1;
                seen_open = true;
            }
            Token::RParen => depth -= 1,
            Token::SingleQuotedString(_) | Token::DoubleQuotedString(_) | Token::Word(_)
                if seen_open && depth == 1 && next_terminates_key(&tokens[i + 1..]) =>
            {
                out.push(Token::Eq);
                out.push(Token::SingleQuotedString(UNSET_SENTINEL.to_string()));
            }
            _ => {}
        }
    }
    out
}

/// True if the next non-whitespace token closes a bare property key (a `,` or `)`), i.e.
fn next_terminates_key(rest: &[Token]) -> bool {
    rest.iter()
        .find(|t| !matches!(t, Token::Whitespace(_)))
        .is_some_and(|t| matches!(t, Token::Comma | Token::RParen))
}

/// Rewrite Spark `ADD COLUMNS (...)` into repeated `ADD COLUMN` so stock sqlparser accepts it.
pub(crate) fn rewrite_add_columns_plural(tokens: &[Token]) -> Vec<Token> {
    // Locate ADD + COLUMNS (not COLUMN) with a following `(`.
    let mut index = 0;
    while index + 1 < tokens.len() {
        if is_word_keyword(&tokens[index], Keyword::ADD) {
            let Some(columns_index) = next_significant(tokens, index + 1) else {
                break;
            };
            if is_word_value(&tokens[columns_index], "COLUMNS") {
                let Some(open_index) = next_significant(tokens, columns_index + 1) else {
                    break;
                };
                if matches!(tokens.get(open_index), Some(Token::LParen)) {
                    return rewrite_add_columns_at(tokens, index, open_index);
                }
            }
        }
        index += 1;
    }
    tokens.to_vec()
}

fn rewrite_add_columns_at(tokens: &[Token], add_index: usize, open_index: usize) -> Vec<Token> {
    // Find matching `)` for the parenthesised column-def list.
    let mut depth = 0_i32;
    let mut close_index = None;
    for (offset, token) in tokens.iter().enumerate().skip(open_index) {
        match token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    close_index = Some(offset);
                    break;
                }
            }
            _ => {}
        }
    }
    let Some(close_index) = close_index else {
        return tokens.to_vec();
    };
    let inner = &tokens[open_index + 1..close_index];
    let defs = split_top_level_comma_segments(inner);
    if defs.is_empty() {
        return tokens.to_vec();
    }
    let mut out = Vec::with_capacity(tokens.len() + defs.len() * 4);
    out.extend_from_slice(&tokens[..add_index]);
    for (def_index, def) in defs.iter().enumerate() {
        if def_index > 0 {
            out.push(Token::Comma);
            out.push(Token::Whitespace(
                datafusion::sql::sqlparser::tokenizer::Whitespace::Space,
            ));
        }
        out.push(Token::Word(Word {
            value: "ADD".into(),
            quote_style: None,
            keyword: Keyword::ADD,
        }));
        out.push(Token::Whitespace(
            datafusion::sql::sqlparser::tokenizer::Whitespace::Space,
        ));
        out.push(Token::Word(Word {
            value: "COLUMN".into(),
            quote_style: None,
            keyword: Keyword::COLUMN,
        }));
        out.push(Token::Whitespace(
            datafusion::sql::sqlparser::tokenizer::Whitespace::Space,
        ));
        out.extend_from_slice(def);
    }
    out.extend_from_slice(&tokens[close_index + 1..]);
    out
}

/// Rewrite Spark `DROP COLUMNS (a, b)` / `DROP COLUMNS a, b` into `DROP COLUMN a, DROP COLUMN b`.
pub(crate) fn rewrite_drop_columns_plural(tokens: &[Token]) -> Vec<Token> {
    let mut index = 0;
    while index + 1 < tokens.len() {
        if is_word_keyword(&tokens[index], Keyword::DROP) {
            let Some(columns_index) = next_significant(tokens, index + 1) else {
                break;
            };
            if is_word_value(&tokens[columns_index], "COLUMNS") {
                return rewrite_drop_columns_at(tokens, index, columns_index);
            }
        }
        index += 1;
    }
    tokens.to_vec()
}

fn rewrite_drop_columns_at(
    tokens: &[Token],
    drop_index: usize,
    columns_index: usize,
) -> Vec<Token> {
    let Some(after_columns) = next_significant(tokens, columns_index + 1) else {
        return tokens.to_vec();
    };
    let (names, rest_start) = if matches!(tokens.get(after_columns), Some(Token::LParen)) {
        let mut depth = 0_i32;
        let mut close_index = None;
        for (offset, token) in tokens.iter().enumerate().skip(after_columns) {
            match token {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        close_index = Some(offset);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(close_index) = close_index else {
            return tokens.to_vec();
        };
        let inner = &tokens[after_columns + 1..close_index];
        (split_top_level_comma_segments(inner), close_index + 1)
    } else {
        // Bare `DROP COLUMNS a, b` — consume identifiers + commas until a non-name token.
        let mut end = after_columns;
        let mut cursor = after_columns;
        loop {
            match tokens.get(cursor) {
                Some(Token::Word(_) | Token::DoubleQuotedString(_)) => {
                    end = cursor + 1;
                    cursor += 1;
                }
                Some(Token::Whitespace(_) | Token::Comma) => {
                    cursor += 1;
                }
                _ => break,
            }
        }
        (
            split_top_level_comma_segments(&tokens[after_columns..end]),
            end,
        )
    };
    if names.is_empty() {
        return tokens.to_vec();
    }
    let mut out = Vec::with_capacity(tokens.len() + names.len() * 4);
    out.extend_from_slice(&tokens[..drop_index]);
    for (name_index, name_tokens) in names.iter().enumerate() {
        if name_index > 0 {
            out.push(Token::Comma);
            out.push(Token::Whitespace(
                datafusion::sql::sqlparser::tokenizer::Whitespace::Space,
            ));
        }
        out.push(Token::Word(Word {
            value: "DROP".into(),
            quote_style: None,
            keyword: Keyword::DROP,
        }));
        out.push(Token::Whitespace(
            datafusion::sql::sqlparser::tokenizer::Whitespace::Space,
        ));
        out.push(Token::Word(Word {
            value: "COLUMN".into(),
            quote_style: None,
            keyword: Keyword::COLUMN,
        }));
        out.push(Token::Whitespace(
            datafusion::sql::sqlparser::tokenizer::Whitespace::Space,
        ));
        out.extend_from_slice(name_tokens);
    }
    out.extend_from_slice(&tokens[rest_start..]);
    out
}

/// Split `tokens` on top-level commas.
fn split_top_level_comma_segments(tokens: &[Token]) -> Vec<Vec<Token>> {
    let mut segments = Vec::new();
    let mut current = Vec::new();
    let mut paren_depth = 0_u32;
    let mut angle_depth = 0_u32;
    for token in tokens {
        match token {
            Token::LParen => paren_depth += 1,
            Token::RParen => paren_depth = paren_depth.saturating_sub(1),
            Token::Lt => angle_depth += 1,
            Token::Gt => angle_depth = angle_depth.saturating_sub(1),
            Token::ShiftLeft => angle_depth += 2,
            Token::ShiftRight => angle_depth = angle_depth.saturating_sub(2),
            Token::Comma if paren_depth == 0 && angle_depth == 0 => {
                let trimmed = trim_ws_tokens(std::mem::take(&mut current));
                if !trimmed.is_empty() {
                    segments.push(trimmed);
                }
                continue;
            }
            _ => {}
        }
        current.push(token.clone());
    }
    let trimmed = trim_ws_tokens(current);
    segments.extend((!trimmed.is_empty()).then_some(trimmed));
    segments
}

fn trim_ws_tokens(tokens: Vec<Token>) -> Vec<Token> {
    let mut out = tokens;
    while matches!(out.first(), Some(Token::Whitespace(_))) {
        out.remove(0);
    }
    while matches!(out.last(), Some(Token::Whitespace(_))) {
        out.pop();
    }
    out
}

pub(crate) fn next_significant(tokens: &[Token], from: usize) -> Option<usize> {
    tokens
        .iter()
        .enumerate()
        .skip(from)
        .find(|(_, token)| !matches!(token, Token::Whitespace(_)))
        .map(|(index, _)| index)
}

pub(crate) fn is_word_keyword(token: &Token, keyword: Keyword) -> bool {
    matches!(token, Token::Word(word) if word.keyword == keyword)
}

fn is_word_value(token: &Token, value: &str) -> bool {
    matches!(token, Token::Word(word) if word.value.eq_ignore_ascii_case(value))
}

/// True when the token stream looks like an `ALTER TABLE` statement (keyword spine).
pub(crate) fn tokens_are_alter_table(tokens: &[Token]) -> bool {
    let mut saw_alter = false;
    for token in tokens {
        match token {
            Token::Whitespace(_) => {}
            Token::Word(word) if !saw_alter && word.keyword == Keyword::ALTER => {
                saw_alter = true;
            }
            Token::Word(word) if saw_alter && word.keyword == Keyword::TABLE => {
                return true;
            }
            _ if saw_alter => return false,
            _ => return false,
        }
    }
    false
}

// === I7 PARTITION FIELD + REPLACE COLUMNS ===

/// A parsed I7 ALTER form that bypasses stock sqlparser.
#[derive(Debug, Clone)]
pub(crate) enum IcebergAlterDdl {
    /// One or more partition-spec evolution ops on a three-part table.
    PartitionSpec {
        table_parts: Vec<String>,
        /// Ordered ops (usually one; multi-clause future-proof).
        changes: Vec<PartitionSpecChange>,
    },
    ReplaceColumns {
        table_parts: Vec<String>,
        /// New top-level column list (order preserved).
        columns: Vec<ReplaceColumnDef>,
    },
}

/// Significant token for the I7 hand parser (mirrors `ref_ddl`).
#[derive(Debug, Clone)]
pub(crate) enum Sig {
    Word(String),
    Period,
    Number(String),
    LParen,
    RParen,
    Comma,
    /// Single-quoted string (COMMENT body).
    String(String),
    Other,
}

/// Try to parse Spark Iceberg `ADD|DROP|REPLACE PARTITION FIELD` or `REPLACE COLUMNS`.
pub(crate) fn try_parse_iceberg_alter_ddl(sql: &str) -> Option<Result<IcebergAlterDdl>> {
    let significant = tokenize_significant(sql)?;
    if significant.len() < 4 {
        return None;
    }
    if !(word_eq(&significant, 0, "ALTER") && word_eq(&significant, 1, "TABLE")) {
        return None;
    }
    let mut index = 2usize;
    word_at(&significant, index)?;
    let table_start = index;
    index += 1;
    while is_period_at(&significant, index) && word_at(&significant, index + 1).is_some() {
        index += 2;
    }
    let table_parts = collect_name_parts(&significant, table_start, index)?;

    // REPLACE COLUMNS (…)
    if word_eq(&significant, index, "REPLACE") && word_eq(&significant, index + 1, "COLUMNS") {
        return Some(crate::replace_columns::parse(sql, table_parts));
    }

    // ADD|DROP|REPLACE PARTITION FIELD …
    if word_eq(&significant, index, "ADD")
        && word_eq(&significant, index + 1, "PARTITION")
        && word_eq(&significant, index + 2, "FIELD")
    {
        return Some(parse_add_partition_field(
            &significant,
            index + 3,
            table_parts,
        ));
    }
    if word_eq(&significant, index, "DROP")
        && word_eq(&significant, index + 1, "PARTITION")
        && word_eq(&significant, index + 2, "FIELD")
    {
        return Some(parse_drop_partition_field(
            &significant,
            index + 3,
            table_parts,
        ));
    }
    if word_eq(&significant, index, "REPLACE")
        && word_eq(&significant, index + 1, "PARTITION")
        && word_eq(&significant, index + 2, "FIELD")
    {
        return Some(crate::replace_partition_field::parse(
            &significant,
            index + 3,
            table_parts,
        ));
    }
    None
}

/// Execute a parsed I7 Iceberg ALTER form (partition-spec evolution or REPLACE COLUMNS).
pub(crate) async fn execute_iceberg_alter_ddl(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    ddl: IcebergAlterDdl,
) -> Result<DataFrame> {
    match ddl {
        IcebergAlterDdl::PartitionSpec {
            table_parts,
            changes,
        } => {
            let (catalog_name, ident) = table_parts_to_ident(catalogs, &table_parts)?;
            let handle = catalog_handle(catalogs, &catalog_name)?;
            repark_iceberg::write::alter::apply_partition_spec_changes(
                handle.as_ref(),
                &ident,
                &changes,
            )
            .await
            .map_err(iceberg_err)?;
            let namespace = crate::namespace_schema_name(ident.namespace());
            reregister(ctx, handle.clone(), &catalog_name, &namespace).await?;
            ctx.read_empty()
        }
        IcebergAlterDdl::ReplaceColumns {
            table_parts,
            columns,
        } => {
            let (catalog_name, ident) = table_parts_to_ident(catalogs, &table_parts)?;
            let handle = catalog_handle(catalogs, &catalog_name)?;
            let timestamp_type = spark_timestamp_type_from_options(ctx.copied_config().options());
            let schema_changes =
                crate::replace_columns::plan(handle.as_ref(), &ident, &columns, timestamp_type)
                    .await?;
            repark_iceberg::write::alter::apply_schema_changes(
                handle.as_ref(),
                &ident,
                &schema_changes,
            )
            .await
            .map_err(iceberg_err)?;
            let namespace = crate::namespace_schema_name(ident.namespace());
            reregister(ctx, handle.clone(), &catalog_name, &namespace).await?;
            ctx.read_empty()
        }
    }
}

pub(crate) fn table_parts_to_ident(
    catalogs: &CatalogRegistry,
    parts: &[String],
) -> Result<(String, TableIdent)> {
    let completed = crate::use_ddl::complete_name(catalogs, parts)?;
    let [catalog, namespace, table] = completed.as_slice() else {
        return Err(DataFusionError::Plan(format!(
            "ALTER TABLE expects a three-part `catalog.namespace.table` name, got `{}`",
            parts.join(".")
        )));
    };
    Ok((
        catalog.clone(),
        TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone()),
    ))
}

fn parse_add_partition_field(
    significant: &[Sig],
    start: usize,
    table_parts: Vec<String>,
) -> Result<IcebergAlterDdl> {
    let (field_spec, name, next) = parse_partition_field_term(significant, start)?;
    // Optional AS name when not already consumed from term form.
    let (name, next) = if name.is_some() {
        (name, next)
    } else if word_eq(significant, next, "AS") {
        let alias = word_at(significant, next + 1).ok_or_else(|| {
            DataFusionError::Plan(
                "ALTER TABLE ADD PARTITION FIELD … AS expects a partition field name".into(),
            )
        })?;
        (Some(alias.to_string()), next + 2)
    } else {
        (None, next)
    };
    if next < significant.len() {
        return Err(DataFusionError::Plan(format!(
            "trailing tokens after ADD PARTITION FIELD (starting at `{}`)",
            render_sig_at(significant, next)
        )));
    }
    let change = partition_field_spec_to_add(&field_spec, name);
    Ok(IcebergAlterDdl::PartitionSpec {
        table_parts,
        changes: vec![change],
    })
}

fn parse_drop_partition_field(
    significant: &[Sig],
    start: usize,
    table_parts: Vec<String>,
) -> Result<IcebergAlterDdl> {
    // Bare name → RemoveFieldByName; transform(source) → RemoveFieldByTransform.
    if matches!(significant.get(start + 1), Some(Sig::LParen)) {
        let (field_spec, _alias, next) = parse_partition_field_term(significant, start)?;
        if next < significant.len() {
            return Err(DataFusionError::Plan(format!(
                "trailing tokens after DROP PARTITION FIELD (starting at `{}`)",
                render_sig_at(significant, next)
            )));
        }
        let change = partition_field_spec_to_remove_by_transform(&field_spec);
        return Ok(IcebergAlterDdl::PartitionSpec {
            table_parts,
            changes: vec![change],
        });
    }
    let name = word_at(significant, start).ok_or_else(|| {
        DataFusionError::Plan(
            "ALTER TABLE DROP PARTITION FIELD expects a partition field name or transform(…)"
                .into(),
        )
    })?;
    if start + 1 < significant.len() {
        return Err(DataFusionError::Plan(format!(
            "trailing tokens after DROP PARTITION FIELD `{name}`"
        )));
    }
    Ok(IcebergAlterDdl::PartitionSpec {
        table_parts,
        changes: vec![PartitionSpecChange::RemoveFieldByName {
            name: name.to_string(),
        }],
    })
}

/// Parse identity, column, bucket, truncate, and year transforms, optionally parenthesised.
pub(crate) fn parse_partition_field_term(
    significant: &[Sig],
    start: usize,
) -> Result<(PartitionFieldSpec, Option<String>, usize)> {
    // Optional wrapping parens: ( bucket(16, id) AS name )
    if matches!(significant.get(start), Some(Sig::LParen)) {
        let (inner_spec, inner_name, after_inner) =
            parse_partition_field_term_inner(significant, start + 1)?;
        if word_eq(significant, after_inner, "AS") {
            let alias = word_at(significant, after_inner + 1).ok_or_else(|| {
                DataFusionError::Plan(
                    "ADD PARTITION FIELD (… AS name) expects a partition field name".into(),
                )
            })?;
            let close = after_inner + 2;
            if !matches!(significant.get(close), Some(Sig::RParen)) {
                return Err(DataFusionError::Plan(
                    "ADD PARTITION FIELD (… AS name) expects closing `)`".into(),
                ));
            }
            return Ok((inner_spec, Some(alias.to_string()), close + 1));
        }
        if !matches!(significant.get(after_inner), Some(Sig::RParen)) {
            return Err(DataFusionError::Plan(
                "ADD PARTITION FIELD (…) expects closing `)`".into(),
            ));
        }
        return Ok((inner_spec, inner_name, after_inner + 1));
    }
    parse_partition_field_term_inner(significant, start)
}

fn parse_partition_field_term_inner(
    significant: &[Sig],
    start: usize,
) -> Result<(PartitionFieldSpec, Option<String>, usize)> {
    let head = word_at(significant, start).ok_or_else(|| {
        DataFusionError::Plan(
            "ADD PARTITION FIELD expects a source column or transform(…) expression".into(),
        )
    })?;
    // Transform call: name(args)
    if matches!(significant.get(start + 1), Some(Sig::LParen)) {
        let (args, after_args) = parse_paren_arg_list(significant, start + 1)?;
        let field_spec = build_transform_field(head, &args).map_err(|error| {
            // Retarget CTAS wording → partition-field wording.
            let message = error
                .to_string()
                .replace("CTAS PARTITIONED BY", "PARTITION FIELD");
            match error {
                DataFusionError::NotImplemented(_) => DataFusionError::NotImplemented(message),
                DataFusionError::Plan(_) => DataFusionError::Plan(message),
                other => other,
            }
        })?;
        return Ok((field_spec, None, after_args));
    }
    // Bare column → identity.
    Ok((
        PartitionFieldSpec::Identity(head.to_string()),
        None,
        start + 1,
    ))
}

/// Parse `(a, b, …)` starting at `LParen`; returns arg strings and index after `RParen`.
fn parse_paren_arg_list(significant: &[Sig], open_index: usize) -> Result<(Vec<String>, usize)> {
    if !matches!(significant.get(open_index), Some(Sig::LParen)) {
        return Err(DataFusionError::Plan(
            "expected `(` in partition field transform".into(),
        ));
    }
    let mut args = Vec::new();
    let mut index = open_index + 1;
    if matches!(significant.get(index), Some(Sig::RParen)) {
        return Ok((args, index + 1));
    }
    loop {
        let arg = match significant.get(index) {
            Some(Sig::Word(word)) => word.clone(),
            Some(Sig::Number(number)) => number.clone(),
            Some(other) => {
                return Err(DataFusionError::Plan(format!(
                    "unexpected token in partition field transform args: {other:?}"
                )));
            }
            None => {
                return Err(DataFusionError::Plan(
                    "unterminated partition field transform argument list".into(),
                ));
            }
        };
        args.push(arg);
        index += 1;
        match significant.get(index) {
            Some(Sig::Comma) => {
                index += 1;
            }
            Some(Sig::RParen) => return Ok((args, index + 1)),
            Some(other) => {
                return Err(DataFusionError::Plan(format!(
                    "expected `,` or `)` in transform args, got {other:?}"
                )));
            }
            None => {
                return Err(DataFusionError::Plan(
                    "unterminated partition field transform argument list".into(),
                ));
            }
        }
    }
}

fn partition_field_spec_to_add(
    field_spec: &PartitionFieldSpec,
    name: Option<String>,
) -> PartitionSpecChange {
    let (source_name, transform) = partition_field_spec_parts(field_spec);
    PartitionSpecChange::AddField {
        source_name,
        transform,
        name,
    }
}

fn partition_field_spec_to_remove_by_transform(
    field_spec: &PartitionFieldSpec,
) -> PartitionSpecChange {
    let (source_name, transform) = partition_field_spec_parts(field_spec);
    PartitionSpecChange::RemoveFieldByTransform {
        source_name,
        transform,
    }
}

pub(crate) fn partition_field_spec_parts(field_spec: &PartitionFieldSpec) -> (String, Transform) {
    (field_spec.column().to_string(), field_spec.transform())
}

pub(crate) fn tokenize_significant(sql: &str) -> Option<Vec<Sig>> {
    use datafusion::sql::sqlparser::dialect::DatabricksDialect;
    use datafusion::sql::sqlparser::tokenizer::Tokenizer;
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql).tokenize().ok()?;
    Some(
        tokens
            .into_iter()
            .filter_map(|token| match token {
                Token::Whitespace(_) | Token::EOF | Token::SemiColon => None,
                Token::Word(word) => Some(Sig::Word(word.value)),
                Token::Period => Some(Sig::Period),
                Token::Number(raw, _) => Some(Sig::Number(raw)),
                Token::LParen => Some(Sig::LParen),
                Token::RParen => Some(Sig::RParen),
                Token::Comma => Some(Sig::Comma),
                Token::SingleQuotedString(text) | Token::DoubleQuotedString(text) => {
                    Some(Sig::String(text))
                }
                _ => Some(Sig::Other),
            })
            .collect(),
    )
}

pub(crate) fn word_eq(significant: &[Sig], index: usize, expected: &str) -> bool {
    word_at(significant, index).is_some_and(|word| word.eq_ignore_ascii_case(expected))
}

pub(crate) fn word_at(significant: &[Sig], index: usize) -> Option<&str> {
    match significant.get(index) {
        Some(Sig::Word(word)) => Some(word.as_str()),
        _ => None,
    }
}

pub(crate) fn is_period_at(significant: &[Sig], index: usize) -> bool {
    matches!(significant.get(index), Some(Sig::Period))
}

pub(crate) fn collect_name_parts(
    significant: &[Sig],
    start: usize,
    end: usize,
) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    let mut index = start;
    while index < end {
        let part = word_at(significant, index)?.to_string();
        parts.push(part);
        index += 1;
        if index < end {
            if !is_period_at(significant, index) {
                return None;
            }
            index += 1;
        }
    }
    if parts.is_empty() { None } else { Some(parts) }
}

pub(crate) fn render_sig_at(significant: &[Sig], index: usize) -> String {
    match significant.get(index) {
        Some(Sig::Word(word)) => word.clone(),
        Some(Sig::Number(number)) => number.clone(),
        Some(Sig::Period) => ".".into(),
        Some(Sig::LParen) => "(".into(),
        Some(Sig::RParen) => ")".into(),
        Some(Sig::Comma) => ",".into(),
        Some(Sig::String(text)) => format!("'{text}'"),
        Some(Sig::Other) => "<other>".into(),
        None => "<eof>".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::{DatabricksDialect, GenericDialect};
    use datafusion::sql::sqlparser::parser::Parser as SqlParser;
    use datafusion::sql::sqlparser::tokenizer::Tokenizer;

    fn rewrite_and_parse(sql: &str) -> String {
        let dialect = DatabricksDialect {};
        let tokens = Tokenizer::new(&dialect, sql).tokenize().unwrap();
        let tokens = rewrite_add_columns_plural(&tokens);
        let tokens = rewrite_drop_columns_plural(&tokens);
        let generic_dialect = GenericDialect {};
        let statements = SqlParser::new(&generic_dialect)
            .with_tokens(tokens)
            .parse_statements()
            .unwrap();
        statements[0].to_string()
    }

    #[test]
    fn rewrite_add_columns_plural_to_multi_add_column() {
        let rendered =
            rewrite_and_parse("ALTER TABLE ice.sales.t ADD COLUMNS (c STRING COMMENT 'x', d INT)");
        assert!(
            rendered.to_ascii_uppercase().contains("ADD COLUMN"),
            "got: {rendered}"
        );
        assert!(
            rendered.contains('c') && rendered.contains('d'),
            "got: {rendered}"
        );
    }

    #[test]
    fn rewrite_drop_columns_plural_paren_form() {
        let rendered = rewrite_and_parse("ALTER TABLE ice.sales.t DROP COLUMNS (c, d)");
        assert!(
            rendered.to_ascii_uppercase().matches("DROP COLUMN").count() >= 2,
            "got: {rendered}"
        );
    }

    /// Bare `DROP COLUMNS a, b` (no parens) rewrites to multi DROP COLUMN.
    #[test]
    fn rewrite_drop_columns_plural_bare_form() {
        let rendered = rewrite_and_parse("ALTER TABLE ice.sales.t DROP COLUMNS c, d");
        assert!(
            rendered.to_ascii_uppercase().matches("DROP COLUMN").count() >= 2,
            "got: {rendered}"
        );
        assert!(
            rendered.contains('c') && rendered.contains('d'),
            "got: {rendered}"
        );
    }

    #[test]
    fn generic_dialect_parses_add_column_first_after() {
        let generic_dialect = GenericDialect {};
        for sql in [
            "ALTER TABLE ice.sales.t ADD COLUMN c STRING FIRST",
            "ALTER TABLE ice.sales.t ADD COLUMN c STRING AFTER id",
        ] {
            SqlParser::parse_sql(&generic_dialect, sql)
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
        }
    }

    #[test]
    fn parse_replace_columns_is_recognized() {
        let parsed = try_parse_iceberg_alter_ddl(
            "ALTER TABLE ice.sales.t REPLACE COLUMNS (a INT, b STRING)",
        )
        .expect("must recognize REPLACE COLUMNS")
        .expect("must parse");
        match parsed {
            IcebergAlterDdl::ReplaceColumns { columns, .. } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name, "a");
                assert_eq!(columns[1].name, "b");
            }
            IcebergAlterDdl::PartitionSpec { .. } => {
                panic!("expected ReplaceColumns, got PartitionSpec")
            }
        }
    }

    #[test]
    fn parse_add_drop_partition_field() {
        let add = try_parse_iceberg_alter_ddl(
            "ALTER TABLE ice.sales.t ADD PARTITION FIELD bucket(8, id) AS id_b8",
        )
        .expect("recognize")
        .expect("parse");
        match add {
            IcebergAlterDdl::PartitionSpec { changes, .. } => {
                assert_eq!(changes.len(), 1);
                match &changes[0] {
                    PartitionSpecChange::AddField {
                        source_name,
                        transform,
                        name,
                    } => {
                        assert_eq!(source_name, "id");
                        assert_eq!(*transform, Transform::Bucket(8));
                        assert_eq!(name.as_deref(), Some("id_b8"));
                    }
                    other => panic!("expected AddField, got {other:?}"),
                }
            }
            IcebergAlterDdl::ReplaceColumns { .. } => {
                panic!("expected PartitionSpec, got ReplaceColumns")
            }
        }

        let drop =
            try_parse_iceberg_alter_ddl("ALTER TABLE ice.sales.t DROP PARTITION FIELD id_b8")
                .expect("recognize")
                .expect("parse");
        match drop {
            IcebergAlterDdl::PartitionSpec { changes, .. } => match &changes[0] {
                PartitionSpecChange::RemoveFieldByName { name } => {
                    assert_eq!(name, "id_b8");
                }
                other => panic!("expected RemoveFieldByName, got {other:?}"),
            },
            IcebergAlterDdl::ReplaceColumns { .. } => {
                panic!("expected PartitionSpec, got ReplaceColumns")
            }
        }

        let unsupported =
            try_parse_iceberg_alter_ddl("ALTER TABLE ice.sales.t ADD PARTITION FIELD void(id)")
                .expect("recognize transform call");
        assert!(
            unsupported.is_err(),
            "unsupported transform must refuse loud"
        );
    }

    fn tokenize_databricks(sql: &str) -> Vec<Token> {
        let dialect = DatabricksDialect {};
        Tokenizer::new(&dialect, sql).tokenize().unwrap()
    }

    fn render_tokens(tokens: &[Token]) -> String {
        tokens
            .iter()
            .map(|token| match token {
                Token::Word(word) => word.value.clone(),
                Token::SingleQuotedString(text) => format!("'{text}'"),
                Token::DoubleQuotedString(text) => format!("\"{text}\""),
                Token::Eq => "=".into(),
                Token::Comma => ",".into(),
                Token::LParen => "(".into(),
                Token::RParen => ")".into(),
                Token::Period => ".".into(),
                Token::Whitespace(_) => " ".into(),
                other => format!("{other:?}"),
            })
            .collect()
    }

    /// Table named `unset` must not trigger the UNSET→SET rewrite.
    #[test]
    fn rewrite_unset_leaves_set_on_table_named_unset() {
        let sql = "ALTER TABLE ice.ns.unset SET TBLPROPERTIES('k'='v')";
        let rewritten = rewrite_unset_tblproperties(&tokenize_databricks(sql));
        let rendered = render_tokens(&rewritten);
        // Table name must survive; sentinel must NOT be injected on a real SET value.
        assert!(
            rendered.to_ascii_lowercase().contains("unset"),
            "table name `unset` must survive SET rewrite, got: {rendered}"
        );
        // The table-name token is `unset` (not rewritten to SET); operation is still SET.
        assert!(
            !rendered.to_ascii_uppercase().contains("NS.SET SET"),
            "must not rewrite table name into SET, got: {rendered}"
        );
        assert!(
            !rendered.contains(UNSET_SENTINEL),
            "SET form must not inject UNSET sentinel, got: {rendered}"
        );
    }

    /// Twin — real `UNSET TBLPROPERTIES` still rewrites to sentinel SET.
    #[test]
    fn rewrite_unset_still_rewrites_real_unset_tblproperties() {
        let sql = "ALTER TABLE ice.ns.t UNSET TBLPROPERTIES('owner')";
        let rewritten = rewrite_unset_tblproperties(&tokenize_databricks(sql));
        let rendered = render_tokens(&rewritten);
        assert!(
            rendered.contains(UNSET_SENTINEL),
            "real UNSET must inject sentinel, got: {rendered}"
        );
        assert!(
            rendered.to_ascii_uppercase().contains("SET"),
            "UNSET token must become SET, got: {rendered}"
        );
    }
}
