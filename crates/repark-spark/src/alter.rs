//! `ALTER TABLE` statement handling: routing + token normalisers for forms sqlparser cannot model.
//!
//! The router's `Statement::AlterTable` arm calls [`execute_alter_table`], which resolves the
//! three-part `catalog.namespace.table` target, dispatches each `AlterTableOperation` to the matching
//! [`repark_iceberg::write::alter`] transaction primitive, and re-registers the DataFusion provider after a
//! mutation (so a `RENAME`'s new name — and schema evolution — become queryable).
//!
//! ## Token rewrites
//!
//! - **`UNSET TBLPROPERTIES`** — sqlparser has no `UNSET` AST node; [`rewrite_unset_tblproperties`]
//!   normalises it into a sentinel-valued `SET` (BUG-012 / existing).
//! - **`ADD COLUMNS (…)`** — Spark's plural parenthesised form is not modelled; rewritten to a
//!   comma-separated list of `ADD COLUMN` ops ([`rewrite_add_columns_plural`]).
//! - **`DROP COLUMNS (…)` / `DROP COLUMNS a, b`** — same gap; rewritten to `DROP COLUMN` ops
//!   ([`rewrite_drop_columns_plural`]).
//!
//! ## FIRST / AFTER
//!
//! `MySQLColumnPosition` is only filled by MySQL/Generic dialects. ALTER TABLE is therefore parsed
//! with [`GenericDialect`] (see [`crate::parse_single_normalized`]) so Spark `ADD COLUMN … FIRST|
//! AFTER x` lands in the AST.
//!
//! ## Schema evolution (I6)
//!
//! READY: ADD COLUMN[S], DROP COLUMN, RENAME COLUMN, SET/UNSET TBLPROPERTIES, RENAME TO.
//! Stretch: ALTER COLUMN TYPE (widen with narrow-refuse twin), DROP NOT NULL, COMMENT.
//! Loud refuse: SET NOT NULL, unsupported options.
//!
//! ## Partition-spec evolution (I7)
//!
//! READY: `ADD PARTITION FIELD` / `DROP PARTITION FIELD` → fork `UpdatePartitionSpec`.
//! Stretch: `REPLACE PARTITION FIELD … WITH …`; `REPLACE COLUMNS` with identity-trap refuse.
//! Loud refuse: WRITE ORDERED/DISTRIBUTED BY; unsupported transforms.

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, ColumnDef, ColumnOption, DataType, ExactNumberInfo,
    Ident, MySQLColumnPosition, ObjectName, RenameTableNameKind, TimezoneInfo,
};
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::{Token, Word};
use iceberg::spec::{PrimitiveType, Transform, Type};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;
use repark_iceberg::write::alter::{ColumnPosition, PartitionSpecChange, SchemaChange};

use crate::create_table::{sql_type_to_iceberg, sql_type_to_iceberg_with_timestamp_type};
use crate::{
    PartitionFieldSpec, build_transform_field, catalog_handle, iceberg_err, name_parts,
    property_value, reregister,
};
use repark_functions::timestamp_type::{SparkTimestampType, spark_timestamp_type_from_options};

/// The value an `UNSET TBLPROPERTIES` key carries after the token rewrite, so the parsed
/// `SetTblProperties` op can be decoded back into a removal. Namespaced + unguessable so it cannot
/// collide with a value a user would actually `SET`.
const UNSET_SENTINEL: &str = "__repark_unset_tblproperty_sentinel__";

/// ===========================================================================================
/// Execute an `ALTER TABLE catalog.namespace.table <op>…` against the iceberg catalog.
///
/// Resolves the three-part target, then dispatches each operation:
/// - `SET TBLPROPERTIES (…)` → [`repark_iceberg::write::alter::alter_table_properties`] as ONE transaction
///   (real sets AND sentinel-flagged removals, from a rewritten `UNSET`, commit together — BUG-012).
/// - `RENAME TO catalog.namespace.table2` → [`repark_iceberg::write::alter::rename_table`] + re-register.
/// - Schema evolution (ADD/DROP/RENAME COLUMN, stretch ALTER COLUMN) → batched into ONE
///   [`repark_iceberg::write::alter::apply_schema_changes`] transaction per contiguous run, then re-register.
///
/// Unsupported operations error rather than silently no-op.
/// ===========================================================================================
///
/// # Errors
/// Propagates name-resolution, iceberg, and re-registration errors as [`DataFusionError`].
pub(crate) async fn execute_alter_table(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    name: &ObjectName,
    operations: &[AlterTableOperation],
) -> Result<DataFrame> {
    let (catalog_name, mut ident) = resolve_table(name)?;
    let handle = catalog_handle(catalogs, &catalog_name)?;
    let timestamp_type = spark_timestamp_type_from_options(ctx.copied_config().options());
    let mut schema_batch: Vec<SchemaChange> = Vec::new();
    let mut schema_dirty = false;

    for operation in operations {
        match operation {
            AlterTableOperation::SetTblProperties { table_properties } => {
                flush_schema_batch(handle.as_ref(), &ident, &mut schema_batch).await?;
                // A `SET TBLPROPERTIES` op may carry both real sets and sentinel-flagged removals
                // (from a token-rewritten `UNSET`). Commit them as ONE transaction so a mid-failure
                // can never leave half-applied property state (BUG-012).
                let (sets, unsets) = partition_tblproperties(table_properties);
                repark_iceberg::write::alter::alter_table_properties(
                    handle.as_ref(),
                    &ident,
                    &sets,
                    &unsets,
                )
                .await
                .map_err(iceberg_err)?;
            }
            AlterTableOperation::RenameTable {
                table_name: RenameTableNameKind::To(dest_name),
            } => {
                flush_schema_batch(handle.as_ref(), &ident, &mut schema_batch).await?;
                // octo C2: subsequent ops must target the new ident after RENAME TO.
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
                    // Iceberg has no IF NOT EXISTS on ADD; soft-skip when the name is already present.
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
                schema_batch.push(schema_change_from_alter_column(
                    column_name,
                    op,
                    timestamp_type,
                )?);
                schema_dirty = true;
            }
            other => {
                flush_schema_batch(handle.as_ref(), &ident, &mut schema_batch).await?;
                return Err(unsupported_alter_op(other));
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
    let (dest_catalog, dest_ident) = resolve_table(dest_name)?;
    if dest_catalog != catalog_name {
        return Err(DataFusionError::Plan(format!(
            "ALTER TABLE RENAME cannot move across catalogs (`{catalog_name}` → `{dest_catalog}`)"
        )));
    }
    let src_namespace = crate::namespace_schema_name(src_ident.namespace());
    let dest_namespace = crate::namespace_schema_name(dest_ident.namespace());
    repark_iceberg::write::alter::rename_table(handle.as_ref(), src_ident, &dest_ident)
        .await
        .map_err(iceberg_err)?;
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
        // Refuse loud rather than silently enabling incompatible evolution.
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

/// Map `ALTER COLUMN` ops: TYPE widen, DROP NOT NULL, refuse SET NOT NULL / DEFAULT / GENERATED.
fn schema_change_from_alter_column(
    column_name: &Ident,
    op: &AlterColumnOperation,
    timestamp_type: SparkTimestampType,
) -> Result<SchemaChange> {
    match op {
        AlterColumnOperation::SetDataType {
            data_type, using, ..
        } => {
            if using.is_some() {
                return Err(DataFusionError::NotImplemented(
                    "ALTER COLUMN … TYPE … USING is not supported (Iceberg promotions are \
                     metadata-only; no row rewrite)"
                        .into(),
                ));
            }
            let iceberg_type = sql_type_to_iceberg_with_timestamp_type(data_type, timestamp_type)?;
            let iceberg::spec::Type::Primitive(new_type) = iceberg_type else {
                return Err(DataFusionError::NotImplemented(format!(
                    "ALTER COLUMN `{}` TYPE to a non-primitive is not supported",
                    column_name.value
                )));
            };
            // Only promotion-shaped targets are accepted at the SQL boundary too (stretch rule:
            // any widen that lands ships with a narrow-refuse twin; non-promotion pairs refuse
            // loud here with a stable message before the fork gate).
            if !is_iceberg_promotion_target(&new_type) {
                return Err(DataFusionError::Plan(format!(
                    "ALTER COLUMN `{}` TYPE `{data_type}` is not an Iceberg type promotion \
                     target — only int→long, float→double, and decimal(p,s)→decimal(p2,s) with \
                     p2≥p (same scale) are allowed; narrowing refuses loud",
                    column_name.value
                )));
            }
            Ok(SchemaChange::UpdateColumnType {
                name: column_name.value.clone(),
                new_type,
            })
        }
        AlterColumnOperation::DropNotNull => Ok(SchemaChange::MakeColumnOptional {
            name: column_name.value.clone(),
        }),
        AlterColumnOperation::SetNotNull => Err(DataFusionError::NotImplemented(format!(
            "ALTER COLUMN `{}` SET NOT NULL is not supported — making a column required is an \
             Iceberg incompatible change (existing nulls cannot be backfilled without a default)",
            column_name.value
        ))),
        AlterColumnOperation::SetDefault { .. } | AlterColumnOperation::DropDefault => {
            Err(DataFusionError::NotImplemented(format!(
                "ALTER COLUMN `{}` SET/DROP DEFAULT is not supported yet",
                column_name.value
            )))
        }
        AlterColumnOperation::AddGenerated { .. } => Err(DataFusionError::NotImplemented(format!(
            "ALTER COLUMN `{}` ADD GENERATED is not supported",
            column_name.value
        ))),
    }
}

/// Targets that *can* appear on the right-hand side of an Iceberg promotion (not a full from→to
/// check — the fork still validates the source column's current type at commit).
fn is_iceberg_promotion_target(new_type: &PrimitiveType) -> bool {
    matches!(
        new_type,
        PrimitiveType::Long
            | PrimitiveType::Double
            | PrimitiveType::Decimal { .. }
            // Identity promotions (same type) are allowed by the fork; listing common primitives
            // keeps `ALTER COLUMN c TYPE INT` on an int column a no-op rather than a Plan refuse.
            | PrimitiveType::Int
            | PrimitiveType::Float
            | PrimitiveType::Boolean
            | PrimitiveType::String
            | PrimitiveType::Date
            | PrimitiveType::Timestamp
            | PrimitiveType::Timestamptz
            | PrimitiveType::Binary
    )
}

fn unsupported_alter_op(other: &AlterTableOperation) -> DataFusionError {
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
        return DataFusionError::NotImplemented(
            "ALTER TABLE partition-spec evolution must use ADD/DROP/REPLACE PARTITION FIELD \
             (I7); this AST shape is not recognised"
                .into(),
        );
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

/// Split a parsed `SET TBLPROPERTIES` option list into real sets vs. sentinel-flagged removals (the
/// latter originating from a token-rewritten `UNSET TBLPROPERTIES`).
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

/// Resolve a three-part `catalog.namespace.table` object name to its catalog name + [`TableIdent`].
fn resolve_table(name: &ObjectName) -> Result<(String, TableIdent)> {
    let parts = name_parts(name);
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

/// ===========================================================================================
/// Rewrite `ALTER TABLE … UNSET TBLPROPERTIES ('k', …)` (which sqlparser 0.59 cannot parse) into a
/// `SET TBLPROPERTIES ('k' = '<sentinel>', …)` the parser accepts; [`execute_alter_table`] decodes
/// the sentinel back into removals. Any non-`ALTER … UNSET TBLPROPERTIES` token stream is returned
/// unchanged, so this is safe to run on every statement (mirroring the other token normalisers).
/// ===========================================================================================
pub(crate) fn rewrite_unset_tblproperties(tokens: &[Token]) -> Vec<Token> {
    // Find the keyword spine, ignoring whitespace, to confirm this is `ALTER TABLE … UNSET
    // TBLPROPERTIES` and to locate the `UNSET` token index.
    //
    // UNSET must be the token *immediately preceding* TBLPROPERTIES (next significant
    // token). sqlparser tags a bare identifier `unset` as `Keyword::UNSET`, so a table
    // named `unset` would otherwise match "first UNSET anywhere" and get rewritten into
    // `SET`, corrupting `ALTER TABLE …unset SET TBLPROPERTIES …` (audit-2026-07-10 /
    // octo C1). Matching only `UNSET` + next-significant `TBLPROPERTIES` leaves SET-form
    // statements (and any table/column named unset) untouched.
    let keyword_at = |kw: Keyword| {
        tokens
            .iter()
            .position(|t| matches!(t, Token::Word(w) if w.keyword == kw))
    };
    let (Some(alter), Some(table)) = (keyword_at(Keyword::ALTER), keyword_at(Keyword::TABLE))
    else {
        return tokens.to_vec();
    };
    // Prefer positional UNSET-immediately-before-TBLPROPERTIES over "first UNSET token".
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
    // Guard the shape: ALTER … TABLE … UNSET … TBLPROPERTIES in order.
    if !(alter < table && table < unset && unset < tblprops) {
        return tokens.to_vec();
    }

    // Locate the property-list parens that follow `TBLPROPERTIES` and track depth, so we only touch
    // key tokens inside them. A key is any string-literal / identifier whose next non-whitespace
    // token is `,` or `)` (i.e. it has no `= value`); we inject `= '<sentinel>'` after it.
    let mut out = Vec::with_capacity(tokens.len() + 8);
    let mut depth: i32 = 0;
    let mut seen_open = false;
    for (i, token) in tokens.iter().enumerate() {
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

/// True if the next non-whitespace token closes a bare property key (a `,` or `)`), i.e. the key
/// carries no `= value` and is therefore an `UNSET` removal target.
fn next_terminates_key(rest: &[Token]) -> bool {
    rest.iter()
        .find(|t| !matches!(t, Token::Whitespace(_)))
        .is_some_and(|t| matches!(t, Token::Comma | Token::RParen))
}

/// ===========================================================================================
/// Rewrite Spark `ADD COLUMNS (c1 TYPE, c2 TYPE, …)` into `ADD COLUMN c1 TYPE, ADD COLUMN c2 TYPE`
/// so stock sqlparser accepts the plural parenthesised form.
/// ===========================================================================================
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

/// ===========================================================================================
/// Rewrite Spark `DROP COLUMNS (a, b)` / `DROP COLUMNS a, b` into `DROP COLUMN a, DROP COLUMN b`.
/// ===========================================================================================
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

/// Split `tokens` on top-level (depth-0) commas; each segment is trimmed of leading/trailing
/// whitespace tokens.
fn split_top_level_comma_segments(tokens: &[Token]) -> Vec<Vec<Token>> {
    let mut segments = Vec::new();
    let mut current = Vec::new();
    let mut depth = 0_i32;
    for token in tokens {
        match token {
            Token::LParen => {
                depth += 1;
                current.push(token.clone());
            }
            Token::RParen => {
                depth -= 1;
                current.push(token.clone());
            }
            Token::Comma if depth == 0 => {
                let trimmed = trim_ws_tokens(current);
                if !trimmed.is_empty() {
                    segments.push(trimmed);
                }
                current = Vec::new();
            }
            other => current.push(other.clone()),
        }
    }
    let trimmed = trim_ws_tokens(current);
    if !trimmed.is_empty() {
        segments.push(trimmed);
    }
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

fn next_significant(tokens: &[Token], from: usize) -> Option<usize> {
    tokens
        .iter()
        .enumerate()
        .skip(from)
        .find(|(_, token)| !matches!(token, Token::Whitespace(_)))
        .map(|(index, _)| index)
}

fn is_word_keyword(token: &Token, keyword: Keyword) -> bool {
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

/// Loud refuse for Spark forms stock sqlparser still cannot parse and I7 does not own.
///
/// Returns `Some(Err)` when the SQL is one of those shapes; `None` otherwise.
///
/// **Not refused here (handled by dedicated parsers):** `ADD|DROP|REPLACE PARTITION FIELD`,
/// `REPLACE COLUMNS` (I7 stretch with identity-trap).
pub(crate) fn refuse_unsupported_alter_sql(sql: &str) -> Option<Result<DataFrame>> {
    let upper = sql.to_ascii_uppercase();
    // Cheap head check — only ALTER TABLE.
    let trimmed = upper.trim_start();
    if !trimmed.starts_with("ALTER") || !trimmed.contains("TABLE") {
        return None;
    }
    // WRITE ORDERED / DISTRIBUTED BY stay OUT (sort-order evolution, not partition-spec).
    if trimmed.contains("WRITE ORDERED BY") || trimmed.contains("WRITE DISTRIBUTED BY") {
        return Some(Err(DataFusionError::NotImplemented(
            "ALTER TABLE WRITE ORDERED BY / WRITE DISTRIBUTED BY is not supported yet — \
             sort-order evolution is out of I7 READY (partition-spec evolution is ADD/DROP/\
             REPLACE PARTITION FIELD)"
                .into(),
        )));
    }
    // Standalone MOVE without FIRST/AFTER on ADD (charter OUT).
    if trimmed.contains("ALTER COLUMN")
        && (trimmed.contains(" FIRST") || trimmed.contains(" AFTER "))
        && !trimmed.contains("ADD")
    {
        // `ALTER COLUMN c FIRST` is a move — refuse.
        return Some(Err(DataFusionError::NotImplemented(
            "ALTER COLUMN … FIRST/AFTER (column MOVE) without ADD is not supported yet — \
             use ADD COLUMN … FIRST|AFTER for new columns (I6)"
                .into(),
        )));
    }
    // ALTER COLUMN COMMENT — sqlparser cannot model it; refuse with a clear path until stretch
    // lands a rewrite (doc update via UpdateColumnDoc is available from the write primitive).
    if trimmed.contains("ALTER COLUMN") && trimmed.contains("COMMENT") {
        // Allow through only if we later add a rewrite; for now surface a targeted message
        // rather than an opaque parse fallthrough.
        return Some(Err(DataFusionError::NotImplemented(
            "ALTER COLUMN … COMMENT is not supported yet via SQL — column COMMENT is accepted \
             on ADD COLUMN; UpdateColumnDoc is available on the write primitive (I6 stretch)"
                .into(),
        )));
    }
    None
}

// ===========================================================================================
// I7 — PARTITION FIELD + REPLACE COLUMNS (stock sqlparser cannot model these Spark forms)
// ===========================================================================================

/// A parsed I7 ALTER form that bypasses stock sqlparser.
#[derive(Debug, Clone)]
pub(crate) enum IcebergAlterDdl {
    /// One or more partition-spec evolution ops on a three-part table.
    PartitionSpec {
        /// `catalog.namespace.table` parts.
        table_parts: Vec<String>,
        /// Ordered ops (usually one; multi-clause future-proof).
        changes: Vec<PartitionSpecChange>,
    },
    /// `REPLACE COLUMNS (col TYPE [COMMENT …] [NOT NULL], …)`.
    ReplaceColumns {
        /// `catalog.namespace.table` parts.
        table_parts: Vec<String>,
        /// New top-level column list (order preserved).
        columns: Vec<ReplaceColumnDef>,
    },
}

/// One column in a `REPLACE COLUMNS` list.
#[derive(Debug, Clone)]
pub(crate) struct ReplaceColumnDef {
    /// Column name.
    name: String,
    /// Iceberg field type.
    field_type: Type,
    /// Optional COMMENT.
    doc: Option<String>,
    /// When true the column is required (`NOT NULL`).
    required: bool,
}

/// Significant token for the I7 hand parser (mirrors `ref_ddl`).
#[derive(Debug, Clone)]
enum Sig {
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

/// ===========================================================================================
/// Try to parse Spark Iceberg `ADD|DROP|REPLACE PARTITION FIELD` or `REPLACE COLUMNS`.
///
/// Returns `None` when the statement is not one of those forms. `Some(Err)` for recognised
/// forms that fail validation (bad transform, arity, identity trap pre-check at plan time is
/// deferred to execute for REPLACE COLUMNS).
/// ===========================================================================================
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
    if table_parts.len() != 3 {
        return Some(Err(DataFusionError::Plan(format!(
            "ALTER TABLE expects a three-part `catalog.namespace.table` name, got `{}`",
            table_parts.join(".")
        ))));
    }

    // REPLACE COLUMNS (…)
    if word_eq(&significant, index, "REPLACE") && word_eq(&significant, index + 1, "COLUMNS") {
        return Some(parse_replace_columns(&significant, index + 2, table_parts));
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
        return Some(parse_replace_partition_field(
            &significant,
            index + 3,
            table_parts,
        ));
    }
    None
}

/// ===========================================================================================
/// Execute a parsed I7 Iceberg ALTER form (partition-spec evolution or REPLACE COLUMNS).
/// ===========================================================================================
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
            let (catalog_name, ident) = table_parts_to_ident(&table_parts)?;
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
            let (catalog_name, ident) = table_parts_to_ident(&table_parts)?;
            let handle = catalog_handle(catalogs, &catalog_name)?;
            let schema_changes = plan_replace_columns(handle.as_ref(), &ident, &columns).await?;
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

fn table_parts_to_ident(parts: &[String]) -> Result<(String, TableIdent)> {
    let [catalog, namespace, table] = parts else {
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

fn parse_replace_partition_field(
    significant: &[Sig],
    start: usize,
    table_parts: Vec<String>,
) -> Result<IcebergAlterDdl> {
    // REPLACE PARTITION FIELD <old> WITH <term> [AS name]
    // old may be a bare name (READY) or transform(…) (stretch — refuse if not bare for simplicity).
    let old_name = word_at(significant, start).ok_or_else(|| {
        DataFusionError::Plan(
            "ALTER TABLE REPLACE PARTITION FIELD expects the existing partition field name".into(),
        )
    })?;
    if matches!(significant.get(start + 1), Some(Sig::LParen)) {
        return Err(DataFusionError::NotImplemented(
            "ALTER TABLE REPLACE PARTITION FIELD with a transform(…) left-hand side is not \
             supported yet — drop by name or DROP PARTITION FIELD transform(col), then ADD \
             (I7 stretch)"
                .into(),
        ));
    }
    if !word_eq(significant, start + 1, "WITH") {
        return Err(DataFusionError::Plan(
            "ALTER TABLE REPLACE PARTITION FIELD expects `WITH <transform>(col) [AS name]`".into(),
        ));
    }
    let (field_spec, name, next) = parse_partition_field_term(significant, start + 2)?;
    let (name, next) = if name.is_some() {
        (name, next)
    } else if word_eq(significant, next, "AS") {
        let alias = word_at(significant, next + 1).ok_or_else(|| {
            DataFusionError::Plan(
                "ALTER TABLE REPLACE PARTITION FIELD … AS expects a partition field name".into(),
            )
        })?;
        (Some(alias.to_string()), next + 2)
    } else {
        (None, next)
    };
    if next < significant.len() {
        return Err(DataFusionError::Plan(format!(
            "trailing tokens after REPLACE PARTITION FIELD (starting at `{}`)",
            render_sig_at(significant, next)
        )));
    }
    let (source_name, transform) = partition_field_spec_parts(&field_spec);
    Ok(IcebergAlterDdl::PartitionSpec {
        table_parts,
        changes: vec![PartitionSpecChange::ReplaceField {
            old_name: old_name.to_string(),
            source_name,
            transform,
            new_name: name,
        }],
    })
}

/// Parse `identity` / `col` / `bucket(n, col)` / `truncate(w, col)` / `year(col)` … optionally
/// parenthesised as `(term AS name)`.
fn parse_partition_field_term(
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

fn partition_field_spec_parts(field_spec: &PartitionFieldSpec) -> (String, Transform) {
    (field_spec.column().to_string(), field_spec.transform())
}

fn parse_replace_columns(
    significant: &[Sig],
    start: usize,
    table_parts: Vec<String>,
) -> Result<IcebergAlterDdl> {
    if !matches!(significant.get(start), Some(Sig::LParen)) {
        return Err(DataFusionError::Plan(
            "ALTER TABLE REPLACE COLUMNS expects `(col TYPE, …)`".into(),
        ));
    }
    // Find matching close paren for the column list.
    let mut depth = 0_i32;
    let mut close = None;
    for (offset, token) in significant.iter().enumerate().skip(start) {
        match token {
            Sig::LParen => depth += 1,
            Sig::RParen => {
                depth -= 1;
                if depth == 0 {
                    close = Some(offset);
                    break;
                }
            }
            _ => {}
        }
    }
    let close = close.ok_or_else(|| {
        DataFusionError::Plan("ALTER TABLE REPLACE COLUMNS: unterminated column list".into())
    })?;
    if close + 1 < significant.len() {
        return Err(DataFusionError::Plan(
            "trailing tokens after REPLACE COLUMNS (…)".into(),
        ));
    }
    let inner = &significant[start + 1..close];
    let segments = split_sig_comma_segments(inner);
    if segments.is_empty() {
        return Err(DataFusionError::Plan(
            "ALTER TABLE REPLACE COLUMNS requires at least one column".into(),
        ));
    }
    let mut columns = Vec::with_capacity(segments.len());
    for segment in segments {
        columns.push(parse_replace_column_segment(segment)?);
    }
    Ok(IcebergAlterDdl::ReplaceColumns {
        table_parts,
        columns,
    })
}

fn split_sig_comma_segments(tokens: &[Sig]) -> Vec<&[Sig]> {
    let mut segments = Vec::new();
    let mut depth = 0_i32;
    let mut start = 0usize;
    for (index, token) in tokens.iter().enumerate() {
        match token {
            Sig::LParen => depth += 1,
            Sig::RParen => depth -= 1,
            Sig::Comma if depth == 0 => {
                if start < index {
                    segments.push(&tokens[start..index]);
                }
                start = index + 1;
            }
            _ => {}
        }
    }
    if start < tokens.len() {
        segments.push(&tokens[start..]);
    }
    segments
}

fn parse_replace_column_segment(segment: &[Sig]) -> Result<ReplaceColumnDef> {
    // name TYPE [NOT NULL] [COMMENT '…']  (order of options flexible)
    let name = word_at(segment, 0).ok_or_else(|| {
        DataFusionError::Plan("REPLACE COLUMNS column entry must start with a name".into())
    })?;
    let type_word = word_at(segment, 1).ok_or_else(|| {
        DataFusionError::Plan(format!("REPLACE COLUMNS column `{name}` expects a type"))
    })?;
    let (data_type, mut index) = parse_sql_type_tokens(segment, 1, type_word)?;
    let field_type = sql_type_to_iceberg(&data_type)?;
    let mut required = false;
    let mut doc = None;
    while index < segment.len() {
        if word_eq(segment, index, "NOT") && word_eq(segment, index + 1, "NULL") {
            required = true;
            index += 2;
            continue;
        }
        if word_eq(segment, index, "NULL") {
            // explicit NULL = optional
            index += 1;
            continue;
        }
        if word_eq(segment, index, "COMMENT") {
            match segment.get(index + 1) {
                Some(Sig::String(text)) => {
                    doc = Some(text.clone());
                    index += 2;
                    continue;
                }
                _ => {
                    return Err(DataFusionError::Plan(format!(
                        "REPLACE COLUMNS column `{name}` COMMENT expects a string literal"
                    )));
                }
            }
        }
        return Err(DataFusionError::Plan(format!(
            "REPLACE COLUMNS column `{name}`: unexpected token at `{}`",
            render_sig_at(segment, index)
        )));
    }
    Ok(ReplaceColumnDef {
        name: name.to_string(),
        field_type,
        doc,
        required,
    })
}

/// Parse a simple SQL type starting at `type_index` (word already known as `type_word`).
fn parse_sql_type_tokens(
    segment: &[Sig],
    type_index: usize,
    type_word: &str,
) -> Result<(DataType, usize)> {
    let lower = type_word.to_ascii_lowercase();
    // DECIMAL(p, s) / DECIMAL(p)
    if lower == "decimal" || lower == "numeric" {
        if !matches!(segment.get(type_index + 1), Some(Sig::LParen)) {
            return Ok((DataType::Decimal(ExactNumberInfo::None), type_index + 1));
        }
        let (args, after) = parse_paren_arg_list(segment, type_index + 1)?;
        let info = match args.as_slice() {
            [precision] => {
                let precision: u64 = precision.parse().map_err(|_| {
                    DataFusionError::Plan(format!(
                        "DECIMAL precision must be an integer, got `{precision}`"
                    ))
                })?;
                ExactNumberInfo::Precision(precision)
            }
            [precision, scale] => {
                let precision: u64 = precision.parse().map_err(|_| {
                    DataFusionError::Plan(format!(
                        "DECIMAL precision must be an integer, got `{precision}`"
                    ))
                })?;
                let scale: i64 = scale.parse().map_err(|_| {
                    DataFusionError::Plan(format!(
                        "DECIMAL scale must be an integer, got `{scale}`"
                    ))
                })?;
                ExactNumberInfo::PrecisionAndScale(precision, scale)
            }
            _ => {
                return Err(DataFusionError::Plan(
                    "DECIMAL expects DECIMAL, DECIMAL(p), or DECIMAL(p, s)".into(),
                ));
            }
        };
        return Ok((DataType::Decimal(info), after));
    }
    // VARCHAR(n) / CHAR(n) — map via sql_type_to_iceberg as string-like
    if lower == "varchar" || lower == "char" || lower == "character" {
        if matches!(segment.get(type_index + 1), Some(Sig::LParen)) {
            let (_args, after) = parse_paren_arg_list(segment, type_index + 1)?;
            return Ok((DataType::Varchar(None), after));
        }
        return Ok((DataType::Varchar(None), type_index + 1));
    }
    let data_type = match lower.as_str() {
        "int" | "integer" => DataType::Int(None),
        "bigint" | "long" => DataType::BigInt(None),
        "smallint" | "short" => DataType::SmallInt(None),
        "tinyint" | "byte" => DataType::TinyInt(None),
        "float" | "real" => DataType::Float(ExactNumberInfo::None),
        "double" | "float8" => DataType::Double(ExactNumberInfo::None),
        "string" | "text" => DataType::String(None),
        "boolean" | "bool" => DataType::Boolean,
        "date" => DataType::Date,
        "timestamp" => DataType::Timestamp(None, TimezoneInfo::None),
        "binary" | "bytes" => DataType::Binary(None),
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "REPLACE COLUMNS type `{other}` is not supported yet (I7 primitives only)"
            )));
        }
    };
    Ok((data_type, type_index + 1))
}

/// Plan REPLACE COLUMNS against the current schema: identity-trap refuse, then drop/add/promote.
async fn plan_replace_columns(
    catalog: &dyn iceberg::Catalog,
    ident: &TableIdent,
    columns: &[ReplaceColumnDef],
) -> Result<Vec<SchemaChange>> {
    let table = catalog.load_table(ident).await.map_err(iceberg_err)?;
    let existing: Vec<_> = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| {
            (
                field.name.clone(),
                field.field_type.as_ref().clone(),
                field.required,
            )
        })
        .collect();
    refuse_replace_columns_identity_and_required(&existing, columns)?;
    Ok(build_replace_column_changes(&existing, columns))
}

/// Identity-trap + required-incompatible gates for REPLACE COLUMNS.
fn refuse_replace_columns_identity_and_required(
    existing: &[(String, Type, bool)],
    columns: &[ReplaceColumnDef],
) -> Result<()> {
    for column in columns {
        if let Some((_, existing_type, _)) = existing
            .iter()
            .find(|(name, _, _)| name.eq_ignore_ascii_case(&column.name))
            && !types_compatible_for_replace(existing_type, &column.field_type)
        {
            return Err(DataFusionError::Plan(format!(
                "ALTER TABLE REPLACE COLUMNS identity trap: column `{}` exists as `{}` but \
                 REPLACE would re-introduce it as `{}` — Iceberg field-ids would be recycled \
                 only under type promotion (int→long, float→double, decimal widen); refuse \
                 rather than silently drop+re-add under the same name (I7)",
                column.name, existing_type, column.field_type
            )));
        }
    }
    for column in columns {
        if !column.required {
            continue;
        }
        let existing_required = existing
            .iter()
            .find(|(name, _, _)| name.eq_ignore_ascii_case(&column.name))
            .map(|(_, _, required)| *required);
        match existing_required {
            None => {
                return Err(DataFusionError::NotImplemented(format!(
                    "ALTER TABLE REPLACE COLUMNS cannot ADD required column `{}` without a \
                     default (incompatible change; omit NOT NULL or use a write-default path)",
                    column.name
                )));
            }
            Some(false) => {
                return Err(DataFusionError::NotImplemented(format!(
                    "ALTER TABLE REPLACE COLUMNS cannot SET NOT NULL on existing column `{}` \
                     (incompatible without backfill; I7)",
                    column.name
                )));
            }
            Some(true) => {}
        }
    }
    Ok(())
}

/// Build the schema-change list for a validated REPLACE COLUMNS.
fn build_replace_column_changes(
    existing: &[(String, Type, bool)],
    columns: &[ReplaceColumnDef],
) -> Vec<SchemaChange> {
    let mut changes = Vec::new();
    for (existing_name, _, _) in existing {
        let kept = columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(existing_name));
        if !kept {
            changes.push(SchemaChange::DropColumn {
                name: existing_name.clone(),
            });
        }
    }
    for column in columns {
        if let Some((existing_name, existing_type, was_required)) = existing
            .iter()
            .find(|(name, _, _)| name.eq_ignore_ascii_case(&column.name))
        {
            if existing_type != &column.field_type
                && let Type::Primitive(new_type) = &column.field_type
            {
                changes.push(SchemaChange::UpdateColumnType {
                    name: existing_name.clone(),
                    new_type: new_type.clone(),
                });
            }
            if *was_required && !column.required {
                changes.push(SchemaChange::MakeColumnOptional {
                    name: existing_name.clone(),
                });
            }
            if column.doc.is_some() {
                changes.push(SchemaChange::UpdateColumnDoc {
                    name: existing_name.clone(),
                    doc: column.doc.clone(),
                });
            }
        } else {
            changes.push(SchemaChange::AddColumn {
                name: column.name.clone(),
                field_type: column.field_type.clone(),
                doc: column.doc.clone(),
                required: false,
                position: None,
            });
        }
    }
    changes
}

/// True when REPLACE may keep the field-id: same type, or Iceberg promotion target pair.
fn types_compatible_for_replace(existing: &Type, new_type: &Type) -> bool {
    if existing == new_type {
        return true;
    }
    match (existing, new_type) {
        (Type::Primitive(from), Type::Primitive(to)) => is_iceberg_type_promotion(from, to),
        _ => false,
    }
}

/// Iceberg type-promotion pairs (Java parity): int→long, float→double, decimal same-scale widen.
fn is_iceberg_type_promotion(from: &PrimitiveType, to: &PrimitiveType) -> bool {
    match (from, to) {
        (PrimitiveType::Int, PrimitiveType::Long)
        | (PrimitiveType::Float, PrimitiveType::Double) => true,
        (
            PrimitiveType::Decimal {
                precision: from_precision,
                scale: from_scale,
            },
            PrimitiveType::Decimal {
                precision: to_precision,
                scale: to_scale,
            },
        ) => from_scale == to_scale && to_precision >= from_precision,
        // Identity (same primitive) already handled by ==; allow same-type restate.
        _ => from == to,
    }
}

fn tokenize_significant(sql: &str) -> Option<Vec<Sig>> {
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

fn word_eq(significant: &[Sig], index: usize, expected: &str) -> bool {
    word_at(significant, index).is_some_and(|word| word.eq_ignore_ascii_case(expected))
}

fn word_at(significant: &[Sig], index: usize) -> Option<&str> {
    match significant.get(index) {
        Some(Sig::Word(word)) => Some(word.as_str()),
        _ => None,
    }
}

fn is_period_at(significant: &[Sig], index: usize) -> bool {
    matches!(significant.get(index), Some(Sig::Period))
}

fn collect_name_parts(significant: &[Sig], start: usize, end: usize) -> Option<Vec<String>> {
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

fn render_sig_at(significant: &[Sig], index: usize) -> String {
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

    /// Octo C7 — bare `DROP COLUMNS a, b` (no parens) rewrites to multi DROP COLUMN.
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
    fn is_promotion_target_accepts_long_double_decimal() {
        assert!(is_iceberg_promotion_target(&PrimitiveType::Long));
        assert!(is_iceberg_promotion_target(&PrimitiveType::Double));
        assert!(is_iceberg_promotion_target(&PrimitiveType::Decimal {
            precision: 10,
            scale: 2
        }));
    }

    #[test]
    fn parse_replace_columns_is_recognized() {
        // I7: REPLACE COLUMNS is no longer a blanket refuse — the dedicated parser owns it.
        assert!(
            refuse_unsupported_alter_sql(
                "ALTER TABLE ice.sales.t REPLACE COLUMNS (a INT, b STRING)",
            )
            .is_none(),
            "REPLACE COLUMNS must not hit the residual refuse path"
        );
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

    /// Octo C1 — table named `unset` must not trigger the UNSET→SET rewrite (sqlparser tags bare
    /// `unset` as `Keyword::UNSET`). Mutation: first-UNSET-anywhere locator → table name becomes SET.
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
        // Reject the corruption shape `ice.ns.SET SET TBLPROPERTIES`.
        assert!(
            !rendered.to_ascii_uppercase().contains("NS.SET SET"),
            "must not rewrite table name into SET, got: {rendered}"
        );
        assert!(
            !rendered.contains(UNSET_SENTINEL),
            "SET form must not inject UNSET sentinel, got: {rendered}"
        );
    }

    /// Octo C1 twin — real `UNSET TBLPROPERTIES` still rewrites to sentinel SET.
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
