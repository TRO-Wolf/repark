//! `ALTER TABLE` — ANSI schema evolution + Trino `SET PROPERTIES` (design §2 Q3-adjacent).

use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::ast::{
    AlterColumnOperation, AlterTable, AlterTableOperation, ColumnDef, ColumnOption, Expr, Ident,
    ObjectName, RenameTableNameKind, SqlOption,
};
use iceberg::spec::{PrimitiveType, Type};
use iceberg::{Catalog, TableIdent};
use repark_core::EngineContext;
use repark_functions::cardinality::repark_sql_settings_from_options;
use repark_functions::format_version::resolve_alter_format_version;
use repark_iceberg::write::alter::SchemaChange;
use repark_iceberg::write::format_version::{
    FORMAT_VERSION_PROPERTY, current_format_version, format_version_from_number,
    set_properties_and_format_version,
};

use crate::create_table::{CreateTarget, resolve_target, sql_type_to_iceberg};
use crate::properties::{refuse_format_value, refuse_sorted_by};
use crate::scan::{blank_out_quoted_and_comments, leading_keyword, word_spans};
use crate::schema_ddl::iceberg_err;

/// The statement name every refusal in this module leads with.
const FORM: &str = "ALTER TABLE";

/// The Iceberg property the curated `format` key maps onto.
const FORMAT_PROPERTY: &str = "write.format.default";

/// Execute a stock-parsed `ALTER TABLE catalog.schema.table <op>…`.
pub(crate) async fn execute_alter_table(
    cx: &EngineContext<'_>,
    alter: &AlterTable,
) -> Result<DataFrame> {
    let mut target = resolve_target(cx, &alter.name, FORM)?;
    let mut batch: Vec<SchemaChange> = Vec::new();
    let mut dirty = false;

    for operation in &alter.operations {
        match operation {
            AlterTableOperation::AddColumn {
                column_def,
                if_not_exists,
                ..
            } => {
                if *if_not_exists
                    && column_exists(target.catalog.as_ref(), &target.ident(), &column_def.name)
                        .await?
                {
                    continue;
                }
                batch.push(add_column_change(cx, column_def).await?);
                dirty = true;
            }
            AlterTableOperation::DropColumn {
                column_names,
                if_exists,
                ..
            } => {
                for column in column_names {
                    if *if_exists
                        && !column_exists(target.catalog.as_ref(), &target.ident(), column).await?
                    {
                        continue;
                    }
                    batch.push(SchemaChange::DropColumn {
                        name: column.value.clone(),
                    });
                    dirty = true;
                }
            }
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                batch.push(SchemaChange::RenameColumn {
                    from: old_column_name.value.clone(),
                    to: new_column_name.value.clone(),
                });
                dirty = true;
            }
            AlterTableOperation::AlterColumn { column_name, op } => {
                batch.push(alter_column_change(cx, column_name, op).await?);
                dirty = true;
            }
            AlterTableOperation::RenameTable {
                table_name: RenameTableNameKind::To(destination),
            } => {
                flush(&target, &mut batch).await?;
                target = rename_table(cx, &target, destination).await?;
                dirty = true;
            }
            AlterTableOperation::SetOptionsParens { options } => {
                flush(&target, &mut batch).await?;
                dirty |= apply_set_properties(cx, &target, options).await?;
            }
            other => {
                flush(&target, &mut batch).await?;
                return Err(unsupported_operation(other));
            }
        }
    }
    flush(&target, &mut batch).await?;
    if dirty {
        invalidate(cx, &target).await?;
    }
    cx.ctx.read_empty()
}

/// Commit the pending schema ops as ONE `UpdateSchema` transaction (a no-op when empty).
async fn flush(target: &CreateTarget, batch: &mut Vec<SchemaChange>) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let changes = std::mem::take(batch);
    repark_iceberg::write::alter::apply_schema_changes(
        target.catalog.as_ref(),
        &target.ident(),
        &changes,
    )
    .await
    .map_err(iceberg_err)
}

/// Re-register the target namespace so the evolved schema (or the new name) becomes queryable.
async fn invalidate(cx: &EngineContext<'_>, target: &CreateTarget) -> Result<()> {
    repark_iceberg::catalog::invalidate_catalog_namespaces(
        cx.ctx,
        std::sync::Arc::clone(&target.catalog),
        &target.catalog_name,
        &[&target.schema_name()],
    )
    .await
}

// Schema evolution handlers.

/// `ADD COLUMN name <type> [NULL] [COMMENT '…']`, as an optional (nullable) Iceberg column.
async fn add_column_change(cx: &EngineContext<'_>, column: &ColumnDef) -> Result<SchemaChange> {
    let mut doc = None;
    for option in &column.options {
        match &option.option {
            ColumnOption::Null => {}
            ColumnOption::Comment(text) => doc = Some(text.clone()),
            ColumnOption::NotNull => {
                return Err(DataFusionError::NotImplemented(format!(
                    "{FORM} ADD COLUMN `{}` NOT NULL is not supported — Iceberg treats a required \
                     add without a default as an incompatible change. Add the column nullable \
                     (omit NOT NULL) and backfill it",
                    column.name.value
                )));
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "{FORM} ADD COLUMN `{}`: column option `{other}` is not supported — only \
                     NULL and COMMENT are accepted",
                    column.name.value
                )));
            }
        }
    }
    Ok(SchemaChange::AddColumn {
        name: column.name.value.clone(),
        field_type: sql_type_to_iceberg(cx.ctx, &column.data_type, FORM).await?,
        doc,
        required: false,
        position: None,
    })
}

/// `ALTER COLUMN c SET DATA TYPE <primitive>` — an Iceberg type PROMOTION, which is metadata-only.
async fn alter_column_change(
    cx: &EngineContext<'_>,
    column_name: &Ident,
    op: &AlterColumnOperation,
) -> Result<SchemaChange> {
    let AlterColumnOperation::SetDataType {
        data_type, using, ..
    } = op
    else {
        return Err(DataFusionError::NotImplemented(format!(
            "{FORM} ALTER COLUMN `{}`: only SET DATA TYPE is supported in this door (got \
             `{op}`) — Iceberg column promotions are metadata-only, and the other ALTER COLUMN \
             forms are either incompatible changes or unimplemented",
            column_name.value
        )));
    };
    if using.is_some() {
        return Err(DataFusionError::NotImplemented(format!(
            "{FORM} ALTER COLUMN `{}` SET DATA TYPE … USING is not supported — an Iceberg \
             promotion rewrites no rows, so a USING expression could not be applied",
            column_name.value
        )));
    }
    let Type::Primitive(new_type) = sql_type_to_iceberg(cx.ctx, data_type, FORM).await? else {
        return Err(DataFusionError::NotImplemented(format!(
            "{FORM} ALTER COLUMN `{}` SET DATA TYPE `{data_type}`: only primitive target types \
             are supported",
            column_name.value
        )));
    };
    if !is_promotion_target(&new_type) {
        return Err(DataFusionError::Plan(format!(
            "{FORM} ALTER COLUMN `{}` SET DATA TYPE `{data_type}` is not an Iceberg promotion \
             target — only int→bigint, real→double, and decimal(p,s)→decimal(p2,s) with p2≥p are \
             allowed. Narrowing refuses because it would lose data that is already written",
            column_name.value
        )));
    }
    Ok(SchemaChange::UpdateColumnType {
        name: column_name.value.clone(),
        new_type,
    })
}

/// Types accepted as Iceberg promotion targets.
fn is_promotion_target(new_type: &PrimitiveType) -> bool {
    matches!(
        new_type,
        PrimitiveType::Long | PrimitiveType::Double | PrimitiveType::Decimal { .. }
    )
}

/// True when the table has a top-level column with this name under case-insensitive matching.
async fn column_exists(catalog: &dyn Catalog, ident: &TableIdent, column: &Ident) -> Result<bool> {
    let table = catalog.load_table(ident).await.map_err(iceberg_err)?;
    Ok(table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .any(|field| field.name.eq_ignore_ascii_case(&column.value)))
}

/// `RENAME TO catalog.schema.table` — cross-catalog renames refuse (there is no such Iceberg op).
async fn rename_table(
    cx: &EngineContext<'_>,
    source: &CreateTarget,
    destination: &ObjectName,
) -> Result<CreateTarget> {
    let target = resolve_target(cx, destination, "ALTER TABLE … RENAME TO")?;
    if target.catalog_name != source.catalog_name {
        return Err(DataFusionError::Plan(format!(
            "{FORM} RENAME cannot move a table across catalogs (`{}` → `{}`) — copy it with \
             CREATE TABLE … AS SELECT instead",
            source.catalog_name, target.catalog_name
        )));
    }
    repark_iceberg::write::alter::rename_table(
        source.catalog.as_ref(),
        &source.ident(),
        &target.ident(),
    )
    .await
    .map_err(iceberg_err)?;
    invalidate(cx, source).await?;
    Ok(target)
}

fn unsupported_operation(operation: &AlterTableOperation) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "{FORM}: operation `{operation}` is not supported by this door. Supported: ADD COLUMN, \
         DROP COLUMN, RENAME COLUMN, ALTER COLUMN … SET DATA TYPE, RENAME TO, and \
         SET PROPERTIES (…)"
    ))
}

// SET PROPERTIES handlers.

/// Rewrite `SET PROPERTIES` into the stock `SET` options form before parsing.
pub(crate) fn rewrite_set_properties(sql: &str) -> Option<String> {
    let scrubbed = blank_out_quoted_and_comments(sql);
    if leading_keyword(&scrubbed).as_deref() != Some("ALTER") {
        return None;
    }
    let words = word_spans(&scrubbed);
    if words.len() < 4 || !words[1].2.eq_ignore_ascii_case("TABLE") {
        return None;
    }
    // The `SET PROPERTIES` pair, anywhere after the table name.
    let (start, end) = words.windows(2).skip(2).find_map(|pair| {
        (pair[0].2.eq_ignore_ascii_case("SET") && pair[1].2.eq_ignore_ascii_case("PROPERTIES"))
            .then_some((pair[1].0, pair[1].1))
    })?;
    let mut rewritten = String::with_capacity(sql.len());
    rewritten.push_str(&sql[..start]);
    rewritten.extend(std::iter::repeat_n(' ', end - start));
    rewritten.push_str(&sql[end..]);
    Some(rewritten)
}

/// Apply a validated `SET PROPERTIES (…)` list as one property transaction.
async fn apply_set_properties(
    cx: &EngineContext<'_>,
    target: &CreateTarget,
    options: &[SqlOption],
) -> Result<bool> {
    let form = "ALTER TABLE … SET PROPERTIES";
    let (mut sets, unsets) = parse_set_properties(options)?;
    let upgrade = match sets.remove(FORMAT_VERSION_PROPERTY) {
        Some(requested) => {
            resolve_upgrade_target(cx, target, &requested, FORMAT_VERSION_PROPERTY, form).await?
        }
        None => None,
    };
    set_properties_and_format_version(
        target.catalog.as_ref(),
        &target.ident(),
        &sets,
        &unsets,
        upgrade,
    )
    .await
    .map_err(iceberg_err)?;
    Ok(upgrade.is_some())
}

async fn resolve_upgrade_target(
    cx: &EngineContext<'_>,
    target: &CreateTarget,
    requested: &str,
    property_name: &str,
    form: &str,
) -> Result<Option<iceberg::spec::FormatVersion>> {
    let allow_v3 = repark_sql_settings_from_options(cx.ctx.copied_config().options())
        .allow_create_format_version_3;
    let current = current_format_version(target.catalog.as_ref(), &target.ident())
        .await
        .map_err(iceberg_err)?;
    let number = resolve_alter_format_version(requested, current, allow_v3, property_name, form)?;
    number
        .map(|value| format_version_from_number(value).map_err(iceberg_err))
        .transpose()
}

/// Validate the curated `SET PROPERTIES` vocabulary into (sets, unsets).
fn parse_set_properties(options: &[SqlOption]) -> Result<(HashMap<String, String>, Vec<String>)> {
    let form = "ALTER TABLE … SET PROPERTIES";
    let mut sets: HashMap<String, String> = HashMap::new();
    let mut unsets: Vec<String> = Vec::new();
    if options.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "{form}: at least one property is required"
        )));
    }

    for option in options {
        let SqlOption::KeyValue { key, value } = option else {
            return Err(DataFusionError::NotImplemented(format!(
                "{form}: only `key = value` properties are supported (got `{option}`)"
            )));
        };
        let name = key.value.to_ascii_lowercase();
        let reset = is_default_keyword(value);

        // A dotted key is a raw Iceberg property.
        if name.contains('.') {
            if reset {
                unsets.push(key.value.clone());
                continue;
            }
            return Err(DataFusionError::Plan(format!(
                "{form}: raw Iceberg property `{}` is SET through the escape hatch — write \
                 extra_properties = MAP(ARRAY['{}'], ARRAY['…']). The dotted spelling is \
                 accepted only to UNSET it (`\"{}\" = DEFAULT`)",
                key.value, key.value, key.value
            )));
        }

        match name.as_str() {
            "extra_properties" => {
                if reset {
                    return Err(DataFusionError::Plan(format!(
                        "{form}: `extra_properties = DEFAULT` is not a spelling this door offers \
                         — it would wipe every raw property at once. Unset raw keys one at a \
                         time: SET PROPERTIES (\"write.merge.mode\" = DEFAULT)"
                    )));
                }
                for (raw_key, raw_value) in crate::properties::parse_extra_properties(value, form)?
                {
                    sets.insert(raw_key, raw_value);
                }
            }
            "format" => {
                if reset {
                    unsets.push(FORMAT_PROPERTY.to_string());
                    continue;
                }
                let literal = crate::properties::string_value(value, &name, form)?;
                refuse_format_value(&literal, form)?;
                sets.insert(FORMAT_PROPERTY.to_string(), literal.trim().to_uppercase());
            }
            "sorted_by" => return Err(refuse_sorted_by(form)),
            "partitioning" => return Err(refuse_partitioning()),
            "format_version" => {
                if reset {
                    return Err(DataFusionError::Plan(format!(
                        "{form}: `format_version = DEFAULT` is not a spelling this door offers — \
                         an Iceberg format version only moves up. Upgrade in place with SET \
                         PROPERTIES (format_version = '3')"
                    )));
                }
                sets.insert(
                    FORMAT_VERSION_PROPERTY.to_string(),
                    crate::properties::scalar_value(value, &name, form)?,
                );
            }
            "location" => {
                return Err(DataFusionError::NotImplemented(format!(
                    "{form}: `location` cannot be changed after creation — moving a table's root \
                     would orphan every data file already written under the old one. Create a \
                     new table at the new location with CREATE TABLE … AS SELECT"
                )));
            }
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "{form}: unknown table property `{}`. Supported here: `format`, \
                     `extra_properties`. Raw Iceberg keys are set through \
                     extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['merge-on-read']) \
                     and unset with (\"write.merge.mode\" = DEFAULT)",
                    key.value
                )));
            }
        }
    }
    Ok((sets, unsets))
}

/// Q3 reserves `partitioning` for future partition-spec evolution.
fn refuse_partitioning() -> DataFusionError {
    DataFusionError::NotImplemented(
        "ALTER TABLE … SET PROPERTIES: `partitioning` (partition-spec replacement) is \
         deliberately absent from SQL in this phase — see docs/design/sql-doors.md §2 Q3. The \
         partition spec is evolved today through the callable operation over the fork's \
         UpdatePartitionSpec (repark-iceberg `apply_partition_spec_changes`). This spelling is \
         the pre-designated future SQL surface, held so it cannot be reused; its TRIGGER is \
         dbt-repark or a first user need."
            .to_string(),
    )
}

/// Trino's reset spelling: the bare keyword `DEFAULT`, which sqlparser reads as an identifier.
fn is_default_keyword(value: &Expr) -> bool {
    matches!(value, Expr::Identifier(ident) if ident.quote_style.is_none()
        && ident.value.eq_ignore_ascii_case("DEFAULT"))
}

#[cfg(test)]
mod tests;
