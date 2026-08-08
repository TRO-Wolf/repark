//! `ALTER TABLE` — ANSI schema evolution + Trino `SET PROPERTIES` (design §2 Q3-adjacent).
//!
//! Four stock-parsed operations reach the fork's `UpdateSchema` through the SAME tier-1 calls the
//! Spark door uses ([`repark_iceberg::write::alter`]): `ADD COLUMN`, `DROP COLUMN`,
//! `RENAME COLUMN`, and `ALTER COLUMN … SET DATA TYPE`. `RENAME TO` rides
//! [`repark_iceberg::write::alter::rename_table`]. There is no door→door edge — the shared code is
//! the tier-1 crate below both doors, not the other door's parser.
//!
//! A contiguous run of schema ops commits as ONE `UpdateSchema` transaction (one catalog CAS), so
//! `ALTER TABLE t ADD COLUMN a INT, ADD COLUMN b INT` can never half-apply.
//!
//! ## `SET PROPERTIES` — the one pre-parse recognizer (design §6 R1)
//!
//! The R1 spike found that stock sqlparser rejects Trino's `SET PROPERTIES (…)` spelling
//! (`Expected: (, found: PROPERTIES`) while accepting the bare `SET (…)` form as
//! `AlterTableOperation::SetOptionsParens`. The fallback the design pre-authorised is therefore as
//! small as a recognizer gets: [`rewrite_set_properties`] blanks the single word `PROPERTIES` out
//! of `ALTER TABLE <name> SET PROPERTIES (…)` and lets the stock parser do everything else —
//! including parsing the VALUES as real expressions, which is what keeps the G4
//! `extra_properties = MAP(ARRAY[…], ARRAY[…])` hatch working here exactly as it does at CREATE.
//!
//! The vocabulary is CURATED, per the ruling:
//! * `extra_properties = MAP(…)` — raw Iceberg keys (this is the one that carries real weight:
//!   flipping an existing table to `write.merge.mode = 'merge-on-read'` lives here).
//! * `format` — validated against the create-time vocabulary; ORC/AVRO keep their G9 refusal.
//! * `"<dotted.key>" = DEFAULT` — the unset spelling for a raw key (the round trip of the hatch).
//! * `<key> = DEFAULT` — Trino's reset spelling on a curated key.
//! * `partitioning` — **deliberately absent** (design §2 Q3). This is the pre-designated FUTURE
//!   spelling for replace-spec, so the refusal says exactly that and names the callable op that
//!   does the job today.
//! * `sorted_by`, `format_version`, `location` — reserved refusals naming their triggers.

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
use repark_iceberg::write::alter::SchemaChange;

use crate::create_table::{CreateTarget, resolve_target, sql_type_to_iceberg};
use crate::properties::{refuse_format_value, refuse_sorted_by};
use crate::scan::{blank_out_quoted_and_comments, leading_keyword, word_spans};
use crate::schema_ddl::iceberg_err;

/// The statement name every refusal in this module leads with.
const FORM: &str = "ALTER TABLE";

/// The Iceberg property the curated `format` key maps onto.
const FORMAT_PROPERTY: &str = "write.format.default";

/// ===========================================================================================
/// Execute a stock-parsed `ALTER TABLE catalog.schema.table <op>…`.
///
/// Schema ops are batched into one `UpdateSchema` transaction per contiguous run; a `RENAME TO`
/// or `SET PROPERTIES` flushes the batch first so ordering is what the user wrote.
/// ===========================================================================================
///
/// # Errors
/// The Q15 target refusal, an unsupported operation or column option, a `SET PROPERTIES`
/// vocabulary refusal, or any iceberg error from the transaction.
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
                apply_set_properties(&target, options).await?;
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

// === Schema evolution =======================================================================

/// `ADD COLUMN name <type> [NULL] [COMMENT '…']`, as an optional (nullable) Iceberg column.
///
/// `NOT NULL` refuses: Iceberg treats a required add without a default as an INCOMPATIBLE change,
/// and enabling incompatible evolution silently is exactly the class of surprise this door exists
/// to avoid.
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
///
/// Every other `ALTER COLUMN` op refuses: `SET NOT NULL` and `SET DEFAULT` are incompatible or
/// unimplemented changes, and a silent no-op on a type request would leave the table not matching
/// what was asked for.
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

/// Types that may appear as the TARGET of an Iceberg promotion. The fork still validates the
/// source column's current type at commit; this is the door-side half of the pair, refusing the
/// obviously-narrowing targets with a stable message before the transaction opens.
fn is_promotion_target(new_type: &PrimitiveType) -> bool {
    matches!(
        new_type,
        PrimitiveType::Long | PrimitiveType::Double | PrimitiveType::Decimal { .. }
    )
}

/// True when the table already has a top-level column with this name (case-insensitive, matching
/// the case-insensitive `UpdateSchema` the tier-1 helper opens).
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

// === SET PROPERTIES =========================================================================

/// ===========================================================================================
/// The R1 pre-parse recognizer: blank the word `PROPERTIES` out of
/// `ALTER TABLE <name> SET PROPERTIES (…)` so the stock parser reads the rest as
/// `AlterTableOperation::SetOptionsParens`. Returns `None` when the statement is not that shape.
/// ===========================================================================================
///
/// Byte offsets come from the SCRUBBED text, whose length is byte-identical to the input, so a
/// `PROPERTIES` inside a string literal, a quoted identifier, or a comment is structurally
/// invisible and can never be edited out of the user's SQL.
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

/// Apply a validated `SET PROPERTIES (…)` list as ONE property transaction (sets and unsets
/// together, so a partial failure cannot leave half-applied property state).
async fn apply_set_properties(target: &CreateTarget, options: &[SqlOption]) -> Result<()> {
    let (sets, unsets) = parse_set_properties(options)?;
    repark_iceberg::write::alter::alter_table_properties(
        target.catalog.as_ref(),
        &target.ident(),
        &sets,
        &unsets,
    )
    .await
    .map_err(iceberg_err)
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

        // A DOTTED key is a raw Iceberg property. It is settable only through the
        // `extra_properties` hatch (design §2 Q1: dotted keys never become bare API), but UNSET
        // has no other spelling, so `"write.merge.mode" = DEFAULT` is the round trip.
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
                return Err(DataFusionError::NotImplemented(format!(
                    "{form}: `format_version` cannot be changed after creation — this engine \
                     creates Iceberg format v2 tables and has no format upgrade path. TRIGGER \
                     for implementing it: a fork `UpgradeFormatVersion` action reachable through \
                     repark-iceberg"
                )));
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

/// Q3: partition-spec evolution is deliberately absent from SQL, and THIS is the spelling the
/// design pre-designated for it — so the refusal says so and names what does the job today.
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
