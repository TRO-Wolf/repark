use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;

use crate::sort_order_parse::{
    OrderParseError, Sig, WriteOrderField, collect_name_parts, is_period_at, parse_order_list,
    render_sig_at, tokenize_significant, word_at, word_eq,
};
use crate::{catalog_handle, iceberg_err, reregister};
use repark_iceberg::write::sort_order::WriteSortField;

pub(crate) struct WriteOrderDdl {
    table_parts: Vec<String>,
    fields: Vec<WriteOrderField>,
    distribution_mode: Option<String>,
}

fn alter_order_error(error: OrderParseError) -> DataFusionError {
    match error {
        OrderParseError::Empty => DataFusionError::Plan(
            "ALTER TABLE WRITE ORDERED BY requires at least one column".into(),
        ),
        OrderParseError::Unterminated => {
            DataFusionError::Plan("ALTER TABLE WRITE ORDERED BY: unterminated column list".into())
        }
        OrderParseError::QuotedName => DataFusionError::Plan(
            "ALTER TABLE WRITE ORDERED BY expects bare column names, quoted names are not \
             supported yet"
                .into(),
        ),
        OrderParseError::MissingName => DataFusionError::Plan(
            "ALTER TABLE WRITE ORDERED BY column entry must start with a column name".into(),
        ),
        OrderParseError::Transform { name } => DataFusionError::NotImplemented(format!(
            "ALTER TABLE WRITE ORDERED BY transform `{name}(…)` is not supported yet — the \
             fork's sort-order action only models identity sort fields"
        )),
        OrderParseError::BadNulls { name, got } => DataFusionError::Plan(format!(
            "ALTER TABLE WRITE ORDERED BY column `{name}` expects NULLS FIRST or NULLS LAST, \
             got `{got}`"
        )),
        OrderParseError::Trailing { name, got } => DataFusionError::Plan(format!(
            "trailing tokens after WRITE ORDERED BY column `{name}` (starting at `{got}`)"
        )),
    }
}

pub(crate) fn try_parse_write_order_ddl(sql: &str) -> Option<Result<WriteOrderDdl>> {
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
    if !word_eq(&significant, index, "WRITE") {
        return None;
    }
    Some(parse_write_clause(&significant, index + 1, table_parts))
}

pub(crate) async fn execute_write_order_ddl(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    ddl: WriteOrderDdl,
) -> Result<DataFrame> {
    let (catalog_name, ident) = table_parts_to_ident(&ddl.table_parts)?;
    let handle = catalog_handle(catalogs, &catalog_name)?;
    let fields = ddl
        .fields
        .iter()
        .map(|field| WriteSortField {
            name: field.name.clone(),
            direction: field.direction,
            null_order: field.null_order,
        })
        .collect::<Vec<_>>();
    repark_iceberg::write::sort_order::apply_write_order(
        handle.as_ref(),
        &ident,
        &fields,
        ddl.distribution_mode.as_deref(),
    )
    .await
    .map_err(iceberg_err)?;
    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, handle.clone(), &catalog_name, &namespace).await?;
    ctx.read_empty()
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

fn parse_write_clause(
    significant: &[Sig],
    start: usize,
    table_parts: Vec<String>,
) -> Result<WriteOrderDdl> {
    if word_eq(significant, start, "UNORDERED") {
        end_of_clause(significant, start + 1)?;
        return Ok(WriteOrderDdl {
            table_parts,
            fields: Vec::new(),
            distribution_mode: Some("none".to_string()),
        });
    }
    if word_eq(significant, start, "DISTRIBUTED") {
        return parse_distributed(significant, start + 1, table_parts);
    }
    let (ordered_start, distribution_mode) = if word_eq(significant, start, "LOCALLY") {
        (start + 1, None)
    } else {
        (start, Some("range".to_string()))
    };
    if !(word_eq(significant, ordered_start, "ORDERED")
        && word_eq(significant, ordered_start + 1, "BY"))
    {
        return Err(DataFusionError::Plan(format!(
            "ALTER TABLE WRITE expects ORDERED BY, LOCALLY ORDERED BY, DISTRIBUTED BY PARTITION, \
             or UNORDERED, got `{}`",
            render_sig_at(significant, start)
        )));
    }
    let (fields, next) =
        parse_order_list(significant, ordered_start + 2).map_err(alter_order_error)?;
    end_of_clause(significant, next)?;
    Ok(WriteOrderDdl {
        table_parts,
        fields,
        distribution_mode,
    })
}

fn parse_distributed(
    significant: &[Sig],
    start: usize,
    table_parts: Vec<String>,
) -> Result<WriteOrderDdl> {
    if !word_eq(significant, start, "BY") {
        return Err(DataFusionError::Plan(format!(
            "ALTER TABLE WRITE DISTRIBUTED expects BY PARTITION, got `{}`",
            render_sig_at(significant, start)
        )));
    }
    if !word_eq(significant, start + 1, "PARTITION") {
        return Err(DataFusionError::Plan(format!(
            "ALTER TABLE WRITE DISTRIBUTED BY got `{}`, expecting 'PARTITION'",
            render_sig_at(significant, start + 1)
        )));
    }
    let mut next = start + 2;
    if word_eq(significant, next, "LOCALLY") {
        next += 1;
    }
    if next >= significant.len() {
        if word_eq(significant, start + 2, "LOCALLY") {
            return Err(DataFusionError::Plan(
                "ALTER TABLE WRITE DISTRIBUTED BY PARTITION LOCALLY expects ORDERED BY (…)".into(),
            ));
        }
        return Ok(WriteOrderDdl {
            table_parts,
            fields: Vec::new(),
            distribution_mode: Some("hash".to_string()),
        });
    }
    if !(word_eq(significant, next, "ORDERED") && word_eq(significant, next + 1, "BY")) {
        return Err(DataFusionError::Plan(format!(
            "ALTER TABLE WRITE DISTRIBUTED BY PARTITION expects end of statement or \
             [LOCALLY] ORDERED BY (…), got `{}`",
            render_sig_at(significant, next)
        )));
    }
    let (fields, after) = parse_order_list(significant, next + 2).map_err(alter_order_error)?;
    end_of_clause(significant, after)?;
    Ok(WriteOrderDdl {
        table_parts,
        fields,
        distribution_mode: Some("hash".to_string()),
    })
}

fn end_of_clause(significant: &[Sig], next: usize) -> Result<()> {
    if next < significant.len() {
        return Err(DataFusionError::Plan(format!(
            "trailing tokens after ALTER TABLE WRITE clause (starting at `{}`)",
            render_sig_at(significant, next)
        )));
    }
    Ok(())
}
