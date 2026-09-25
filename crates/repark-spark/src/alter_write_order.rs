use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::spec::Transform;
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, illegal_argument_error};

use crate::sort_order_parse::{
    OrderParseError, Sig, collect_name_parts, is_period_at, order_list_segments,
    parse_order_segment, render_sig_at, split_sig_comma_segments, tokenize_significant, word_at,
    word_eq,
};
use crate::{catalog_handle, iceberg_err, reregister};
use repark_iceberg::write::sort_order::WriteSortField;
use repark_iceberg::write::unsupported_error;

pub(crate) struct WriteOrderDdl {
    table_parts: Vec<String>,
    fields: Vec<WriteSortField>,
    distribution_mode: Option<String>,
}

enum TermArgument {
    Column(String),
    Integer(i64),
    Constant,
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
            "ALTER TABLE WRITE ORDERED BY transform `{name}(…)` is not supported: a transform \
             name is one identifier"
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
    let (catalog_name, ident) = table_parts_to_ident(catalogs, &ddl.table_parts)?;
    let handle = catalog_handle(catalogs, &catalog_name)?;
    repark_iceberg::write::sort_order::apply_write_order(
        handle.as_ref(),
        &ident,
        &ddl.fields,
        ddl.distribution_mode.as_deref(),
    )
    .await
    .map_err(iceberg_err)?;
    let namespace = crate::namespace_schema_name(ident.namespace());
    reregister(ctx, handle.clone(), &catalog_name, &namespace).await?;
    ctx.read_empty()
}

fn table_parts_to_ident(
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
    let (fields, next) = parse_write_order_list(significant, ordered_start + 2)?;
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
    let (fields, after) = parse_write_order_list(significant, next + 2)?;
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

fn parse_write_order_list(
    significant: &[Sig],
    start: usize,
) -> Result<(Vec<WriteSortField>, usize)> {
    let (segments, next) = order_list_segments(significant, start).map_err(alter_order_error)?;
    let fields = segments
        .into_iter()
        .map(parse_write_order_term)
        .collect::<Result<Vec<_>>>()?;
    Ok((fields, next))
}

fn parse_write_order_term(segment: &[Sig]) -> Result<WriteSortField> {
    let (Some(Sig::Word(function)), Some(Sig::LParen)) = (segment.first(), segment.get(1)) else {
        let field = parse_order_segment(segment).map_err(alter_order_error)?;
        return Ok(WriteSortField {
            name: field.name,
            transform: Transform::Identity,
            direction: field.direction,
            null_order: field.null_order,
        });
    };
    let close = transform_close(segment)?;
    let arguments = split_sig_comma_segments(&segment[2..close]);
    let described = describe_term(function, &arguments)?;
    let parsed = arguments
        .iter()
        .map(|argument| parse_term_argument(argument, &described))
        .collect::<Result<Vec<_>>>()?;
    let (name, transform) = resolve_term(function, &parsed, &described)?;
    let mut suffix = vec![Sig::Word(described)];
    suffix.extend_from_slice(&segment[close + 1..]);
    let order = parse_order_segment(&suffix).map_err(alter_order_error)?;
    Ok(WriteSortField {
        name,
        transform,
        direction: order.direction,
        null_order: order.null_order,
    })
}

fn transform_close(segment: &[Sig]) -> Result<usize> {
    let mut depth = 0_i32;
    for (index, token) in segment.iter().enumerate().skip(1) {
        match token {
            Sig::LParen => depth += 1,
            Sig::RParen => {
                depth -= 1;
                if depth == 0 {
                    return Ok(index);
                }
            }
            _ => {}
        }
    }
    Err(alter_order_error(OrderParseError::Unterminated))
}

fn describe_term(function: &str, arguments: &[&[Sig]]) -> Result<String> {
    let mut rendered = Vec::with_capacity(arguments.len());
    for argument in arguments {
        let text = (0..argument.len())
            .map(|index| render_sig_at(argument, index))
            .collect::<String>();
        if text.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "ALTER TABLE WRITE ORDERED BY transform `{function}(…)` has an empty argument"
            )));
        }
        rendered.push(text);
    }
    Ok(format!("{function}({})", rendered.join(", ")))
}

fn parse_term_argument(argument: &[Sig], described: &str) -> Result<TermArgument> {
    match argument {
        [Sig::Number(raw)] => Ok(integer_argument(raw)),
        [Sig::Minus, Sig::Number(raw)] => Ok(integer_argument(&format!("-{raw}"))),
        [Sig::String(_)] => Ok(TermArgument::Constant),
        [Sig::Word(_), ..] => collect_name_parts(argument, 0, argument.len())
            .map(|parts| TermArgument::Column(parts.join(".")))
            .ok_or_else(|| unsupported_argument(described)),
        _ => Err(unsupported_argument(described)),
    }
}

fn integer_argument(raw: &str) -> TermArgument {
    raw.parse::<i64>()
        .map_or(TermArgument::Constant, TermArgument::Integer)
}

fn unsupported_argument(described: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "ALTER TABLE WRITE ORDERED BY transform `{described}` takes column names and constants \
         only"
    ))
}

fn resolve_term(
    function: &str,
    arguments: &[TermArgument],
    described: &str,
) -> Result<(String, Transform)> {
    let lowered = function.to_ascii_lowercase();
    let columns: Vec<&String> = arguments
        .iter()
        .filter_map(|argument| match argument {
            TermArgument::Column(name) => Some(name),
            _ => None,
        })
        .collect();
    if lowered == "zorder" {
        return Err(illegal_argument_error("Term must be unbound".to_string()));
    }
    let [column] = columns.as_slice() else {
        return Err(illegal_argument_error(format!(
            "Cannot convert transform with more than one column reference: {described}"
        )));
    };
    let transform = match lowered.as_str() {
        "identity" => Transform::Identity,
        "bucket" => Transform::Bucket(transform_width(arguments, described)?),
        "truncate" => Transform::Truncate(transform_width(arguments, described)?),
        "year" | "years" => Transform::Year,
        "month" | "months" => Transform::Month,
        "date" | "day" | "days" => Transform::Day,
        "date_hour" | "hour" | "hours" => Transform::Hour,
        _ => {
            return Err(unsupported_error(format!(
                "Transform is not supported: {described}"
            )));
        }
    };
    Ok(((*column).clone(), transform))
}

fn transform_width(arguments: &[TermArgument], described: &str) -> Result<u32> {
    let width = arguments.iter().find_map(|argument| match argument {
        TermArgument::Integer(value) => Some(*value),
        _ => None,
    });
    let Some(width) = width else {
        return Err(illegal_argument_error(format!(
            "Cannot find width for transform: {described}"
        )));
    };
    i32::try_from(width)
        .ok()
        .filter(|value| *value > 0)
        .map(i32::unsigned_abs)
        .ok_or_else(|| {
            illegal_argument_error(format!("Unsupported width for transform: {described}"))
        })
}
