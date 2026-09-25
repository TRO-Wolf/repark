use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::spec::Transform;
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, illegal_argument_error};

use crate::sort_order_parse::{
    OrderParseError, Sig, collect_name_parts, hex_constant, hex_literal_body, is_period_at,
    order_list_segments, parse_order_segment, quote_constant, quote_if_needed, render_sig_at,
    tokenize_significant, word_at, word_eq,
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
    Integer { value: i64, long: bool },
    Constant,
}

struct ParsedArgument {
    argument: TermArgument,
    rendered: String,
}

const AFTER_TERM_EXPECTING: &str = "{<EOF>, ',', 'ASC', 'DESC', 'DISTRIBUTED', 'LOCALLY', \
                                    'NULLS', 'ORDERED', 'UNORDERED'}";

fn alter_order_error(error: OrderParseError, sql: &str) -> DataFusionError {
    match error {
        OrderParseError::EmptySegment { got } => no_viable_alternative(&got, sql),
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

pub(crate) fn verbatim_write_order_sql(sql: &str) -> Option<std::borrow::Cow<'_, str>> {
    let mentions_write = sql
        .as_bytes()
        .windows(5)
        .any(|window| window.eq_ignore_ascii_case(b"WRITE"));
    (mentions_write && try_parse_write_order_ddl(sql).is_some())
        .then_some(std::borrow::Cow::Borrowed(sql))
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
    Some(parse_write_clause(
        &significant,
        index + 1,
        table_parts,
        sql,
    ))
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
    sql: &str,
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
        return parse_distributed(significant, start + 1, table_parts, sql);
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
    let (fields, next) = parse_write_order_list(significant, ordered_start + 2, sql)?;
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
    sql: &str,
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
    let (fields, after) = parse_write_order_list(significant, next + 2, sql)?;
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
    sql: &str,
) -> Result<(Vec<WriteSortField>, usize)> {
    let (segments, next) =
        order_list_segments(significant, start).map_err(|error| alter_order_error(error, sql))?;
    let fields = segments
        .into_iter()
        .map(|segment| parse_write_order_term(segment, sql))
        .collect::<Result<Vec<_>>>()?;
    Ok((fields, next))
}

fn parse_write_order_term(segment: &[Sig], sql: &str) -> Result<WriteSortField> {
    let (Some(Sig::Word(function)), Some(Sig::LParen)) = (segment.first(), segment.get(1)) else {
        let field = parse_order_segment(segment).map_err(|error| alter_order_error(error, sql))?;
        return Ok(WriteSortField {
            name: field.name,
            transform: Transform::Identity,
            direction: field.direction,
            null_order: field.null_order,
        });
    };
    let close = transform_close(segment)
        .ok_or_else(|| alter_order_error(OrderParseError::Unterminated, sql))?;
    if matches!(segment.get(close + 1), Some(Sig::LParen)) {
        return Err(extension_parse_error(
            &format!("mismatched input '(' expecting {AFTER_TERM_EXPECTING}"),
            sql,
        ));
    }
    let parsed = term_arguments(&segment[2..=close], sql)?
        .into_iter()
        .map(|argument| parse_term_argument(argument, function, &segment[..=close], sql))
        .collect::<Result<Vec<_>>>()?;
    let described = format!(
        "{function}({})",
        parsed
            .iter()
            .map(|argument| argument.rendered.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    let arguments: Vec<TermArgument> = parsed.into_iter().map(|parsed| parsed.argument).collect();
    let (name, transform) = resolve_term(function, &arguments, &described)?;
    let mut suffix = vec![Sig::Word(described)];
    suffix.extend_from_slice(&segment[close + 1..]);
    let order = parse_order_segment(&suffix).map_err(|error| alter_order_error(error, sql))?;
    Ok(WriteSortField {
        name,
        transform,
        direction: order.direction,
        null_order: order.null_order,
    })
}

fn transform_close(segment: &[Sig]) -> Option<usize> {
    let mut depth = 0_i32;
    for (index, token) in segment.iter().enumerate().skip(1) {
        match token {
            Sig::LParen => depth += 1,
            Sig::RParen => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn extension_parse_error(detail: &str, sql: &str) -> DataFusionError {
    DataFusionError::Plan(format!("\n{detail}\n== SQL ==\n{sql}"))
}

fn no_viable_alternative(token: &str, sql: &str) -> DataFusionError {
    extension_parse_error(&format!("no viable alternative at input '{token}'"), sql)
}

fn invalid_typed_literal(typed: &str, sql: &str) -> DataFusionError {
    let body = hex_literal_body(typed).unwrap_or(typed);
    extension_parse_error(
        &format!(
            "[INVALID_TYPED_LITERAL] The value of the typed literal \"X\" is invalid: '{body}'. \
             SQLSTATE: 42604"
        ),
        sql,
    )
}

fn term_arguments<'a>(inner_and_close: &'a [Sig], sql: &str) -> Result<Vec<&'a [Sig]>> {
    let mut arguments = Vec::new();
    let mut depth = 0_i32;
    let mut start = 0usize;
    let last = inner_and_close.len() - 1;
    for (index, token) in inner_and_close.iter().enumerate() {
        let ends_argument = index == last || (depth == 0 && matches!(token, Sig::Comma));
        if ends_argument {
            if start == index {
                return Err(no_viable_alternative(
                    &render_sig_at(inner_and_close, index),
                    sql,
                ));
            }
            arguments.push(&inner_and_close[start..index]);
            start = index + 1;
            continue;
        }
        match token {
            Sig::LParen => depth += 1,
            Sig::RParen => depth -= 1,
            _ => {}
        }
    }
    Ok(arguments)
}

fn parse_term_argument(
    argument: &[Sig],
    function: &str,
    term: &[Sig],
    sql: &str,
) -> Result<ParsedArgument> {
    let (negative, body) = match argument {
        [Sig::Minus, rest @ ..] if !rest.is_empty() => (true, rest),
        _ => (false, argument),
    };
    let sign = if negative { "-" } else { "" };
    match body {
        [Sig::Number(raw)] => Ok(number_literal(sign, raw)),
        [Sig::Word(word)] if word.starts_with(|first: char| first.is_ascii_digit()) => {
            typed_literal(sign, word).map_or_else(|| column_argument(argument, function, term), Ok)
        }
        [Sig::String(text)] if !negative => Ok(ParsedArgument {
            argument: TermArgument::Constant,
            rendered: quote_constant(text),
        }),
        [Sig::Hex(typed)] if !negative => match hex_constant(typed) {
            Some(rendered) => Ok(ParsedArgument {
                argument: TermArgument::Constant,
                rendered,
            }),
            None => Err(invalid_typed_literal(typed, sql)),
        },
        [Sig::Word(_), ..] if !negative => column_argument(argument, function, term),
        _ => Err(no_viable_alternative(&render_sig_at(argument, 0), sql)),
    }
}

fn number_literal(sign: &str, raw: &str) -> ParsedArgument {
    let signed = format!("{sign}{raw}");
    if let Ok(value) = signed.parse::<i64>() {
        return integer_argument(value, false);
    }
    let rendered = if raw.contains(['e', 'E']) {
        signed.parse::<f64>().map_or(signed, java_double_text)
    } else {
        let trimmed = raw.strip_suffix('.').unwrap_or(raw);
        let leading_zero = if trimmed.starts_with('.') { "0" } else { "" };
        format!("{sign}{leading_zero}{trimmed}")
    };
    ParsedArgument {
        argument: TermArgument::Constant,
        rendered,
    }
}

fn integer_literal(text: &str, long: bool) -> ParsedArgument {
    match text.parse::<i64>() {
        Ok(value) => integer_argument(value, long),
        Err(_) => ParsedArgument {
            argument: TermArgument::Constant,
            rendered: text.to_string(),
        },
    }
}

fn integer_argument(value: i64, long: bool) -> ParsedArgument {
    ParsedArgument {
        argument: TermArgument::Integer {
            value,
            long: long || i32::try_from(value).is_err(),
        },
        rendered: value.to_string(),
    }
}

fn java_double_text(value: f64) -> String {
    let magnitude = value.abs();
    if magnitude >= 1e7 || (magnitude < 1e-3 && magnitude != 0.0) {
        let scientific = format!("{value:e}");
        let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
        let mantissa = if mantissa.contains('.') {
            mantissa.to_string()
        } else {
            format!("{mantissa}.0")
        };
        return format!("{mantissa}E{exponent}");
    }
    if value.fract() == 0.0 {
        format!("{value:.1}")
    } else {
        value.to_string()
    }
}

fn typed_literal(sign: &str, word: &str) -> Option<ParsedArgument> {
    let digits_end = word
        .find(|character: char| !(character.is_ascii_digit() || character == '.'))
        .unwrap_or(word.len());
    let (number, suffix) = word.split_at(digits_end);
    let number = format!("{sign}{number}");
    match suffix.to_ascii_uppercase().as_str() {
        "L" if !number.contains('.') => Some(integer_literal(&number, true)),
        "S" | "Y" if !number.contains('.') => Some(ParsedArgument {
            argument: TermArgument::Constant,
            rendered: integer_literal(&number, false).rendered,
        }),
        "BD" => Some(ParsedArgument {
            argument: TermArgument::Constant,
            rendered: number,
        }),
        "D" | "F" => Some(ParsedArgument {
            argument: TermArgument::Constant,
            rendered: java_double_text(number.parse::<f64>().ok()?),
        }),
        _ => None,
    }
}

fn column_argument(argument: &[Sig], function: &str, term: &[Sig]) -> Result<ParsedArgument> {
    let parts = collect_name_parts(argument, 0, argument.len()).ok_or_else(|| {
        let rendered = (0..term.len())
            .map(|index| render_sig_at(term, index))
            .collect::<Vec<_>>()
            .join(" ");
        DataFusionError::Plan(format!(
            "ALTER TABLE WRITE ORDERED BY transform `{rendered}` takes column names and \
             constants only (transform `{function}`)"
        ))
    })?;
    let rendered = parts
        .iter()
        .map(|part| quote_if_needed(part))
        .collect::<Vec<_>>()
        .join(".");
    Ok(ParsedArgument {
        argument: TermArgument::Column(parts.join(".")),
        rendered,
    })
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
    let Some((value, long)) = arguments.iter().find_map(|argument| match argument {
        TermArgument::Integer { value, long } => Some((*value, *long)),
        _ => None,
    }) else {
        return Err(illegal_argument_error(format!(
            "Cannot find width for transform: {described}"
        )));
    };
    i32::try_from(value)
        .ok()
        .filter(|width| *width > 0 && !(long && *width == i32::MAX))
        .map(i32::unsigned_abs)
        .ok_or_else(|| {
            illegal_argument_error(format!("Unsupported width for transform: {described}"))
        })
}
