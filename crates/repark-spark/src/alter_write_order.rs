use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::spec::{NullOrder, SortDirection};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;

use crate::{catalog_handle, iceberg_err, reregister};
use repark_iceberg::write::sort_order::WriteSortField;

pub(crate) struct WriteOrderField {
    name: String,
    direction: SortDirection,
    null_order: NullOrder,
}

pub(crate) struct WriteOrderDdl {
    table_parts: Vec<String>,
    fields: Vec<WriteOrderField>,
    distribution_mode: Option<String>,
}

#[derive(Debug, Clone)]
enum Sig {
    Word(String),
    Period,
    Number(String),
    LParen,
    RParen,
    Comma,
    String(String),
    Other,
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
    let (fields, next) = parse_order_list(significant, ordered_start + 2)?;
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
    let (fields, after) = parse_order_list(significant, next + 2)?;
    end_of_clause(significant, after)?;
    Ok(WriteOrderDdl {
        table_parts,
        fields,
        distribution_mode: Some("hash".to_string()),
    })
}

fn parse_order_list(significant: &[Sig], start: usize) -> Result<(Vec<WriteOrderField>, usize)> {
    if !matches!(significant.get(start), Some(Sig::LParen)) {
        let segments = split_sig_comma_segments(&significant[start..]);
        if segments.is_empty() {
            return Err(DataFusionError::Plan(
                "ALTER TABLE WRITE ORDERED BY requires at least one column".into(),
            ));
        }
        let mut fields = Vec::with_capacity(segments.len());
        for segment in segments {
            fields.push(parse_order_segment(segment)?);
        }
        return Ok((fields, significant.len()));
    }
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
        DataFusionError::Plan("ALTER TABLE WRITE ORDERED BY: unterminated column list".into())
    })?;
    let segments = split_sig_comma_segments(&significant[start + 1..close]);
    if segments.is_empty() {
        return Err(DataFusionError::Plan(
            "ALTER TABLE WRITE ORDERED BY requires at least one column".into(),
        ));
    }
    let mut fields = Vec::with_capacity(segments.len());
    for segment in segments {
        fields.push(parse_order_segment(segment)?);
    }
    Ok((fields, close + 1))
}

fn parse_order_segment(segment: &[Sig]) -> Result<WriteOrderField> {
    let mut name = match segment.first() {
        Some(Sig::Word(word)) => word.clone(),
        Some(Sig::String(_)) => {
            return Err(DataFusionError::Plan(
                "ALTER TABLE WRITE ORDERED BY expects bare column names, quoted names are not \
                 supported yet"
                    .into(),
            ));
        }
        _ => {
            return Err(DataFusionError::Plan(
                "ALTER TABLE WRITE ORDERED BY column entry must start with a column name".into(),
            ));
        }
    };
    let mut index = 1usize;
    while matches!(segment.get(index), Some(Sig::Period)) {
        let Some(Sig::Word(part)) = segment.get(index + 1) else {
            break;
        };
        name.push('.');
        name.push_str(part);
        index += 2;
    }
    if matches!(segment.get(index), Some(Sig::LParen)) {
        return Err(DataFusionError::NotImplemented(format!(
            "ALTER TABLE WRITE ORDERED BY transform `{name}(…)` is not supported yet — the \
             fork's sort-order action only models identity sort fields"
        )));
    }
    let mut direction = SortDirection::Ascending;
    if word_eq(segment, index, "ASC") {
        index += 1;
    } else if word_eq(segment, index, "DESC") {
        direction = SortDirection::Descending;
        index += 1;
    }
    let mut null_order = match direction {
        SortDirection::Ascending => NullOrder::First,
        SortDirection::Descending => NullOrder::Last,
    };
    if word_eq(segment, index, "NULLS") {
        if word_eq(segment, index + 1, "FIRST") {
            null_order = NullOrder::First;
        } else if word_eq(segment, index + 1, "LAST") {
            null_order = NullOrder::Last;
        } else {
            return Err(DataFusionError::Plan(format!(
                "ALTER TABLE WRITE ORDERED BY column `{name}` expects NULLS FIRST or NULLS LAST, \
                 got `{}`",
                render_sig_at(segment, index + 1)
            )));
        }
        index += 2;
    }
    if index < segment.len() {
        return Err(DataFusionError::Plan(format!(
            "trailing tokens after WRITE ORDERED BY column `{name}` (starting at `{}`)",
            render_sig_at(segment, index)
        )));
    }
    Ok(WriteOrderField {
        name,
        direction,
        null_order,
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

fn end_of_clause(significant: &[Sig], next: usize) -> Result<()> {
    if next < significant.len() {
        return Err(DataFusionError::Plan(format!(
            "trailing tokens after ALTER TABLE WRITE clause (starting at `{}`)",
            render_sig_at(significant, next)
        )));
    }
    Ok(())
}

fn tokenize_significant(sql: &str) -> Option<Vec<Sig>> {
    use datafusion::sql::sqlparser::dialect::DatabricksDialect;
    use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
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
