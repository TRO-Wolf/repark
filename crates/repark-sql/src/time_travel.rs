//! ANSI/Trino time travel: `FOR VERSION AS OF <n | 'ref'>` and `FOR TIMESTAMP AS OF <ts>`

use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};
use repark_core::time_travel::{
    RefSelector, TimeTravelSpec, evaluate_sql_timestamp_asof, extract_timestamp_expr,
    read_table_at, selector_time_travel_refusal,
};
use repark_core::{
    EngineContext, branch_time_travel_refusal, invalid_version_pin, parse_version_value,
};

/// Process-wide counter for ephemeral temp-view names.
static TEMP_VIEW_SEQ: AtomicU64 = AtomicU64::new(1);

/// One FROM/JOIN relation carrying a `FOR … AS OF` pin, with token indices for the splice.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TimeTravelSpan {
    /// Token index of the first table-name token.
    table_start: usize,
    /// Token index one past the last AS OF value token.
    clause_end: usize,
    /// The dotted table name, unquoted.
    table_parts: Vec<String>,
    /// What the clause pins to.
    pin: TimeTravelPin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TimeTravelPin {
    Version(TimeTravelSpec),
    TimestampExpr(Vec<Token>),
}

/// Which AS OF flavour a clause is.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TimeTravelKind {
    Version,
    Timestamp,
}

impl TimeTravelKind {
    /// The clause as the user spells it, for messages.
    const fn spelling(self) -> &'static str {
        match self {
            Self::Version => "FOR VERSION AS OF",
            Self::Timestamp => "FOR TIMESTAMP AS OF",
        }
    }
}

/// True when `sql` carries an ANSI `FOR VERSION|TIMESTAMP AS OF` clause this door must rewrite.
#[cfg(test)]
pub(crate) fn sql_has_time_travel(sql: &str) -> bool {
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql).tokenize() else {
        return false;
    };
    matches!(find_time_travel_spans(&tokens), Ok(spans) if !spans.is_empty())
}

/// The ephemeral names registered by one rewrite are released by the router after planning.
#[derive(Debug, Default)]
pub(crate) struct PinnedViews {
    names: Vec<String>,
}

impl PinnedViews {
    /// Deregister everything this statement registered.
    pub(crate) fn release(&self, ctx: &datafusion::prelude::SessionContext) {
        for name in &self.names {
            let _ = ctx.deregister_table(name.as_str());
        }
    }
}

/// Rewrite every `FOR … AS OF` relation in `sql` to an ephemeral snapshot-pinned temp view.
pub(crate) async fn prepare_time_travel_sql(
    cx: &EngineContext<'_>,
    sql: &str,
    pinned: &mut PinnedViews,
) -> Result<Option<String>> {
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql).tokenize() else {
        // A tokenizer failure is the parser's error to report, with the sniff on top.
        return Ok(None);
    };
    let spans = find_time_travel_spans(&tokens)?;
    if spans.is_empty() {
        return Ok(None);
    }

    // Resolve + register right-to-left so earlier token indices stay valid across the splices.
    let mut tokens = tokens;
    for span in spans.into_iter().rev() {
        let name = register_pinned_view(cx, &span, pinned).await?;
        let replacement = Token::Word(Word {
            value: name,
            quote_style: None,
            keyword: Keyword::NoKeyword,
        });
        tokens.splice(
            span.table_start..span.clause_end,
            std::iter::once(replacement),
        );
    }
    Ok(Some(tokens_to_sql(&tokens)))
}

/// Resolve one span and register it under the rewrite's ephemeral name.
async fn register_pinned_view(
    cx: &EngineContext<'_>,
    span: &TimeTravelSpan,
    pinned: &mut PinnedViews,
) -> Result<String> {
    if span.table_parts.len() >= 4 {
        match RefSelector::from_table_parts(&span.table_parts) {
            RefSelector::Branch => return Err(branch_time_travel_refusal()),
            RefSelector::Tag => return Err(selector_time_travel_refusal()),
            RefSelector::None => {}
        }
    }
    if span.table_parts.len() != 3 {
        return Err(DataFusionError::Plan(format!(
            "time travel requires a three-part `catalog.schema.table` name, got `{}`",
            span.table_parts.join(".")
        )));
    }
    let spec = match &span.pin {
        TimeTravelPin::Version(spec) => spec.clone(),
        TimeTravelPin::TimestampExpr(tokens) => {
            let millis = evaluate_sql_timestamp_asof(cx.ctx, tokens, &cx.session_time_zone).await?;
            TimeTravelSpec::TimestampMs(millis)
        }
    };
    let frame = read_table_at(
        cx.ctx,
        cx.catalogs,
        &span.table_parts,
        &spec,
        &cx.session_time_zone,
    )
    .await?;
    // `read_table_at` registers the core name first; record it before consuming the frame.
    if let Some(core_name) = core_pinned_name(frame.logical_plan()) {
        pinned.names.push(core_name);
    }
    let name = format!(
        "__repark_ansi_tt_{}",
        TEMP_VIEW_SEQ.fetch_add(1, Ordering::Relaxed)
    );
    // Record the ANSI name before `register_table` so cleanup covers a failed registration.
    pinned.names.push(name.clone());
    cx.ctx
        .register_table(name.as_str(), frame.into_view())
        .map_err(|error| {
            DataFusionError::Plan(format!(
                "failed to register the time-travel view for `{}`: {error}",
                span.table_parts.join(".")
            ))
        })?;
    Ok(name)
}

/// Extract the core name after [`read_table_at`] registers it; SQL cannot discover that name.
fn core_pinned_name(plan: &datafusion::logical_expr::LogicalPlan) -> Option<String> {
    let datafusion::logical_expr::LogicalPlan::TableScan(scan) = plan else {
        return None;
    };
    let name = scan.table_name.table();
    name.starts_with("__repark_tt_").then(|| name.to_string())
}

// The scanner.

/// A token paired with its index in the original stream, whitespace and EOF dropped.
type Sig<'a> = (usize, &'a Token);

/// Scan for `<name> FOR VERSION|TIMESTAMP AS OF <value>`.
fn find_time_travel_spans(tokens: &[Token]) -> Result<Vec<TimeTravelSpan>> {
    let significant: Vec<Sig<'_>> = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token, Token::Whitespace(_) | Token::EOF))
        .collect();

    let mut spans = Vec::new();
    let mut index = 0usize;
    while index < significant.len() {
        let Some(kind) = clause_kind_at(&significant, index) else {
            index += 1;
            continue;
        };
        // `FOR <kind> AS OF` occupies four significant tokens starting at `index`.
        let value_index = index + 4;
        let (pin, consumed) = parse_as_of_value(kind, tokens, &significant, value_index)?;

        // The table name is the ident-dot-ident run immediately before FOR.
        let (name_start, table_parts) =
            table_name_before(&significant, index).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "{} must follow a table reference (FROM catalog.schema.table {} …)",
                    kind.spelling(),
                    kind.spelling()
                ))
            })?;

        spans.push(TimeTravelSpan {
            table_start: significant[name_start].0,
            clause_end: if consumed == 0 {
                significant[value_index.min(significant.len().saturating_sub(1))].0
            } else {
                significant[value_index + consumed - 1].0 + 1
            },
            table_parts,
            pin,
        });
        index = (value_index + consumed).max(index + 1);
    }
    Ok(spans)
}

/// The clause kind when `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF` starts at `index`.
fn clause_kind_at(significant: &[Sig<'_>], index: usize) -> Option<TimeTravelKind> {
    if !word_eq(significant, index, "FOR")
        || !word_eq(significant, index + 2, "AS")
        || !word_eq(significant, index + 3, "OF")
    {
        return None;
    }
    match word_at(significant, index + 1)?
        .to_ascii_uppercase()
        .as_str()
    {
        "VERSION" => Some(TimeTravelKind::Version),
        "TIMESTAMP" => Some(TimeTravelKind::Timestamp),
        _ => None,
    }
}

fn parse_as_of_value(
    kind: TimeTravelKind,
    tokens: &[Token],
    significant: &[Sig<'_>],
    index: usize,
) -> Result<(TimeTravelPin, usize)> {
    if matches!(kind, TimeTravelKind::Timestamp) {
        let found = extract_timestamp_expr(significant, index);
        let consumed = found.len();
        if consumed == 0 {
            return Ok((TimeTravelPin::TimestampExpr(Vec::new()), 0));
        }
        let start = significant[index].0;
        let end = significant[index + consumed - 1].0 + 1;
        return Ok((
            TimeTravelPin::TimestampExpr(tokens[start..end].to_vec()),
            consumed,
        ));
    }
    let token = significant
        .get(index)
        .map(|(_, token)| *token)
        .ok_or_else(invalid_version_pin)?;

    // Unary minus then number: sqlparser emits Minus then Number; a negative pin needs this arm.
    if matches!(token, Token::Minus) {
        let Some(Token::Number(text, _)) = significant.get(index + 1).map(|(_, token)| *token)
        else {
            return Err(invalid_version_pin());
        };
        return parse_version_value(&format!("-{text}"))
            .map(|spec| (TimeTravelPin::Version(spec), 2))
            .map_err(|_| invalid_version_pin());
    }

    let raw = match token {
        Token::Number(text, _) | Token::SingleQuotedString(text) => text.clone(),
        _ => return Err(invalid_version_pin()),
    };
    parse_version_value(&raw)
        .map(|spec| (TimeTravelPin::Version(spec), 1))
        .map_err(|_| invalid_version_pin())
}

/// Walk left from `clause_start` over ident-dot-ident, returning the run start and token list.
fn table_name_before(significant: &[Sig<'_>], clause_start: usize) -> Option<(usize, Vec<String>)> {
    if clause_start == 0 {
        return None;
    }
    let mut start = clause_start - 1;
    if !is_ident_token(significant[start].1) {
        return None;
    }
    while start >= 2
        && matches!(significant[start - 1].1, Token::Period)
        && is_ident_token(significant[start - 2].1)
    {
        start -= 2;
    }
    let parts: Vec<String> = significant[start..clause_start]
        .iter()
        .filter_map(|(_, token)| match token {
            Token::Word(word) => Some(word.value.clone()),
            _ => None,
        })
        .collect();
    if parts.is_empty() {
        None
    } else {
        Some((start, parts))
    }
}

/// ANSI identifier tokens: a word, quoted or not.
fn is_ident_token(token: &Token) -> bool {
    matches!(token, Token::Word(_))
}

fn word_at<'a>(significant: &[Sig<'a>], index: usize) -> Option<&'a str> {
    match significant.get(index).map(|(_, token)| *token) {
        Some(Token::Word(word)) => Some(word.value.as_str()),
        _ => None,
    }
}

fn word_eq(significant: &[Sig<'_>], index: usize, expected: &str) -> bool {
    word_at(significant, index).is_some_and(|word| word.eq_ignore_ascii_case(expected))
}

fn tokens_to_sql(tokens: &[Token]) -> String {
    tokens.iter().map(ToString::to_string).collect()
}

#[cfg(test)]
mod exec_tests;
#[cfg(test)]
mod tests;
