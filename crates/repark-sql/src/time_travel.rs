//! ANSI/Trino time travel: `FOR VERSION AS OF <n | 'ref'>` and `FOR TIMESTAMP AS OF <ts>`
//! (design §2 Q5, graft G7).
//!
//! The R1 spike settled the mechanism: stock sqlparser consumes `FOR` as the head of
//! `FOR UPDATE` / `FOR SHARE`, so `SELECT … FROM t FOR VERSION AS OF 1` cannot parse. The clause
//! is therefore recognized and REWRITTEN before the parser ever sees it — the table reference plus
//! its AS OF clause are replaced by an ephemeral temp view backed by a snapshot-pinned provider,
//! and the rewritten SQL parses as ordinary ANSI.
//!
//! **Ordering is load-bearing** (BUG-010, the ordering-defect class the design's judges named):
//! the multi-statement refuse runs FIRST, in [`crate::guards`], so a script can never have its
//! SECOND statement rewritten by this scanner. [`crate::router::execute`] slots the scan directly
//! after the guard set and before the parse.
//!
//! **Quoting is ANSI, and that is the parameterization** (graft G7). The Spark door's scanner
//! tokenizes with `DatabricksDialect` and treats a single-quoted string as a possible identifier;
//! this one tokenizes with [`GenericDialect`] and takes `"quoted"` — never `'quoted'` — as an
//! identifier. So `ice."sales"."t"` is a three-part name here, while `'sales'` in table position
//! is not. The AS OF VALUE reads the other way round: `'main'` is a branch/tag ref STRING, and a
//! double-quoted value is an identifier, which is not a legal version literal.
//!
//! `FOR` is MANDATORY (design §2 Q5). Bare `VERSION AS OF` / `TIMESTAMP AS OF` and Spark's
//! `FOR SYSTEM_VERSION` / `FOR SYSTEM_TIME` are deliberately NOT scanned: they fail to parse and
//! the wrong-door sniff ([`crate::sniff`]) names the ANSI spelling.
//!
//! Resolution is the phase-1 hoist, not a copy: [`repark_core::TimeTravelSpec`],
//! [`repark_core::parse_version_value`] / [`repark_core::parse_timestamp_to_ms`], and
//! [`repark_core::read_table_at`] (which builds the fork's snapshot-pinned static provider). This
//! module owns only the SQL-TEXT half.

use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};
use repark_core::{
    EngineContext, TimeTravelSpec, parse_timestamp_to_ms, parse_version_value, read_table_at,
};

/// Process-wide counter so ephemeral temp-view names never collide across concurrent sessions.
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
    spec: TimeTravelSpec,
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

/// ===========================================================================================
/// True when `sql` carries an ANSI `FOR VERSION|TIMESTAMP AS OF` clause this door must rewrite.
///
/// The router does not need this predicate — [`prepare_time_travel_sql`] answers "nothing to do"
/// with `Ok(None)` on the same scan — but the RECOGNITION boundary is a behavior in its own
/// right (which spellings this door claims, and which it leaves to the wrong-door sniff), so it
/// is pinned separately.
/// ===========================================================================================
#[cfg(test)]
pub(crate) fn sql_has_time_travel(sql: &str) -> bool {
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql).tokenize() else {
        return false;
    };
    matches!(find_time_travel_spans(&tokens), Ok(spans) if !spans.is_empty())
}

/// ===========================================================================================
/// Rewrite every `FOR … AS OF` relation in `sql` to an ephemeral snapshot-pinned temp view,
/// returning the rewritten SQL. `Ok(None)` means there was nothing to rewrite.
/// ===========================================================================================
///
/// # Errors
/// A recognized-but-malformed clause (an AS OF value this door cannot read, a name that is not
/// three-part), an unregistered catalog, a snapshot that does not resolve, or a provider /
/// registration failure.
pub(crate) async fn prepare_time_travel_sql(
    cx: &EngineContext<'_>,
    sql: &str,
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
        let name = register_pinned_view(cx, &span).await?;
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

/// Resolve one span to a snapshot-pinned relation and register it under a fresh ephemeral name.
///
/// [`read_table_at`] is the hoisted core half: it resolves the snapshot id against the table's
/// metadata and builds the fork's `IcebergStaticTableProvider` — a real pinned scan, never a
/// post-hoc filter over the current snapshot. Re-registering its frame as a view is what gives
/// the rewrite a NAME to splice into the statement.
async fn register_pinned_view(cx: &EngineContext<'_>, span: &TimeTravelSpan) -> Result<String> {
    if span.table_parts.len() != 3 {
        return Err(DataFusionError::Plan(format!(
            "time travel requires a three-part `catalog.schema.table` name, got `{}`",
            span.table_parts.join(".")
        )));
    }
    let frame = read_table_at(cx.ctx, cx.catalogs, &span.table_parts, &span.spec).await?;
    let name = format!(
        "__repark_ansi_tt_{}",
        TEMP_VIEW_SEQ.fetch_add(1, Ordering::Relaxed)
    );
    let _ = cx.ctx.deregister_table(name.as_str());
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

// === The scanner ============================================================================

/// A token paired with its index in the original stream, whitespace and EOF dropped.
type Sig<'a> = (usize, &'a Token);

/// Scan for `<name> FOR VERSION|TIMESTAMP AS OF <value>`.
///
/// Returns `Err` when a clause is RECOGNIZED (the `FOR <kind> AS OF` keyword run is present) but
/// its value cannot be read. Falling through silently would hand the user the parser's
/// `Expected: one of UPDATE or SHARE, found: VERSION`, which explains nothing.
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
        let (spec, consumed) = parse_as_of_value(kind, &significant, value_index)?;

        // The table name is the `ident (. ident)*` run immediately before `FOR`.
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
            clause_end: significant[value_index + consumed - 1].0 + 1,
            table_parts,
            spec,
        });
        index = value_index + consumed;
    }
    Ok(spans)
}

/// The clause kind when `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF` starts at `index`.
///
/// Spark's `FOR SYSTEM_VERSION` / `FOR SYSTEM_TIME` and the bare (FOR-less) forms are NOT
/// recognized here by design — they are wrong-door spellings the sniff answers.
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

/// Read the AS OF value, returning the spec and how many significant tokens it consumed.
///
/// Accepted: a number (with an optional unary minus — Iceberg snapshot ids are signed `i64` and
/// are routinely negative), a single-quoted string (a branch/tag ref for VERSION, a timestamp
/// literal for TIMESTAMP), and `TIMESTAMP '…'`. A double-quoted token is an IDENTIFIER in this
/// door, not a literal, so it is refused rather than silently read as a ref name.
fn parse_as_of_value(
    kind: TimeTravelKind,
    significant: &[Sig<'_>],
    index: usize,
) -> Result<(TimeTravelSpec, usize)> {
    let bad = |detail: &str| {
        DataFusionError::Plan(format!(
            "{} value is not usable: {detail}. Write {}",
            kind.spelling(),
            match kind {
                TimeTravelKind::Version =>
                    "FOR VERSION AS OF <snapshot-id> or FOR VERSION AS OF 'branch-or-tag'",
                TimeTravelKind::Timestamp =>
                    "FOR TIMESTAMP AS OF '2024-01-01 00:00:00' (or TIMESTAMP '…')",
            }
        ))
    };
    let token = significant
        .get(index)
        .map(|(_, token)| *token)
        .ok_or_else(|| bad("the clause ends after AS OF"))?;

    // `TIMESTAMP '…'` — two significant tokens.
    if matches!(token, Token::Word(word) if word.value.eq_ignore_ascii_case("TIMESTAMP")) {
        let Some(Token::SingleQuotedString(text)) = significant.get(index + 1).map(|(_, t)| *t)
        else {
            return Err(bad("TIMESTAMP must be followed by a single-quoted literal"));
        };
        return match kind {
            TimeTravelKind::Timestamp => parse_timestamp_to_ms(text)
                .map(|ms| (TimeTravelSpec::TimestampMs(ms), 2))
                .map_err(|error| bad(&error.to_string())),
            TimeTravelKind::Version => Err(bad("a TIMESTAMP literal is not a version")),
        };
    }

    // Unary minus + number: sqlparser emits `Minus` then `Number`, so without this arm a
    // negative snapshot id would not be recognized at all.
    if matches!(token, Token::Minus) {
        let Some(Token::Number(text, _)) = significant.get(index + 1).map(|(_, t)| *t) else {
            return Err(bad("`-` must be followed by a number"));
        };
        return spec_from_literal(kind, &format!("-{text}"), 2).map_err(|detail| bad(&detail));
    }

    match token {
        Token::Number(text, _) | Token::SingleQuotedString(text) => {
            spec_from_literal(kind, text, 1).map_err(|detail| bad(&detail))
        }
        // A `"quoted"` token arrives as a `Word` carrying its quote style — it is an IDENTIFIER in
        // this door, and silently reading it as a ref name would make `"main"` and `'main'` mean
        // the same thing here while meaning different things everywhere else in ANSI SQL.
        Token::Word(word) if word.quote_style == Some('"') => Err(bad(&format!(
            "`\"{0}\"` is a quoted IDENTIFIER in this door, not a literal — use '{0}'",
            word.value
        ))),
        Token::Word(word) => Err(bad(&format!(
            "`{}` is a bare identifier, not a literal",
            word.value
        ))),
        other => Err(bad(&format!("`{other}` is not a literal"))),
    }
}

/// Turn an already-extracted literal into a [`TimeTravelSpec`], reusing the hoisted core parsers.
fn spec_from_literal(
    kind: TimeTravelKind,
    raw: &str,
    consumed: usize,
) -> std::result::Result<(TimeTravelSpec, usize), String> {
    match kind {
        TimeTravelKind::Version => parse_version_value(raw)
            .map(|spec| (spec, consumed))
            .map_err(|error| error.to_string()),
        TimeTravelKind::Timestamp => parse_timestamp_to_ms(raw)
            .map(|ms| (TimeTravelSpec::TimestampMs(ms), consumed))
            .map_err(|error| error.to_string()),
    }
}

/// Walk left from `clause_start` over `ident (. ident)*`, returning the run's start index and the
/// unquoted name parts.
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

/// ANSI identifier tokens: a word, quoted or not (sqlparser models a `"quoted"` identifier as a
/// `Word` carrying its quote style). A single-quoted string is a STRING in this door and can never
/// be a table reference — that is the G7 parameterization, and it is the whole difference from the
/// Spark door's scanner, which accepts `'…'` in table position.
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
mod tests;
