//! `MERGE INTO` lowering — sqlparser AST → [`repark_iceberg::write::merge::MergeSpec`].
//!
//! This side owns dialect/AST concerns only: rewrite the star forms stock sqlparser cannot parse
//! (`UPDATE SET *` / `INSERT *`) into a sentinel shape it can, resolve the three-part target,
//! pick the aliases Spark scoping rules imply (an unaliased table is referenced by its bare
//! name), re-render every expression back to SQL verbatim, and keep clause declaration order.
//! Execution semantics — first-match-wins, star expansion against the schemas, the cardinality
//! check, the copy-on-write rewrite, the commit — live in `repark_iceberg::write::merge`.

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    Assignment, AssignmentTarget, Expr, MergeAction, MergeClause, MergeClauseKind, MergeInsertExpr,
    MergeInsertKind, ObjectName, TableFactor,
};
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::Token;
use iceberg::{NamespaceIdent, TableIdent};
use repark_iceberg::write::merge::{
    InsertAction, InsertClause, MatchedAction, MatchedClause, MergeSpec,
};

use repark_core::CatalogRegistry;

use crate::{catalog_handle, name_parts};

/// The identifier [`rewrite_merge_stars`] substitutes for the `*` in `UPDATE SET *` /
/// `INSERT *` so stock sqlparser can parse the statement; [`lower_clause`] maps it back to the
/// star markers. Namespaced + unguessable (the `UNSET_SENTINEL` pattern in [`crate::alter`]);
/// hand-writing the exact substituted shape is indistinguishable from the star and behaves as
/// one, and any other use surfaces as an unknown-column error downstream.
const STAR_SENTINEL: &str = "__repark_merge_star_sentinel__";

/// ===========================================================================================
/// Route a parsed `MERGE INTO` statement: lower the AST to a [`MergeSpec`], resolve the target's
/// iceberg catalog handle, and hand off to the copy-on-write executor in `repark-write`.
/// ===========================================================================================
///
/// # Errors
/// Planning errors for malformed statements (non-three-part target, subquery source without an
/// alias, invalid clause/action combinations), `NotImplemented` for the documented v1 limits,
/// and anything the executor surfaces (including `MERGE_CARDINALITY_VIOLATION`).
pub(crate) async fn execute_merge(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &TableFactor,
    source: &TableFactor,
    on: &Expr,
    clauses: &[MergeClause],
) -> Result<DataFrame> {
    let (catalog_name, spec) = lower(table, source, on, clauses)?;
    let handle = catalog_handle(catalogs, &catalog_name)?;
    repark_iceberg::write::merge::execute_merge(ctx, handle, &spec).await?;
    ctx.read_empty()
}

/// ===========================================================================================
/// Rewrite the MERGE star forms stock sqlparser cannot parse — `THEN UPDATE SET *` and
/// `THEN INSERT *` — into sentinel forms it can (`SET <sentinel> = <sentinel>` /
/// `INSERT (<sentinel>) VALUES (<sentinel>)`); [`lower_clause`] maps the sentinel back to the
/// typed star markers. Two guards keep a multiplication `*` untouched: the star must FOLLOW the
/// `THEN [UPDATE] …` keyword run, and it must be able to END a clause there
/// ([`star_can_end_here`]) — the tokenizer tags any bare identifier SPELLED like a keyword
/// (`… CASE WHEN c THEN insert * 2 …` — `insert` carries `Keyword::INSERT`), so the anchor
/// alone is not position-proof.
/// ===========================================================================================
pub(crate) fn rewrite_merge_stars(tokens: &[Token]) -> Vec<Token> {
    let mut out = Vec::with_capacity(tokens.len());
    for (index, token) in tokens.iter().enumerate() {
        if matches!(token, Token::Mul) {
            let prior = keywords_before(tokens, index, 3);
            if prior.ends_with(&[Keyword::THEN, Keyword::UPDATE, Keyword::SET])
                && star_can_end_here(tokens, index + 1, true)
            {
                out.extend([sentinel_token(), Token::Eq, sentinel_token()]);
                continue;
            }
            if prior.ends_with(&[Keyword::THEN, Keyword::INSERT])
                && star_can_end_here(tokens, index + 1, false)
            {
                out.extend([
                    Token::LParen,
                    sentinel_token(),
                    Token::RParen,
                    Token::make_keyword("VALUES"),
                    Token::LParen,
                    sentinel_token(),
                    Token::RParen,
                ]);
                continue;
            }
        }
        out.push(token.clone());
    }
    out
}

/// True when whatever follows position `after` (skipping whitespace) can legally FOLLOW a real
/// star form: the next `WHEN` clause, an `OUTPUT`/`RETURNING` clause, a statement terminator,
/// or end of input — plus, for the SET form only, the comma of Spark's forbidden
/// `SET *, col = …` mix (rewritten so [`star_update`] can reject it with a targeted error).
/// Anything else (`2` in `CASE WHEN c THEN insert * 2`) marks the `*` as multiplication whose
/// left operand merely spells like a keyword.
fn star_can_end_here(tokens: &[Token], after: usize, allow_comma: bool) -> bool {
    for token in &tokens[after..] {
        match token {
            Token::Whitespace(_) => {}
            Token::Word(word) => {
                return matches!(
                    word.keyword,
                    Keyword::WHEN | Keyword::OUTPUT | Keyword::RETURNING
                );
            }
            Token::SemiColon => return true,
            Token::Comma => return allow_comma,
            _ => return false,
        }
    }
    true
}

/// The last (up to) `n` keyword tokens strictly before `index`, in statement order. Collection
/// stops at any non-word, non-whitespace token, so a `.`/`)`/operator boundary breaks the
/// pattern (quoted identifiers carry no keyword and break it too).
fn keywords_before(tokens: &[Token], index: usize, n: usize) -> Vec<Keyword> {
    let mut found = Vec::with_capacity(n);
    for token in tokens[..index].iter().rev() {
        match token {
            Token::Whitespace(_) => {}
            Token::Word(word) => {
                found.push(word.keyword);
                if found.len() == n {
                    break;
                }
            }
            _ => break,
        }
    }
    found.reverse();
    found
}

/// The sentinel as an unquoted identifier token.
fn sentinel_token() -> Token {
    Token::make_word(STAR_SENTINEL, None)
}

/// Lower the sqlparser MERGE pieces into the executor's plain-string spec.
fn lower(
    table: &TableFactor,
    source: &TableFactor,
    on: &Expr,
    clauses: &[MergeClause],
) -> Result<(String, MergeSpec)> {
    let (target_name, target_alias) = target_table(table)?;
    let parts = name_parts(&target_name);
    let [catalog, namespace, table_name] = parts.as_slice() else {
        return Err(DataFusionError::Plan(format!(
            "MERGE INTO target must be a three-part `catalog.namespace.table` name, got `{target_name}`"
        )));
    };
    let target_alias = match target_alias {
        Some(alias) => alias,
        None => bare_name_alias(&target_name)?,
    };
    let (source_from_sql, source_alias) = source_table(source)?;

    let mut matched = Vec::new();
    let mut not_matched = Vec::new();
    for clause in clauses {
        lower_clause(clause, &mut matched, &mut not_matched)?;
    }
    if matched.is_empty() && not_matched.is_empty() {
        return Err(DataFusionError::Plan(
            "MERGE INTO requires at least one WHEN clause".to_string(),
        ));
    }

    let spec = MergeSpec {
        target: TableIdent::new(NamespaceIdent::new(namespace.clone()), table_name.clone()),
        target_alias,
        source_from_sql,
        source_alias,
        on_sql: on.to_string(),
        matched,
        not_matched,
    };
    Ok((catalog.clone(), spec))
}

/// Sort one WHEN clause into the matched / not-matched lists, validating the kind–action pairing.
fn lower_clause(
    clause: &MergeClause,
    matched: &mut Vec<MatchedClause>,
    not_matched: &mut Vec<InsertClause>,
) -> Result<()> {
    let predicate_sql = clause.predicate.as_ref().map(ToString::to_string);
    match (&clause.clause_kind, &clause.action) {
        (MergeClauseKind::Matched, MergeAction::Update(update_expr)) => {
            let assignments = &update_expr.assignments;
            let action = match star_update(assignments)? {
                Some(all) => all,
                None => MatchedAction::Update {
                    assignments: lower_assignments(assignments)?,
                },
            };
            matched.push(MatchedClause {
                predicate_sql,
                action,
            });
        }
        (MergeClauseKind::Matched, MergeAction::Delete { .. }) => {
            matched.push(MatchedClause {
                predicate_sql,
                action: MatchedAction::Delete,
            });
        }
        (MergeClauseKind::Matched, MergeAction::Insert(_)) => {
            return Err(DataFusionError::Plan(
                "INSERT is not a valid WHEN MATCHED action".to_string(),
            ));
        }
        (
            MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget,
            MergeAction::Insert(insert),
        ) => {
            if star_insert(insert) {
                not_matched.push(InsertClause {
                    predicate_sql,
                    action: InsertAction::All,
                });
                return Ok(());
            }
            let MergeInsertKind::Values(values) = &insert.kind else {
                return Err(DataFusionError::NotImplemented(
                    "MERGE `INSERT ROW` is not supported; use INSERT (cols) VALUES (…)".to_string(),
                ));
            };
            let [row] = values.rows.as_slice() else {
                return Err(DataFusionError::Plan(
                    "MERGE INSERT expects exactly one VALUES row".to_string(),
                ));
            };
            not_matched.push(InsertClause {
                predicate_sql,
                action: InsertAction::Explicit {
                    columns: insert.columns.iter().map(object_name_column).collect(),
                    values_sql: row.iter().map(ToString::to_string).collect(),
                },
            });
        }
        (MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget, _) => {
            return Err(DataFusionError::Plan(
                "only INSERT is valid in a WHEN NOT MATCHED clause".to_string(),
            ));
        }
        (MergeClauseKind::NotMatchedBySource, _) => {
            return Err(DataFusionError::NotImplemented(
                "WHEN NOT MATCHED BY SOURCE is not supported yet (tracked in task/todo.md)"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

/// Detect the sentinel shape [`rewrite_merge_stars`] substitutes for `UPDATE SET *`: exactly
/// one `<sentinel> = <sentinel>` assignment maps to [`MatchedAction::UpdateAll`]. A sentinel
/// target alongside other assignments is Spark's forbidden `SET *, col = expr` mix — rejected
/// with a targeted error instead of the unknown-column error it would hit downstream.
fn star_update(assignments: &[Assignment]) -> Result<Option<MatchedAction>> {
    let targets_sentinel = |assignment: &Assignment| {
        matches!(&assignment.target, AssignmentTarget::ColumnName(name)
            if name_parts(name).last().is_some_and(|part| part == STAR_SENTINEL))
    };
    if !assignments.iter().any(targets_sentinel) {
        return Ok(None);
    }
    if let [only] = assignments
        && targets_sentinel(only)
        && expr_is_sentinel(&only.value)
    {
        return Ok(Some(MatchedAction::UpdateAll));
    }
    Err(DataFusionError::Plan(
        "MERGE `UPDATE SET *` cannot be combined with other assignments".to_string(),
    ))
}

/// Detect the sentinel shape [`rewrite_merge_stars`] substitutes for `INSERT *`: the exact
/// `(<sentinel>) VALUES (<sentinel>)` single-column form. Anything else — including a sentinel
/// mixed into a real column list, which only hand-written SQL can produce — falls through to
/// the explicit-clause path and its unknown-column validation.
///
/// sqlparser 0.62: `ObjectName` is a newtype over `Vec<ObjectNamePart>` (no `.value`).
fn object_name_column(name: &ObjectName) -> String {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map_or_else(|| name.to_string(), |ident| ident.value.clone())
}

fn star_insert(insert: &MergeInsertExpr) -> bool {
    let [column] = insert.columns.as_slice() else {
        return false;
    };
    if object_name_column(column) != STAR_SENTINEL {
        return false;
    }
    let MergeInsertKind::Values(values) = &insert.kind else {
        return false;
    };
    matches!(values.rows.as_slice(), [row] if matches!(row.as_slice(), [expr] if expr_is_sentinel(expr)))
}

/// True when the expression is exactly the bare sentinel identifier.
fn expr_is_sentinel(expr: &Expr) -> bool {
    matches!(expr, Expr::Identifier(ident) if ident.value == STAR_SENTINEL)
}

/// `SET col = expr` pairs; a qualified target (`t.col`) keeps only the column name.
fn lower_assignments(assignments: &[Assignment]) -> Result<Vec<(String, String)>> {
    assignments
        .iter()
        .map(|assignment| {
            let AssignmentTarget::ColumnName(name) = &assignment.target else {
                return Err(DataFusionError::NotImplemented(
                    "MERGE UPDATE SET (a, b) = … tuple assignment is not supported".to_string(),
                ));
            };
            let column = name_parts(name).pop().ok_or_else(|| {
                DataFusionError::Plan(format!("cannot resolve SET target `{name}`"))
            })?;
            Ok((column, assignment.value.to_string()))
        })
        .collect()
}

/// The MERGE target: a plain table factor with an optional alias. The alias is rendered with
/// [`ToString`] — NOT `Ident.value` — so a quoted alias (`AS "MyAlias"`) keeps its quoting and
/// matches the quoted references the user's ON/SET expressions re-render with; an unquoted
/// alias round-trips unchanged.
fn target_table(
    factor: &TableFactor,
) -> Result<(datafusion::sql::sqlparser::ast::ObjectName, Option<String>)> {
    let TableFactor::Table { name, alias, .. } = factor else {
        return Err(DataFusionError::Plan(
            "MERGE INTO target must be a table, not a subquery".to_string(),
        ));
    };
    Ok((name.clone(), alias.as_ref().map(|a| a.name.to_string())))
}

/// The default alias for an unaliased table: its last name part rendered WITH any quoting
/// (Spark scoping — an unaliased table is referenced by its bare name).
fn bare_name_alias(name: &datafusion::sql::sqlparser::ast::ObjectName) -> Result<String> {
    name.0
        .last()
        .map(ToString::to_string)
        .ok_or_else(|| DataFusionError::Plan(format!("cannot resolve MERGE name `{name}`")))
}

/// The `USING` source: a table (aliased or referenced by its bare name, Spark-style) or an
/// aliased subquery. Returns the `FROM`-ready SQL plus the alias user expressions resolve with
/// (quote style preserved — see [`target_table`]).
fn source_table(factor: &TableFactor) -> Result<(String, String)> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let alias = alias.as_ref().map(|a| a.name.to_string());
            let bare = bare_name_alias(name)?;
            Ok((name.to_string(), alias.unwrap_or(bare)))
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let alias = alias.as_ref().map(|a| a.name.to_string()).ok_or_else(|| {
                DataFusionError::Plan(
                    "a MERGE subquery source requires an alias (USING (SELECT …) AS s)".to_string(),
                )
            })?;
            Ok((format!("({subquery})"), alias))
        }
        other => Err(DataFusionError::Plan(format!(
            "unsupported MERGE source: `{other}`"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::sql::sqlparser::ast::Statement;
    use datafusion::sql::sqlparser::dialect::DatabricksDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn parse_merge(sql: &str) -> (TableFactor, TableFactor, Expr, Vec<MergeClause>) {
        use datafusion::sql::sqlparser::tokenizer::Tokenizer;
        let dialect = DatabricksDialect {};
        let tokens = rewrite_merge_stars(&Tokenizer::new(&dialect, sql).tokenize().unwrap());
        let mut statements = Parser::new(&dialect)
            .with_tokens(tokens)
            .parse_statements()
            .expect("merge should parse");
        let Statement::Merge(merge) = statements.remove(0) else {
            panic!("expected a MERGE statement");
        };
        (merge.table, merge.source, *merge.on, merge.clauses)
    }

    /// The source publish job's shape: aliased target + source, one UPDATE, one INSERT — lowers
    /// with aliases, ON text, ordered clauses, and rendered expressions intact.
    #[test]
    fn lowers_classic_upsert() {
        let (table, source, on, clauses) = parse_merge(
            "MERGE INTO ice.sales.t AS t USING updates AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
        );
        let (catalog, spec) = lower(&table, &source, &on, &clauses).unwrap();
        assert_eq!(catalog, "ice");
        assert_eq!(spec.target.name(), "t");
        assert_eq!(spec.target_alias, "t");
        assert_eq!(spec.source_from_sql, "updates");
        assert_eq!(spec.source_alias, "s");
        assert_eq!(spec.on_sql, "t.id = s.id");
        assert_eq!(spec.matched.len(), 1);
        let MatchedAction::Update { assignments } = &spec.matched[0].action else {
            panic!("expected an UPDATE clause");
        };
        assert_eq!(assignments[0], ("name".to_string(), "s.name".to_string()));
        assert_eq!(spec.not_matched.len(), 1);
        let InsertAction::Explicit {
            columns,
            values_sql,
        } = &spec.not_matched[0].action
        else {
            panic!("expected an explicit INSERT clause");
        };
        assert_eq!(columns, &["id", "name"]);
        assert_eq!(values_sql, &["s.id", "s.name"]);
    }

    /// Unaliased target and source fall back to their bare names (Spark scoping), and a
    /// qualified `SET t.name = …` keeps only the column.
    #[test]
    fn defaults_aliases_to_bare_names() {
        let (table, source, on, clauses) = parse_merge(
            "MERGE INTO ice.sales.t USING updates ON t.id = updates.id \
             WHEN MATCHED AND updates.op = 'd' THEN DELETE \
             WHEN MATCHED THEN UPDATE SET t.name = updates.name",
        );
        let (_, spec) = lower(&table, &source, &on, &clauses).unwrap();
        assert_eq!(spec.target_alias, "t");
        assert_eq!(spec.source_alias, "updates");
        assert_eq!(
            spec.matched[0].predicate_sql.as_deref(),
            Some("updates.op = 'd'")
        );
        assert!(matches!(spec.matched[0].action, MatchedAction::Delete));
        let MatchedAction::Update { assignments } = &spec.matched[1].action else {
            panic!("expected an UPDATE clause");
        };
        assert_eq!(assignments[0].0, "name");
    }

    /// Quoted aliases keep their quote style through lowering, so the generated FROM alias
    /// declaration matches the user's quoted (case-sensitive) references in ON/SET.
    #[test]
    fn quoted_aliases_preserve_quoting() {
        let (table, source, on, clauses) = parse_merge(
            "MERGE INTO ice.sales.t AS \"Tgt\" USING u AS \"Src\" ON \"Tgt\".id = \"Src\".id \
             WHEN MATCHED THEN DELETE",
        );
        let (_, spec) = lower(&table, &source, &on, &clauses).unwrap();
        assert_eq!(spec.target_alias, "\"Tgt\"");
        assert_eq!(spec.source_alias, "\"Src\"");
        assert_eq!(spec.on_sql, "\"Tgt\".id = \"Src\".id");
    }

    /// A subquery source without an alias is rejected with a targeted message.
    #[test]
    fn subquery_source_requires_alias() {
        let (table, source, on, clauses) = parse_merge(
            "MERGE INTO ice.sales.t USING (SELECT * FROM u) ON t.id = u.id \
             WHEN MATCHED THEN DELETE",
        );
        let err = lower(&table, &source, &on, &clauses).unwrap_err();
        assert!(err.to_string().contains("requires an alias"));
    }

    /// `WHEN NOT MATCHED BY SOURCE` is a documented v1 limit — deterministic `NotImplemented`.
    #[test]
    fn not_matched_by_source_rejected() {
        let (table, source, on, clauses) = parse_merge(
            "MERGE INTO ice.sales.t AS t USING u AS s ON t.id = s.id \
             WHEN NOT MATCHED BY SOURCE THEN DELETE",
        );
        let err = lower(&table, &source, &on, &clauses).unwrap_err();
        assert!(err.to_string().contains("NOT MATCHED BY SOURCE"));
    }

    /// The star forms (`UPDATE SET *` / `INSERT *` — the source publish job's upsert shape) are
    /// token-rewritten into a parseable sentinel and lowered to the typed star markers.
    #[test]
    fn star_forms_lower_to_markers() {
        let (table, source, on, clauses) = parse_merge(
            "MERGE INTO ice.sales.t AS Target USING v AS Source ON Target.id = Source.id \
             WHEN MATCHED THEN UPDATE SET * \
             WHEN NOT MATCHED THEN INSERT *",
        );
        let (_, spec) = lower(&table, &source, &on, &clauses).unwrap();
        assert!(matches!(spec.matched[0].action, MatchedAction::UpdateAll));
        assert!(matches!(spec.not_matched[0].action, InsertAction::All));
    }

    /// A `*` used as multiplication is never rewritten: assignments and VALUES expressions keep
    /// their arithmetic, including inside a subquery source.
    #[test]
    fn multiplication_star_untouched() {
        let (table, source, on, clauses) = parse_merge(
            "MERGE INTO ice.sales.t AS t USING (SELECT id, qty * 2 AS qty FROM u) AS s \
             ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.qty * 2 \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.qty * 3)",
        );
        let (_, spec) = lower(&table, &source, &on, &clauses).unwrap();
        assert_eq!(spec.source_from_sql, "(SELECT id, qty * 2 AS qty FROM u)");
        let MatchedAction::Update { assignments } = &spec.matched[0].action else {
            panic!("expected an UPDATE clause");
        };
        assert_eq!(
            assignments[0],
            ("name".to_string(), "s.qty * 2".to_string())
        );
        let InsertAction::Explicit { values_sql, .. } = &spec.not_matched[0].action else {
            panic!("expected an explicit INSERT clause");
        };
        assert_eq!(values_sql[1], "s.qty * 3");
    }

    /// A bare column that merely SPELLS like a keyword after a CASE-expression `THEN` must not
    /// trigger the star rewrite (the tokenizer tags `insert` with `Keyword::INSERT` even as an
    /// identifier — the look-ahead guard is what disambiguates). Review-caught repro.
    #[test]
    fn keyword_named_column_after_case_then_untouched() {
        let (table, source, on, clauses) = parse_merge(
            "MERGE INTO ice.sales.t AS t USING s AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET flag = CASE WHEN s.cond THEN insert * 2 ELSE 0 END",
        );
        let (_, spec) = lower(&table, &source, &on, &clauses).unwrap();
        let MatchedAction::Update { assignments } = &spec.matched[0].action else {
            panic!("expected an UPDATE clause");
        };
        assert_eq!(assignments[0].0, "flag");
        assert!(assignments[0].1.contains("insert * 2"));
    }

    /// Spark forbids mixing `SET *` with explicit assignments — targeted error, not the
    /// downstream unknown-column error the sentinel would otherwise hit.
    #[test]
    fn star_mixed_with_assignments_rejected() {
        let (table, source, on, clauses) = parse_merge(
            "MERGE INTO ice.sales.t AS t USING u AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET *, name = s.name",
        );
        let err = lower(&table, &source, &on, &clauses).unwrap_err();
        assert!(
            err.to_string()
                .contains("cannot be combined with other assignments")
        );
    }

    /// A two-part target name is rejected (default-catalog resolution is a follow-up).
    #[test]
    fn two_part_target_rejected() {
        let (table, source, on, clauses) = parse_merge(
            "MERGE INTO sales.t AS t USING u AS s ON t.id = s.id WHEN MATCHED THEN DELETE",
        );
        let err = lower(&table, &source, &on, &clauses).unwrap_err();
        assert!(err.to_string().contains("three-part"));
    }
}
