//! `MERGE INTO` lowering from sqlparser AST to [`repark_iceberg::write::merge::MergeSpec`].

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    Assignment, AssignmentTarget, Expr, MergeAction, MergeClause, MergeClauseKind, MergeInsertExpr,
    MergeInsertKind, MergeUpdateExpr, ObjectName, TableFactor,
};
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::Token;
use iceberg::{NamespaceIdent, TableIdent};
use repark_iceberg::write::merge::{
    InsertAction, InsertClause, MatchedAction, MatchedClause, MergeSpec, NotMatchedBySourceAction,
    NotMatchedBySourceClause,
};

use repark_core::CatalogRegistry;

use crate::{catalog_handle, name_parts};

/// The identifier [`rewrite_merge_stars`] substitutes for the `*` in `UPDATE SET *` / `INSERT *`.
const STAR_SENTINEL: &str = "__repark_merge_star_sentinel__";

/// Oracle-style action sub-predicates are not Spark MERGE grammar.
const ORACLE_STYLE_SUB_PREDICATE_REFUSAL: &str = "Oracle-style `UPDATE SET … WHERE` / `DELETE WHERE` / `INSERT … WHERE` is not Spark MERGE \
     grammar; move the predicate into `WHEN MATCHED AND <cond>` / `WHEN NOT MATCHED AND <cond>`";

/// Verbatim ANSI-door needle: Spark `INSERT VALUES` without a column list is refused the same way.
const MERGE_INSERT_COLUMN_LIST_REQUIRED: &str =
    "MERGE INSERT requires an explicit column list: INSERT (a, b) VALUES (…)";

/// Spark analysis error-class: an unconditioned MATCHED clause that is not last of its kind.
const NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION: &str = "NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION: When there are more than one MATCHED clauses in a \
     MERGE statement, only the last MATCHED clause can omit the condition";

/// Spark analysis error-class: an unconditioned NOT MATCHED clause that is not last of its kind.
const NON_LAST_NOT_MATCHED_CLAUSE_OMIT_CONDITION: &str = "NON_LAST_NOT_MATCHED_CLAUSE_OMIT_CONDITION: When there are more than one NOT MATCHED \
     clauses in a MERGE statement, only the last NOT MATCHED clause can omit the condition";

/// Spark analysis error-class: an unconditioned NOT MATCHED BY SOURCE clause that is not last of its kind.
const NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION: &str = "NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION: When there are more than one NOT MATCHED \
     BY SOURCE clauses in a MERGE statement, only the last NOT MATCHED BY SOURCE clause can omit the condition";

/// Route MERGE INTO: lower the AST to `MergeSpec`, resolve the Iceberg handle, and execute COW.
/// # Errors
/// Returns planning errors for malformed MERGE statements and `NotImplemented` for residual forms.
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

/// Rewrite `UPDATE SET *` and `INSERT *` into sentinel forms stock sqlparser can parse.
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

/// True when whatever follows position `after` can legally FOLLOW a real star form.
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

/// The last (up to) `n` keyword tokens strictly before `index`, in statement order.
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
    let mut not_matched_by_source = Vec::new();
    refuse_clause_kind_order(clauses)?;
    for clause in clauses {
        lower_clause(
            clause,
            &target_alias,
            &mut matched,
            &mut not_matched,
            &mut not_matched_by_source,
        )?;
    }
    if matched.is_empty() && not_matched.is_empty() && not_matched_by_source.is_empty() {
        return Err(DataFusionError::Plan(
            "MERGE INTO requires at least one WHEN clause".to_string(),
        ));
    }
    refuse_non_last_unconditional_clause(&matched, &not_matched, &not_matched_by_source)?;

    let spec = MergeSpec {
        target: TableIdent::new(NamespaceIdent::new(namespace.clone()), table_name.clone()),
        target_alias,
        source_from_sql,
        source_alias,
        on_sql: on.to_string(),
        matched,
        not_matched,
        not_matched_by_source,
    };
    Ok((catalog.clone(), spec))
}

/// Sort one WHEN clause into the matched / not-matched lists, validating the kind–action pairing.
fn lower_clause(
    clause: &MergeClause,
    target_alias: &str,
    matched: &mut Vec<MatchedClause>,
    not_matched: &mut Vec<InsertClause>,
    not_matched_by_source: &mut Vec<NotMatchedBySourceClause>,
) -> Result<()> {
    let predicate_sql = clause.predicate.as_ref().map(ToString::to_string);
    match (&clause.clause_kind, &clause.action) {
        (MergeClauseKind::Matched, MergeAction::Update(update_expr)) => {
            let MergeUpdateExpr {
                update_token: _,
                assignments,
                update_predicate,
                delete_predicate,
            } = update_expr;
            refuse_oracle_style_sub_predicate(update_predicate.as_ref())?;
            refuse_oracle_style_sub_predicate(delete_predicate.as_ref())?;
            let action = match star_update(assignments)? {
                Some(all) => all,
                None => MatchedAction::Update {
                    assignments: lower_assignments(assignments, target_alias)?,
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
            let MergeInsertExpr {
                insert_token: _,
                columns,
                kind_token: _,
                kind,
                insert_predicate,
            } = insert;
            refuse_oracle_style_sub_predicate(insert_predicate.as_ref())?;
            if star_insert(insert) {
                not_matched.push(InsertClause {
                    predicate_sql,
                    action: InsertAction::All,
                });
                return Ok(());
            }
            let MergeInsertKind::Values(values) = kind else {
                return Err(DataFusionError::NotImplemented(
                    "MERGE `INSERT ROW` is not supported; use INSERT (cols) VALUES (…)".to_string(),
                ));
            };
            let [row] = values.rows.as_slice() else {
                return Err(DataFusionError::Plan(
                    "MERGE INSERT expects exactly one VALUES row".to_string(),
                ));
            };
            if columns.is_empty() {
                return Err(DataFusionError::Plan(
                    MERGE_INSERT_COLUMN_LIST_REQUIRED.to_string(),
                ));
            }
            let columns = columns
                .iter()
                .map(|name| resolve_merge_column(name, target_alias, "INSERT column"))
                .collect::<Result<Vec<_>>>()?;
            not_matched.push(InsertClause {
                predicate_sql,
                action: InsertAction::Explicit {
                    columns,
                    values_sql: row.iter().map(ToString::to_string).collect(),
                },
            });
        }
        (MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget, _) => {
            return Err(DataFusionError::Plan(
                "only INSERT is valid in a WHEN NOT MATCHED clause".to_string(),
            ));
        }
        (MergeClauseKind::NotMatchedBySource, action) => {
            not_matched_by_source.push(lower_nmbs_action(action, predicate_sql, target_alias)?);
        }
    }
    Ok(())
}

fn lower_nmbs_action(
    action: &MergeAction,
    predicate_sql: Option<String>,
    target_alias: &str,
) -> Result<NotMatchedBySourceClause> {
    match action {
        MergeAction::Delete { .. } => Ok(NotMatchedBySourceClause {
            predicate_sql,
            action: NotMatchedBySourceAction::Delete,
        }),
        MergeAction::Update(update_expr) => {
            let MergeUpdateExpr {
                update_token: _,
                assignments,
                update_predicate,
                delete_predicate,
            } = update_expr;
            refuse_oracle_style_sub_predicate(update_predicate.as_ref())?;
            refuse_oracle_style_sub_predicate(delete_predicate.as_ref())?;
            if star_update(assignments)?.is_some() {
                return Err(DataFusionError::Plan(
                    "UPDATE SET * is not Spark MERGE grammar on WHEN NOT MATCHED BY SOURCE"
                        .to_string(),
                ));
            }
            Ok(NotMatchedBySourceClause {
                predicate_sql,
                action: NotMatchedBySourceAction::Update {
                    assignments: lower_assignments(assignments, target_alias)?,
                },
            })
        }
        MergeAction::Insert(_) => Err(DataFusionError::Plan(
            "only UPDATE or DELETE is valid in a WHEN NOT MATCHED BY SOURCE clause".to_string(),
        )),
    }
}

/// Detect the sentinel shape [`rewrite_merge_stars`] substitutes for `UPDATE SET *`.
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

/// Detect the sentinel shape [`rewrite_merge_stars`] substitutes for `INSERT *`.
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

/// `SET col = expr` pairs.
fn lower_assignments(
    assignments: &[Assignment],
    target_alias: &str,
) -> Result<Vec<(String, String)>> {
    assignments
        .iter()
        .map(|assignment| {
            let AssignmentTarget::ColumnName(name) = &assignment.target else {
                return Err(DataFusionError::NotImplemented(
                    "MERGE UPDATE SET (a, b) = … tuple assignment is not supported".to_string(),
                ));
            };
            let column = resolve_merge_column(name, target_alias, "SET target")?;
            Ok((column, assignment.value.to_string()))
        })
        .collect()
}

/// Loud Plan error when an Oracle-style action sub-predicate is present.
fn refuse_oracle_style_sub_predicate(predicate: Option<&Expr>) -> Result<()> {
    if predicate.is_some() {
        return Err(DataFusionError::Plan(
            ORACLE_STYLE_SUB_PREDICATE_REFUSAL.to_string(),
        ));
    }
    Ok(())
}

/// After clause collection: an unconditioned clause may only be last of its kind.
fn refuse_non_last_unconditional_clause(
    matched: &[MatchedClause],
    not_matched: &[InsertClause],
    not_matched_by_source: &[NotMatchedBySourceClause],
) -> Result<()> {
    if matched
        .iter()
        .enumerate()
        .any(|(index, clause)| clause.predicate_sql.is_none() && index + 1 < matched.len())
    {
        return Err(DataFusionError::Plan(
            NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION.to_string(),
        ));
    }
    if not_matched
        .iter()
        .enumerate()
        .any(|(index, clause)| clause.predicate_sql.is_none() && index + 1 < not_matched.len())
    {
        return Err(DataFusionError::Plan(
            NON_LAST_NOT_MATCHED_CLAUSE_OMIT_CONDITION.to_string(),
        ));
    }
    if not_matched_by_source
        .iter()
        .enumerate()
        .any(|(index, clause)| {
            clause.predicate_sql.is_none() && index + 1 < not_matched_by_source.len()
        })
    {
        return Err(DataFusionError::Plan(
            NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION.to_string(),
        ));
    }
    Ok(())
}

/// Spark parses MATCHED*, then NOT MATCHED*, then NOT MATCHED BY SOURCE*.
fn refuse_clause_kind_order(clauses: &[MergeClause]) -> Result<()> {
    let mut seen_not_matched = false;
    let mut seen_by_source = false;
    for clause in clauses {
        match clause.clause_kind {
            MergeClauseKind::Matched if seen_not_matched || seen_by_source => {
                return Err(DataFusionError::Plan(
                    "WHEN MATCHED clauses must precede WHEN NOT MATCHED and WHEN NOT MATCHED BY \
                     SOURCE"
                        .to_string(),
                ));
            }
            MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget if seen_by_source => {
                return Err(DataFusionError::Plan(
                    "WHEN NOT MATCHED clauses must precede WHEN NOT MATCHED BY SOURCE".to_string(),
                ));
            }
            MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget => {
                seen_not_matched = true;
            }
            MergeClauseKind::NotMatchedBySource => seen_by_source = true,
            MergeClauseKind::Matched => {}
        }
    }
    Ok(())
}

/// Bare column, or `<target-alias>.column`.
fn resolve_merge_column(name: &ObjectName, target_alias: &str, construct: &str) -> Result<String> {
    let parts = name_parts(name);
    match parts.as_slice() {
        [] => Err(DataFusionError::Plan(format!(
            "cannot resolve {construct} `{name}`"
        ))),
        [column] => Ok(column.clone()),
        [qualifier, column] => {
            if qualifier.eq_ignore_ascii_case(unquoted_ident(target_alias)) {
                Ok(column.clone())
            } else {
                Err(DataFusionError::Plan(format!(
                    "{construct} qualifier `{qualifier}` is not the MERGE target alias `{target_alias}`"
                )))
            }
        }
        _ => Err(DataFusionError::Plan(
            "nested-field assignment is not supported".to_string(),
        )),
    }
}

/// Strip a matching pair of double quotes or backticks from a rendered identifier.
fn unquoted_ident(rendered: &str) -> &str {
    let trimmed = rendered.trim();
    let bytes = trimmed.as_bytes();
    if bytes.len() >= 2 {
        let quote = bytes[0];
        if matches!(quote, b'"' | b'`') && bytes[bytes.len() - 1] == quote {
            return &trimmed[1..trimmed.len() - 1];
        }
    }
    trimmed
}

/// The MERGE target: a plain table factor with an optional alias.
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

/// The default alias for an unaliased table: its last name part rendered WITH any quoting.
fn bare_name_alias(name: &datafusion::sql::sqlparser::ast::ObjectName) -> Result<String> {
    name.0
        .last()
        .map(ToString::to_string)
        .ok_or_else(|| DataFusionError::Plan(format!("cannot resolve MERGE name `{name}`")))
}

/// The `USING` source: a table or an aliased subquery.
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

    /// The source publish job's shape: aliased target + source, one UPDATE, one INSERT.
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

    /// Unaliased target and source fall back to their bare names.
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

    /// Quoted aliases keep their quote style through lowering.
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

    /// `WHEN NOT MATCHED BY SOURCE THEN DELETE` lowers to the third arm.
    #[test]
    fn not_matched_by_source_delete_lowers() {
        let (table, source, on, clauses) = parse_merge(
            "MERGE INTO ice.sales.t AS t USING u AS s ON t.id = s.id \
             WHEN NOT MATCHED BY SOURCE THEN DELETE",
        );
        let (_, spec) = lower(&table, &source, &on, &clauses).unwrap();
        assert_eq!(spec.not_matched_by_source.len(), 1);
        assert!(matches!(
            spec.not_matched_by_source[0].action,
            NotMatchedBySourceAction::Delete
        ));
    }

    /// Star forms are token-rewritten into a parseable sentinel and lowered to typed star markers.
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

    /// A `*` used as multiplication is never rewritten.
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

    /// A CASE THEN identifier that merely spells like a keyword must not trigger star rewrite.
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

    /// Spark forbids mixing `SET *` with explicit assignments.
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

    /// A two-part target name is rejected; this door requires a three-part target.
    #[test]
    fn two_part_target_rejected() {
        let (table, source, on, clauses) = parse_merge(
            "MERGE INTO sales.t AS t USING u AS s ON t.id = s.id WHEN MATCHED THEN DELETE",
        );
        let err = lower(&table, &source, &on, &clauses).unwrap_err();
        assert!(err.to_string().contains("three-part"));
    }

    fn lower_error(sql: &str) -> String {
        let (table, source, on, clauses) = parse_merge(sql);
        lower(&table, &source, &on, &clauses)
            .expect_err("lowering must refuse")
            .to_string()
    }

    /// M2 / r5 — Oracle-style `UPDATE SET … WHERE` is destructured and refused, not dropped.
    #[test]
    fn oracle_style_update_where_predicate_refuses() {
        let err = lower_error(
            "MERGE INTO ice.sales.t AS t USING u AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name WHERE s.name IS NOT NULL",
        );
        assert!(
            err.contains("UPDATE SET … WHERE"),
            "must name the UPDATE WHERE construct: {err}"
        );
        assert!(
            err.contains("is not Spark MERGE grammar"),
            "must name the Spark form: {err}"
        );
        assert!(
            err.contains("WHEN MATCHED AND <cond>"),
            "must name the Spark rewrite: {err}"
        );
    }

    /// M2 — Oracle-style `UPDATE SET … DELETE WHERE` is destructured and refused.
    #[test]
    fn oracle_style_delete_where_predicate_refuses() {
        let err = lower_error(
            "MERGE INTO ice.sales.t AS t USING u AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name DELETE WHERE s.name IS NULL",
        );
        assert!(
            err.contains("DELETE WHERE"),
            "must name the DELETE WHERE construct: {err}"
        );
        assert!(
            err.contains("is not Spark MERGE grammar"),
            "must name the Spark form: {err}"
        );
    }

    /// M2 — Oracle-style `INSERT … WHERE` is destructured and refused.
    #[test]
    fn oracle_style_insert_where_predicate_refuses() {
        let err = lower_error(
            "MERGE INTO ice.sales.t AS t USING u AS s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name) WHERE s.id > 0",
        );
        assert!(
            err.contains("INSERT … WHERE"),
            "must name the INSERT WHERE construct: {err}"
        );
        assert!(
            err.contains("is not Spark MERGE grammar"),
            "must name the Spark form: {err}"
        );
    }

    /// M3 / r7 — a source-qualified SET target is refused, naming qualifier and target alias.
    #[test]
    fn source_qualified_set_target_refuses() {
        let err = lower_error(
            "MERGE INTO ice.sales.t AS t USING u AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET s.name = 'hacked'",
        );
        assert!(
            err.contains("`s`"),
            "must name the received qualifier: {err}"
        );
        assert!(
            err.contains("target alias `t`"),
            "must name the target alias: {err}"
        );
    }

    /// M3 — three-or-more-part SET targets refuse as nested-field assignment.
    #[test]
    fn nested_field_set_target_refuses() {
        let err = lower_error(
            "MERGE INTO ice.sales.t AS t USING u AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.addr.city = s.name",
        );
        assert!(
            err.contains("nested-field assignment is not supported"),
            "{err}"
        );
    }

    /// M3 positive — `t.name` and bare `name` both lower to the target column.
    #[test]
    fn target_qualified_and_bare_set_targets_lower() {
        let (table, source, on, clauses) = parse_merge(
            "MERGE INTO ice.sales.t AS t USING u AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.name = s.name",
        );
        let (_, spec) = lower(&table, &source, &on, &clauses).unwrap();
        let MatchedAction::Update { assignments } = &spec.matched[0].action else {
            panic!("expected an UPDATE clause");
        };
        assert_eq!(assignments[0], ("name".to_string(), "s.name".to_string()));

        let (table, source, on, clauses) = parse_merge(
            "MERGE INTO ice.sales.t AS t USING u AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        );
        let (_, spec) = lower(&table, &source, &on, &clauses).unwrap();
        let MatchedAction::Update { assignments } = &spec.matched[0].action else {
            panic!("expected an UPDATE clause");
        };
        assert_eq!(assignments[0], ("name".to_string(), "s.name".to_string()));

        let (table, source, on, clauses) = parse_merge(
            "MERGE INTO ice.sales.t AS t USING u AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET T.name = s.name",
        );
        let (_, spec) = lower(&table, &source, &on, &clauses).unwrap();
        let MatchedAction::Update { assignments } = &spec.matched[0].action else {
            panic!("expected an UPDATE clause");
        };
        assert_eq!(assignments[0].0, "name");
    }

    /// M3 — quoted target alias + `"Tgt".col` still resolves (unquote before compare).
    #[test]
    fn quoted_target_alias_set_target_lowers() {
        let (table, source, on, clauses) = parse_merge(
            "MERGE INTO ice.sales.t AS \"Tgt\" USING u AS s ON \"Tgt\".id = s.id \
             WHEN MATCHED THEN UPDATE SET \"Tgt\".name = s.name",
        );
        let (_, spec) = lower(&table, &source, &on, &clauses).unwrap();
        assert_eq!(spec.target_alias, "\"Tgt\"");
        let MatchedAction::Update { assignments } = &spec.matched[0].action else {
            panic!("expected an UPDATE clause");
        };
        assert_eq!(assignments[0], ("name".to_string(), "s.name".to_string()));
    }

    /// M3 — source-qualified INSERT columns refuse, naming qualifier and target alias.
    #[test]
    fn source_qualified_insert_column_refuses() {
        let err = lower_error(
            "MERGE INTO ice.sales.t AS t USING u AS s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (s.id, s.name) VALUES (s.id, s.name)",
        );
        assert!(
            err.contains("`s`"),
            "must name the received qualifier: {err}"
        );
        assert!(
            err.contains("target alias `t`"),
            "must name the target alias: {err}"
        );
    }

    /// M3 — three-or-more-part INSERT columns refuse as nested-field assignment.
    #[test]
    fn nested_field_insert_column_refuses() {
        let err = lower_error(
            "MERGE INTO ice.sales.t AS t USING u AS s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (t.addr.city) VALUES (s.name)",
        );
        assert!(
            err.contains("nested-field assignment is not supported"),
            "{err}"
        );
    }

    /// M8 / r6 — column-list-less `INSERT VALUES` is refused with the ANSI needle.
    #[test]
    fn insert_without_column_list_refuses() {
        let err = lower_error(
            "MERGE INTO ice.sales.t AS t USING u AS s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name)",
        );
        assert!(
            err.contains(MERGE_INSERT_COLUMN_LIST_REQUIRED),
            "must copy the ANSI needle verbatim: {err}"
        );
    }

    /// M8 positive — `INSERT *` still lowers to the star marker.
    #[test]
    fn insert_star_still_lowers() {
        let (table, source, on, clauses) = parse_merge(
            "MERGE INTO ice.sales.t AS t USING u AS s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT *",
        );
        let (_, spec) = lower(&table, &source, &on, &clauses).unwrap();
        assert!(matches!(spec.not_matched[0].action, InsertAction::All));
    }

    /// M10 / r12 — an unconditioned MATCHED clause before another MATCHED clause refuses.
    #[test]
    fn non_last_unconditional_matched_clause_refuses() {
        let err = lower_error(
            "MERGE INTO ice.sales.t AS t USING u AS s ON t.id = s.id \
             WHEN MATCHED THEN DELETE \
             WHEN MATCHED AND s.name = 'b' THEN UPDATE SET name = s.name",
        );
        assert!(
            err.contains("NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION"),
            "{err}"
        );
    }

    /// M10 — an unconditioned NOT MATCHED clause before another NOT MATCHED clause refuses.
    #[test]
    fn non_last_unconditional_not_matched_clause_refuses() {
        let err = lower_error(
            "MERGE INTO ice.sales.t AS t USING u AS s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name) \
             WHEN NOT MATCHED AND s.name = 'x' THEN INSERT (id, name) VALUES (s.id, s.name)",
        );
        assert!(
            err.contains("NON_LAST_NOT_MATCHED_CLAUSE_OMIT_CONDITION"),
            "{err}"
        );
    }

    /// M10 positive — an unconditioned LAST MATCHED clause still lowers.
    #[test]
    fn unconditional_last_matched_clause_still_lowers() {
        let (table, source, on, clauses) = parse_merge(
            "MERGE INTO ice.sales.t AS t USING u AS s ON t.id = s.id \
             WHEN MATCHED AND s.name = 'd' THEN DELETE \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        );
        let (_, spec) = lower(&table, &source, &on, &clauses).unwrap();
        assert_eq!(spec.matched.len(), 2);
        assert!(spec.matched[0].predicate_sql.is_some());
        assert!(spec.matched[1].predicate_sql.is_none());
    }
}
