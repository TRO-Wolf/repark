use datafusion::sql::sqlparser::ast::{
    Cte, Expr, Ident, ObjectName, ObjectNamePart, Parens, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, TableAliasColumnDef, TableFactor, Value,
    ValueWithSpan, Values, WildcardAdditionalOptions,
};
use datafusion::sql::sqlparser::tokenizer::Span;

use super::sibling_scope::{scoped_arm_sql, unmapped_arm};

#[derive(Debug, Clone, Copy)]
pub(crate) struct SourceCell<'a> {
    pub(crate) rows: &'a [Parens<Vec<Expr>>],
    pub(crate) column: usize,
}

#[derive(Debug)]
pub(crate) struct ArmMap<'a> {
    pub(crate) positions: Vec<Vec<SourceCell<'a>>>,
    pub(crate) provenance: Vec<Vec<SiblingProvenance<'a>>>,
    pub(crate) arm_sql: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) enum SiblingProvenance<'a> {
    Table {
        table: &'a ObjectName,
        column: String,
    },
    Expr {
        expr: &'a Expr,
    },
}

struct FactorDef<'a> {
    name: Option<String>,
    names: Vec<String>,
    columns: Vec<Vec<SourceCell<'a>>>,
    provenance: Vec<Vec<SiblingProvenance<'a>>>,
}

struct TableRef<'a> {
    name: &'a ObjectName,
    alias: Option<&'a str>,
}

const QUERY_DEPTH: usize = 16;

#[must_use]
pub(crate) fn resolve_insert_arms(source: &Query, case_insensitive: bool) -> Vec<ArmMap<'_>> {
    arms_in(source, &mut Vec::new(), case_insensitive, 0)
}

#[must_use]
pub(crate) fn arm_row_groups<'a>(arm: &'a ArmMap<'a>) -> Vec<&'a [Parens<Vec<Expr>>]> {
    let mut seen = std::collections::HashSet::new();
    let mut groups = Vec::new();
    for cell in arm.positions.iter().flatten() {
        if seen.insert(cell.rows.as_ptr()) {
            groups.push(cell.rows);
        }
    }
    groups
}

#[must_use]
pub(crate) fn null_cell() -> Expr {
    Expr::Value(ValueWithSpan {
        value: Value::Null,
        span: Span::empty(),
    })
}

fn arms_in<'a>(
    query: &'a Query,
    scope: &mut Vec<&'a Cte>,
    case_insensitive: bool,
    depth: usize,
) -> Vec<ArmMap<'a>> {
    if depth > QUERY_DEPTH {
        return Vec::new();
    }
    let kept = scope.len();
    if let Some(with) = query.with.as_ref() {
        scope.extend(with.cte_tables.iter());
    }
    let arms = arms_in_set(&query.body, scope, case_insensitive, depth);
    scope.truncate(kept);
    arms
}

fn arms_in_set<'a>(
    body: &'a SetExpr,
    scope: &mut Vec<&'a Cte>,
    case_insensitive: bool,
    depth: usize,
) -> Vec<ArmMap<'a>> {
    match body {
        SetExpr::Values(values) => vec![identity_map(values)],
        SetExpr::Select(select) => select_arm(select, scope, case_insensitive, depth)
            .into_iter()
            .collect(),
        SetExpr::SetOperation { left, right, .. } => {
            let mut arms = arms_in_set(left, scope, case_insensitive, depth);
            arms.extend(arms_in_set(right, scope, case_insensitive, depth));
            arms
        }
        SetExpr::Query(inner) => arms_in(inner, scope, case_insensitive, depth),
        _ => Vec::new(),
    }
}

fn identity_map(values: &Values) -> ArmMap<'_> {
    let width = values.rows.first().map_or(0, |row| row.content.len());
    ArmMap {
        positions: (0..width)
            .map(|column| {
                vec![SourceCell {
                    rows: values.rows.as_slice(),
                    column,
                }]
            })
            .collect(),
        provenance: vec![Vec::new(); width],
        arm_sql: None,
    }
}

fn select_arm<'a>(
    select: &'a Select,
    scope: &mut Vec<&'a Cte>,
    case_insensitive: bool,
    depth: usize,
) -> Option<ArmMap<'a>> {
    let factors: Vec<Option<FactorDef<'_>>> = select
        .from
        .iter()
        .flat_map(|from| {
            std::iter::once(&from.relation).chain(from.joins.iter().map(|join| &join.relation))
        })
        .map(|factor| factor_def(factor, scope, case_insensitive, depth))
        .collect();
    let tables: Vec<Option<TableRef<'_>>> = select
        .from
        .iter()
        .flat_map(|from| {
            std::iter::once(&from.relation).chain(from.joins.iter().map(|join| &join.relation))
        })
        .map(|factor| table_ref(factor, scope, case_insensitive))
        .collect();
    let mut positions = Vec::with_capacity(select.projection.len());
    let mut provenance = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                positions.push(resolve_expr(expr, &factors, case_insensitive));
                provenance.push(resolve_provenance(
                    expr,
                    &factors,
                    &tables,
                    case_insensitive,
                ));
            }
            SelectItem::Wildcard(options) => {
                if !plain_options(options) {
                    return None;
                }
                for factor in &factors {
                    let Some(factor) = factor.as_ref() else {
                        return Some(unmapped_arm(select, scope, case_insensitive));
                    };
                    positions.extend(factor.columns.iter().cloned());
                    provenance.extend(factor.provenance.iter().cloned());
                }
            }
            SelectItem::QualifiedWildcard(kind, options) => {
                if !plain_options(options) {
                    return None;
                }
                let SelectItemQualifiedWildcardKind::ObjectName(name) = kind else {
                    return None;
                };
                let Some(factor) = qualified_factor(name, &factors, case_insensitive) else {
                    return Some(unmapped_arm(select, scope, case_insensitive));
                };
                positions.extend(factor.columns.iter().cloned());
                provenance.extend(factor.provenance.iter().cloned());
            }
            SelectItem::ExprWithAliases { .. } => return None,
        }
    }
    Some(ArmMap {
        positions,
        provenance,
        arm_sql: scoped_arm_sql(select, scope, case_insensitive),
    })
}

fn resolve_expr<'a>(
    expr: &Expr,
    factors: &[Option<FactorDef<'a>>],
    case_insensitive: bool,
) -> Vec<SourceCell<'a>> {
    match peel(expr) {
        Expr::Identifier(ident) => {
            let [factor] = factors else {
                return Vec::new();
            };
            let Some(factor) = factor else {
                return Vec::new();
            };
            column_cells(factor, &ident.value, case_insensitive)
        }
        Expr::CompoundIdentifier(parts) => {
            let [qualifier, column] = parts.as_slice() else {
                return Vec::new();
            };
            factors
                .iter()
                .flatten()
                .find(|factor| {
                    factor
                        .name
                        .as_ref()
                        .is_some_and(|name| name_matches(name, &qualifier.value, case_insensitive))
                })
                .map_or(Vec::new(), |factor| {
                    column_cells(factor, &column.value, case_insensitive)
                })
        }
        _ => Vec::new(),
    }
}

fn column_cells<'a>(
    factor: &FactorDef<'a>,
    wanted: &str,
    case_insensitive: bool,
) -> Vec<SourceCell<'a>> {
    let mut found = None;
    for (index, name) in factor.names.iter().enumerate() {
        if name_matches(name, wanted, case_insensitive) {
            if found.is_some() {
                return Vec::new();
            }
            found = Some(index);
        }
    }
    found.map_or(Vec::new(), |index| factor.columns[index].clone())
}

fn resolve_provenance<'a>(
    expr: &'a Expr,
    factors: &[Option<FactorDef<'a>>],
    tables: &[Option<TableRef<'a>>],
    case_insensitive: bool,
) -> Vec<SiblingProvenance<'a>> {
    if factors.is_empty() {
        return vec![SiblingProvenance::Expr { expr }];
    }
    match peel(expr) {
        Expr::Identifier(ident) => {
            let ([factor], [table]) = (factors, tables) else {
                return Vec::new();
            };
            match (factor, table) {
                (Some(factor), None) => column_provenance(factor, &ident.value, case_insensitive),
                (None, Some(table)) => vec![SiblingProvenance::Table {
                    table: table.name,
                    column: ident.value.clone(),
                }],
                _ => Vec::new(),
            }
        }
        Expr::CompoundIdentifier(parts) => {
            let [qualifier, column] = parts.as_slice() else {
                return Vec::new();
            };
            let derived = factors.iter().flatten().find(|factor| {
                factor
                    .name
                    .as_ref()
                    .is_some_and(|name| name_matches(name, &qualifier.value, case_insensitive))
            });
            let table = tables
                .iter()
                .flatten()
                .find(|table| qualifier_matches_table(table, &qualifier.value, case_insensitive));
            match (derived, table) {
                (Some(factor), None) => column_provenance(factor, &column.value, case_insensitive),
                (None, Some(table)) => vec![SiblingProvenance::Table {
                    table: table.name,
                    column: column.value.clone(),
                }],
                _ => Vec::new(),
            }
        }
        _ => Vec::new(),
    }
}

fn column_provenance<'a>(
    factor: &FactorDef<'a>,
    wanted: &str,
    case_insensitive: bool,
) -> Vec<SiblingProvenance<'a>> {
    let mut found = None;
    for (index, name) in factor.names.iter().enumerate() {
        if name_matches(name, wanted, case_insensitive) {
            if found.is_some() {
                return Vec::new();
            }
            found = Some(index);
        }
    }
    found.map_or(Vec::new(), |index| factor.provenance[index].clone())
}

fn table_ref<'a>(
    factor: &'a TableFactor,
    scope: &[&'a Cte],
    case_insensitive: bool,
) -> Option<TableRef<'a>> {
    let TableFactor::Table {
        name,
        alias,
        args: None,
        ..
    } = factor
    else {
        return None;
    };
    if cte_named(name, scope, case_insensitive).is_some() {
        return None;
    }
    Some(TableRef {
        name,
        alias: alias.as_ref().map(|alias| alias.name.value.as_str()),
    })
}

fn qualifier_matches_table(table: &TableRef, qualifier: &str, case_insensitive: bool) -> bool {
    if let Some(alias) = table.alias {
        return name_matches(alias, qualifier, case_insensitive);
    }
    table_short_name(table.name)
        .is_some_and(|short| name_matches(short, qualifier, case_insensitive))
}

fn table_short_name(name: &ObjectName) -> Option<&str> {
    name.0
        .iter()
        .rev()
        .find_map(|part| part.as_ident())
        .map(|ident| ident.value.as_str())
}

fn qualified_factor<'a, 'b>(
    name: &ObjectName,
    factors: &'b [Option<FactorDef<'a>>],
    case_insensitive: bool,
) -> Option<&'b FactorDef<'a>> {
    let wanted = single_ident(name)?;
    factors.iter().flatten().find(|factor| {
        factor
            .name
            .as_ref()
            .is_some_and(|name| name_matches(name, &wanted.value, case_insensitive))
    })
}

fn factor_def<'a>(
    factor: &'a TableFactor,
    scope: &mut Vec<&'a Cte>,
    case_insensitive: bool,
    depth: usize,
) -> Option<FactorDef<'a>> {
    match factor {
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let mut defined = factor_in(subquery, scope, case_insensitive, depth + 1)?;
            if let Some(alias) = alias {
                if !apply_columns(&mut defined, &alias.columns) {
                    return None;
                }
                defined.name = Some(alias.name.value.clone());
            }
            Some(defined)
        }
        TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } => {
            let cte = cte_named(name, scope, case_insensitive)?;
            let mut defined = factor_in(&cte.query, scope, case_insensitive, depth + 1)?;
            let columns = alias
                .as_ref()
                .filter(|alias| !alias.columns.is_empty())
                .map_or(cte.alias.columns.as_slice(), |alias| {
                    alias.columns.as_slice()
                });
            if !apply_columns(&mut defined, columns) {
                return None;
            }
            defined.name = Some(alias.as_ref().map_or_else(
                || cte.alias.name.value.clone(),
                |alias| alias.name.value.clone(),
            ));
            Some(defined)
        }
        _ => None,
    }
}

fn factor_in<'a>(
    query: &'a Query,
    scope: &mut Vec<&'a Cte>,
    case_insensitive: bool,
    depth: usize,
) -> Option<FactorDef<'a>> {
    if depth > QUERY_DEPTH {
        return None;
    }
    let kept = scope.len();
    if let Some(with) = query.with.as_ref() {
        scope.extend(with.cte_tables.iter());
    }
    let defined = factor_in_set(&query.body, scope, case_insensitive, depth);
    scope.truncate(kept);
    defined
}

fn factor_in_set<'a>(
    body: &'a SetExpr,
    scope: &mut Vec<&'a Cte>,
    case_insensitive: bool,
    depth: usize,
) -> Option<FactorDef<'a>> {
    match body {
        SetExpr::Values(values) => Some(values_factor(values)),
        SetExpr::Select(select) => select_factor(select, scope, case_insensitive, depth),
        SetExpr::SetOperation { left, right, .. } => {
            let left = factor_in_set(left, scope, case_insensitive, depth)?;
            let right = factor_in_set(right, scope, case_insensitive, depth)?;
            merge_factors(left, right)
        }
        SetExpr::Query(inner) => factor_in(inner, scope, case_insensitive, depth),
        _ => None,
    }
}

fn values_factor(values: &Values) -> FactorDef<'_> {
    let width = values.rows.first().map_or(0, |row| row.content.len());
    FactorDef {
        name: None,
        names: vec![String::new(); width],
        columns: (0..width)
            .map(|column| {
                vec![SourceCell {
                    rows: values.rows.as_slice(),
                    column,
                }]
            })
            .collect(),
        provenance: vec![Vec::new(); width],
    }
}

fn select_factor<'a>(
    select: &'a Select,
    scope: &mut Vec<&'a Cte>,
    case_insensitive: bool,
    depth: usize,
) -> Option<FactorDef<'a>> {
    let factors: Vec<Option<FactorDef<'_>>> = select
        .from
        .iter()
        .flat_map(|from| {
            std::iter::once(&from.relation).chain(from.joins.iter().map(|join| &join.relation))
        })
        .map(|factor| factor_def(factor, scope, case_insensitive, depth))
        .collect();
    let tables: Vec<Option<TableRef<'_>>> = select
        .from
        .iter()
        .flat_map(|from| {
            std::iter::once(&from.relation).chain(from.joins.iter().map(|join| &join.relation))
        })
        .map(|factor| table_ref(factor, scope, case_insensitive))
        .collect();
    let mut names = Vec::with_capacity(select.projection.len());
    let mut columns = Vec::with_capacity(select.projection.len());
    let mut provenance = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                names.push(output_name(expr, None));
                columns.push(resolve_expr(expr, &factors, case_insensitive));
                provenance.push(resolve_provenance(
                    expr,
                    &factors,
                    &tables,
                    case_insensitive,
                ));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                names.push(output_name(expr, Some(alias)));
                columns.push(resolve_expr(expr, &factors, case_insensitive));
                provenance.push(resolve_provenance(
                    expr,
                    &factors,
                    &tables,
                    case_insensitive,
                ));
            }
            SelectItem::Wildcard(options) => {
                if !plain_options(options) {
                    return None;
                }
                for factor in &factors {
                    let factor = factor.as_ref()?;
                    names.extend(factor.names.iter().cloned());
                    columns.extend(factor.columns.iter().cloned());
                    provenance.extend(factor.provenance.iter().cloned());
                }
            }
            SelectItem::QualifiedWildcard(kind, options) => {
                if !plain_options(options) {
                    return None;
                }
                let SelectItemQualifiedWildcardKind::ObjectName(name) = kind else {
                    return None;
                };
                let factor = qualified_factor(name, &factors, case_insensitive)?;
                names.extend(factor.names.iter().cloned());
                columns.extend(factor.columns.iter().cloned());
                provenance.extend(factor.provenance.iter().cloned());
            }
            SelectItem::ExprWithAliases { .. } => return None,
        }
    }
    Some(FactorDef {
        name: None,
        names,
        columns,
        provenance,
    })
}

fn merge_factors<'a>(left: FactorDef<'a>, right: FactorDef<'a>) -> Option<FactorDef<'a>> {
    if left.names.len() != right.names.len() {
        return None;
    }
    Some(FactorDef {
        name: None,
        names: left.names,
        columns: left
            .columns
            .into_iter()
            .zip(right.columns)
            .map(|(mut left, right)| {
                left.extend(right);
                left
            })
            .collect(),
        provenance: left
            .provenance
            .into_iter()
            .zip(right.provenance)
            .map(|(mut left, right)| {
                left.extend(right);
                left
            })
            .collect(),
    })
}

fn apply_columns(defined: &mut FactorDef, columns: &[TableAliasColumnDef]) -> bool {
    if columns.is_empty() {
        return true;
    }
    if columns.len() != defined.names.len() {
        return false;
    }
    defined.names = columns
        .iter()
        .map(|column| column.name.value.clone())
        .collect();
    true
}

fn output_name(expr: &Expr, alias: Option<&Ident>) -> String {
    if let Some(alias) = alias {
        return alias.value.clone();
    }
    match peel(expr) {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map_or(String::new(), |ident| ident.value.clone()),
        _ => String::new(),
    }
}

fn peel(mut expr: &Expr) -> &Expr {
    while let Expr::Nested(child) = expr {
        expr = child;
    }
    expr
}

fn name_matches(have: &str, wanted: &str, case_insensitive: bool) -> bool {
    if case_insensitive {
        have.eq_ignore_ascii_case(wanted)
    } else {
        have == wanted
    }
}

fn cte_named<'a>(name: &ObjectName, scope: &[&'a Cte], case_insensitive: bool) -> Option<&'a Cte> {
    let ident = single_ident(name)?;
    scope
        .iter()
        .rev()
        .find(|cte| name_matches(&cte.alias.name.value, &ident.value, case_insensitive))
        .copied()
}

fn single_ident(name: &ObjectName) -> Option<&Ident> {
    match name.0.as_slice() {
        [ObjectNamePart::Identifier(ident)] => Some(ident),
        _ => None,
    }
}

fn plain_options(options: &WildcardAdditionalOptions) -> bool {
    options.opt_ilike.is_none()
        && options.opt_exclude.is_none()
        && options.opt_except.is_none()
        && options.opt_replace.is_none()
        && options.opt_rename.is_none()
        && options.opt_alias.is_none()
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::ast::Statement;
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use super::*;

    fn mapped(sql: &str) -> Vec<Vec<Vec<String>>> {
        mapped_case(sql, true)
    }

    fn mapped_case(sql: &str, case_insensitive: bool) -> Vec<Vec<Vec<String>>> {
        let mut statements = Parser::parse_sql(&GenericDialect, sql).unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        resolve_insert_arms(&source, case_insensitive)
            .iter()
            .map(|arm| {
                arm.positions
                    .iter()
                    .map(|position| {
                        position
                            .iter()
                            .map(|cell| {
                                let first = cell
                                    .rows
                                    .first()
                                    .and_then(|row| row.content.get(cell.column))
                                    .map_or(String::new(), ToString::to_string);
                                format!("{}/{}/{first}", cell.rows.len(), cell.column)
                            })
                            .collect()
                    })
                    .collect()
            })
            .collect()
    }

    fn proven(sql: &str) -> Vec<Vec<Vec<String>>> {
        let mut statements = Parser::parse_sql(&GenericDialect, sql).unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        resolve_insert_arms(&source, true)
            .iter()
            .map(|arm| {
                arm.provenance
                    .iter()
                    .map(|position| {
                        position
                            .iter()
                            .map(|entry| match entry {
                                SiblingProvenance::Table { table, column } => {
                                    format!("{table}#{column}")
                                }
                                SiblingProvenance::Expr { expr } => expr.to_string(),
                            })
                            .collect()
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn table_columns_and_select_literals_carry_provenance() {
        let unioned = proven(
            "INSERT INTO t SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT id, tsc FROM src",
        );
        assert_eq!(
            unioned,
            vec![
                vec![Vec::<String>::new(), Vec::<String>::new()],
                vec![vec![String::from("src#id")], vec![String::from("src#tsc")]],
            ],
            "{unioned:?}"
        );
        let literals = proven(
            "INSERT INTO t SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT 2, 7L",
        );
        assert_eq!(
            literals,
            vec![
                vec![Vec::<String>::new(), Vec::<String>::new()],
                vec![vec![String::from("2")], vec![String::from("7L")]],
            ],
            "{literals:?}"
        );
        let qualified = proven(
            "INSERT INTO t SELECT s.tsc FROM src AS s UNION ALL SELECT a FROM (VALUES (1)) AS v(a)",
        );
        assert_eq!(
            qualified,
            vec![
                vec![vec![String::from("src#tsc")]],
                vec![Vec::<String>::new()],
            ],
            "{qualified:?}"
        );
    }

    #[test]
    fn derived_unions_merge_cells_with_table_provenance() {
        let merged = proven(
            "INSERT INTO t WITH u AS (SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT id, tsc FROM src) SELECT * FROM u",
        );
        assert_eq!(
            merged,
            vec![vec![
                vec![String::from("src#id")],
                vec![String::from("src#tsc")]
            ]],
            "{merged:?}"
        );
        let cells = mapped(
            "INSERT INTO t WITH u AS (SELECT * FROM (VALUES (1, 'x')) AS v(a, b) UNION ALL SELECT id, tsc FROM src) SELECT * FROM u",
        );
        assert_eq!(
            cells,
            vec![vec![
                vec![String::from("1/0/1")],
                vec![String::from("1/1/'x'")],
            ]],
            "{cells:?}"
        );
    }

    #[test]
    fn direct_star_cte_and_alias_shapes_map_to_the_values_columns() {
        let direct = mapped(
            "INSERT INTO t SELECT a, b FROM VALUES (1, TIMESTAMP'2020-01-01 10:00:00') AS v(a, b)",
        );
        assert_eq!(
            direct,
            vec![vec![
                vec![String::from("1/0/1")],
                vec![String::from("1/1/TIMESTAMP '2020-01-01 10:00:00'")],
            ]],
            "{direct:?}"
        );
        let star = mapped("INSERT INTO t SELECT * FROM (VALUES (1, 2), (3, 4)) AS v(a, b)");
        assert_eq!(
            star,
            vec![vec![
                vec![String::from("2/0/1")],
                vec![String::from("2/1/2")]
            ]],
            "{star:?}"
        );
        let cte = mapped("INSERT INTO t WITH v(a, b) AS (VALUES (1, 2)) SELECT b, a FROM v");
        assert_eq!(
            cte,
            vec![vec![
                vec![String::from("1/1/2")],
                vec![String::from("1/0/1")]
            ]],
            "{cte:?}"
        );
        let nested = mapped(
            "INSERT INTO t SELECT x, y FROM (SELECT a AS x, b AS y FROM (VALUES (1, 2)) AS v(a, b)) AS w",
        );
        assert_eq!(
            nested,
            vec![vec![
                vec![String::from("1/0/1")],
                vec![String::from("1/1/2")]
            ]],
            "{nested:?}"
        );
    }

    #[test]
    fn union_arms_map_independently_and_joins_map_per_factor() {
        let union = mapped(
            "INSERT INTO t SELECT a, b FROM (VALUES (1, 2)) AS v(a, b) UNION ALL SELECT a, b FROM (VALUES (3, 4)) AS w(a, b)",
        );
        assert_eq!(
            union,
            vec![
                vec![vec![String::from("1/0/1")], vec![String::from("1/1/2")]],
                vec![vec![String::from("1/0/3")], vec![String::from("1/1/4")]],
            ],
            "{union:?}"
        );
        let joined = mapped(
            "INSERT INTO t SELECT l.a, r.x FROM (VALUES (1, 2)) AS l(a, b) JOIN (VALUES (3)) AS r(x) ON l.a = r.x",
        );
        assert_eq!(
            joined,
            vec![vec![
                vec![String::from("1/0/1")],
                vec![String::from("1/0/3")]
            ]],
            "{joined:?}"
        );
    }

    #[test]
    fn table_view_and_function_positions_stay_unmapped() {
        assert_eq!(
            mapped("INSERT INTO t SELECT tsc FROM src"),
            vec![vec![Vec::<String>::new()]],
        );
        assert_eq!(
            mapped("INSERT INTO t SELECT * FROM (SELECT tsc FROM src) AS w"),
            vec![vec![Vec::<String>::new()]],
        );
        assert_eq!(
            mapped(
                "INSERT INTO t SELECT CAST(b AS BIGINT), unix_seconds(b), 1 FROM (VALUES (1, TIMESTAMP'2020-01-01 10:00:00')) AS v(a, b)",
            ),
            vec![vec![
                Vec::<String>::new(),
                Vec::<String>::new(),
                Vec::<String>::new()
            ]],
        );
        let mixed = mapped(
            "INSERT INTO t SELECT l.b, s.tsc FROM (VALUES (1, TIMESTAMP'2020-01-01 10:00:00')) AS l(a, b) JOIN src AS s ON l.a = s.id",
        );
        assert_eq!(
            mixed,
            vec![vec![
                vec![String::from("1/1/TIMESTAMP '2020-01-01 10:00:00'")],
                Vec::<String>::new(),
            ]],
            "{mixed:?}"
        );
    }

    #[test]
    fn unqualified_names_resolve_only_with_one_factor() {
        let qualified = mapped(
            "INSERT INTO t SELECT l.b FROM (VALUES (1, 2)) AS l(a, b) JOIN (VALUES (3, 4)) AS r(a, b) ON l.a = r.a",
        );
        assert_eq!(
            qualified,
            vec![vec![vec![String::from("1/1/2")]]],
            "{qualified:?}"
        );
        assert_eq!(
            mapped(
                "INSERT INTO t SELECT b FROM (VALUES (1, 2)) AS l(a, b) JOIN (VALUES (3, 4)) AS r(a, b) ON l.a = r.a",
            ),
            vec![vec![Vec::<String>::new()]],
        );
        assert_eq!(
            mapped("INSERT INTO t SELECT v.b FROM (VALUES (1, 2)) AS v(a, b)"),
            vec![vec![vec![String::from("1/1/2")]]],
        );
    }

    #[test]
    fn stars_expand_only_over_resolved_factors() {
        assert_eq!(
            mapped("INSERT INTO t SELECT v.* FROM (VALUES (1, 2)) AS v(a, b)"),
            vec![vec![
                vec![String::from("1/0/1")],
                vec![String::from("1/1/2")]
            ]],
        );
        assert_eq!(
            mapped(
                "INSERT INTO t SELECT * FROM (VALUES (1, 2)) AS v(a, b) JOIN src ON v.a = src.id",
            ),
            vec![Vec::<Vec<String>>::new()],
        );
        assert_eq!(
            mapped("INSERT INTO t SELECT * EXCEPT (a) FROM (VALUES (1, 2)) AS v(a, b)"),
            Vec::<Vec<Vec<String>>>::new(),
        );
    }

    #[test]
    fn duplicate_mismatched_and_recursive_shapes_stay_unmapped() {
        assert_eq!(
            mapped(
                "INSERT INTO t SELECT b FROM (SELECT a AS b, a AS b FROM (VALUES (1, 2)) AS v(a, c)) AS w",
            ),
            vec![vec![Vec::<String>::new()]],
        );
        assert_eq!(
            mapped("INSERT INTO t SELECT b FROM (VALUES (1, 2)) AS v(a)"),
            vec![vec![Vec::<String>::new()]],
        );
        assert_eq!(
            mapped("INSERT INTO t WITH RECURSIVE v AS (SELECT * FROM v) SELECT * FROM v",),
            vec![Vec::<Vec<String>>::new()],
        );
    }

    #[test]
    fn shadowing_and_case_follow_the_session_flag() {
        let shadowed = mapped(
            "INSERT INTO t WITH v(a, b) AS (VALUES (1, 2)) SELECT * FROM (WITH v(a, b) AS (VALUES (3, 4)) SELECT * FROM v) AS w",
        );
        assert_eq!(
            shadowed,
            vec![vec![
                vec![String::from("1/0/3")],
                vec![String::from("1/1/4")]
            ]],
            "{shadowed:?}"
        );
        assert_eq!(
            mapped_case(
                "INSERT INTO t SELECT B FROM (VALUES (1, 2)) AS v(a, b)",
                true,
            ),
            vec![vec![vec![String::from("1/1/2")]]],
        );
        assert_eq!(
            mapped_case(
                "INSERT INTO t SELECT B FROM (VALUES (1, 2)) AS v(a, b)",
                false,
            ),
            vec![vec![Vec::<String>::new()]],
        );
    }

    #[test]
    fn groups_follow_distinct_values_nodes() {
        let mut statements = Parser::parse_sql(
            &GenericDialect,
            "INSERT INTO t SELECT l.a, r.x FROM (VALUES (1, 2)) AS l(a, b) JOIN (VALUES (3)) AS r(x) ON l.a = r.x",
        )
        .unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        let arms = resolve_insert_arms(&source, true);
        assert_eq!(arms.len(), 1);
        let groups = arm_row_groups(&arms[0]);
        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups
                .iter()
                .map(|rows| rows[0].content.len())
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
    }
}
