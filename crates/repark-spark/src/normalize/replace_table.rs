use datafusion::error::Result;
use datafusion::sql::sqlparser::ast::ObjectName;
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Whitespace, Word};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;

use crate::catalog_ops::{catalog_handle, iceberg_err, name_parts, table_or_view_not_found};

pub(crate) fn rewrite_replace_table(tokens: Vec<Token>) -> Vec<Token> {
    let Some(head) = replace_table_head(&tokens) else {
        return tokens;
    };
    let mut out = Vec::with_capacity(tokens.len() + 4);
    out.extend_from_slice(&tokens[..head]);
    out.push(word_token("CREATE", Keyword::CREATE));
    out.push(Token::Whitespace(Whitespace::Space));
    out.push(word_token("OR", Keyword::OR));
    out.push(Token::Whitespace(Whitespace::Space));
    out.extend_from_slice(&tokens[head..]);
    out
}

pub(crate) fn is_replace_table_sql(sql: &str) -> bool {
    Tokenizer::new(&DatabricksDialect {}, sql)
        .tokenize()
        .is_ok_and(|tokens| replace_table_head(&tokens).is_some())
}

pub(crate) async fn refuse_missing_replace_target(
    catalogs: &CatalogRegistry,
    sql: &str,
    name: &ObjectName,
) -> Result<()> {
    if !is_replace_table_sql(sql) {
        return Ok(());
    }
    let parts = name_parts(name);
    let [catalog, namespace, table] = parts.as_slice() else {
        return Ok(());
    };
    let handle = catalog_handle(catalogs, catalog)?;
    let ident = TableIdent::new(NamespaceIdent::new(namespace.clone()), table.clone());
    if handle.table_exists(&ident).await.map_err(iceberg_err)? {
        return Ok(());
    }
    Err(table_or_view_not_found(catalog, namespace, table))
}

fn replace_table_head(tokens: &[Token]) -> Option<usize> {
    let head = significant_index(tokens, 0)?;
    if !is_keyword(tokens.get(head), Keyword::REPLACE) {
        return None;
    }
    let second = significant_index(tokens, head + 1)?;
    if !is_keyword(tokens.get(second), Keyword::TABLE) {
        return None;
    }
    Some(head)
}

fn significant_index(tokens: &[Token], from: usize) -> Option<usize> {
    (from..tokens.len()).find(|index| !matches!(tokens[*index], Token::Whitespace(_)))
}

fn is_keyword(token: Option<&Token>, keyword: Keyword) -> bool {
    matches!(token, Some(Token::Word(word)) if word.keyword == keyword)
}

fn word_token(value: &str, keyword: Keyword) -> Token {
    Token::Word(Word {
        value: value.to_string(),
        quote_style: None,
        keyword,
    })
}
