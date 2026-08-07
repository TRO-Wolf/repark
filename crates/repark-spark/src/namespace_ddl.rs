//! Namespace DDL — PR-2 PARTIAL rider: only [`consume_word`], the parser helper the spine's
//! DESCRIBE/SHOW module shares. The v1 module's handlers (`try_parse_create_namespace`,
//! `execute_create_namespace`, `execute_drop_namespace`, `execute_drop_table`) land verbatim in
//! phase-2 PR-3a; the router refuses their statement shapes loudly until then (ledger-declared).

use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;

/// Consume the next token iff it is a `Word` whose value equals `word` (case-insensitive) — used for
/// the `NAMESPACE` / `DBPROPERTIES` / `PROPERTIES` spellings sqlparser 0.59 does not model as
/// keywords. Returns whether it was consumed.
pub(crate) fn consume_word(parser: &mut Parser, word: &str) -> bool {
    if let Token::Word(peeked) = &parser.peek_token().token
        && peeked.value.eq_ignore_ascii_case(word)
    {
        parser.next_token();
        return true;
    }
    false
}
