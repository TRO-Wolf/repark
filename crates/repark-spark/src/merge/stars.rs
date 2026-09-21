use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::Token;

pub(crate) const STAR_SENTINEL: &str = "__repark_merge_star_sentinel__";

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

fn sentinel_token() -> Token {
    Token::make_word(STAR_SENTINEL, None)
}
