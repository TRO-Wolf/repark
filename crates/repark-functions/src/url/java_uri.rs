//! Raw RFC-2396 component splitting matching `java.net.URI` without normalization.

/// A split URI.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct JavaUri {
    scheme: Option<String>,
    authority: Option<String>,
    user_info: Option<String>,
    host: Option<String>,
    path: Option<String>,
    query: Option<String>,
    fragment: Option<String>,
}

impl JavaUri {
    /// Split `input` the way `new java.net.URI(input)` does.
    /// # Errors
    /// Returns the `URISyntaxException` reason when `input` is not a legal RFC-2396 URI.
    pub(crate) fn parse(input: &str) -> Result<Self, String> {
        let chars: Vec<char> = input.chars().collect();
        Parser {
            n: chars.len(),
            input: chars,
            uri: Self::default(),
        }
        .run()
    }

    /// `getScheme()` — raw (a scheme cannot contain an escape).
    pub(crate) fn scheme(&self) -> Option<&str> {
        self.scheme.as_deref()
    }

    /// `getHost()` — raw, and NULL for a registry-based authority (e.g.
    pub(crate) fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    /// `getRawAuthority()` — verbatim; keeps an explicit port and empty-userinfo punctuation.
    pub(crate) fn raw_authority(&self) -> Option<&str> {
        self.authority.as_deref()
    }

    /// `getRawUserInfo()` — verbatim; `http://@host/` yields `Some("")`, not NULL.
    pub(crate) fn raw_user_info(&self) -> Option<&str> {
        self.user_info.as_deref()
    }

    /// `getRawPath()` — verbatim, escapes and dot segments intact; NULL for an opaque URI.
    pub(crate) fn raw_path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    /// `getRawQuery()` — verbatim; NULL when there is no `?`.
    pub(crate) fn raw_query(&self) -> Option<&str> {
        self.query.as_deref()
    }

    /// `getRawFragment()` — verbatim; NULL when there is no `#`.
    pub(crate) fn raw_fragment(&self) -> Option<&str> {
        self.fragment.as_deref()
    }
}

/// Character classes (RFC 2396 as `java.net.URI` spells them).
#[derive(Clone, Copy)]
struct CharClass {
    matches: fn(char) -> bool,
    escaped: bool,
}

fn is_alpha(c: char) -> bool {
    c.is_ascii_alphabetic()
}

fn is_alphanum(c: char) -> bool {
    c.is_ascii_alphanumeric()
}

fn is_alphanum_dash(c: char) -> bool {
    is_alphanum(c) || c == '-'
}

fn is_digit(c: char) -> bool {
    c.is_ascii_digit()
}

fn is_scheme(c: char) -> bool {
    is_alphanum(c) || matches!(c, '+' | '-' | '.')
}

fn is_mark(c: char) -> bool {
    matches!(c, '-' | '_' | '.' | '!' | '~' | '*' | '\'' | '(' | ')')
}

fn is_unreserved(c: char) -> bool {
    is_alphanum(c) || is_mark(c)
}

fn is_uric(c: char) -> bool {
    is_unreserved(c)
        || matches!(
            c,
            ';' | '/' | '?' | ':' | '@' | '&' | '=' | '+' | '$' | ',' | '[' | ']'
        )
}

fn is_path(c: char) -> bool {
    is_unreserved(c) || matches!(c, ':' | '@' | '&' | '=' | '+' | '$' | ',' | ';' | '/')
}

fn is_userinfo(c: char) -> bool {
    is_unreserved(c) || matches!(c, ';' | ':' | '&' | '=' | '+' | '$' | ',')
}

fn is_reg_name(c: char) -> bool {
    is_unreserved(c) || matches!(c, '$' | ',' | ';' | ':' | '@' | '&' | '=' | '+')
}

fn is_server(c: char) -> bool {
    is_userinfo(c) || is_alphanum(c) || c == '-' || matches!(c, '.' | ':' | '@' | '[' | ']')
}

fn is_server_percent(c: char) -> bool {
    is_server(c) || c == '%'
}

const SCHEME: CharClass = CharClass {
    matches: is_scheme,
    escaped: false,
};
const URIC: CharClass = CharClass {
    matches: is_uric,
    escaped: true,
};
const PATH: CharClass = CharClass {
    matches: is_path,
    escaped: true,
};
const USERINFO: CharClass = CharClass {
    matches: is_userinfo,
    escaped: true,
};
const REG_NAME: CharClass = CharClass {
    matches: is_reg_name,
    escaped: true,
};
const SERVER: CharClass = CharClass {
    matches: is_server,
    escaped: true,
};
const SERVER_PERCENT: CharClass = CharClass {
    matches: is_server_percent,
    escaped: true,
};
const DIGIT: CharClass = CharClass {
    matches: is_digit,
    escaped: false,
};
const ALPHANUM_DASH: CharClass = CharClass {
    matches: is_alphanum_dash,
    escaped: false,
};

/// Match Java's Unicode space categories rejected by URI parsing.
fn is_unicode_space(c: char) -> bool {
    matches!(
        c,
        '\u{20}' | '\u{a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}'
    )
}

/// The parser.
struct Parser {
    input: Vec<char>,
    n: usize,
    uri: JavaUri,
}

impl Parser {
    fn at(&self, position: usize, c: char) -> bool {
        position < self.n && self.input[position] == c
    }

    fn span(&self, start: usize, end: usize) -> String {
        self.input[start..end].iter().collect()
    }

    /// Java `scan(start, n, err, stop)`: stop index, or `None` when an `err` char came first.
    fn scan_stop(&self, start: usize, n: usize, err: &str, stop: &str) -> Option<usize> {
        let mut p = start;
        while p < n {
            let c = self.input[p];
            if err.contains(c) {
                return None;
            }
            if stop.contains(c) {
                break;
            }
            p += 1;
        }
        Some(p)
    }

    /// Java `scan(start, n, lowMask, highMask)` + `scanEscape`.
    fn scan_class(&self, start: usize, n: usize, class: CharClass) -> Result<usize, String> {
        let mut p = start;
        while p < n {
            let c = self.input[p];
            if (class.matches)(c) {
                p += 1;
                continue;
            }
            if class.escaped {
                if c == '%' {
                    if p + 3 <= n
                        && self.input[p + 1].is_ascii_hexdigit()
                        && self.input[p + 2].is_ascii_hexdigit()
                    {
                        p += 3;
                        continue;
                    }
                    return Err(format!("Malformed escape pair at index {p}"));
                }
                if c as u32 > 128 && !is_unicode_space(c) && !c.is_control() {
                    p += 1;
                    continue;
                }
            }
            break;
        }
        Ok(p)
    }

    fn check_chars(
        &self,
        start: usize,
        end: usize,
        class: CharClass,
        what: &str,
    ) -> Result<(), String> {
        let p = self.scan_class(start, end, class)?;
        if p < end {
            return Err(format!("Illegal character in {what} at index {p}"));
        }
        Ok(())
    }

    fn run(mut self) -> Result<JavaUri, String> {
        let n = self.n;
        let mut p;
        match self.scan_stop(0, n, "/?#", ":") {
            Some(colon) if self.at(colon, ':') => {
                if colon == 0 {
                    return Err("Expected scheme name at index 0".to_string());
                }
                if !is_alpha(self.input[0]) {
                    return Err("Illegal character in scheme name at index 0".to_string());
                }
                self.check_chars(1, colon, SCHEME, "scheme name")?;
                self.uri.scheme = Some(self.span(0, colon));
                p = colon + 1;
                if self.at(p, '/') {
                    p = self.parse_hierarchical(p, n)?;
                } else {
                    let q = self
                        .scan_stop(p, n, "", "#")
                        .ok_or_else(|| "Expected scheme-specific part".to_string())?;
                    if q <= p {
                        return Err(format!("Expected scheme-specific part at index {p}"));
                    }
                    self.check_chars(p, q, URIC, "opaque part")?;
                    p = q;
                }
            }
            _ => {
                p = self.parse_hierarchical(0, n)?;
            }
        }
        if self.at(p, '#') {
            self.check_chars(p + 1, n, URIC, "fragment")?;
            self.uri.fragment = Some(self.span(p + 1, n));
            p = n;
        }
        if p < n {
            return Err(format!("Expected end of URI at index {p}"));
        }
        Ok(self.uri)
    }

    fn parse_hierarchical(&mut self, start: usize, n: usize) -> Result<usize, String> {
        let mut p = start;
        if self.at(p, '/') && self.at(p + 1, '/') {
            p += 2;
            let q = self
                .scan_stop(p, n, "", "/?#")
                .ok_or_else(|| "Expected authority".to_string())?;
            if q > p {
                p = self.parse_authority(p, q)?;
            } else if q < n {
            } else {
                return Err(format!("Expected authority at index {p}"));
            }
        }
        let q = self
            .scan_stop(p, n, "", "?#")
            .ok_or_else(|| "Expected path".to_string())?;
        self.check_chars(p, q, PATH, "path")?;
        self.uri.path = Some(self.span(p, q));
        p = q;
        if self.at(p, '?') {
            p += 1;
            let q = self
                .scan_stop(p, n, "", "#")
                .ok_or_else(|| "Expected query".to_string())?;
            self.check_chars(p, q, URIC, "query")?;
            self.uri.query = Some(self.span(p, q));
            p = q;
        }
        Ok(p)
    }

    fn parse_authority(&mut self, start: usize, n: usize) -> Result<usize, String> {
        let bracketed = self.scan_stop(start, n, "", "]").is_some_and(|q| q > start);
        let server_class = if bracketed { SERVER_PERCENT } else { SERVER };
        let server_chars = self.scan_class(start, n, server_class)? == n;
        let reg_chars = self.scan_class(start, n, REG_NAME)? == n;

        if reg_chars && !server_chars {
            self.uri.authority = Some(self.span(start, n));
            return Ok(n);
        }

        let mut q = start;
        let mut server_error: Option<String> = None;
        if server_chars {
            match self.parse_server(start, n) {
                Ok(end) if end < n => {
                    self.uri.user_info = None;
                    self.uri.host = None;
                    server_error = Some(format!("Expected end of authority at index {end}"));
                }
                Ok(end) => {
                    q = end;
                    self.uri.authority = Some(self.span(start, n));
                }
                Err(error) => {
                    self.uri.user_info = None;
                    self.uri.host = None;
                    server_error = Some(error);
                }
            }
        }

        if q < n {
            if reg_chars {
                self.uri.authority = Some(self.span(start, n));
            } else if let Some(error) = server_error {
                return Err(error);
            } else {
                return Err(format!("Illegal character in authority at index {q}"));
            }
        }
        Ok(n)
    }

    fn parse_server(&mut self, start: usize, n: usize) -> Result<usize, String> {
        let mut p = start;
        if let Some(q) = self.scan_stop(p, n, "/?#", "@")
            && self.at(q, '@')
        {
            {
                self.check_chars(p, q, USERINFO, "user info")?;
                self.uri.user_info = Some(self.span(p, q));
                p = q + 1;
            }
        }

        if self.at(p, '[') {
            p += 1;
            let q = self
                .scan_stop(p, n, "/?#", "]")
                .ok_or_else(|| "Expected closing bracket for IPv6 address".to_string())?;
            if q <= p || !self.at(q, ']') {
                return Err(format!("Expected closing bracket for IPv6 address at {q}"));
            }
            match self.scan_stop(p, q, "", "%") {
                Some(scope) if scope < q => {
                    if scope + 1 == q {
                        return Err("scope id expected".to_string());
                    }
                    self.check_chars(scope + 1, q, ALPHANUM_DASH, "scope id")?;
                }
                _ => {}
            }
            self.uri.host = Some(self.span(p - 1, q + 1));
            p = q + 1;
        } else {
            let after_ipv4 = self.parse_ipv4(p, n);
            p = match after_ipv4 {
                Some(end) => {
                    self.uri.host = Some(self.span(p, end));
                    end
                }
                None => self.parse_hostname(p, n)?,
            };
        }

        if self.at(p, ':') {
            p += 1;
            let q = self
                .scan_stop(p, n, "", "/")
                .ok_or_else(|| "Expected port number".to_string())?;
            if q > p {
                self.check_chars(p, q, DIGIT, "port number")?;
                p = q;
            }
        }
        if p < n {
            return Err(format!("Expected port number at index {p}"));
        }
        Ok(p)
    }

    /// A dotted-quad host.
    fn parse_ipv4(&self, start: usize, n: usize) -> Option<usize> {
        let mut p = start;
        for group in 0..4 {
            if group > 0 {
                if !self.at(p, '.') {
                    return None;
                }
                p += 1;
            }
            let digits_start = p;
            while p < n && p - digits_start < 3 && is_digit(self.input[p]) {
                p += 1;
            }
            if p == digits_start {
                return None;
            }
            let value: u32 = self.span(digits_start, p).parse().ok()?;
            if value > 255 {
                return None;
            }
        }
        if p < n && self.input[p] != ':' {
            return None;
        }
        Some(p)
    }

    /// Java `parseHostname`: alphanumeric labels; a multi-label last label starts with a letter.
    fn parse_hostname(&mut self, start: usize, n: usize) -> Result<usize, String> {
        let mut p = start;
        let mut last_label: Option<usize> = None;
        loop {
            let q = self.scan_class(
                p,
                n,
                CharClass {
                    matches: is_alphanum,
                    escaped: false,
                },
            )?;
            if q > p {
                last_label = Some(p);
                p = q;
                let q = self.scan_class(p, n, ALPHANUM_DASH)?;
                if q > p {
                    if self.input[q - 1] == '-' {
                        return Err(format!("Illegal character in hostname at index {}", q - 1));
                    }
                    p = q;
                }
            }
            if !self.at(p, '.') {
                break;
            }
            p += 1;
            if p >= n {
                break;
            }
        }
        if p < n && !self.at(p, ':') {
            return Err(format!("Illegal character in hostname at index {p}"));
        }
        let Some(label) = last_label else {
            return Err(format!("Expected hostname at index {start}"));
        };
        if label > start && !is_alpha(self.input[label]) {
            return Err(format!("Illegal character in hostname at index {label}"));
        }
        self.uri.host = Some(self.span(start, p));
        Ok(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uri(input: &str) -> JavaUri {
        JavaUri::parse(input).expect("should parse")
    }

    /// X8-a: an explicit port survives in AUTHORITY (`url::Url` drops the default port).
    #[test]
    fn authority_keeps_an_explicit_port() {
        assert_eq!(uri("https://host:443/x").raw_authority(), Some("host:443"));
        assert_eq!(uri("http://h:80/x").raw_authority(), Some("h:80"));
        assert_eq!(uri("https://host/x").raw_authority(), Some("host"));
    }

    /// X8-b: scheme and host keep their case (`url::Url` lowercases both).
    #[test]
    fn scheme_and_host_keep_their_case() {
        let parsed = uri("HTTPS://Example.COM/x");
        assert_eq!(parsed.scheme(), Some("HTTPS"));
        assert_eq!(parsed.host(), Some("Example.COM"));
    }

    /// X8-c: dot segments are NOT resolved (`url::Url` collapses them).
    #[test]
    fn dot_segments_are_not_resolved() {
        assert_eq!(uri("http://h/a/./b/../c").raw_path(), Some("/a/./b/../c"));
    }

    /// X8-d: an IDN host is a registry-based authority ⇒ HOST is NULL, not punycode.
    #[test]
    fn idn_host_is_null_not_punycode() {
        let parsed = uri("http://例え.jp/x");
        assert_eq!(parsed.host(), None);
        assert_eq!(parsed.raw_authority(), Some("例え.jp"));
    }

    /// X8-e: empty-userinfo punctuation is kept, and USERINFO is `""` rather than NULL.
    #[test]
    fn empty_userinfo_punctuation_is_kept() {
        let parsed = uri("http://@host/x");
        assert_eq!(parsed.raw_user_info(), Some(""));
        assert_eq!(parsed.raw_authority(), Some("@host"));
        assert_eq!(parsed.host(), Some("host"));
    }

    /// X8-f: an opaque URI has no path / query / authority.
    #[test]
    fn opaque_uri_has_no_path() {
        let parsed = uri("mailto:a@b.com");
        assert_eq!(parsed.scheme(), Some("mailto"));
        assert_eq!(parsed.raw_path(), None);
        assert_eq!(parsed.raw_query(), None);
        assert_eq!(parsed.raw_authority(), None);
        assert_eq!(parsed.host(), None);
    }

    /// X8-g: preserve percent-escapes in Spark's raw URI components.
    #[test]
    fn percent_escapes_are_never_decoded() {
        assert_eq!(uri("http://h/a/%2e%2e/b").raw_path(), Some("/a/%2e%2e/b"));
        assert_eq!(uri("http://h/a%20b").raw_path(), Some("/a%20b"));
        assert_eq!(uri("http://h/a%2Fb").raw_path(), Some("/a%2Fb"));
        assert_eq!(uri("http://h/p?a=1%26b=2").raw_query(), Some("a=1%26b=2"));
        assert_eq!(uri("http://h/p#f%20g").raw_fragment(), Some("f%20g"));
        let escaped_user = uri("http://us%65r@host/x");
        assert_eq!(escaped_user.raw_user_info(), Some("us%65r"));
        assert_eq!(escaped_user.raw_authority(), Some("us%65r@host"));
        assert_eq!(escaped_user.host(), Some("host"));
        assert_eq!(escaped_user.scheme(), Some("http"));
    }

    #[test]
    fn hierarchical_components_split_like_java() {
        let parsed = uri("http://user:pw@h:8080/p/q?a=1&b=2#frag");
        assert_eq!(parsed.scheme(), Some("http"));
        assert_eq!(parsed.raw_user_info(), Some("user:pw"));
        assert_eq!(parsed.host(), Some("h"));
        assert_eq!(parsed.raw_authority(), Some("user:pw@h:8080"));
        assert_eq!(parsed.raw_path(), Some("/p/q"));
        assert_eq!(parsed.raw_query(), Some("a=1&b=2"));
        assert_eq!(parsed.raw_fragment(), Some("frag"));
    }

    #[test]
    fn empty_path_for_a_bare_authority() {
        assert_eq!(uri("http://h").raw_path(), Some(""));
        assert_eq!(uri("http://h").raw_query(), None);
    }

    #[test]
    fn ipv4_and_ipv6_hosts() {
        assert_eq!(uri("http://127.0.0.1:9/x").host(), Some("127.0.0.1"));
        assert_eq!(uri("http://[::1]:9/x").host(), Some("[::1]"));
    }

    /// Reject spaces as Java does.
    #[test]
    fn illegal_characters_are_a_syntax_error() {
        assert!(JavaUri::parse("not a url").is_err());
        assert!(JavaUri::parse("inva lid://host").is_err());
        assert!(JavaUri::parse("http://h/a%2").is_err());
    }

    /// Accept a relative reference with path, query, and fragment components.
    #[test]
    fn relative_reference_has_only_a_path() {
        let parsed = uri("a/b?c=1#d");
        assert_eq!(parsed.scheme(), None);
        assert_eq!(parsed.host(), None);
        assert_eq!(parsed.raw_path(), Some("a/b"));
        assert_eq!(parsed.raw_query(), Some("c=1"));
        assert_eq!(parsed.raw_fragment(), Some("d"));
    }

    /// Preserve escaped and literal non-ASCII path text verbatim.
    #[test]
    fn multibyte_escapes_and_literal_non_ascii_both_survive_verbatim() {
        assert_eq!(uri("http://h/%E4%BE%8B").raw_path(), Some("/%E4%BE%8B"));
        assert_eq!(uri("http://h/例").raw_path(), Some("/例"));
    }
}
