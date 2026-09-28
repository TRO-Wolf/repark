#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameRule {
    Exact,
    IgnoreCase,
}

impl NameRule {
    #[must_use]
    pub fn from_case_sensitive(case_sensitive: bool) -> Self {
        if case_sensitive {
            Self::Exact
        } else {
            Self::IgnoreCase
        }
    }

    #[must_use]
    pub fn matches(self, written: &str, held: &str) -> bool {
        match self {
            Self::Exact => written == held,
            Self::IgnoreCase => java_equals_ignore_case(written, held),
        }
    }

    #[must_use]
    pub fn lookup<'a>(self, written: &str, held: &'a [String]) -> NameHit<'a> {
        let mut exact: Vec<&'a str> = Vec::new();
        let mut folded: Vec<&'a str> = Vec::new();
        for candidate in held {
            if candidate == written {
                exact.push(candidate.as_str());
            } else if java_equals_ignore_case(candidate, written) {
                folded.push(candidate.as_str());
            }
        }
        match self {
            Self::Exact => match exact.as_slice() {
                [] => {
                    if folded.is_empty() {
                        NameHit::None
                    } else {
                        NameHit::CaseOnly(folded)
                    }
                }
                [one] => NameHit::One(one),
                _ => NameHit::Many(exact),
            },
            Self::IgnoreCase => {
                let mut hits = exact;
                hits.append(&mut folded);
                match hits.as_slice() {
                    [] => NameHit::None,
                    [one] => NameHit::One(one),
                    _ => NameHit::Many(hits),
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NameHit<'a> {
    One(&'a str),
    Many(Vec<&'a str>),
    CaseOnly(Vec<&'a str>),
    None,
}

#[must_use]
pub fn folded_duplicate(names: &[impl AsRef<str>]) -> Option<String> {
    let mut seen: Vec<&str> = Vec::with_capacity(names.len());
    for name in names {
        let name = name.as_ref();
        if seen
            .iter()
            .any(|earlier| java_equals_ignore_case(earlier, name))
        {
            return Some(name.to_ascii_lowercase());
        }
        seen.push(name);
    }
    None
}

fn java_equals_ignore_case(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    if left.encode_utf16().count() != right.encode_utf16().count() {
        return false;
    }
    let mut left_chars = left.chars();
    let mut right_chars = right.chars();
    loop {
        match (left_chars.next(), right_chars.next()) {
            (None, None) => return true,
            (Some(one), Some(other)) => {
                if !java_char_equal(one, other) {
                    return false;
                }
            }
            (None, Some(_)) | (Some(_), None) => return false,
        }
    }
}

fn java_char_equal(one: char, other: char) -> bool {
    one == other
        || single_upper(one) == single_upper(other)
        || single_lower(one) == single_lower(other)
}

fn single_upper(value: char) -> char {
    let mut mapped = value.to_uppercase();
    match (mapped.next(), mapped.next()) {
        (Some(one), None) => one,
        _ => value,
    }
}

fn single_lower(value: char) -> char {
    let mut mapped = value.to_lowercase();
    match (mapped.next(), mapped.next()) {
        (Some(one), None) => one,
        _ => value,
    }
}

#[must_use]
pub fn column_already_exists(name: &str) -> String {
    format!(
        "[COLUMN_ALREADY_EXISTS] The column `{name}` already exists. Choose another name or \
         rename the existing column. SQLSTATE: 42711"
    )
}

#[cfg(test)]
mod tests {
    use super::{NameHit, NameRule, column_already_exists, folded_duplicate};

    #[test]
    fn lookup_answers_each_rule() {
        let held = ["id".to_string(), "Data".to_string(), "ID".to_string()];
        let legs: &[(NameRule, &str, NameHit<'_>)] = &[
            (NameRule::Exact, "id", NameHit::One("id")),
            (NameRule::Exact, "Data", NameHit::One("Data")),
            (NameRule::Exact, "ID", NameHit::One("ID")),
            (NameRule::Exact, "data", NameHit::CaseOnly(vec!["Data"])),
            (NameRule::Exact, "Id", NameHit::CaseOnly(vec!["id", "ID"])),
            (NameRule::Exact, "nope", NameHit::None),
            (NameRule::IgnoreCase, "DATA", NameHit::One("Data")),
            (NameRule::IgnoreCase, "nope", NameHit::None),
            (NameRule::IgnoreCase, "id", NameHit::Many(vec!["id", "ID"])),
            (NameRule::IgnoreCase, "Id", NameHit::Many(vec!["id", "ID"])),
        ];
        for (rule, written, expected) in legs {
            assert_eq!(rule.lookup(written, &held), *expected, "{rule:?} {written}");
        }
        assert_eq!(NameRule::from_case_sensitive(true), NameRule::Exact,);
        assert_eq!(NameRule::from_case_sensitive(false), NameRule::IgnoreCase,);
        assert!(NameRule::Exact.matches("ID", "ID"));
        assert!(!NameRule::Exact.matches("ID", "id"));
        assert!(NameRule::IgnoreCase.matches("ID", "id"));
    }

    #[test]
    fn folded_duplicate_reports_the_lower_cased_twin() {
        assert_eq!(folded_duplicate(&["a", "A"]), Some("a".to_string()));
        assert_eq!(folded_duplicate(&["ID", "x", "id"]), Some("id".to_string()));
        assert_eq!(folded_duplicate(&["a", "b"]), None);
        assert_eq!(folded_duplicate(&["a", "A", "a"]), Some("a".to_string()));
        assert_eq!(
            folded_duplicate(&["a".to_string(), "A".to_string()]),
            Some("a".to_string())
        );
        assert_eq!(
            column_already_exists("a"),
            "[COLUMN_ALREADY_EXISTS] The column `a` already exists. Choose another name or \
             rename the existing column. SQLSTATE: 42711"
        );
    }

    #[test]
    fn ignore_case_folds_unicode_like_java_equals_ignore_case() {
        let folded: &[(&str, &str)] = &[
            ("ünï", "Ünï"),
            ("ÜNÏ", "Ünï"),
            ("éte", "Éte"),
            ("ÉTE", "Éte"),
            ("id", "ID"),
            ("Data", "DATA"),
        ];
        for (written, held) in folded {
            assert!(
                NameRule::IgnoreCase.matches(written, held),
                "{written} {held}"
            );
            assert!(
                NameRule::IgnoreCase.matches(held, written),
                "{held} {written}"
            );
        }
        let split: &[(&str, &str)] = &[
            ("STRASSE", "straße"),
            ("ß", "SS"),
            ("ß", "S"),
            ("id", "idx"),
            ("ünï", "ünïx"),
        ];
        for (written, held) in split {
            assert!(
                !NameRule::IgnoreCase.matches(written, held),
                "{written} {held}"
            );
        }
        let held = ["id".to_string(), "Ünï".to_string(), "Éte".to_string()];
        assert_eq!(
            NameRule::IgnoreCase.lookup("ünï", &held),
            NameHit::One("Ünï")
        );
        assert_eq!(
            NameRule::IgnoreCase.lookup("ÜNÏ", &held),
            NameHit::One("Ünï")
        );
        assert_eq!(
            NameRule::IgnoreCase.lookup("éte", &held),
            NameHit::One("Éte")
        );
        assert_eq!(NameRule::IgnoreCase.lookup("nope", &held), NameHit::None);
        assert_eq!(folded_duplicate(&["Ünï", "ünï"]), Some("ünï".to_string()));
        assert_eq!(folded_duplicate(&["straße", "STRASSE"]), None);
    }
}
