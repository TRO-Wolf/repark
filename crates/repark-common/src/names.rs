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
            Self::IgnoreCase => written.eq_ignore_ascii_case(held),
        }
    }

    #[must_use]
    pub fn lookup<'a>(self, written: &str, held: &'a [String]) -> NameHit<'a> {
        let mut exact: Vec<&'a str> = Vec::new();
        let mut folded: Vec<&'a str> = Vec::new();
        for candidate in held {
            if candidate == written {
                exact.push(candidate.as_str());
            } else if candidate.eq_ignore_ascii_case(written) {
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

#[cfg(test)]
mod tests {
    use super::{NameHit, NameRule};

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
}
