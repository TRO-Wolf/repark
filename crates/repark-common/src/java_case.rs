const UPPER_IDENTITY: &[char] = &[
    '\u{19b}',
    '\u{264}',
    '\u{1c8a}',
    '\u{2c5f}',
    '\u{a7c1}',
    '\u{a7cd}',
    '\u{a7cf}',
    '\u{a7d1}',
    '\u{a7d3}',
    '\u{a7d5}',
    '\u{a7d7}',
    '\u{a7d9}',
    '\u{a7db}',
    '\u{10597}',
    '\u{10598}',
    '\u{10599}',
    '\u{1059a}',
    '\u{1059b}',
    '\u{1059c}',
    '\u{1059d}',
    '\u{1059e}',
    '\u{1059f}',
    '\u{105a0}',
    '\u{105a1}',
    '\u{105a3}',
    '\u{105a4}',
    '\u{105a5}',
    '\u{105a6}',
    '\u{105a7}',
    '\u{105a8}',
    '\u{105a9}',
    '\u{105aa}',
    '\u{105ab}',
    '\u{105ac}',
    '\u{105ad}',
    '\u{105ae}',
    '\u{105af}',
    '\u{105b0}',
    '\u{105b1}',
    '\u{105b3}',
    '\u{105b4}',
    '\u{105b5}',
    '\u{105b6}',
    '\u{105b7}',
    '\u{105b8}',
    '\u{105b9}',
    '\u{105bb}',
    '\u{105bc}',
    '\u{10d70}',
    '\u{10d71}',
    '\u{10d72}',
    '\u{10d73}',
    '\u{10d74}',
    '\u{10d75}',
    '\u{10d76}',
    '\u{10d77}',
    '\u{10d78}',
    '\u{10d79}',
    '\u{10d7a}',
    '\u{10d7b}',
    '\u{10d7c}',
    '\u{10d7d}',
    '\u{10d7e}',
    '\u{10d7f}',
    '\u{10d80}',
    '\u{10d81}',
    '\u{10d82}',
    '\u{10d83}',
    '\u{10d84}',
    '\u{10d85}',
    '\u{16ebb}',
    '\u{16ebc}',
    '\u{16ebd}',
    '\u{16ebe}',
    '\u{16ebf}',
    '\u{16ec0}',
    '\u{16ec1}',
    '\u{16ec2}',
    '\u{16ec3}',
    '\u{16ec4}',
    '\u{16ec5}',
    '\u{16ec6}',
    '\u{16ec7}',
    '\u{16ec8}',
    '\u{16ec9}',
    '\u{16eca}',
    '\u{16ecb}',
    '\u{16ecc}',
    '\u{16ecd}',
    '\u{16ece}',
    '\u{16ecf}',
    '\u{16ed0}',
    '\u{16ed1}',
    '\u{16ed2}',
    '\u{16ed3}',
];

const LOWER_IDENTITY: &[char] = &[
    '\u{1c89}',
    '\u{2c2f}',
    '\u{a7c0}',
    '\u{a7cb}',
    '\u{a7cc}',
    '\u{a7ce}',
    '\u{a7d0}',
    '\u{a7d2}',
    '\u{a7d4}',
    '\u{a7d6}',
    '\u{a7d8}',
    '\u{a7da}',
    '\u{a7dc}',
    '\u{10570}',
    '\u{10571}',
    '\u{10572}',
    '\u{10573}',
    '\u{10574}',
    '\u{10575}',
    '\u{10576}',
    '\u{10577}',
    '\u{10578}',
    '\u{10579}',
    '\u{1057a}',
    '\u{1057c}',
    '\u{1057d}',
    '\u{1057e}',
    '\u{1057f}',
    '\u{10580}',
    '\u{10581}',
    '\u{10582}',
    '\u{10583}',
    '\u{10584}',
    '\u{10585}',
    '\u{10586}',
    '\u{10587}',
    '\u{10588}',
    '\u{10589}',
    '\u{1058a}',
    '\u{1058c}',
    '\u{1058d}',
    '\u{1058e}',
    '\u{1058f}',
    '\u{10590}',
    '\u{10591}',
    '\u{10592}',
    '\u{10594}',
    '\u{10595}',
    '\u{10d50}',
    '\u{10d51}',
    '\u{10d52}',
    '\u{10d53}',
    '\u{10d54}',
    '\u{10d55}',
    '\u{10d56}',
    '\u{10d57}',
    '\u{10d58}',
    '\u{10d59}',
    '\u{10d5a}',
    '\u{10d5b}',
    '\u{10d5c}',
    '\u{10d5d}',
    '\u{10d5e}',
    '\u{10d5f}',
    '\u{10d60}',
    '\u{10d61}',
    '\u{10d62}',
    '\u{10d63}',
    '\u{10d64}',
    '\u{10d65}',
    '\u{16ea0}',
    '\u{16ea1}',
    '\u{16ea2}',
    '\u{16ea3}',
    '\u{16ea4}',
    '\u{16ea5}',
    '\u{16ea6}',
    '\u{16ea7}',
    '\u{16ea8}',
    '\u{16ea9}',
    '\u{16eaa}',
    '\u{16eab}',
    '\u{16eac}',
    '\u{16ead}',
    '\u{16eae}',
    '\u{16eaf}',
    '\u{16eb0}',
    '\u{16eb1}',
    '\u{16eb2}',
    '\u{16eb3}',
    '\u{16eb4}',
    '\u{16eb5}',
    '\u{16eb6}',
    '\u{16eb7}',
    '\u{16eb8}',
];

const UPPER_SINGLE: &[(char, char)] = &[
    ('\u{1f80}', '\u{1f88}'),
    ('\u{1f81}', '\u{1f89}'),
    ('\u{1f82}', '\u{1f8a}'),
    ('\u{1f83}', '\u{1f8b}'),
    ('\u{1f84}', '\u{1f8c}'),
    ('\u{1f85}', '\u{1f8d}'),
    ('\u{1f86}', '\u{1f8e}'),
    ('\u{1f87}', '\u{1f8f}'),
    ('\u{1f90}', '\u{1f98}'),
    ('\u{1f91}', '\u{1f99}'),
    ('\u{1f92}', '\u{1f9a}'),
    ('\u{1f93}', '\u{1f9b}'),
    ('\u{1f94}', '\u{1f9c}'),
    ('\u{1f95}', '\u{1f9d}'),
    ('\u{1f96}', '\u{1f9e}'),
    ('\u{1f97}', '\u{1f9f}'),
    ('\u{1fa0}', '\u{1fa8}'),
    ('\u{1fa1}', '\u{1fa9}'),
    ('\u{1fa2}', '\u{1faa}'),
    ('\u{1fa3}', '\u{1fab}'),
    ('\u{1fa4}', '\u{1fac}'),
    ('\u{1fa5}', '\u{1fad}'),
    ('\u{1fa6}', '\u{1fae}'),
    ('\u{1fa7}', '\u{1faf}'),
    ('\u{1fb3}', '\u{1fbc}'),
    ('\u{1fc3}', '\u{1fcc}'),
    ('\u{1ff3}', '\u{1ffc}'),
];

fn java_upper_char(value: char) -> char {
    if UPPER_IDENTITY.binary_search(&value).is_ok() {
        return value;
    }
    if let Ok(index) = UPPER_SINGLE.binary_search_by_key(&value, |(key, _)| *key) {
        return UPPER_SINGLE[index].1;
    }
    let mut uppered = value.to_uppercase();
    match (uppered.next(), uppered.next()) {
        (Some(single), None) => single,
        _ => value,
    }
}

fn java_lower_char(value: char) -> char {
    if value == '\u{130}' {
        return 'i';
    }
    if LOWER_IDENTITY.binary_search(&value).is_ok() {
        return value;
    }
    let mut lowered = value.to_lowercase();
    match (lowered.next(), lowered.next()) {
        (Some(single), None) => single,
        _ => value,
    }
}

fn is_final_sigma(values: &[char], index: usize) -> bool {
    values[..index].iter().any(|value| value.is_alphabetic())
        && !values[index + 1..]
            .iter()
            .any(|value| value.is_alphabetic())
}

#[must_use]
pub fn string_lowered(value: &str) -> String {
    let values: Vec<char> = value.chars().collect();
    let mut lowered = String::with_capacity(value.len());
    for (index, current) in values.iter().enumerate() {
        if *current == '\u{130}' {
            lowered.push_str("i\u{307}");
        } else if *current == '\u{3a3}' {
            lowered.push(if is_final_sigma(&values, index) {
                '\u{3c2}'
            } else {
                '\u{3c3}'
            });
        } else {
            lowered.push(java_lower_char(*current));
        }
    }
    lowered
}

#[must_use]
pub fn string_lower_equal(first: &str, second: &str) -> bool {
    first.chars().count() == second.chars().count()
        && string_lowered(first) == string_lowered(second)
}

#[must_use]
pub fn fold_b_equal(first: &str, second: &str) -> bool {
    let mut left = first.chars();
    let mut right = second.chars();
    loop {
        match (left.next(), right.next()) {
            (None, None) => return true,
            (Some(one), Some(other)) => {
                if one != other
                    && java_upper_char(one) != java_upper_char(other)
                    && java_lower_char(java_upper_char(one))
                        != java_lower_char(java_upper_char(other))
                {
                    return false;
                }
            }
            (None, Some(_)) | (Some(_), None) => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{fold_b_equal, java_lower_char, java_upper_char, string_lower_equal};

    const DUMP: &str = include_str!("java_case_dump.txt");

    fn fixture() -> Vec<(u32, u32, u32)> {
        let mut entries = Vec::new();
        for line in DUMP.lines() {
            if line.starts_with('#') {
                continue;
            }
            let mut parts = line.split(' ');
            let point = parts.next().expect("dump line carries a codepoint");
            let upper = parts.next().expect("dump line carries an upper mapping");
            let lower = parts.next().expect("dump line carries a lower mapping");
            assert!(parts.next().is_none(), "dump line has three fields");
            let parsed = [
                u32::from_str_radix(point, 16).expect("dump codepoint parses"),
                u32::from_str_radix(upper, 16).expect("dump upper parses"),
                u32::from_str_radix(lower, 16).expect("dump lower parses"),
            ];
            entries.push((parsed[0], parsed[1], parsed[2]));
        }
        entries
    }

    #[test]
    fn dump_fixture_is_sorted_complete_and_spot_checked() {
        let entries = fixture();
        assert_eq!(entries.len(), 2799);
        for pair in entries.windows(2) {
            assert!(pair[0].0 < pair[1].0, "dump stays sorted");
        }
        for absent in ['\u{10570}', '\u{a7c0}'] {
            assert!(
                entries
                    .binary_search_by_key(&(u32::from(absent)), |(point, _, _)| *point)
                    .is_err()
            );
        }
        for (point, upper, lower) in [(0x41, 0x41, 0x61), (0x130, 0x130, 0x69)] {
            assert!(entries.binary_search(&(point, upper, lower)).is_ok());
        }
    }

    #[test]
    fn every_codepoint_matches_the_dump() {
        let entries = fixture();
        let mut index = 0;
        let mut bad_upper = Vec::new();
        let mut bad_lower = Vec::new();
        for point in 0..0x11_0000u32 {
            if (0xd800..0xe000).contains(&point) {
                continue;
            }
            while index < entries.len() && entries[index].0 < point {
                index += 1;
            }
            let expected = if index < entries.len() && entries[index].0 == point {
                (entries[index].1, entries[index].2)
            } else {
                (point, point)
            };
            let value = char::from_u32(point).expect("non-surrogate codepoint is a char");
            let got_upper = u32::from(java_upper_char(value));
            let got_lower = u32::from(java_lower_char(value));
            if got_upper != expected.0 {
                bad_upper.push((point, got_upper, expected.0));
            }
            if got_lower != expected.1 {
                bad_lower.push((point, got_lower, expected.1));
            }
        }
        for (point, got, want) in bad_upper.iter().take(80) {
            println!("UPPER U+{point:04X} rust={got:04X} java={want:04X}");
        }
        for (point, got, want) in bad_lower.iter().take(80) {
            println!("LOWER U+{point:04X} rust={got:04X} java={want:04X}");
        }
        println!(
            "bad_upper={} bad_lower={}",
            bad_upper.len(),
            bad_lower.len()
        );
        assert!(bad_upper.is_empty() && bad_lower.is_empty());
    }

    #[test]
    fn fold_b_accepts_single_upper_and_lower_variants() {
        for (held, written) in [
            ("I", "\u{131}"),
            ("\u{b5}", "\u{3bc}"),
            ("s", "\u{17f}"),
            ("k", "\u{212a}"),
            ("\u{df}", "\u{1e9e}"),
            ("\u{3a3}", "\u{3c3}"),
            ("\u{3a3}", "\u{3c2}"),
            ("\u{130}", "\u{131}"),
            ("\u{130}d", "id"),
            ("\u{130}d", "\u{131}d"),
        ] {
            assert!(fold_b_equal(held, written), "{held} folds to {written}");
            assert!(fold_b_equal(written, held), "{written} folds to {held}");
        }
    }

    #[test]
    fn fold_b_rejects_expansions_newer_scripts_and_mismatches() {
        for (held, written) in [
            ("\u{df}", "SS"),
            ("\u{130}", "i\u{307}"),
            ("\u{fb00}", "FF"),
            ("\u{10570}", "\u{10597}"),
            ("\u{a7c0}", "\u{a7c1}"),
            ("\u{390}", "\u{1fd3}"),
            ("v", "x"),
            ("", "v"),
        ] {
            assert!(
                !fold_b_equal(held, written),
                "{held} must not fold to {written}"
            );
            assert!(
                !fold_b_equal(written, held),
                "{written} must not fold to {held}"
            );
        }
    }

    #[test]
    fn fold_b_accepts_supplementary_cased_pair() {
        assert!(fold_b_equal("\u{10400}", "\u{10428}"));
        assert!(fold_b_equal("\u{10428}", "\u{10400}"));
    }

    #[test]
    fn string_lower_accepts_lower_variants() {
        for (held, written) in [
            ("k", "\u{212a}"),
            ("\u{df}", "\u{1e9e}"),
            ("\u{3a3}", "\u{3c3}"),
            ("\u{130}d", "\u{130}D"),
            ("a\u{3a3}", "A\u{3c2}"),
            ("\u{3a3}\u{3a3}", "\u{3c3}\u{3c2}"),
            ("a\u{3a3}b", "a\u{3c3}b"),
        ] {
            assert!(
                string_lower_equal(held, written),
                "{held} folds to {written}"
            );
            assert!(
                string_lower_equal(written, held),
                "{written} folds to {held}"
            );
        }
    }

    #[test]
    fn string_lower_rejects_upper_only_pairs_expansions_and_mismatches() {
        for (held, written) in [
            ("I", "\u{131}"),
            ("\u{b5}", "\u{3bc}"),
            ("s", "\u{17f}"),
            ("\u{3a3}", "\u{3c2}"),
            ("\u{df}", "SS"),
            ("\u{130}", "i\u{307}"),
            ("\u{130}d", "id"),
            ("\u{130}d", "\u{131}d"),
            ("id", "\u{131}d"),
            ("v", "x"),
            ("", "v"),
        ] {
            assert!(
                !string_lower_equal(held, written),
                "{held} must not fold to {written}"
            );
            assert!(
                !string_lower_equal(written, held),
                "{written} must not fold to {held}"
            );
        }
    }
}
