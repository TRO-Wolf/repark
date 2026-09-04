use std::iter::Peekable;
use std::str::Chars;

#[derive(Clone, Copy)]
enum ClassAtom {
    Char(char),
    Range(char, char),
}

pub(crate) fn translate_java_char_classes(pattern: &str) -> String {
    let mut chars = pattern.chars().peekable();
    let mut out = String::with_capacity(pattern.len());
    while let Some(character) = chars.next() {
        if character == '\\' {
            out.push('\\');
            if let Some(escaped) = chars.next() {
                out.push(escaped);
            }
            continue;
        }
        if character == '[' {
            match parse_java_class(&mut chars) {
                Some(emitted) => out.push_str(&emitted),
                None => out.push('['),
            }
            continue;
        }
        out.push(character);
    }
    out
}

fn parse_java_class(chars: &mut Peekable<Chars<'_>>) -> Option<String> {
    let mut negated = false;
    if chars.peek() == Some(&'^') {
        chars.next();
        negated = true;
    }
    let mut atoms: Vec<ClassAtom> = Vec::new();
    let mut at_start = true;
    let mut pending: Option<char> = None;
    let mut range_pending = false;
    loop {
        let character = chars.next()?;
        if character == '\\' {
            let escaped = chars.next()?;
            take_atom(&mut atoms, &mut pending, &mut range_pending, escaped);
            at_start = false;
            continue;
        }
        if character == ']' && !at_start {
            if let Some(last) = pending.take() {
                atoms.push(ClassAtom::Char(last));
            }
            if range_pending {
                atoms.push(ClassAtom::Char('-'));
            }
            return Some(emit_class(negated, &atoms));
        }
        if character == '[' {
            let nested_atoms = parse_nested_class_atoms(chars)?;
            if range_pending {
                if let Some(start) = pending.take() {
                    atoms.push(ClassAtom::Char(start));
                }
                atoms.push(ClassAtom::Char('-'));
                range_pending = false;
            } else if let Some(start) = pending.take() {
                atoms.push(ClassAtom::Char(start));
            }
            atoms.extend(nested_atoms);
            at_start = false;
            continue;
        }
        if character == '-' && !at_start && !range_pending && pending.is_some() {
            range_pending = true;
            continue;
        }
        take_atom(&mut atoms, &mut pending, &mut range_pending, character);
        at_start = false;
    }
}

fn parse_nested_class_atoms(chars: &mut Peekable<Chars<'_>>) -> Option<Vec<ClassAtom>> {
    let mut atoms: Vec<ClassAtom> = Vec::new();
    let mut at_start = true;
    let mut pending: Option<char> = None;
    let mut range_pending = false;
    if chars.peek() == Some(&'^') {
        chars.next();
        at_start = true;
    }
    loop {
        let character = chars.next()?;
        if character == '\\' {
            let escaped = chars.next()?;
            take_atom(&mut atoms, &mut pending, &mut range_pending, escaped);
            at_start = false;
            continue;
        }
        if character == ']' && !at_start {
            if let Some(last) = pending.take() {
                atoms.push(ClassAtom::Char(last));
            }
            if range_pending {
                atoms.push(ClassAtom::Char('-'));
            }
            return Some(atoms);
        }
        if character == '[' {
            let inner = parse_nested_class_atoms(chars)?;
            if let Some(start) = pending.take() {
                atoms.push(ClassAtom::Char(start));
            }
            if range_pending {
                atoms.push(ClassAtom::Char('-'));
                range_pending = false;
            }
            atoms.extend(inner);
            at_start = false;
            continue;
        }
        if character == '-' && !at_start && !range_pending && pending.is_some() {
            range_pending = true;
            continue;
        }
        take_atom(&mut atoms, &mut pending, &mut range_pending, character);
        at_start = false;
    }
}

fn take_atom(
    atoms: &mut Vec<ClassAtom>,
    pending: &mut Option<char>,
    range_pending: &mut bool,
    character: char,
) {
    if *range_pending {
        if let Some(start) = pending.take() {
            atoms.push(ClassAtom::Range(start, character));
        } else {
            atoms.push(ClassAtom::Char('-'));
            *pending = Some(character);
        }
        *range_pending = false;
        return;
    }
    if let Some(previous) = pending.replace(character) {
        atoms.push(ClassAtom::Char(previous));
    }
}

fn emit_class(negated: bool, atoms: &[ClassAtom]) -> String {
    let mut out = String::from("[");
    if negated {
        out.push('^');
    }
    for atom in atoms {
        match *atom {
            ClassAtom::Char(character) => out.push_str(&hex_char(character)),
            ClassAtom::Range(start, end) => {
                out.push_str(&hex_char(start));
                out.push('-');
                out.push_str(&hex_char(end));
            }
        }
    }
    out.push(']');
    out
}

fn hex_char(character: char) -> String {
    format!("\\x{{{:X}}}", u32::from(character))
}

#[cfg(test)]
mod tests {
    use regex::Regex;

    use super::translate_java_char_classes;

    fn count(pattern: &str, text: &str) -> usize {
        let translated = translate_java_char_classes(pattern);
        let compiled = Regex::new(&translated).expect("translated pattern compiles");
        compiled.find_iter(text).count()
    }

    #[test]
    fn posix_alpha_is_java_nested_class() {
        assert_eq!(count("[[:alpha:]]", "a1b2 Ünï_9"), 1);
        assert_eq!(count("[[:alpha:]]", "foo"), 0);
        assert_eq!(count("[[:alpha:]]", "aabbaa"), 4);
        assert_eq!(count("[[:alpha:]]", ""), 0);
        assert_eq!(count("[[:alpha:]]", "[:alpha:]"), 7);
        assert_eq!(count("[[:alpha:]]", "["), 0);
        assert_eq!(count("[[:digit:]]", "[:alpha:]"), 2);
        assert_eq!(count("[[:alnum:]]", "a1b2 Ünï_9"), 2);
        assert_eq!(count("[[:space:]]", "a1b2 Ünï_9"), 1);
        assert_eq!(count("[^[:alpha:]]", "foo"), 3);
        assert_eq!(count("[^[:alpha:]]", "aabbaa"), 2);
        assert_eq!(count("[[:alpha:]0-9]", "a1b2 Ünï_9"), 4);
        assert_eq!(count("[[:ALPHA:]]", "[:alpha:]"), 2);
    }
}
