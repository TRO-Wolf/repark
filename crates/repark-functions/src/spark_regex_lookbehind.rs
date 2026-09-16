use datafusion::common::Result;

use crate::spark_regex_engine::{
    GroupKind, Spans, find_quote_end, group_kind, match_group, skip_class,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GroupTarget {
    Skeleton(usize),
    Lookbehind { assertion: usize, local: usize },
}

pub(crate) struct AssertionPlan {
    pub(crate) marker: usize,
    pub(crate) negative: bool,
    pub(crate) body: String,
    pub(crate) max_len: Option<usize>,
}

pub(crate) struct LookbehindPlan {
    pub(crate) skeleton: String,
    pub(crate) assertions: Vec<AssertionPlan>,
    pub(crate) group_map: Vec<GroupTarget>,
    pub(crate) named: Vec<(String, usize)>,
}

pub(crate) struct LookbehindSpan {
    start: usize,
    close: usize,
    negative: bool,
}

pub(crate) fn find_lookbehinds(pattern: &str) -> Vec<LookbehindSpan> {
    let bytes = pattern.as_bytes();
    let mut spans = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => {
                if bytes.get(index + 1) == Some(&b'Q') {
                    index = find_quote_end(bytes, index + 2).saturating_sub(1);
                } else {
                    index += 1;
                }
            }
            b'[' => index = skip_class(bytes, index).saturating_sub(1),
            b'(' => {
                if matches!(group_kind(bytes, index), GroupKind::Lookbehind)
                    && let Some(close) = match_group(bytes, index)
                {
                    spans.push(LookbehindSpan {
                        start: index,
                        close,
                        negative: bytes.get(index + 3) == Some(&b'!'),
                    });
                    index = close;
                }
            }
            _ => {}
        }
        index += 1;
    }
    spans
}

pub(crate) fn named_group_table(pattern: &str) -> Vec<(String, usize)> {
    let bytes = pattern.as_bytes();
    let mut table = Vec::new();
    let mut index = 0;
    let mut groups = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => {
                if bytes.get(index + 1) == Some(&b'Q') {
                    index = find_quote_end(bytes, index + 2).saturating_sub(1);
                } else {
                    index += 1;
                }
            }
            b'[' => index = skip_class(bytes, index).saturating_sub(1),
            b'(' if group_kind(bytes, index) == GroupKind::Consuming => {
                groups += 1;
                if bytes.get(index + 1) == Some(&b'?')
                    && bytes.get(index + 2) == Some(&b'<')
                    && !matches!(bytes.get(index + 3), Some(b'=' | b'!'))
                    && let Some(end) = bytes[index + 3..].iter().position(|byte| *byte == b'>')
                {
                    table.push((pattern[index + 3..index + 3 + end].to_owned(), groups));
                }
            }
            _ => {}
        }
        index += 1;
    }
    table
}

pub(crate) fn normalize_backrefs(pattern: &str, groups: usize) -> String {
    let table = named_group_table(pattern);
    if table.is_empty() || !has_numeric_backref(pattern, groups) {
        return pattern.to_owned();
    }
    let bytes = pattern.as_bytes();
    let mut out = String::with_capacity(pattern.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            if bytes.get(index + 1) == Some(&b'Q') {
                let end = find_quote_end(bytes, index + 2).min(pattern.len());
                out.push_str(&pattern[index..end]);
                index = end;
                continue;
            }
            if bytes.get(index + 1) == Some(&b'k')
                && bytes.get(index + 2) == Some(&b'<')
                && let Some(end) = bytes[index + 3..].iter().position(|byte| *byte == b'>')
                && let Some((_, number)) = table
                    .iter()
                    .find(|(name, _)| *name == pattern[index + 3..index + 3 + end])
            {
                out.push('\\');
                out.push_str(&number.to_string());
                index += 4 + end;
                continue;
            }
            let width = 1 + pattern[index + 1..]
                .chars()
                .next()
                .map_or(0, char::len_utf8);
            out.push_str(&pattern[index..index + width]);
            index += width;
            continue;
        }
        if bytes[index] == b'[' {
            let end = skip_class(bytes, index).min(pattern.len());
            out.push_str(&pattern[index..end]);
            index = end;
            continue;
        }
        if bytes[index] == b'('
            && bytes.get(index + 1) == Some(&b'?')
            && bytes.get(index + 2) == Some(&b'<')
            && !matches!(bytes.get(index + 3), Some(b'=' | b'!'))
            && let Some(end) = bytes[index + 3..].iter().position(|byte| *byte == b'>')
        {
            out.push('(');
            index += 4 + end;
            continue;
        }
        let width = pattern[index..].chars().next().map_or(1, char::len_utf8);
        out.push_str(&pattern[index..index + width]);
        index += width;
    }
    out
}

fn has_numeric_backref(pattern: &str, groups: usize) -> bool {
    let bytes = pattern.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => {
                if bytes.get(index + 1) == Some(&b'Q') {
                    index = find_quote_end(bytes, index + 2).saturating_sub(1);
                } else if let Some(first) = bytes.get(index + 1)
                    && matches!(first, b'1'..=b'9')
                {
                    let mut value = (first - b'0') as usize;
                    if bytes.get(index + 2).is_some_and(u8::is_ascii_digit) {
                        value = value * 10 + (bytes[index + 2] - b'0') as usize;
                    }
                    if value <= groups {
                        return true;
                    }
                    index += 1;
                } else {
                    index += 1;
                }
            }
            b'[' => index = skip_class(bytes, index).saturating_sub(1),
            _ => {}
        }
        index += 1;
    }
    false
}

pub(crate) enum PlanError {
    InteriorBackref,
}

pub(crate) fn plan_lookbehind(pattern: &str) -> Option<Result<LookbehindPlan, PlanError>> {
    let spans = find_lookbehinds(pattern);
    if spans.is_empty() {
        return None;
    }
    let mut plan = LookbehindPlan {
        skeleton: String::with_capacity(pattern.len()),
        assertions: Vec::new(),
        group_map: vec![GroupTarget::Skeleton(0)],
        named: Vec::new(),
    };
    let mut java_groups = 0usize;
    let mut skeleton_groups = 0usize;
    let mut cursor = 0usize;
    for (assertion, span) in spans.iter().enumerate() {
        if copy_region(
            pattern,
            &mut plan,
            cursor,
            span.start,
            &mut java_groups,
            &mut skeleton_groups,
        )
        .is_err()
        {
            return Some(Err(PlanError::InteriorBackref));
        }
        cursor = span.close + 1;
        skeleton_groups += 1;
        let marker = skeleton_groups;
        plan.skeleton.push_str("()");
        let body = pattern[span.start + 4..span.close].to_owned();
        let mut local = 0usize;
        count_body_groups(&body, &mut java_groups, &mut local, &mut plan, assertion);
        plan.assertions.push(AssertionPlan {
            marker,
            negative: span.negative,
            max_len: lookbehind_max_len(&body),
            body,
        });
    }
    if copy_region(
        pattern,
        &mut plan,
        cursor,
        pattern.len(),
        &mut java_groups,
        &mut skeleton_groups,
    )
    .is_err()
    {
        return Some(Err(PlanError::InteriorBackref));
    }
    Some(Ok(plan))
}

fn count_body_groups(
    body: &str,
    java_groups: &mut usize,
    local: &mut usize,
    plan: &mut LookbehindPlan,
    assertion: usize,
) {
    let bytes = body.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => {
                if bytes.get(index + 1) == Some(&b'Q') {
                    index = find_quote_end(bytes, index + 2).saturating_sub(1);
                } else {
                    index += 1;
                }
            }
            b'[' => index = skip_class(bytes, index).saturating_sub(1),
            b'(' if group_kind(bytes, index) == GroupKind::Consuming => {
                *java_groups += 1;
                *local += 1;
                plan.group_map.push(GroupTarget::Lookbehind {
                    assertion,
                    local: *local,
                });
                if bytes.get(index + 1) == Some(&b'?')
                    && bytes.get(index + 2) == Some(&b'<')
                    && !matches!(bytes.get(index + 3), Some(b'=' | b'!'))
                    && let Some(end) = bytes[index + 3..].iter().position(|byte| *byte == b'>')
                {
                    plan.named
                        .push((body[index + 3..index + 3 + end].to_owned(), *java_groups));
                }
            }
            _ => {}
        }
        index += 1;
    }
}

fn copy_region(
    pattern: &str,
    plan: &mut LookbehindPlan,
    from: usize,
    to: usize,
    java_groups: &mut usize,
    skeleton_groups: &mut usize,
) -> Result<(), PlanError> {
    let bytes = pattern.as_bytes();
    let mut index = from;
    while index < to {
        match bytes[index] {
            b'\\' => {
                if bytes.get(index + 1) == Some(&b'Q') {
                    let end = find_quote_end(bytes, index + 2).min(to);
                    plan.skeleton.push_str(&pattern[index..end]);
                    index = end;
                    continue;
                }
                if let Some(first) = bytes.get(index + 1)
                    && matches!(first, b'1'..=b'9')
                {
                    let mut value = (first - b'0') as usize;
                    let mut width = 2;
                    if index + 2 < to && bytes.get(index + 2).is_some_and(u8::is_ascii_digit) {
                        value = value * 10 + (bytes[index + 2] - b'0') as usize;
                        width = 3;
                    }
                    let Some(target) = plan.group_map.get(value).copied() else {
                        return Err(PlanError::InteriorBackref);
                    };
                    match target {
                        GroupTarget::Skeleton(skeleton) => {
                            plan.skeleton.push('\\');
                            plan.skeleton.push_str(&skeleton.to_string());
                        }
                        GroupTarget::Lookbehind { .. } => {
                            return Err(PlanError::InteriorBackref);
                        }
                    }
                    index += width;
                    continue;
                }
                let width = 1 + pattern[index + 1..]
                    .chars()
                    .next()
                    .map_or(0, char::len_utf8);
                plan.skeleton.push_str(&pattern[index..index + width]);
                index += width;
                continue;
            }
            b'[' => {
                let end = skip_class(bytes, index).min(to);
                plan.skeleton.push_str(&pattern[index..end]);
                index = end;
                continue;
            }
            b'(' => {
                if group_kind(bytes, index) == GroupKind::Consuming {
                    *java_groups += 1;
                    *skeleton_groups += 1;
                    plan.group_map.push(GroupTarget::Skeleton(*skeleton_groups));
                    if bytes.get(index + 1) == Some(&b'?')
                        && bytes.get(index + 2) == Some(&b'<')
                        && !matches!(bytes.get(index + 3), Some(b'=' | b'!'))
                        && let Some(end) = bytes[index + 3..to.min(bytes.len())]
                            .iter()
                            .position(|byte| *byte == b'>')
                    {
                        plan.named
                            .push((pattern[index + 3..index + 3 + end].to_owned(), *java_groups));
                    }
                }
                let width = pattern[index..].chars().next().map_or(1, char::len_utf8);
                plan.skeleton.push_str(&pattern[index..index + width]);
                index += width;
                continue;
            }
            _ => {}
        }
        let width = pattern[index..].chars().next().map_or(1, char::len_utf8);
        plan.skeleton.push_str(&pattern[index..index + width]);
        index += width;
    }
    Ok(())
}

pub(crate) fn lookbehind_max_len(body: &str) -> Option<usize> {
    let bytes = body.as_bytes();
    let (max, _) = alt_max_len(bytes, 0)?;
    Some(max)
}

fn alt_max_len(bytes: &[u8], index: usize) -> Option<(usize, usize)> {
    let mut best = 0usize;
    let mut index = index;
    loop {
        let (branch, next) = concat_max_len(bytes, index)?;
        best = best.max(branch);
        index = next;
        if bytes.get(index) == Some(&b'|') {
            index += 1;
            continue;
        }
        return Some((best, index));
    }
}

fn concat_max_len(bytes: &[u8], index: usize) -> Option<(usize, usize)> {
    let mut total = 0usize;
    let mut index = index;
    loop {
        if index >= bytes.len() {
            return Some((total, index));
        }
        if matches!(bytes[index], b'|' | b')') {
            return Some((total, index));
        }
        if matches!(bytes[index], b'^' | b'$') {
            index += 1;
            continue;
        }
        if matches!(bytes[index], b'*' | b'+' | b'?' | b'{' | b'}') {
            return Some((total, index));
        }
        let (atom, next) = atom_max_len(bytes, index)?;
        let (repeated, after) = apply_repetition(bytes, next, atom)?;
        total = total.checked_add(repeated)?;
        index = after;
    }
}

fn atom_max_len(bytes: &[u8], index: usize) -> Option<(usize, usize)> {
    match bytes[index] {
        b'\\' => {
            if bytes.get(index + 1) == Some(&b'Q') {
                let end = find_quote_end(bytes, index + 2);
                Some((end.saturating_sub(index + 2), end))
            } else {
                match bytes.get(index + 1) {
                    Some(b'b' | b'B') => Some((0, index + 2)),
                    Some(b'k') => None,
                    Some(byte) if byte.is_ascii_digit() => None,
                    _ => Some((4, index + 2)),
                }
            }
        }
        b'[' => Some((4, skip_class(bytes, index))),
        b'(' => {
            let close = match_group(bytes, index)?;
            let inner = if matches!(
                group_kind(bytes, index),
                GroupKind::Lookahead | GroupKind::Lookbehind
            ) {
                0
            } else {
                let (start, end) = group_body_range(bytes, index, close)?;
                alt_max_len(&bytes[start..end.min(bytes.len())], 0)?.0
            };
            Some((inner, close + 1))
        }
        _ => {
            let width = char_width(bytes, index);
            Some((width, index + width))
        }
    }
}

fn apply_repetition(bytes: &[u8], index: usize, inner: usize) -> Option<(usize, usize)> {
    match bytes.get(index) {
        Some(b'*' | b'+') => None,
        Some(b'?') => {
            let mut next = index + 1;
            if matches!(bytes.get(next), Some(b'?' | b'+')) {
                next += 1;
            }
            Some((inner, next))
        }
        Some(b'{') => {
            let mut cursor = index + 1;
            while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
                cursor += 1;
            }
            if cursor == index + 1 {
                return None;
            }
            if bytes.get(cursor) == Some(&b',') {
                cursor += 1;
                let digits = cursor;
                while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
                    cursor += 1;
                }
                if bytes.get(cursor) != Some(&b'}') {
                    return None;
                }
                let mut next = cursor + 1;
                if matches!(bytes.get(next), Some(b'?' | b'+')) {
                    next += 1;
                }
                if cursor == digits {
                    return None;
                }
                let max: usize = core::str::from_utf8(&bytes[digits..cursor])
                    .ok()?
                    .parse()
                    .ok()?;
                Some((inner.checked_mul(max)?, next))
            } else {
                let count: usize = core::str::from_utf8(&bytes[index + 1..cursor])
                    .ok()?
                    .parse()
                    .ok()?;
                if bytes.get(cursor) != Some(&b'}') {
                    return None;
                }
                let mut next = cursor + 1;
                if matches!(bytes.get(next), Some(b'?' | b'+')) {
                    next += 1;
                }
                Some((inner.checked_mul(count)?, next))
            }
        }
        _ => Some((inner, index)),
    }
}

fn group_body_range(bytes: &[u8], open: usize, close: usize) -> Option<(usize, usize)> {
    if bytes.get(open + 1) != Some(&b'?') {
        return Some((open + 1, close));
    }
    match bytes.get(open + 2) {
        Some(b':' | b'=' | b'!' | b'>') => Some((open + 3, close)),
        Some(quote) if *quote == b'<' || *quote == b'\'' => {
            let term = if *quote == b'<' { b'>' } else { b'\'' };
            let mut index = open + 3;
            while index < close && bytes[index] != term {
                index += 1;
            }
            if index >= close {
                return None;
            }
            Some((index + 1, close))
        }
        _ => {
            let mut index = open + 2;
            while index < close && (bytes[index].is_ascii_lowercase() || bytes[index] == b'-') {
                index += 1;
            }
            if bytes.get(index) == Some(&b':') {
                return Some((index + 1, close));
            }
            if index == close {
                return Some((close, close));
            }
            None
        }
    }
}

fn char_width(bytes: &[u8], index: usize) -> usize {
    let slice = &bytes[index..];
    let text = core::str::from_utf8(slice).unwrap_or("");
    text.chars()
        .next()
        .map_or(1, |first| first.len_utf8().min(slice.len()))
}

pub(crate) const LOOKBEHIND_SEARCH_BUDGET: usize = 1_000_000;

pub(crate) struct SearchBudget {
    remaining: usize,
}

impl SearchBudget {
    pub(crate) fn budget() -> Self {
        Self {
            remaining: LOOKBEHIND_SEARCH_BUDGET,
        }
    }

    pub(crate) fn take(&mut self, java_pattern: &str) -> Result<()> {
        if self.remaining == 0 {
            return Err(crate::spark_regex_engine::overrun_error(
                java_pattern,
                "lookbehind search budget",
                LOOKBEHIND_SEARCH_BUDGET,
            ));
        }
        self.remaining -= 1;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Assertion {
    pub(crate) marker: usize,
    pub(crate) negative: bool,
    pub(crate) max_len: Option<usize>,
    pub(crate) body: crate::spark_regex_engine::SparkRegex,
}

#[derive(Clone, Debug)]
pub(crate) struct LookbehindCompiled {
    pub(crate) skeleton: Box<crate::spark_regex_engine::SparkRegex>,
    pub(crate) assertions: Vec<Assertion>,
    pub(crate) group_map: Vec<GroupTarget>,
    pub(crate) named: Vec<(String, usize)>,
}

pub(crate) struct VerifiedMatch {
    pub(crate) span: (usize, usize),
    pub(crate) skel: Spans,
    pub(crate) interior: Vec<Spans>,
}

impl LookbehindCompiled {
    pub(crate) fn verified_from(
        &self,
        text: &str,
        start: usize,
        budget: &mut SearchBudget,
    ) -> Result<Option<VerifiedMatch>> {
        let mut byte = start;
        loop {
            if byte > text.len() {
                return Ok(None);
            }
            let Some((found_start, found_end)) = self.skeleton.find_from(text, byte)? else {
                return Ok(None);
            };
            if let Some(matched) = self.verify(text, found_start, found_end, budget)? {
                return Ok(Some(matched));
            }
            if found_start == found_end {
                if found_start == text.len() {
                    return Ok(None);
                }
                let Some(character) = text[found_start..].chars().next() else {
                    return Ok(None);
                };
                byte = found_start + character.len_utf8();
            } else {
                byte = found_end;
            }
        }
    }

    fn verify(
        &self,
        text: &str,
        found_start: usize,
        found_end: usize,
        budget: &mut SearchBudget,
    ) -> Result<Option<VerifiedMatch>> {
        let skel = self.skeleton.captures_spans(text, found_start)?;
        let mut interior = Vec::with_capacity(self.assertions.len());
        for assertion in &self.assertions {
            let position = skel
                .get(assertion.marker)
                .copied()
                .flatten()
                .map(|(start, _)| start);
            let Some(position) = position else {
                interior.push(Vec::new());
                continue;
            };
            match Self::check_assertion(assertion, text, position, budget)? {
                Some(spans) => interior.push(spans),
                None => return Ok(None),
            }
        }
        Ok(Some(VerifiedMatch {
            span: (found_start, found_end),
            skel,
            interior,
        }))
    }

    fn check_assertion(
        assertion: &Assertion,
        text: &str,
        position: usize,
        budget: &mut SearchBudget,
    ) -> Result<Option<Spans>> {
        if !text.is_char_boundary(position) {
            return Ok(if assertion.negative {
                Some(Vec::new())
            } else {
                None
            });
        }
        let mut low = match assertion.max_len {
            Some(max) => position.saturating_sub(max),
            None => 0,
        };
        while low < position && !text.is_char_boundary(low) {
            low += 1;
        }
        let prefix = &text[..position];
        let mut start = position;
        loop {
            if let Some(spans) = assertion
                .body
                .match_exact_spans(prefix, start, position, budget)?
            {
                return Ok(if assertion.negative {
                    None
                } else {
                    Some(spans)
                });
            }
            if start <= low {
                break;
            }
            start -= 1;
            while start > low && !text.is_char_boundary(start) {
                start -= 1;
            }
        }
        Ok(if assertion.negative {
            Some(Vec::new())
        } else {
            None
        })
    }

    pub(crate) fn java_spans(&self, skel: &[Option<(usize, usize)>]) -> Spans {
        let mut java = vec![None; self.group_map.len()];
        if !java.is_empty()
            && let Some(first) = skel.first().copied()
        {
            java[0] = first;
        }
        for (java_index, target) in self.group_map.iter().enumerate().skip(1) {
            if let GroupTarget::Skeleton(skeleton) = target
                && let Some(span) = skel.get(*skeleton).copied().flatten()
            {
                java[java_index] = Some(span);
            }
        }
        java
    }

    pub(crate) fn capture_java(
        &self,
        matched: &VerifiedMatch,
        group: usize,
        text: &str,
    ) -> Option<String> {
        let (start, end) = match self
            .group_map
            .get(group)
            .copied()
            .unwrap_or(GroupTarget::Skeleton(group))
        {
            GroupTarget::Skeleton(skeleton) => matched.skel.get(skeleton).copied().flatten()?,
            GroupTarget::Lookbehind { assertion, local } => matched
                .interior
                .get(assertion)?
                .get(local)
                .copied()
                .flatten()?,
        };
        text.get(start..end).map(str::to_owned)
    }

    fn lookup_num(&self, number: usize, matched: &VerifiedMatch, text: &str) -> Option<String> {
        if number == 0 {
            return text.get(matched.span.0..matched.span.1).map(str::to_owned);
        }
        self.capture_java(matched, number, text)
    }

    fn lookup_template(&self, id: &str, matched: &VerifiedMatch, text: &str) -> Option<String> {
        if let Some((_, number)) = self.named.iter().find(|(name, _)| name == id) {
            return self.lookup_num(*number, matched, text);
        }
        if let Ok(number) = id.parse::<usize>() {
            return self.lookup_num(number, matched, text);
        }
        None
    }

    pub(crate) fn expand_verified(
        &self,
        out: &mut String,
        matched: &VerifiedMatch,
        template: &str,
        text: &str,
    ) {
        let mut rest = template;
        while let Some(dollar) = rest.find('$') {
            out.push_str(&rest[..dollar]);
            let tail = &rest[dollar + 1..];
            if let Some(after) = tail.strip_prefix('$') {
                out.push('$');
                rest = after;
                continue;
            }
            if let Some((id, skip)) = parse_braced_id(tail).or_else(|| parse_bare_id(tail)) {
                if let Some(value) = self.lookup_template(id, matched, text) {
                    out.push_str(&value);
                }
                rest = &tail[skip..];
                continue;
            }
            if let Some((skip, number)) = parse_decimal_prefix(tail) {
                if let Some(value) = self.lookup_num(number, matched, text) {
                    out.push_str(&value);
                }
                rest = &tail[skip..];
                continue;
            }
            out.push('$');
            rest = tail;
        }
        out.push_str(rest);
    }
}

fn parse_braced_id(tail: &str) -> Option<(&str, usize)> {
    let braced = tail.strip_prefix('{')?;
    let close = braced.find('}')?;
    let id = &braced[..close];
    if id.is_empty() || !id.chars().all(is_template_id_char) {
        return None;
    }
    Some((id, close + 2))
}

fn parse_bare_id(tail: &str) -> Option<(&str, usize)> {
    let length = tail
        .char_indices()
        .take_while(|(_, character)| is_template_id_char(*character))
        .map(|(offset, character)| offset + character.len_utf8())
        .last()?;
    Some((&tail[..length], length))
}

fn parse_decimal_prefix(tail: &str) -> Option<(usize, usize)> {
    let mut end = 0;
    while tail.as_bytes().get(end).is_some_and(u8::is_ascii_digit) {
        end += 1;
    }
    tail[..end]
        .parse::<usize>()
        .ok()
        .map(|number| (end, number))
}

fn is_template_id_char(character: char) -> bool {
    character.is_alphanumeric() || character == '_'
}
