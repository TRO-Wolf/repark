use super::reader::{JsonValue, json_number_text, write_compact, write_escaped};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PathStep {
    Named(String),
    Index(usize),
    Wildcard,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Style {
    Raw,
    New,
    Flatten,
}

pub(crate) struct PathOutput {
    pub json: String,
    pub plain: Option<String>,
}

pub(crate) fn parse_path(path: &str) -> Option<Vec<PathStep>> {
    let mut rest = path.strip_prefix('$')?;
    let mut steps = Vec::new();
    while !rest.is_empty() {
        if let Some(after_dot) = rest.strip_prefix('.') {
            let end = after_dot.find(['.', '[']).unwrap_or(after_dot.len());
            if end == 0 {
                return None;
            }
            let name = &after_dot[..end];
            steps.push(if name == "*" {
                PathStep::Wildcard
            } else {
                PathStep::Named(name.to_string())
            });
            rest = &after_dot[end..];
            continue;
        }
        let after_bracket = rest.strip_prefix('[')?;
        if let Some(quoted) = after_bracket.strip_prefix('\'') {
            let end = quoted.find('\'')?;
            let name = &quoted[..end];
            if name.is_empty() {
                return None;
            }
            steps.push(PathStep::Named(name.to_string()));
            rest = quoted[end + 1..].strip_prefix(']')?;
            continue;
        }
        let end = after_bracket.find(']')?;
        let token = &after_bracket[..end];
        if token == "*" {
            steps.push(PathStep::Wildcard);
        } else {
            steps.push(PathStep::Index(token.parse::<usize>().ok()?));
        }
        rest = &after_bracket[end + 1..];
    }
    Some(steps)
}

pub(crate) fn evaluate_path(value: &JsonValue<'_>, steps: &[PathStep]) -> Option<PathOutput> {
    if steps.is_empty() && matches!(value, JsonValue::Null) {
        return Some(PathOutput {
            json: "null".to_string(),
            plain: None,
        });
    }
    let mut collected = evaluate(value, steps, Style::Raw);
    if collected.len() == 1 {
        return collected.pop();
    }
    if collected.is_empty() {
        return None;
    }
    Some(wrap(&collected))
}

fn evaluate(value: &JsonValue<'_>, steps: &[PathStep], style: Style) -> Vec<PathOutput> {
    let Some(step) = steps.first() else {
        return render_leaf(value, style);
    };
    let rest = &steps[1..];
    match step {
        PathStep::Named(name) => match value {
            JsonValue::Object(entries) => entries
                .iter()
                .find(|(key, _)| key == name)
                .map_or_else(Vec::new, |child| evaluate(&child.1, rest, style)),
            _ => Vec::new(),
        },
        PathStep::Index(index) => index_step(value, *index, rest, style),
        PathStep::Wildcard => wildcard_step(value, rest, style),
    }
}

fn index_step(
    value: &JsonValue<'_>,
    index: usize,
    rest: &[PathStep],
    style: Style,
) -> Vec<PathOutput> {
    let JsonValue::Array(items) = value else {
        return Vec::new();
    };
    let Some(item) = items.get(index) else {
        return Vec::new();
    };
    let next = if style == Style::Raw && matches!(rest.first(), Some(PathStep::Wildcard)) {
        Style::New
    } else {
        style
    };
    evaluate(item, rest, next)
}

fn wildcard_step(value: &JsonValue<'_>, rest: &[PathStep], style: Style) -> Vec<PathOutput> {
    let JsonValue::Array(items) = value else {
        return Vec::new();
    };
    if matches!(rest.first(), Some(PathStep::Wildcard)) {
        let tail = &rest[1..];
        let mut collected = Vec::new();
        for item in items {
            collected.extend(evaluate(item, tail, Style::Flatten));
        }
        return finish(collected, style, true);
    }
    let child = if style == Style::Flatten {
        Style::Flatten
    } else {
        Style::New
    };
    let mut collected = Vec::new();
    for item in items {
        collected.extend(evaluate(item, rest, child));
    }
    finish(collected, style, false)
}

fn finish(collected: Vec<PathOutput>, style: Style, always_array: bool) -> Vec<PathOutput> {
    if collected.is_empty() {
        return Vec::new();
    }
    match style {
        Style::Flatten => collected,
        Style::Raw if !always_array && collected.len() == 1 => collected,
        _ => vec![wrap(&collected)],
    }
}

fn wrap(collected: &[PathOutput]) -> PathOutput {
    let mut json = String::from("[");
    for (index, item) in collected.iter().enumerate() {
        if index > 0 {
            json.push(',');
        }
        json.push_str(&item.json);
    }
    json.push(']');
    PathOutput { json, plain: None }
}

fn render_leaf(value: &JsonValue<'_>, style: Style) -> Vec<PathOutput> {
    if style == Style::Flatten
        && let JsonValue::Array(items) = value
    {
        let mut collected = Vec::new();
        for item in items {
            collected.extend(render_leaf(item, Style::Flatten));
        }
        return collected;
    }
    let top = style == Style::Raw;
    match value {
        JsonValue::Null if top => Vec::new(),
        JsonValue::Null => vec![PathOutput {
            json: "null".to_string(),
            plain: None,
        }],
        JsonValue::Text(text) => {
            let mut json = String::new();
            write_escaped(text, &mut json);
            vec![PathOutput {
                json,
                plain: if top { Some(text.to_string()) } else { None },
            }]
        }
        JsonValue::Number(raw) => vec![PathOutput {
            json: json_number_text(raw),
            plain: None,
        }],
        JsonValue::NonFinite(_) => {
            let mut json = String::new();
            write_compact(value, &mut json);
            vec![PathOutput { json, plain: None }]
        }
        JsonValue::Bool(flag) => vec![PathOutput {
            json: if *flag { "true" } else { "false" }.to_string(),
            plain: None,
        }],
        other => {
            let mut json = String::new();
            write_compact(other, &mut json);
            vec![PathOutput { json, plain: None }]
        }
    }
}
