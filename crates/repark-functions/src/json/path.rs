use super::reader::{JsonValue, json_number_text, write_compact, write_escaped};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PathStep {
    Named(String),
    Index(usize),
    Wildcard,
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

pub(crate) fn evaluate_path(
    value: &JsonValue<'_>,
    steps: &[PathStep],
    top: bool,
) -> Option<PathOutput> {
    let Some(step) = steps.first() else {
        return render_value(value, top);
    };
    let rest = &steps[1..];
    match step {
        PathStep::Named(name) => match value {
            JsonValue::Object(entries) => {
                let child = entries.iter().find(|(key, _)| key == name)?;
                evaluate_path(&child.1, rest, top)
            }
            _ => None,
        },
        PathStep::Index(index) => match value {
            JsonValue::Array(items) => evaluate_path(items.get(*index)?, rest, top),
            _ => None,
        },
        PathStep::Wildcard => evaluate_wildcard(value, rest, top),
    }
}

fn evaluate_wildcard(value: &JsonValue<'_>, rest: &[PathStep], top: bool) -> Option<PathOutput> {
    if matches!(rest.first(), Some(PathStep::Index(_) | PathStep::Wildcard)) {
        let JsonValue::Array(items) = value else {
            return None;
        };
        let mut collected = Vec::new();
        for item in items {
            collect_flattened(item, rest, &mut collected);
        }
        return wrap_outputs(&collected);
    }
    let JsonValue::Array(items) = value else {
        return None;
    };
    let mut collected = Vec::new();
    for item in items {
        if let Some(found) = evaluate_path(item, rest, false) {
            collected.push(found);
        }
    }
    if top && collected.len() == 1 {
        return collected.pop();
    }
    wrap_outputs(&collected)
}

fn wrap_outputs(collected: &[PathOutput]) -> Option<PathOutput> {
    if collected.is_empty() {
        return None;
    }
    let mut json = String::from("[");
    for (index, item) in collected.iter().enumerate() {
        if index > 0 {
            json.push(',');
        }
        json.push_str(&item.json);
    }
    json.push(']');
    Some(PathOutput { json, plain: None })
}

fn collect_flattened(value: &JsonValue<'_>, steps: &[PathStep], out: &mut Vec<PathOutput>) {
    let Some(step) = steps.first() else {
        if let Some(found) = render_value(value, false) {
            out.push(found);
        }
        return;
    };
    let rest = &steps[1..];
    match step {
        PathStep::Wildcard => match value {
            JsonValue::Array(items) => {
                for item in items {
                    collect_flattened(item, rest, out);
                }
            }
            other => collect_flattened(other, rest, out),
        },
        PathStep::Index(index) => {
            if let JsonValue::Array(items) = value
                && let Some(item) = items.get(*index)
            {
                collect_flattened(item, rest, out);
            }
        }
        PathStep::Named(name) => {
            if let JsonValue::Object(entries) = value
                && let Some(child) = entries.iter().find(|(key, _)| key == name)
            {
                collect_flattened(&child.1, rest, out);
            }
        }
    }
}

fn render_value(value: &JsonValue<'_>, top: bool) -> Option<PathOutput> {
    match value {
        JsonValue::Null => {
            if top {
                None
            } else {
                Some(PathOutput {
                    json: "null".to_string(),
                    plain: None,
                })
            }
        }
        JsonValue::Text(text) => {
            let mut json = String::new();
            write_escaped(text, &mut json);
            Some(PathOutput {
                json,
                plain: if top { Some(text.to_string()) } else { None },
            })
        }
        JsonValue::Number(raw) => Some(PathOutput {
            json: json_number_text(raw),
            plain: None,
        }),
        JsonValue::Bool(flag) => Some(PathOutput {
            json: if *flag { "true" } else { "false" }.to_string(),
            plain: None,
        }),
        other => {
            let mut json = String::new();
            write_compact(other, &mut json);
            Some(PathOutput { json, plain: None })
        }
    }
}
