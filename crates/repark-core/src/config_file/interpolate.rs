use repark_common::{Error, Result};

use super::EnvironmentLookup;

#[allow(dead_code)]
pub(crate) fn interpolate_table(
    table: &toml::Table,
    environment: EnvironmentLookup<'_>,
) -> Result<toml::Table> {
    let mut interpolated = toml::Table::new();
    for (key, value) in table {
        interpolated.insert(key.clone(), interpolate_value(key, value, environment)?);
    }
    Ok(interpolated)
}

#[allow(dead_code)]
fn interpolate_value(
    path: &str,
    value: &toml::Value,
    environment: EnvironmentLookup<'_>,
) -> Result<toml::Value> {
    match value {
        toml::Value::String(text) => Ok(toml::Value::String(interpolate_string(
            path,
            text,
            environment,
        )?)),
        toml::Value::Table(inner) => {
            let mut interpolated = toml::Table::new();
            for (key, value) in inner {
                let child_path = join_path(path, key);
                interpolated.insert(
                    key.clone(),
                    interpolate_value(&child_path, value, environment)?,
                );
            }
            Ok(toml::Value::Table(interpolated))
        }
        toml::Value::Array(items) => {
            let mut interpolated = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                let child_path = format!("{path}[{index}]");
                interpolated.push(interpolate_value(&child_path, item, environment)?);
            }
            Ok(toml::Value::Array(interpolated))
        }
        toml::Value::Integer(_)
        | toml::Value::Float(_)
        | toml::Value::Boolean(_)
        | toml::Value::Datetime(_) => Ok(value.clone()),
    }
}

fn join_path(parent: &str, key: &str) -> String {
    if parent.is_empty() {
        key.to_string()
    } else {
        format!("{parent}.{key}")
    }
}

#[allow(dead_code)]
fn interpolate_string(
    path: &str,
    text: &str,
    environment: EnvironmentLookup<'_>,
) -> Result<String> {
    let mut expanded = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(position) = rest.find('$') {
        expanded.push_str(&rest[..position]);
        let tail = &rest[position + 1..];
        if let Some(reference) = tail.strip_prefix('{') {
            let Some(end) = reference.find('}') else {
                expanded.push_str("${");
                rest = reference;
                continue;
            };
            let variable = &reference[..end];
            if variable.is_empty() {
                return Err(Error::Config(format!(
                    "empty variable name at key `{path}`"
                )));
            }
            let value = environment(variable).ok_or_else(|| {
                Error::Config(format!(
                    "missing environment variable `{variable}` for key `{path}`"
                ))
            })?;
            expanded.push_str(&value);
            rest = &reference[end + 1..];
        } else {
            expanded.push('$');
            rest = tail;
        }
    }
    expanded.push_str(rest);
    Ok(expanded)
}
