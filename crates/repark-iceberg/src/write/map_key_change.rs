use iceberg::spec::{Schema, Type};

use super::column_move::{nested_name_known_ci, split_dotted};
use super::nested_column::{
    ColumnPathChange, MapKeyTouch, NestedTypeRefusal, ResolvedColumnPath,
    column_paths_commit_refusal, resolve_nested_path,
};

fn known_path(schema: &Schema, path: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        schema.field_by_name(path).is_some()
    } else {
        nested_name_known_ci(schema, path).is_ok_and(|known| known)
    }
}

fn path_parts(path: &str, case_sensitive: bool) -> Vec<String> {
    split_dotted(path)
        .into_iter()
        .map(|part| {
            let container = ["key", "value", "element"]
                .iter()
                .any(|name| part.eq_ignore_ascii_case(name));
            if container && !case_sensitive {
                part.to_ascii_lowercase()
            } else {
                part
            }
        })
        .collect()
}

fn renamed_sibling(path: &str, to: &str) -> String {
    match path.rsplit_once('.') {
        Some((parent, _)) => format!("{parent}.{to}"),
        None => to.to_string(),
    }
}

#[must_use]
pub fn column_path_changes_map_key_refusal(
    schema: &Schema,
    changes: &[ColumnPathChange],
    case_sensitive: bool,
) -> Option<NestedTypeRefusal> {
    let mut adds = Vec::new();
    let mut alters = Vec::new();
    for change in changes {
        let (path, adding) = match change {
            ColumnPathChange::Add {
                parent: Some(parent),
                ..
            } => (parent, true),
            ColumnPathChange::Add { parent: None, .. } => continue,
            ColumnPathChange::Rename { path, to }
                if known_path(schema, &renamed_sibling(path, to), case_sensitive) =>
            {
                continue;
            }
            ColumnPathChange::Rename { path, .. } | ColumnPathChange::Drop { path } => {
                (path, false)
            }
        };
        if !known_path(schema, path, case_sensitive) {
            continue;
        }
        let Ok(resolved) = resolve_nested_path(schema, &path_parts(path, case_sensitive)) else {
            continue;
        };
        match resolved.map_key {
            Some(MapKeyTouch::Alter(map)) => {
                alters.push(ResolvedColumnPath::under_map_key(
                    resolved.names,
                    MapKeyTouch::Alter(map),
                ));
            }
            Some(MapKeyTouch::Update(map))
                if adding && matches!(map.key_field.field_type.as_ref(), Type::Struct(_)) =>
            {
                adds.push(ResolvedColumnPath::under_map_key(
                    resolved.names,
                    MapKeyTouch::AddFields(map),
                ));
            }
            _ => {}
        }
    }
    adds.extend(alters);
    column_paths_commit_refusal(schema, &adds)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MapKeyMove {
    Applies,
    KeyStructUnchanged,
    Refused(NestedTypeRefusal),
}

#[must_use]
pub fn map_key_move(schema: &Schema, name: &str) -> MapKeyMove {
    let path = path_parts(name, false);
    let Ok(resolved) = resolve_nested_path(schema, &path) else {
        return MapKeyMove::Applies;
    };
    let Some(touch @ MapKeyTouch::Alter(_)) = resolved.map_key else {
        return MapKeyMove::Applies;
    };
    let parent_is_key = path.split_last().is_some_and(|(_, parent)| {
        resolve_nested_path(schema, parent)
            .is_ok_and(|parent| matches!(parent.map_key, Some(MapKeyTouch::Update(_))))
    });
    if parent_is_key {
        MapKeyMove::KeyStructUnchanged
    } else {
        MapKeyMove::Refused(touch.refusal())
    }
}
