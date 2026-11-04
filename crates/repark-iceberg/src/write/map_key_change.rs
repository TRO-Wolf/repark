use iceberg::spec::{NestedField, Schema, StructType, Type, is_promotion_allowed};

use super::alter::ColumnPosition;
use super::column_move::{nested_name_known_ci, split_dotted};
use super::nested_column::{
    ColumnPathChange, MapKeyTouch, NestedTypeRefusal, ResolvedColumnPath,
    column_paths_commit_refusal, resolve_nested_path,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum KeyStep {
    Update,
    Add,
    Alter,
}

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

fn map_key_steps_refusal(
    schema: &Schema,
    steps: Vec<(Vec<String>, KeyStep)>,
) -> Option<NestedTypeRefusal> {
    let mut touched: Vec<(KeyStep, ResolvedColumnPath)> = steps
        .into_iter()
        .filter_map(|(path, step)| {
            let resolved = resolve_nested_path(schema, &path).ok()?;
            let (rank, touch) = match (resolved.map_key?, step) {
                (MapKeyTouch::Alter(map), _) => (KeyStep::Alter, MapKeyTouch::Alter(map)),
                (MapKeyTouch::Update(map), KeyStep::Add)
                    if matches!(map.key_field.field_type.as_ref(), Type::Struct(_)) =>
                {
                    (KeyStep::Add, MapKeyTouch::AddFields(map))
                }
                (MapKeyTouch::Update(map), KeyStep::Update) => {
                    (KeyStep::Update, MapKeyTouch::Update(map))
                }
                _ => return None,
            };
            Some((
                rank,
                ResolvedColumnPath::under_map_key(resolved.names, touch),
            ))
        })
        .collect();
    touched.sort_by_key(|(rank, _)| *rank);
    let paths: Vec<ResolvedColumnPath> = touched.into_iter().map(|(_, path)| path).collect();
    column_paths_commit_refusal(schema, &paths)
}

#[must_use]
pub fn column_path_changes_map_key_refusal(
    schema: &Schema,
    changes: &[ColumnPathChange],
    case_sensitive: bool,
) -> Option<NestedTypeRefusal> {
    let steps = changes
        .iter()
        .filter_map(|change| {
            let (path, step) = match change {
                ColumnPathChange::Add {
                    parent: Some(parent),
                    ..
                } => (parent, KeyStep::Add),
                ColumnPathChange::Add { parent: None, .. } => return None,
                ColumnPathChange::Rename { path, .. } | ColumnPathChange::Drop { path } => {
                    (path, KeyStep::Alter)
                }
            };
            known_path(schema, path, case_sensitive)
                .then(|| (path_parts(path, case_sensitive), step))
        })
        .collect();
    map_key_steps_refusal(schema, steps)
}

fn child_path(path: &[String], name: &str) -> Vec<String> {
    let mut child = path.to_vec();
    child.push(name.to_string());
    child
}

fn union_struct_steps(
    current: &StructType,
    incoming: &StructType,
    path: &[String],
    steps: &mut Vec<(Vec<String>, KeyStep)>,
) {
    for field in incoming.fields() {
        let existing = current
            .fields()
            .iter()
            .find(|existing| existing.name.eq_ignore_ascii_case(&field.name));
        match existing {
            Some(existing) => {
                union_member_steps(existing, field, &child_path(path, &existing.name), steps);
            }
            None => steps.push((path.to_vec(), KeyStep::Add)),
        }
    }
}

fn union_member_steps(
    existing: &NestedField,
    incoming: &NestedField,
    path: &[String],
    steps: &mut Vec<(Vec<String>, KeyStep)>,
) {
    let widened = match (existing.field_type.as_ref(), incoming.field_type.as_ref()) {
        (Type::Primitive(from), Type::Primitive(to)) => {
            from != to && is_promotion_allowed(&existing.field_type, to)
        }
        _ => false,
    };
    if widened || (existing.required && !incoming.required) {
        steps.push((path.to_vec(), KeyStep::Update));
    }
    match (existing.field_type.as_ref(), incoming.field_type.as_ref()) {
        (Type::Struct(current), Type::Struct(incoming)) => {
            union_struct_steps(current, incoming, path, steps);
        }
        (Type::List(current), Type::List(incoming)) => union_member_steps(
            &current.element_field,
            &incoming.element_field,
            &child_path(path, "element"),
            steps,
        ),
        (Type::Map(current), Type::Map(incoming)) => {
            union_member_steps(
                &current.key_field,
                &incoming.key_field,
                &child_path(path, "key"),
                steps,
            );
            union_member_steps(
                &current.value_field,
                &incoming.value_field,
                &child_path(path, "value"),
                steps,
            );
        }
        _ => {}
    }
}

#[must_use]
pub fn union_map_key_refusal(current: &Schema, incoming: &Schema) -> Option<NestedTypeRefusal> {
    let mut steps = Vec::new();
    union_struct_steps(current.as_struct(), incoming.as_struct(), &[], &mut steps);
    map_key_steps_refusal(current, steps)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MapKeyMove {
    Applies,
    KeyStructUnchanged,
    Refused(NestedTypeRefusal),
}

fn moves_after_itself(schema: &Schema, names: &[String], position: &ColumnPosition) -> bool {
    let ColumnPosition::After(reference) = position else {
        return false;
    };
    resolve_nested_path(schema, &path_parts(reference, false))
        .is_ok_and(|reference| reference.names == names)
}

#[must_use]
pub fn map_key_move(schema: &Schema, name: &str, position: &ColumnPosition) -> MapKeyMove {
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
    if !parent_is_key {
        MapKeyMove::Refused(touch.refusal())
    } else if moves_after_itself(schema, &resolved.names, position) {
        MapKeyMove::Applies
    } else {
        MapKeyMove::KeyStructUnchanged
    }
}
