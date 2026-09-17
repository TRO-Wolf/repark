use iceberg::spec::{Schema, StructType, Type};

use super::alter::ColumnPosition;

#[expect(
    clippy::missing_errors_doc,
    reason = "round comment ban: the Err message shape is the Spark UNRESOLVED_COLUMN framing recorded in the unit ledger"
)]
pub fn check_column_move(
    schema: &Schema,
    name: &str,
    position: &ColumnPosition,
) -> std::result::Result<bool, String> {
    let top = schema.as_struct();
    let mover = resolve_move_field(top, name)
        .ok_or_else(|| unresolved_column(name, &sibling_names(top)))?;
    match position {
        ColumnPosition::First => {
            let order = sibling_ids(mover.parent);
            Ok(order.first() != Some(&mover.id))
        }
        ColumnPosition::After(reference) => {
            let target = resolve_move_field(top, reference)
                .ok_or_else(|| unresolved_column(reference, &sibling_names(mover.parent)))?;
            if target.id == mover.id || !std::ptr::eq(target.parent, mover.parent) {
                return Ok(true);
            }
            let mut order = sibling_ids(mover.parent);
            order.retain(|id| *id != mover.id);
            let at = order
                .iter()
                .position(|id| *id == target.id)
                .ok_or_else(|| unresolved_column(reference, &sibling_names(mover.parent)))?;
            order.insert(at + 1, mover.id);
            Ok(order != sibling_ids(mover.parent))
        }
    }
}

struct ResolvedMoveField<'a> {
    id: i32,
    parent: &'a StructType,
}

fn resolve_move_field<'a>(top: &'a StructType, path: &str) -> Option<ResolvedMoveField<'a>> {
    let mut scope = top;
    let mut segments = path.split('.').peekable();
    loop {
        let segment = segments.next()?;
        if segment.is_empty() {
            return None;
        }
        let needle = segment.to_ascii_lowercase();
        let found = scope
            .fields()
            .iter()
            .find(|existing| existing.name.to_ascii_lowercase() == needle)?;
        if segments.peek().is_none() {
            return Some(ResolvedMoveField {
                id: found.id,
                parent: scope,
            });
        }
        let Type::Struct(inner) = found.field_type.as_ref() else {
            return None;
        };
        scope = inner;
    }
}

fn sibling_names(parent: &StructType) -> Vec<String> {
    parent
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect()
}

fn sibling_ids(parent: &StructType) -> Vec<i32> {
    parent.fields().iter().map(|field| field.id).collect()
}

fn unresolved_column(name: &str, candidates: &[String]) -> String {
    if candidates.is_empty() {
        return format!(
            "[UNRESOLVED_COLUMN.WITHOUT_SUGGESTION] A column, variable, or function parameter \
             with name `{name}` cannot be resolved. SQLSTATE: 42703"
        );
    }
    let suggestions = candidates
        .iter()
        .map(|candidate| format!("`{candidate}`"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with \
         name `{name}` cannot be resolved. Did you mean one of the following? [{suggestions}]. \
         SQLSTATE: 42703"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use iceberg::spec::{NestedField, PrimitiveType};

    #[test]
    fn check_column_move_names_noops_and_nested() {
        use iceberg::spec::StructType;
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(
                    2,
                    "s",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(3, "a", Type::Primitive(PrimitiveType::Int)).into(),
                        NestedField::optional(4, "b", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])),
                )
                .into(),
                NestedField::optional(5, "tail", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();
        assert_eq!(
            check_column_move(&schema, "tail", &ColumnPosition::First),
            Ok(true)
        );
        assert_eq!(
            check_column_move(&schema, "ID", &ColumnPosition::First),
            Ok(false)
        );
        assert_eq!(
            check_column_move(&schema, "s.b", &ColumnPosition::First),
            Ok(true)
        );
        assert_eq!(
            check_column_move(&schema, "s.a", &ColumnPosition::First),
            Ok(false)
        );
        assert_eq!(
            check_column_move(&schema, "nope", &ColumnPosition::First),
            Err(unresolved_column(
                "nope",
                &["id".into(), "s".into(), "tail".into()]
            ))
        );
        assert_eq!(
            check_column_move(&schema, "id", &ColumnPosition::After("s.nope".into())),
            Err(unresolved_column(
                "s.nope",
                &["id".into(), "s".into(), "tail".into()]
            ))
        );
    }
}
