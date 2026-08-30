//! Plan-rewrite kernel for `dynamicFlatten`: structs first, then lists one at a time.

use std::collections::HashMap;
use std::fmt::Write;

use arrow::datatypes::{DataType, Fields};
use datafusion::common::{Column, ScalarValue, UnnestOptions};
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, cast, or, when};
use datafusion::prelude::{DataFrame, array_length, get_field, lit, make_array};

use crate::{Error, Result, engine_err};

/// Options for [`dynamic_flatten`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicFlattenOptions {
    /// Parent-path prefix separator for expanded struct fields.
    pub separator: String,
    /// Explode list columns one-at-a-time (list-of-struct becomes a same-name struct).
    pub explode_lists: bool,
    /// Drop `List(Null)` / `array<void>` columns instead of exploding them.
    pub drop_null_lists: bool,
    /// `true`: NULL and EMPTY lists become one null-element row.
    pub empty_as_null: bool,
    /// Rewrite-pass bound, not a row-cartesian or schema-width memory limiter.
    pub max_depth: usize,
}

impl Default for DynamicFlattenOptions {
    fn default() -> Self {
        Self {
            separator: "_".to_string(),
            explode_lists: true,
            drop_null_lists: true,
            empty_as_null: true,
            max_depth: 100,
        }
    }
}

/// One slot in an in-place struct-expansion projection (schema field order).
enum ProjectionSlot {
    Keep(String),
    Expand {
        parent: String,
        fields: Vec<ExpandedField>,
    },
}

/// One nested struct field, already prefix-claimed.
struct ExpandedField {
    nested_name: String,
    prefixed: String,
    data_type: DataType,
}

/// Recursively flatten nested structs (and optionally explode lists) as a plan rewrite.
/// # Errors
/// Returns [`Error::Analysis`] with a `DYNAMIC_FLATTEN_*` token.
pub fn dynamic_flatten(frame: DataFrame, options: DynamicFlattenOptions) -> Result<DataFrame> {
    let DynamicFlattenOptions {
        separator,
        explode_lists,
        drop_null_lists,
        empty_as_null,
        max_depth,
    } = options;

    let mut current = frame;
    let mut completed_cleanly = false;

    for _pass in 0..max_depth {
        if has_struct_columns(current.schema().fields()) {
            current = expand_structs(current, &separator)?;
            continue;
        }
        if !explode_lists {
            completed_cleanly = true;
            break;
        }
        if let Some(name) = first_list_view_column(current.schema().fields()) {
            return Err(list_view_refused(&name));
        }
        if !has_list_columns(current.schema().fields()) {
            completed_cleanly = true;
            break;
        }
        current = explode_lists_in_schema_order(current, drop_null_lists, empty_as_null)?;
    }

    if !completed_cleanly {
        let fields = current.schema().fields();
        let still_structs = has_struct_columns(fields);
        let still_lists = explode_lists && has_list_columns(fields);
        if still_structs || still_lists {
            return Err(Error::Analysis(format!(
                "[DYNAMIC_FLATTEN_MAX_DEPTH] dynamicFlatten exceeded max_depth={max_depth} \
                 with nested columns remaining (repark refuses silent truncation; raise \
                 max_depth or pre-flatten). Remaining schema: {}",
                format_fields(fields)
            )));
        }
    }
    if explode_lists && let Some(name) = first_list_view_column(current.schema().fields()) {
        return Err(list_view_refused(&name));
    }
    Ok(current)
}

/// Expand every top-level struct in place with parent-path prefixes (null-safe Project).
fn expand_structs(frame: DataFrame, separator: &str) -> Result<DataFrame> {
    let schema_fields: Vec<(String, DataType)> = frame
        .schema()
        .fields()
        .iter()
        .map(|field| (field.name().clone(), field.data_type().clone()))
        .collect();

    let mut claimed: HashMap<String, String> = HashMap::new();
    let mut slots: Vec<ProjectionSlot> = Vec::new();
    let mut any_expand_fields = false;

    for (name, data_type) in &schema_fields {
        match struct_fields(data_type) {
            None => {
                if let Some(existing) = claimed.get(name) {
                    return Err(top_level_collision(name, existing));
                }
                claimed.insert(name.clone(), format!("top-level column {name:?}"));
                slots.push(ProjectionSlot::Keep(name.clone()));
            }
            Some(nested_fields) => {
                let mut expanded = Vec::new();
                for nested in nested_fields {
                    let prefixed = format!("{name}{separator}{}", nested.name());
                    if let Some(existing) = claimed.get(&prefixed) {
                        return Err(prefixed_collision(name, nested.name(), &prefixed, existing));
                    }
                    claimed.insert(
                        prefixed.clone(),
                        format!("struct field {name:?}.{}", nested.name()),
                    );
                    expanded.push(ExpandedField {
                        nested_name: nested.name().clone(),
                        prefixed,
                        data_type: nested.data_type().clone(),
                    });
                    any_expand_fields = true;
                }
                slots.push(ProjectionSlot::Expand {
                    parent: name.clone(),
                    fields: expanded,
                });
            }
        }
    }

    let has_keep = slots
        .iter()
        .any(|slot| matches!(slot, ProjectionSlot::Keep(_)));
    if !has_keep && !any_expand_fields {
        return Err(Error::Analysis(
            "[DYNAMIC_FLATTEN_EMPTY_STRUCT] dynamicFlatten: schema is only empty struct \
             column(s) with no fields to expand."
                .to_string(),
        ));
    }

    let mut projection = Vec::new();
    for slot in slots {
        match slot {
            ProjectionSlot::Keep(name) => projection.push(project_as(&name)),
            ProjectionSlot::Expand { parent, fields } => {
                let parent_expr = unqualified_expr(&parent);
                for expanded in fields {
                    projection.push(null_safe_field(
                        parent_expr.clone(),
                        &expanded.nested_name,
                        &expanded.prefixed,
                        &expanded.data_type,
                    )?);
                }
            }
        }
    }
    frame.select(projection).map_err(engine_err)
}

/// Null-safe struct field extract: CASE WHEN parent IS NULL THEN <typed null> ELSE `get_field`.
fn null_safe_field(
    parent: Expr,
    nested_name: &str,
    prefixed: &str,
    field_type: &DataType,
) -> Result<Expr> {
    let typed_null = typed_null_literal(field_type)?;
    when(parent.clone().is_null(), typed_null)
        .otherwise(get_field(parent, nested_name))
        .map_err(engine_err)
        .map(|expr| expr.alias(prefixed))
}

/// Explode every list column in schema order (drop void lists when `drop_null_lists`).
fn explode_lists_in_schema_order(
    mut frame: DataFrame,
    drop_null_lists: bool,
    empty_as_null: bool,
) -> Result<DataFrame> {
    let list_columns: Vec<(String, DataType, DataType)> = frame
        .schema()
        .fields()
        .iter()
        .filter_map(|field| {
            list_element_type(field.data_type()).map(|element| {
                (
                    field.name().clone(),
                    field.data_type().clone(),
                    element.clone(),
                )
            })
        })
        .collect();

    for (name, column_type, element_type) in list_columns {
        if drop_null_lists && matches!(element_type, DataType::Null) {
            frame = drop_unqualified_column(frame, &name)?;
            continue;
        }
        if is_map_type(&element_type) {
            return Err(map_element_refused(&name));
        }
        frame = explode_one_list(frame, &name, empty_as_null, &column_type, &element_type)?;
    }
    Ok(frame)
}

/// Rewrite one list column, then unnest it in place with unqualified binding.
fn explode_one_list(
    frame: DataFrame,
    name: &str,
    empty_as_null: bool,
    column_type: &DataType,
    element_type: &DataType,
) -> Result<DataFrame> {
    // Dictionary<_, List> is a list for the walk, but DataFusion Unnest rejects Dictionary.
    let column = list_column_as_list(name, column_type);
    let singleton_null_list = make_array(vec![typed_null_literal(element_type)?]);
    let rewritten = if empty_as_null {
        let is_empty = array_length(column.clone()).eq(lit(0_u64));
        when(or(column.clone().is_null(), is_empty), singleton_null_list)
            .otherwise(column)
            .map_err(engine_err)?
            .alias(name)
    } else {
        when(column.clone().is_null(), singleton_null_list)
            .otherwise(column)
            .map_err(engine_err)?
            .alias(name)
    };

    let projection: Vec<Expr> = frame
        .schema()
        .fields()
        .iter()
        .map(|field| {
            if field.name() == name {
                rewritten.clone()
            } else {
                project_as(field.name())
            }
        })
        .collect();
    let frame = frame.select(projection).map_err(engine_err)?;
    unnest_list_column(frame, name)
}

/// Unnest a list column via `Column::new_unqualified` (`From<&str>` parses `s.f` as qualified).
fn unnest_list_column(frame: DataFrame, name: &str) -> Result<DataFrame> {
    let (session_state, plan) = frame.into_parts();
    let options = UnnestOptions {
        preserve_nulls: false,
        recursions: Vec::new(),
    };
    let plan = LogicalPlanBuilder::from(plan)
        .unnest_columns_with_options(vec![Column::new_unqualified(name)], options)
        .map_err(engine_err)?
        .build()
        .map_err(engine_err)?;
    // Re-alias every output unqualified.
    reproject_unqualified(DataFrame::new(session_state, plan))
}

/// Project every field as an unqualified name (`Column::new_unqualified` + alias).
fn reproject_unqualified(frame: DataFrame) -> Result<DataFrame> {
    let projection: Vec<Expr> = frame
        .schema()
        .fields()
        .iter()
        .map(|field| project_as(field.name()))
        .collect();
    frame.select(projection).map_err(engine_err)
}

/// Drop a column by projecting every other field through [`Column::new_unqualified`].
fn drop_unqualified_column(frame: DataFrame, name: &str) -> Result<DataFrame> {
    let projection: Vec<Expr> = frame
        .schema()
        .fields()
        .iter()
        .filter(|field| field.name() != name)
        .map(|field| project_as(field.name()))
        .collect();
    frame.select(projection).map_err(engine_err)
}

fn unqualified_expr(name: &str) -> Expr {
    Expr::Column(Column::new_unqualified(name))
}

fn project_as(name: &str) -> Expr {
    unqualified_expr(name).alias(name)
}

fn typed_null_literal(data_type: &DataType) -> Result<Expr> {
    let scalar = ScalarValue::try_from(data_type).map_err(engine_err)?;
    Ok(lit(scalar))
}

fn list_column_as_list(name: &str, column_type: &DataType) -> Expr {
    let column = unqualified_expr(name);
    match column_type {
        DataType::Dictionary(_, value) if list_element_type(value.as_ref()).is_some() => {
            cast(column, value.as_ref().clone())
        }
        _ => column,
    }
}

fn unwrap_dictionary_one_level(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value) => value.as_ref(),
        other => other,
    }
}

fn struct_fields(data_type: &DataType) -> Option<&Fields> {
    match unwrap_dictionary_one_level(data_type) {
        DataType::Struct(fields) => Some(fields),
        _ => None,
    }
}

fn list_element_type(data_type: &DataType) -> Option<&DataType> {
    match unwrap_dictionary_one_level(data_type) {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Some(field.data_type())
        }
        _ => None,
    }
}

fn has_struct_columns(fields: &Fields) -> bool {
    fields
        .iter()
        .any(|field| struct_fields(field.data_type()).is_some())
}

fn has_list_columns(fields: &Fields) -> bool {
    fields
        .iter()
        .any(|field| list_element_type(field.data_type()).is_some())
}

fn is_map_type(data_type: &DataType) -> bool {
    matches!(unwrap_dictionary_one_level(data_type), DataType::Map(_, _))
}

/// Bound on remaining-schema Debug dumped into `[DYNAMIC_FLATTEN_MAX_DEPTH]`.
const REMAINING_SCHEMA_CHAR_LIMIT: usize = 240;

/// Streams remaining-schema Debug into a char-capped buffer (never joins the full dump first).
struct RemainingSchemaWriter {
    out: String,
    remaining_chars: usize,
}

impl Write for RemainingSchemaWriter {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        if self.remaining_chars == 0 {
            return Err(std::fmt::Error);
        }
        for character in text.chars() {
            if self.remaining_chars == 0 {
                return Err(std::fmt::Error);
            }
            self.out.push(character);
            self.remaining_chars -= 1;
        }
        Ok(())
    }
}

fn format_fields(fields: &Fields) -> String {
    let mut writer = RemainingSchemaWriter {
        out: String::new(),
        remaining_chars: REMAINING_SCHEMA_CHAR_LIMIT,
    };
    let mut truncated = false;
    for (index, field) in fields.iter().enumerate() {
        if index > 0 && write!(writer, ", ").is_err() {
            truncated = true;
            break;
        }
        if write!(writer, "{}: {:?}", field.name(), field.data_type()).is_err() {
            truncated = true;
            break;
        }
    }
    if truncated {
        format!(
            "{}… ({} fields; remaining-schema text truncated)",
            writer.out,
            fields.len()
        )
    } else {
        writer.out
    }
}

fn first_list_view_column(fields: &Fields) -> Option<String> {
    fields.iter().find_map(|field| {
        if is_list_view_type(field.data_type()) {
            Some(field.name().clone())
        } else {
            None
        }
    })
}

fn is_list_view_type(data_type: &DataType) -> bool {
    matches!(
        unwrap_dictionary_one_level(data_type),
        DataType::ListView(_) | DataType::LargeListView(_)
    )
}

fn top_level_collision(name: &str, existing: &str) -> Error {
    Error::Analysis(format!(
        "[DYNAMIC_FLATTEN_NAME_COLLISION] dynamicFlatten: column {name:?} \
         collides with {existing} (parent-path prefix could not \
         disambiguate; rename before flatten)."
    ))
}

fn map_element_refused(name: &str) -> Error {
    Error::Analysis(format!(
        "[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT] dynamicFlatten cannot explode \
         list of map {name:?} (maps are not unnested; explode is unsupported \
         for map elements — cast the array or use a supported element type)."
    ))
}

fn list_view_refused(name: &str) -> Error {
    Error::Analysis(format!(
        "[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT] dynamicFlatten cannot explode \
         list-view {name:?} (ListView / LargeListView are not exploded — cast \
         to List first)."
    ))
}

fn prefixed_collision(parent: &str, nested: &str, prefixed: &str, existing: &str) -> Error {
    Error::Analysis(format!(
        "[DYNAMIC_FLATTEN_NAME_COLLISION] dynamicFlatten: prefixed field \
         {prefixed:?} (from struct {parent:?}.{nested}) collides with \
         {existing} (parent-path prefix could not disambiguate; \
         rename before flatten)."
    ))
}

#[cfg(test)]
mod tests;
