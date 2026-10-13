use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema, TimeUnit};
use datafusion::error::{DataFusionError, Result};
use iceberg::spec::{NestedField, Schema, Type};

use crate::write::append::iceberg_err;
use crate::write::schema_evolution::incoming_schema;
use crate::write::void_store::arrow_schema_to_iceberg_with_unknown;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Asked {
    Every,
    Columns(Vec<bool>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Remedies {
    pub nanosecond_cast: bool,
    pub setting: &'static str,
    pub naive_microseconds: Option<&'static str>,
    pub zoned_microseconds: Option<&'static str>,
    pub create_v3: &'static str,
    pub replace_v3: &'static str,
    pub upgrade_v3: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    FormatV3,
    CreatedBelowV3,
    ReplacedBelowV3,
    EvolvedBelowV3,
}

impl Target {
    #[must_use]
    pub fn of(holds_v3: bool, below: Self) -> Self {
        if holds_v3 { Self::FormatV3 } else { below }
    }

    fn remedy(self, remedies: &Remedies) -> Option<&'static str> {
        match self {
            Self::FormatV3 => None,
            Self::CreatedBelowV3 => Some(remedies.create_v3),
            Self::ReplacedBelowV3 => Some(remedies.replace_v3),
            Self::EvolvedBelowV3 => Some(remedies.upgrade_v3),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NanosecondAsk {
    asked: Asked,
    remedies: Remedies,
}

impl NanosecondAsk {
    #[must_use]
    pub fn every(remedies: Remedies) -> Self {
        Self {
            asked: Asked::Every,
            remedies,
        }
    }

    #[must_use]
    pub fn columns(asked: Vec<bool>, remedies: Remedies) -> Self {
        Self {
            asked: Asked::Columns(asked),
            remedies,
        }
    }

    fn asks(&self, index: usize) -> bool {
        match &self.asked {
            Asked::Every => true,
            Asked::Columns(flags) => flags.get(index).copied().unwrap_or(false),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NewColumns<'a> {
    pub arrow: &'a ArrowSchema,
    pub ask: &'a NanosecondAsk,
    pub existing: Option<&'a Schema>,
    pub form: &'a str,
    pub case_sensitive: bool,
    pub target: Target,
}

#[allow(clippy::missing_errors_doc)]
pub fn new_table_schema(request: &NewColumns<'_>) -> Result<Schema> {
    let schema = arrow_schema_to_iceberg_with_unknown(request.arrow).map_err(iceberg_err)?;
    refuse_new_nanoseconds(request, &schema, is_kept)?;
    Ok(schema)
}

#[allow(clippy::missing_errors_doc)]
pub fn added_columns_schema(request: &NewColumns<'_>) -> Result<Schema> {
    let schema = incoming_schema(request.arrow)?;
    refuse_new_nanoseconds(request, &schema, is_held)?;
    Ok(schema)
}

fn refuse_new_nanoseconds(
    request: &NewColumns<'_>,
    converted: &Schema,
    stays: fn(&NewColumns<'_>, &NestedField) -> bool,
) -> Result<()> {
    let stored = converted.as_struct().fields();
    for (index, (field, stored)) in request.arrow.fields().iter().zip(stored).enumerate() {
        let Some(zoned) = first_nanosecond(field.data_type()) else {
            continue;
        };
        if stays(request, stored) {
            continue;
        }
        if !request.ask.asks(index) {
            return Err(not_requested(request, field.name(), zoned));
        }
        if let Some(remedy) = request.target.remedy(&request.ask.remedies) {
            return Err(requires_format_v3(field.name(), zoned, remedy));
        }
    }
    Ok(())
}

fn first_nanosecond(data_type: &DataType) -> Option<bool> {
    match data_type {
        DataType::Timestamp(TimeUnit::Nanosecond, zone) => Some(zone.is_some()),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => first_nanosecond(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .find_map(|field| first_nanosecond(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .find_map(|(_, field)| first_nanosecond(field.data_type())),
        DataType::Dictionary(_, values) => first_nanosecond(values),
        DataType::RunEndEncoded(_, values) => first_nanosecond(values.data_type()),
        _ => None,
    }
}

fn same_name(request: &NewColumns<'_>, held: &NestedField, stored: &NestedField) -> bool {
    if request.case_sensitive {
        held.name == stored.name
    } else {
        held.name.to_lowercase() == stored.name.to_lowercase()
    }
}

fn held_fields<'a>(request: &'a NewColumns<'_>) -> &'a [std::sync::Arc<NestedField>] {
    request
        .existing
        .map_or(&[], |existing| existing.as_struct().fields())
}

fn is_kept(request: &NewColumns<'_>, stored: &NestedField) -> bool {
    held_fields(request).iter().any(|held| {
        same_name(request, held, stored) && same_type(&held.field_type, &stored.field_type)
    })
}

fn is_held(request: &NewColumns<'_>, stored: &NestedField) -> bool {
    held_fields(request)
        .iter()
        .any(|held| same_name(request, held, stored))
}

fn same_type(held: &Type, stored: &Type) -> bool {
    match (held, stored) {
        (Type::Primitive(held), Type::Primitive(stored)) => held == stored,
        (Type::Struct(held), Type::Struct(stored)) => {
            held.fields().len() == stored.fields().len()
                && held
                    .fields()
                    .iter()
                    .zip(stored.fields())
                    .all(|(held, stored)| {
                        held.name == stored.name && same_type(&held.field_type, &stored.field_type)
                    })
        }
        (Type::List(held), Type::List(stored)) => same_type(
            &held.element_field.field_type,
            &stored.element_field.field_type,
        ),
        (Type::Map(held), Type::Map(stored)) => {
            same_type(&held.key_field.field_type, &stored.key_field.field_type)
                && same_type(&held.value_field.field_type, &stored.value_field.field_type)
        }
        _ => false,
    }
}

const fn nano_name(zoned: bool) -> &'static str {
    if zoned {
        "timestamptz_ns"
    } else {
        "timestamp_ns"
    }
}

fn not_requested(request: &NewColumns<'_>, name: &str, zoned: bool) -> DataFusionError {
    let remedies = &request.ask.remedies;
    let nano = nano_name(zoned);
    let setting = remedies.setting;
    let ask = if remedies.nanosecond_cast {
        format!(
            " Ask with CAST(<expression> AS {nano}) on the column (SQL or selectExpr), or \
             {setting}."
        )
    } else {
        format!(" To ask, {setting}.")
    };
    let version = request
        .target
        .remedy(remedies)
        .map_or_else(String::new, |remedy| {
            format!(" A nanosecond column also needs a format version 3 table. {remedy}")
        });
    let spelling = if zoned {
        remedies.zoned_microseconds
    } else {
        remedies.naive_microseconds
    };
    let micro = spelling.map_or_else(String::new, |spelling| {
        format!(" For microseconds write {spelling}.")
    });
    DataFusionError::Plan(format!(
        "[TIMESTAMP_NANOS_NOT_REQUESTED] {form} would create column `{name}` as {nano}, a type \
         Spark cannot read, and the statement does not ask for it.{ask}{version}{micro}",
        form = request.form
    ))
}

fn requires_format_v3(name: &str, zoned: bool, remedy: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[TIMESTAMP_NANOS_REQUIRES_FORMAT_V3] Invalid type for {name}: {nano} is not supported \
         until v3. {remedy}",
        nano = nano_name(zoned)
    ))
}

#[cfg(test)]
pub(crate) const TEST_REMEDIES: Remedies = Remedies {
    nanosecond_cast: true,
    setting: "turn the setting on",
    naive_microseconds: Some("CAST(<expression> AS TIMESTAMP_NTZ)"),
    zoned_microseconds: Some("CAST(<expression> AS TIMESTAMP)"),
    create_v3: "Create it as v3.",
    replace_v3: "Replace it as v3.",
    upgrade_v3: "Upgrade it to v3.",
};

#[cfg(test)]
#[allow(clippy::unnecessary_wraps)]
pub(crate) fn asks_nothing(
    _: &datafusion::prelude::SessionContext,
    _: &datafusion::logical_expr::LogicalPlan,
) -> Result<NanosecondAsk> {
    Ok(NanosecondAsk::columns(Vec::new(), TEST_REMEDIES))
}

#[cfg(test)]
#[path = "nanosecond_door_tests.rs"]
mod tests;
