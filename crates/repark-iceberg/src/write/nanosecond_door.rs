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
    pub naive_microseconds: &'static str,
    pub zoned_microseconds: &'static str,
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
}

#[allow(clippy::missing_errors_doc)]
pub fn new_table_schema(request: &NewColumns<'_>) -> Result<Schema> {
    let schema = arrow_schema_to_iceberg_with_unknown(request.arrow).map_err(iceberg_err)?;
    refuse_unasked(request, &schema)?;
    Ok(schema)
}

#[allow(clippy::missing_errors_doc)]
pub fn added_columns_schema(request: &NewColumns<'_>) -> Result<Schema> {
    let schema = incoming_schema(request.arrow)?;
    refuse_unasked(request, &schema)?;
    Ok(schema)
}

fn refuse_unasked(request: &NewColumns<'_>, converted: &Schema) -> Result<()> {
    let stored = converted.as_struct().fields();
    for (index, (field, stored)) in request.arrow.fields().iter().zip(stored).enumerate() {
        let Some(zoned) = first_nanosecond(field.data_type()) else {
            continue;
        };
        if is_kept(request, stored) || request.ask.asks(index) {
            continue;
        }
        return Err(not_requested(request, field.name(), zoned));
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

fn is_kept(request: &NewColumns<'_>, stored: &NestedField) -> bool {
    let Some(existing) = request.existing else {
        return false;
    };
    existing.as_struct().fields().iter().any(|held| {
        let same_name = if request.case_sensitive {
            held.name == stored.name
        } else {
            held.name.to_lowercase() == stored.name.to_lowercase()
        };
        same_name && same_type(&held.field_type, &stored.field_type)
    })
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

fn not_requested(request: &NewColumns<'_>, name: &str, zoned: bool) -> DataFusionError {
    let remedies = request.ask.remedies;
    let (nano, micro) = if zoned {
        ("timestamptz_ns", remedies.zoned_microseconds)
    } else {
        ("timestamp_ns", remedies.naive_microseconds)
    };
    let ask = if remedies.nanosecond_cast {
        format!(
            "Ask with CAST(<expression> AS {nano}) on the column (SQL or selectExpr), or set \
             `repark.sql.allowImplicitTimestampNanos` = true on the session."
        )
    } else {
        "Set `repark.sql.allowImplicitTimestampNanos` = true on the session.".to_string()
    };
    DataFusionError::Plan(format!(
        "[TIMESTAMP_NANOS_NOT_REQUESTED] {form} would create column `{name}` as {nano}, a type \
         Spark cannot read, and the statement does not ask for it. {ask} For microseconds write \
         CAST(<expression> AS {micro}).",
        form = request.form
    ))
}

#[cfg(test)]
pub(crate) const TEST_REMEDIES: Remedies = Remedies {
    nanosecond_cast: true,
    naive_microseconds: "TIMESTAMP_NTZ",
    zoned_microseconds: "TIMESTAMP",
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
