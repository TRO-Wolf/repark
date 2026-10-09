use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, GenericListArray, MapArray, OffsetSizeTrait, StructArray, make_array,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, FieldRef, Fields};
use datafusion::common::{DataFusionError, Result};

use super::{Conversion, target_name, target_type, timestamp_ns_target};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pairing {
    Cast,
    Name,
    Exact,
}

impl Pairing {
    #[must_use]
    pub const fn word(self) -> &'static str {
        match self {
            Self::Cast => "cast",
            Self::Name => "name",
            Self::Exact => "exact",
        }
    }

    #[must_use]
    pub fn parse(word: &str) -> Option<Self> {
        [Self::Cast, Self::Name, Self::Exact]
            .into_iter()
            .find(|pairing| pairing.word() == word)
    }

    const fn under_list(self, large: bool, target: &DataType) -> Self {
        match (self, large, target) {
            (Self::Exact, _, _) => Self::Exact,
            (Self::Name, false, DataType::List(_)) => Self::Name,
            _ => Self::Cast,
        }
    }

    const fn under_map(self) -> Self {
        match self {
            Self::Exact => Self::Exact,
            Self::Cast | Self::Name => Self::Cast,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unstorable {
    Layout,
    Shape,
    Unpaired(Pairing),
    Required,
    Narrowed,
    Door,
}

impl fmt::Display for Unstorable {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Layout => out.write_str("the source layout cannot carry a timestamp"),
            Self::Shape => out.write_str("the source nests differently from the target"),
            Self::Unpaired(Pairing::Cast) => out.write_str(
                "no source field pairs with it by position or by a complete set of names",
            ),
            Self::Unpaired(Pairing::Name) => {
                out.write_str("no source field of that name and nullability pairs with it")
            }
            Self::Unpaired(Pairing::Exact) => {
                out.write_str("the source field names are not the target's, in order")
            }
            Self::Required => out.write_str("the leaf is required and the value is NULL"),
            Self::Narrowed => {
                out.write_str("the source narrows a nanosecond value to microseconds")
            }
            Self::Door => out.write_str("this statement cannot store a nested value"),
        }
    }
}

#[must_use]
pub fn nested_refusal(
    path: &[String],
    source: &DataType,
    zoned: bool,
    why: Unstorable,
) -> DataFusionError {
    let leaf = path
        .iter()
        .map(|part| format!("`{}`", part.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".");
    DataFusionError::Plan(format!(
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the \
         table ``: Cannot safely cast {leaf} \"{source}\" to \"{target}\". The nested source \
         shape cannot be stored into this {lower} leaf: {why}. SQLSTATE: KD000",
        target = target_name(zoned),
        lower = target_name(zoned).to_ascii_lowercase(),
    ))
}

#[must_use]
pub fn holds_timestamp_ns(target: &DataType, zoned: bool) -> bool {
    let holds = |field: &FieldRef| {
        timestamp_ns_target(field.data_type()) == Some(zoned)
            || holds_timestamp_ns(field.data_type(), zoned)
    };
    match target {
        DataType::Struct(fields) => fields.iter().any(holds),
        DataType::Map(field, _)
        | DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => holds(field),
        _ => false,
    }
}

enum Layout<'a> {
    Struct(&'a Fields),
    List(&'a FieldRef, bool),
    Map(&'a FieldRef, bool),
    Encoded(&'a DataType),
    Moment,
    Null,
    Union,
    Scalar,
}

fn layout(source: &DataType) -> Layout<'_> {
    match source {
        DataType::Struct(fields) => Layout::Struct(fields),
        DataType::List(field) | DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
            Layout::List(field, false)
        }
        DataType::LargeList(field) | DataType::LargeListView(field) => Layout::List(field, true),
        DataType::Map(entries, sorted) => Layout::Map(entries, *sorted),
        DataType::Dictionary(_, values) => Layout::Encoded(values),
        DataType::RunEndEncoded(_, values) => Layout::Encoded(values.data_type()),
        DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => Layout::Moment,
        DataType::Null => Layout::Null,
        DataType::Union(_, _) => Layout::Union,
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => Layout::Scalar,
    }
}

fn element(target: &DataType) -> Option<&FieldRef> {
    match target {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => Some(field),
        _ => None,
    }
}

fn entries(map: &FieldRef) -> Option<(&FieldRef, &FieldRef)> {
    match map.data_type() {
        DataType::Struct(pair) if pair.len() == 2 => Some((&pair[0], &pair[1])),
        _ => None,
    }
}

pub(super) struct Store<'a> {
    pub(super) zoned: bool,
    pub(super) column: &'a str,
}

struct Site<'a> {
    store: &'a Store<'a>,
    pairing: Pairing,
    path: Vec<String>,
}

impl Site<'_> {
    fn wants(&self, target: &DataType) -> bool {
        timestamp_ns_target(target) == Some(self.store.zoned)
            || holds_timestamp_ns(target, self.store.zoned)
    }

    fn refuse(&self, source: &DataType, why: Unstorable) -> DataFusionError {
        nested_refusal(&self.path, source, self.store.zoned, why)
    }

    fn leaf_below(&self, targets: &Fields) -> Self {
        let mut site = self.below("", self.pairing);
        site.path.pop();
        let mut fields = Some(targets);
        while let Some(field) =
            fields.and_then(|fields| fields.iter().find(|field| self.wants(field.data_type())))
        {
            site.path.push(field.name().clone());
            fields = match field.data_type() {
                DataType::Struct(inner) => Some(inner),
                _ => None,
            };
        }
        site
    }

    fn below(&self, name: &str, pairing: Pairing) -> Self {
        let mut path = self.path.clone();
        path.push(name.to_string());
        Site {
            store: self.store,
            pairing,
            path,
        }
    }

    fn conformed_type(&self, source: &DataType, target: &DataType) -> Result<DataType> {
        if !self.wants(target) {
            return Ok(source.clone());
        }
        let leaf = timestamp_ns_target(target) == Some(self.store.zoned);
        match layout(source) {
            Layout::Encoded(values) => self.conformed_type(values, target),
            Layout::Null => Ok(source.clone()),
            Layout::Union | Layout::Scalar => Err(self.refuse(source, Unstorable::Layout)),
            Layout::Moment if leaf => Ok(target_type(self.store.zoned)),
            Layout::Moment => Err(self.refuse(source, Unstorable::Shape)),
            Layout::Struct(sources) => {
                let DataType::Struct(targets) = target else {
                    return Err(self.refuse(source, Unstorable::Shape));
                };
                Ok(DataType::Struct(
                    self.conformed_fields(source, sources, targets)?,
                ))
            }
            Layout::List(item, large) => {
                let Some(to) = element(target) else {
                    return Err(self.refuse(source, Unstorable::Shape));
                };
                let below = self.below("element", self.pairing.under_list(large, target));
                let item = below.conformed_field(item, to)?;
                Ok(if large {
                    DataType::LargeList(item)
                } else {
                    DataType::List(item)
                })
            }
            Layout::Map(pairs, sorted) => {
                let DataType::Map(to, _) = target else {
                    return Err(self.refuse(source, Unstorable::Shape));
                };
                let (Some((key, value)), Some((to_key, to_value))) = (entries(pairs), entries(to))
                else {
                    return Err(self.refuse(source, Unstorable::Shape));
                };
                let pairing = self.pairing.under_map();
                let key = self.below("key", pairing).conformed_field(key, to_key)?;
                let key = Arc::new(key.as_ref().clone().with_nullable(false));
                let value = self
                    .below("value", pairing)
                    .conformed_field(value, to_value)?;
                let pair = DataType::Struct(Fields::from(vec![key, value]));
                Ok(DataType::Map(
                    Arc::new(pairs.as_ref().clone().with_data_type(pair)),
                    sorted,
                ))
            }
        }
    }

    fn conformed_field(&self, source: &FieldRef, target: &FieldRef) -> Result<FieldRef> {
        let conformed = self.conformed_type(source.data_type(), target.data_type())?;
        if conformed == *source.data_type() {
            return Ok(Arc::clone(source));
        }
        let leaf = timestamp_ns_target(target.data_type()) == Some(self.store.zoned);
        let nullable = source.is_nullable() || (leaf && target.is_nullable());
        Ok(Arc::new(
            source
                .as_ref()
                .clone()
                .with_data_type(conformed)
                .with_nullable(nullable),
        ))
    }

    fn paired(
        &self,
        source: &DataType,
        sources: &Fields,
        targets: &Fields,
    ) -> Result<Vec<Option<usize>>> {
        let named = |target: &FieldRef| {
            sources
                .iter()
                .position(|field| field.name() == target.name())
        };
        let unpaired = || {
            self.leaf_below(targets)
                .refuse(source, Unstorable::Unpaired(self.pairing))
        };
        let in_order = sources.len() == targets.len()
            && sources
                .iter()
                .zip(targets)
                .all(|(field, target)| field.name() == target.name());
        let all_named = targets.iter().all(|target| named(target).is_some());
        let by_target: Vec<Option<usize>> = match self.pairing {
            Pairing::Name => {
                let found: Vec<Option<usize>> = targets.iter().map(named).collect();
                let refused = found
                    .iter()
                    .zip(targets)
                    .any(|(index, target)| match index {
                        Some(index) => {
                            let field = &sources[*index];
                            !target.is_nullable()
                                && (field.is_nullable() || field.data_type() == &DataType::Null)
                        }
                        None => !target.is_nullable(),
                    });
                if refused || found.iter().all(Option::is_none) {
                    return Err(unpaired());
                }
                found
            }
            Pairing::Cast if !in_order && all_named => targets.iter().map(named).collect(),
            Pairing::Cast if sources.len() < targets.len() => return Err(unpaired()),
            Pairing::Exact if !in_order => return Err(unpaired()),
            Pairing::Cast | Pairing::Exact => (0..targets.len()).map(Some).collect(),
        };
        let mut by_source = vec![None; sources.len()];
        for (index, (found, target)) in by_target.iter().zip(targets).enumerate() {
            match found {
                Some(found) => by_source[*found] = Some(index),
                None if self.wants(target.data_type()) => {
                    return Err(self
                        .leaf_below(&Fields::from(vec![Arc::clone(target)]))
                        .refuse(source, Unstorable::Unpaired(self.pairing)));
                }
                None => {}
            }
        }
        Ok(by_source)
    }

    fn conformed_fields(
        &self,
        source: &DataType,
        sources: &Fields,
        targets: &Fields,
    ) -> Result<Fields> {
        let paired = self.paired(source, sources, targets)?;
        sources
            .iter()
            .zip(paired)
            .map(|(field, index)| match index {
                Some(index) => {
                    let target = &targets[index];
                    self.below(target.name(), self.pairing)
                        .conformed_field(field, target)
                }
                None => Ok(Arc::clone(field)),
            })
            .collect()
    }

    fn conform(
        &self,
        conversion: &Conversion,
        array: &ArrayRef,
        target: &DataType,
    ) -> Result<ArrayRef> {
        if !self.wants(target) {
            return Ok(Arc::clone(array));
        }
        let source = array.data_type();
        match layout(source) {
            Layout::Encoded(values) => {
                let plain = cast(array.as_ref(), values)?;
                self.conform(conversion, &plain, target)
            }
            Layout::Null => Ok(Arc::clone(array)),
            Layout::Union | Layout::Scalar => Err(self.refuse(source, Unstorable::Layout)),
            Layout::Moment if timestamp_ns_target(target) == Some(self.store.zoned) => {
                conversion.convert(array)
            }
            Layout::Moment => Err(self.refuse(source, Unstorable::Shape)),
            Layout::Struct(sources) => {
                let DataType::Struct(targets) = target else {
                    return Err(self.refuse(source, Unstorable::Shape));
                };
                let rows = array.as_struct();
                self.conform_struct(conversion, source, rows, sources, targets)
                    .map(|rows| Arc::new(rows) as ArrayRef)
            }
            Layout::List(item, large) => {
                let Some(to) = element(target) else {
                    return Err(self.refuse(source, Unstorable::Shape));
                };
                let below = self.below("element", self.pairing.under_list(large, target));
                let field = below.conformed_field(item, to)?;
                if large {
                    let plain = cast(array.as_ref(), &DataType::LargeList(Arc::clone(item)))?;
                    below.conform_list::<i64>(conversion, &plain, field, to)
                } else {
                    let plain = cast(array.as_ref(), &DataType::List(Arc::clone(item)))?;
                    below.conform_list::<i32>(conversion, &plain, field, to)
                }
            }
            Layout::Map(pairs, sorted) => {
                let DataType::Map(to, _) = target else {
                    return Err(self.refuse(source, Unstorable::Shape));
                };
                let (Some((_, _)), Some((to_key, to_value))) = (entries(pairs), entries(to)) else {
                    return Err(self.refuse(source, Unstorable::Shape));
                };
                let DataType::Map(out, _) = self.conformed_type(source, target)? else {
                    return Err(self.refuse(source, Unstorable::Shape));
                };
                let DataType::Struct(fields) = out.data_type() else {
                    return Err(self.refuse(source, Unstorable::Shape));
                };
                let rows = array.as_map();
                let pairing = self.pairing.under_map();
                let keys = self.below("key", pairing).conform(
                    conversion,
                    rows.keys(),
                    to_key.data_type(),
                )?;
                let values = self.below("value", pairing).conform(
                    conversion,
                    rows.values(),
                    to_value.data_type(),
                )?;
                let conformed = StructArray::try_new(fields.clone(), vec![keys, values], None)?;
                Ok(Arc::new(MapArray::try_new(
                    Arc::clone(&out),
                    rows.offsets().clone(),
                    conformed,
                    rows.nulls().cloned(),
                    sorted,
                )?))
            }
        }
    }

    fn conform_struct(
        &self,
        conversion: &Conversion,
        source: &DataType,
        rows: &StructArray,
        sources: &Fields,
        targets: &Fields,
    ) -> Result<StructArray> {
        let paired = self.paired(source, sources, targets)?;
        let mut fields = Vec::with_capacity(sources.len());
        let mut columns = Vec::with_capacity(sources.len());
        for ((field, column), index) in sources.iter().zip(rows.columns()).zip(paired) {
            let Some(index) = index else {
                fields.push(Arc::clone(field));
                columns.push(Arc::clone(column));
                continue;
            };
            let target = &targets[index];
            let below = self.below(target.name(), self.pairing);
            let conformed = below.conformed_field(field, target)?;
            let visible = under(rows.nulls(), column)?;
            let stored = if conformed.data_type() == field.data_type() {
                Arc::clone(column)
            } else {
                below.conform(conversion, &visible, target.data_type())?
            };
            let unmasked = stored.null_count().max(visible.null_count()) > rows.null_count();
            if !target.is_nullable() && below.wants(target.data_type()) && unmasked {
                return Err(below.refuse(field.data_type(), Unstorable::Required));
            }
            fields.push(conformed);
            columns.push(stored);
        }
        Ok(StructArray::try_new(
            Fields::from(fields),
            columns,
            rows.nulls().cloned(),
        )?)
    }

    fn conform_list<O: OffsetSizeTrait>(
        &self,
        conversion: &Conversion,
        plain: &ArrayRef,
        field: FieldRef,
        to: &FieldRef,
    ) -> Result<ArrayRef> {
        let rows = plain.as_list::<O>();
        let values = self.conform(conversion, rows.values(), to.data_type())?;
        Ok(Arc::new(GenericListArray::<O>::try_new(
            field,
            rows.offsets().clone(),
            values,
            rows.nulls().cloned(),
        )?))
    }
}

fn under(parent: Option<&NullBuffer>, child: &ArrayRef) -> Result<ArrayRef> {
    let Some(parent) = parent else {
        return Ok(Arc::clone(child));
    };
    let nulls = NullBuffer::union(Some(parent), child.nulls());
    let data = child.to_data().into_builder().nulls(nulls).build()?;
    Ok(make_array(data))
}

impl Store<'_> {
    fn site(&self, pairing: Pairing) -> Site<'_> {
        Site {
            store: self,
            pairing,
            path: vec![self.column.to_string()],
        }
    }

    pub(super) fn conformed_type(
        &self,
        source: &DataType,
        target: &DataType,
        pairing: Pairing,
    ) -> Result<DataType> {
        self.site(pairing).conformed_type(source, target)
    }

    pub(super) fn conform(
        &self,
        conversion: &Conversion,
        array: &ArrayRef,
        target: &DataType,
        pairing: Pairing,
    ) -> Result<ArrayRef> {
        self.site(pairing).conform(conversion, array, target)
    }
}
