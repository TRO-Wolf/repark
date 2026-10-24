use std::any::Any;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayData, ArrayRef, make_array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility, WriteOp,
};
use datafusion::scalar::ScalarValue;

use super::store_assign::without_field_metadata;
use super::update_cast::refuse_unassignable_leaves;

pub use repark_common::udf_names::STORE_PAIR_KERNEL_NAME;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StructPairing {
    ByPosition,
    ByName { case_sensitive: bool },
}

impl StructPairing {
    #[must_use]
    pub fn by_name(case_sensitive: bool) -> Self {
        Self::ByName { case_sensitive }
    }

    #[must_use]
    pub fn from_source_by_name(source_by_name: bool, case_sensitive: bool) -> Self {
        if source_by_name {
            Self::ByName { case_sensitive }
        } else {
            Self::ByPosition
        }
    }

    fn literal(self) -> &'static str {
        match self {
            Self::ByPosition => "by_position",
            Self::ByName {
                case_sensitive: false,
            } => "by_name",
            Self::ByName {
                case_sensitive: true,
            } => "by_name_case_sensitive",
        }
    }

    fn from_literal(text: &str) -> Option<Self> {
        match text {
            "by_position" => Some(Self::ByPosition),
            "by_name" => Some(Self::by_name(false)),
            "by_name_case_sensitive" => Some(Self::by_name(true)),
            _ => None,
        }
    }

    fn matches(self, source: &str, target: &str) -> bool {
        match self {
            Self::ByName {
                case_sensitive: true,
            } => source == target,
            _ => spark_names_equal_ignoring_case(source, target),
        }
    }
}

fn single(mut mapped: impl Iterator<Item = char>) -> Option<char> {
    let first = mapped.next()?;
    mapped.next().is_none().then_some(first)
}

fn java_upper(c: char) -> char {
    match c {
        '\u{1F80}'..='\u{1F87}' | '\u{1F90}'..='\u{1F97}' | '\u{1FA0}'..='\u{1FA7}' => {
            char::from_u32(u32::from(c) + 8).unwrap_or(c)
        }
        '\u{1FB3}' => '\u{1FBC}',
        '\u{1FC3}' => '\u{1FCC}',
        '\u{1FF3}' => '\u{1FFC}',
        _ => single(c.to_uppercase()).unwrap_or(c),
    }
}

fn java_lower(c: char) -> char {
    match c {
        '\u{130}' => 'i',
        _ => single(c.to_lowercase()).unwrap_or(c),
    }
}

#[must_use]
pub fn spark_names_equal_ignoring_case(source: &str, target: &str) -> bool {
    if source.encode_utf16().count() != target.encode_utf16().count()
        || source.chars().count() != target.chars().count()
    {
        return false;
    }
    source.chars().zip(target.chars()).all(|(left, right)| {
        if left == right {
            return true;
        }
        let (upper_left, upper_right) = (java_upper(left), java_upper(right));
        upper_left == upper_right || java_lower(upper_left) == java_lower(upper_right)
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreEntry {
    Batch,
    StaticPartition,
}

impl StoreEntry {
    fn hint(self) -> &'static str {
        match self {
            Self::Batch => {
                "This entry does not pair struct fields; give the value the table's field names \
                 in the table's order."
            }
            Self::StaticPartition => {
                "INSERT OVERWRITE with a static PARTITION value does not pair struct fields; \
                 write the struct with the table's field names in the table's field order."
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafPair {
    pub path: String,
    pub source: DataType,
    pub target: DataType,
}

#[derive(Debug, Clone)]
enum Plan {
    Leaf,
    Node {
        data_type: DataType,
        children: Vec<(usize, Plan)>,
    },
}

impl Plan {
    fn is_leaf(&self) -> bool {
        matches!(self, Self::Leaf)
    }
}

#[derive(Debug, Clone)]
pub struct StorePairs {
    plan: Plan,
    paired: DataType,
    leaves: Vec<LeafPair>,
}

impl StorePairs {
    #[must_use]
    pub fn is_identity(&self) -> bool {
        self.plan.is_leaf()
    }

    #[must_use]
    pub fn paired_type(&self) -> DataType {
        self.paired.clone()
    }

    #[must_use]
    pub fn leaves(&self) -> &[LeafPair] {
        &self.leaves
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn apply(&self, value: &ArrayRef) -> Result<ArrayRef> {
        if self.plan.is_leaf() {
            return Ok(Arc::clone(value));
        }
        Ok(make_array(apply_plan(&self.plan, value.to_data())?))
    }
}

fn apply_plan(plan: &Plan, data: ArrayData) -> Result<ArrayData> {
    let Plan::Node {
        data_type,
        children,
    } = plan
    else {
        return Ok(data);
    };
    let sources = data.child_data().to_vec();
    let mut paired = Vec::with_capacity(children.len());
    for (index, child) in children {
        let source = sources.get(*index).cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "struct pairing lost child {index} of a {}",
                data.data_type()
            ))
        })?;
        paired.push(apply_plan(child, source)?);
    }
    data.into_builder()
        .data_type(data_type.clone())
        .child_data(paired)
        .build()
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

fn backtick(part: &str) -> String {
    format!("`{}`", part.replace('`', "``"))
}

fn render_path(path: &[String]) -> String {
    path.join(".")
}

fn render_names<'a>(names: impl Iterator<Item = &'a str>) -> String {
    names.map(backtick).collect::<Vec<_>>().join(", ")
}

fn incompatible(table: &str, class: &str, detail: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[INCOMPATIBLE_DATA_FOR_TABLE.{class}] Cannot write incompatible data for the table \
         {table}: {detail}. SQLSTATE: KD000"
    ))
}

fn cannot_find_data(table: &str, path: &[String]) -> DataFusionError {
    incompatible(
        table,
        "CANNOT_FIND_DATA",
        &format!(
            "Cannot find data for the output column {}",
            render_path(path)
        ),
    )
}

fn extra_struct_fields(table: &str, extra: &[String], path: &[String]) -> DataFusionError {
    incompatible(
        table,
        "EXTRA_STRUCT_FIELDS",
        &format!(
            "Cannot write extra fields {} to the struct {}",
            render_names(extra.iter().map(String::as_str)),
            render_path(path)
        ),
    )
}

fn struct_missing_fields(table: &str, missing: &[String], path: &[String]) -> DataFusionError {
    incompatible(
        table,
        "STRUCT_MISSING_FIELDS",
        &format!(
            "Struct {} missing fields: {}",
            render_path(path),
            render_names(missing.iter().map(String::as_str))
        ),
    )
}

fn ambiguous_column(table: &str, path: &[String]) -> DataFusionError {
    incompatible(
        table,
        "AMBIGUOUS_COLUMN_NAME",
        &format!(
            "Ambiguous column name in the input data {}",
            render_path(path)
        ),
    )
}

fn unpaired_struct(
    table: &str,
    entry: StoreEntry,
    path: &[String],
    source: &Fields,
    target: &Fields,
) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the \
         table {table}: the struct {} has the fields {} and the table has {}. {}",
        render_path(path),
        render_names(source.iter().map(|field| field.name().as_str())),
        render_names(target.iter().map(|field| field.name().as_str())),
        entry.hint()
    ))
}

fn unpaired_layout(
    table: &str,
    entry: StoreEntry,
    path: &[String],
    layout: &DataType,
) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the \
         table {table}: the layout {layout} is not written into a struct column {}. {}",
        render_path(path),
        entry.hint()
    ))
}

fn holds_struct(data_type: &DataType) -> bool {
    match data_type {
        DataType::Struct(_) => true,
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::ListView(element)
        | DataType::LargeListView(element)
        | DataType::FixedSizeList(element, _)
        | DataType::Map(element, _) => holds_struct(element.data_type()),
        DataType::Dictionary(_, values) => holds_struct(values),
        DataType::RunEndEncoded(_, values) => holds_struct(values.data_type()),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| holds_struct(field.data_type())),
        _ => false,
    }
}

fn list_element(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::ListView(element)
        | DataType::LargeListView(element)
        | DataType::FixedSizeList(element, _) => Some(element),
        _ => None,
    }
}

fn with_element(list: &DataType, element: FieldRef) -> DataType {
    match list {
        DataType::LargeList(_) => DataType::LargeList(element),
        DataType::ListView(_) => DataType::ListView(element),
        DataType::LargeListView(_) => DataType::LargeListView(element),
        DataType::FixedSizeList(_, size) => DataType::FixedSizeList(element, *size),
        _ => DataType::List(element),
    }
}

fn retyped(field: &Field, data_type: DataType) -> FieldRef {
    Arc::new(
        Field::new(field.name(), data_type, field.is_nullable())
            .with_metadata(field.metadata().clone()),
    )
}

fn map_pair(entries: &Field) -> Option<(&FieldRef, &FieldRef)> {
    match entries.data_type() {
        DataType::Struct(pair) if pair.len() == 2 => Some((&pair[0], &pair[1])),
        _ => None,
    }
}

struct Walk<'a> {
    pairing: StructPairing,
    table: &'a str,
    leaves: Vec<LeafPair>,
}

impl Walk<'_> {
    fn pair(
        &mut self,
        source: &DataType,
        target: &DataType,
        path: &mut Vec<String>,
    ) -> Result<(Plan, DataType)> {
        match (source, target) {
            (DataType::Dictionary(key, values), _) if holds_struct(values) => {
                let (plan, paired) = self.pair(values, target, path)?;
                Ok(wrap_one(
                    plan,
                    source,
                    DataType::Dictionary(key.clone(), Box::new(paired)),
                    0,
                ))
            }
            (DataType::RunEndEncoded(ends, values), _) if holds_struct(values.data_type()) => {
                let (plan, paired) = self.pair(values.data_type(), target, path)?;
                if plan.is_leaf() {
                    return Ok((Plan::Leaf, source.clone()));
                }
                let paired = DataType::RunEndEncoded(Arc::clone(ends), retyped(values, paired));
                Ok((
                    Plan::Node {
                        data_type: paired.clone(),
                        children: vec![(0, Plan::Leaf), (1, plan)],
                    },
                    paired,
                ))
            }
            (DataType::Union(_, _), _) if holds_struct(target) => {
                Err(unpaired_layout(self.table, StoreEntry::Batch, path, source))
            }
            (DataType::Struct(from), DataType::Struct(to)) => self.pair_struct(from, to, path),
            (DataType::Map(from, sorted), DataType::Map(to, _)) => {
                let (Some((key, value)), Some((target_key, target_value))) =
                    (map_pair(from), map_pair(to))
                else {
                    return Ok(self.leaf(source, target, path));
                };
                path.push("`key`".to_string());
                let key_pair = self.pair(key.data_type(), target_key.data_type(), path);
                path.pop();
                let (key_plan, key_type) = key_pair?;
                path.push("`value`".to_string());
                let value_pair = self.pair(value.data_type(), target_value.data_type(), path);
                path.pop();
                let (value_plan, value_type) = value_pair?;
                if key_plan.is_leaf() && value_plan.is_leaf() {
                    return Ok((Plan::Leaf, source.clone()));
                }
                let entries_type = DataType::Struct(Fields::from(vec![
                    retyped(key, key_type),
                    retyped(value, value_type),
                ]));
                let entries = Plan::Node {
                    data_type: entries_type.clone(),
                    children: vec![(0, key_plan), (1, value_plan)],
                };
                let paired = DataType::Map(retyped(from, entries_type), *sorted);
                Ok((
                    Plan::Node {
                        data_type: paired.clone(),
                        children: vec![(0, entries)],
                    },
                    paired,
                ))
            }
            _ => match (list_element(source), list_element(target)) {
                (Some(element), Some(target_element)) => {
                    path.push("`element`".to_string());
                    let pair = self.pair(element.data_type(), target_element.data_type(), path);
                    path.pop();
                    let (plan, paired) = pair?;
                    let paired = with_element(source, retyped(element, paired));
                    Ok(wrap_one(plan, source, paired, 0))
                }
                _ => Ok(self.leaf(source, target, path)),
            },
        }
    }

    fn leaf(&mut self, source: &DataType, target: &DataType, path: &[String]) -> (Plan, DataType) {
        self.leaves.push(LeafPair {
            path: render_path(path),
            source: source.clone(),
            target: target.clone(),
        });
        (Plan::Leaf, source.clone())
    }

    fn pick(&self, from: &Fields, target: &str, path: &[String]) -> Result<usize> {
        let mut found = from
            .iter()
            .enumerate()
            .filter(|(_, field)| self.pairing.matches(field.name(), target))
            .map(|(index, _)| index);
        let first = found
            .next()
            .ok_or_else(|| cannot_find_data(self.table, path))?;
        if found.next().is_some() {
            return Err(ambiguous_column(self.table, path));
        }
        Ok(first)
    }

    fn picks(&self, from: &Fields, to: &Fields, path: &mut Vec<String>) -> Result<Vec<usize>> {
        if self.pairing == StructPairing::ByPosition {
            if from.len() > to.len() {
                let extra: Vec<String> = from
                    .iter()
                    .skip(to.len())
                    .map(|f| f.name().clone())
                    .collect();
                return Err(extra_struct_fields(self.table, &extra, path));
            }
            if from.len() < to.len() {
                let missing: Vec<String> = to
                    .iter()
                    .skip(from.len())
                    .map(|f| f.name().clone())
                    .collect();
                return Err(struct_missing_fields(self.table, &missing, path));
            }
            return Ok((0..to.len()).collect());
        }
        let mut picks = Vec::with_capacity(to.len());
        for field in to {
            path.push(backtick(field.name()));
            let picked = self.pick(from, field.name(), path);
            path.pop();
            picks.push(picked?);
        }
        let extra: Vec<String> = from
            .iter()
            .enumerate()
            .filter(|(index, _)| !picks.contains(index))
            .map(|(_, field)| field.name().clone())
            .collect();
        if !extra.is_empty() {
            return Err(extra_struct_fields(self.table, &extra, path));
        }
        Ok(picks)
    }

    fn pair_struct(
        &mut self,
        from: &Fields,
        to: &Fields,
        path: &mut Vec<String>,
    ) -> Result<(Plan, DataType)> {
        let picks = self.picks(from, to, path)?;
        let mut identity = from.len() == to.len();
        let mut children = Vec::with_capacity(to.len());
        let mut fields = Vec::with_capacity(to.len());
        for (position, (index, target)) in picks.iter().zip(to.iter()).enumerate() {
            let source = &from[*index];
            path.push(backtick(target.name()));
            let pair = self.pair(source.data_type(), target.data_type(), path);
            path.pop();
            let (plan, paired) = pair?;
            identity &= *index == position && source.name() == target.name() && plan.is_leaf();
            fields.push(Arc::new(Field::new(
                target.name(),
                paired,
                source.is_nullable(),
            )));
            children.push((*index, plan));
        }
        if identity {
            return Ok((Plan::Leaf, DataType::Struct(from.clone())));
        }
        let paired = DataType::Struct(Fields::from(fields));
        Ok((
            Plan::Node {
                data_type: paired.clone(),
                children,
            },
            paired,
        ))
    }
}

fn wrap_one(plan: Plan, source: &DataType, paired: DataType, child: usize) -> (Plan, DataType) {
    if plan.is_leaf() {
        return (Plan::Leaf, source.clone());
    }
    (
        Plan::Node {
            data_type: paired.clone(),
            children: vec![(child, plan)],
        },
        paired,
    )
}

#[allow(clippy::missing_errors_doc)]
pub fn pair_store_value(
    pairing: StructPairing,
    table: &str,
    column: &str,
    source: &DataType,
    target: &DataType,
) -> Result<StorePairs> {
    pair_store_value_at(pairing, table, &backtick(column), source, target)
}

#[allow(clippy::missing_errors_doc)]
pub fn pair_store_value_at(
    pairing: StructPairing,
    table: &str,
    root: &str,
    source: &DataType,
    target: &DataType,
) -> Result<StorePairs> {
    let mut walk = Walk {
        pairing,
        table,
        leaves: Vec::new(),
    };
    let mut path = vec![root.to_string()];
    let (plan, paired) = walk.pair(source, target, &mut path)?;
    Ok(StorePairs {
        plan,
        paired,
        leaves: walk.leaves,
    })
}

enum Unpaired<'a> {
    Fields(Vec<String>, &'a Fields, &'a Fields),
    Layout(Vec<String>, &'a DataType),
}

fn first_unpaired<'a>(
    source: &'a DataType,
    target: &'a DataType,
    path: &mut Vec<String>,
) -> Option<Unpaired<'a>> {
    match (source, target) {
        (DataType::Dictionary(_, values), _) => first_unpaired(values, target, path),
        (DataType::RunEndEncoded(_, values), _) => first_unpaired(values.data_type(), target, path),
        (DataType::Union(_, _), _) if holds_struct(target) => {
            Some(Unpaired::Layout(path.clone(), source))
        }
        (DataType::Struct(from), DataType::Struct(to)) => {
            let named_alike = from.len() == to.len()
                && from
                    .iter()
                    .zip(to.iter())
                    .all(|(source, target)| source.name() == target.name());
            if !named_alike {
                return Some(Unpaired::Fields(path.clone(), from, to));
            }
            from.iter().zip(to.iter()).find_map(|(source, target)| {
                path.push(backtick(target.name()));
                let found = first_unpaired(source.data_type(), target.data_type(), path);
                path.pop();
                found
            })
        }
        (DataType::Map(from, _), DataType::Map(to, _)) => {
            let ((key, value), (target_key, target_value)) = (map_pair(from)?, map_pair(to)?);
            path.push("`key`".to_string());
            let found = first_unpaired(key.data_type(), target_key.data_type(), path);
            path.pop();
            if found.is_some() {
                return found;
            }
            path.push("`value`".to_string());
            let found = first_unpaired(value.data_type(), target_value.data_type(), path);
            path.pop();
            found
        }
        _ => {
            let (element, target_element) = (list_element(source)?, list_element(target)?);
            path.push("`element`".to_string());
            let found = first_unpaired(element.data_type(), target_element.data_type(), path);
            path.pop();
            found
        }
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_unpaired_struct(
    table: &str,
    entry: StoreEntry,
    column: &str,
    source: &DataType,
    target: &DataType,
) -> Result<()> {
    if !holds_struct(target) {
        return Ok(());
    }
    let mut path = vec![backtick(column)];
    let unpaired = first_unpaired(source, target, &mut path);
    if let Some(Unpaired::Layout(at, layout)) = &unpaired {
        return Err(unpaired_layout(table, entry, at, layout));
    }
    let pairs = pair_store_value(StructPairing::ByPosition, table, column, source, target)?;
    refuse_unassignable_leaves(table, &pairs)?;
    match unpaired {
        Some(Unpaired::Fields(at, from, to)) => Err(unpaired_struct(table, entry, &at, from, to)),
        _ => Ok(()),
    }
}

#[must_use]
pub fn store_pair_sql(
    expr_sql: &str,
    target: &DataType,
    pairing: StructPairing,
    table: &str,
    root: &str,
) -> Option<String> {
    if !holds_struct(target) {
        return None;
    }
    let quote = |text: &str| format!("'{}'", text.replace('\'', "''"));
    Some(format!(
        "{STORE_PAIR_KERNEL_NAME}(({expr_sql}), {}, {}, {}, {})",
        quote(&without_field_metadata(target).to_string()),
        quote(pairing.literal()),
        quote(table),
        quote(root)
    ))
}

#[must_use]
pub fn store_pair_expr(
    value: Expr,
    target: &DataType,
    pairing: StructPairing,
    table: &str,
    column: &str,
) -> Expr {
    let text = |value: &str| Expr::Literal(ScalarValue::Utf8(Some(value.to_string())), None);
    Expr::ScalarFunction(ScalarFunction::new_udf(
        store_pair_udf(),
        vec![
            value,
            text(&without_field_metadata(target).to_string()),
            text(pairing.literal()),
            text(table),
            text(&backtick(column)),
        ],
    ))
}

#[must_use]
pub fn holds_struct_type(data_type: &DataType) -> bool {
    holds_struct(data_type)
}

#[must_use]
pub fn column_root(path: &[String]) -> String {
    path.iter()
        .map(|part| backtick(part))
        .collect::<Vec<_>>()
        .join(".")
}

struct KernelSpec {
    target: DataType,
    pairing: StructPairing,
    table: String,
    root: String,
}

fn kernel_text(value: Option<&ScalarValue>, what: &str) -> Result<String> {
    match value {
        Some(ScalarValue::Utf8(Some(text)) | ScalarValue::Utf8View(Some(text))) => Ok(text.clone()),
        _ => Err(DataFusionError::Plan(format!(
            "{STORE_PAIR_KERNEL_NAME} takes its {what} as a string literal"
        ))),
    }
}

impl KernelSpec {
    fn read(scalars: &[Option<&ScalarValue>]) -> Result<Self> {
        let [_, target, pairing, table, column] = scalars else {
            return Err(DataFusionError::Plan(format!(
                "{STORE_PAIR_KERNEL_NAME} takes five arguments"
            )));
        };
        let target = kernel_text(*target, "target type")?;
        let target = DataType::from_str(&target)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        let pairing = kernel_text(*pairing, "pairing")?;
        let pairing = StructPairing::from_literal(&pairing).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{STORE_PAIR_KERNEL_NAME} has no pairing `{pairing}`"
            ))
        })?;
        Ok(Self {
            target,
            pairing,
            table: kernel_text(*table, "table label")?,
            root: kernel_text(*column, "column")?,
        })
    }

    fn pairs(&self, source: &DataType) -> Result<StorePairs> {
        pair_store_value_at(self.pairing, &self.table, &self.root, source, &self.target)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct StorePairKernel {
    signature: Signature,
}

impl Default for StorePairKernel {
    fn default() -> Self {
        Self {
            signature: Signature::any(5, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StorePairKernel {
    fn name(&self) -> &str {
        STORE_PAIR_KERNEL_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(|| {
            DataFusionError::Plan(format!("{STORE_PAIR_KERNEL_NAME} takes five arguments"))
        })
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let value = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Plan(format!("{STORE_PAIR_KERNEL_NAME} takes five arguments"))
        })?;
        let pairs = KernelSpec::read(args.scalar_arguments)?.pairs(value.data_type())?;
        Ok(Arc::new(Field::new(
            self.name(),
            pairs.paired_type(),
            value.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalars: Vec<Option<ScalarValue>> = args
            .args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Scalar(value) => Some(value.clone()),
                ColumnarValue::Array(_) => None,
            })
            .collect();
        let borrowed: Vec<Option<&ScalarValue>> = scalars.iter().map(Option::as_ref).collect();
        let spec = KernelSpec::read(&borrowed)?;
        let source = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Plan(format!("{STORE_PAIR_KERNEL_NAME} takes five arguments"))
        })?;
        let pairs = spec.pairs(source.data_type())?;
        match args.args.into_iter().next() {
            Some(ColumnarValue::Array(array)) => Ok(ColumnarValue::Array(pairs.apply(&array)?)),
            Some(ColumnarValue::Scalar(value)) => {
                let array = value.to_array()?;
                let paired = pairs.apply(&array)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &paired, 0,
                )?))
            }
            None => Err(DataFusionError::Plan(format!(
                "{STORE_PAIR_KERNEL_NAME} takes five arguments"
            ))),
        }
    }
}

#[must_use]
pub fn store_pair_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(StorePairKernel::default()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StatementPairing {
    pub source_by_name: bool,
    pub case_sensitive: bool,
}

impl ConfigExtension for StatementPairing {
    const PREFIX: &'static str = "repark.store-pairing";
}

impl ExtensionOptions for StatementPairing {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(*self)
    }

    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not settable",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

#[must_use]
pub fn statement_pairing(config: &ConfigOptions, op: &WriteOp) -> StructPairing {
    let carried = config.extensions.get::<StatementPairing>().copied();
    let case_sensitive = carried.is_some_and(|carried| carried.case_sensitive);
    match op {
        WriteOp::Update => StructPairing::by_name(case_sensitive),
        _ => StructPairing::from_source_by_name(
            carried.is_some_and(|carried| carried.source_by_name),
            case_sensitive,
        ),
    }
}

#[cfg(test)]
mod tests;
