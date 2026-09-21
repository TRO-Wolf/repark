use std::collections::{HashMap, HashSet};

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Value, ValueWithSpan,
};
use iceberg::maintenance::RewriteStrategy;
use iceberg::spec::TableProperties;
use iceberg::table::Table;
use repark_core::illegal_argument_error;
use repark_functions::java_double::java_double_text;

use crate::call_args::BoundArgs;
use crate::iceberg_err;

const RDF_ACCEPTED: &[&str] = &[
    "target-file-size-bytes",
    "min-file-size-bytes",
    "max-file-size-bytes",
    "min-input-files",
    "rewrite-all",
    "max-file-group-size-bytes",
    "delete-file-threshold",
    "delete-ratio-threshold",
    "use-starting-sequence-number",
    "remove-dangling-deletes",
    "output-spec-id",
    "rewrite-job-order",
    "partial-progress.enabled",
    "partial-progress.max-commits",
    "partial-progress.max-failed-commits",
    "max-concurrent-file-group-rewrites",
];

const RPD_ACCEPTED: &[&str] = &[
    "target-file-size-bytes",
    "min-file-size-bytes",
    "max-file-size-bytes",
    "min-input-files",
    "rewrite-all",
    "max-file-group-size-bytes",
    "rewrite-job-order",
    "partial-progress.enabled",
    "partial-progress.max-commits",
    "max-concurrent-file-group-rewrites",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum RewriteJobOrder {
    #[default]
    None,
    FilesAsc,
    FilesDesc,
    BytesAsc,
    BytesDesc,
}

impl From<RewriteJobOrder> for iceberg::maintenance::RewriteJobOrder {
    fn from(order: RewriteJobOrder) -> Self {
        match order {
            RewriteJobOrder::None => Self::None,
            RewriteJobOrder::FilesAsc => Self::FilesAsc,
            RewriteJobOrder::FilesDesc => Self::FilesDesc,
            RewriteJobOrder::BytesAsc => Self::BytesAsc,
            RewriteJobOrder::BytesDesc => Self::BytesDesc,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RewriteOptions {
    pub(crate) strategy: RewriteStrategy,
    pub(crate) shuffle_partitions_per_file: Option<usize>,
    pub(crate) compression_factor: Option<f64>,
    pub(crate) target_file_size_bytes: Option<u64>,
    pub(crate) min_file_size_bytes: Option<i64>,
    pub(crate) max_file_size_bytes: Option<i64>,
    pub(crate) min_input_files: Option<usize>,
    pub(crate) delete_file_threshold: Option<usize>,
    pub(crate) delete_ratio_threshold: Option<f64>,
    pub(crate) max_file_group_size_bytes: Option<u64>,
    pub(crate) use_starting_sequence_number: Option<bool>,
    pub(crate) remove_dangling_deletes: Option<bool>,
    pub(crate) rewrite_all: bool,
    pub(crate) output_spec_id: Option<i64>,
    pub(crate) rewrite_job_order: RewriteJobOrder,
    pub(crate) partial_progress_enabled: bool,
    pub(crate) partial_progress_max_commits: Option<i64>,
    pub(crate) partial_progress_max_failed_commits: Option<i64>,
    pub(crate) max_concurrent_file_group_rewrites: Option<i64>,
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn extract_option_pairs(
    bound: &BoundArgs,
    procedure: &str,
) -> Result<Vec<(String, Option<String>)>> {
    let Some(expr) = bound.get("options") else {
        return Ok(Vec::new());
    };
    match expr {
        Expr::Function(function) if function.name.to_string().eq_ignore_ascii_case("map") => {
            pairs_from_map_args(&function.args, procedure, "options")
        }
        other => Err(DataFusionError::Plan(format!(
            "CALL {procedure} argument `options` must be map(k, v, …), got {other}"
        ))),
    }
}

#[allow(clippy::missing_errors_doc)]
pub(in crate::call) fn pairs_from_map_args(
    args: &FunctionArguments,
    procedure: &str,
    argument: &str,
) -> Result<Vec<(String, Option<String>)>> {
    let FunctionArguments::List(list) = args else {
        return Err(DataFusionError::Plan(format!(
            "CALL {procedure} argument `{argument}` must be map(k, v, …)"
        )));
    };
    let mut exprs = Vec::with_capacity(list.args.len());
    for arg in &list.args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => exprs.push(expr),
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "CALL {procedure} argument `{argument}` must be map(k, v, …) of string literals"
                )));
            }
        }
    }
    if exprs.len() % 2 != 0 {
        return Err(DataFusionError::Plan(format!(
            "CALL {procedure} argument `{argument}` must list key/value pairs (got {} arguments)",
            exprs.len()
        )));
    }
    let mut pairs = Vec::with_capacity(exprs.len() / 2);
    let mut seen = HashSet::with_capacity(exprs.len() / 2);
    for chunk in exprs.chunks_exact(2) {
        let key = scalar_text(chunk[0], procedure, argument)?.ok_or_else(|| {
            DataFusionError::Plan(format!(
                "CALL {procedure} argument `{argument}` map keys must be string literals"
            ))
        })?;
        if !seen.insert(key.clone()) {
            return Err(DataFusionError::Execution(format!(
                "[DUPLICATED_MAP_KEY] Duplicate map key {key} was found, please check the input data.\nIf you want to remove the duplicated keys, you can set \"spark.sql.mapKeyDedupPolicy\" to \"LAST_WIN\" so that the key inserted at last takes precedence. SQLSTATE: 23505"
            )));
        }
        pairs.push((key, scalar_text(chunk[1], procedure, argument)?));
    }
    Ok(pairs)
}

#[allow(clippy::missing_errors_doc)]
fn scalar_text(expr: &Expr, procedure: &str, argument: &str) -> Result<Option<String>> {
    match expr {
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            Value::SingleQuotedString(text)
            | Value::DoubleQuotedString(text)
            | Value::NationalStringLiteral(text) => Ok(Some(text.clone())),
            Value::Number(raw, _) => Ok(Some(raw.clone())),
            Value::Boolean(flag) => Ok(Some(flag.to_string())),
            Value::Null => Ok(None),
            _ => Err(DataFusionError::Plan(format!(
                "CALL {procedure} argument `{argument}` map keys and values must be string literals"
            ))),
        },
        _ => Err(DataFusionError::Plan(format!(
            "CALL {procedure} argument `{argument}` map keys and values must be string literals"
        ))),
    }
}

fn number_format(raw: &str) -> DataFusionError {
    illegal_argument_error(format!("For input string: \"{raw}\""))
}

fn illegal_argument(message: String) -> DataFusionError {
    illegal_argument_error(message)
}

fn parse_java_long(raw: &str) -> std::result::Result<i64, DataFusionError> {
    raw.strip_prefix('+')
        .unwrap_or(raw)
        .parse::<i64>()
        .map_err(|_| number_format(raw))
}

fn parse_java_double(raw: &str) -> std::result::Result<f64, DataFusionError> {
    if raw.eq_ignore_ascii_case("inf")
        || raw.eq_ignore_ascii_case("+inf")
        || raw.eq_ignore_ascii_case("-inf")
    {
        return Err(number_format(raw));
    }
    raw.parse::<f64>().map_err(|_| number_format(raw))
}

struct TypedValues {
    longs: HashMap<String, i64>,
    ratio: Option<f64>,
    compression_factor: Option<f64>,
    job_order: Option<String>,
    spec_id: Option<i64>,
}

fn typed_values(pairs: &[(String, Option<String>)]) -> Result<TypedValues> {
    let mut longs = HashMap::with_capacity(pairs.len());
    let mut ratio = None;
    let mut compression_factor = None;
    let mut job_order = None;
    let mut spec_id = None;
    for (key, value) in pairs {
        let Some(raw) = value else { continue };
        match key.as_str() {
            "delete-ratio-threshold" => ratio = Some(parse_java_double(raw)?),
            "compression-factor" => compression_factor = Some(parse_java_double(raw)?),
            "rewrite-job-order" => job_order = Some(raw.clone()),
            "output-spec-id" => spec_id = Some(parse_java_long(raw)?),
            "rewrite-all"
            | "use-starting-sequence-number"
            | "remove-dangling-deletes"
            | "partial-progress.enabled" => {}
            _ => {
                longs.insert(key.clone(), parse_java_long(raw)?);
            }
        }
    }
    Ok(TypedValues {
        longs,
        ratio,
        compression_factor,
        job_order,
        spec_id,
    })
}

fn bool_option(pairs: &[(String, Option<String>)], key: &str) -> Option<bool> {
    pairs
        .iter()
        .find(|(pair_key, _)| pair_key == key)
        .and_then(|(_, value)| value.as_ref())
        .map(|raw| raw.eq_ignore_ascii_case("true"))
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn has_option_key(pairs: &[(String, Option<String>)], key: &str) -> bool {
    pairs.iter().any(|(pair_key, _)| pair_key == key)
}

fn reject_unknown(pairs: &[(String, Option<String>)], accepted: &[&str]) -> Result<()> {
    reject_unknown_for(pairs, accepted, "BIN-PACK")
}

fn reject_unknown_for(
    pairs: &[(String, Option<String>)],
    accepted: &[&str],
    rewriter: &str,
) -> Result<()> {
    let unknown: Vec<&str> = pairs
        .iter()
        .map(|(key, _)| key.as_str())
        .filter(|key| !accepted.contains(key))
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    Err(illegal_argument(format!(
        "Cannot use options [{}], they are not supported by the action or the rewriter {rewriter}",
        unknown.join(", ")
    )))
}

fn layout_int(values: &TypedValues, key: &str, out_of_range: &str) -> Result<Option<u32>> {
    let Some(value) = long_value(values, key) else {
        return Ok(None);
    };
    match u32::try_from(value) {
        Ok(fits) if value <= i64::from(i32::MAX) => Ok(Some(fits)),
        _ => Err(illegal_argument(
            out_of_range.replace("{value}", &value.to_string()),
        )),
    }
}

fn layout_strategy(strategy: RewriteStrategy, values: &TypedValues) -> Result<RewriteStrategy> {
    let RewriteStrategy::ZOrder(mut spec) = strategy else {
        return Ok(strategy);
    };
    if let Some(bytes) = layout_int(
        values,
        "var-length-contribution",
        "Cannot use less than 1 byte for variable length types with ZOrder, \
         'var-length-contribution' was set to {value}",
    )? {
        spec = spec.var_length_contribution(bytes);
    }
    if let Some(bytes) = layout_int(
        values,
        "max-output-size",
        "Cannot have the interleaved ZOrder value use less than 1 byte, 'max-output-size' \
         was set to {value}",
    )? {
        spec = spec.max_output_size(bytes);
    }
    Ok(RewriteStrategy::ZOrder(spec))
}

fn job_order_from(raw: &str) -> Result<RewriteJobOrder> {
    if raw.eq_ignore_ascii_case("none") {
        return Ok(RewriteJobOrder::None);
    }
    if raw.eq_ignore_ascii_case("files-asc") {
        return Ok(RewriteJobOrder::FilesAsc);
    }
    if raw.eq_ignore_ascii_case("files-desc") {
        return Ok(RewriteJobOrder::FilesDesc);
    }
    if raw.eq_ignore_ascii_case("bytes-asc") {
        return Ok(RewriteJobOrder::BytesAsc);
    }
    if raw.eq_ignore_ascii_case("bytes-desc") {
        return Ok(RewriteJobOrder::BytesDesc);
    }
    Err(illegal_argument(format!(
        "Invalid rewrite job order name: {raw}"
    )))
}

fn long_value(values: &TypedValues, key: &str) -> Option<i64> {
    values.longs.get(key).copied()
}

fn usize_value(values: &TypedValues, key: &str, procedure: &str) -> Result<Option<usize>> {
    match long_value(values, key) {
        None => Ok(None),
        Some(value) => usize::try_from(value).map(Some).map_err(|_| {
            DataFusionError::Plan(format!(
                "CALL {procedure} option `{key}` value {value} does not fit usize"
            ))
        }),
    }
}

fn data_target_default(properties: &HashMap<String, String>) -> Result<u64> {
    match properties.get(TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES) {
        None => Ok(TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT as u64),
        Some(raw) => raw.parse::<u64>().map_err(|_| {
            iceberg_err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!(
                    "Invalid value '{raw}' for table property '{}'",
                    TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES
                ),
            ))
        }),
    }
}

fn delete_target_default(properties: &HashMap<String, String>) -> Result<u64> {
    match properties.get(TableProperties::PROPERTY_WRITE_DELETE_TARGET_FILE_SIZE_BYTES) {
        None => Ok(TableProperties::PROPERTY_WRITE_DELETE_TARGET_FILE_SIZE_BYTES_DEFAULT),
        Some(raw) => raw.parse::<u64>().map_err(|_| {
            iceberg_err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!(
                    "Invalid value '{raw}' for table property '{}'",
                    TableProperties::PROPERTY_WRITE_DELETE_TARGET_FILE_SIZE_BYTES
                ),
            ))
        }),
    }
}

#[allow(clippy::missing_errors_doc, clippy::too_many_lines)]
pub(crate) fn parse_rdf_options(
    pairs: &[(String, Option<String>)],
    table: &Table,
    strategy: RewriteStrategy,
) -> Result<RewriteOptions> {
    let mut accepted: Vec<&str> = RDF_ACCEPTED.to_vec();
    accepted.extend_from_slice(strategy.valid_option_names());
    reject_unknown_for(pairs, &accepted, strategy.description())?;
    let values = typed_values(pairs)?;
    let mut options = RewriteOptions {
        strategy: layout_strategy(strategy, &values)?,
        shuffle_partitions_per_file: layout_int(
            &values,
            "shuffle-partitions-per-file",
            "'shuffle-partitions-per-file' is set to {value} but must be > 0",
        )?
        .map(|count| count as usize),
        compression_factor: values.compression_factor,
        ..Default::default()
    };
    if let Some(raw) = &values.job_order {
        options.rewrite_job_order = job_order_from(raw)?;
    }
    if let Some(spec) = values.spec_id {
        let known = i32::try_from(spec)
            .ok()
            .and_then(|id| table.metadata().partition_spec_by_id(id))
            .is_some();
        if !known {
            return Err(illegal_argument(format!(
                "Cannot use output spec id {spec} because the table does not contain a reference to this spec-id."
            )));
        }
        options.output_spec_id = Some(spec);
    }
    if let Some(value) = long_value(&values, "max-concurrent-file-group-rewrites")
        && value <= 0
    {
        return Err(illegal_argument(format!(
            "Cannot set max-concurrent-file-group-rewrites to {value}, the value must be positive."
        )));
    }
    options.max_concurrent_file_group_rewrites =
        long_value(&values, "max-concurrent-file-group-rewrites");
    options.partial_progress_enabled =
        bool_option(pairs, "partial-progress.enabled").unwrap_or(false);
    options.partial_progress_max_commits = long_value(&values, "partial-progress.max-commits");
    options.partial_progress_max_failed_commits =
        long_value(&values, "partial-progress.max-failed-commits");
    if options.partial_progress_enabled
        && let Some(value) = options.partial_progress_max_commits
        && value <= 0
    {
        return Err(illegal_argument(format!(
            "Cannot set partial-progress.max-commits to {value}, the value must be positive when partial-progress.enabled is true"
        )));
    }
    let properties = table.metadata().properties();
    let default_target = data_target_default(properties)?;
    let target = match long_value(&values, "target-file-size-bytes") {
        None => default_target,
        Some(value) => {
            if value <= 0 {
                return Err(illegal_argument(format!(
                    "'target-file-size-bytes' is set to {value} but must be > 0"
                )));
            }
            u64::try_from(value).map_err(|_| {
                DataFusionError::Plan(format!(
                    "CALL rewrite_data_files option `target-file-size-bytes` value {value} does not fit u64"
                ))
            })?
        }
    };
    if let Some(min) = long_value(&values, "min-file-size-bytes")
        && i128::from(target) <= i128::from(min)
    {
        return Err(illegal_argument(format!(
            "'target-file-size-bytes' ({target}) must be > 'min-file-size-bytes' ({min}), all new files will be smaller than the min threshold"
        )));
    }
    if let Some(value) = long_value(&values, "min-file-size-bytes")
        && value < 0
    {
        return Err(illegal_argument(format!(
            "'min-file-size-bytes' is set to {value} but must be >= 0"
        )));
    }
    if let Some(min) = long_value(&values, "min-file-size-bytes")
        && i128::from(target) <= i128::from(min)
    {
        return Err(illegal_argument(format!(
            "'target-file-size-bytes' ({target}) must be > 'min-file-size-bytes' ({min}), all new files will be smaller than the min threshold"
        )));
    }
    if let Some(max) = long_value(&values, "max-file-size-bytes")
        && i128::from(target) >= i128::from(max)
    {
        return Err(illegal_argument(format!(
            "'target-file-size-bytes' ({target}) must be < 'max-file-size-bytes' ({max}), all new files will be larger than the max threshold"
        )));
    }
    if let Some(value) = long_value(&values, "min-input-files")
        && value <= 0
    {
        return Err(illegal_argument(format!(
            "'min-input-files' is set to {value} but must be > 0"
        )));
    }
    if let Some(value) = long_value(&values, "max-file-group-size-bytes")
        && value <= 0
    {
        return Err(illegal_argument(format!(
            "'max-file-group-size-bytes' is set to {value} but must be > 0"
        )));
    }
    if let Some(value) = long_value(&values, "delete-file-threshold")
        && value < 0
    {
        return Err(illegal_argument(format!(
            "'delete-file-threshold' is set to {value} but must be >= 0"
        )));
    }
    if let Some(ratio) = values.ratio {
        if ratio.is_nan() || ratio <= 0.0 {
            return Err(illegal_argument(format!(
                "'delete-ratio-threshold' is set to {} but must be > 0",
                java_double_text(ratio)
            )));
        }
        if ratio > 1.0 {
            return Err(illegal_argument(format!(
                "'delete-ratio-threshold' is set to {} but must be <= 1",
                java_double_text(ratio)
            )));
        }
        options.delete_ratio_threshold = Some(ratio);
    }
    options.target_file_size_bytes = long_value(&values, "target-file-size-bytes").map(|_| target);
    options.min_file_size_bytes = long_value(&values, "min-file-size-bytes");
    options.max_file_size_bytes = long_value(&values, "max-file-size-bytes");
    options.min_input_files = usize_value(&values, "min-input-files", "rewrite_data_files")?;
    options.max_file_group_size_bytes =
        usize_value(&values, "max-file-group-size-bytes", "rewrite_data_files")?
            .map(|value| value as u64);
    options.delete_file_threshold =
        usize_value(&values, "delete-file-threshold", "rewrite_data_files")?;
    options.use_starting_sequence_number = bool_option(pairs, "use-starting-sequence-number");
    options.remove_dangling_deletes = bool_option(pairs, "remove-dangling-deletes");
    options.rewrite_all = bool_option(pairs, "rewrite-all").unwrap_or(false);
    Ok(options)
}

const RPD_UNWIRED: &[&str] = &[
    "rewrite-job-order",
    "partial-progress.enabled",
    "partial-progress.max-commits",
    "max-concurrent-file-group-rewrites",
];

#[allow(clippy::missing_errors_doc)]
pub(crate) fn refuse_rpd_unwired(pairs: &[(String, Option<String>)]) -> Result<()> {
    let hit: Vec<&str> = pairs
        .iter()
        .map(|(key, _)| key.as_str())
        .filter(|key| RPD_UNWIRED.contains(key))
        .collect();
    if hit.is_empty() {
        return Ok(());
    }
    Err(DataFusionError::NotImplemented(format!(
        "CALL rewrite_position_delete_files option [{}] is not supported in RePark \
         (registry ICE-RDF-OPTIONS-1, 2026-09-17): the fork's RewritePositionDeleteFiles has no \
         rewrite-job-order / partial-progress / concurrent-group path — run without the key for \
         Spark's default single-commit shape",
        hit.join(", ")
    )))
}

#[allow(clippy::missing_errors_doc, clippy::too_many_lines)]
pub(crate) fn parse_rpd_options(
    pairs: &[(String, Option<String>)],
    table: &Table,
) -> Result<RewriteOptions> {
    reject_unknown(pairs, RPD_ACCEPTED)?;
    let values = typed_values(pairs)?;
    let mut options = RewriteOptions::default();
    if let Some(raw) = &values.job_order {
        options.rewrite_job_order = job_order_from(raw)?;
    }
    if let Some(value) = long_value(&values, "max-concurrent-file-group-rewrites")
        && value <= 0
    {
        return Err(illegal_argument(format!(
            "Cannot set max-concurrent-file-group-rewrites to {value}, the value must be positive."
        )));
    }
    options.max_concurrent_file_group_rewrites =
        long_value(&values, "max-concurrent-file-group-rewrites");
    options.partial_progress_enabled =
        bool_option(pairs, "partial-progress.enabled").unwrap_or(false);
    options.partial_progress_max_commits = long_value(&values, "partial-progress.max-commits");
    if options.partial_progress_enabled
        && let Some(value) = options.partial_progress_max_commits
        && value <= 0
    {
        return Err(illegal_argument(format!(
            "Cannot set partial-progress.max-commits to {value}, the value must be positive when partial-progress.enabled is true"
        )));
    }
    refuse_rpd_unwired(pairs)?;
    let default_target = delete_target_default(table.metadata().properties())?;
    let target = match long_value(&values, "target-file-size-bytes") {
        None => default_target,
        Some(value) => {
            if value <= 0 {
                return Err(illegal_argument(format!(
                    "'target-file-size-bytes' is set to {value} but must be > 0"
                )));
            }
            u64::try_from(value).map_err(|_| {
                DataFusionError::Plan(format!(
                    "CALL rewrite_position_delete_files option `target-file-size-bytes` value {value} does not fit u64"
                ))
            })?
        }
    };
    if let Some(value) = long_value(&values, "min-file-size-bytes")
        && value < 0
    {
        return Err(illegal_argument(format!(
            "'min-file-size-bytes' is set to {value} but must be >= 0"
        )));
    }
    if let Some(min) = long_value(&values, "min-file-size-bytes")
        && i128::from(target) <= i128::from(min)
    {
        return Err(illegal_argument(format!(
            "'target-file-size-bytes' ({target}) must be > 'min-file-size-bytes' ({min}), all new files will be smaller than the min threshold"
        )));
    }
    if let Some(max) = long_value(&values, "max-file-size-bytes")
        && i128::from(target) >= i128::from(max)
    {
        return Err(illegal_argument(format!(
            "'target-file-size-bytes' ({target}) must be < 'max-file-size-bytes' ({max}), all new files will be larger than the max threshold"
        )));
    }
    if let Some(value) = long_value(&values, "min-input-files")
        && value <= 0
    {
        return Err(illegal_argument(format!(
            "'min-input-files' is set to {value} but must be > 0"
        )));
    }
    if let Some(value) = long_value(&values, "max-file-group-size-bytes")
        && value <= 0
    {
        return Err(illegal_argument(format!(
            "'max-file-group-size-bytes' is set to {value} but must be > 0"
        )));
    }
    options.target_file_size_bytes = long_value(&values, "target-file-size-bytes").map(|_| target);
    options.min_file_size_bytes = long_value(&values, "min-file-size-bytes");
    options.max_file_size_bytes = long_value(&values, "max-file-size-bytes");
    options.min_input_files =
        usize_value(&values, "min-input-files", "rewrite_position_delete_files")?;
    options.max_file_group_size_bytes = usize_value(
        &values,
        "max-file-group-size-bytes",
        "rewrite_position_delete_files",
    )?
    .map(|value| value as u64);
    options.rewrite_all = bool_option(pairs, "rewrite-all").unwrap_or(false);
    Ok(options)
}
