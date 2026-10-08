use std::collections::{HashMap, HashSet};

use arrow::datatypes::{DataType, SchemaRef};
use datafusion::prelude::DataFrame;
use repark_common::Error;

use super::file_format::{TEXT_CSV_FORMAT_NAME, TEXT_JSON_FORMAT_NAME};
use super::spec::TextWriteSpec;
use super::{PatternKind, compile_write_pattern, pattern_failure_error, write_option_patterns};
use crate::session::ReparkSession;
use crate::session::df_guards::duplicate_names::display_name;
use crate::session_time_zone::{
    TimeParserPolicy, TimeParserPolicyConfig, conf_dump_selects_legacy_policy,
};

pub(crate) fn contains_temporal(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => true,
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            contains_temporal(field.data_type())
        }
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_temporal(field.data_type())),
        DataType::Map(field, _) => match field.data_type() {
            DataType::Struct(entries) => entries
                .iter()
                .enumerate()
                .any(|(position, entry)| position == 1 && contains_temporal(entry.data_type())),
            _ => false,
        },
        _ => false,
    }
}

fn validate_user_patterns(
    timestamp: Option<&String>,
    ntz: Option<&String>,
    date: Option<&String>,
) -> crate::Result<()> {
    for (pattern, kind) in [
        (timestamp, PatternKind::Timestamp),
        (ntz, PatternKind::TimestampNtz),
        (date, PatternKind::Date),
    ] {
        if let Some(text) = pattern
            && let Err(failure) = compile_write_pattern(text, kind)
        {
            return Err(pattern_failure_error(&failure));
        }
    }
    Ok(())
}

pub(crate) fn text_write_needs_format(schema: &SchemaRef, partition_by: &[String]) -> bool {
    let partitions: HashSet<String> = partition_by
        .iter()
        .map(|name| name.to_lowercase())
        .collect();
    schema.fields().iter().any(|field| {
        !partitions.contains(&field.name().to_lowercase()) && contains_temporal(field.data_type())
    })
}

#[derive(Debug)]
pub struct TextWriteCopyParts {
    pub select_sql: String,
    pub stored_as: String,
    pub spec_options_sql: String,
}

pub(crate) fn merge_spec_options(base: String, spec_sql: &str) -> String {
    if spec_sql.is_empty() {
        return base;
    }
    if base.is_empty() {
        return format!(" OPTIONS ({spec_sql})");
    }
    match base.strip_suffix(')') {
        Some(head) => format!("{head}, {spec_sql})"),
        None => base,
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn build_text_write_copy_parts(
    schema: &SchemaRef,
    view_sql: &str,
    zone_id: &str,
    options: &HashMap<String, String>,
    partition_by: &[String],
    stored_as: &str,
) -> crate::Result<TextWriteCopyParts> {
    let (timestamp, ntz, date) = write_option_patterns(options);
    validate_user_patterns(timestamp.as_ref(), ntz.as_ref(), date.as_ref())?;
    let display_header = stored_as.eq_ignore_ascii_case("csv")
        && schema
            .fields()
            .iter()
            .any(|field| display_name(field.name()).is_some());
    if !display_header && !text_write_needs_format(schema, partition_by) {
        return Ok(TextWriteCopyParts {
            select_sql: format!("SELECT * FROM {view_sql}"),
            stored_as: stored_as.to_string(),
            spec_options_sql: String::new(),
        });
    }
    let resolved = match stored_as.to_ascii_lowercase().as_str() {
        "csv" => TEXT_CSV_FORMAT_NAME.to_string(),
        "json" => TEXT_JSON_FORMAT_NAME.to_string(),
        _ => stored_as.to_string(),
    };
    Ok(TextWriteCopyParts {
        select_sql: format!("SELECT * FROM {view_sql}"),
        stored_as: resolved,
        spec_options_sql: TextWriteSpec::options_sql(
            zone_id,
            timestamp.as_deref(),
            ntz.as_deref(),
            date.as_deref(),
            display_header,
        ),
    })
}

impl ReparkSession {
    #[allow(clippy::missing_errors_doc)]
    pub fn text_write_copy_parts(
        &self,
        frame: &DataFrame,
        view_sql: &str,
        options: &HashMap<String, String>,
        partition_by: &[String],
        stored_as: &str,
    ) -> crate::Result<TextWriteCopyParts> {
        let live = self
            .context()
            .copied_config()
            .options()
            .extensions
            .get::<TimeParserPolicyConfig>()
            .map(|carrier| carrier.policy);
        let legacy = live.map_or_else(
            || conf_dump_selects_legacy_policy(&self.conf_dump),
            TimeParserPolicy::is_legacy,
        );
        if legacy {
            let temporal_columns = frame
                .schema()
                .inner()
                .fields()
                .iter()
                .any(|field| contains_temporal(field.data_type()));
            if temporal_columns {
                return Err(Error::Analysis(
                    "CSV/JSON text writes with spark.sql.legacy.timeParserPolicy=LEGACY are not \
                     supported yet (legacy SimpleDateFormat rendering is not implemented; unset \
                     the policy or set it to CORRECTED)"
                        .to_string(),
                ));
            }
        }
        build_text_write_copy_parts(
            frame.schema().inner(),
            view_sql,
            self.session_time_zone().id(),
            options,
            partition_by,
            stored_as,
        )
    }
}
