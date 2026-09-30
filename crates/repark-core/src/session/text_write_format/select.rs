use std::collections::{HashMap, HashSet};

use arrow::datatypes::{DataType, SchemaRef};
use datafusion::prelude::DataFrame;
use repark_common::Error;

use super::{PatternKind, compile_write_pattern, pattern_failure_error, write_option_patterns};
use crate::session::ReparkSession;
use crate::session_time_zone::{
    TimeParserPolicy, TimeParserPolicyConfig, conf_dump_selects_legacy_policy,
};

fn contains_temporal(data_type: &DataType) -> bool {
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

fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn quote_literal(text: &str) -> String {
    format!("'{}'", text.replace('\\', "\\\\").replace('\'', "''"))
}

fn pattern_argument(pattern: Option<&String>) -> String {
    match pattern {
        Some(text) => quote_literal(text),
        None => "NULL".to_string(),
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

#[allow(clippy::missing_errors_doc)]
pub fn build_text_write_select(
    schema: &SchemaRef,
    view_sql: &str,
    zone_id: &str,
    options: &HashMap<String, String>,
    partition_by: &[String],
) -> crate::Result<String> {
    let (timestamp, ntz, date) = write_option_patterns(options);
    validate_user_patterns(timestamp.as_ref(), ntz.as_ref(), date.as_ref())?;
    let partitions: HashSet<String> = partition_by
        .iter()
        .map(|name| name.to_lowercase())
        .collect();
    let mut items = Vec::with_capacity(schema.fields().len());
    let mut wrapped = 0usize;
    for field in schema.fields() {
        let name = field.name();
        let folded = name.to_lowercase();
        if partitions.contains(&folded) || !contains_temporal(field.data_type()) {
            items.push(quote_ident(name));
            continue;
        }
        items.push(format!(
            "{}({}, {}, {}, {}, {}) AS {}",
            super::udf::WRITE_FORMAT_FUNCTION,
            quote_ident(name),
            pattern_argument(timestamp.as_ref()),
            pattern_argument(ntz.as_ref()),
            pattern_argument(date.as_ref()),
            quote_literal(zone_id),
            quote_ident(name)
        ));
        wrapped += 1;
    }
    if wrapped == 0 {
        return Ok(format!("SELECT * FROM {view_sql}"));
    }
    Ok(format!("SELECT {} FROM {view_sql}", items.join(", ")))
}

impl ReparkSession {
    #[allow(clippy::missing_errors_doc)]
    pub fn text_write_select_sql(
        &self,
        frame: &DataFrame,
        view_sql: &str,
        options: &HashMap<String, String>,
        partition_by: &[String],
    ) -> crate::Result<String> {
        let live = self
            .context()
            .copied_config()
            .options()
            .extensions
            .get::<TimeParserPolicyConfig>()
            .map(|carrier| carrier.policy);
        let legacy = live.map_or_else(
            || conf_dump_selects_legacy_policy(&self.conf_dump()),
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
        build_text_write_select(
            frame.schema().inner(),
            view_sql,
            self.session_time_zone().id(),
            options,
            partition_by,
        )
    }
}
