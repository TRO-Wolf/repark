use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Date32;
use chrono::{DateTime, NaiveDate, Utc};
use datafusion::common::{ScalarValue, internal_err};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use repark_common::zone_horizon::wall_at_instant;

use crate::session_time_zone::session_time_zone_from_options;
use crate::timestamp_cast::parse_session_zone;

#[must_use]
pub fn current_date_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(SparkCurrentDate::new()))
}

#[must_use]
pub fn current_date() -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(current_date_udf(), Vec::new()))
}

pub(crate) fn session_start_days(start: &DateTime<Utc>, zone_id: &str) -> Result<i32> {
    let zone = parse_session_zone(zone_id)?;
    let wall = wall_at_instant(&zone, &start.naive_utc()).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "current_date: session start {start} is out of range"
        ))
    })?;
    epoch_days(wall.date())
}

fn epoch_days(date: NaiveDate) -> Result<i32> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(|| {
        DataFusionError::Internal("1970-01-01 is a valid calendar date".to_string())
    })?;
    let days = date.signed_duration_since(epoch).num_days();
    i32::try_from(days)
        .map_err(|_| DataFusionError::Execution(format!("current_date: {date} is out of range")))
}

#[derive(Debug)]
struct SparkCurrentDate {
    signature: Signature,
    aliases: Vec<String>,
}

impl SparkCurrentDate {
    fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Stable),
            aliases: vec![String::from("today")],
        }
    }
}

impl PartialEq for SparkCurrentDate {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkCurrentDate {}

impl Hash for SparkCurrentDate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkCurrentDate {
    fn name(&self) -> &'static str {
        "current_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Date32)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("invoke should not be called on a simplified current_date() function")
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let Some(start) = info.query_execution_start_time() else {
            return Ok(ExprSimplifyResult::Original(args));
        };
        let zone_id = session_time_zone_from_options(info.config_options());
        let days = session_start_days(&start, zone_id)?;
        Ok(ExprSimplifyResult::Simplified(Expr::Literal(
            ScalarValue::Date32(Some(days)),
            None,
        )))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::AsArray;
    use arrow::datatypes::{DataType, Date32Type};
    use chrono::{DateTime, NaiveDate, TimeZone, Utc};
    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::session_start_days;

    fn days(year: i32, month: u32, day: u32) -> i32 {
        let date = NaiveDate::from_ymd_opt(year, month, day).expect("valid expected date");
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
        let span = date.signed_duration_since(epoch).num_days();
        i32::try_from(span).expect("expected date in range")
    }

    fn ctx_at(zone: &str) -> SessionContext {
        let config = crate::session_time_zone::with_session_time_zone(SessionConfig::new(), zone);
        let ctx = SessionContext::new_with_config(config);
        crate::register_all(&ctx);
        ctx
    }

    async fn single_date(ctx: &SessionContext, sql: &str) -> (i32, DataType) {
        let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
        let field = batches[0].schema().field(0).clone();
        let value = batches[0].column(0).as_primitive::<Date32Type>().value(0);
        (value, field.data_type().clone())
    }

    #[test]
    fn fixed_instants_land_on_the_zone_date() {
        let start = Utc
            .with_ymd_and_hms(2026, 10, 10, 22, 0, 0)
            .single()
            .expect("valid fixed start");
        for (zone, year, month, day) in [
            ("UTC", 2026, 10, 10),
            ("Pacific/Pago_Pago", 2026, 10, 10),
            ("America/Los_Angeles", 2026, 10, 10),
            ("Asia/Tokyo", 2026, 10, 11),
            ("Pacific/Kiritimati", 2026, 10, 11),
            ("+14:00", 2026, 10, 11),
            ("-08:00", 2026, 10, 10),
        ] {
            let got = session_start_days(&start, zone).expect(zone);
            assert_eq!(got, days(year, month, day), "zone {zone}");
        }
    }

    #[test]
    fn midnight_boundary_instant_splits_adjacent_zones() {
        let start = Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 30, 0)
            .single()
            .expect("valid boundary start");
        let east = session_start_days(&start, "Pacific/Kiritimati").expect("east");
        let west = session_start_days(&start, "Pacific/Pago_Pago").expect("west");
        assert_eq!(east, days(2026, 1, 1));
        assert_eq!(west, days(2025, 12, 31));
    }

    #[test]
    fn unknown_zone_refuses_like_the_cast_sibling() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).expect("valid start");
        let error = session_start_days(&start, "Mars/Olympus").expect_err("unknown zone refuses");
        assert!(
            error.to_string().contains("Mars/Olympus"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn sql_answers_the_session_zone_date() {
        let probe = Utc::now();
        let kiritimati = session_start_days(&probe, "Pacific/Kiritimati").expect("kiritimati");
        let utc = session_start_days(&probe, "UTC").expect("utc");
        let (zone, expected) = if kiritimati == utc {
            let pago = session_start_days(&probe, "Pacific/Pago_Pago").expect("pago");
            ("Pacific/Pago_Pago", pago)
        } else {
            ("Pacific/Kiritimati", kiritimati)
        };
        let (got, data_type) = single_date(&ctx_at(zone), "SELECT current_date()").await;
        assert_eq!(data_type, DataType::Date32);
        assert_eq!(got, expected, "zone {zone}");
    }

    #[tokio::test]
    async fn sql_under_utc_answers_the_utc_date() {
        let expected = session_start_days(&Utc::now(), "UTC").expect("utc");
        let (got, data_type) = single_date(&ctx_at("UTC"), "SELECT current_date()").await;
        assert_eq!(data_type, DataType::Date32);
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn today_alias_and_repeat_reference_agree() {
        let (zone, expected) = {
            let probe = Utc::now();
            let kiritimati = session_start_days(&probe, "Pacific/Kiritimati").expect("kiritimati");
            let utc = session_start_days(&probe, "UTC").expect("utc");
            if kiritimati == utc {
                let pago = session_start_days(&probe, "Pacific/Pago_Pago").expect("pago");
                ("Pacific/Pago_Pago", pago)
            } else {
                ("Pacific/Kiritimati", kiritimati)
            }
        };
        let ctx = ctx_at(zone);
        let batches = ctx
            .sql("SELECT current_date() AS a, today() AS b, current_date() AS c")
            .await
            .expect("sql")
            .collect()
            .await
            .expect("collect");
        let row = &batches[0];
        for name in ["a", "b", "c"] {
            let value = row
                .column_by_name(name)
                .expect(name)
                .as_primitive::<Date32Type>()
                .value(0);
            assert_eq!(value, expected, "column {name} in zone {zone}");
        }
    }
}
