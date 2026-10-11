use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{DataFrame, SessionContext};
use repark_functions::null_narrowing::{NARROWED_BESIDE_NULL_NAME, NARROWED_BESIDE_VALUE_NAME};

const SAME_TEXT: &str = "SAME TEXT AS ABOVE";
const FINAL_PLAN: &str = "logical_plan";

pub(crate) async fn explain(ctx: &SessionContext, plan: LogicalPlan) -> Result<DataFrame> {
    let batches = ctx.execute_logical_plan(plan).await?.collect().await?;
    let mut shown: Option<String> = None;
    let cleared = batches
        .iter()
        .map(|batch| clear_batch(batch, &mut shown))
        .collect::<Result<Vec<RecordBatch>>>()?;
    ctx.read_batches(cleared)
}

fn clear_batch(batch: &RecordBatch, shown: &mut Option<String>) -> Result<RecordBatch> {
    let kinds = batch.column_by_name("plan_type").map(Arc::clone);
    let columns = batch
        .columns()
        .iter()
        .zip(batch.schema().fields())
        .map(|(column, field)| {
            if field.name() != "plan" || column.data_type() != &DataType::Utf8 {
                return Arc::clone(column);
            }
            let texts = column.as_string::<i32>();
            let kind = |row: usize| match kinds.as_ref() {
                Some(kinds) if kinds.data_type() == &DataType::Utf8 => {
                    kinds.as_string::<i32>().value(row)
                }
                _ => FINAL_PLAN,
            };
            let cleared: StringArray = (0..texts.len())
                .map(|row| {
                    if texts.is_null(row) {
                        return None;
                    }
                    let text = texts.value(row);
                    if text == SAME_TEXT || !kind(row).contains(FINAL_PLAN) {
                        return Some(text.to_string());
                    }
                    let plain = strip_marks(text);
                    let repeats = shown.as_deref() == Some(plain.as_str());
                    *shown = Some(plain.clone());
                    if repeats && kind(row) != FINAL_PLAN {
                        return Some(SAME_TEXT.to_string());
                    }
                    Some(plain)
                })
                .collect();
            Arc::new(cleared) as ArrayRef
        })
        .collect();
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

fn strip_marks(text: &str) -> String {
    let mut plain = text.to_string();
    for name in [NARROWED_BESIDE_NULL_NAME, NARROWED_BESIDE_VALUE_NAME] {
        let call = format!("{name}(");
        while let Some(start) = plain.find(&call) {
            let open = start + call.len();
            let Some(close) = closing(&plain, open) else {
                break;
            };
            plain.replace_range(close..=close, "");
            plain.replace_range(start..open, "");
        }
    }
    plain
}

fn closing(text: &str, open: usize) -> Option<usize> {
    let mut depth = 0_usize;
    let mut quoted = false;
    for (offset, letter) in text[open..].char_indices() {
        match letter {
            '"' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted && depth == 0 => return Some(open + offset),
            ')' if !quoted => depth -= 1,
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mark_is_erased_from_a_printed_plan_and_its_argument_kept() {
        let marked = format!(
            "Projection: coalesce(CAST({NARROWED_BESIDE_VALUE_NAME}(t.c + f(\")\", 1)) AS \
             Timestamp(µs, \"UTC\")), {NARROWED_BESIDE_NULL_NAME}(t.d)) AS v"
        );
        assert_eq!(
            strip_marks(&marked),
            "Projection: coalesce(CAST(t.c + f(\")\", 1) AS Timestamp(µs, \"UTC\")), t.d) AS v"
        );
        assert_eq!(strip_marks("Projection: t.c"), "Projection: t.c");
    }

    #[test]
    fn a_row_that_repeats_the_row_above_is_collapsed_and_the_final_plan_is_not() {
        let marked = format!("Projection: f({NARROWED_BESIDE_VALUE_NAME}(t.c))");
        let kinds = StringArray::from(vec![
            "initial_logical_plan",
            "logical_plan after a",
            "logical_plan after b",
            "logical_plan after c",
            "logical_plan",
            "physical_plan",
        ]);
        let plans = StringArray::from(vec![
            "Projection: f(t.c)",
            marked.as_str(),
            SAME_TEXT,
            "Projection: g(t.c)",
            "Projection: g(t.c)",
            "ProjectionExec",
        ]);
        let batch = RecordBatch::try_from_iter(vec![
            ("plan_type", Arc::new(kinds) as ArrayRef),
            ("plan", Arc::new(plans) as ArrayRef),
        ])
        .unwrap();
        let cleared = clear_batch(&batch, &mut None).unwrap();
        let shown: Vec<&str> = (cleared.column(1).as_string::<i32>().iter())
            .flatten()
            .collect();
        assert_eq!(
            shown,
            vec![
                "Projection: f(t.c)",
                SAME_TEXT,
                SAME_TEXT,
                "Projection: g(t.c)",
                "Projection: g(t.c)",
                "ProjectionExec",
            ]
        );
    }
}
