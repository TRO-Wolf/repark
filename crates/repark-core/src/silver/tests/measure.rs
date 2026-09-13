use std::time::Instant;

use crate::silver::{
    Column, ConflictingTie, EmptyInput, IdenticalSourcePayloadTie, InputContract, OrderDirection,
    OrderField, Publication, QualityRule, Selection, SilverPlan, Source, TargetType,
};

fn synthetic_plan(column_count: usize) -> SilverPlan {
    let mut columns = Vec::with_capacity(column_count);
    for index in 0..column_count {
        let source_field_id = i32::try_from(index + 1).expect("column id fits i32");
        columns.push(Column {
            source_field_id,
            target_name: format!("col_{index}"),
            target_type: TargetType::Utf8,
            nullable: true,
            transforms: Vec::new(),
            validators: Vec::new(),
        });
    }
    SilverPlan {
        plan_format_version: 1,
        dataset_id: "bench.silver".to_string(),
        source: Source {
            source_system_id: "bench".to_string(),
            catalog: "cat".to_string(),
            schema: "sch".to_string(),
            table: "tbl".to_string(),
        },
        input_contract: InputContract::RowSnapshot {
            bronze_record_id_field: 1,
        },
        columns,
        selection: Selection::LatestSourceVersion {
            key_fields: vec![1],
            order_fields: vec![OrderField {
                source_field_id: 1,
                direction: OrderDirection::Ascending,
            }],
            conflicting_tie: ConflictingTie::FailRun,
            identical_source_payload_tie: IdenticalSourcePayloadTie::LowestBronzeRecordId,
        },
        quality: vec![QualityRule::ExactDispositionAccounting {
            rule_id: "row_accounting".to_string(),
        }],
        publication: Publication::SingleClassifiedTable {
            empty_input: EmptyInput::RequireExplicitEmptyReplaceOption,
        },
    }
}

fn median_nanos(samples: &mut [u128]) -> u128 {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn time_canonical(plan: &SilverPlan) -> (u128, usize) {
    let _ = plan.canonical();
    let mut samples = [0_u128; 3];
    let mut size = 0;
    for sample in &mut samples {
        let start = Instant::now();
        let bytes = plan.canonical();
        *sample = start.elapsed().as_nanos();
        size = bytes.len();
    }
    (median_nanos(&mut samples), size)
}

fn time_explain(plan: &SilverPlan) -> (u128, usize) {
    let _ = plan.explain();
    let mut samples = [0_u128; 3];
    let mut size = 0;
    for sample in &mut samples {
        let start = Instant::now();
        let text = plan.explain();
        *sample = start.elapsed().as_nanos();
        size = text.len();
    }
    (median_nanos(&mut samples), size)
}

#[ignore = "C-009 wall and size printer; run with --ignored"]
#[test]
fn measure_canonical_and_explain_at_50_and_500_columns() {
    for column_count in [50_usize, 500_usize] {
        let plan = synthetic_plan(column_count);
        let (canonical_nanos, canonical_size) = time_canonical(&plan);
        let (explain_nanos, explain_size) = time_explain(&plan);
        eprintln!(
            "columns={column_count} canonical_median_ns={canonical_nanos} canonical_bytes={canonical_size} explain_median_ns={explain_nanos} explain_bytes={explain_size}"
        );
    }
}
