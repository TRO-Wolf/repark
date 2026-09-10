use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::TableIdent;
use repark_core::{CatalogRegistry, TablePolicy, parse_duration};

use super::{CallArgs, now_millis, resolve_table_ident};

struct InlineOverrides {
    target_file_size_bytes: Option<u64>,
    snapshot_retain_last: Option<u64>,
    snapshot_older_than: Option<String>,
    orphan_older_than: Option<String>,
    rewrite_manifests: Option<bool>,
    position_delete_ratio: Option<f64>,
}

impl InlineOverrides {
    fn is_empty(&self) -> bool {
        self.target_file_size_bytes.is_none()
            && self.snapshot_retain_last.is_none()
            && self.snapshot_older_than.is_none()
            && self.orphan_older_than.is_none()
            && self.rewrite_manifests.is_none()
            && self.position_delete_ratio.is_none()
    }
}

struct TableStats {
    data_bytes: u64,
    delete_bytes: u64,
}

struct PlannedStep {
    ordinal: i32,
    procedure: &'static str,
    arguments: String,
}

pub(super) async fn execute_run_maintenance(
    ctx: &SessionContext,
    catalog_name: &str,
    args: &CallArgs,
    catalogs: &CatalogRegistry,
) -> Result<DataFrame> {
    args.reject_unknown_named(&[
        "adaptive_partitioning",
        "dry_run",
        "orphan_older_than",
        "position_delete_ratio",
        "rewrite_manifests",
        "snapshot_older_than",
        "snapshot_retain_last",
        "table",
        "target_file_size_bytes",
    ])?;
    args.reject_excess_positional(2)?;
    if args.has_named("adaptive_partitioning") {
        return Err(DataFusionError::Plan(
            "key `run_maintenance.adaptive_partitioning` is not yet supported".to_string(),
        ));
    }
    let table_arg = args.require_string("table", 0)?;
    let dry_run = args.optional_bool("dry_run", Some(1))?.unwrap_or(true);
    if !dry_run {
        return Err(DataFusionError::NotImplemented(
            "CALL run_maintenance dry_run => false is not supported in this build: \
             the dry run is the only mode until the apply path lands"
                .to_string(),
        ));
    }
    let inline = InlineOverrides {
        target_file_size_bytes: optional_non_negative_u64(args, "target_file_size_bytes")?,
        snapshot_retain_last: optional_non_negative_u64(args, "snapshot_retain_last")?,
        snapshot_older_than: args.optional_string("snapshot_older_than")?,
        orphan_older_than: args.optional_string("orphan_older_than")?,
        rewrite_manifests: args.optional_bool("rewrite_manifests", None)?,
        position_delete_ratio: args.optional_f64("position_delete_ratio", None)?,
    };
    let stamped = catalogs.maintenance_policy();
    let profile_name = stamped.map_or("default", |(name, _)| name);
    let file_policy = stamped.and_then(|(_, policy)| policy);
    let ident = resolve_table_ident(catalog_name, &table_arg)?;
    let base = file_policy.map(|policy| {
        let canonical = canonical_table_name(catalog_name, &ident);
        if policy.tables.contains_key(&canonical) {
            policy.resolve(&canonical)
        } else {
            policy.resolve(&table_arg)
        }
    });
    if base.is_none() && inline.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "run_maintenance: no [{profile_name}.maintenance] table and no inline keys \
             for {table_arg}"
        )));
    }
    let effective = apply_inline(&base.unwrap_or_default(), &inline)?;
    let stats = table_stats(ctx, catalogs, catalog_name, &ident).await?;
    let steps = plan_steps(&effective, catalog_name, &table_arg, &stats, now_millis()?);
    plan_dataframe(ctx, &steps)
}

fn optional_non_negative_u64(args: &CallArgs, name: &str) -> Result<Option<u64>> {
    match args.optional_i64(name, None)? {
        None => Ok(None),
        Some(value) => u64::try_from(value).map(Some).map_err(|_| {
            DataFusionError::Plan(format!(
                "CALL argument `{name}` must be a non-negative integer, got {value}"
            ))
        }),
    }
}

fn canonical_table_name(catalog_name: &str, ident: &TableIdent) -> String {
    let mut parts = vec![catalog_name.to_string()];
    parts.extend(ident.namespace().as_ref().iter().cloned());
    parts.push(ident.name().to_string());
    parts.join(".")
}

fn maintenance_err(error: &repark_core::Error) -> DataFusionError {
    DataFusionError::Plan(error.to_string())
}

fn apply_inline(base: &TablePolicy, inline: &InlineOverrides) -> Result<TablePolicy> {
    let mut effective = base.clone();
    if let Some(value) = inline.target_file_size_bytes {
        effective.target_file_size_bytes = Some(value);
    }
    if let Some(value) = inline.snapshot_retain_last {
        effective.snapshot_retain_last = Some(value);
    }
    if let Some(text) = inline.snapshot_older_than.as_deref() {
        effective.snapshot_older_than = Some(
            parse_duration("run_maintenance.snapshot_older_than", text)
                .map_err(|error| maintenance_err(&error))?,
        );
    }
    if let Some(text) = inline.orphan_older_than.as_deref() {
        effective.orphan_older_than = Some(
            parse_duration("run_maintenance.orphan_older_than", text)
                .map_err(|error| maintenance_err(&error))?,
        );
    }
    if let Some(flag) = inline.rewrite_manifests {
        effective.rewrite_manifests = Some(flag);
    }
    if let Some(ratio) = inline.position_delete_ratio {
        effective.position_delete_ratio = Some(ratio);
    }
    Ok(effective)
}

fn quote_ident(part: &str) -> String {
    format!("\"{}\"", part.replace('"', "\"\""))
}

fn metadata_path(catalog_name: &str, ident: &TableIdent, suffix: &str) -> String {
    let mut parts = vec![catalog_name.to_string()];
    parts.extend(ident.namespace().as_ref().iter().cloned());
    parts.push(ident.name().to_string());
    parts.push(suffix.to_string());
    parts
        .iter()
        .map(|part| quote_ident(part))
        .collect::<Vec<_>>()
        .join(".")
}

async fn sum_metadata_bytes(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    ident: &TableIdent,
    suffix: &str,
    filter: Option<&str>,
) -> Result<u64> {
    let path = metadata_path(catalog_name, ident, suffix);
    let predicate = filter
        .map(|clause| format!(" WHERE {clause}"))
        .unwrap_or_default();
    let sql = format!(
        r#"SELECT COALESCE(SUM("file_size_in_bytes"), 0) AS "bytes" FROM {path}{predicate}"#
    );
    let frame = Box::pin(crate::router::execute(ctx, catalogs, &sql)).await?;
    let batches = frame.collect().await?;
    let mut total: i64 = 0;
    let mut saw_row = false;
    for batch in &batches {
        let column = batch.column_by_name("bytes").ok_or_else(|| {
            DataFusionError::Plan(format!(
                "run_maintenance byte sum over `{path}` missed column `bytes`"
            ))
        })?;
        let values = column
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "run_maintenance byte sum over `{path}` is not Int64"
                ))
            })?;
        for index in 0..values.len() {
            saw_row = true;
            if !values.is_valid(index) {
                return Err(DataFusionError::Plan(format!(
                    "run_maintenance byte sum over `{path}` answered null"
                )));
            }
            total = total.checked_add(values.value(index)).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "run_maintenance byte sum over `{path}` does not fit i64 \
                     (refusing to fabricate MAX)"
                ))
            })?;
        }
    }
    if !saw_row {
        return Err(DataFusionError::Plan(format!(
            "run_maintenance byte sum over `{path}` answered no rows"
        )));
    }
    u64::try_from(total).map_err(|_| {
        DataFusionError::Plan(format!(
            "run_maintenance byte sum over `{path}` is negative ({total})"
        ))
    })
}

async fn table_stats(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    catalog_name: &str,
    ident: &TableIdent,
) -> Result<TableStats> {
    let data_bytes = sum_metadata_bytes(
        ctx,
        catalogs,
        catalog_name,
        ident,
        "files",
        Some("content = 0"),
    )
    .await?;
    let delete_bytes =
        sum_metadata_bytes(ctx, catalogs, catalog_name, ident, "delete_files", None).await?;
    Ok(TableStats {
        data_bytes,
        delete_bytes,
    })
}

#[allow(clippy::cast_precision_loss)]
fn delete_ratio(stats: &TableStats) -> f64 {
    if stats.data_bytes == 0 {
        0.0
    } else {
        stats.delete_bytes as f64 / stats.data_bytes as f64
    }
}

fn step_1_admits(threshold: Option<f64>, stats: &TableStats) -> bool {
    threshold.is_some_and(|limit| delete_ratio(stats) >= limit)
}

fn older_than_ms(now_ms: i64, duration: Duration) -> i64 {
    let millis = i64::try_from(duration.as_millis()).unwrap_or(i64::MAX);
    now_ms.saturating_sub(millis)
}

fn render_table(table_arg: &str) -> String {
    table_arg.replace('\'', "''")
}

fn call_text(catalog_name: &str, procedure: &str, table_arg: &str, extras: &str) -> String {
    format!(
        "CALL {catalog_name}.system.{procedure}(table => '{}'{extras})",
        render_table(table_arg)
    )
}

fn plan_steps(
    policy: &TablePolicy,
    catalog_name: &str,
    table_arg: &str,
    stats: &TableStats,
    now_ms: i64,
) -> Vec<PlannedStep> {
    let mut steps = Vec::new();
    if step_1_admits(policy.position_delete_ratio, stats) {
        steps.push(PlannedStep {
            ordinal: 1,
            procedure: "rewrite_position_delete_files",
            arguments: call_text(catalog_name, "rewrite_position_delete_files", table_arg, ""),
        });
    }
    let rewrite_extras = policy.target_file_size_bytes.map_or(String::new(), |size| {
        format!(", options => map('target-file-size-bytes', '{size}')")
    });
    steps.push(PlannedStep {
        ordinal: 2,
        procedure: "rewrite_data_files",
        arguments: call_text(
            catalog_name,
            "rewrite_data_files",
            table_arg,
            &rewrite_extras,
        ),
    });
    if policy.rewrite_manifests == Some(true) {
        steps.push(PlannedStep {
            ordinal: 3,
            procedure: "rewrite_manifests",
            arguments: call_text(catalog_name, "rewrite_manifests", table_arg, ""),
        });
    }
    let mut expire_parts = Vec::new();
    if let Some(older_than) = policy.snapshot_older_than {
        expire_parts.push(format!(
            ", older_than => {}",
            older_than_ms(now_ms, older_than)
        ));
    }
    if let Some(retain) = policy.snapshot_retain_last {
        expire_parts.push(format!(", retain_last => {retain}"));
    }
    steps.push(PlannedStep {
        ordinal: 4,
        procedure: "expire_snapshots",
        arguments: call_text(
            catalog_name,
            "expire_snapshots",
            table_arg,
            &expire_parts.concat(),
        ),
    });
    if let Some(older_than) = policy.orphan_older_than {
        steps.push(PlannedStep {
            ordinal: 5,
            procedure: "remove_orphan_files",
            arguments: call_text(
                catalog_name,
                "remove_orphan_files",
                table_arg,
                &format!(", older_than => {}", older_than_ms(now_ms, older_than)),
            ),
        });
    }
    steps
}

fn plan_dataframe(ctx: &SessionContext, steps: &[PlannedStep]) -> Result<DataFrame> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("step", DataType::Int32, false),
        Field::new("procedure", DataType::Utf8, false),
        Field::new("arguments", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("result", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                steps.iter().map(|step| step.ordinal).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                steps.iter().map(|step| step.procedure).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                steps
                    .iter()
                    .map(|step| step.arguments.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec!["planned"; steps.len()])),
            Arc::new(StringArray::from(vec![""; steps.len()])),
        ],
    )?;
    ctx.read_batches(vec![batch])
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW_MS: i64 = 1_700_000_000_000;

    fn stats(data_bytes: u64, delete_bytes: u64) -> TableStats {
        TableStats {
            data_bytes,
            delete_bytes,
        }
    }

    fn ordinals(steps: &[PlannedStep]) -> Vec<i32> {
        steps.iter().map(|step| step.ordinal).collect()
    }

    fn procedures(steps: &[PlannedStep]) -> Vec<&str> {
        steps.iter().map(|step| step.procedure).collect()
    }

    #[test]
    fn an_exact_threshold_ratio_admits_step_1() {
        let policy = TablePolicy {
            position_delete_ratio: Some(0.3),
            ..TablePolicy::default()
        };
        let steps = plan_steps(&policy, "ice", "sales.t", &stats(1000, 300), NOW_MS);
        assert_eq!(procedures(&steps)[0], "rewrite_position_delete_files");
    }

    #[test]
    fn a_ratio_below_the_threshold_skips_step_1() {
        let policy = TablePolicy {
            position_delete_ratio: Some(0.3),
            ..TablePolicy::default()
        };
        let steps = plan_steps(&policy, "ice", "sales.t", &stats(1000, 299), NOW_MS);
        assert!(
            procedures(&steps)
                .iter()
                .all(|procedure| *procedure != "rewrite_position_delete_files")
        );
    }

    #[test]
    fn an_unset_threshold_skips_step_1() {
        let steps = plan_steps(
            &TablePolicy::default(),
            "ice",
            "sales.t",
            &stats(1000, 900),
            NOW_MS,
        );
        assert!(
            procedures(&steps)
                .iter()
                .all(|procedure| *procedure != "rewrite_position_delete_files")
        );
    }

    #[test]
    fn an_empty_table_counts_a_zero_ratio() {
        let policy = TablePolicy {
            position_delete_ratio: Some(0.0),
            ..TablePolicy::default()
        };
        let admitted = plan_steps(&policy, "ice", "sales.t", &stats(0, 0), NOW_MS);
        assert_eq!(procedures(&admitted)[0], "rewrite_position_delete_files");
        let policy = TablePolicy {
            position_delete_ratio: Some(0.1),
            ..TablePolicy::default()
        };
        let skipped = plan_steps(&policy, "ice", "sales.t", &stats(0, 0), NOW_MS);
        assert!(
            procedures(&skipped)
                .iter()
                .all(|procedure| *procedure != "rewrite_position_delete_files")
        );
    }

    #[test]
    fn rewrite_manifests_plans_only_when_true() {
        for flag in [None, Some(false)] {
            let policy = TablePolicy {
                rewrite_manifests: flag,
                ..TablePolicy::default()
            };
            let steps = plan_steps(&policy, "ice", "sales.t", &stats(100, 0), NOW_MS);
            assert!(
                procedures(&steps)
                    .iter()
                    .all(|procedure| *procedure != "rewrite_manifests"),
                "flag {flag:?} must skip step 3"
            );
        }
        let policy = TablePolicy {
            rewrite_manifests: Some(true),
            ..TablePolicy::default()
        };
        let steps = plan_steps(&policy, "ice", "sales.t", &stats(100, 0), NOW_MS);
        assert!(procedures(&steps).contains(&"rewrite_manifests"));
    }

    #[test]
    fn an_unset_orphan_older_than_omits_step_5() {
        let steps = plan_steps(
            &TablePolicy::default(),
            "ice",
            "sales.t",
            &stats(100, 0),
            NOW_MS,
        );
        assert_eq!(ordinals(&steps), vec![2, 4]);
    }

    #[test]
    fn durations_render_as_now_minus_duration_ms() {
        let policy = TablePolicy {
            snapshot_older_than: Some(
                repark_core::parse_duration("test.snapshot_older_than", "7d")
                    .expect("fixture duration"),
            ),
            orphan_older_than: Some(
                repark_core::parse_duration("test.orphan_older_than", "3d")
                    .expect("fixture duration"),
            ),
            ..TablePolicy::default()
        };
        let steps = plan_steps(&policy, "ice", "sales.t", &stats(100, 0), NOW_MS);
        let expire = steps
            .iter()
            .find(|step| step.procedure == "expire_snapshots")
            .expect("expire plans");
        assert!(
            expire.arguments.contains("older_than => 1699395200000"),
            "got: {}",
            expire.arguments
        );
        let orphan = steps
            .iter()
            .find(|step| step.procedure == "remove_orphan_files")
            .expect("orphan plans");
        assert!(
            orphan.arguments.contains("older_than => 1699740800000"),
            "got: {}",
            orphan.arguments
        );
    }

    #[test]
    fn a_quote_in_the_table_name_renders_doubled() {
        let steps = plan_steps(
            &TablePolicy::default(),
            "ice",
            "sales.o'brien",
            &stats(100, 0),
            NOW_MS,
        );
        let rewrite = steps
            .iter()
            .find(|step| step.procedure == "rewrite_data_files")
            .expect("rewrite plans");
        assert_eq!(
            rewrite.arguments,
            "CALL ice.system.rewrite_data_files(table => 'sales.o''brien')"
        );
    }

    #[test]
    fn expire_renders_only_the_set_keys() {
        let bare = plan_steps(
            &TablePolicy::default(),
            "ice",
            "sales.t",
            &stats(100, 0),
            NOW_MS,
        );
        let expire = bare
            .iter()
            .find(|step| step.procedure == "expire_snapshots")
            .expect("expire plans");
        assert_eq!(
            expire.arguments,
            "CALL ice.system.expire_snapshots(table => 'sales.t')"
        );
        let policy = TablePolicy {
            snapshot_retain_last: Some(5),
            ..TablePolicy::default()
        };
        let steps = plan_steps(&policy, "ice", "sales.t", &stats(100, 0), NOW_MS);
        let expire = steps
            .iter()
            .find(|step| step.procedure == "expire_snapshots")
            .expect("expire plans");
        assert_eq!(
            expire.arguments,
            "CALL ice.system.expire_snapshots(table => 'sales.t', retain_last => 5)"
        );
    }

    #[test]
    fn rewrite_options_render_only_with_a_set_size() {
        let bare = plan_steps(
            &TablePolicy::default(),
            "ice",
            "sales.t",
            &stats(100, 0),
            NOW_MS,
        );
        let rewrite = bare
            .iter()
            .find(|step| step.procedure == "rewrite_data_files")
            .expect("rewrite plans");
        assert_eq!(
            rewrite.arguments,
            "CALL ice.system.rewrite_data_files(table => 'sales.t')"
        );
        let policy = TablePolicy {
            target_file_size_bytes: Some(134_217_728),
            ..TablePolicy::default()
        };
        let steps = plan_steps(&policy, "ice", "sales.t", &stats(100, 0), NOW_MS);
        let rewrite = steps
            .iter()
            .find(|step| step.procedure == "rewrite_data_files")
            .expect("rewrite plans");
        assert_eq!(
            rewrite.arguments,
            "CALL ice.system.rewrite_data_files(table => 'sales.t', \
             options => map('target-file-size-bytes', '134217728'))"
        );
    }

    #[test]
    fn a_huge_duration_saturates_rather_than_wraps() {
        let policy = TablePolicy {
            snapshot_older_than: Some(Duration::from_secs(u64::MAX)),
            ..TablePolicy::default()
        };
        let steps = plan_steps(&policy, "ice", "sales.t", &stats(100, 0), NOW_MS);
        let expire = steps
            .iter()
            .find(|step| step.procedure == "expire_snapshots")
            .expect("expire plans");
        assert!(
            expire.arguments.contains(&format!(
                "older_than => {}",
                NOW_MS.saturating_sub(i64::MAX)
            )),
            "got: {}",
            expire.arguments
        );
    }
}
