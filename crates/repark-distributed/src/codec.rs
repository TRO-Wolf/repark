use std::fmt;
use std::sync::Arc;

use ballista_core::serde::{
    BallistaCodec, BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec,
};
use datafusion::common::DataFusionError;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use repark_core::{Error, Result as ReparkResult};

use crate::iceberg_provider::{
    ICEBERG_TABLE_SCAN, IcebergScanSpec, MAGIC, scan_node_filters, scan_node_resolved_snapshot_id,
    scan_node_table_ident,
};
use crate::session_provider::ReparkSessionProvider;

pub struct ReparkPhysicalExtensionCodec {
    inner: BallistaPhysicalExtensionCodec,
    context: SessionContext,
}

impl ReparkPhysicalExtensionCodec {
    #[must_use]
    pub fn new(context: SessionContext) -> Self {
        Self {
            inner: BallistaPhysicalExtensionCodec::default(),
            context,
        }
    }

    fn decode_iceberg_scan(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "{ICEBERG_TABLE_SCAN} decode expects no input plans, got {}",
                inputs.len()
            )));
        }
        let spec = IcebergScanSpec::decode(buf).map_err(codec_error)?;
        rebuild_iceberg_scan(self.context.clone(), spec)
    }

    fn encode_iceberg_scan(
        &self,
        node: &Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> DataFusionResult<()> {
        let spec = IcebergScanSpec::from_scan_node(node, &self.context).map_err(codec_error)?;
        let rebuilt =
            rebuild_iceberg_scan(self.context.clone(), spec.clone()).map_err(|error| {
                DataFusionError::Execution(format!(
                    "{ICEBERG_TABLE_SCAN} encode refused: the spec could not be rebuilt into an \
                 equivalent scan — the predicate or table identifier field did not re-parse \
                 under session authority: {error}"
                ))
            })?;
        verify_scan_identity(node, &rebuilt)?;
        buf.extend_from_slice(&spec.encode().map_err(codec_error)?);
        Ok(())
    }
}

impl Default for ReparkPhysicalExtensionCodec {
    fn default() -> Self {
        Self::new(SessionContext::new())
    }
}

impl fmt::Debug for ReparkPhysicalExtensionCodec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReparkPhysicalExtensionCodec")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl PhysicalExtensionCodec for ReparkPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if buf.starts_with(MAGIC) {
            return self.decode_iceberg_scan(buf, inputs);
        }
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DataFusionResult<()> {
        if node.name() == ICEBERG_TABLE_SCAN {
            return self.encode_iceberg_scan(&node, buf);
        }
        self.inner.try_encode(node, buf)
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> DataFusionResult<Arc<ScalarUDF>> {
        self.inner.try_decode_udf(name, buf)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> DataFusionResult<()> {
        self.inner.try_encode_udf(node, buf)
    }

    fn try_decode_expr(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn PhysicalExpr>],
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        self.inner.try_decode_expr(buf, inputs)
    }

    fn try_encode_expr(
        &self,
        node: &Arc<dyn PhysicalExpr>,
        buf: &mut Vec<u8>,
    ) -> DataFusionResult<()> {
        self.inner.try_encode_expr(node, buf)
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> DataFusionResult<Arc<AggregateUDF>> {
        self.inner.try_decode_udaf(name, buf)
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> DataFusionResult<()> {
        self.inner.try_encode_udaf(node, buf)
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> DataFusionResult<Arc<WindowUDF>> {
        self.inner.try_decode_udwf(name, buf)
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> DataFusionResult<()> {
        self.inner.try_encode_udwf(node, buf)
    }
}

#[derive(Debug, Default)]
pub struct ReparkLogicalExtensionCodec {
    inner: BallistaLogicalExtensionCodec,
}

impl ReparkLogicalExtensionCodec {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn into_inner(self) -> BallistaLogicalExtensionCodec {
        self.inner
    }
}

#[must_use]
pub fn repark_ballista_codec(provider: &ReparkSessionProvider) -> BallistaCodec {
    let context = SessionContext::new_with_state(provider.session_state());
    BallistaCodec::new(
        Arc::new(ReparkLogicalExtensionCodec::new().into_inner()),
        Arc::new(ReparkPhysicalExtensionCodec::new(context)),
    )
}

fn codec_error(error: Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn scan_identity_mismatch(field: &str, before: &str, after: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "{ICEBERG_TABLE_SCAN} encode refused: the {field} field rebuilt to {after} from an \
         original of {before}; the codec never emits a spec that describes a different scan"
    ))
}

fn verify_scan_identity(
    original: &Arc<dyn ExecutionPlan>,
    rebuilt: &Arc<dyn ExecutionPlan>,
) -> DataFusionResult<()> {
    if rebuilt.name() != ICEBERG_TABLE_SCAN {
        return Err(scan_identity_mismatch(
            "node type",
            ICEBERG_TABLE_SCAN,
            rebuilt.name(),
        ));
    }
    let before_ident = scan_node_table_ident(&format!("{original:?}")).map_err(codec_error)?;
    let after_ident = scan_node_table_ident(&format!("{rebuilt:?}")).map_err(codec_error)?;
    if after_ident != before_ident {
        return Err(scan_identity_mismatch(
            "table identifier",
            &format!("{before_ident:?}"),
            &format!("{after_ident:?}"),
        ));
    }
    let before_snapshot = scan_node_resolved_snapshot_id(original).map_err(codec_error)?;
    let after_snapshot = scan_node_resolved_snapshot_id(rebuilt).map_err(codec_error)?;
    if after_snapshot != before_snapshot {
        return Err(scan_identity_mismatch(
            "resolved snapshot id",
            &before_snapshot.to_string(),
            &after_snapshot.to_string(),
        ));
    }
    if rebuilt.schema() != original.schema() {
        return Err(scan_identity_mismatch(
            "projected schema",
            &format!("{:?}", original.schema()),
            &format!("{:?}", rebuilt.schema()),
        ));
    }
    let before_predicate = scan_node_filters(original).map_err(codec_error)?;
    let after_predicate = scan_node_filters(rebuilt).map_err(codec_error)?;
    if after_predicate != before_predicate {
        return Err(scan_identity_mismatch(
            "predicate",
            &format!("{before_predicate:?}"),
            &format!("{after_predicate:?}"),
        ));
    }
    let before_partitions = original.output_partitioning().partition_count();
    let after_partitions = rebuilt.output_partitioning().partition_count();
    if after_partitions != before_partitions {
        return Err(scan_identity_mismatch(
            "partition count",
            &before_partitions.to_string(),
            &after_partitions.to_string(),
        ));
    }
    Ok(())
}

fn rebuild_iceberg_scan(
    context: SessionContext,
    spec: IcebergScanSpec,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let join = std::thread::Builder::new()
        .name("repark-iceberg-scan-decode".to_owned())
        .spawn(move || -> ReparkResult<Arc<dyn ExecutionPlan>> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| {
                    Error::DataFusion(format!(
                        "{ICEBERG_TABLE_SCAN} decode could not build a runtime: {error}"
                    ))
                })?;
            let rebuilt = runtime.block_on(spec.scan(&context))?;
            if let Some(frozen) = spec.snapshot_id {
                let resolved = scan_node_resolved_snapshot_id(&rebuilt)?;
                if resolved != frozen {
                    return Err(Error::DataFusion(format!(
                        "{ICEBERG_TABLE_SCAN} rebuilt against snapshot {resolved} but the spec \
                         froze {frozen}; the table moved or the scan pinned a snapshot the \
                         executor cannot bind"
                    )));
                }
            }
            Ok(rebuilt)
        })
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "{ICEBERG_TABLE_SCAN} decode could not spawn its rebuild thread: {error}"
            ))
        })?;
    match join.join() {
        Ok(result) => result.map_err(codec_error),
        Err(_) => Err(DataFusionError::Execution(format!(
            "{ICEBERG_TABLE_SCAN} decode rebuild thread panicked"
        ))),
    }
}
