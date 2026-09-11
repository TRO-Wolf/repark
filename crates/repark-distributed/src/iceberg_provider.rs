use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::DFSchema;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion::sql::TableReference;
use iceberg_datafusion::IcebergTableScan;
use repark_core::{CatalogKind, CatalogSpec, Error, Result, engine_err};

use crate::predicate_expr::{decode_expr, encode_expr, predicate_to_expr};

pub(crate) const MAGIC: &[u8; 4] = b"RPIC";
const CODEC_VERSION: u8 = 2;
const MAX_ITEM_BYTES: usize = 1_048_576;
const KIND_GLUE: u8 = 0;
const KIND_S3_TABLES: u8 = 1;
const KIND_MEMORY: u8 = 2;
const KIND_POSTGRES: u8 = 3;
const FILTER_TAG_SQL: u8 = 0;
const FILTER_TAG_EXPR: u8 = 1;
pub(crate) const ICEBERG_TABLE_SCAN: &str = "IcebergTableScan";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergScanSpec {
    pub catalog: CatalogSpec,
    pub table_identifier: Vec<String>,
    pub snapshot_id: Option<i64>,
    pub projection: Option<Vec<String>>,
    pub filters: Vec<String>,
    pub filter_expr_bytes: Vec<Vec<u8>>,
}

impl IcebergScanSpec {
    #[must_use]
    pub fn new(
        catalog: CatalogSpec,
        table_identifier: Vec<String>,
        snapshot_id: Option<i64>,
        projection: Option<Vec<String>>,
        filters: Vec<String>,
    ) -> Self {
        Self {
            catalog,
            table_identifier,
            snapshot_id,
            projection,
            filters,
            filter_expr_bytes: Vec::new(),
        }
    }

    #[allow(clippy::missing_errors_doc)]
    pub(crate) fn from_scan_node(
        node: &Arc<dyn ExecutionPlan>,
        context: &SessionContext,
    ) -> Result<Self> {
        let scan = node
            .as_ref()
            .downcast_ref::<IcebergTableScan>()
            .ok_or_else(|| {
                codec_err(format!("node {} is not {ICEBERG_TABLE_SCAN}", node.name()))
            })?;
        let identifier = scan.table().identifier();
        let namespace = identifier.namespace();
        if namespace.len() != 1 {
            return Err(codec_err(format!(
                "{ICEBERG_TABLE_SCAN} namespace {namespace:?} is nested; the spec encodes \
                 catalog.namespace.table"
            )));
        }
        let table = identifier.name().to_owned();
        let snapshot_id = Some(scan.resolved_snapshot_id());
        let projection = scan.projection().map(<[String]>::to_vec);
        let filter_expr_bytes = match scan.predicates() {
            None => Vec::new(),
            Some(predicate) => {
                let expr = predicate_to_expr(predicate)?;
                vec![encode_expr(&expr)?]
            }
        };
        let catalog = session_catalog_spec(context, &namespace[0], &table)?;
        Ok(Self {
            table_identifier: vec![catalog.name.clone(), namespace[0].clone(), table],
            catalog,
            snapshot_id,
            projection,
            filters: Vec::new(),
            filter_expr_bytes,
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(MAGIC);
        buffer.push(CODEC_VERSION);
        write_string(&mut buffer, &self.catalog.name)?;
        buffer.push(kind_code(self.catalog.kind));
        write_props(&mut buffer, &self.catalog.props)?;
        write_string_list(&mut buffer, &self.table_identifier)?;
        write_optional_i64(&mut buffer, self.snapshot_id);
        match &self.projection {
            None => buffer.push(0),
            Some(columns) => {
                buffer.push(1);
                write_string_list(&mut buffer, columns)?;
            }
        }
        write_bytes_list(&mut buffer, &self.wire_filter_bytes())?;
        Ok(buffer)
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cursor = 0_usize;
        let magic = read_exact(bytes, &mut cursor, 4)?;
        if magic != MAGIC {
            return Err(codec_err(format!(
                "Iceberg scan spec magic {magic:?} != {MAGIC:?}"
            )));
        }
        let version = read_u8(bytes, &mut cursor)?;
        if version != CODEC_VERSION {
            return Err(codec_err(format!(
                "Iceberg scan spec version {version} is not {CODEC_VERSION}"
            )));
        }
        let catalog_name = read_string(bytes, &mut cursor)?;
        let kind = kind_from_code(read_u8(bytes, &mut cursor)?)?;
        let props = read_props(bytes, &mut cursor)?;
        let table_identifier = read_string_list(bytes, &mut cursor)?;
        let snapshot_id = read_optional_i64(bytes, &mut cursor)?;
        let projection = match read_u8(bytes, &mut cursor)? {
            0 => None,
            1 => Some(read_string_list(bytes, &mut cursor)?),
            flag => {
                return Err(codec_err(format!(
                    "Iceberg scan spec projection flag {flag} is not 0 or 1"
                )));
            }
        };
        let raw_filters = read_bytes_list(bytes, &mut cursor)?;
        if cursor != bytes.len() {
            return Err(codec_err(format!(
                "Iceberg scan spec has {} trailing byte(s)",
                bytes.len() - cursor
            )));
        }
        let (filters, filter_expr_bytes) = split_wire_filters(raw_filters)?;
        Ok(Self {
            catalog: CatalogSpec {
                name: catalog_name,
                kind,
                props,
            },
            table_identifier,
            snapshot_id,
            projection,
            filters,
            filter_expr_bytes,
        })
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn rebuild_provider(
        &self,
        context: &SessionContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let table_ref = self.table_reference()?;
        let catalog_name = table_ref.catalog().unwrap_or(self.catalog.name.as_str());
        if context.catalog(catalog_name).is_none() {
            return Err(Error::DataFusion(format!(
                "catalog '{catalog_name}' is not registered on the session; Iceberg scans \
                 resolve through ReparkSessionProvider, never ambient authority"
            )));
        }
        context.table_provider(table_ref).await.map_err(engine_err)
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn scan(&self, context: &SessionContext) -> Result<Arc<dyn ExecutionPlan>> {
        let provider = self.rebuild_provider(context).await?;
        let schema = provider.schema();
        let projection = match &self.projection {
            None => None,
            Some(names) => Some(projection_indices(&schema, names)?),
        };
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).map_err(engine_err)?;
        let filters = self.scan_filter_exprs(context, &df_schema)?;
        let state = context.state();
        provider
            .scan(&state, projection.as_ref(), &filters, None)
            .await
            .map_err(engine_err)
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn data_file_paths(&self, context: &SessionContext) -> Result<Vec<String>> {
        let _ = self.rebuild_provider(context).await?;
        let table_ref = self.table_reference()?;
        let files_sql = files_metadata_sql(&table_ref);
        let frame = context.sql(&files_sql).await.map_err(engine_err)?;
        let batches = frame.collect().await.map_err(engine_err)?;
        let mut paths = Vec::new();
        for batch in batches {
            let column = batch.column(0);
            let Some(strings) = column.as_any().downcast_ref::<StringArray>() else {
                return Err(codec_err(format!(
                    "Iceberg files metadata column 0 is {:?}, want Utf8",
                    column.data_type()
                )));
            };
            for index in 0..strings.len() {
                if strings.is_null(index) {
                    continue;
                }
                paths.push(strings.value(index).to_owned());
            }
        }
        paths.sort();
        Ok(paths)
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn rewrite_iceberg_table_scans_as_file_groups(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &SessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if count_named(plan.as_ref(), ICEBERG_TABLE_SCAN) == 0 {
            return Ok(plan);
        }
        let files = self.data_file_paths(context).await?;
        if files.is_empty() {
            return Err(codec_err(format!(
                "Iceberg table {} has no data files to distribute",
                self.table_identifier.join(".")
            )));
        }
        replace_iceberg_scans(plan, context, &files).await
    }

    #[allow(clippy::missing_errors_doc)]
    fn wire_filter_bytes(&self) -> Vec<Vec<u8>> {
        let mut items = Vec::with_capacity(self.filters.len() + self.filter_expr_bytes.len());
        for filter in &self.filters {
            let mut item = Vec::with_capacity(1 + filter.len());
            item.push(FILTER_TAG_SQL);
            item.extend_from_slice(filter.as_bytes());
            items.push(item);
        }
        for bytes in &self.filter_expr_bytes {
            let mut item = Vec::with_capacity(1 + bytes.len());
            item.push(FILTER_TAG_EXPR);
            item.extend_from_slice(bytes);
            items.push(item);
        }
        items
    }

    fn scan_filter_exprs(
        &self,
        context: &SessionContext,
        df_schema: &DFSchema,
    ) -> Result<Vec<Expr>> {
        let mut exprs = Vec::with_capacity(self.filters.len() + self.filter_expr_bytes.len());
        for filter in &self.filters {
            exprs.push(
                context
                    .parse_sql_expr(filter, df_schema)
                    .map_err(engine_err)?,
            );
        }
        for bytes in &self.filter_expr_bytes {
            exprs.push(decode_expr(bytes)?);
        }
        Ok(exprs)
    }

    fn table_reference(&self) -> Result<TableReference> {
        match self.table_identifier.as_slice() {
            [catalog, namespace, table] => Ok(TableReference::full(
                catalog.clone(),
                namespace.clone(),
                table.clone(),
            )),
            parts => Err(codec_err(format!(
                "Iceberg table identifier needs catalog.namespace.table, got {parts:?}"
            ))),
        }
    }
}

fn codec_err(message: String) -> Error {
    Error::DataFusion(message)
}

const fn kind_code(kind: CatalogKind) -> u8 {
    match kind {
        CatalogKind::Glue => KIND_GLUE,
        CatalogKind::S3Tables => KIND_S3_TABLES,
        CatalogKind::Memory => KIND_MEMORY,
        CatalogKind::Postgres => KIND_POSTGRES,
    }
}

fn kind_from_code(code: u8) -> Result<CatalogKind> {
    match code {
        KIND_GLUE => Ok(CatalogKind::Glue),
        KIND_S3_TABLES => Ok(CatalogKind::S3Tables),
        KIND_MEMORY => Ok(CatalogKind::Memory),
        KIND_POSTGRES => Ok(CatalogKind::Postgres),
        other => Err(codec_err(format!(
            "Iceberg scan spec catalog kind {other} is unknown"
        ))),
    }
}

fn write_u32(buffer: &mut Vec<u8>, value: u32) {
    buffer.extend_from_slice(&value.to_le_bytes());
}

fn write_string(buffer: &mut Vec<u8>, value: &str) -> Result<()> {
    let len = u32::try_from(value.len()).map_err(|_| {
        codec_err(format!(
            "Iceberg scan spec string length {} exceeds u32",
            value.len()
        ))
    })?;
    if value.len() > MAX_ITEM_BYTES {
        return Err(codec_err(format!(
            "Iceberg scan spec string length {} exceeds {MAX_ITEM_BYTES}",
            value.len()
        )));
    }
    write_u32(buffer, len);
    buffer.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_bytes(buffer: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u32::try_from(value.len()).map_err(|_| {
        codec_err(format!(
            "Iceberg scan spec bytes length {} exceeds u32",
            value.len()
        ))
    })?;
    if value.len() > MAX_ITEM_BYTES {
        return Err(codec_err(format!(
            "Iceberg scan spec bytes length {} exceeds {MAX_ITEM_BYTES}",
            value.len()
        )));
    }
    write_u32(buffer, len);
    buffer.extend_from_slice(value);
    Ok(())
}

fn write_bytes_list(buffer: &mut Vec<u8>, values: &[Vec<u8>]) -> Result<()> {
    let len = u32::try_from(values.len()).map_err(|_| {
        codec_err(format!(
            "Iceberg scan spec list length {} exceeds u32",
            values.len()
        ))
    })?;
    write_u32(buffer, len);
    for value in values {
        write_bytes(buffer, value)?;
    }
    Ok(())
}

fn write_string_list(buffer: &mut Vec<u8>, values: &[String]) -> Result<()> {
    let len = u32::try_from(values.len()).map_err(|_| {
        codec_err(format!(
            "Iceberg scan spec list length {} exceeds u32",
            values.len()
        ))
    })?;
    write_u32(buffer, len);
    for value in values {
        write_string(buffer, value)?;
    }
    Ok(())
}

fn write_props(buffer: &mut Vec<u8>, props: &HashMap<String, String>) -> Result<()> {
    let mut keys: Vec<&String> = props.keys().collect();
    keys.sort();
    let len = u32::try_from(keys.len()).map_err(|_| {
        codec_err(format!(
            "Iceberg scan spec props length {} exceeds u32",
            keys.len()
        ))
    })?;
    write_u32(buffer, len);
    for key in keys {
        write_string(buffer, key)?;
        write_string(
            buffer,
            props
                .get(key)
                .ok_or_else(|| codec_err(format!("Iceberg scan spec missing prop {key}")))?,
        )?;
    }
    Ok(())
}

fn write_optional_i64(buffer: &mut Vec<u8>, value: Option<i64>) {
    match value {
        None => buffer.push(0),
        Some(number) => {
            buffer.push(1);
            buffer.extend_from_slice(&number.to_le_bytes());
        }
    }
}

fn read_exact<'bytes>(bytes: &'bytes [u8], cursor: &mut usize, len: usize) -> Result<&'bytes [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| codec_err("Iceberg scan spec cursor overflow".to_owned()))?;
    if end > bytes.len() {
        return Err(codec_err(format!(
            "Iceberg scan spec truncated: need {len} byte(s) at {cursor}, have {}",
            bytes.len()
        )));
    }
    let slice = &bytes[*cursor..end];
    *cursor = end;
    Ok(slice)
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8> {
    let slice = read_exact(bytes, cursor, 1)?;
    Ok(slice[0])
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let slice = read_exact(bytes, cursor, 4)?;
    let mut raw = [0_u8; 4];
    raw.copy_from_slice(slice);
    Ok(u32::from_le_bytes(raw))
}

fn read_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64> {
    let slice = read_exact(bytes, cursor, 8)?;
    let mut raw = [0_u8; 8];
    raw.copy_from_slice(slice);
    Ok(i64::from_le_bytes(raw))
}

fn read_string(bytes: &[u8], cursor: &mut usize) -> Result<String> {
    let len = usize::try_from(read_u32(bytes, cursor)?)
        .map_err(|_| codec_err("Iceberg scan spec string length does not fit usize".to_owned()))?;
    if len > MAX_ITEM_BYTES {
        return Err(codec_err(format!(
            "Iceberg scan spec string length {len} exceeds {MAX_ITEM_BYTES}"
        )));
    }
    let slice = read_exact(bytes, cursor, len)?;
    String::from_utf8(slice.to_vec())
        .map_err(|error| codec_err(format!("Iceberg scan spec string is not utf-8: {error}")))
}

fn read_bytes(bytes: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let len = usize::try_from(read_u32(bytes, cursor)?)
        .map_err(|_| codec_err("Iceberg scan spec bytes length does not fit usize".to_owned()))?;
    if len > MAX_ITEM_BYTES {
        return Err(codec_err(format!(
            "Iceberg scan spec bytes length {len} exceeds {MAX_ITEM_BYTES}"
        )));
    }
    Ok(read_exact(bytes, cursor, len)?.to_vec())
}

fn read_bytes_list(bytes: &[u8], cursor: &mut usize) -> Result<Vec<Vec<u8>>> {
    let len = usize::try_from(read_u32(bytes, cursor)?)
        .map_err(|_| codec_err("Iceberg scan spec list length does not fit usize".to_owned()))?;
    if len > MAX_ITEM_BYTES {
        return Err(codec_err(format!(
            "Iceberg scan spec list length {len} exceeds {MAX_ITEM_BYTES}"
        )));
    }
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(read_bytes(bytes, cursor)?);
    }
    Ok(values)
}

fn split_wire_filters(raw: Vec<Vec<u8>>) -> Result<(Vec<String>, Vec<Vec<u8>>)> {
    let mut filters = Vec::new();
    let mut filter_expr_bytes = Vec::new();
    for item in raw {
        let Some((tag, payload)) = item.split_first() else {
            return Err(codec_err(
                "Iceberg scan spec filter item is empty; a tag byte is required".to_owned(),
            ));
        };
        match *tag {
            FILTER_TAG_SQL => {
                let sql = String::from_utf8(payload.to_vec()).map_err(|error| {
                    codec_err(format!(
                        "Iceberg scan spec SQL filter is not utf-8: {error}"
                    ))
                })?;
                filters.push(sql);
            }
            FILTER_TAG_EXPR => {
                decode_expr(payload)?;
                filter_expr_bytes.push(payload.to_vec());
            }
            tag => {
                return Err(codec_err(format!(
                    "Iceberg scan spec filter tag {tag} is unknown; want 0 (SQL) or 1 (Expr)"
                )));
            }
        }
    }
    Ok((filters, filter_expr_bytes))
}

fn read_string_list(bytes: &[u8], cursor: &mut usize) -> Result<Vec<String>> {
    let len = usize::try_from(read_u32(bytes, cursor)?)
        .map_err(|_| codec_err("Iceberg scan spec list length does not fit usize".to_owned()))?;
    if len > MAX_ITEM_BYTES {
        return Err(codec_err(format!(
            "Iceberg scan spec list length {len} exceeds {MAX_ITEM_BYTES}"
        )));
    }
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(read_string(bytes, cursor)?);
    }
    Ok(values)
}

fn read_props(bytes: &[u8], cursor: &mut usize) -> Result<HashMap<String, String>> {
    let len = usize::try_from(read_u32(bytes, cursor)?)
        .map_err(|_| codec_err("Iceberg scan spec props length does not fit usize".to_owned()))?;
    if len > MAX_ITEM_BYTES {
        return Err(codec_err(format!(
            "Iceberg scan spec props length {len} exceeds {MAX_ITEM_BYTES}"
        )));
    }
    let mut props = HashMap::with_capacity(len);
    for _ in 0..len {
        let key = read_string(bytes, cursor)?;
        let value = read_string(bytes, cursor)?;
        if props.insert(key.clone(), value).is_some() {
            return Err(codec_err(format!(
                "Iceberg scan spec duplicate catalog prop {key}"
            )));
        }
    }
    Ok(props)
}

fn read_optional_i64(bytes: &[u8], cursor: &mut usize) -> Result<Option<i64>> {
    match read_u8(bytes, cursor)? {
        0 => Ok(None),
        1 => Ok(Some(read_i64(bytes, cursor)?)),
        flag => Err(codec_err(format!(
            "Iceberg scan spec snapshot flag {flag} is not 0 or 1"
        ))),
    }
}

fn projection_indices(schema: &SchemaRef, names: &[String]) -> Result<Vec<usize>> {
    let mut indices = Vec::with_capacity(names.len());
    for name in names {
        let index = schema
            .index_of(name)
            .map_err(|error| Error::DataFusion(error.to_string()))?;
        indices.push(index);
    }
    Ok(indices)
}

fn files_metadata_sql(table_ref: &TableReference) -> String {
    let catalog = table_ref.catalog().unwrap_or("datafusion");
    let namespace = table_ref.schema().unwrap_or("public");
    let table = table_ref.table();
    format!("SELECT file_path FROM {catalog}.{namespace}.\"{table}$files\"")
}

fn count_named(root: &dyn ExecutionPlan, name: &str) -> usize {
    let mut count = 0_usize;
    let mut stack: Vec<&dyn ExecutionPlan> = vec![root];
    while let Some(node) = stack.pop() {
        if node.name() == name {
            count += 1;
        }
        for child in node.children() {
            stack.push(child.as_ref());
        }
    }
    count
}

async fn parquet_plan_for_schema(
    context: &SessionContext,
    files: &[String],
    schema: SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let frame = context
        .read_parquet(files.to_vec(), ParquetReadOptions::default())
        .await
        .map_err(engine_err)?;
    let names: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let frame = if names.is_empty() {
        frame
    } else {
        let columns: Vec<&str> = names.iter().map(String::as_str).collect();
        frame.select_columns(&columns).map_err(engine_err)?
    };
    frame.create_physical_plan().await.map_err(engine_err)
}

fn replace_named_leaves(
    plan: Arc<dyn ExecutionPlan>,
    replacement: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if plan.name() == ICEBERG_TABLE_SCAN {
        return Ok(replacement);
    }
    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }
    let mut next = Vec::with_capacity(children.len());
    for child in children {
        next.push(replace_named_leaves(
            Arc::clone(child),
            Arc::clone(&replacement),
        )?);
    }
    plan.with_new_children(next).map_err(engine_err)
}

async fn replace_iceberg_scans(
    plan: Arc<dyn ExecutionPlan>,
    context: &SessionContext,
    files: &[String],
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = first_named_schema(plan.as_ref(), ICEBERG_TABLE_SCAN).ok_or_else(|| {
        codec_err("Iceberg table scan vanished while rewriting file groups".to_owned())
    })?;
    let replacement = parquet_plan_for_schema(context, files, schema).await?;
    replace_named_leaves(plan, replacement)
}

fn first_named_schema(root: &dyn ExecutionPlan, name: &str) -> Option<SchemaRef> {
    let mut stack: Vec<&dyn ExecutionPlan> = vec![root];
    while let Some(node) = stack.pop() {
        if node.name() == name {
            return Some(node.schema());
        }
        for child in node.children() {
            stack.push(child.as_ref());
        }
    }
    None
}

fn debug_field<'text>(text: &'text str, marker: &str) -> Result<&'text str> {
    text.find(marker)
        .map(|at| &text[at + marker.len()..])
        .ok_or_else(|| {
            codec_err(format!(
                "{ICEBERG_TABLE_SCAN} debug text is missing field {marker:?}"
            ))
        })
}

fn debug_quoted_values(text: &str) -> Result<Vec<String>> {
    let mut values = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find('"') {
        rest = &rest[start + 1..];
        let Some(end) = rest.find('"') else {
            return Err(codec_err(format!(
                "{ICEBERG_TABLE_SCAN} debug text has an unterminated string in {text:?}"
            )));
        };
        values.push(rest[..end].to_owned());
        rest = &rest[end + 1..];
    }
    Ok(values)
}

pub(crate) fn scan_node_resolved_snapshot_id(node: &Arc<dyn ExecutionPlan>) -> Result<i64> {
    node.as_ref()
        .downcast_ref::<IcebergTableScan>()
        .map(IcebergTableScan::resolved_snapshot_id)
        .ok_or_else(|| codec_err(format!("node {} is not {ICEBERG_TABLE_SCAN}", node.name())))
}

#[must_use]
pub fn iceberg_scan_predicates_match(
    left: &Arc<dyn ExecutionPlan>,
    right: &Arc<dyn ExecutionPlan>,
) -> bool {
    scan_predicates(left) == scan_predicates(right)
}

fn scan_predicates(node: &Arc<dyn ExecutionPlan>) -> Option<iceberg::expr::Predicate> {
    node.as_ref()
        .downcast_ref::<IcebergTableScan>()
        .and_then(IcebergTableScan::predicates)
        .cloned()
}

fn session_catalog_spec(
    context: &SessionContext,
    namespace: &str,
    table: &str,
) -> Result<CatalogSpec> {
    let mut hits = Vec::new();
    for name in context.catalog_names() {
        let Some(catalog) = context.catalog(&name) else {
            continue;
        };
        let debug = format!("{catalog:?}");
        if !debug.starts_with("ReparkCatalogProvider")
            && !debug.starts_with("IcebergCatalogProvider")
        {
            continue;
        }
        let Some(schema) = catalog.schema(namespace) else {
            continue;
        };
        if schema
            .table_names()
            .iter()
            .any(|candidate| candidate == table)
        {
            hits.push((name, debug));
        }
    }
    match hits.as_slice() {
        [] => Err(codec_err(format!(
            "no session catalog holds Iceberg table {namespace}.{table}; {ICEBERG_TABLE_SCAN} \
             resolves through ReparkSessionProvider, never ambient authority"
        ))),
        [(name, debug)] => catalog_spec_from_debug(name, debug),
        _ => Err(codec_err(format!(
            "session catalogs {:?} all hold {namespace}.{table}; {ICEBERG_TABLE_SCAN} encode \
             is ambiguous",
            hits.iter().map(|(name, _)| name).collect::<Vec<_>>()
        ))),
    }
}

fn catalog_spec_from_debug(name: &str, debug: &str) -> Result<CatalogSpec> {
    let rest = debug_field(debug, "catalog: ")?;
    let end = rest
        .find(|character: char| !(character.is_alphanumeric() || character == '_'))
        .unwrap_or(rest.len());
    let kind = match &rest[..end] {
        "MemoryCatalog" => CatalogKind::Memory,
        "GlueCatalog" => CatalogKind::Glue,
        "S3TablesCatalog" => CatalogKind::S3Tables,
        "PostgresCatalog" => CatalogKind::Postgres,
        other => {
            return Err(codec_err(format!(
                "session catalog {name:?} debug handle {other:?} is not a known Iceberg \
                 catalog kind"
            )));
        }
    };
    let mut props = HashMap::new();
    if let Some(warehouse) = debug_string_field(rest, "warehouse: \"") {
        props.insert("warehouse".to_owned(), warehouse);
    }
    for (key, value) in debug_props_map(rest)? {
        props.insert(key, value);
    }
    Ok(CatalogSpec {
        name: name.to_owned(),
        kind,
        props,
    })
}

fn debug_string_field(text: &str, marker: &str) -> Option<String> {
    let rest = &text[text.find(marker)? + marker.len()..];
    rest.find('"').map(|end| rest[..end].to_owned())
}

fn debug_props_map(text: &str) -> Result<Vec<(String, String)>> {
    let Some(start) = text.find("props: {") else {
        return Ok(Vec::new());
    };
    let rest = &text[start + "props: {".len()..];
    let Some(end) = rest.find('}') else {
        return Err(codec_err(format!(
            "{ICEBERG_TABLE_SCAN} catalog props map is unterminated"
        )));
    };
    let values = debug_quoted_values(&rest[..end])?;
    let mut pairs = Vec::with_capacity(values.len() / 2);
    for pair in values.chunks_exact(2) {
        pairs.push((pair[0].clone(), pair[1].clone()));
    }
    Ok(pairs)
}
