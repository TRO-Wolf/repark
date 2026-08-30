//! `ALTER TABLE` table-level mutations on the iceberg-rust 0.9.1 **public** API.

use std::collections::HashMap;
use std::hash::BuildHasher;

use iceberg::spec::{PrimitiveType, Transform, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Result, TableIdent};

/// Where a newly added column lands in its parent struct (Spark `FIRST` / `AFTER col`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnPosition {
    /// Move the column to the start of its containing struct.
    First,
    /// Move the column to immediately after the named sibling column.
    After(String),
}

/// One schema-evolution op to fold into a single [`apply_schema_changes`] transaction.
#[derive(Debug, Clone)]
pub enum SchemaChange {
    /// `ADD COLUMN name type [COMMENT …] [FIRST|AFTER x]` — optional by default.
    AddColumn {
        /// Column name (top-level; no `.` path — nested ADD is out of I6 READY).
        name: String,
        /// Iceberg field type.
        field_type: Type,
        /// Optional column doc (Spark `COMMENT '…'`).
        doc: Option<String>,
        /// When true the add is required (incompatible without a default — the SQL layer refuses
        required: bool,
        /// Optional Spark column position after the add.
        position: Option<ColumnPosition>,
    },
    /// `DROP COLUMN name`.
    DropColumn {
        /// Column name to delete (and all descendants).
        name: String,
    },
    /// `RENAME COLUMN old TO new`.
    RenameColumn {
        /// Existing column name.
        from: String,
        /// New column name (field-id preserved).
        to: String,
    },
    /// `ALTER COLUMN name TYPE <primitive>` — Iceberg type promotion only (int→long, float→double,
    UpdateColumnType {
        /// Column to promote.
        name: String,
        /// Target primitive type.
        new_type: PrimitiveType,
    },
    /// `ALTER COLUMN name DROP NOT NULL` — make the column optional.
    MakeColumnOptional {
        /// Column to relax.
        name: String,
    },
    /// `ALTER COLUMN name COMMENT '…'` / clear doc with `None`.
    UpdateColumnDoc {
        /// Column whose doc is replaced.
        name: String,
        /// New documentation string (`None` clears).
        doc: Option<String>,
    },
}

/// One partition-spec evolution op to fold into a single [`apply_partition_spec_changes`]
#[derive(Debug, Clone)]
pub enum PartitionSpecChange {
    /// `ADD PARTITION FIELD <transform>(source) [AS name]` — identity when transform is
    AddField {
        /// Source column name (schema field).
        source_name: String,
        /// Partition transform (identity / bucket[N] / truncate[W] / year / month / day / hour).
        transform: Transform,
        /// Optional partition field name (`AS name`).
        name: Option<String>,
    },
    /// `DROP PARTITION FIELD name` — remove by partition (target) name.
    RemoveFieldByName {
        /// Partition field name to remove.
        name: String,
    },
    /// `DROP PARTITION FIELD <transform>(source)` — remove by source + transform pair.
    RemoveFieldByTransform {
        /// Source column name.
        source_name: String,
        /// Transform that identifies the field.
        transform: Transform,
    },
    /// `REPLACE PARTITION FIELD old WITH <transform>(source) [AS name]` — remove then add in one
    ReplaceField {
        /// Existing partition field name to drop.
        old_name: String,
        /// Source column for the replacement field.
        source_name: String,
        /// Transform for the replacement field.
        transform: Transform,
        /// Optional new partition field name.
        new_name: Option<String>,
    },
    /// Rename an existing partition field (not a Spark DDL surface in I7 READY; available for
    RenameField {
        /// Current partition field name.
        name: String,
        /// New partition field name.
        new_name: String,
    },
}

/// `ALTER TABLE … SET TBLPROPERTIES (…)` — set/overwrite table properties.
/// # Errors
/// Propagates any [`iceberg::Error`] from loading the table or committing the transaction.
pub async fn set_table_properties<S: BuildHasher>(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    properties: &HashMap<String, String, S>,
) -> Result<()> {
    let table = catalog.load_table(ident).await?;
    let tx = Transaction::new(&table);
    let mut action = tx.update_table_properties();
    for (key, value) in properties {
        action = action.set(key.clone(), value.clone());
    }
    let tx = action.apply(tx)?;
    tx.commit(catalog).await?;
    Ok(())
}

/// `ALTER TABLE … UNSET TBLPROPERTIES (…)` — remove table properties by key.
/// # Errors
/// Propagates any [`iceberg::Error`] from loading the table or committing the transaction.
pub async fn unset_table_properties(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    keys: &[String],
) -> Result<()> {
    let table = catalog.load_table(ident).await?;
    let tx = Transaction::new(&table);
    let mut action = tx.update_table_properties();
    for key in keys {
        action = action.remove(key.clone());
    }
    let tx = action.apply(tx)?;
    tx.commit(catalog).await?;
    Ok(())
}

/// Apply all property changes in one atomic transaction (BUG-012).
/// # Errors
/// Propagates any [`iceberg::Error`] from loading the table or committing the transaction.
pub async fn alter_table_properties<S: BuildHasher>(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    sets: &HashMap<String, String, S>,
    unsets: &[String],
) -> Result<()> {
    let table = catalog.load_table(ident).await?;
    let tx = Transaction::new(&table);
    let mut action = tx.update_table_properties();
    for (key, value) in sets {
        action = action.set(key.clone(), value.clone());
    }
    for key in unsets {
        action = action.remove(key.clone());
    }
    let tx = action.apply(tx)?;
    tx.commit(catalog).await?;
    Ok(())
}

/// `ALTER TABLE … RENAME TO …` — rename a table within (or across namespaces of) a catalog.
/// # Errors
/// Propagates any [`iceberg::Error`] (e.g.
pub async fn rename_table(
    catalog: &dyn Catalog,
    src: &TableIdent,
    dest: &TableIdent,
) -> Result<()> {
    catalog.rename_table(src, dest).await
}

/// Apply a batch of schema-evolution ops as ONE `UpdateSchema` transaction (I6).
/// # Errors
/// Propagates any [`iceberg::Error`] from load, action apply (validation), or commit.
pub async fn apply_schema_changes(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    changes: &[SchemaChange],
) -> Result<()> {
    if changes.is_empty() {
        return Ok(());
    }
    let table = catalog.load_table(ident).await?;
    let tx = Transaction::new(&table);
    // Spark `spark.sql.caseSensitive=false` default — match column names case-insensitively.
    let mut action = tx.update_schema().case_sensitive(false);
    for change in changes {
        action = match change {
            SchemaChange::AddColumn {
                name,
                field_type,
                doc,
                required,
                position,
            } => {
                let with_add = if *required {
                    action.add_required_column_to(None, name, field_type.clone(), doc.as_deref())
                } else {
                    action.add_column_to(None, name, field_type.clone(), doc.as_deref())
                };
                match position {
                    Some(ColumnPosition::First) => with_add.move_first(name),
                    Some(ColumnPosition::After(reference)) => with_add.move_after(name, reference),
                    None => with_add,
                }
            }
            SchemaChange::DropColumn { name } => action.delete_column(name),
            SchemaChange::RenameColumn { from, to } => action.rename_column(from, to),
            SchemaChange::UpdateColumnType { name, new_type } => {
                action.update_column(name, new_type.clone())
            }
            SchemaChange::MakeColumnOptional { name } => action.make_column_optional(name),
            SchemaChange::UpdateColumnDoc { name, doc } => {
                action.update_column_doc(name, doc.as_deref())
            }
        };
    }
    let tx = action.apply(tx)?;
    tx.commit(catalog).await?;
    Ok(())
}

/// Apply partition-spec changes in one transaction (I7).
/// # Errors
/// Propagates any [`iceberg::Error`] from load, action apply (validation), or commit.
pub async fn apply_partition_spec_changes(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    changes: &[PartitionSpecChange],
) -> Result<()> {
    if changes.is_empty() {
        return Ok(());
    }
    let table = catalog.load_table(ident).await?;
    // Seed known partition-field names from the current default spec; track explicit names added
    let mut known_field_names: Vec<String> = table
        .metadata()
        .default_partition_spec()
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .collect();
    let resolve_field_name = |known: &[String], requested: &str| -> String {
        known
            .iter()
            .find(|name| name.eq_ignore_ascii_case(requested))
            .cloned()
            .unwrap_or_else(|| requested.to_string())
    };
    let forget_field_name = |known: &mut Vec<String>, name: &str| {
        known.retain(|existing| !existing.eq_ignore_ascii_case(name));
    };
    let tx = Transaction::new(&table);
    // Spark `spark.sql.caseSensitive=false` default — match source columns case-insensitively.
    let mut action = tx.update_partition_spec().case_sensitive(false);
    for change in changes {
        action = match change {
            PartitionSpecChange::AddField {
                source_name,
                transform,
                name,
            } => {
                if let Some(explicit_name) = name {
                    forget_field_name(&mut known_field_names, explicit_name);
                    known_field_names.push(explicit_name.clone());
                }
                action.add_field_with_transform(name.as_deref(), source_name, *transform)
            }
            PartitionSpecChange::RemoveFieldByName { name } => {
                let resolved = resolve_field_name(&known_field_names, name);
                forget_field_name(&mut known_field_names, &resolved);
                action.remove_field(&resolved)
            }
            PartitionSpecChange::RemoveFieldByTransform {
                source_name,
                transform,
            } => action.remove_field_by_transform(source_name, *transform),
            PartitionSpecChange::ReplaceField {
                old_name,
                source_name,
                transform,
                new_name,
            } => {
                let resolved_old = resolve_field_name(&known_field_names, old_name);
                forget_field_name(&mut known_field_names, &resolved_old);
                if let Some(explicit_name) = new_name {
                    forget_field_name(&mut known_field_names, explicit_name);
                    known_field_names.push(explicit_name.clone());
                }
                action.remove_field(&resolved_old).add_field_with_transform(
                    new_name.as_deref(),
                    source_name,
                    *transform,
                )
            }
            PartitionSpecChange::RenameField { name, new_name } => {
                let resolved = resolve_field_name(&known_field_names, name);
                forget_field_name(&mut known_field_names, &resolved);
                known_field_names.push(new_name.clone());
                action.rename_field(&resolved, new_name)
            }
        };
    }
    let tx = action.apply(tx)?;
    tx.commit(catalog).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::table::Table;
    use iceberg::{
        CatalogBuilder, Error, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation,
    };
    use tempfile::TempDir;

    /// An in-memory Iceberg catalog (local-FS warehouse) with a `sales` namespace and one table
    async fn setup(wh: &TempDir) -> (Arc<dyn Catalog>, TableIdent) {
        let warehouse = wh.path().to_str().unwrap().to_string();
        let catalog: Arc<dyn Catalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "memory",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
                )
                .await
                .unwrap(),
        );
        let ns = NamespaceIdent::new("sales".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        let creation = TableCreation::builder()
            .name("t".to_string())
            .schema(schema)
            .properties(HashMap::new())
            .build();
        catalog.create_table(&ns, creation).await.unwrap();
        (catalog, TableIdent::new(ns, "t".to_string()))
    }

    #[tokio::test]
    async fn set_then_unset_round_trips() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = setup(&wh).await;

        let props = HashMap::from([
            ("team".to_string(), "example-team".to_string()),
            ("write.format.default".to_string(), "parquet".to_string()),
        ]);
        set_table_properties(catalog.as_ref(), &ident, &props)
            .await
            .unwrap();

        let table = catalog.load_table(&ident).await.unwrap();
        let after = table.metadata().properties();
        assert_eq!(after.get("team").map(String::as_str), Some("example-team"));
        assert_eq!(
            after.get("write.format.default").map(String::as_str),
            Some("parquet")
        );

        // UNSET one key; the other survives.
        unset_table_properties(catalog.as_ref(), &ident, &["team".to_string()])
            .await
            .unwrap();
        let table = catalog.load_table(&ident).await.unwrap();
        let after = table.metadata().properties();
        assert!(!after.contains_key("team"));
        assert_eq!(
            after.get("write.format.default").map(String::as_str),
            Some("parquet")
        );
    }

    #[tokio::test]
    async fn unset_absent_key_is_a_no_op() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = setup(&wh).await;
        // Removing a key that was never set must succeed (idempotent, like Spark).
        unset_table_properties(catalog.as_ref(), &ident, &["never-set".to_string()])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn rename_moves_the_table() {
        let wh = TempDir::new().unwrap();
        let (catalog, src) = setup(&wh).await;
        let dest = TableIdent::new(NamespaceIdent::new("sales".to_string()), "t2".to_string());

        rename_table(catalog.as_ref(), &src, &dest).await.unwrap();

        assert!(catalog.table_exists(&dest).await.unwrap());
        assert!(!catalog.table_exists(&src).await.unwrap());
        // The renamed table loads under the new ident.
        catalog.load_table(&dest).await.unwrap();
    }

    /// The boxed-future return type of an `#[async_trait]` `Catalog` method — spelled out once so
    type BoxedCatalogFuture<'a, T> = Pin<Box<dyn Future<Output = iceberg::Result<T>> + Send + 'a>>;

    /// A fully-delegating `Catalog` wrapper that COUNTS every `update_table` (commit) CAS and can
    #[derive(Debug)]
    struct CommitFaultCatalog {
        inner: Arc<dyn Catalog>,
        update_table_calls: AtomicUsize,
        /// If `Some(n)`, the `n`-th `update_table` call returns an error instead of committing.
        fail_on_call: Option<usize>,
    }

    impl CommitFaultCatalog {
        fn new(inner: Arc<dyn Catalog>, fail_on_call: Option<usize>) -> Self {
            Self {
                inner,
                update_table_calls: AtomicUsize::new(0),
                fail_on_call,
            }
        }

        /// How many `update_table` (commit) CAS calls have been attempted through this wrapper.
        fn update_table_calls(&self) -> usize {
            self.update_table_calls.load(Ordering::SeqCst)
        }
    }

    impl Catalog for CommitFaultCatalog {
        fn list_namespaces<'life0, 'life1, 'async_trait>(
            &'life0 self,
            parent: Option<&'life1 NamespaceIdent>,
        ) -> BoxedCatalogFuture<'async_trait, Vec<NamespaceIdent>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.list_namespaces(parent)
        }

        fn create_namespace<'life0, 'life1, 'async_trait>(
            &'life0 self,
            namespace: &'life1 NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> BoxedCatalogFuture<'async_trait, Namespace>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.create_namespace(namespace, properties)
        }

        fn get_namespace<'life0, 'life1, 'async_trait>(
            &'life0 self,
            namespace: &'life1 NamespaceIdent,
        ) -> BoxedCatalogFuture<'async_trait, Namespace>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.get_namespace(namespace)
        }

        fn namespace_exists<'life0, 'life1, 'async_trait>(
            &'life0 self,
            namespace: &'life1 NamespaceIdent,
        ) -> BoxedCatalogFuture<'async_trait, bool>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.namespace_exists(namespace)
        }

        fn update_namespace<'life0, 'life1, 'async_trait>(
            &'life0 self,
            namespace: &'life1 NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> BoxedCatalogFuture<'async_trait, ()>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.update_namespace(namespace, properties)
        }

        fn drop_namespace<'life0, 'life1, 'async_trait>(
            &'life0 self,
            namespace: &'life1 NamespaceIdent,
        ) -> BoxedCatalogFuture<'async_trait, ()>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.drop_namespace(namespace)
        }

        fn list_tables<'life0, 'life1, 'async_trait>(
            &'life0 self,
            namespace: &'life1 NamespaceIdent,
        ) -> BoxedCatalogFuture<'async_trait, Vec<TableIdent>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.list_tables(namespace)
        }

        fn create_table<'life0, 'life1, 'async_trait>(
            &'life0 self,
            namespace: &'life1 NamespaceIdent,
            creation: TableCreation,
        ) -> BoxedCatalogFuture<'async_trait, Table>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.create_table(namespace, creation)
        }

        fn load_table<'life0, 'life1, 'async_trait>(
            &'life0 self,
            table: &'life1 TableIdent,
        ) -> BoxedCatalogFuture<'async_trait, Table>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.load_table(table)
        }

        fn drop_table<'life0, 'life1, 'async_trait>(
            &'life0 self,
            table: &'life1 TableIdent,
        ) -> BoxedCatalogFuture<'async_trait, ()>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.drop_table(table)
        }

        fn table_exists<'life0, 'life1, 'async_trait>(
            &'life0 self,
            table: &'life1 TableIdent,
        ) -> BoxedCatalogFuture<'async_trait, bool>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.table_exists(table)
        }

        fn rename_table<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 self,
            src: &'life1 TableIdent,
            dest: &'life2 TableIdent,
        ) -> BoxedCatalogFuture<'async_trait, ()>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.rename_table(src, dest)
        }

        fn register_table<'life0, 'life1, 'async_trait>(
            &'life0 self,
            table: &'life1 TableIdent,
            metadata_location: String,
        ) -> BoxedCatalogFuture<'async_trait, Table>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.register_table(table, metadata_location)
        }

        fn update_table<'life0, 'async_trait>(
            &'life0 self,
            commit: TableCommit,
        ) -> BoxedCatalogFuture<'async_trait, Table>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move {
                let call = self.update_table_calls.fetch_add(1, Ordering::SeqCst) + 1;
                if self.fail_on_call == Some(call) {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        format!("injected commit failure on update_table call #{call}"),
                    ));
                }
                self.inner.update_table(commit).await
            })
        }
    }

    /// Seed two properties on `t` directly (an out-of-band commit not counted by the wrapper).
    async fn seed_properties(catalog: &Arc<dyn Catalog>, ident: &TableIdent) {
        let seed = HashMap::from([
            ("keep".to_string(), "old".to_string()),
            ("drop".to_string(), "x".to_string()),
        ]);
        set_table_properties(catalog.as_ref(), ident, &seed)
            .await
            .unwrap();
    }

    /// B12-1 — a mixed (disjoint-key) SET+UNSET commits in EXACTLY ONE catalog `update_table`
    #[tokio::test]
    async fn alter_mixed_disjoint_set_unset_commits_exactly_once() {
        let wh = TempDir::new().unwrap();
        let (inner, ident) = setup(&wh).await;
        seed_properties(&inner, &ident).await;

        let counting = CommitFaultCatalog::new(inner.clone(), None);
        let sets = HashMap::from([("keep".to_string(), "new".to_string())]);
        let unsets = vec!["drop".to_string()];
        alter_table_properties(&counting, &ident, &sets, &unsets)
            .await
            .unwrap();

        assert_eq!(
            counting.update_table_calls(),
            1,
            "a mixed SET+UNSET must commit as ONE transaction (one update_table CAS)"
        );
        let table = inner.load_table(&ident).await.unwrap();
        let props = table.metadata().properties();
        assert_eq!(props.get("keep").map(String::as_str), Some("new"));
        assert!(
            !props.contains_key("drop"),
            "the UNSET must have landed too"
        );
    }

    /// B12-2 — a fault armed at the SECOND catalog commit (the exact gap the old two-commit path
    #[tokio::test]
    async fn alter_mixed_injected_second_commit_failure_leaves_no_partial_state() {
        let wh = TempDir::new().unwrap();
        let (inner, ident) = setup(&wh).await;
        seed_properties(&inner, &ident).await;

        let faulting = CommitFaultCatalog::new(inner.clone(), Some(2));
        let sets = HashMap::from([("keep".to_string(), "new".to_string())]);
        let unsets = vec!["drop".to_string()];
        let result = alter_table_properties(&faulting, &ident, &sets, &unsets).await;

        assert!(
            result.is_ok(),
            "one transaction means the fault armed at the 2nd commit never trips: {result:?}"
        );
        let table = inner.load_table(&ident).await.unwrap();
        let props = table.metadata().properties();
        assert_eq!(props.get("keep").map(String::as_str), Some("new"));
        assert!(
            !props.contains_key("drop"),
            "no half-applied state: the UNSET landed atomically with the SET"
        );
    }

    /// A key present in BOTH the set and unset lists is rejected by the single action's
    #[tokio::test]
    async fn alter_same_key_set_and_unset_is_atomic_loud() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = setup(&wh).await;
        seed_properties(&catalog, &ident).await;

        let sets = HashMap::from([("keep".to_string(), "new".to_string())]);
        let unsets = vec!["keep".to_string()];
        let error = alter_table_properties(catalog.as_ref(), &ident, &sets, &unsets)
            .await
            .expect_err("a key both set and unset in one transaction must fail loud");
        assert!(
            error.to_string().contains("both"),
            "the error must name the set/unset overlap, got: {error}"
        );
        // Nothing changed: the original seed survives.
        let table = catalog.load_table(&ident).await.unwrap();
        let props = table.metadata().properties();
        assert_eq!(props.get("keep").map(String::as_str), Some("old"));
    }

    async fn schema_field_names(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Vec<String> {
        let table = catalog.load_table(ident).await.unwrap();
        table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect()
    }

    async fn schema_field_ids_by_name(
        catalog: &Arc<dyn Catalog>,
        ident: &TableIdent,
    ) -> HashMap<String, i32> {
        let table = catalog.load_table(ident).await.unwrap();
        table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| (field.name.clone(), field.id))
            .collect()
    }

    /// I6 — ADD + RENAME + DROP in one transaction: names and field-id stability on rename.
    #[tokio::test]
    async fn schema_add_rename_drop_round_trip() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = setup(&wh).await;

        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[
                SchemaChange::AddColumn {
                    name: "name".into(),
                    field_type: Type::Primitive(PrimitiveType::String),
                    doc: Some("display name".into()),
                    required: false,
                    position: None,
                },
                SchemaChange::AddColumn {
                    name: "score".into(),
                    field_type: Type::Primitive(PrimitiveType::Int),
                    doc: None,
                    required: false,
                    position: Some(ColumnPosition::After("id".into())),
                },
            ],
        )
        .await
        .unwrap();

        assert_eq!(
            schema_field_names(&catalog, &ident).await,
            vec!["id".to_string(), "score".to_string(), "name".to_string()]
        );
        let ids_before = schema_field_ids_by_name(&catalog, &ident).await;
        let score_id = ids_before["score"];

        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::RenameColumn {
                from: "score".into(),
                to: "rating".into(),
            }],
        )
        .await
        .unwrap();
        let ids_after = schema_field_ids_by_name(&catalog, &ident).await;
        assert_eq!(
            ids_after.get("rating").copied(),
            Some(score_id),
            "rename must preserve field-id"
        );
        assert!(!ids_after.contains_key("score"));

        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::DropColumn {
                name: "name".into(),
            }],
        )
        .await
        .unwrap();
        assert_eq!(
            schema_field_names(&catalog, &ident).await,
            vec!["id".to_string(), "rating".to_string()]
        );
    }

    /// I6 — ADD FIRST lands the new column at the front of the struct.
    #[tokio::test]
    async fn schema_add_column_first_position() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = setup(&wh).await;
        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::AddColumn {
                name: "lead".into(),
                field_type: Type::Primitive(PrimitiveType::Boolean),
                doc: None,
                required: false,
                position: Some(ColumnPosition::First),
            }],
        )
        .await
        .unwrap();
        assert_eq!(
            schema_field_names(&catalog, &ident).await,
            vec!["lead".to_string(), "id".to_string()]
        );
    }

    /// I6 stretch — int→long widen lands; long→int narrow refuses loud (twin pin).
    #[tokio::test]
    async fn schema_type_widen_int_to_long_and_narrow_refuses() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = setup(&wh).await;
        // setup's `id` is Int.
        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::UpdateColumnType {
                name: "id".into(),
                new_type: PrimitiveType::Long,
            }],
        )
        .await
        .unwrap();
        let table = catalog.load_table(&ident).await.unwrap();
        let id_field = table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .find(|field| field.name == "id")
            .unwrap();
        assert_eq!(
            id_field.field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );

        let error = apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::UpdateColumnType {
                name: "id".into(),
                new_type: PrimitiveType::Int,
            }],
        )
        .await
        .expect_err("narrow long→int must refuse");
        let message = error.to_string().to_lowercase();
        assert!(
            message.contains("cannot change column type")
                || message.contains("cannot be promoted")
                || message.contains("promote"),
            "narrow refusal must name the type change, got: {error}"
        );
    }

    /// I6 stretch — DROP NOT NULL makes a required column optional.
    #[tokio::test]
    async fn schema_make_column_optional() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = setup(&wh).await;
        // setup's `id` is required.
        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::MakeColumnOptional { name: "id".into() }],
        )
        .await
        .unwrap();
        let table = catalog.load_table(&ident).await.unwrap();
        let id_field = table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .find(|field| field.name == "id")
            .unwrap();
        assert!(!id_field.required);
    }

    /// Build a one-column optional table for type-promotion twins.
    async fn setup_typed(
        warehouse: &TempDir,
        name: &str,
        field_type: Type,
    ) -> (Arc<dyn Catalog>, TableIdent) {
        let warehouse_path = warehouse.path().to_str().unwrap().to_string();
        let catalog: Arc<dyn Catalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "memory",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path)]),
                )
                .await
                .unwrap(),
        );
        let namespace = NamespaceIdent::new("sales".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![NestedField::optional(1, name, field_type).into()])
            .build()
            .unwrap();
        let creation = TableCreation::builder()
            .name("t".to_string())
            .schema(schema)
            .properties(HashMap::new())
            .build();
        catalog.create_table(&namespace, creation).await.unwrap();
        (catalog, TableIdent::new(namespace, "t".to_string()))
    }

    async fn field_primitive(
        catalog: &Arc<dyn Catalog>,
        ident: &TableIdent,
        name: &str,
    ) -> PrimitiveType {
        let table = catalog.load_table(ident).await.unwrap();
        match table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .find(|field| field.name == name)
            .unwrap()
            .field_type
            .as_ref()
        {
            Type::Primitive(primitive) => primitive.clone(),
            other => panic!("expected primitive, got {other:?}"),
        }
    }

    /// Float→double widen lands; double→float narrow refuses (ledger twin claim).
    #[tokio::test]
    async fn schema_type_widen_float_to_double_and_narrow_refuses() {
        let warehouse = TempDir::new().unwrap();
        let (catalog, ident) =
            setup_typed(&warehouse, "measure", Type::Primitive(PrimitiveType::Float)).await;
        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::UpdateColumnType {
                name: "measure".into(),
                new_type: PrimitiveType::Double,
            }],
        )
        .await
        .unwrap();
        assert_eq!(
            field_primitive(&catalog, &ident, "measure").await,
            PrimitiveType::Double
        );

        let error = apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::UpdateColumnType {
                name: "measure".into(),
                new_type: PrimitiveType::Float,
            }],
        )
        .await
        .expect_err("narrow double→float must refuse");
        let message = error.to_string().to_lowercase();
        assert!(
            message.contains("cannot change column type")
                || message.contains("cannot be promoted")
                || message.contains("promote"),
            "narrow refusal must name the type change, got: {error}"
        );
    }

    /// Decimal same-scale precision widen lands; narrow precision refuses.
    #[tokio::test]
    async fn schema_type_widen_decimal_precision_and_narrow_refuses() {
        let warehouse = TempDir::new().unwrap();
        let (catalog, ident) = setup_typed(
            &warehouse,
            "amount",
            Type::Primitive(PrimitiveType::Decimal {
                precision: 5,
                scale: 2,
            }),
        )
        .await;
        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::UpdateColumnType {
                name: "amount".into(),
                new_type: PrimitiveType::Decimal {
                    precision: 10,
                    scale: 2,
                },
            }],
        )
        .await
        .unwrap();
        assert_eq!(
            field_primitive(&catalog, &ident, "amount").await,
            PrimitiveType::Decimal {
                precision: 10,
                scale: 2
            }
        );

        let error = apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::UpdateColumnType {
                name: "amount".into(),
                new_type: PrimitiveType::Decimal {
                    precision: 5,
                    scale: 2,
                },
            }],
        )
        .await
        .expect_err("narrow decimal precision must refuse");
        let message = error.to_string().to_lowercase();
        assert!(
            message.contains("cannot change column type")
                || message.contains("cannot be promoted")
                || message.contains("promote")
                || message.contains("decimal"),
            "narrow refusal must name the type change, got: {error}"
        );
    }

    /// Multi schema-op batch commits as exactly ONE catalog `update_table` CAS.
    #[tokio::test]
    async fn schema_multi_change_commits_exactly_once() {
        let warehouse = TempDir::new().unwrap();
        let (inner, ident) = setup(&warehouse).await;
        let counting = CommitFaultCatalog::new(inner.clone(), None);
        apply_schema_changes(
            &counting,
            &ident,
            &[
                SchemaChange::AddColumn {
                    name: "a".into(),
                    field_type: Type::Primitive(PrimitiveType::String),
                    doc: None,
                    required: false,
                    position: None,
                },
                SchemaChange::AddColumn {
                    name: "b".into(),
                    field_type: Type::Primitive(PrimitiveType::Int),
                    doc: None,
                    required: false,
                    position: Some(ColumnPosition::After("id".into())),
                },
                SchemaChange::RenameColumn {
                    from: "id".into(),
                    to: "event_id".into(),
                },
            ],
        )
        .await
        .unwrap();
        assert_eq!(
            counting.update_table_calls(),
            1,
            "batched schema changes must be one UpdateSchema CAS"
        );
        assert_eq!(
            schema_field_names(&inner, &ident).await,
            vec!["event_id".to_string(), "b".to_string(), "a".to_string()]
        );
    }

    /// Twin — injected commit failure leaves schema unchanged (no partial batch).
    #[tokio::test]
    async fn schema_injected_commit_failure_leaves_no_partial_state() {
        let warehouse = TempDir::new().unwrap();
        let (inner, ident) = setup(&warehouse).await;
        let before = schema_field_names(&inner, &ident).await;
        let faulting = CommitFaultCatalog::new(inner.clone(), Some(1));
        let error = apply_schema_changes(
            &faulting,
            &ident,
            &[
                SchemaChange::AddColumn {
                    name: "ghost".into(),
                    field_type: Type::Primitive(PrimitiveType::String),
                    doc: None,
                    required: false,
                    position: None,
                },
                SchemaChange::DropColumn { name: "id".into() },
            ],
        )
        .await
        .expect_err("injected commit failure must surface");
        assert!(
            error.to_string().contains("injected commit failure"),
            "got: {error}"
        );
        assert_eq!(
            schema_field_names(&inner, &ident).await,
            before,
            "failed CAS must leave schema unchanged"
        );
    }

    /// Residual F-I6-C4-R1 — same-batch add-then-rename of the *new* column is refused loud by the
    #[tokio::test]
    async fn schema_same_batch_add_then_rename_new_column_refuses_loud() {
        let warehouse = TempDir::new().unwrap();
        let (catalog, ident) = setup(&warehouse).await;
        let before = schema_field_names(&catalog, &ident).await;
        let error = apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[
                SchemaChange::AddColumn {
                    name: "fresh".into(),
                    field_type: Type::Primitive(PrimitiveType::String),
                    doc: None,
                    required: false,
                    position: None,
                },
                SchemaChange::RenameColumn {
                    from: "fresh".into(),
                    to: "renamed".into(),
                },
            ],
        )
        .await
        .expect_err("same-batch add-then-rename of new column must refuse");
        let message = error.to_string().to_lowercase();
        assert!(
            message.contains("missing") || message.contains("cannot rename"),
            "got: {error}"
        );
        assert_eq!(
            schema_field_names(&catalog, &ident).await,
            before,
            "failed batch must leave schema unchanged"
        );
    }

    /// Case-insensitive column resolution (Spark default) for rename + drop.
    #[tokio::test]
    async fn schema_case_insensitive_rename_and_drop() {
        let warehouse = TempDir::new().unwrap();
        let (catalog, ident) = setup(&warehouse).await;
        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::AddColumn {
                name: "Name".into(),
                field_type: Type::Primitive(PrimitiveType::String),
                doc: None,
                required: false,
                position: None,
            }],
        )
        .await
        .unwrap();
        let ids_before = schema_field_ids_by_name(&catalog, &ident).await;
        let name_id = ids_before["Name"];

        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::RenameColumn {
                from: "name".into(), // different case
                to: "display".into(),
            }],
        )
        .await
        .unwrap();
        let ids_after = schema_field_ids_by_name(&catalog, &ident).await;
        assert_eq!(ids_after.get("display").copied(), Some(name_id));
        assert!(!ids_after.contains_key("Name"));

        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::DropColumn {
                name: "DISPLAY".into(),
            }],
        )
        .await
        .unwrap();
        assert_eq!(
            schema_field_names(&catalog, &ident).await,
            vec!["id".to_string()]
        );
    }

    /// Partition-field names on the current default spec (ordered).
    async fn default_partition_field_names(
        catalog: &Arc<dyn Catalog>,
        ident: &TableIdent,
    ) -> Vec<String> {
        let table = catalog.load_table(ident).await.unwrap();
        table
            .metadata()
            .default_partition_spec()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect()
    }

    /// Default partition-spec id after evolution.
    async fn default_spec_id(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> i32 {
        let table = catalog.load_table(ident).await.unwrap();
        table.metadata().default_partition_spec_id()
    }

    /// I7 — ADD identity + bucket, DROP by name, REPLACE field; default spec id advances.
    #[tokio::test]
    async fn partition_spec_add_drop_replace_round_trip() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = setup(&wh).await;
        // setup: unpartitioned, schema has `id` int.
        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::AddColumn {
                name: "category".into(),
                field_type: Type::Primitive(PrimitiveType::String),
                doc: None,
                required: false,
                position: None,
            }],
        )
        .await
        .unwrap();

        let before_id = default_spec_id(&catalog, &ident).await;
        apply_partition_spec_changes(
            catalog.as_ref(),
            &ident,
            &[PartitionSpecChange::AddField {
                source_name: "category".into(),
                transform: Transform::Identity,
                name: None,
            }],
        )
        .await
        .unwrap();
        assert_eq!(
            default_partition_field_names(&catalog, &ident).await,
            vec!["category".to_string()]
        );
        let after_add = default_spec_id(&catalog, &ident).await;
        assert_ne!(after_add, before_id, "ADD must publish a new default spec");

        apply_partition_spec_changes(
            catalog.as_ref(),
            &ident,
            &[PartitionSpecChange::AddField {
                source_name: "id".into(),
                transform: Transform::Bucket(8),
                name: Some("id_b8".into()),
            }],
        )
        .await
        .unwrap();
        let names = default_partition_field_names(&catalog, &ident).await;
        assert!(names.contains(&"category".to_string()));
        assert!(names.contains(&"id_b8".to_string()));

        apply_partition_spec_changes(
            catalog.as_ref(),
            &ident,
            &[PartitionSpecChange::RemoveFieldByName {
                name: "category".into(),
            }],
        )
        .await
        .unwrap();
        assert_eq!(
            default_partition_field_names(&catalog, &ident).await,
            vec!["id_b8".to_string()]
        );

        apply_partition_spec_changes(
            catalog.as_ref(),
            &ident,
            &[PartitionSpecChange::ReplaceField {
                old_name: "id_b8".into(),
                source_name: "id".into(),
                transform: Transform::Bucket(16),
                new_name: Some("id_b16".into()),
            }],
        )
        .await
        .unwrap();
        assert_eq!(
            default_partition_field_names(&catalog, &ident).await,
            vec!["id_b16".to_string()]
        );
    }

    /// I7 — DROP by transform pair; auto-named bucket field.
    #[tokio::test]
    async fn partition_spec_remove_by_transform() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = setup(&wh).await;
        apply_partition_spec_changes(
            catalog.as_ref(),
            &ident,
            &[PartitionSpecChange::AddField {
                source_name: "id".into(),
                transform: Transform::Bucket(4),
                name: None,
            }],
        )
        .await
        .unwrap();
        // Fork auto-name: id_bucket_4
        assert_eq!(
            default_partition_field_names(&catalog, &ident).await,
            vec!["id_bucket_4".to_string()]
        );
        apply_partition_spec_changes(
            catalog.as_ref(),
            &ident,
            &[PartitionSpecChange::RemoveFieldByTransform {
                source_name: "id".into(),
                transform: Transform::Bucket(4),
            }],
        )
        .await
        .unwrap();
        assert!(
            default_partition_field_names(&catalog, &ident)
                .await
                .is_empty()
                || catalog
                    .load_table(&ident)
                    .await
                    .unwrap()
                    .metadata()
                    .default_partition_spec()
                    .is_unpartitioned()
        );
    }

    /// I7 — case-insensitive source column on ADD (Spark caseSensitive=false).
    #[tokio::test]
    async fn partition_spec_add_case_insensitive_source() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = setup(&wh).await;
        apply_partition_spec_changes(
            catalog.as_ref(),
            &ident,
            &[PartitionSpecChange::AddField {
                source_name: "ID".into(),
                transform: Transform::Identity,
                name: Some("id_part".into()),
            }],
        )
        .await
        .unwrap();
        assert_eq!(
            default_partition_field_names(&catalog, &ident).await,
            vec!["id_part".to_string()]
        );
    }

    /// I7 — ADD unknown source refuses loud.
    #[tokio::test]
    async fn partition_spec_add_unknown_source_refuses() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = setup(&wh).await;
        let error = apply_partition_spec_changes(
            catalog.as_ref(),
            &ident,
            &[PartitionSpecChange::AddField {
                source_name: "missing_col".into(),
                transform: Transform::Identity,
                name: None,
            }],
        )
        .await
        .expect_err("unknown source must refuse");
        let message = error.to_string().to_lowercase();
        assert!(
            message.contains("missing_col")
                || message.contains("cannot find")
                || message.contains("not found")
                || message.contains("unknown"),
            "got: {error}"
        );
    }

    /// Multi [`PartitionSpecChange`] batch commits as exactly ONE catalog CAS.
    #[tokio::test]
    async fn partition_spec_multi_change_commits_exactly_once() {
        let warehouse = TempDir::new().unwrap();
        let (inner, ident) = setup(&warehouse).await;
        apply_schema_changes(
            inner.as_ref(),
            &ident,
            &[SchemaChange::AddColumn {
                name: "category".into(),
                field_type: Type::Primitive(PrimitiveType::String),
                doc: None,
                required: false,
                position: None,
            }],
        )
        .await
        .unwrap();
        let counting = CommitFaultCatalog::new(inner.clone(), None);
        apply_partition_spec_changes(
            &counting,
            &ident,
            &[
                PartitionSpecChange::AddField {
                    source_name: "category".into(),
                    transform: Transform::Identity,
                    name: Some("cat".into()),
                },
                PartitionSpecChange::AddField {
                    source_name: "id".into(),
                    transform: Transform::Bucket(8),
                    name: Some("id_b8".into()),
                },
            ],
        )
        .await
        .unwrap();
        assert_eq!(
            counting.update_table_calls(),
            1,
            "batched partition-spec changes must be one UpdatePartitionSpec CAS"
        );
        let names = default_partition_field_names(&inner, &ident).await;
        assert!(names.contains(&"cat".to_string()) && names.contains(&"id_b8".to_string()));
    }

    /// Twin — injected partition commit failure leaves default spec unchanged.
    #[tokio::test]
    async fn partition_spec_injected_commit_failure_leaves_no_partial_state() {
        let warehouse = TempDir::new().unwrap();
        let (inner, ident) = setup(&warehouse).await;
        let before_id = default_spec_id(&inner, &ident).await;
        let before_names = default_partition_field_names(&inner, &ident).await;
        let faulting = CommitFaultCatalog::new(inner.clone(), Some(1));
        let error = apply_partition_spec_changes(
            &faulting,
            &ident,
            &[PartitionSpecChange::AddField {
                source_name: "id".into(),
                transform: Transform::Bucket(4),
                name: Some("id_b4".into()),
            }],
        )
        .await
        .expect_err("injected commit failure must surface");
        let _ = error;
        assert_eq!(faulting.update_table_calls(), 1);
        assert_eq!(default_spec_id(&inner, &ident).await, before_id);
        assert_eq!(
            default_partition_field_names(&inner, &ident).await,
            before_names
        );
    }

    /// Double ADD of the same identity field refuses loud (no silent no-op).
    #[tokio::test]
    async fn partition_spec_double_add_same_field_refuses() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = setup(&wh).await;
        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::AddColumn {
                name: "category".into(),
                field_type: Type::Primitive(PrimitiveType::String),
                doc: None,
                required: false,
                position: None,
            }],
        )
        .await
        .unwrap();
        apply_partition_spec_changes(
            catalog.as_ref(),
            &ident,
            &[PartitionSpecChange::AddField {
                source_name: "category".into(),
                transform: Transform::Identity,
                name: Some("cat".into()),
            }],
        )
        .await
        .unwrap();
        let error = apply_partition_spec_changes(
            catalog.as_ref(),
            &ident,
            &[PartitionSpecChange::AddField {
                source_name: "category".into(),
                transform: Transform::Identity,
                name: Some("cat".into()),
            }],
        )
        .await
        .expect_err("double ADD same partition field must refuse");
        let message = error.to_string().to_lowercase();
        assert!(
            message.contains("already")
                || message.contains("duplicate")
                || message.contains("more than once")
                || message.contains("cannot")
                || message.contains("exist"),
            "got: {error}"
        );
    }

    /// DROP/REPLACE partition field names are case-insensitive (Spark default).
    #[tokio::test]
    async fn partition_spec_drop_replace_field_name_case_insensitive() {
        let wh = TempDir::new().unwrap();
        let (catalog, ident) = setup(&wh).await;
        apply_schema_changes(
            catalog.as_ref(),
            &ident,
            &[SchemaChange::AddColumn {
                name: "category".into(),
                field_type: Type::Primitive(PrimitiveType::String),
                doc: None,
                required: false,
                position: None,
            }],
        )
        .await
        .unwrap();
        apply_partition_spec_changes(
            catalog.as_ref(),
            &ident,
            &[PartitionSpecChange::AddField {
                source_name: "category".into(),
                transform: Transform::Identity,
                name: Some("cat".into()),
            }],
        )
        .await
        .unwrap();
        // DROP with different case than stored field name.
        apply_partition_spec_changes(
            catalog.as_ref(),
            &ident,
            &[PartitionSpecChange::RemoveFieldByName { name: "CAT".into() }],
        )
        .await
        .expect("DROP PARTITION FIELD name must be case-insensitive");
        assert!(
            catalog
                .load_table(&ident)
                .await
                .unwrap()
                .metadata()
                .default_partition_spec()
                .is_unpartitioned()
                || default_partition_field_names(&catalog, &ident)
                    .await
                    .is_empty()
        );

        apply_partition_spec_changes(
            catalog.as_ref(),
            &ident,
            &[PartitionSpecChange::AddField {
                source_name: "id".into(),
                transform: Transform::Bucket(4),
                name: Some("id_b4".into()),
            }],
        )
        .await
        .unwrap();
        apply_partition_spec_changes(
            catalog.as_ref(),
            &ident,
            &[PartitionSpecChange::ReplaceField {
                old_name: "ID_B4".into(),
                source_name: "id".into(),
                transform: Transform::Bucket(8),
                new_name: Some("id_b8".into()),
            }],
        )
        .await
        .expect("REPLACE PARTITION FIELD old name must be case-insensitive");
        assert_eq!(
            default_partition_field_names(&catalog, &ident).await,
            vec!["id_b8".to_string()]
        );
    }
}
