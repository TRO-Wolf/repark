use std::collections::HashMap;

use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use iceberg::spec::{
    Schema as IcebergSchema, SqlViewRepresentation, ViewRepresentation, ViewRepresentations,
};
use iceberg::view::View;
use iceberg::{Catalog, ErrorKind, NamespaceIdent, TableIdent, ViewCreation};
use repark_common::spark_error;

use super::catalog::{iceberg_to_datafusion, schema_not_found_on_drop};
use super::write::unsupported_error;

#[cfg(test)]
mod tests;

pub const VIEW_DIALECT: &str = "spark";
pub const VIEW_COMMENT_PROPERTY: &str = "comment";
pub const VIEW_PROVIDER_PROPERTY: &str = "provider";
pub const VIEW_LOCATION_PROPERTY: &str = "location";
pub const VIEW_FORMAT_VERSION_PROPERTY: &str = "format-version";
pub const VIEW_PROVIDER_ICEBERG: &str = "iceberg";
const ARROW_DOC_METADATA_KEY: &str = "doc";

pub struct ViewTarget<'a> {
    pub catalog_name: &'a str,
    pub catalog: &'a dyn Catalog,
    pub namespace: &'a NamespaceIdent,
    pub namespace_name: &'a str,
    pub view_name: &'a str,
}

pub struct ViewDefinition {
    pub sql: String,
    pub schema: IcebergSchema,
    pub properties: HashMap<String, String>,
    pub warehouse: String,
    pub location_override: Option<String>,
    pub default_catalog: String,
}

pub struct ViewReadSpec {
    pub sql: String,
    pub default_catalog: Option<String>,
    pub default_namespace: NamespaceIdent,
    pub column_names: Vec<String>,
}

#[allow(clippy::missing_errors_doc)]
pub fn view_schema_for_output(
    output: &ArrowSchema,
    aliases: &[(String, Option<String>)],
    view_name: &str,
) -> Result<IcebergSchema> {
    let mut fields = Vec::with_capacity(output.fields().len());
    if aliases.is_empty() {
        for field in output.fields() {
            fields.push((**field).clone());
        }
    } else {
        if aliases.len() != output.fields().len() {
            return Err(view_arity_mismatch(view_name, aliases, output));
        }
        for (field, (alias, comment)) in output.fields().iter().zip(aliases.iter()) {
            let mut metadata = field.metadata().clone();
            if let Some(comment) = comment {
                metadata.insert(ARROW_DOC_METADATA_KEY.to_string(), comment.clone());
            }
            fields.push(
                Field::new(alias, field.data_type().clone(), field.is_nullable())
                    .with_metadata(metadata),
            );
        }
    }
    let renamed = ArrowSchema::new(fields);
    iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&renamed).map_err(iceberg_to_datafusion)
}

fn view_arity_mismatch(
    view_name: &str,
    aliases: &[(String, Option<String>)],
    output: &ArrowSchema,
) -> DataFusionError {
    let view_columns = aliases
        .iter()
        .map(|(alias, _)| alias.clone())
        .collect::<Vec<_>>()
        .join(", ");
    let data_columns = output
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>()
        .join(", ");
    let condition = if aliases.len() > output.fields().len() {
        spark_error::CREATE_VIEW_COLUMN_ARITY_MISMATCH_NOT_ENOUGH_DATA_COLUMNS
    } else {
        spark_error::CREATE_VIEW_COLUMN_ARITY_MISMATCH_TOO_MANY_DATA_COLUMNS
    };
    DataFusionError::Plan(spark_error::message(
        condition,
        &[
            ("viewName", view_name),
            ("viewColumns", &view_columns),
            ("dataColumns", &data_columns),
        ],
    ))
}

#[allow(clippy::missing_errors_doc)]
pub fn split_view_properties<S: std::hash::BuildHasher>(
    view_comment: Option<&str>,
    tblproperties: &HashMap<String, String, S>,
) -> Result<(HashMap<String, String>, Option<String>)> {
    if let Some(provider) = tblproperties.get(VIEW_PROVIDER_PROPERTY)
        && !provider.eq_ignore_ascii_case(VIEW_PROVIDER_ICEBERG)
    {
        return Err(DataFusionError::Configuration(format!(
            "Unsupported format in USING: {provider}"
        )));
    }
    let mut stored = HashMap::new();
    if let Some(comment) = view_comment {
        stored.insert(VIEW_COMMENT_PROPERTY.to_string(), comment.to_string());
    }
    for (key, value) in tblproperties {
        if key == VIEW_PROVIDER_PROPERTY
            || key == VIEW_LOCATION_PROPERTY
            || key == VIEW_FORMAT_VERSION_PROPERTY
        {
            continue;
        }
        stored.insert(key.clone(), value.clone());
    }
    Ok((stored, tblproperties.get(VIEW_LOCATION_PROPERTY).cloned()))
}

#[allow(clippy::missing_errors_doc)]
pub fn view_location(
    warehouse: &str,
    namespace: &NamespaceIdent,
    view_name: &str,
) -> Result<String> {
    let root = warehouse.trim_end_matches('/');
    if root.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "cannot create view `{view_name}`: the catalog warehouse is empty"
        )));
    }
    Ok(format!("{root}/{}/{view_name}", namespace.join("/")))
}

#[allow(clippy::missing_errors_doc)]
pub async fn create_or_replace_view(
    target: &ViewTarget<'_>,
    or_replace: bool,
    if_not_exists: bool,
    definition: ViewDefinition,
) -> Result<Option<View>> {
    let ident = TableIdent::new(target.namespace.clone(), target.view_name.to_string());
    let view_exists = match target.catalog.view_exists(&ident).await {
        Ok(exists) => exists,
        Err(error) if error.kind() == ErrorKind::FeatureUnsupported => false,
        Err(error) if error.kind() == ErrorKind::NamespaceNotFound => {
            return Err(schema_not_found_on_drop(
                target.catalog_name,
                target.namespace_name,
            ));
        }
        Err(error) => return Err(iceberg_to_datafusion(error)),
    };
    let table_exists = match target.catalog.table_exists(&ident).await {
        Ok(exists) => exists,
        Err(error) if error.kind() == ErrorKind::NamespaceNotFound => {
            return Err(schema_not_found_on_drop(
                target.catalog_name,
                target.namespace_name,
            ));
        }
        Err(error) => return Err(iceberg_to_datafusion(error)),
    };
    if or_replace {
        if table_exists && !view_exists {
            return Err(view_already_exists(target.namespace_name, target.view_name));
        }
        if view_exists {
            return replace_view_version(target, &ident, definition)
                .await
                .map(Some);
        }
        return create_catalog_view(target, definition, or_replace)
            .await
            .map(Some);
    }
    if view_exists || table_exists {
        if if_not_exists {
            return Ok(None);
        }
        return Err(view_already_exists(target.namespace_name, target.view_name));
    }
    create_catalog_view(target, definition, or_replace)
        .await
        .map(Some)
}

async fn create_catalog_view(
    target: &ViewTarget<'_>,
    definition: ViewDefinition,
    or_replace: bool,
) -> Result<View> {
    let location = match definition.location_override {
        Some(location) => location,
        None => view_location(&definition.warehouse, target.namespace, target.view_name)?,
    };
    let creation = ViewCreation::builder()
        .name(target.view_name.to_string())
        .location(location)
        .representations(ViewRepresentations::new(vec![ViewRepresentation::Sql(
            SqlViewRepresentation {
                sql: definition.sql,
                dialect: VIEW_DIALECT.to_string(),
            },
        )]))
        .schema(definition.schema)
        .properties(definition.properties)
        .default_namespace(target.namespace.clone())
        .default_catalog(Some(definition.default_catalog))
        .build();
    match target.catalog.create_view(target.namespace, creation).await {
        Ok(view) => Ok(view),
        Err(error)
            if error.kind() == ErrorKind::ViewAlreadyExists
                || error.kind() == ErrorKind::TableAlreadyExists =>
        {
            Err(view_already_exists(target.namespace_name, target.view_name))
        }
        Err(error) if error.kind() == ErrorKind::NamespaceNotFound => Err(
            schema_not_found_on_drop(target.catalog_name, target.namespace_name),
        ),
        Err(error) if error.kind() == ErrorKind::FeatureUnsupported => {
            Err(view_unsupported(target.catalog_name, or_replace))
        }
        Err(error) => Err(iceberg_to_datafusion(error)),
    }
}

async fn replace_view_version(
    target: &ViewTarget<'_>,
    ident: &TableIdent,
    definition: ViewDefinition,
) -> Result<View> {
    let view = match target.catalog.load_view(ident).await {
        Ok(view) => view,
        Err(error) if error.kind() == ErrorKind::ViewNotFound => {
            return create_catalog_view(target, definition, true).await;
        }
        Err(error) => return Err(iceberg_to_datafusion(error)),
    };
    let commit = view
        .replace_version()
        .with_query(VIEW_DIALECT, definition.sql)
        .with_schema(definition.schema)
        .with_default_namespace(target.namespace.clone())
        .with_default_catalog(definition.default_catalog)
        .to_commit()
        .map_err(iceberg_to_datafusion)?;
    match target.catalog.update_view(commit).await {
        Ok(view) => Ok(view),
        Err(error) if error.kind() == ErrorKind::FeatureUnsupported => {
            Err(view_unsupported(target.catalog_name, true))
        }
        Err(error) => Err(iceberg_to_datafusion(error)),
    }
}

#[allow(clippy::missing_errors_doc)]
pub async fn drop_catalog_view(target: &ViewTarget<'_>, if_exists: bool) -> Result<()> {
    let ident = TableIdent::new(target.namespace.clone(), target.view_name.to_string());
    let view_exists = match target.catalog.view_exists(&ident).await {
        Ok(exists) => exists,
        Err(error)
            if error.kind() == ErrorKind::NamespaceNotFound
                || error.kind() == ErrorKind::FeatureUnsupported =>
        {
            false
        }
        Err(error) => return Err(iceberg_to_datafusion(error)),
    };
    if !view_exists {
        let table_exists = match target.catalog.table_exists(&ident).await {
            Ok(exists) => exists,
            Err(error)
                if error.kind() == ErrorKind::NamespaceNotFound
                    || error.kind() == ErrorKind::FeatureUnsupported =>
            {
                false
            }
            Err(error) => return Err(iceberg_to_datafusion(error)),
        };
        if if_exists && !table_exists {
            return Ok(());
        }
        return Err(view_not_found(target.namespace_name, target.view_name));
    }
    match target.catalog.drop_view(&ident).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::ViewNotFound && if_exists => Ok(()),
        Err(error) if error.kind() == ErrorKind::ViewNotFound => {
            Err(view_not_found(target.namespace_name, target.view_name))
        }
        Err(error) => Err(iceberg_to_datafusion(error)),
    }
}

#[allow(clippy::missing_errors_doc)]
pub async fn list_catalog_views(
    catalog_name: &str,
    catalog: &dyn Catalog,
    namespace: &NamespaceIdent,
    namespace_name: &str,
) -> Result<Vec<String>> {
    match catalog.list_views(namespace).await {
        Ok(views) => Ok(views
            .into_iter()
            .map(|ident| ident.name().to_string())
            .collect()),
        Err(error) if error.kind() == ErrorKind::FeatureUnsupported => Ok(Vec::new()),
        Err(error) if error.kind() == ErrorKind::NamespaceNotFound => {
            Err(schema_not_found_on_drop(catalog_name, namespace_name))
        }
        Err(error) => Err(iceberg_to_datafusion(error)),
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn view_read_spec(view: &View) -> Result<ViewReadSpec> {
    let version = view.metadata().current_version();
    let sql = version
        .representations()
        .iter()
        .map(|representation| match representation {
            ViewRepresentation::Sql(sql) => sql.sql.clone(),
        })
        .next()
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "view `{}` has a current version with no SQL representation",
                view.identifier().name()
            ))
        })?;
    let schema = view.metadata().current_schema();
    Ok(ViewReadSpec {
        sql,
        default_catalog: version.default_catalog().cloned(),
        default_namespace: version.default_namespace().clone(),
        column_names: schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect(),
    })
}

fn view_already_exists(namespace_name: &str, view_name: &str) -> DataFusionError {
    DataFusionError::Plan(spark_error::message(
        spark_error::VIEW_ALREADY_EXISTS,
        &[("relationName", &format!("{namespace_name}.{view_name}"))],
    ))
}

fn view_not_found(namespace_name: &str, view_name: &str) -> DataFusionError {
    DataFusionError::Plan(spark_error::message(
        spark_error::VIEW_NOT_FOUND,
        &[("relationName", &format!("{namespace_name}.{view_name}"))],
    ))
}

fn view_unsupported(catalog_name: &str, or_replace: bool) -> DataFusionError {
    let message = if or_replace {
        format!("Replacing a view is not supported by catalog: {catalog_name}")
    } else {
        format!("Creating a view is not supported by catalog: {catalog_name}")
    };
    unsupported_error(message)
}
