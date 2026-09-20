use crate::catalog_config::CatalogKind;

pub(crate) fn kind_from_catalog_impl(value: &str) -> Option<CatalogKind> {
    let value = value.trim();
    if value.ends_with("GlueCatalog") {
        Some(CatalogKind::Glue)
    } else if value.ends_with("S3TablesCatalog") {
        Some(CatalogKind::S3Tables)
    } else if value.ends_with("JDBCTableCatalog") {
        Some(CatalogKind::Postgres)
    } else if value.ends_with("InMemoryCatalog") {
        Some(CatalogKind::Memory)
    } else {
        None
    }
}

pub(crate) fn kind_from_type(value: &str) -> Option<CatalogKind> {
    match value.trim().to_ascii_lowercase().as_str() {
        "glue" => Some(CatalogKind::Glue),
        "s3tables" => Some(CatalogKind::S3Tables),
        "memory" | "hadoop" => Some(CatalogKind::Memory),
        "postgres" | "postgresql" | "jdbc" => Some(CatalogKind::Postgres),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hadoop_type_aliases_to_memory() {
        assert_eq!(kind_from_type("hadoop"), Some(CatalogKind::Memory));
        assert_eq!(kind_from_type(" Hadoop "), Some(CatalogKind::Memory));
        assert_eq!(kind_from_type("memory"), Some(CatalogKind::Memory));
        assert_eq!(kind_from_type("glue"), Some(CatalogKind::Glue));
        assert_eq!(kind_from_type("s3tables"), Some(CatalogKind::S3Tables));
        assert_eq!(kind_from_type("postgres"), Some(CatalogKind::Postgres));
        assert_eq!(kind_from_type("nope"), None);
    }

    #[test]
    fn inmemory_catalog_impl_aliases_to_memory() {
        assert_eq!(
            kind_from_catalog_impl("org.apache.iceberg.memory.InMemoryCatalog"),
            Some(CatalogKind::Memory)
        );
        assert_eq!(
            kind_from_catalog_impl("org.apache.iceberg.aws.glue.GlueCatalog"),
            Some(CatalogKind::Glue)
        );
        assert_eq!(
            kind_from_catalog_impl("software.amazon.s3tables.iceberg.S3TablesCatalog"),
            Some(CatalogKind::S3Tables)
        );
        assert_eq!(
            kind_from_catalog_impl("org.apache.iceberg.jdbc.JDBCTableCatalog"),
            Some(CatalogKind::Postgres)
        );
        assert_eq!(kind_from_catalog_impl("com.example.Other"), None);
    }
}
