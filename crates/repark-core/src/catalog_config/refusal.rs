use std::collections::HashMap;
use std::hash::BuildHasher;

use repark_common::Error;

use super::Block;
use super::CatalogKind;
use super::CatalogSpec;

pub(crate) const CATALOG_EXTENSIONS_KEY: &str = "repark.sql.catalogExtensions";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RefusalClass {
    UnsupportedOperation,
    IllegalArgument,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogRefusal {
    message: String,
    class: RefusalClass,
}

impl CatalogRefusal {
    #[must_use]
    pub fn error(&self) -> Error {
        match self.class {
            RefusalClass::UnsupportedOperation => Error::NotImplemented(self.message.clone()),
            RefusalClass::IllegalArgument => Error::IllegalArgument(self.message.clone()),
        }
    }
}

#[must_use]
pub(crate) fn catalog_extensions_enabled<S: BuildHasher>(
    config: &HashMap<String, String, S>,
) -> bool {
    config
        .iter()
        .any(|(key, value)| key.eq_ignore_ascii_case(CATALOG_EXTENSIONS_KEY) && value == "true")
}

fn is_memory_type(value: &str) -> bool {
    value.eq_ignore_ascii_case("memory")
}

impl Block {
    pub(crate) fn refusal_for(&self, name: &str, extensions: bool) -> Option<CatalogRefusal> {
        match (&self.type_value, &self.impl_value) {
            (Some(type_value), Some(impl_value)) => {
                if extensions {
                    None
                } else {
                    Some(CatalogRefusal {
                        message: format!(
                            "Cannot create catalog {name}, both type and catalog-impl are set: \
                             type={type_value}, catalog-impl={impl_value}"
                        ),
                        class: RefusalClass::IllegalArgument,
                    })
                }
            }
            (Some(type_value), None) if is_memory_type(type_value) => {
                if extensions {
                    None
                } else {
                    Some(CatalogRefusal {
                        message: format!("Unknown catalog type: {type_value}"),
                        class: RefusalClass::UnsupportedOperation,
                    })
                }
            }
            _ => None,
        }
    }

    pub(crate) fn refused_spec(self, name: String, refusal: CatalogRefusal) -> CatalogSpec {
        CatalogSpec {
            name,
            kind: CatalogKind::Refused,
            props: self.props,
            refusal: Some(refusal),
        }
    }
}
