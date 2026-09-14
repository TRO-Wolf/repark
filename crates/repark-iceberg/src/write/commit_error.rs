use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};

use datafusion::error::DataFusionError;
use iceberg::ErrorKind;
use iceberg::table::Table;
use uuid::Uuid;

use crate::write::merge::OPERATION_ID_PROP;

#[derive(Debug)]
pub struct CommitStateUnknownError {
    inner: iceberg::Error,
    operation_id: String,
}

impl CommitStateUnknownError {
    pub fn inner(&self) -> &iceberg::Error {
        &self.inner
    }

    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }
}

impl Display for CommitStateUnknownError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.inner, formatter)
    }
}

impl StdError for CommitStateUnknownError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.inner)
    }
}

pub fn commit_err(err: iceberg::Error, operation_id: &str) -> DataFusionError {
    if err.kind() == ErrorKind::CommitStateUnknown {
        DataFusionError::External(Box::new(CommitStateUnknownError {
            inner: err,
            operation_id: operation_id.to_string(),
        }))
    } else {
        DataFusionError::External(Box::new(err))
    }
}

pub(crate) fn commit_result(
    result: iceberg::Result<Table>,
    operation_id: &str,
) -> Result<Table, DataFusionError> {
    result.map_err(|error| commit_err(error, operation_id))
}

pub(crate) fn operation_id_and_summary() -> (String, HashMap<String, String>) {
    let operation_id = Uuid::new_v4().to_string();
    let summary = HashMap::from([(OPERATION_ID_PROP.to_string(), operation_id.clone())]);
    (operation_id, summary)
}
