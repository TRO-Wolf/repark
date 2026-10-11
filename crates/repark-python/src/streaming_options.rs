use std::collections::BTreeMap;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use repark_core::microbatch::MicroBatchError;

use crate::exceptions::IllegalArgumentException;
use crate::streaming::{CATALOG_TIMEOUT_KEY, DoorKind, SINK_KEY};
use crate::streaming_errors::microbatch_py_err;

const CHECKPOINT_KEY: &str = "checkpointLocation";
pub(crate) const FANOUT_KEY: &str = "fanout-enabled";
const STREAMING_PREFIX: &str = "streaming-";
const STREAM_PREFIX: &str = "stream-";
const REPARK_CDC_PREFIX: &str = "repark.cdc.";
pub(crate) const CONF_CHECKPOINT: &str = "spark.sql.streaming.checkpointLocation";
pub(crate) const EMPTY_CHECKPOINT_TEXT: &str = "Can not create a Path from an empty string";

#[pyfunction]
#[allow(clippy::missing_errors_doc)]
pub fn store_stream_option(
    options: &Bound<'_, PyDict>,
    key: &str,
    value: Option<String>,
) -> PyResult<()> {
    let folded = key.to_ascii_lowercase();
    let mut stale = Vec::new();
    for existing in options.keys() {
        let text = existing.extract::<String>()?;
        if text.to_ascii_lowercase() == folded {
            stale.push(text);
        }
    }
    for text in stale {
        options.del_item(text)?;
    }
    options.set_item(key, value)?;
    Ok(())
}

#[pyfunction]
#[allow(clippy::missing_errors_doc)]
pub fn stream_option_path(options: &Bound<'_, PyDict>) -> PyResult<Option<String>> {
    for entry in options.iter() {
        let (key, value) = entry;
        if key.extract::<String>()?.eq_ignore_ascii_case("path") {
            let found: Option<String> = value.extract()?;
            if let Some(path) = found
                && !path.is_empty()
            {
                return Ok(Some(path));
            }
        }
    }
    Ok(None)
}

fn has_interpreted_prefix(folded: &str) -> bool {
    folded.starts_with(STREAMING_PREFIX)
        || folded.starts_with(STREAM_PREFIX)
        || folded.starts_with(REPARK_CDC_PREFIX)
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn validate_writer_options(
    py: Python<'_>,
    options: &BTreeMap<String, String>,
) -> Result<(), PyErr> {
    for key in options.keys() {
        let known = key.eq_ignore_ascii_case(CHECKPOINT_KEY)
            || key.eq_ignore_ascii_case(SINK_KEY)
            || key.eq_ignore_ascii_case(FANOUT_KEY)
            || key.eq_ignore_ascii_case(CATALOG_TIMEOUT_KEY);
        if !known && has_interpreted_prefix(&key.to_ascii_lowercase()) {
            return Err(microbatch_py_err(
                py,
                &MicroBatchError::UnknownOption { key: key.clone() },
                None,
            ));
        }
    }
    Ok(())
}

pub(crate) fn writer_option<'a>(
    options: &'a BTreeMap<String, String>,
    key: &str,
) -> Option<&'a str> {
    options
        .iter()
        .find_map(|(known, value)| known.eq_ignore_ascii_case(key).then_some(value.as_str()))
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn check_checkpoint(
    py: Python<'_>,
    options: &BTreeMap<String, String>,
    streaming_confs: &BTreeMap<String, String>,
    door: DoorKind,
) -> Result<Option<String>, PyErr> {
    let value = writer_option(options, CHECKPOINT_KEY)
        .or_else(|| writer_option(streaming_confs, CONF_CHECKPOINT))
        .map(str::to_string);
    match value {
        Some(location) if location.is_empty() => {
            Err(IllegalArgumentException::new_err(EMPTY_CHECKPOINT_TEXT))
        }
        Some(location) => Ok(Some(location)),
        None => match door {
            DoorKind::Table => Err(microbatch_py_err(
                py,
                &MicroBatchError::CheckpointLocationMissing,
                None,
            )),
            #[allow(clippy::match_same_arms)]
            DoorKind::ForeachBatch => Err(microbatch_py_err(
                py,
                &MicroBatchError::CheckpointLocationMissing,
                None,
            )),
        },
    }
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn check_sink_declared<'a>(
    py: Python<'_>,
    options: &'a BTreeMap<String, String>,
) -> Result<&'a str, PyErr> {
    match writer_option(options, SINK_KEY) {
        Some(sink) => Ok(sink),
        None => Err(microbatch_py_err(
            py,
            &MicroBatchError::SinkUndeclared,
            None,
        )),
    }
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;
    use pyo3::types::PyDict;

    use super::*;

    fn options<'py>(py: Python<'py>, pairs: &[(&str, Option<&str>)]) -> Bound<'py, PyDict> {
        let dict = PyDict::new(py);
        for (key, value) in pairs {
            dict.set_item(key, value).expect("a fixture pair stores");
        }
        dict
    }

    fn stored(dict: &Bound<'_, PyDict>, key: &str) -> Option<String> {
        dict.get_item(key)
            .expect("a lookup passes")
            .map(|found| found.extract::<String>().expect("a stored str"))
    }

    #[test]
    fn store_replaces_a_case_variant_key_and_keeps_the_rest() {
        Python::attach(|py| {
            let dict = options(py, &[("Max-Rows", Some("1")), ("other", Some("x"))]);
            store_stream_option(&dict, "max-rows", Some(String::from("2")))
                .expect("a store passes");
            assert_eq!(dict.len(), 2);
            assert_eq!(stored(&dict, "max-rows"), Some(String::from("2")));
            assert_eq!(stored(&dict, "other"), Some(String::from("x")));
            assert!(
                dict.get_item("Max-Rows")
                    .expect("a lookup passes")
                    .is_none()
            );
        });
    }

    #[test]
    fn store_keeps_a_none_value_for_the_door_call_to_drop() {
        Python::attach(|py| {
            let dict = options(py, &[]);
            store_stream_option(&dict, "nothing", None).expect("a store passes");
            assert_eq!(dict.len(), 1);
            assert!(
                dict.get_item("nothing")
                    .expect("a lookup passes")
                    .expect("a none stores")
                    .is_none()
            );
        });
    }

    #[test]
    fn path_finds_the_folded_key_and_skips_empty_values() {
        Python::attach(|py| {
            let dict = options(py, &[("PATH", Some("sc.db.src"))]);
            assert_eq!(
                stream_option_path(&dict).expect("a lookup passes"),
                Some(String::from("sc.db.src"))
            );
            let dict = options(py, &[("Path", Some("")), ("other", Some("x"))]);
            assert_eq!(stream_option_path(&dict).expect("a lookup passes"), None);
            let dict = options(py, &[("nothing", None)]);
            assert_eq!(stream_option_path(&dict).expect("a lookup passes"), None);
        });
    }
}
