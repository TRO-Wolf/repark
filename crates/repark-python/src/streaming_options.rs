use pyo3::prelude::*;
use pyo3::types::PyDict;

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
