use datafusion::error::{DataFusionError, Result};

use crate::cardinality::resolve_create_format_version;

pub const MAX_SUPPORTED_FORMAT_VERSION: u8 = 3;

#[allow(clippy::missing_errors_doc)]
pub fn resolve_alter_format_version(
    requested: &str,
    current: u8,
    allow_v3: bool,
    property_name: &str,
    form: &str,
) -> Result<Option<u8>> {
    let raw = requested.trim();
    let Ok(target) = raw.parse::<u8>() else {
        return Err(DataFusionError::NotImplemented(format!(
            "{form} '{property_name}' = '{raw}' is not an Iceberg format version — a v{current} \
             table upgrades only to '{MAX_SUPPORTED_FORMAT_VERSION}'"
        )));
    };
    if target > MAX_SUPPORTED_FORMAT_VERSION {
        return Err(DataFusionError::NotImplemented(format!(
            "{form} '{property_name}' = '{target}' is not supported — this engine writes Iceberg \
             format v1 through v{MAX_SUPPORTED_FORMAT_VERSION}, so a v{current} table upgrades \
             only to '{MAX_SUPPORTED_FORMAT_VERSION}'"
        )));
    }
    if target < current {
        return Err(DataFusionError::NotImplemented(format!(
            "{form} '{property_name}' = '{target}' cannot downgrade a v{current} table to \
             v{target} — an Iceberg format version only moves up"
        )));
    }
    if target == current {
        return Ok(None);
    }
    if target == MAX_SUPPORTED_FORMAT_VERSION {
        resolve_create_format_version(Some("3"), allow_v3, property_name, form)?;
    }
    Ok(Some(target))
}

#[cfg(test)]
mod tests {
    use super::{MAX_SUPPORTED_FORMAT_VERSION, resolve_alter_format_version};
    use crate::cardinality::ALLOW_CREATE_FORMAT_VERSION_3_KEY;

    fn resolve(requested: &str, current: u8, allow_v3: bool) -> Result<Option<u8>, String> {
        resolve_alter_format_version(
            requested,
            current,
            allow_v3,
            "format-version",
            "TBLPROPERTIES",
        )
        .map_err(|err| err.to_string())
    }

    #[test]
    fn upgrade_v2_to_v3_needs_the_opt_in() {
        assert_eq!(resolve("3", 2, true), Ok(Some(3)));
        let err = resolve("3", 2, false).unwrap_err();
        assert!(
            err.contains(ALLOW_CREATE_FORMAT_VERSION_3_KEY) && err.contains("format-version"),
            "the opt-in refusal must name the conf and the key: {err}"
        );
    }

    #[test]
    fn same_version_is_a_no_op_and_needs_no_opt_in() {
        assert_eq!(resolve("3", 3, false), Ok(None));
        assert_eq!(resolve("2", 2, false), Ok(None));
    }

    #[test]
    fn downgrade_names_both_versions() {
        let err = resolve("2", 3, true).unwrap_err();
        assert!(
            err.contains("v3") && err.contains("v2") && err.contains("format-version"),
            "downgrade refusal must name the key and both versions: {err}"
        );
        let err = resolve("1", 2, true).unwrap_err();
        assert!(err.contains("v2") && err.contains("v1"), "{err}");
    }

    #[test]
    fn above_the_supported_ceiling_and_unparsable_values_refuse() {
        let err = resolve("4", 2, true).unwrap_err();
        assert!(
            err.contains('4') && err.contains("v2") && err.contains("format-version"),
            "the v4 refusal must name the key, the value and the current version: {err}"
        );
        for raw in ["x", "", "3.0", "-1"] {
            let err = resolve(raw, 2, true).unwrap_err();
            assert!(
                err.contains("format-version") && err.contains("not an Iceberg format version"),
                "`{raw}` must refuse as unparsable: {err}"
            );
        }
        assert_eq!(MAX_SUPPORTED_FORMAT_VERSION, 3);
    }

    #[test]
    fn v1_upgrades_to_v2_without_the_opt_in() {
        assert_eq!(resolve("2", 1, false), Ok(Some(2)));
    }
}
