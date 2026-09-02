use datafusion::error::{DataFusionError, Result};

use crate::cardinality::ALLOW_CREATE_FORMAT_VERSION_3_KEY;

pub const MAX_SUPPORTED_FORMAT_VERSION: i64 = 3;

#[allow(clippy::missing_errors_doc)]
pub fn resolve_alter_format_version(
    requested: &str,
    current: i64,
    allow_v3: bool,
    property_name: &str,
    form: &str,
) -> Result<Option<i64>> {
    let Ok(target) = requested.parse::<i64>() else {
        return Err(DataFusionError::NotImplemented(format!(
            "{form} '{property_name}' = '{requested}' is not an Iceberg format version — a \
             v{current} table upgrades only to '{MAX_SUPPORTED_FORMAT_VERSION}'"
        )));
    };
    if target < current {
        return Err(DataFusionError::NotImplemented(format!(
            "{form} '{property_name}' = '{target}' cannot downgrade a v{current} table to \
             v{target} — an Iceberg format version only moves up"
        )));
    }
    if target > MAX_SUPPORTED_FORMAT_VERSION {
        return Err(DataFusionError::NotImplemented(format!(
            "{form} '{property_name}' = '{target}' is not supported — this engine writes Iceberg \
             format v1 through v{MAX_SUPPORTED_FORMAT_VERSION}, so a v{current} table upgrades \
             only to '{MAX_SUPPORTED_FORMAT_VERSION}'"
        )));
    }
    if target == current {
        return Ok(None);
    }
    if target == MAX_SUPPORTED_FORMAT_VERSION && !allow_v3 {
        return Err(DataFusionError::NotImplemented(format!(
            "{form} '{property_name}' = '3' is not enabled — set \
             `{ALLOW_CREATE_FORMAT_VERSION_3_KEY}` = true (a v{current} table stays v{current} \
             until the opt-in is on)"
        )));
    }
    Ok(Some(target))
}

#[cfg(test)]
mod tests {
    use super::{MAX_SUPPORTED_FORMAT_VERSION, resolve_alter_format_version};
    use crate::cardinality::ALLOW_CREATE_FORMAT_VERSION_3_KEY;

    fn resolve(requested: &str, current: i64, allow_v3: bool) -> Result<Option<i64>, String> {
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
        assert_eq!(resolve("+3", 2, true), Ok(Some(3)));
        let err = resolve("3", 2, false).unwrap_err();
        assert!(
            err.contains(ALLOW_CREATE_FORMAT_VERSION_3_KEY) && err.contains("format-version"),
            "the opt-in refusal must name the conf and the key: {err}"
        );
        assert!(
            !err.contains("create"),
            "the ALTER refusal must not carry the CREATE door's phrasing: {err}"
        );
    }

    #[test]
    fn same_version_is_a_no_op_and_needs_no_opt_in() {
        assert_eq!(resolve("3", 3, false), Ok(None));
        assert_eq!(resolve("2", 2, false), Ok(None));
    }

    #[test]
    fn every_version_below_the_current_one_is_a_downgrade() {
        for (requested, current) in [("2", 3), ("1", 2), ("0", 3), ("-1", 2)] {
            let err = resolve(requested, current, true).unwrap_err();
            assert!(
                err.contains("format-version")
                    && err.contains(&format!("v{current}"))
                    && err.contains(&format!("v{requested}")),
                "`{requested}` on v{current} must refuse as a downgrade naming both: {err}"
            );
        }
    }

    #[test]
    fn above_the_supported_ceiling_and_unparsable_values_refuse() {
        let err = resolve("4", 2, true).unwrap_err();
        assert!(
            err.contains('4') && err.contains("v2") && err.contains("format-version"),
            "the v4 refusal must name the key, the value and the current version: {err}"
        );
        for raw in ["x", "", "3.0", " 3 "] {
            let err = resolve(raw, 2, true).unwrap_err();
            assert!(
                err.contains("format-version") && err.contains("not an Iceberg format version"),
                "`{raw}` must refuse as unparsable: {err}"
            );
        }
        assert_eq!(MAX_SUPPORTED_FORMAT_VERSION, 3);
    }

    #[test]
    fn v1_upgrades_to_v3_behind_the_same_opt_in() {
        assert_eq!(resolve("2", 1, false), Ok(Some(2)));
        assert_eq!(resolve("3", 1, true), Ok(Some(3)));
        assert!(resolve("3", 1, false).is_err());
    }
}
