#[derive(Clone, Debug)]
pub struct FileMetadataError {
    pub message: String,
    pub error_class: Option<&'static str>,
    pub message_parameters: Vec<(String, String)>,
    pub sql_state: Option<&'static str>,
}

impl FileMetadataError {
    pub(crate) fn unresolved(name: &str) -> Self {
        Self {
            message: format!(
                "[UNRESOLVED_COLUMN.WITHOUT_SUGGESTION] A column, variable, or function \
                 parameter with name `{name}` cannot be resolved.  SQLSTATE: 42703"
            ),
            error_class: Some("UNRESOLVED_COLUMN.WITHOUT_SUGGESTION"),
            message_parameters: vec![("objectName".to_string(), format!("`{name}`"))],
            sql_state: Some("42703"),
        }
    }

    pub(crate) fn missing(hidden: &str, available: &[String], requested: &str) -> Self {
        let listed = available
            .iter()
            .map(|name| format!("\"{name}\""))
            .collect::<Vec<_>>()
            .join(", ");
        Self {
            message: format!(
                "[MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT] Resolved \
                 attribute(s) \"{hidden}\" missing from {listed} in operator !Project \
                 [{requested}].  SQLSTATE: XX000;"
            ),
            error_class: Some("MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT"),
            message_parameters: vec![],
            sql_state: Some("XX000"),
        }
    }

    pub(crate) fn engine(message: String) -> Self {
        Self {
            message,
            error_class: None,
            message_parameters: vec![],
            sql_state: None,
        }
    }
}
