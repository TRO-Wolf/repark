use crate::write::writer_partitioning::{
    SaveTarget, WriterLayout, already_exists_error, decide_save_target,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterAction {
    SaveAsTable,
    Save,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterStatement {
    Ctas,
    Rtas,
    Append,
    Overwrite,
    Skip,
}

impl WriterStatement {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ctas => "ctas",
            Self::Rtas => "rtas",
            Self::Append => "append",
            Self::Overwrite => "overwrite",
            Self::Skip => "skip",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterPlan {
    pub statement: WriterStatement,
    pub check_layout: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct WriterRequest<'a> {
    pub action: WriterAction,
    pub target: &'a str,
    pub relation_parts: &'a [String],
    pub exists: bool,
    pub mode: &'a str,
    pub explicit_format: bool,
    pub layout: &'a WriterLayout,
    pub frame_columns: &'a [String],
    pub case_sensitive: bool,
}

#[derive(Debug)]
pub enum WriterRefusal {
    MissingBucketColumn(String),
    Spark(repark_common::Error),
}

impl From<repark_common::Error> for WriterRefusal {
    fn from(error: repark_common::Error) -> Self {
        Self::Spark(error)
    }
}

#[must_use]
pub fn missing_column_name(column: &str) -> String {
    if column.contains('.') {
        format!("`{column}`")
    } else {
        column.to_string()
    }
}

#[must_use]
pub fn missing_column_message(column: &str, schema_tree: &str) -> String {
    let name = missing_column_name(column);
    format!("Couldn't find column {name} in:\n{schema_tree}")
}

fn missing_bucket_column(request: &WriterRequest<'_>) -> Option<String> {
    let found = |column: &str| {
        request.frame_columns.iter().any(|name| {
            if request.case_sensitive {
                name == column
            } else {
                name.to_lowercase() == column.to_lowercase()
            }
        })
    };
    request.layout.num_buckets?;
    request
        .layout
        .bucket_columns
        .iter()
        .chain(&request.layout.sort_columns)
        .find(|column| !found(column))
        .cloned()
}

fn plan_save(request: &WriterRequest<'_>) -> Result<WriterPlan, WriterRefusal> {
    let target = decide_save_target(
        request.target,
        request.relation_parts,
        request.exists,
        request.mode,
        request.explicit_format,
    )?;
    let (statement, check_layout) = match target {
        SaveTarget::Create => (WriterStatement::Ctas, false),
        SaveTarget::Append => (WriterStatement::Append, true),
        SaveTarget::Overwrite => (WriterStatement::Overwrite, true),
        SaveTarget::Skip => (WriterStatement::Skip, false),
    };
    Ok(WriterPlan {
        statement,
        check_layout,
    })
}

fn plan_save_as_table(request: &WriterRequest<'_>) -> Result<WriterPlan, WriterRefusal> {
    let mode = match request.mode {
        "errorifexists" => "error",
        other => other,
    };
    if mode == "append" && request.exists {
        return Ok(WriterPlan {
            statement: WriterStatement::Append,
            check_layout: true,
        });
    }
    if let Some(column) = missing_bucket_column(request) {
        return Err(WriterRefusal::MissingBucketColumn(column));
    }
    let statement = match (mode, request.exists) {
        ("overwrite", _) => WriterStatement::Rtas,
        (_, false) => WriterStatement::Ctas,
        ("ignore", true) => WriterStatement::Skip,
        _ => return Err(already_exists_error(request.relation_parts).into()),
    };
    Ok(WriterPlan {
        statement,
        check_layout: false,
    })
}

#[allow(clippy::missing_errors_doc)]
pub fn plan_writer(request: &WriterRequest<'_>) -> Result<WriterPlan, WriterRefusal> {
    match request.action {
        WriterAction::Save => plan_save(request),
        WriterAction::SaveAsTable => plan_save_as_table(request),
    }
}
