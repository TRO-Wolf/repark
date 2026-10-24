use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, Insert, Merge, MergeAction, ObjectName, ObjectNamePart, Statement,
    TableFactor,
};

use crate::write_options::StatementWriteOptions;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WriteDoor {
    D1Insert,
    D2InsertOptions,
    D3InsertByName,
    D4InsertOverwrite,
    D5ReplaceWhere,
    D6Ctas,
    D7MergeInsert,
    D8MergeUpdateStruct,
    D9MergeUpdateField,
    D10Update,
    D14NativeInsert,
    D15NativeInsertConf,
    D16NativeOverwritePartition,
    D17NativeCtas,
    D18NativeAppendApi,
    D19NativeMerge,
    D20NativePredicateDml,
    D21MicroBatchSink,
}

impl WriteDoor {
    pub const ALL: [WriteDoor; 18] = [
        WriteDoor::D1Insert,
        WriteDoor::D2InsertOptions,
        WriteDoor::D3InsertByName,
        WriteDoor::D4InsertOverwrite,
        WriteDoor::D5ReplaceWhere,
        WriteDoor::D6Ctas,
        WriteDoor::D7MergeInsert,
        WriteDoor::D8MergeUpdateStruct,
        WriteDoor::D9MergeUpdateField,
        WriteDoor::D10Update,
        WriteDoor::D14NativeInsert,
        WriteDoor::D15NativeInsertConf,
        WriteDoor::D16NativeOverwritePartition,
        WriteDoor::D17NativeCtas,
        WriteDoor::D18NativeAppendApi,
        WriteDoor::D19NativeMerge,
        WriteDoor::D20NativePredicateDml,
        WriteDoor::D21MicroBatchSink,
    ];

    #[must_use]
    pub fn id(self) -> &'static str {
        match self {
            WriteDoor::D1Insert => "D1",
            WriteDoor::D2InsertOptions => "D2",
            WriteDoor::D3InsertByName => "D3",
            WriteDoor::D4InsertOverwrite => "D4",
            WriteDoor::D5ReplaceWhere => "D5",
            WriteDoor::D6Ctas => "D6",
            WriteDoor::D7MergeInsert => "D7",
            WriteDoor::D8MergeUpdateStruct => "D8",
            WriteDoor::D9MergeUpdateField => "D9",
            WriteDoor::D10Update => "D10",
            WriteDoor::D14NativeInsert => "D14",
            WriteDoor::D15NativeInsertConf => "D15",
            WriteDoor::D16NativeOverwritePartition => "D16",
            WriteDoor::D17NativeCtas => "D17",
            WriteDoor::D18NativeAppendApi => "D18",
            WriteDoor::D19NativeMerge => "D19",
            WriteDoor::D20NativePredicateDml => "D20",
            WriteDoor::D21MicroBatchSink => "D21",
        }
    }

    #[must_use]
    pub fn is_spark_door(self) -> bool {
        match self {
            WriteDoor::D1Insert
            | WriteDoor::D2InsertOptions
            | WriteDoor::D3InsertByName
            | WriteDoor::D4InsertOverwrite
            | WriteDoor::D5ReplaceWhere
            | WriteDoor::D6Ctas
            | WriteDoor::D7MergeInsert
            | WriteDoor::D8MergeUpdateStruct
            | WriteDoor::D9MergeUpdateField
            | WriteDoor::D10Update => true,
            WriteDoor::D14NativeInsert
            | WriteDoor::D15NativeInsertConf
            | WriteDoor::D16NativeOverwritePartition
            | WriteDoor::D17NativeCtas
            | WriteDoor::D18NativeAppendApi
            | WriteDoor::D19NativeMerge
            | WriteDoor::D20NativePredicateDml
            | WriteDoor::D21MicroBatchSink => false,
        }
    }
}

#[must_use]
#[allow(clippy::too_many_lines)]
pub fn door_of(statement: &Statement, options: &StatementWriteOptions) -> Option<WriteDoor> {
    match statement {
        Statement::Insert(insert) => Some(insert_door(insert, options)),
        Statement::CreateTable(create) => create.query.as_ref().map(|_| WriteDoor::D6Ctas),
        Statement::Merge(merge) => merge_door(merge),
        Statement::Update { .. } => Some(WriteDoor::D10Update),
        Statement::Analyze { .. }
        | Statement::Set { .. }
        | Statement::Truncate { .. }
        | Statement::Msck { .. }
        | Statement::Query { .. }
        | Statement::Install { .. }
        | Statement::Load { .. }
        | Statement::Directory { .. }
        | Statement::Case { .. }
        | Statement::If { .. }
        | Statement::While { .. }
        | Statement::Raise { .. }
        | Statement::Call { .. }
        | Statement::Copy { .. }
        | Statement::CopyIntoSnowflake { .. }
        | Statement::Open { .. }
        | Statement::Close { .. }
        | Statement::Delete { .. }
        | Statement::CreateView { .. }
        | Statement::CreateVirtualTable { .. }
        | Statement::CreateIndex { .. }
        | Statement::CreateRole { .. }
        | Statement::CreateSecret { .. }
        | Statement::CreateServer { .. }
        | Statement::CreatePolicy { .. }
        | Statement::CreateConnector { .. }
        | Statement::CreateOperator { .. }
        | Statement::CreateOperatorFamily { .. }
        | Statement::CreateOperatorClass { .. }
        | Statement::AlterTable { .. }
        | Statement::AlterSchema { .. }
        | Statement::AlterIndex { .. }
        | Statement::AlterView { .. }
        | Statement::AlterFunction { .. }
        | Statement::AlterType { .. }
        | Statement::AlterCollation { .. }
        | Statement::AlterOperator { .. }
        | Statement::AlterOperatorFamily { .. }
        | Statement::AlterOperatorClass { .. }
        | Statement::AlterRole { .. }
        | Statement::AlterPolicy { .. }
        | Statement::AlterConnector { .. }
        | Statement::AlterSession { .. }
        | Statement::AttachDatabase { .. }
        | Statement::AttachDuckDBDatabase { .. }
        | Statement::DetachDuckDBDatabase { .. }
        | Statement::Drop { .. }
        | Statement::DropFunction { .. }
        | Statement::DropDomain { .. }
        | Statement::DropProcedure { .. }
        | Statement::DropSecret { .. }
        | Statement::DropPolicy { .. }
        | Statement::DropConnector { .. }
        | Statement::Declare { .. }
        | Statement::CreateExtension { .. }
        | Statement::CreateCollation { .. }
        | Statement::DropExtension { .. }
        | Statement::DropOperator { .. }
        | Statement::DropOperatorFamily { .. }
        | Statement::DropOperatorClass { .. }
        | Statement::Fetch { .. }
        | Statement::Flush { .. }
        | Statement::Discard { .. }
        | Statement::ShowFunctions { .. }
        | Statement::ShowVariable { .. }
        | Statement::ShowStatus { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowCreate { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowCatalogs { .. }
        | Statement::ShowDatabases { .. }
        | Statement::ShowProcessList { .. }
        | Statement::ShowSchemas { .. }
        | Statement::ShowCharset { .. }
        | Statement::ShowObjects { .. }
        | Statement::ShowTables { .. }
        | Statement::ShowViews { .. }
        | Statement::ShowCollation { .. }
        | Statement::Use { .. }
        | Statement::StartTransaction { .. }
        | Statement::Comment { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. }
        | Statement::CreateSchema { .. }
        | Statement::CreateDatabase { .. }
        | Statement::CreateFunction { .. }
        | Statement::CreateTrigger { .. }
        | Statement::DropTrigger { .. }
        | Statement::CreateProcedure { .. }
        | Statement::CreateMacro { .. }
        | Statement::CreateStage { .. }
        | Statement::Assert { .. }
        | Statement::Grant { .. }
        | Statement::Deny { .. }
        | Statement::Revoke { .. }
        | Statement::Deallocate { .. }
        | Statement::Execute { .. }
        | Statement::Prepare { .. }
        | Statement::Kill { .. }
        | Statement::ExplainTable { .. }
        | Statement::Explain { .. }
        | Statement::Savepoint { .. }
        | Statement::ReleaseSavepoint { .. }
        | Statement::Cache { .. }
        | Statement::UNCache { .. }
        | Statement::CreateSequence { .. }
        | Statement::CreateDomain { .. }
        | Statement::CreateType { .. }
        | Statement::Pragma { .. }
        | Statement::Lock { .. }
        | Statement::LockTables { .. }
        | Statement::UnlockTables
        | Statement::Unload { .. }
        | Statement::OptimizeTable { .. }
        | Statement::LISTEN { .. }
        | Statement::UNLISTEN { .. }
        | Statement::NOTIFY { .. }
        | Statement::LoadData { .. }
        | Statement::RenameTable { .. }
        | Statement::List { .. }
        | Statement::Remove { .. }
        | Statement::RaisError { .. }
        | Statement::Throw { .. }
        | Statement::Print { .. }
        | Statement::WaitFor { .. }
        | Statement::Return { .. }
        | Statement::ExportData { .. }
        | Statement::CreateUser { .. }
        | Statement::AlterUser { .. }
        | Statement::Vacuum { .. }
        | Statement::Reset { .. } => None,
    }
}

pub(crate) fn tag(door: Option<WriteDoor>) {
    tracing::debug!(
        target: "repark_spark::write_door",
        door = door.map_or("none", WriteDoor::id)
    );
}

fn insert_door(insert: &Insert, options: &StatementWriteOptions) -> WriteDoor {
    if insert.overwrite {
        WriteDoor::D4InsertOverwrite
    } else if options.is_empty() {
        WriteDoor::D1Insert
    } else {
        WriteDoor::D2InsertOptions
    }
}

fn merge_door(merge: &Merge) -> Option<WriteDoor> {
    let qualifiers = target_qualifiers(&merge.table);
    merge
        .clauses
        .iter()
        .find_map(|clause| match &clause.action {
            MergeAction::Insert(_) => Some(WriteDoor::D7MergeInsert),
            MergeAction::Update(update) => {
                let field = update
                    .assignments
                    .iter()
                    .any(|assignment| match &assignment.target {
                        AssignmentTarget::ColumnName(name) => names_a_field(name, &qualifiers),
                        AssignmentTarget::Tuple(names) => {
                            names.iter().any(|name| names_a_field(name, &qualifiers))
                        }
                    });
                Some(if field {
                    WriteDoor::D9MergeUpdateField
                } else {
                    WriteDoor::D8MergeUpdateStruct
                })
            }
            MergeAction::Delete { .. } => None,
        })
}

fn target_qualifiers(table: &TableFactor) -> Vec<String> {
    match table {
        TableFactor::Table { name, alias, .. } => alias
            .iter()
            .map(|alias| alias.name.value.clone())
            .chain(name.0.last().map(part_text))
            .collect(),
        _ => Vec::new(),
    }
}

fn names_a_field(name: &ObjectName, qualifiers: &[String]) -> bool {
    let parts: Vec<String> = name.0.iter().map(part_text).collect();
    let column_parts = match parts.split_first() {
        Some((head, rest))
            if !rest.is_empty()
                && qualifiers
                    .iter()
                    .any(|qualifier| qualifier.eq_ignore_ascii_case(head)) =>
        {
            rest.len()
        }
        _ => parts.len(),
    };
    column_parts > 1
}

fn part_text(part: &ObjectNamePart) -> String {
    part.as_ident()
        .map_or_else(|| part.to_string(), |ident| ident.value.clone())
}
