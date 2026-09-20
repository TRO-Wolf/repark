use super::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Condition {
    TableOrViewNotFound,
    TableOrViewAlreadyExists,
    NotSupportedCommandForV2Table,
    UnresolvedColumnWithSuggestion,
    UnsupportedFeatureTableOperation,
    InvalidPartitionOperationPartitionManagementIsUnsupported,
    ParseSyntaxError,
    IncompatibleDataForTableCannotSafelyCast,
    InsertColumnArityMismatchNotEnoughDataColumns,
    NotNullAssertViolation,
    NotSupportedChangeColumn,
    FailedToLoadRoutine,
    CastInvalidInput,
    RequiresSinglePartNamespace,
    UnsupportedFeatureGeospatialDisabled,
    MergeCardinalityViolation,
    PathNotFound,
    UnableToInferSchema,
    FailedReadFileCannotReadFileFooter,
    InvalidTimeTravelSpec,
    InvalidTimeTravelTimestampExprInput,
    InvalidTimeTravelTimestampExprNonDeterministic,
    AmbiguousReference,
    DatatypeMismatchValueOutOfRange,
    DatatypeMismatchStackColumnDiffTypes,
    DatatypeMismatchUnexpectedInputType,
    WrongNumArgsWithoutSuggestion,
    InvalidConfValueTimeZone,
    CannotMergeSchemas,
}

pub const TABLE_OR_VIEW_NOT_FOUND: Condition = Condition::TableOrViewNotFound;
pub const TABLE_OR_VIEW_ALREADY_EXISTS: Condition = Condition::TableOrViewAlreadyExists;
pub const NOT_SUPPORTED_COMMAND_FOR_V2_TABLE: Condition = Condition::NotSupportedCommandForV2Table;
pub const UNRESOLVED_COLUMN_WITH_SUGGESTION: Condition = Condition::UnresolvedColumnWithSuggestion;
pub const UNSUPPORTED_FEATURE_TABLE_OPERATION: Condition =
    Condition::UnsupportedFeatureTableOperation;
pub const INVALID_PARTITION_OPERATION_PARTITION_MANAGEMENT_IS_UNSUPPORTED: Condition =
    Condition::InvalidPartitionOperationPartitionManagementIsUnsupported;
pub const PARSE_SYNTAX_ERROR: Condition = Condition::ParseSyntaxError;
pub const INCOMPATIBLE_DATA_FOR_TABLE_CANNOT_SAFELY_CAST: Condition =
    Condition::IncompatibleDataForTableCannotSafelyCast;
pub const INSERT_COLUMN_ARITY_MISMATCH_NOT_ENOUGH_DATA_COLUMNS: Condition =
    Condition::InsertColumnArityMismatchNotEnoughDataColumns;
pub const NOT_NULL_ASSERT_VIOLATION: Condition = Condition::NotNullAssertViolation;
pub const NOT_SUPPORTED_CHANGE_COLUMN: Condition = Condition::NotSupportedChangeColumn;
pub const FAILED_TO_LOAD_ROUTINE: Condition = Condition::FailedToLoadRoutine;
pub const CAST_INVALID_INPUT: Condition = Condition::CastInvalidInput;
pub const REQUIRES_SINGLE_PART_NAMESPACE: Condition = Condition::RequiresSinglePartNamespace;
pub const UNSUPPORTED_FEATURE_GEOSPATIAL_DISABLED: Condition =
    Condition::UnsupportedFeatureGeospatialDisabled;
pub const MERGE_CARDINALITY_VIOLATION: Condition = Condition::MergeCardinalityViolation;
pub const PATH_NOT_FOUND: Condition = Condition::PathNotFound;
pub const UNABLE_TO_INFER_SCHEMA: Condition = Condition::UnableToInferSchema;
pub const FAILED_READ_FILE_CANNOT_READ_FILE_FOOTER: Condition =
    Condition::FailedReadFileCannotReadFileFooter;
pub const INVALID_TIME_TRAVEL_SPEC: Condition = Condition::InvalidTimeTravelSpec;
pub const INVALID_TIME_TRAVEL_TIMESTAMP_EXPR_INPUT: Condition =
    Condition::InvalidTimeTravelTimestampExprInput;
pub const INVALID_TIME_TRAVEL_TIMESTAMP_EXPR_NON_DETERMINISTIC: Condition =
    Condition::InvalidTimeTravelTimestampExprNonDeterministic;
pub const AMBIGUOUS_REFERENCE: Condition = Condition::AmbiguousReference;
pub const DATATYPE_MISMATCH_VALUE_OUT_OF_RANGE: Condition =
    Condition::DatatypeMismatchValueOutOfRange;
pub const DATATYPE_MISMATCH_STACK_COLUMN_DIFF_TYPES: Condition =
    Condition::DatatypeMismatchStackColumnDiffTypes;
pub const DATATYPE_MISMATCH_UNEXPECTED_INPUT_TYPE: Condition =
    Condition::DatatypeMismatchUnexpectedInputType;
pub const WRONG_NUM_ARGS_WITHOUT_SUGGESTION: Condition = Condition::WrongNumArgsWithoutSuggestion;
pub const INVALID_CONF_VALUE_TIME_ZONE: Condition = Condition::InvalidConfValueTimeZone;
pub const CANNOT_MERGE_SCHEMAS: Condition = Condition::CannotMergeSchemas;

impl Condition {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::TableOrViewNotFound => "TABLE_OR_VIEW_NOT_FOUND",
            Self::TableOrViewAlreadyExists => "TABLE_OR_VIEW_ALREADY_EXISTS",
            Self::NotSupportedCommandForV2Table => "NOT_SUPPORTED_COMMAND_FOR_V2_TABLE",
            Self::UnresolvedColumnWithSuggestion => "UNRESOLVED_COLUMN.WITH_SUGGESTION",
            Self::UnsupportedFeatureTableOperation => "UNSUPPORTED_FEATURE.TABLE_OPERATION",
            Self::InvalidPartitionOperationPartitionManagementIsUnsupported => {
                "INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED"
            }
            Self::ParseSyntaxError => "PARSE_SYNTAX_ERROR",
            Self::IncompatibleDataForTableCannotSafelyCast => {
                "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST"
            }
            Self::InsertColumnArityMismatchNotEnoughDataColumns => {
                "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS"
            }
            Self::NotNullAssertViolation => "NOT_NULL_ASSERT_VIOLATION",
            Self::NotSupportedChangeColumn => "NOT_SUPPORTED_CHANGE_COLUMN",
            Self::FailedToLoadRoutine => "FAILED_TO_LOAD_ROUTINE",
            Self::CastInvalidInput => "CAST_INVALID_INPUT",
            Self::RequiresSinglePartNamespace => "REQUIRES_SINGLE_PART_NAMESPACE",
            Self::UnsupportedFeatureGeospatialDisabled => "UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED",
            Self::MergeCardinalityViolation => "MERGE_CARDINALITY_VIOLATION",
            Self::PathNotFound => "PATH_NOT_FOUND",
            Self::UnableToInferSchema => "UNABLE_TO_INFER_SCHEMA",
            Self::FailedReadFileCannotReadFileFooter => "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER",
            Self::InvalidTimeTravelSpec => "INVALID_TIME_TRAVEL_SPEC",
            Self::InvalidTimeTravelTimestampExprInput => "INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.INPUT",
            Self::InvalidTimeTravelTimestampExprNonDeterministic => {
                "INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.NON_DETERMINISTIC"
            }
            Self::AmbiguousReference => "AMBIGUOUS_REFERENCE",
            Self::DatatypeMismatchValueOutOfRange => "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE",
            Self::DatatypeMismatchStackColumnDiffTypes => {
                "DATATYPE_MISMATCH.STACK_COLUMN_DIFF_TYPES"
            }
            Self::DatatypeMismatchUnexpectedInputType => "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
            Self::WrongNumArgsWithoutSuggestion => "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
            Self::InvalidConfValueTimeZone => "INVALID_CONF_VALUE.TIME_ZONE",
            Self::CannotMergeSchemas => "CANNOT_MERGE_SCHEMAS",
        }
    }

    #[must_use]
    pub fn sqlstate(self) -> Option<&'static str> {
        match self {
            Self::TableOrViewNotFound => Some("42P01"),
            Self::TableOrViewAlreadyExists => Some("42P07"),
            Self::NotSupportedCommandForV2Table => Some("0A000"),
            Self::UnresolvedColumnWithSuggestion => Some("42703"),
            Self::UnsupportedFeatureTableOperation => Some("0A000"),
            Self::InvalidPartitionOperationPartitionManagementIsUnsupported => Some("42601"),
            Self::ParseSyntaxError => Some("42601"),
            Self::IncompatibleDataForTableCannotSafelyCast => Some("KD000"),
            Self::InsertColumnArityMismatchNotEnoughDataColumns => Some("21S01"),
            Self::NotNullAssertViolation => Some("42000"),
            Self::NotSupportedChangeColumn => Some("0A000"),
            Self::FailedToLoadRoutine => Some("38000"),
            Self::CastInvalidInput => Some("22018"),
            Self::RequiresSinglePartNamespace => Some("42K05"),
            Self::UnsupportedFeatureGeospatialDisabled => Some("0A000"),
            Self::MergeCardinalityViolation => Some("23K01"),
            Self::PathNotFound => Some("42K03"),
            Self::UnableToInferSchema => Some("42KD9"),
            Self::FailedReadFileCannotReadFileFooter => Some("KD001"),
            Self::InvalidTimeTravelSpec => Some("42K0E"),
            Self::InvalidTimeTravelTimestampExprInput => Some("42K0E"),
            Self::InvalidTimeTravelTimestampExprNonDeterministic => Some("42K0E"),
            Self::AmbiguousReference => Some("42704"),
            Self::DatatypeMismatchValueOutOfRange => Some("42K09"),
            Self::DatatypeMismatchStackColumnDiffTypes => Some("42K09"),
            Self::DatatypeMismatchUnexpectedInputType => Some("42K09"),
            Self::WrongNumArgsWithoutSuggestion => Some("42605"),
            Self::InvalidConfValueTimeZone => Some("22022"),
            Self::CannotMergeSchemas => None,
        }
    }

    fn template(self) -> &'static str {
        match self {
            Self::TableOrViewNotFound => {
                "The table or view {relationName} cannot be found. Verify the spelling and correctness of the schema and catalog.\nIf you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct schema and catalog.\nTo tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS."
            }
            Self::TableOrViewAlreadyExists => {
                "Cannot create table or view {relationName} because it already exists.\nChoose a different name, drop or replace the existing object, or add the IF NOT EXISTS clause to tolerate pre-existing objects."
            }
            Self::NotSupportedCommandForV2Table => "{command} is not supported for v2 tables.",
            Self::UnresolvedColumnWithSuggestion => {
                "A column, variable, or function parameter with name {columnName} cannot be resolved. Did you mean one of the following? [{suggestions}]."
            }
            Self::UnsupportedFeatureTableOperation => {
                "The feature is not supported: Table {tableName} does not support column default value. Please check the current catalog and namespace to make sure the qualified table name is expected, and also check the catalog implementation which is configured by \"spark.sql.catalog\"."
            }
            Self::InvalidPartitionOperationPartitionManagementIsUnsupported => {
                "The partition command is invalid. Table {tableName} does not support partition management."
            }
            Self::ParseSyntaxError => "Syntax error at or near {near}.",
            Self::IncompatibleDataForTableCannotSafelyCast => {
                "Cannot write incompatible data for the table {tableName}: Cannot safely cast {columnName} \"{fromType}\" to \"{toType}\"."
            }
            Self::InsertColumnArityMismatchNotEnoughDataColumns => {
                "Cannot write to {tableName}, the reason is not enough data columns:\nTable columns: {tableColumns}.\nData columns: {dataColumns}."
            }
            Self::NotNullAssertViolation => {
                "NULL value appeared in non-nullable field: \n{fieldName}\nIf the schema is inferred from a Scala tuple/case class, or a Java bean, please try to use scala.Option[_] or other nullable types (such as java.lang.Integer instead of int/scala.Int)."
            }
            Self::NotSupportedChangeColumn => {
                "ALTER TABLE ALTER/CHANGE COLUMN is not supported for changing {tableName}'s column {columnName} with type \"{fromType}\" to {columnName} with type \"{toType}\"."
            }
            Self::FailedToLoadRoutine => "Failed to load routine {routineName}.",
            Self::CastInvalidInput => {
                "The value {value} of the type \"{fromType}\" cannot be cast to \"{toType}\" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead."
            }
            Self::RequiresSinglePartNamespace => {
                "{catalogName} requires a single-part namespace, but got {namespace}."
            }
            Self::UnsupportedFeatureGeospatialDisabled => {
                "The feature is not supported: Geospatial feature is disabled."
            }
            Self::MergeCardinalityViolation => {
                "The ON search condition of the MERGE statement matched a single row from the target table with multiple rows of the source table.\nThis could result in the target row being operated on more than once with an update or delete operation and is not allowed."
            }
            Self::PathNotFound => "Path does not exist: file:{path}.",
            Self::UnableToInferSchema => {
                "Unable to infer schema for ORC. It must be specified manually."
            }
            Self::FailedReadFileCannotReadFileFooter => {
                "Encountered error while reading file file:{path}. Could not read footer. {detail}"
            }
            Self::InvalidTimeTravelSpec => {
                "Cannot specify both version and timestamp when time travelling the table."
            }
            Self::InvalidTimeTravelTimestampExprInput => {
                "The time travel timestamp expression \"{display}\" is invalid. Cannot be casted to the \"TIMESTAMP\" type."
            }
            Self::InvalidTimeTravelTimestampExprNonDeterministic => {
                "The time travel timestamp expression \"{display}\" is invalid. Must be deterministic."
            }
            Self::AmbiguousReference => {
                "Reference {reference} is ambiguous, could be: [{options}]."
            }
            Self::DatatypeMismatchValueOutOfRange => {
                "Cannot resolve stack due to data type mismatch: The `n` must be between (0, 2147483647]{valueClause}."
            }
            Self::DatatypeMismatchStackColumnDiffTypes => {
                "Cannot resolve stack due to data type mismatch: The data type of the column do not have the same type: \"{leftType}\" <> \"{rightType}\"."
            }
            Self::DatatypeMismatchUnexpectedInputType => {
                "Cannot resolve stack due to data type mismatch: The first parameter requires the \"INT\" type, however the argument has the type \"{actualType}\"."
            }
            Self::WrongNumArgsWithoutSuggestion => {
                "The `stack` requires > 1 parameters but the actual number is {actualNumber}."
            }
            Self::InvalidConfValueTimeZone => {
                "The value '{value}' in the config \"{configKey}\" is invalid. Cannot resolve the given timezone."
            }
            Self::CannotMergeSchemas => {
                "Failed to merge ORC schemas: column `{columnName}` has conflicting types ({firstType} and {secondType})"
            }
        }
    }
}

fn substitute(template: &str, params: &[(&str, &str)]) -> String {
    let mut rendered = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        rendered.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else {
            rendered.push_str(&rest[open..]);
            break;
        };
        let key = &after[..close];
        match params.iter().find(|(name, _)| *name == key) {
            Some((_, value)) => rendered.push_str(value),
            None => {
                rendered.push('{');
                rendered.push_str(key);
                rendered.push('}');
            }
        }
        rest = &after[close + 1..];
    }
    rendered.push_str(rest);
    rendered
}

#[must_use]
pub fn message(condition: Condition, params: &[(&str, &str)]) -> String {
    let rendered = substitute(condition.template(), params);
    match condition.sqlstate() {
        Some(state) => format!("[{}] {rendered} SQLSTATE: {state}", condition.name()),
        None => format!("[{}] {rendered}", condition.name()),
    }
}

#[must_use]
pub fn analysis(condition: Condition, params: &[(&str, &str)]) -> Error {
    Error::Analysis(message(condition, params))
}

#[must_use]
pub fn parse(condition: Condition, params: &[(&str, &str)]) -> Error {
    Error::Parse(message(condition, params))
}

#[must_use]
pub fn unsupported(condition: Condition, params: &[(&str, &str)]) -> Error {
    Error::NotImplemented(message(condition, params))
}

#[must_use]
pub fn illegal_argument(condition: Condition, params: &[(&str, &str)]) -> Error {
    Error::IllegalArgument(message(condition, params))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorClass;

    const ALL_PARAMS: &[(&str, &str)] = &[
        ("relationName", "`sc`.`ns`.`t`"),
        ("command", "MSCK REPAIR TABLE"),
        ("columnName", "`id`"),
        ("suggestions", "`data`, `cat`"),
        ("tableName", "`sc`.`ns`.`t`"),
        ("near", "'LIKE'"),
        ("fromType", "STRING"),
        ("toType", "BIGINT"),
        ("tableColumns", "`id`, `data`"),
        ("dataColumns", "`col1`"),
        ("fieldName", "id"),
        ("routineName", "`sc`.`system`.`no_such_procedure`"),
        ("value", "'abc'"),
        ("catalogName", "spark_catalog"),
        ("namespace", "`SC`.`NS`"),
        ("path", "/tmp/missing"),
        ("detail", "boom"),
        ("display", "not a ts"),
        ("reference", "`id`"),
        ("options", "`t`.`id`, `t`.`id`"),
        ("valueClause", " (current value = 0)"),
        ("leftType", "Int64"),
        ("rightType", "Utf8"),
        ("actualType", "Utf8"),
        ("actualNumber", "1"),
        ("configKey", "spark.sql.session.timeZone"),
        ("firstType", "Int64"),
        ("secondType", "Utf8"),
    ];

    const ALL: &[Condition] = &[
        TABLE_OR_VIEW_NOT_FOUND,
        TABLE_OR_VIEW_ALREADY_EXISTS,
        NOT_SUPPORTED_COMMAND_FOR_V2_TABLE,
        UNRESOLVED_COLUMN_WITH_SUGGESTION,
        UNSUPPORTED_FEATURE_TABLE_OPERATION,
        INVALID_PARTITION_OPERATION_PARTITION_MANAGEMENT_IS_UNSUPPORTED,
        PARSE_SYNTAX_ERROR,
        INCOMPATIBLE_DATA_FOR_TABLE_CANNOT_SAFELY_CAST,
        INSERT_COLUMN_ARITY_MISMATCH_NOT_ENOUGH_DATA_COLUMNS,
        NOT_NULL_ASSERT_VIOLATION,
        NOT_SUPPORTED_CHANGE_COLUMN,
        FAILED_TO_LOAD_ROUTINE,
        CAST_INVALID_INPUT,
        REQUIRES_SINGLE_PART_NAMESPACE,
        UNSUPPORTED_FEATURE_GEOSPATIAL_DISABLED,
        MERGE_CARDINALITY_VIOLATION,
        PATH_NOT_FOUND,
        UNABLE_TO_INFER_SCHEMA,
        FAILED_READ_FILE_CANNOT_READ_FILE_FOOTER,
        INVALID_TIME_TRAVEL_SPEC,
        INVALID_TIME_TRAVEL_TIMESTAMP_EXPR_INPUT,
        INVALID_TIME_TRAVEL_TIMESTAMP_EXPR_NON_DETERMINISTIC,
        AMBIGUOUS_REFERENCE,
        DATATYPE_MISMATCH_VALUE_OUT_OF_RANGE,
        DATATYPE_MISMATCH_STACK_COLUMN_DIFF_TYPES,
        DATATYPE_MISMATCH_UNEXPECTED_INPUT_TYPE,
        WRONG_NUM_ARGS_WITHOUT_SUGGESTION,
        INVALID_CONF_VALUE_TIME_ZONE,
        CANNOT_MERGE_SCHEMAS,
    ];

    #[test]
    fn catalogue_lists_every_condition_once() {
        assert_eq!(ALL.len(), 29);
        let mut names: Vec<&str> = ALL.iter().map(|condition| condition.name()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), ALL.len());
    }

    #[test]
    fn every_condition_renders_name_and_sqlstate_clause() {
        for condition in ALL {
            let text = message(*condition, ALL_PARAMS);
            let head = format!("[{}] ", condition.name());
            assert!(text.starts_with(&head), "{condition:?}: {text}");
            assert!(!text.contains('{'), "{condition:?}: {text}");
            assert!(!text.contains('}'), "{condition:?}: {text}");
            match condition.sqlstate() {
                Some(state) => {
                    let clause = format!("SQLSTATE: {state}");
                    assert!(text.contains(&clause), "{condition:?}: {text}");
                }
                None => assert!(!text.contains("SQLSTATE"), "{condition:?}: {text}"),
            }
        }
    }

    #[test]
    fn take_conditions_carry_the_recorded_sqlstates() {
        let rows: [(Condition, &[(&str, &str)], &str); 16] = [
            (
                TABLE_OR_VIEW_NOT_FOUND,
                &[("relationName", "`ns`.`t`")],
                "42P01",
            ),
            (
                TABLE_OR_VIEW_ALREADY_EXISTS,
                &[("relationName", "`ns`.`t`")],
                "42P07",
            ),
            (
                NOT_SUPPORTED_COMMAND_FOR_V2_TABLE,
                &[("command", "MSCK REPAIR TABLE")],
                "0A000",
            ),
            (
                UNRESOLVED_COLUMN_WITH_SUGGESTION,
                &[("columnName", "`c`"), ("suggestions", "`a`")],
                "42703",
            ),
            (
                UNSUPPORTED_FEATURE_TABLE_OPERATION,
                &[("tableName", "`sc`.`ns`.`t`")],
                "0A000",
            ),
            (
                INVALID_PARTITION_OPERATION_PARTITION_MANAGEMENT_IS_UNSUPPORTED,
                &[("tableName", "`sc`.`ns`.`t`")],
                "42601",
            ),
            (PARSE_SYNTAX_ERROR, &[("near", "'LIKE'")], "42601"),
            (
                INCOMPATIBLE_DATA_FOR_TABLE_CANNOT_SAFELY_CAST,
                &[
                    ("tableName", "`sc`.`ns`.`t`"),
                    ("columnName", "`id`"),
                    ("fromType", "STRING"),
                    ("toType", "BIGINT"),
                ],
                "KD000",
            ),
            (
                INSERT_COLUMN_ARITY_MISMATCH_NOT_ENOUGH_DATA_COLUMNS,
                &[
                    ("tableName", "`sc`.`ns`.`t`"),
                    ("tableColumns", "`id`"),
                    ("dataColumns", "`col1`"),
                ],
                "21S01",
            ),
            (NOT_NULL_ASSERT_VIOLATION, &[("fieldName", "id")], "42000"),
            (
                NOT_SUPPORTED_CHANGE_COLUMN,
                &[
                    ("tableName", "`sc`.`ns`.`t`"),
                    ("columnName", "`id`"),
                    ("fromType", "BIGINT"),
                    ("toType", "INT"),
                ],
                "0A000",
            ),
            (
                FAILED_TO_LOAD_ROUTINE,
                &[("routineName", "`sc`.`system`.`p`")],
                "38000",
            ),
            (
                CAST_INVALID_INPUT,
                &[
                    ("value", "'abc'"),
                    ("fromType", "STRING"),
                    ("toType", "BIGINT"),
                ],
                "22018",
            ),
            (
                REQUIRES_SINGLE_PART_NAMESPACE,
                &[("catalogName", "spark_catalog"), ("namespace", "`SC`.`NS`")],
                "42K05",
            ),
            (UNSUPPORTED_FEATURE_GEOSPATIAL_DISABLED, &[], "0A000"),
            (MERGE_CARDINALITY_VIOLATION, &[], "23K01"),
        ];
        for (condition, params, sqlstate) in rows {
            let text = message(condition, params);
            let head = format!("[{}] ", condition.name());
            let clause = format!("SQLSTATE: {sqlstate}");
            assert!(text.starts_with(&head), "{condition:?}: {text}");
            assert!(text.contains(&clause), "{condition:?}: {text}");
            assert_eq!(condition.sqlstate(), Some(sqlstate));
        }
    }

    #[test]
    fn moved_scan_rows_reproduce_legacy_bytes() {
        assert_eq!(
            message(PATH_NOT_FOUND, &[("path", "gone")]),
            "[PATH_NOT_FOUND] Path does not exist: file:gone. SQLSTATE: 42K03"
        );
        assert_eq!(
            message(UNABLE_TO_INFER_SCHEMA, &[]),
            "[UNABLE_TO_INFER_SCHEMA] Unable to infer schema for ORC. It must be specified manually. SQLSTATE: 42KD9"
        );
        assert_eq!(
            message(
                FAILED_READ_FILE_CANNOT_READ_FILE_FOOTER,
                &[("path", "f.orc"), ("detail", "boom")]
            ),
            "[FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER] Encountered error while reading file file:f.orc. Could not read footer. boom SQLSTATE: KD001"
        );
        assert_eq!(
            message(
                CANNOT_MERGE_SCHEMAS,
                &[
                    ("columnName", "c"),
                    ("firstType", "Int64"),
                    ("secondType", "Utf8"),
                ]
            ),
            "[CANNOT_MERGE_SCHEMAS] Failed to merge ORC schemas: column `c` has conflicting types (Int64 and Utf8)"
        );
    }

    #[test]
    fn moved_time_travel_rows_reproduce_legacy_bytes() {
        assert_eq!(
            message(INVALID_TIME_TRAVEL_SPEC, &[]),
            "[INVALID_TIME_TRAVEL_SPEC] Cannot specify both version and timestamp when time travelling the table. SQLSTATE: 42K0E"
        );
        assert_eq!(
            message(
                INVALID_TIME_TRAVEL_TIMESTAMP_EXPR_INPUT,
                &[("display", "not a ts")]
            ),
            "[INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.INPUT] The time travel timestamp expression \"not a ts\" is invalid. Cannot be casted to the \"TIMESTAMP\" type. SQLSTATE: 42K0E"
        );
        assert_eq!(
            message(
                INVALID_TIME_TRAVEL_TIMESTAMP_EXPR_NON_DETERMINISTIC,
                &[("display", "rand()")]
            ),
            "[INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.NON_DETERMINISTIC] The time travel timestamp expression \"rand()\" is invalid. Must be deterministic. SQLSTATE: 42K0E"
        );
    }

    #[test]
    fn moved_stack_and_misc_rows_reproduce_legacy_bytes() {
        assert_eq!(
            message(
                AMBIGUOUS_REFERENCE,
                &[("reference", "`id`"), ("options", "`t`.`id`, `t`.`id`")]
            ),
            "[AMBIGUOUS_REFERENCE] Reference `id` is ambiguous, could be: [`t`.`id`, `t`.`id`]. SQLSTATE: 42704"
        );
        assert_eq!(
            message(
                DATATYPE_MISMATCH_VALUE_OUT_OF_RANGE,
                &[("valueClause", " (current value = 0)")]
            ),
            "[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE] Cannot resolve stack due to data type mismatch: The `n` must be between (0, 2147483647] (current value = 0). SQLSTATE: 42K09"
        );
        assert_eq!(
            message(DATATYPE_MISMATCH_VALUE_OUT_OF_RANGE, &[("valueClause", "")]),
            "[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE] Cannot resolve stack due to data type mismatch: The `n` must be between (0, 2147483647]. SQLSTATE: 42K09"
        );
        assert_eq!(
            message(
                DATATYPE_MISMATCH_STACK_COLUMN_DIFF_TYPES,
                &[("leftType", "Int64"), ("rightType", "Utf8")]
            ),
            "[DATATYPE_MISMATCH.STACK_COLUMN_DIFF_TYPES] Cannot resolve stack due to data type mismatch: The data type of the column do not have the same type: \"Int64\" <> \"Utf8\". SQLSTATE: 42K09"
        );
        assert_eq!(
            message(
                DATATYPE_MISMATCH_UNEXPECTED_INPUT_TYPE,
                &[("actualType", "Utf8")]
            ),
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve stack due to data type mismatch: The first parameter requires the \"INT\" type, however the argument has the type \"Utf8\". SQLSTATE: 42K09"
        );
        assert_eq!(
            message(WRONG_NUM_ARGS_WITHOUT_SUGGESTION, &[("actualNumber", "1")]),
            "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `stack` requires > 1 parameters but the actual number is 1. SQLSTATE: 42605"
        );
        assert_eq!(
            message(
                INVALID_CONF_VALUE_TIME_ZONE,
                &[
                    ("value", "Mars/Olympus"),
                    ("configKey", "spark.sql.session.timeZone"),
                ]
            ),
            "[INVALID_CONF_VALUE.TIME_ZONE] The value 'Mars/Olympus' in the config \"spark.sql.session.timeZone\" is invalid. Cannot resolve the given timezone. SQLSTATE: 22022"
        );
    }

    #[test]
    fn constructors_route_to_their_error_partitions() {
        assert_eq!(
            analysis(TABLE_OR_VIEW_NOT_FOUND, &[("relationName", "`t`")]).exception_class(),
            ErrorClass::Analysis
        );
        assert_eq!(
            parse(PARSE_SYNTAX_ERROR, &[("near", "'X'")]).exception_class(),
            ErrorClass::Parse
        );
        assert_eq!(
            unsupported(UNSUPPORTED_FEATURE_GEOSPATIAL_DISABLED, &[]).exception_class(),
            ErrorClass::Unsupported
        );
        assert_eq!(
            illegal_argument(CAST_INVALID_INPUT, ALL_PARAMS).exception_class(),
            ErrorClass::IllegalArgument
        );
    }

    #[test]
    fn analysis_renders_the_brief_example() {
        let error = analysis(TABLE_OR_VIEW_NOT_FOUND, &[("relationName", "`t`")]);
        let text = error.to_string();
        assert!(text.starts_with("[TABLE_OR_VIEW_NOT_FOUND]"), "{text}");
        assert!(text.contains("SQLSTATE: 42P01"), "{text}");
    }

    #[test]
    fn missing_param_stays_literal() {
        assert_eq!(
            message(PATH_NOT_FOUND, &[]),
            "[PATH_NOT_FOUND] Path does not exist: file:{path}. SQLSTATE: 42K03"
        );
    }
}
