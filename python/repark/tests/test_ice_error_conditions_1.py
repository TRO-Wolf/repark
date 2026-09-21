"""pins: ice-error-conditions-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007,
C-008, C-009, C-011"""

from __future__ import annotations

import pytest

from repark.errors import (
    AnalysisException,
    CommitStateUnknownException,
    IllegalArgumentException,
    ParseException,
    PySparkException,
    UnsupportedOperationException,
)
from repark.spark._integral import attach_error_condition


def test_condition_parser_rejects_lowercase_bracket() -> None:
    """A lowercase bracket tag is not a condition."""
    error = AnalysisException("[lowercase] x")
    assert error.getCondition() is None
    assert error.getErrorClass() is None


def test_condition_parser_rejects_mid_message_bracket() -> None:
    """A bracket not at column 0 of a prefix-free message is not a condition."""
    error = AnalysisException("hello [TABLE_OR_VIEW_NOT_FOUND] x")
    assert error.getCondition() is None


def test_condition_parser_rejects_non_condition_bracket() -> None:
    """User text in brackets at column 0 is not a condition."""
    error = AnalysisException("[some user text] x")
    assert error.getCondition() is None


def test_condition_parser_rejects_legacy_underscore() -> None:
    """A leading-underscore Spark internal id is not a condition."""
    error = AnalysisException("[_LEGACY_ERROR_TEMP_2330] x SQLSTATE: 0A000")
    assert error.getCondition() is None
    assert error.getSqlState() == "0A000"


def test_condition_parser_accepts_sub_condition() -> None:
    """A dotted sub-condition parses with its SQLSTATE."""
    error = AnalysisException("[UNRESOLVED_COLUMN.WITH_SUGGESTION] missing SQLSTATE: 42703")
    assert error.getCondition() == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
    assert error.getSqlState() == "42703"


def test_condition_parser_strips_planning_prefix() -> None:
    """The recorded D-TRUNCATE-PARTITION text parses after one known prefix."""
    error = AnalysisException(
        "Error during planning: "
        "[INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED] "
        "The partition command is invalid. Table does not support partition management. "
        "SQLSTATE: 42601"
    )
    assert error.getCondition() == "INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED"
    assert error.getSqlState() == "42601"


@pytest.mark.parametrize(
    "prefix",
    ["Error during planning: ", "datafusion engine error: ", "SQL error: "],
)
def test_condition_parser_strips_each_known_prefix(prefix: str) -> None:
    """Each known engine prefix yields to a column-0 bracket."""
    error = AnalysisException(f"{prefix}[FOO] x SQLSTATE: 42P01")
    assert error.getCondition() == "FOO"
    assert error.getSqlState() == "42P01"


def test_condition_parser_does_not_search_for_a_bracket() -> None:
    """After one prefix the bracket must still sit at column 0."""
    error = AnalysisException(
        "Error during planning: boom [TABLE_OR_VIEW_NOT_FOUND] x SQLSTATE: 42P01"
    )
    assert error.getCondition() is None
    assert error.getSqlState() == "42P01"


def test_condition_parser_strips_only_one_prefix() -> None:
    """Two stacked prefixes leave the bracket off column 0."""
    error = AnalysisException("Error during planning: datafusion engine error: [FOO] x")
    assert error.getCondition() is None


def test_sqlstate_parser_requires_five_chars() -> None:
    """A four-char SQLSTATE is malformed; the condition still parses."""
    error = AnalysisException("[A.B] text SQLSTATE: 42K0")
    assert error.getCondition() == "A.B"
    assert error.getSqlState() is None


def test_sqlstate_parser_takes_last_occurrence_not_the_tail() -> None:
    """A trailing position suffix must not hide the SQLSTATE."""
    error = AnalysisException("[NOT_SUPPORTED_CHANGE_COLUMN] x SQLSTATE: 0A000; line 1 pos 0;")
    assert error.getCondition() == "NOT_SUPPORTED_CHANGE_COLUMN"
    assert error.getSqlState() == "0A000"


def test_sqlstate_parser_survives_caret_block() -> None:
    """A `== SQL ==` caret block after the SQLSTATE must not hide it."""
    error = AnalysisException(
        "[PARSE_SYNTAX_ERROR] Syntax error at or near 'SELCT'. "
        "SQLSTATE: 42601 (line 1, pos 14)\n== SQL ==\nSELECT\n---^^^"
    )
    assert error.getCondition() == "PARSE_SYNTAX_ERROR"
    assert error.getSqlState() == "42601"


def test_message_without_bracket_is_unchanged() -> None:
    """A message with no bracket reports no condition and keeps its text."""
    error = AnalysisException("native diagnostic")
    assert error.getCondition() is None
    assert error.getErrorClass() is None
    assert error.getSqlState() is None
    assert str(error) == "native diagnostic"


def test_get_error_class_alias_matches_get_condition() -> None:
    """getErrorClass is the deprecated alias of getCondition."""
    shaped = AnalysisException("[FOO] x SQLSTATE: 42P01")
    assert shaped.getErrorClass() == shaped.getCondition() == "FOO"
    plain = AnalysisException("native diagnostic")
    assert plain.getErrorClass() is None
    assert plain.getCondition() is None


def test_col0_table_or_view_not_found_without_sqlstate() -> None:
    """The recorded W-DF-V2-REPLACE-MISSING-ERR text parses its col-0 condition."""
    error = AnalysisException(
        "[TABLE_OR_VIEW_NOT_FOUND] Cannot replace table "
        "'sc.ns.t_w_df_v2_replace_missing_err' because it does not exist. "
        "Use create() or createOrReplace() to create it."
    )
    assert error.getCondition() == "TABLE_OR_VIEW_NOT_FOUND"
    assert error.getSqlState() is None


def test_col0_table_or_view_already_exists_without_sqlstate() -> None:
    """The recorded W-DF-V2-CREATE-EXISTS-ERR text parses its col-0 condition."""
    error = AnalysisException(
        "[TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view "
        "'sc.ns.t_w_df_v2_create_exists_err' because it already exists. "
        "Choose a different name, drop or replace the existing object, or use createOrReplace()."
    )
    assert error.getCondition() == "TABLE_OR_VIEW_ALREADY_EXISTS"
    assert error.getSqlState() is None


def test_drop_table_missing_stamped_message_parses() -> None:
    """The D-DROP-TABLE-MISSING-ERR stamp parses through the planning prefix."""
    error = AnalysisException(
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view "
        "`ice`.`sales`.`never_there` cannot be found. Verify the spelling and correctness "
        "of the schema and catalog. If you did not qualify the name with a schema, verify "
        "the current_schema() output, or qualify the name with the correct schema and "
        "catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF "
        "EXISTS. SQLSTATE: 42P01"
    )
    assert isinstance(error, AnalysisException)
    assert error.getCondition() == "TABLE_OR_VIEW_NOT_FOUND"
    assert error.getSqlState() == "42P01"


def test_create_table_existing_stamped_message_parses() -> None:
    """The D-CREATE-EXISTS-ERR/D-CTAS-EXISTS-ERR stamp parses its condition and SQLSTATE."""
    error = AnalysisException(
        "Error during planning: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or "
        "view `ice`.`sales`.`dup` because it already exists. Choose a different name, "
        "drop or replace the existing object, or add the IF NOT EXISTS clause to "
        "tolerate pre-existing objects. SQLSTATE: 42P07"
    )
    assert isinstance(error, AnalysisException)
    assert error.getCondition() == "TABLE_OR_VIEW_ALREADY_EXISTS"
    assert error.getSqlState() == "42P07"


def test_save_as_table_errorifexists_stamped_message_parses() -> None:
    """The W-DF-SAVEASTABLE-ERRORIFEXISTS stamp parses its condition and SQLSTATE."""
    error = AnalysisException(
        "[TABLE_OR_VIEW_ALREADY_EXISTS] table 'cat.ns.t' already exists; use mode("
        "'append'|'overwrite'|'ignore') to write into an existing table. SQLSTATE: 42P07"
    )
    assert isinstance(error, AnalysisException)
    assert error.getCondition() == "TABLE_OR_VIEW_ALREADY_EXISTS"
    assert error.getSqlState() == "42P07"


def test_writer_v2_create_stamped_message_parses() -> None:
    """The W-DF-V2-CREATE-EXISTS-ERR stamp now reports its SQLSTATE."""
    error = AnalysisException(
        "[TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view 'cat.ns.t' because "
        "it already exists. Choose a different name, drop or replace the existing "
        "object, or use createOrReplace(). SQLSTATE: 42P07"
    )
    assert isinstance(error, AnalysisException)
    assert error.getCondition() == "TABLE_OR_VIEW_ALREADY_EXISTS"
    assert error.getSqlState() == "42P07"


def test_writer_v2_replace_stamped_message_parses() -> None:
    """The W-DF-V2-REPLACE-MISSING-ERR stamp now reports its SQLSTATE."""
    error = AnalysisException(
        "[TABLE_OR_VIEW_NOT_FOUND] Cannot replace table 'cat.ns.t' because it does "
        "not exist. Use create() or createOrReplace() to create it. SQLSTATE: 42P01"
    )
    assert isinstance(error, AnalysisException)
    assert error.getCondition() == "TABLE_OR_VIEW_NOT_FOUND"
    assert error.getSqlState() == "42P01"


def test_writer_v2_append_missing_stamped_message_parses() -> None:
    """The W-DF-V2-APPEND-MISSING-ERR stamp parses its condition and SQLSTATE."""
    error = AnalysisException(
        "[TABLE_OR_VIEW_NOT_FOUND] Cannot write to table 'cat.ns.t' because it "
        "does not exist. Use create() or createOrReplace() first. SQLSTATE: 42P01"
    )
    assert isinstance(error, AnalysisException)
    assert error.getCondition() == "TABLE_OR_VIEW_NOT_FOUND"
    assert error.getSqlState() == "42P01"


def test_truncate_partition_stamped_message_parses() -> None:
    error = AnalysisException(
        "Error during planning: "
        "[INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED] "
        "The partition command is invalid. Table `ice`.`sales`.`part` does not support "
        "partition management. SQLSTATE: 42601"
    )
    assert isinstance(error, AnalysisException)
    assert error.getCondition() == "INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED"
    assert error.getSqlState() == "42601"


def test_alter_add_partition_stamped_message_parses() -> None:
    error = AnalysisException(
        "Error during planning: "
        "[INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED] "
        "The partition command is invalid. Table `ice`.`sales`.`t` does not support "
        "partition management. SQLSTATE: 42601"
    )
    assert isinstance(error, AnalysisException)
    assert error.getCondition() == "INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED"
    assert error.getSqlState() == "42601"


def test_show_partitions_stamped_message_parses() -> None:
    error = AnalysisException(
        "Error during planning: "
        "[INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED] "
        "The partition command is invalid. Table `ice`.`sales`.`t` does not support "
        "partition management. SQLSTATE: 42601"
    )
    assert isinstance(error, AnalysisException)
    assert error.getCondition() == "INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED"
    assert error.getSqlState() == "42601"


def test_set_serde_stamped_message_parses() -> None:
    error = AnalysisException(
        "Error during planning: "
        "[NOT_SUPPORTED_COMMAND_FOR_V2_TABLE] "
        "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES] is not supported for v2 tables. "
        "SQLSTATE: 0A000"
    )
    assert isinstance(error, AnalysisException)
    assert error.getCondition() == "NOT_SUPPORTED_COMMAND_FOR_V2_TABLE"
    assert error.getSqlState() == "0A000"


def test_describe_as_json_stamped_message_parses() -> None:
    error = AnalysisException(
        "Error during planning: "
        "[NOT_SUPPORTED_COMMAND_FOR_V2_TABLE] "
        "DESCRIBE TABLE AS JSON is not supported for v2 tables. "
        "SQLSTATE: 0A000"
    )
    assert isinstance(error, AnalysisException)
    assert error.getCondition() == "NOT_SUPPORTED_COMMAND_FOR_V2_TABLE"
    assert error.getSqlState() == "0A000"


def test_msck_repair_stamped_message_parses() -> None:
    error = AnalysisException(
        "Error during planning: "
        "[NOT_SUPPORTED_COMMAND_FOR_V2_TABLE] "
        "MSCK REPAIR TABLE is not supported for v2 tables. "
        "SQLSTATE: 0A000"
    )
    assert isinstance(error, AnalysisException)
    assert error.getCondition() == "NOT_SUPPORTED_COMMAND_FOR_V2_TABLE"
    assert error.getSqlState() == "0A000"


def test_analyze_table_stamped_message_parses() -> None:
    error = AnalysisException(
        "Error during planning: "
        "[NOT_SUPPORTED_COMMAND_FOR_V2_TABLE] "
        "ANALYZE TABLE is not supported for v2 tables. "
        "SQLSTATE: 0A000"
    )
    assert isinstance(error, AnalysisException)
    assert error.getCondition() == "NOT_SUPPORTED_COMMAND_FOR_V2_TABLE"
    assert error.getSqlState() == "0A000"


def test_attach_error_condition_wins_over_class_parser() -> None:
    """An instance attach must outrank the class-level message parser."""
    error = AnalysisException("[WRONG_CLASS] hello SQLSTATE: 00000")
    attach_error_condition(error, "PATH_NOT_FOUND", "42K03")
    error._spark_message_parameters = {"path": "file:/x"}
    assert error.getCondition() == "PATH_NOT_FOUND"
    assert error.getErrorClass() == "PATH_NOT_FOUND"
    assert error.getSqlState() == "42K03"
    assert error.getMessageParameters() == {"path": "file:/x"}


@pytest.mark.parametrize(
    "exception_type",
    [
        PySparkException,
        AnalysisException,
        ParseException,
        UnsupportedOperationException,
        IllegalArgumentException,
        CommitStateUnknownException,
    ],
)
def test_condition_parser_binds_on_every_native_class(
    exception_type: type[PySparkException],
) -> None:
    """The for-loop binds the parser on every native class, not Analysis only."""
    error = exception_type("[FOO] x SQLSTATE: 42P01")
    assert error.getCondition() == "FOO"
    assert error.getErrorClass() == "FOO"
    assert error.getSqlState() == "42P01"
    assert error.getMessageParameters() is None
