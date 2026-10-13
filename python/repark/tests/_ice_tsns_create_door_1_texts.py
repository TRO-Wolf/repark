"""ICE-TSNS-CREATE-DOOR-1: the exact refusal texts of the Spark door, by seat."""

from __future__ import annotations

from typing import Any

SETTING_KEY = "repark.sql.allowImplicitTimestampNanos"
SETTING = (
    f"build the session with `{SETTING_KEY}` = true (`ReparkSession.builder.config`; the key is "
    "read once, when the session is built)"
)
CREATE_V3 = (
    "Create the table with TBLPROPERTIES ('format-version'='3') in SQL or with "
    "`writeTo(...).tableProperty('format-version', '3')`; `saveAsTable` cannot set the format "
    "version."
)
REPLACE_V3 = (
    "Replace the table with TBLPROPERTIES ('format-version'='3') in SQL or with "
    "`writeTo(...).tableProperty('format-version', '3')`, or upgrade it first with ALTER TABLE … "
    "SET TBLPROPERTIES ('format-version'='3')."
)
UPGRADE_V3 = "Upgrade the table first with ALTER TABLE … SET TBLPROPERTIES ('format-version'='3')."
PROPERTY = "The table must be format version 3: table property 'format-version' = '3'."
NANO = {"c": "timestamp_ns", "cz": "timestamptz_ns"}
MICRO = {
    "timestamp_ns": "CAST(<expression> AS TIMESTAMP_NTZ)",
    "timestamptz_ns": "CAST(<expression> AS TIMESTAMP)",
}
MERGE_MICRO = {**MICRO, "timestamptz_ns": "to_timestamp(<expression>)"}
MERGE_FORM = "MERGE WITH SCHEMA EVOLUTION"


def not_requested(
    form: str, name: str, nano: str | None = None, below: str | None = None
) -> dict[str, Any]:
    """Return the refusal of an unasked nanosecond column ``name`` through ``form``."""
    nano = nano or NANO[name]
    micro = (MERGE_MICRO if form == MERGE_FORM else MICRO)[nano]
    also = f" A nanosecond column also needs a format version 3 table. {below}" if below else ""
    text = (
        f"Error during planning: [TIMESTAMP_NANOS_NOT_REQUESTED] {form} would create column "
        f"`{name}` as {nano}, a type Spark cannot read, and the statement does not ask for it. "
        f"Ask with CAST(<expression> AS {nano}) on the column (SQL or selectExpr), or "
        f"{SETTING}.{also} For microseconds write {micro}."
    )
    return {
        "class": "AnalysisException",
        "condition": "TIMESTAMP_NANOS_NOT_REQUESTED",
        "text": text,
    }


def requires_v3(name: str, nano: str, remedy: str) -> dict[str, Any]:
    """Return the refusal of a nanosecond type the statement asked for below format v3."""
    return {
        "class": "AnalysisException",
        "condition": "TIMESTAMP_NANOS_REQUIRES_FORMAT_V3",
        "text": (
            "Error during planning: [TIMESTAMP_NANOS_REQUIRES_FORMAT_V3] Invalid type for "
            f"{name}: {nano} is not supported until v3. {remedy}"
        ),
    }
