from __future__ import annotations

from pathlib import Path
from typing import Any

from repark_parity.scoreboard.encoding import cell_digest, encode_line, read_records, write_lines

CLASSES = (
    "EQUAL",
    "BOTH-REFUSED",
    "WRONG-VALUE",
    "WRONG-TYPE",
    "NULLABILITY-ONLY",
    "ACCEPTED-WHERE-SPARK-REFUSED",
    "REFUSED-WHERE-SPARK-ANSWERS",
    "PANIC",
    "EXCLUDED",
)
NULLABILITY_KEYS = frozenset({"nullable", "containsNull", "valueContainsNull"})
LABEL_KEYS = frozenset({"name", "metadata"}) | NULLABILITY_KEYS


def strip_nullability(schema: Any) -> Any:
    """Remove nullability at every depth and keep names and decimal types."""
    if isinstance(schema, list):
        return [strip_nullability(item) for item in schema]
    if isinstance(schema, dict):
        return {
            key: strip_nullability(item)
            for key, item in schema.items()
            if key not in NULLABILITY_KEYS
        }
    return schema


def schema_parts(schema: Any) -> Any:
    """Remove field labels, metadata and nullability and keep the logical type structure."""
    if isinstance(schema, list):
        return [schema_parts(item) for item in schema]
    if isinstance(schema, dict):
        return {key: schema_parts(item) for key, item in schema.items() if key not in LABEL_KEYS}
    return schema


def primary_rows(answer: dict) -> list:
    """Return the public columns of each row, without the supplemental rendered text."""
    width = len(answer["schema"]["fields"])
    rows = [item[:width] for item in answer["value"]]
    return rows if answer.get("ordered") else sorted(rows, key=encode_line)


def _answered_details(left: dict, right: dict) -> tuple[str, dict]:
    details: dict[str, Any] = {}
    values = left["value"] != right["value"]
    if "fields" in (left.get("schema") or {}) and "fields" in (right.get("schema") or {}):
        primary_diff = primary_rows(left) != primary_rows(right)
        rendering = (
            left.get("rendered_columns") == right.get("rendered_columns")
            and bool(left.get("rendered_columns"))
            and left.get("observation") == right.get("observation")
        )
        details["primary_value_diff"] = primary_diff
        details["render_diff"] = values if rendering and not primary_diff else None
        if not rendering and (left.get("rendered_columns") or right.get("rendered_columns")):
            values = primary_diff
    schemas = left["schema"] != right["schema"]
    types = strip_nullability(left["schema"]) != strip_nullability(right["schema"])
    details.update(
        value_diff=values,
        schema_diff=schemas,
        type_or_name_diff=types,
        logical_type_diff=schema_parts(left["schema"]) != schema_parts(right["schema"]),
    )
    if types:
        return "WRONG-TYPE", details
    if values:
        return "WRONG-VALUE", details
    if schemas:
        return "NULLABILITY-ONLY", details
    return "EQUAL", details


def classify_pair(left: dict, right: dict) -> dict:
    """Classify a Spark answer (left) against a RePark answer (right)."""
    outcomes = (left["outcome"], right["outcome"])
    details: dict[str, Any] = {}
    if "panicked" in outcomes:
        label = "PANIC"
    elif "excluded" in outcomes:
        label = "EXCLUDED"
        details["reason"] = [row.get("reason") for row in (left, right)]
    elif outcomes == ("refused", "refused"):
        label = "BOTH-REFUSED"
        for field in ("error_class", "sqlstate"):
            spark_code, repark_code = left.get(field), right.get(field)
            known = spark_code is not None and repark_code is not None
            details[field + "_match"] = spark_code == repark_code if known else None
    elif outcomes == ("refused", "answered"):
        label = "ACCEPTED-WHERE-SPARK-REFUSED"
    elif outcomes == ("answered", "refused"):
        label = "REFUSED-WHERE-SPARK-ANSWERS"
    elif outcomes == ("answered", "answered"):
        label, details = _answered_details(left, right)
    else:
        raise ValueError(f"unknown outcome combination {outcomes}")
    return {"id": left["id"], "class": label, **details}


def check_answers(cases: dict, answers: dict, engine: str) -> None:
    """Fail unless every answer carries its engine tag and its cell's exact digest."""
    for key, cell in cases.items():
        answer = answers[key]
        if answer["input_sha256"] != cell_digest(cell) or answer["engine"] != engine:
            raise ValueError(f"wrong engine or input digest: {key}")


def diff_rows(cases: dict, spark: dict, repark: dict) -> list[dict]:
    """Join both answer sets to the cells and classify each pair, failing closed."""
    if not (cases.keys() == spark.keys() == repark.keys()):
        raise ValueError("input or engine answer ID sets differ")
    check_answers(cases, spark, "spark")
    check_answers(cases, repark, "repark")
    rows = []
    for key, cell in cases.items():
        ordered = cell.get("ordered", False)
        verdict = classify_pair(
            {**spark[key], "ordered": ordered}, {**repark[key], "ordered": ordered}
        )
        rows.append(
            {
                **verdict,
                "ansi": cell["ansi"],
                "zone": cell["zone"],
                "seat": cell["seat"],
                "surface": cell["surface"],
                "names": cell.get("names", []),
            }
        )
    return rows


def diff_files(cases: Path, spark: Path, repark: Path, output: Path) -> list[dict]:
    """Classify one lane's answer files and write its diff file."""
    rows = diff_rows(read_records(cases), read_records(spark), read_records(repark))
    write_lines(output, rows)
    return rows
