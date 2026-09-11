"""Both-door pins for the secrets torture family and the flag_secret_columns read option."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
from _support import read_frame_door, read_sql_door

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark._secrets import prop_key_is_secret
from repark_parity.torture import FAMILIES, Family, FamilyOutput
from repark_parity.torture.secrets import (
    FLAGGED_COLUMN_NAMES,
    ORDINARY_COLUMN_NAMES,
    SECRETS_FAMILY,
)

PARQUET_VIEW = "torture_secrets_parquet"
CSV_VIEW = "torture_secrets_csv"
FLAG_VIEW = "torture_secrets_flagged"


def test_family_satisfies_protocol() -> None:
    """The secrets module exposes the registry's Family implementor."""
    assert isinstance(FAMILIES["secrets"], Family)
    assert FAMILIES["secrets"] is SECRETS_FAMILY


def test_flagged_names_match_the_needle_set() -> None:
    """Every flagged name trips prop_key_is_secret; every ordinary name does not (D-4a)."""
    for name in FLAGGED_COLUMN_NAMES:
        assert prop_key_is_secret(name), name
    for name in ORDINARY_COLUMN_NAMES:
        assert not prop_key_is_secret(name), name


def test_secrets_parquet_dataframe_door(spark: ReparkSession, secrets_data: FamilyOutput) -> None:
    """The DataFrame door reads every generated secrets row under the declared schema."""
    table = read_frame_door(spark, secrets_data.parquet_path, "parquet")
    expected = SECRETS_FAMILY.expected_read_schema("parquet")
    assert expected is not None
    assert table.num_rows == secrets_data.rows
    assert table.schema.equals(expected)


def test_secrets_parquet_sql_door(spark: ReparkSession, secrets_data: FamilyOutput) -> None:
    """The spark.sql door reads every generated secrets row under the declared schema."""
    table = read_sql_door(spark, secrets_data.parquet_path, "parquet", PARQUET_VIEW)
    expected = SECRETS_FAMILY.expected_read_schema("parquet")
    assert expected is not None
    assert table.num_rows == secrets_data.rows
    assert table.schema.equals(expected)


def test_secrets_csv_dataframe_door(spark: ReparkSession, secrets_data: FamilyOutput) -> None:
    """The DataFrame door reads every generated secrets CSV row under the declared schema."""
    table = read_frame_door(spark, secrets_data.csv_path, "csv")
    expected = SECRETS_FAMILY.expected_read_schema("csv")
    assert expected is not None
    assert table.num_rows == secrets_data.rows
    assert table.schema.equals(expected)


def test_secrets_csv_sql_door(spark: ReparkSession, secrets_data: FamilyOutput) -> None:
    """The spark.sql door reads every generated secrets CSV row under the declared schema."""
    table = read_sql_door(spark, secrets_data.csv_path, "csv", CSV_VIEW)
    expected = SECRETS_FAMILY.expected_read_schema("csv")
    assert expected is not None
    assert table.num_rows == secrets_data.rows
    assert table.schema.equals(expected)


def _flagged_csv_frame(session: ReparkSession, path: Path, mode: str) -> Any:
    """One DataFrame-door CSV read under flag_secret_columns=<mode>."""
    return session.read.option("flag_secret_columns", mode).csv(
        str(path), header=True, inferSchema=True
    )


def _finish_door(session: ReparkSession, frame: Any, door: str, view: str) -> Any:
    """Finish one door: collect the frame or register it and query the temp view."""
    if door == "sql":
        frame.createOrReplaceTempView(view)
        return session.sql(f"SELECT * FROM {view}").to_arrow()
    return frame.to_arrow()


@pytest.mark.parametrize("door", ["dataframe", "sql"])
def test_secret_flag_off_warn_refuse(
    spark: ReparkSession,
    secrets_data: FamilyOutput,
    door: str,
    capfd: pytest.CaptureFixture[str],
) -> None:
    """off reads clean; warn logs one WARNING naming flagged columns; refuse raises naming them."""
    off_frame = _flagged_csv_frame(spark, secrets_data.csv_path, "off")
    off_table = _finish_door(spark, off_frame, door, f"{FLAG_VIEW}_off_{door}")
    assert off_table.num_rows == secrets_data.rows
    capfd.readouterr()

    warn_frame = _flagged_csv_frame(spark, secrets_data.csv_path, "warn")
    warn_table = _finish_door(spark, warn_frame, door, f"{FLAG_VIEW}_warn_{door}")
    assert warn_table.num_rows == secrets_data.rows
    warn_lines = [
        line for line in capfd.readouterr().err.splitlines() if "flag_secret_columns" in line
    ]
    assert len(warn_lines) == 1, warn_lines
    assert warn_lines[0].startswith("WARNING"), warn_lines
    for name in FLAGGED_COLUMN_NAMES:
        assert re.search(rf"\b{re.escape(name)}\b", warn_lines[0]), name
    for name in ORDINARY_COLUMN_NAMES:
        assert not re.search(rf"\b{re.escape(name)}\b", warn_lines[0]), name

    with pytest.raises(AnalysisException) as refusal:
        refuse_frame = _flagged_csv_frame(spark, secrets_data.csv_path, "refuse")
        _finish_door(spark, refuse_frame, door, f"{FLAG_VIEW}_refuse_{door}")
    message = str(refusal.value)
    assert "flag_secret_columns" in message
    for name in FLAGGED_COLUMN_NAMES:
        assert re.search(rf"\b{re.escape(name)}\b", message), name
    for name in ORDINARY_COLUMN_NAMES:
        assert not re.search(rf"\b{re.escape(name)}\b", message), name


def test_secret_flag_bad_value_refuses_loud(
    spark: ReparkSession, secrets_data: FamilyOutput
) -> None:
    """A flag_secret_columns value outside off/warn/refuse refuses naming all three."""
    with pytest.raises(AnalysisException) as error:
        spark.read.option("flag_secret_columns", "bogus").csv(
            str(secrets_data.csv_path), header=True, inferSchema=True
        )
    message = str(error.value)
    assert "flag_secret_columns" in message
    for accepted in ("off", "warn", "refuse"):
        assert re.search(rf"\b{accepted}\b", message), accepted


def test_secret_flag_refused_on_reads_without_the_option_map(
    spark: ReparkSession, secrets_data: FamilyOutput
) -> None:
    """flag_secret_columns is a csv/json option; parquet and table reads refuse it loud."""
    with pytest.raises(AnalysisException, match="flag_secret_columns"):
        spark.read.option("flag_secret_columns", "warn").parquet(str(secrets_data.parquet_path))
    with pytest.raises(AnalysisException, match="flag_secret_columns"):
        spark.read.format("parquet").option("flag_secret_columns", "refuse").load(
            str(secrets_data.parquet_path)
        )


def test_secret_flag_refuse_names_flagged_columns_on_json(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """The flag reaches the JSON option-map reader and refuses with the same naming."""
    path = tmp_path / "flagged.json"
    path.write_text('{"id": 1, "password": "repark-fake-pw"}\n', encoding="utf-8")
    with pytest.raises(AnalysisException) as error:
        spark.read.option("flag_secret_columns", "refuse").json(str(path))
    message = str(error.value)
    assert "password" in message
    assert not re.search(r"\bid\b", message)
