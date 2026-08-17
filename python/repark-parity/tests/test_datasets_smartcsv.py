"""Generator-shape and determinism pins for the smartCsv torture family (DS-3).

Pure pyarrow — no repark, no Spark, no JVM. The family grows the three-row inline
messy-CSV example in the facade suite into a generator-scale corpus: BOM, preamble
lines, ragged rows, a four-delimiter zoo, currency/decimal width variants, bool
spellings, duplicate headers and null-token variants.

CSV is the battleground; parquet is typed truth, and the A3 determinism pin is the
parquet table (not raw file bytes). The CSV deliberately does not naively round-trip
to the parquet — that gap is the torture. Facade reads land in DS-4.
"""

from __future__ import annotations

import csv
import importlib
import subprocess
import sys
import types
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark_parity import assert_frames_equal

_DATASETS_DIR = Path(__file__).resolve().parents[1] / "datasets"
_DATAGEN_PATH = _DATASETS_DIR / "smartcsv" / "datagen.py"


def _load_datasets() -> None:
    package_name = "repark_datasets"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(_DATASETS_DIR)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package


def _datagen() -> Any:
    _load_datasets()
    return importlib.import_module("repark_datasets.smartcsv.datagen")


def _parsed_rows(datagen: Any, text: str, delimiter: str) -> list[list[str]]:
    """Strip BOM + preamble, then parse with the scheme's delimiter (header first)."""
    body = text[len(datagen.BOM) :] if text.startswith(datagen.BOM) else text
    lines = body.splitlines()
    return list(csv.reader(lines[len(datagen.PREAMBLE_LINES) :], delimiter=delimiter))


def test_small_defaults_are_a9_64_and_42() -> None:
    datagen = _datagen()
    assert datagen.SMALL_ROWS == 64
    assert datagen.DEFAULT_SEED == 42
    assert datagen.DEFAULT_CLI_ROWS == 1_000_000
    table = datagen.small()
    assert table.num_rows == 64
    assert table.schema.equals(datagen.SCHEMA)


def test_small_same_seed_is_table_identical() -> None:
    datagen = _datagen()
    first = datagen.small(rows=64, seed=42)
    second = datagen.small(rows=64, seed=42)
    assert_frames_equal(first, second, order_sensitive=True)


def test_small_seed_change_moves_values() -> None:
    datagen = _datagen()
    first = datagen.small(rows=64, seed=42)
    second = datagen.small(rows=64, seed=0)
    with pytest.raises(Exception, match="mismatch"):
        assert_frames_equal(first, second, order_sensitive=True)


def test_manifest_column_classes_are_all_emitted_by_small() -> None:
    datagen = _datagen()
    manifest = datagen.load_manifest()
    assert manifest["family"] == "smartcsv"
    table = datagen.small()
    names = set(table.column_names)
    column_classes = [entry for entry in manifest["classes"] if entry["scope"] == "column"]
    assert {entry["column"] for entry in column_classes} == names
    for entry in column_classes:
        values = table.column(entry["column"]).to_pylist()
        assert any(value is not None for value in values), entry["id"]


def test_manifest_file_classes_are_all_visible_at_small_scale() -> None:
    """Every file-scoped class must be provable in the emitted text at 64 rows."""
    datagen = _datagen()
    file_classes = {
        entry["id"] for entry in datagen.load_manifest()["classes"] if entry["scope"] == "file"
    }
    assert file_classes == {
        "utf8_bom",
        "preamble_lines",
        "duplicate_header_row",
        "ragged_rows",
        "delimiter_zoo",
        "quoted_embedded_delimiter",
    }
    assert set(datagen.load_manifest()["delimiter_schemes"]) == set(datagen.DELIMITERS)

    for scheme, delimiter in datagen.DELIMITERS.items():
        text = datagen.render_csv(64, 42, scheme)
        # utf8_bom
        assert text.startswith(datagen.BOM), scheme
        # preamble_lines
        body = text[len(datagen.BOM) :]
        assert body.splitlines()[: len(datagen.PREAMBLE_LINES)] == list(datagen.PREAMBLE_LINES)
        # quoted_embedded_delimiter — the embedded value forces quoting in every scheme
        assert '"' in body, scheme
        rows = _parsed_rows(datagen, text, delimiter)
        header, data = rows[0], rows[1:]
        # duplicate_header_row
        assert header.count(datagen.DUPLICATE_HEADER_NAME) == 2, scheme
        assert header == list(datagen.CSV_HEADER)
        # ragged_rows — both directions present
        widths = {len(row) for row in data}
        assert len(header) in widths
        assert min(widths) == len(header) - datagen.SHORT_ROW_MISSING_CELLS, scheme
        assert max(widths) == len(header) + 1, scheme
        assert len(data) == 64


def test_delimiter_zoo_emits_one_file_per_scheme(tmp_path: Path) -> None:
    datagen = _datagen()
    written = datagen.write_files(rows=64, seed=42, out=tmp_path)
    assert set(datagen.DELIMITERS) == {"comma", "semicolon", "tab", "pipe"}
    for scheme, delimiter in datagen.DELIMITERS.items():
        path = written / datagen.csv_file_name(scheme)
        assert path.is_file(), scheme
        text = path.read_text(encoding="utf-8")
        assert text == datagen.render_csv(64, 42, scheme)
        rows = _parsed_rows(datagen, text, delimiter)
        assert rows[0] == list(datagen.CSV_HEADER)
        # Every scheme reconstructs the same embedded value carrying all four delimiters.
        embedded_index = list(datagen.CSV_HEADER).index("embedded_delims")
        assert rows[1][embedded_index] == "r0,s;t\tu|v"
    with pytest.raises(ValueError, match="delimiter scheme"):
        datagen.csv_file_name("colon")


def test_ragged_rows_null_the_trailing_columns() -> None:
    datagen = _datagen()
    table = datagen.small(rows=64, seed=42)
    short_indices = [index for index in range(64) if datagen.is_short_row(index)]
    long_indices = [index for index in range(64) if datagen.is_long_row(index)]
    assert short_indices == [5, 16, 27, 38, 49, 60]
    assert long_indices == [7, 20, 33, 46, 59]
    tail_1 = table.column("ragged_tail_1").to_pylist()
    tail_2 = table.column("ragged_tail_2").to_pylist()
    for index in range(64):
        if index in short_indices:
            assert tail_1[index] is None and tail_2[index] is None, index
        else:
            assert tail_1[index] == f"tail1-{index:04d}", index
            assert tail_2[index] == f"tail2-{index:04d}", index
    # Short wins over long on a row that qualifies for both (the first is 137).
    assert datagen.is_short_row(137) and not datagen.is_long_row(137)


def test_bool_spellings_and_non_bool_lookalikes() -> None:
    datagen = _datagen()
    text = datagen.render_csv(64, 42, "comma")
    rows = _parsed_rows(datagen, text, ",")
    flag_index = list(datagen.CSV_HEADER).index("flag")
    spelled = {row[flag_index] for row in rows[1:]}
    assert spelled == {token for token, _value in datagen.BOOL_SPELLINGS}
    table = datagen.small(rows=64, seed=42)
    assert table.schema.field("flag").type == pa.bool_()
    flags = table.column("flag").to_pylist()
    for index, (_token, expected) in enumerate(datagen.BOOL_SPELLINGS):
        assert flags[index] is expected, index
    yes_no = set(table.column("yes_no").to_pylist())
    assert yes_no == set(datagen.YES_NO_TOKENS)
    assert pa.types.is_string(table.schema.field("yes_no").type)


def test_null_token_variants_split_recognized_from_literal() -> None:
    datagen = _datagen()
    text = datagen.render_csv(64, 42, "comma")
    rows = _parsed_rows(datagen, text, ",")
    note_index = list(datagen.CSV_HEADER).index("nullable_note")
    emitted = {row[note_index] for row in rows[1:] if len(row) > note_index}
    assert emitted == set(datagen.NULL_TOKEN_CYCLE)
    notes = datagen.small(rows=64, seed=42).column("nullable_note").to_pylist()
    for index in range(len(datagen.NULL_TOKEN_CYCLE)):
        token = datagen.null_token_for(index)
        if token in datagen.RECOGNIZED_NULL_TOKENS:
            assert notes[index] is None, token
        else:
            assert notes[index] == token, token
    assert any(note is None for note in notes)
    assert any(note is not None for note in notes)


def test_currency_and_decimal_width_variants() -> None:
    datagen = _datagen()
    table = datagen.small(rows=64, seed=42)
    marks = {value[0] for value in table.column("amount_currency").to_pylist()}
    assert marks == set(datagen.CURRENCY_MARKS)
    assert any("," in value for value in table.column("amount_currency").to_pylist())
    widths = set(table.column("amount_wide").to_pylist())
    assert widths == set(datagen.DECIMAL_WIDTHS)
    assert all("," in value for value in table.column("euro_decimal").to_pylist())


def test_duplicate_header_columns_stay_distinct_in_typed_truth() -> None:
    datagen = _datagen()
    table = datagen.small(rows=64, seed=42)
    assert table.column("dup_label").to_pylist()[3] == "left-0003"
    assert table.column("dup_label_2").to_pylist()[3] == "right-0003"
    assert list(datagen.CSV_HEADER).count(datagen.DUPLICATE_HEADER_NAME) == 2


def test_write_parquet_matches_small(tmp_path: Path) -> None:
    datagen = _datagen()
    expected = datagen.small(rows=64, seed=42)
    written = datagen.write_files(rows=64, seed=42, out=tmp_path)
    parquet_path = written / datagen.DATA_PARQUET
    assert parquet_path.is_file()
    assert_frames_equal(datagen.read_parquet(parquet_path), expected, order_sensitive=True)


def test_cli_writes_under_out(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(_DATAGEN_PATH), "--rows", "8", "--seed", "42", "--out", str(tmp_path)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    datagen = _datagen()
    expected = datagen.small(rows=8, seed=42)
    assert_frames_equal(
        datagen.read_parquet(tmp_path / datagen.DATA_PARQUET),
        expected,
        order_sensitive=True,
    )
    for scheme in datagen.DELIMITERS:
        assert (tmp_path / datagen.csv_file_name(scheme)).is_file(), scheme


def test_refuse_rows_seed_and_scheme() -> None:
    datagen = _datagen()
    with pytest.raises(ValueError, match="rows"):
        datagen.small(rows=0, seed=42)
    with pytest.raises(ValueError, match="seed"):
        datagen.small(rows=8, seed=-1)
    with pytest.raises(ValueError, match="delimiter scheme"):
        datagen.render_csv(8, 42, "colon")
