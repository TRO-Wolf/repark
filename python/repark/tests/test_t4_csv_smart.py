"""r25 T4 — smartCsv + inference PROTOCOL pins (Arrow path: value AND type).

Charter: greylit Q1/Q5; ledger ``task/t4-csv-smart-ledger.md``.
Default ``spark.read.csv`` remains r20-R1 byte-identical (separate pin below).
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark._csv_smart import (
    RUNG_ORDER,
    detect_delimiter,
    prepare_messy_csv,
    resolve_cell_rung,
    resolve_column_type,
)
from repark.spark.session import _reset_active_session_for_tests


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """Isolated session (no AWS)."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-t4-csv-smart").getOrCreate()
    yield session
    session.stop()
    _reset_active_session_for_tests()


# ==================================================================================================
# Protocol pure pins (no engine)
# ==================================================================================================


def test_protocol_rung_order_frozen() -> None:
    """Ladder order is the greylit Q1 contract (deterministic SSOT)."""
    assert RUNG_ORDER == (
        "bool",
        "int32",
        "int64",
        "decimal128",
        "float64",
        "date",
        "timestamp",
        "string",
    )


def test_protocol_cell_rungs() -> None:
    """Per-cell least-general rung; 1/0 are int not bool; terminal string."""
    assert resolve_cell_rung("true") == "bool"
    assert resolve_cell_rung("FALSE") == "bool"
    assert resolve_cell_rung("1") == "int32"
    assert resolve_cell_rung("0") == "int32"
    assert resolve_cell_rung(str(2**31)) == "int64"
    assert resolve_cell_rung("1.5") == "decimal128"
    assert resolve_cell_rung("1,25") == "decimal128"  # euro comma decimal
    assert resolve_cell_rung("1e3") == "float64"
    assert resolve_cell_rung("2020-01-15") == "date"
    assert resolve_cell_rung("2020-01-15T12:30:00") == "timestamp"
    assert resolve_cell_rung("2020-01-15 12:30:00.123") == "timestamp"
    assert resolve_cell_rung("not-a-type") == "string"


def test_detect_delimiter_ds4_ragged_wide_beats_quoted_two_field_rival() -> None:
    """(a) DS-4 class: quoted mid-field rival must not beat a ragged 12-col split.

    Measured: ``,`` (1, 4, -12) over ``;`` (0, 0, 0). origin/main agreement-first
    elects ``;`` (naive (2, 7) vs (12, 4)).
    """
    header = ",".join(f"col_{index}" for index in range(12))
    full = ",".join(["1"] * 11 + ['"a;b"'])
    short = ",".join(["1"] * 9 + ['"c;d"'])
    lines = [header, full, full, full, short, short, short, short]
    assert detect_delimiter(lines) == ","
    assert detect_delimiter(lines, preferred=";") == ";"


def test_detect_delimiter_tsv_with_unquoted_commas_keeps_tab() -> None:
    """(b) 2-col TSV with commas in the text column stays tab.

    Measured: ``\\t`` (1, 4, -2) over ``,`` (1, 3, -3). Field-count-first elects
    ``,`` (3, 3) over ``\\t`` (2, 4).
    """
    lines = [
        "name\tnote",
        "alice\thello, world, more",
        "bob\tfoo, bar, baz",
        "carol\ta, b, c",
    ]
    assert detect_delimiter(lines) == "\t"


def test_detect_delimiter_headerless_tsv_with_unquoted_commas_keeps_tab() -> None:
    """Headerless (b): both join; agr tie; ``-mode`` picks tab.

    Measured: ``\\t`` (1, 3, -2) over ``,`` (1, 3, -3). Rank-truncation mutant
    (drop third slot) elects ``,`` by candidate order.
    """
    lines = [
        "alice\thello, world, more",
        "bob\tfoo, bar, baz",
        "carol\ta, b, c",
    ]
    assert detect_delimiter(lines) == "\t"


def test_detect_delimiter_two_column_semicolon_beats_wider_comma() -> None:
    """(c) 2-col ``;`` file with commas in the text column stays ``;``.

    Measured: ``;`` (1, 4, -2) over ``,`` (1, 3, -3).
    """
    lines = [
        "id;note",
        "1;a,b,c",
        "2;d,e,f",
        "3;g,h,i",
    ]
    assert detect_delimiter(lines) == ";"


def test_detect_delimiter_headerless_two_column_semicolon_beats_wider_comma() -> None:
    """Headerless (c): both join; agr tie; ``-mode`` picks ``;``.

    Measured: ``;`` (1, 3, -2) over ``,`` (1, 3, -3).
    """
    lines = [
        "1;a,b,c",
        "2;d,e,f",
        "3;g,h,i",
    ]
    assert detect_delimiter(lines) == ";"


def test_detect_delimiter_quoted_pipe_list_does_not_elect_pipe() -> None:
    """(d) Headed quoted pipe-list: comma joins, pipe is unusable.

    Measured: ``,`` (1, 4, -3) over ``|`` (0, 0, 0). Join decides before quoting
    is consulted — not the quote-blind discriminator (see A7 pin).
    """
    lines = [
        "id,note,extra",
        '1,"a|b|c|d",x',
        '2,"e|f|g|h",y',
        '3,"i|j|k|l",z',
    ]
    assert detect_delimiter(lines) == ","


def test_detect_delimiter_honest_small_file_wide_rival_does_not_decide() -> None:
    """(e) Honest corpus ``[id,name / 1;2;3;4;5]``.

    Measured: ``,`` (0, 1, -2) over ``;`` (0, 1, -5). origin/main and round-1
    field-count-first elect ``;`` (mode 5 vs 2, agr tie). The old (e) pin with
    ``id,name`` + ``1,ok`` + ``x;y;z;w;v`` scored ``;`` as (0,0,0) via mode<2.
    """
    assert detect_delimiter(["id,name", "1;2;3;4;5"]) == ","


def test_detect_delimiter_header_join_beats_unquoted_data_commas() -> None:
    """Unquoted data commas must not beat a ``;`` structural header-join.

    Measured: ``;`` (1, 4, -5) over ``,`` (0, 7, -3).
    """
    lines = [
        "col_a;col_b;col_c;col_d;col_e",
        "1;1,234;760,35;x;y",
        "2;2,345;12,00;x;y",
        "3;3,456;99,50;x;y",
        "4;4,567;11,00;x",
        "5;5,678;22,00;x",
        "6;6,789;33,00;x",
        "7;7,890;44,00;x",
    ]
    assert detect_delimiter(lines) == ";"


def test_detect_delimiter_tsv_forgery_empty_leading_cell_keeps_tab() -> None:
    """A later ``\\tblue,gold`` row must not mint comma join.

    Measured: ``\\t`` (1, 4, -2) over ``,`` (0, 1, -2).
    """
    lines = [
        "name\tnote",
        "alice\thello",
        "\tblue,gold",
        "bob\tok",
    ]
    assert detect_delimiter(lines) == "\t"


def test_detect_delimiter_inch_mark_does_not_zero_semicolon() -> None:
    """C3: unbalanced quote falls back to plain split (probe A).

    Measured: ``;`` (1, 3, -3). Without the EOL fallback every later ``;`` is
    swallowed and the true delimiter goes unusable.
    """
    lines = [
        "id;size;n",
        '1;5" pipe;3',
        "2;ok;4",
    ]
    assert detect_delimiter(lines) == ";"


def test_detect_delimiter_inch_mark_on_every_row_keeps_semicolon() -> None:
    """C3 probe B: inch marks on every data row still leave ``;`` usable."""
    lines = [
        "id;size;n",
        '1;5" pipe;3',
        '2;3" nail;4',
        '3;1" bit;5',
    ]
    assert detect_delimiter(lines) == ";"


def test_detect_delimiter_one_comma_preamble_keeps_semicolon() -> None:
    """A2: one rival-bearing preamble must not elect comma.

    Measured: ``;`` (1, 4, -3) over ``,`` (0, 1, -2).
    """
    lines = [
        "Exported, 2026",
        "id;name;note",
        "1;a;b",
        "2;c;d",
        "3;e;f",
    ]
    assert detect_delimiter(lines) == ";"


def test_detect_delimiter_two_comma_preambles_keeps_semicolon() -> None:
    """A2: two rival-bearing preamble lines must not elect comma.

    Measured: ``;`` (1, 4, -3) over ``,`` (0, 2, -2).
    """
    lines = [
        "Exported, 2026",
        "note, v2",
        "id;name;note",
        "1;a;b",
        "2;c;d",
        "3;e;f",
    ]
    assert detect_delimiter(lines) == ";"


def test_detect_delimiter_quote_blind_ragged_comma_uniform_quoted_pipe() -> None:
    """Quote-blind mutant elects pipe; quote-aware elects comma.

    Measured quote-aware: ``,`` (0, 1, -5) over ``|`` (0, 0, 0).
    Measured quote-blind csv.reader: ``|`` (1, 3, -2) over ``,`` (0, 1, -5).
    """
    lines = [
        '1,"a|b",x,y',
        '2,"c|d",x',
        '3,"e|f",x,y,z',
    ]
    assert detect_delimiter(lines) == ","


def test_detect_delimiter_header_one_cell_not_bare_identifier() -> None:
    """One non-identifier header cell must not drop comma join."""
    for header in (
        "id,Amount (USD),note",
        "id,amount.usd,note",
        "id,1amount,note",
        "id,cantidad,note",
        "id,金額,note",
    ):
        lines = [header, "1,2,3", "4,5,6"]
        assert detect_delimiter(lines) == ",", header


def test_detect_delimiter_mode_below_two_is_unusable() -> None:
    """A candidate whose modal width is 1 stays (0,0,0); comma still wins.

    ``;`` never reaches width 2. Dropping the mode>=2 guard would let a
    1-field candidate compete on agreement of every line.
    """
    lines = ["a,b", "c,d", "e,f"]
    assert detect_delimiter(lines) == ","


def test_detect_delimiter_preferred_refuses_non_single_char() -> None:
    """Declared preferred: empty / multi-char / newline / CR / quote refuse."""
    lines = ["a,b", "1,2"]
    for bad in ("", "||", "sep", "\n", "\r", '"'):
        with pytest.raises(ValueError, match="single character"):
            detect_delimiter(lines, preferred=bad)
    assert detect_delimiter(lines, preferred=";") == ";"
    assert detect_delimiter(lines, preferred="\x01") == "\x01"


def test_parse_quoted_embedded_delimiter_unquotes(tmp_path: Path) -> None:
    """A1: quoted cell with embedded delimiter round-trips through prepare."""
    path = tmp_path / "q.csv"
    path.write_text('id,note\n1,"a,b"\n', encoding="utf-8")
    prepared = prepare_messy_csv(path)
    assert prepared.report.delimiter == ","
    assert prepared.rows[0][1] == "a,b"


def test_parse_escaped_quote_unescapes(tmp_path: Path) -> None:
    """A1: ``""`` inside a quoted cell unescapes to one ``"``."""
    path = tmp_path / "esc.csv"
    path.write_text('id,note\n1,"say ""hi"""\n', encoding="utf-8")
    prepared = prepare_messy_csv(path)
    assert prepared.rows[0][1] == 'say "hi"'


def test_parse_unbalanced_quote_keeps_raw_text(tmp_path: Path) -> None:
    """A1: C3-fallback line keeps raw plain-split text (no invented unquote)."""
    path = tmp_path / "inch.csv"
    path.write_text('id;size;n\n1;5" pipe;3\n', encoding="utf-8")
    prepared = prepare_messy_csv(path, sep=";")
    assert prepared.rows[0] == ["1", '5" pipe', "3"]


def test_parse_preserves_leading_trailing_whitespace(tmp_path: Path) -> None:
    """A1: whitespace is not stripped (csv.reader skipinitialspace=False)."""
    path = tmp_path / "ws.csv"
    path.write_text("id,note\n1,  hi  \n", encoding="utf-8")
    prepared = prepare_messy_csv(path)
    assert prepared.rows[0][1] == "  hi  "


def test_protocol_column_promotion_and_nulls() -> None:
    """Column type = most general rung; nulls do not constrain; all-null → string."""
    assert resolve_column_type(["1", "2", "3"]).rung == "int32"
    assert resolve_column_type(["true", "false", "t"]).rung == "bool"
    assert resolve_column_type(["1", "2.5"]).rung == "decimal128"
    assert resolve_column_type(["1e10", "2"]).rung == "float64"
    assert resolve_column_type(["2020-01-01", "2020-01-02"]).rung == "date"
    mixed = resolve_column_type(["2020-01-01", "2020-01-01T00:00:00"])
    assert mixed.rung == "timestamp"
    assert resolve_column_type([None, "", "null", "NA"]).rung == "string"
    # Determinism: same cells → same rung twice.
    cells = ["10", "20", "30"]
    assert resolve_column_type(cells).rung == resolve_column_type(list(cells)).rung


# ==================================================================================================
# smartCsv integration — messy fixtures, value + type on Arrow path
# ==================================================================================================


def test_smart_csv_messy_preamble_bom_types(spark: ReparkSession, tmp_path: Path) -> None:
    """Preamble junk + BOM skipped; protocol types on Arrow (value AND type)."""
    path = tmp_path / "messy.csv"
    # UTF-8 BOM + two junk lines then a real header table.
    body = (
        "\ufeffNOTE: export dump v1\n"
        "# not,csv,header\n"
        "id,flag,amount,when,note\n"
        "1,true,1.50,2020-01-15,hi\n"
        "2,false,3.00,2020-01-16,\n"
        "3,true,4.25,2020-01-17,x\n"
    )
    path.write_text(body, encoding="utf-8")

    frame = spark.read.smartCsv(str(path))
    report = frame.describe_ingest()
    assert report["source"] == "smartCsv"
    assert report["bom_stripped"] is True
    assert report["skipped_lines"] == 2
    assert report["header_row_index"] == 2
    assert report["delimiter"] == ","
    assert report["data_row_count"] == 3

    table = frame.orderBy("id").to_arrow()
    # Types (Arrow path)
    assert table.schema.field("id").type == pa.int32()
    assert table.schema.field("flag").type == pa.bool_()
    assert pa.types.is_decimal(table.schema.field("amount").type)
    assert table.schema.field("when").type == pa.date32()
    assert pa.types.is_string(table.schema.field("note").type) or pa.types.is_large_string(
        table.schema.field("note").type
    )
    # Values
    rows = table.to_pylist()
    assert rows[0]["id"] == 1
    assert rows[0]["flag"] is True
    assert rows[0]["amount"] == Decimal("1.50")
    assert rows[0]["when"] == date(2020, 1, 15)
    assert rows[0]["note"] == "hi"
    assert rows[1]["note"] is None
    assert rows[2]["amount"] == Decimal("4.25")

    by_name = {col["name"]: col for col in report["columns"]}
    assert by_name["id"]["resolved_type"] == "int32"
    assert by_name["flag"]["resolved_type"] == "bool"
    assert by_name["amount"]["resolved_type"].startswith("decimal128")
    assert by_name["when"]["resolved_type"] == "date"
    assert by_name["note"]["resolved_type"] == "string"
    assert by_name["note"]["fallback_count"] >= 1


def test_smart_csv_ragged_rows_and_diagnostics(spark: ReparkSession, tmp_path: Path) -> None:
    """Ragged rows null-padded; pad count surfaceable (no silent magic)."""
    path = tmp_path / "ragged.csv"
    path.write_text("a,b,c\n1,2,3\n4,5\n6,7,8,extra\n", encoding="utf-8")
    frame = spark.read.smartCsv(str(path))
    report = frame.describe_ingest()
    assert report["ragged_rows_padded"] >= 1
    rows = frame.orderBy("a").to_arrow().to_pylist()
    # Short row → c is null
    short = next(row for row in rows if row["a"] == 4)
    assert short["b"] == 5
    assert short["c"] is None


def test_smart_csv_semicolon_and_int64(spark: ReparkSession, tmp_path: Path) -> None:
    """Delimiter auto-detect (semicolon) + int64 beyond int32 range."""
    path = tmp_path / "semi.csv"
    big = str(2**31)  # 2147483648 → int64
    path.write_text(f"id;qty\n1;{big}\n2;10\n", encoding="utf-8")
    frame = spark.read.smartCsv(str(path))
    table = frame.orderBy("id").to_arrow()
    assert table.schema.field("qty").type == pa.int64()
    assert table.to_pylist()[0]["qty"] == 2**31
    assert frame.describe_ingest()["delimiter"] == ";"


def test_smart_csv_timestamp_and_float_scientific(spark: ReparkSession, tmp_path: Path) -> None:
    """Timestamp + scientific float64 on Arrow path."""
    path = tmp_path / "ts.csv"
    path.write_text(
        "ts,mag\n2020-01-15T12:30:00,1.5e2\n2020-01-16 00:00:00,3e0\n",
        encoding="utf-8",
    )
    frame = spark.read.smartCsv(str(path))
    table = frame.to_arrow()
    assert pa.types.is_timestamp(table.schema.field("ts").type)
    assert table.schema.field("mag").type == pa.float64()
    rows = table.to_pylist()
    assert isinstance(rows[0]["ts"], datetime)
    assert rows[0]["mag"] == pytest.approx(150.0)


def test_smart_csv_header_normalize_opt_in(spark: ReparkSession, tmp_path: Path) -> None:
    """Header case normalization is OPT-IN (never silent rename by default)."""
    path = tmp_path / "hdr.csv"
    path.write_text("UserId,FullName\n1,a\n", encoding="utf-8")
    default = spark.read.smartCsv(str(path))
    assert default.columns == ["UserId", "FullName"]
    assert default.describe_ingest()["header_normalized"] is False

    lower = spark.read.smartCsv(str(path), normalizeHeaderCase="lower")
    assert lower.columns == ["userid", "fullname"]
    assert lower.describe_ingest()["header_normalized"] is True

    snake = spark.read.smartCsv(str(path), normalizeHeaderCase="snake")
    assert snake.columns == ["user_id", "full_name"]


def test_smart_csv_describe_ingest_empty_on_plain_csv(spark: ReparkSession, tmp_path: Path) -> None:
    """Ordinary read.csv frames have no smart diagnostics (empty describe_ingest)."""
    path = tmp_path / "plain.csv"
    path.write_text("id,name\n1,a\n", encoding="utf-8")
    frame = spark.read.csv(str(path), header=True, inferSchema=True)
    assert frame.describe_ingest() == {}


def test_default_csv_still_r20_r1_all_string_when_infer_false(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """Default .csv with inferSchema=false stays all-string (r20-R1 pin shape)."""
    path = tmp_path / "s.csv"
    path.write_text("id,name\n1,a\n", encoding="utf-8")
    frame = spark.read.option("header", "true").option("inferSchema", "false").csv(str(path))
    table = frame.to_arrow()
    assert pa.types.is_string(table.schema.field("id").type) or pa.types.is_large_string(
        table.schema.field("id").type
    )
    assert table.to_pylist() == [{"id": "1", "name": "a"}]


def test_default_csv_header_values_unchanged(spark: ReparkSession, tmp_path: Path) -> None:
    """Default .csv header+inferSchema value pin (r20-R1 regression guard)."""
    path = tmp_path / "t.csv"
    path.write_text("id,name\n1,a\n2,b\n", encoding="utf-8")
    frame = spark.read.csv(str(path), header=True, inferSchema=True)
    rows = frame.orderBy("id").to_arrow().to_pylist()
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_smart_csv_custom_null_value_and_space_header(spark: ReparkSession, tmp_path: Path) -> None:
    """Custom nullValue token + mixed-case / space headers (quoted selectExpr path)."""
    path = tmp_path / "space.csv"
    path.write_text("Full Name,flag\nalice,ZZ\nbob,true\n", encoding="utf-8")
    frame = spark.read.smartCsv(str(path), nullValue="ZZ")
    report = frame.describe_ingest()
    assert "zz" in report["null_tokens"]
    table = frame.orderBy("Full Name").to_arrow()
    rows = table.to_pylist()
    assert rows[0] == {"Full Name": "alice", "flag": None}
    assert rows[1]["flag"] is True
    assert table.schema.field("flag").type == pa.bool_()


def test_smart_csv_duplicate_headers_deduped(spark: ReparkSession, tmp_path: Path) -> None:
    """Duplicate header names get deterministic suffixes (a, a_2, …)."""
    path = tmp_path / "dup.csv"
    path.write_text("a,a,b\n1,2,3\n", encoding="utf-8")
    frame = spark.read.smartCsv(str(path))
    assert frame.columns == ["a", "a_2", "b"]
    assert frame.to_arrow().to_pylist() == [{"a": 1, "a_2": 2, "b": 3}]


def test_protocol_int64_overflow_falls_to_decimal_or_string() -> None:
    """Values outside int64 range leave the int rung (decimal or higher)."""
    overflow = str(2**63)  # one past int64 max
    rung = resolve_column_type([overflow]).rung
    assert rung in {"decimal128", "float64", "string"}
    assert rung != "int64"
    assert resolve_column_type([str(2**63 - 1)]).rung == "int64"


# --- r26 T2: decimal union + sampling --------------------------------------------------------


def test_decimal_union_mixed_int_digits_max_not_max_precision(spark: Any, tmp_path: Path) -> None:
    """19.99 + 250 + 3.5 → decimal(5,2), not decimal(4,2); Arrow values pin."""
    path = tmp_path / "mixed_decimal.csv"
    path.write_text("amount\n19.99\n250\n3.5\n", encoding="utf-8")
    frame = spark.read.smartCsv(str(path), header=True)
    table = frame.toArrow()
    field = table.schema.field("amount")
    assert pa.types.is_decimal(field.type)
    assert field.type.precision == 5
    assert field.type.scale == 2
    values = [row.as_py() for row in table.column("amount")]
    assert values == [Decimal("19.99"), Decimal("250.00"), Decimal("3.50")]


def test_decimal_union_widening_pure_fraction_and_int(spark: Any, tmp_path: Path) -> None:
    """0.001 + 12345 → decimal(8,3) (int_digits 5 + scale 3)."""
    path = tmp_path / "wide_decimal.csv"
    path.write_text("amount\n0.001\n12345\n", encoding="utf-8")
    frame = spark.read.smartCsv(str(path), header=True)
    table = frame.toArrow()
    field = table.schema.field("amount")
    assert pa.types.is_decimal(field.type)
    assert field.type.precision == 8
    assert field.type.scale == 3


def test_decimal_union_pure_fraction_alone() -> None:
    """0.001 alone → decimal(3,3); integer digits 0."""
    resolution = resolve_column_type(["0.001"])
    assert resolution.rung == "decimal128"
    assert resolution.decimal_precision == 3
    assert resolution.decimal_scale == 3


def test_decimal_union_uniform_scale_regression() -> None:
    """19.99 / 250.00 / 3.50 still decimal(5,2)."""
    resolution = resolve_column_type(["19.99", "250.00", "3.50"])
    assert resolution.rung == "decimal128"
    assert resolution.decimal_precision == 5
    assert resolution.decimal_scale == 2


def test_decimal_precision_over_38_promotes_float64() -> None:
    """p > 38 → float64 rung (protocol ladder)."""
    # 40 integer digits
    huge = "1" + ("0" * 39)
    resolution = resolve_column_type([huge + ".1", "1.1"])
    assert resolution.rung == "float64"
    assert resolution.decimal_precision is None


def test_decimal_signs_and_leading_zeros_stripped() -> None:
    """Signs / leading zeros do not inflate integer digits."""
    resolution = resolve_column_type(["-019.99", "+250"])
    assert resolution.rung == "decimal128"
    assert resolution.decimal_precision == 5
    assert resolution.decimal_scale == 2


def test_scientific_stays_off_decimal_rung() -> None:
    """Scientific notation excluded from decimal; falls through to float64."""
    assert resolve_cell_rung("1.5e3") == "float64"
    resolution = resolve_column_type(["1.5e3", "2.0e1"])
    assert resolution.rung == "float64"


def test_sampling_cap_describe_ingest_keys(spark: Any, tmp_path: Path) -> None:
    """describe_ingest exposes inference_rows_scanned / inference_capped / sampling_rows_limit."""
    path = tmp_path / "sample.csv"
    path.write_text("x\n1\n2\n3\n", encoding="utf-8")
    frame = spark.read.smartCsv(str(path), header=True)
    report = frame.describe_ingest()
    assert report["inference_rows_scanned"] == 3
    assert report["inference_capped"] is False
    assert report["sampling_rows_limit"] == 3  # effective limit when file smaller


def test_sampling_rows_zero_loud_refuse(spark: Any, tmp_path: Path) -> None:
    """samplingRows <= 0 → IllegalArgumentException."""
    from repark.errors import IllegalArgumentException

    path = tmp_path / "sample.csv"
    path.write_text("x\n1\n", encoding="utf-8")
    try:
        spark.read.smartCsv(str(path), header=True, samplingRows=0)
        raise AssertionError("expected IllegalArgumentException")
    except IllegalArgumentException as exc:
        assert "samplingRows" in str(exc)


def test_sampling_cap_limits_inference_rows(spark: Any, tmp_path: Path) -> None:
    """With samplingRows=2, inference scans 2 rows; full data still loaded (3 rows)."""
    path = tmp_path / "cap.csv"
    path.write_text("amount\n1.5\n2.5\n3.5\n", encoding="utf-8")
    frame = spark.read.smartCsv(str(path), header=True, samplingRows=2)
    report = frame.describe_ingest()
    assert report["inference_rows_scanned"] == 2
    assert report["inference_capped"] is True
    assert report["sampling_rows_limit"] == 2
    assert frame.count() == 3


def test_sampling_rows_via_option_map(spark: Any, tmp_path: Path) -> None:
    """option("samplingRows", N) must honor the same cap as kwargs (octo C1-Q-001)."""
    path = tmp_path / "opt_cap.csv"
    path.write_text("amount\n1.5\n2.5\n3.5\n", encoding="utf-8")
    frame = spark.read.option("samplingRows", 2).smartCsv(str(path), header=True)
    report = frame.describe_ingest()
    assert report["inference_rows_scanned"] == 2
    assert report["inference_capped"] is True
    assert report["sampling_rows_limit"] == 2
    assert frame.count() == 3


def test_smart_csv_sep_refuses_non_single_char(spark: Any, tmp_path: Path) -> None:
    """smartCsv sep= empty / multi-char / newline / CR / quote refuse loud."""
    from repark.errors import IllegalArgumentException

    path = tmp_path / "sep.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")
    for bad in ("", "||", "sep", "\n", "\r", '"'):
        with pytest.raises(IllegalArgumentException, match="single character"):
            spark.read.smartCsv(str(path), sep=bad)


def test_smart_csv_option_empty_sep_refuses_and_does_not_fall_through(
    spark: Any, tmp_path: Path
) -> None:
    """option('sep','') refuses; it must not fall through to option('delimiter', ';')."""
    from repark.errors import IllegalArgumentException

    path = tmp_path / "sep.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")
    with pytest.raises(IllegalArgumentException, match="single character"):
        spark.read.option("sep", "").smartCsv(str(path))
    with pytest.raises(IllegalArgumentException, match="single character"):
        spark.read.option("sep", "").option("delimiter", ";").smartCsv(str(path))
    frame = spark.read.option("sep", ",").smartCsv(str(path), header=True)
    assert frame.columns == ["a", "b"]
    frame_control = spark.read.smartCsv(str(path), sep="\x01")
    assert frame_control.describe_ingest()["delimiter"] == "\x01"


def test_sampling_rows_empty_string_loud_refuse(spark: Any, tmp_path: Path) -> None:
    """Empty samplingRows is LOUD refuse (not silent default)."""
    from repark.errors import IllegalArgumentException

    path = tmp_path / "sample.csv"
    path.write_text("x\n1\n", encoding="utf-8")
    try:
        spark.read.option("samplingRows", "").smartCsv(str(path), header=True)
        raise AssertionError("expected IllegalArgumentException")
    except IllegalArgumentException as exc:
        assert "samplingRows" in str(exc)


def test_sampling_rows_non_integral_float_loud_refuse(spark: Any, tmp_path: Path) -> None:
    """Non-integral samplingRows must refuse (no silent truncation)."""
    from repark.errors import IllegalArgumentException

    path = tmp_path / "sample.csv"
    path.write_text("x\n1\n", encoding="utf-8")
    for bad in (2.5, "2.5", 1.1):
        try:
            spark.read.smartCsv(str(path), header=True, samplingRows=bad)
            raise AssertionError(f"expected refuse for {bad!r}")
        except IllegalArgumentException as exc:
            assert "samplingRows" in str(exc)


def test_decimal_union_order_independent_int_before_fraction() -> None:
    """Integer cells before first fraction must still widen int_digits (octo C3-Q-001)."""
    forward = resolve_column_type(["19.99", "250", "3.5"])
    reverse = resolve_column_type(["250", "19.99", "3.5"])
    int_first = resolve_column_type(["100", "200", "1.5"])
    frac_first = resolve_column_type(["1.5", "100", "200"])
    for resolution in (forward, reverse):
        assert resolution.rung == "decimal128"
        assert resolution.decimal_precision == 5
        assert resolution.decimal_scale == 2
    assert int_first.rung == "decimal128"
    assert int_first.decimal_precision == 4  # int_digits 3 + scale 1
    assert int_first.decimal_scale == 1
    assert frac_first.decimal_precision == int_first.decimal_precision
    assert frac_first.decimal_scale == int_first.decimal_scale


def test_decimal_union_order_independent_arrow_path(spark: Any, tmp_path: Path) -> None:
    """Arrow path: 250 before 19.99 still decimal(5,2) and stores 250.00."""
    path = tmp_path / "order.csv"
    path.write_text("amount\n250\n19.99\n3.5\n", encoding="utf-8")
    frame = spark.read.smartCsv(str(path), header=True)
    table = frame.toArrow()
    field = table.schema.field("amount")
    assert pa.types.is_decimal(field.type)
    assert field.type.precision == 5
    assert field.type.scale == 2
    values = [row.as_py() for row in table.column("amount")]
    assert values == [Decimal("250.00"), Decimal("19.99"), Decimal("3.50")]


def test_sampling_rows_bool_loud_refuse(spark: Any, tmp_path: Path) -> None:
    """bool samplingRows must refuse (bool is a subclass of int in Python)."""
    from repark.errors import IllegalArgumentException

    path = tmp_path / "sample.csv"
    path.write_text("x\n1\n", encoding="utf-8")
    for bad in (True, False):
        try:
            spark.read.smartCsv(str(path), header=True, samplingRows=bad)
            raise AssertionError(f"expected refuse for {bad!r}")
        except IllegalArgumentException as exc:
            assert "samplingRows" in str(exc)
