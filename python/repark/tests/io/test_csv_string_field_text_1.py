from __future__ import annotations

import gzip
from collections.abc import Callable, Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark.session import _reset_active_session_for_tests
from repark.spark.types import DecimalType, StringType, StructField, StructType

pytestmark = pytest.mark.xdist_group("csv_string_field_text_1")
LATE_ROWS = "".join(f"{index},k\n" for index in range(1, 1201)) + "abc,k\n1.5,k\n"
FILES: dict[str, bytes] = {
    "card.csv": b"007\n1.50\n1e3\nTRUE\n",
    "card_h.csv": b"x\n007\n1.50\n1e3\nTRUE\n",
    "infer.csv": b"x,k\n007,abc\n",
    "n1.csv": b"1e3,2.0\n1.5,7\n3,4\n",
    "mis.csv": b"abc,k1\n2147483648,k2\n42,k3\n 7 ,k4\n1e3,k5\n",
    "swap.csv": b"k,x\nabc,007\n",
    "part.csv": b"x,z\n007,abc\n",
    "emptyname.csv": b",k\n007,k1\n",
    "case.csv": b"X,K\n007,abc\n",
    "w3h.csv": b"a,b,c\n007,abc,zz\n",
    "w2h.csv": b"x,k\n007,abc\n",
    "w3.csv": b"007,abc,zz\n",
    "w2.csv": b"007,abc\n",
    "late.csv": LATE_ROWS.encode(),
    "nullname.csv": b"NA,k\n007,NA\n",
    "untrimmed.csv": b" x , k \n007,abc\n",
    "gz.csv.gz": gzip.compress(b"x,k\n007,1e3\n"),
    "semi.csv": b"x;k\n007;1e3\n",
    "bom.csv": b"\xef\xbb\xbfx,k\n007,1e3\n",
    "glob/a.csv": b"x,k\n007,k1\n",
    "glob/b.csv": b"x,k\n1.50,k2\n",
    "two/a.csv": b"x,k\n007,k1\n",
    "two/b.csv": b"k,x\nk2,1e3\nk3,TRUE\n",
}
LATE = ("count(*) AS n", "count(x) AS nx", "max(CAST(x AS STRING)) AS mx")
CARD_TEXT = [("007",), ("1.50",), ("1e3",), ("TRUE",)]
S2 = ["string", "string"]
Read = Callable[[Any, Path], Any]


def header(read: Any) -> Any:
    return read.option("header", "true")


CELLS: dict[str, tuple[Read, list[str], list[str], list[tuple[Any, ...]]]] = {
    "card/default": (lambda r, p: r.csv(str(p / "card.csv")), ["_c0"], ["string"], CARD_TEXT),
    "card/infer_false": (
        lambda r, p: r.option("inferSchema", "false").csv(str(p / "card.csv")),
        ["_c0"],
        ["string"],
        CARD_TEXT,
    ),
    "card/string": (
        lambda r, p: r.schema("x STRING").csv(str(p / "card.csv")),
        ["x"],
        ["string"],
        CARD_TEXT,
    ),
    "card/load": (
        lambda r, p: r.format("csv").schema("x STRING").load(str(p / "card.csv")),
        ["x"],
        ["string"],
        CARD_TEXT,
    ),
    "card/header_infer_true": (
        lambda r, p: header(r.schema("x STRING").option("inferSchema", "true")).csv(
            str(p / "card_h.csv")
        ),
        ["x"],
        ["string"],
        CARD_TEXT,
    ),
    "infer/string": (
        lambda r, p: header(r.schema("x STRING, k STRING").option("inferSchema", "true")).csv(
            str(p / "infer.csv")
        ),
        ["x", "k"],
        S2,
        [("007", "abc")],
    ),
    "infer/int": (
        lambda r, p: header(r.schema("x INT, k INT").option("inferSchema", "true")).csv(
            str(p / "infer.csv")
        ),
        ["x", "k"],
        ["int", "int"],
        [(7, None)],
    ),
    "n1": (
        lambda r, p: r.schema("a INT, b BIGINT").csv(str(p / "n1.csv")),
        ["a", "b"],
        ["int", "bigint"],
        [(3, 4), (None, 7), (None, None)],
    ),
    "misfit/int": (
        lambda r, p: r.schema("x INT, k STRING").csv(str(p / "mis.csv")),
        ["x", "k"],
        ["int", "string"],
        [(42, "k3"), (None, "k1"), (None, "k2"), (None, "k4"), (None, "k5")],
    ),
    "misfit/boolean": (
        lambda r, p: r.schema("x BOOLEAN, k STRING").csv(str(p / "mis.csv")),
        ["x", "k"],
        ["boolean", "string"],
        [(None, "k1"), (None, "k2"), (None, "k3"), (None, "k4"), (None, "k5")],
    ),
    "misfit/double": (
        lambda r, p: r.schema("x DOUBLE, k STRING").csv(str(p / "mis.csv")),
        ["x", "k"],
        ["double", "string"],
        [(1000.0, "k5"), (2147483648.0, "k2"), (42.0, "k3"), (7.0, "k4"), (None, "k1")],
    ),
    "misfit/decimal": (
        lambda r, p: r.schema("x DECIMAL(10,2), k STRING").csv(str(p / "mis.csv")),
        ["x", "k"],
        ["decimal(10,2)", "string"],
        [
            (Decimal("1000.00"), "k5"),
            (Decimal("42.00"), "k3"),
            (None, "k1"),
            (None, "k2"),
            (None, "k4"),
        ],
    ),
    "pair/swap_int": (
        lambda r, p: header(r.schema("x INT, k STRING")).csv(str(p / "swap.csv")),
        ["x", "k"],
        ["int", "string"],
        [(None, "007")],
    ),
    "pair/swap_string": (
        lambda r, p: header(r.schema("x STRING, k STRING")).csv(str(p / "swap.csv")),
        ["x", "k"],
        S2,
        [("abc", "007")],
    ),
    "pair/part_int": (
        lambda r, p: header(r.schema("x INT, k STRING")).csv(str(p / "part.csv")),
        ["x", "k"],
        ["int", "string"],
        [(7, "abc")],
    ),
    "pair/part_string": (
        lambda r, p: header(r.schema("x STRING, k STRING")).csv(str(p / "part.csv")),
        ["x", "k"],
        S2,
        [("007", "abc")],
    ),
    "pair/emptyname_schema": (
        lambda r, p: header(r.schema("x STRING, k STRING")).csv(str(p / "emptyname.csv")),
        ["x", "k"],
        S2,
        [("007", "k1")],
    ),
    "pair/case_string": (
        lambda r, p: header(r.schema("x STRING, k STRING")).csv(str(p / "case.csv")),
        ["x", "k"],
        S2,
        [("007", "abc")],
    ),
    "width/narrow_hdr": (
        lambda r, p: header(r.schema("x STRING, k STRING")).csv(str(p / "w3h.csv")),
        ["x", "k"],
        S2,
        [("007", "abc")],
    ),
    "width/narrow_hdr_int": (
        lambda r, p: header(r.schema("x INT, k STRING")).csv(str(p / "w3h.csv")),
        ["x", "k"],
        ["int", "string"],
        [(7, "abc")],
    ),
    "width/wide_hdr": (
        lambda r, p: header(r.schema("x STRING, k STRING, z STRING")).csv(str(p / "w2h.csv")),
        ["x", "k", "z"],
        ["string"] * 3,
        [("007", "abc", None)],
    ),
    "width/narrow_off": (
        lambda r, p: r.schema("x STRING, k STRING").csv(str(p / "w3.csv")),
        ["x", "k"],
        S2,
        [("007", "abc")],
    ),
    "width/wide_off": (
        lambda r, p: r.schema("x STRING, k STRING, z STRING").csv(str(p / "w2.csv")),
        ["x", "k", "z"],
        ["string"] * 3,
        [("007", "abc", None)],
    ),
    "width/wide_off_int": (
        lambda r, p: r.schema("x INT, k STRING, z INT").csv(str(p / "w2.csv")),
        ["x", "k", "z"],
        ["int", "string", "int"],
        [(7, "abc", None)],
    ),
    "late/string": (
        lambda r, p: r.schema("x STRING, k STRING").csv(str(p / "late.csv")).selectExpr(*LATE),
        ["n", "nx", "mx"],
        ["bigint", "bigint", "string"],
        [(1202, 1202, "abc")],
    ),
    "late/int": (
        lambda r, p: r.schema("x INT, k STRING").csv(str(p / "late.csv")).selectExpr(*LATE),
        ["n", "nx", "mx"],
        ["bigint", "bigint", "string"],
        [(1202, 1200, "999")],
    ),
    "late/none": (
        lambda r, p: r.csv(str(p / "late.csv")).withColumnRenamed("_c0", "x").selectExpr(*LATE),
        ["n", "nx", "mx"],
        ["bigint", "bigint", "string"],
        [(1202, 1202, "abc")],
    ),
    "names/empty": (
        lambda r, p: header(r).csv(str(p / "emptyname.csv")),
        ["_c0", "k"],
        S2,
        [("007", "k1")],
    ),
    "names/null_token": (
        lambda r, p: header(r).option("nullValue", "NA").csv(str(p / "nullname.csv")),
        ["_c0", "k"],
        S2,
        [("007", None)],
    ),
    "names/untrimmed": (
        lambda r, p: header(r).csv(str(p / "untrimmed.csv")),
        [" x ", " k "],
        S2,
        [("007", "abc")],
    ),
    "names/untrimmed_schema": (
        lambda r, p: header(r.schema("x STRING, k STRING")).csv(str(p / "untrimmed.csv")),
        ["x", "k"],
        S2,
        [("007", "abc")],
    ),
    "topo/file_url": (
        lambda r, p: header(r).csv((p / "infer.csv").absolute().as_uri()),
        ["x", "k"],
        S2,
        [("007", "abc")],
    ),
    "topo/file_url_schema": (
        lambda r, p: header(r.schema("x STRING, k STRING")).csv(
            (p / "infer.csv").absolute().as_uri()
        ),
        ["x", "k"],
        S2,
        [("007", "abc")],
    ),
    "topo/gzip": (
        lambda r, p: header(r).option("compression", "gzip").csv(str(p / "gz.csv.gz")),
        ["x", "k"],
        S2,
        [("007", "1e3")],
    ),
    "topo/sep": (
        lambda r, p: header(r).option("sep", ";").csv(str(p / "semi.csv")),
        ["x", "k"],
        S2,
        [("007", "1e3")],
    ),
    "topo/bom": (
        lambda r, p: header(r).csv(str(p / "bom.csv")),
        ["x", "k"],
        S2,
        [("007", "1e3")],
    ),
    "topo/glob": (
        lambda r, p: header(r).csv(str(p / "glob" / "*.csv")),
        ["x", "k"],
        S2,
        [("007", "k1"), ("1.50", "k2")],
    ),
    "topo/glob_schema": (
        lambda r, p: header(r.schema("x STRING, k STRING")).csv(str(p / "glob" / "*.csv")),
        ["x", "k"],
        S2,
        [("007", "k1"), ("1.50", "k2")],
    ),
    "topo/two_schema": (
        lambda r, p: header(r.schema("x STRING, k STRING")).csv(str(p / "two")),
        ["x", "k"],
        S2,
        [("007", "k1"), ("k2", "1e3"), ("k3", "TRUE")],
    ),
    "topo/two_values": (
        lambda r, p: header(r).csv(str(p / "two")).toDF("p", "q"),
        ["p", "q"],
        S2,
        [("007", "k1"), ("k2", "1e3"), ("k3", "TRUE")],
    ),
    "empty_value/empty": (
        lambda r, p: header(r).option("emptyValue", "").csv(str(p / "infer.csv")),
        ["x", "k"],
        S2,
        [("007", "abc")],
    ),
    "user_text_scan": (
        lambda r, p: (
            header(r)
            .option("inferSchema", "true")
            .option("text_scan", "true")
            .csv(str(p / "infer.csv"))
        ),
        ["x", "k"],
        ["int", "string"],
        [(7, "abc")],
    ),
}
DECIMAL_2_5 = StructType([StructField("x", DecimalType(2, 5)), StructField("k", StringType())])
DECIMAL_5_M2 = StructType([StructField("x", DecimalType(5, -2)), StructField("k", StringType())])
REFUSED: dict[str, tuple[Any, str]] = {
    "decimal_2_5": (DECIMAL_2_5, "Decimal scale (5) cannot be greater than precision (2)."),
    "decimal_5_m2": (DECIMAL_5_M2, "[NEGATIVE_SCALE_DISALLOWED] Negative scale is not allowed"),
    "char": ("x CHAR(5), k STRING", "[UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING]"),
}
MISFIT_TEXT = [(" 7 ", "k4"), ("1e3", "k5"), ("2147483648", "k2"), ("42", "k3"), ("abc", "k1")]
LABEL: dict[str, list[tuple[Any, ...]]] = {
    "STRING": MISFIT_TEXT,
    "INT": CELLS["misfit/int"][3],
    "DECIMAL(10,2)": CELLS["misfit/decimal"][3],
}


def ordered(rows: list[Any]) -> list[tuple[Any, ...]]:
    return sorted((tuple(row) for row in rows), key=repr)


def answer(frame: Any) -> tuple[list[str], list[str], list[tuple[Any, ...]]]:
    return list(frame.columns), [kind for _, kind in frame.dtypes], ordered(frame.collect())


def write_label(session: Any, root: Path, ansi: str) -> dict[str, Any]:
    found: dict[str, Any] = {}
    session.sql("CREATE NAMESPACE IF NOT EXISTS lc.w").collect()
    for kind in LABEL:
        name = f"lc.w.t{kind.split('(')[0].lower()}{ansi}"
        source = session.read.schema(f"x {kind}, k STRING").csv(str(root / "mis.csv"))
        source.writeTo(f"{name}_wt").tableProperty("format-version", "2").create()
        found[f"write_to/{kind}"] = ordered(session.table(f"{name}_wt").collect())
        source.createOrReplaceTempView("src")
        session.sql(
            f"CREATE TABLE {name}_in (x {kind}, k STRING) USING iceberg "
            "TBLPROPERTIES ('format-version'='2')"
        ).collect()
        session.sql(f"INSERT INTO {name}_in SELECT * FROM src").collect()
        found[f"insert/{kind}"] = ordered(session.table(f"{name}_in").collect())
    return found


@pytest.fixture(scope="module", params=["true", "false"])
def measured(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[dict[str, Any]]:
    root = tmp_path_factory.mktemp(f"csv-string-field-text-{request.param}")
    for name, body in FILES.items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_bytes(body)
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-csv-string-field-text-1")
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", str(root / "lc"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", request.param)
        .getOrCreate()
    )
    found: dict[str, Any] = {"root": root, "session": session}
    for tag, (build, _, _, _) in CELLS.items():
        try:
            found[tag] = answer(build(session.read, root))
        except Exception as error:
            found[tag] = f"{type(error).__name__}: {error}"
    found["label"] = write_label(session, root, request.param)
    yield found
    session.stop()
    _reset_active_session_for_tests()


@pytest.mark.parametrize("tag", sorted(CELLS))
def test_a_csv_read_answers_spark_recorded_cell(measured: dict[str, Any], tag: str) -> None:
    _, names, kinds, rows = CELLS[tag]
    assert measured[tag] == (names, kinds, ordered(rows))


@pytest.mark.parametrize("write", ["write_to", "insert"])
@pytest.mark.parametrize("kind", sorted(LABEL))
def test_an_iceberg_v2_write_stores_the_field_spark_reads(
    measured: dict[str, Any], write: str, kind: str
) -> None:
    assert measured["label"][f"{write}/{kind}"] == ordered(LABEL[kind])


@pytest.mark.parametrize("shape", sorted(REFUSED))
def test_a_field_type_spark_refuses_is_refused_when_the_frame_is_planned(
    measured: dict[str, Any], shape: str
) -> None:
    schema, text = REFUSED[shape]
    reader = measured["session"].read.schema(schema).option("header", "true")
    with pytest.raises(AnalysisException) as raised:
        reader.csv(str(measured["root"] / "infer.csv"))
    assert text in str(raised.value)


def test_a_non_empty_empty_value_is_refused_by_name(measured: dict[str, Any]) -> None:
    reader = measured["session"].read.option("header", "true").option("emptyValue", "E")
    with pytest.raises(AnalysisException) as raised:
        reader.csv(str(measured["root"] / "infer.csv"))
    assert "'emptyValue' is not supported by repark yet" in str(raised.value)


def test_the_plan_parses_each_field_with_the_kernel_over_a_text_scan(
    measured: dict[str, Any],
) -> None:
    session = measured["session"]
    frame = session.read.schema("x INT, k STRING").csv(str(measured["root"] / "mis.csv"))
    frame.createOrReplaceTempView("plan_view")
    plan = " ".join(
        str(cell)
        for row in session.sql("EXPLAIN SELECT * FROM plan_view").collect()
        for cell in row
    )
    assert "__repark_csv_field__" in plan
    assert "CAST(" not in plan
