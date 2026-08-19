"""FN-GT1 retro (GT1-FIX) — ColumnOrName wiring + oracle pins.

Live PySpark 4.1.2 oracle (2026-08-18): throwaway venv outside the repo
(``pyspark==4.1.2``, ``py4j==0.10.9.9``, CPython 3.12.3, OpenJDK 21).
Every expected value / type / NULL / signature claim below cites that oracle.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fn-gt1").getOrCreate()
    yield session
    session.stop()


def _table(frame: object) -> pa.Table:
    return frame.to_arrow()  # type: ignore[attr-defined]


def test_bin_hex_unhex(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.bin(F.lit(13)).alias("b"),
            F.hex(F.lit(17)).alias("h"),
            F.unhex(F.lit("48656C6C6F")).alias("u"),
        )
    )
    assert table.column("b").to_pylist() == ["1101"]
    assert table.schema.field("b").type in (pa.string(), pa.large_string())
    assert table.column("h").to_pylist() == ["11"]
    assert table.schema.field("h").type in (pa.string(), pa.large_string())
    raw = table.column("u").to_pylist()[0]
    assert bytes(raw) == b"Hello"
    assert table.schema.field("u").type in (pa.binary(), pa.large_binary())


def test_factorial_domain(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(0,), (5,), (20,), (21,), (-1,)], ["n"])
    table = _table(frame.select(F.factorial("n").alias("f")))
    assert table.column("f").to_pylist() == [1, 120, 2432902008176640000, None, None]
    assert table.schema.field("f").type == pa.int64()


def test_rint(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1.2,), (1.5,), (-1.5,), (2.5,)], ["x"])
    table = _table(frame.select(F.rint("x").alias("r")))
    assert table.column("r").to_pylist() == [1.0, 2.0, -2.0, 2.0]
    assert table.schema.field("r").type == pa.float64()


def test_width_bucket(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(frame.select(F.width_bucket(F.lit(5.0), F.lit(0.0), F.lit(10.0), 5).alias("w")))
    assert table.column("w").to_pylist() == [3]
    assert table.schema.field("w").type == pa.int64()
    named = spark.createDataFrame([(5.0, 0.0, 10.0, 5)], ["v", "mn", "mx", "n"])
    named_table = _table(named.select(F.width_bucket("v", "mn", "mx", "n").alias("w2")))
    assert named_table.column("w2").to_pylist() == [3]
    keyed = _table(
        frame.select(F.width_bucket(F.lit(5.0), F.lit(0.0), F.lit(10.0), numBucket=5).alias("wk"))
    )
    assert keyed.column("wk").to_pylist() == [3]


def test_bit_count_and_bit_get(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(7,), (6,)], ["x"])
    table = _table(
        frame.select(
            F.bit_count("x").alias("c"),
            F.bit_get("x", F.lit(1)).alias("g"),
            F.getbit("x", F.lit(1)).alias("a"),
        )
    )
    assert table.column("c").to_pylist() == [3, 2]
    assert table.column("g").to_pylist() == [1, 1]
    assert table.column("a").to_pylist() == table.column("g").to_pylist()
    assert table.schema.field("c").type == pa.int32()
    assert table.schema.field("g").type == pa.int8()
    assert table.schema.field("a").type == pa.int8()


def test_getbit_projects_getbit_name(spark: ReparkSession) -> None:
    """G7: F.getbit projects the live-PySpark name ``getbit(...)``, not ``bit_get``."""
    frame = spark.createDataFrame([(6,)], ["x"])
    table = _table(frame.select(F.getbit("x", F.lit(1)), F.bit_get("x", F.lit(1))))
    getbit_name, bit_get_name = table.schema.names
    assert getbit_name == "getbit(x, 1)"
    assert bit_get_name == "bit_get(x, 1)"
    assert table.column(getbit_name).to_pylist() == [1]
    assert table.column(bit_get_name).to_pylist() == [1]


def test_shifts(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.shiftleft(F.lit(2), 1).alias("l"),
            F.shiftright(F.lit(-2), 1).alias("r"),
            F.shiftrightunsigned(F.lit(8), 1).alias("u"),
        )
    )
    assert table.column("l").to_pylist() == [4]
    assert table.column("r").to_pylist() == [-1]
    assert table.column("u").to_pylist() == [4]
    keyed = _table(
        frame.select(
            F.shiftleft(F.lit(2), numBits=1).alias("kl"),
            F.shiftright(F.lit(-2), numBits=1).alias("kr"),
            F.shiftrightunsigned(F.lit(8), numBits=1).alias("ku"),
        )
    )
    assert keyed.column("kl").to_pylist() == [4]
    assert keyed.column("kr").to_pylist() == [-1]
    assert keyed.column("ku").to_pylist() == [4]


def test_shiftrightunsigned_negative_diverges(spark: ReparkSession) -> None:
    """P3: unsigned vs signed right-shift on a negative input (not the 8>>1 fixed point)."""
    frame = spark.createDataFrame([(-2,), (-8,)], ["x"])
    table = _table(
        frame.select(
            F.shiftright("x", 1).alias("sr"),
            F.shiftrightunsigned("x", 1).alias("sru"),
        )
    )
    assert table.column("sr").to_pylist() == [-1, -4]
    # Spark logical shift of int64: (-2)>>1 unsigned = 2^63-1, (-8)>>1 = 2^63-4.
    assert table.column("sru").to_pylist() == [9223372036854775807, 9223372036854775804]
    assert table.column("sru").to_pylist() != table.column("sr").to_pylist()
    assert table.schema.field("sru").type == pa.int64()
    assert table.schema.field("sr").type == pa.int64()


def test_split_part_and_regexp(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.split_part(F.lit("a.b.c"), F.lit("."), F.lit(2)).alias("p"),
            F.regexp_count(F.lit("ababab"), F.lit("ab")).alias("c"),
            F.regexp_instr(F.lit("abcde"), F.lit("c")).alias("i"),
        )
    )
    assert table.column("p").to_pylist() == ["b"]
    assert table.schema.field("p").type in (pa.string(), pa.large_string())
    assert table.column("c").to_pylist() == [3]
    assert table.schema.field("c").type == pa.int32()
    assert table.column("i").to_pylist() == [3]
    assert table.schema.field("i").type == pa.int32()
    keyed = _table(
        frame.select(F.split_part(F.lit("a.b.c"), F.lit("."), partNum=F.lit(2)).alias("pk"))
    )
    assert keyed.column("pk").to_pylist() == ["b"]


def test_regexp_count_str_is_column_name(spark: ReparkSession) -> None:
    """G1/P5: bare str pattern is a column name, not a literal (kills force-lit)."""
    frame = spark.createDataFrame([("ababab", "ab"), ("ababab", "xy")], ["str", "regexp"])
    col_table = _table(frame.select(F.regexp_count("str", "regexp").alias("c")))
    lit_table = _table(frame.select(F.regexp_count("str", F.lit("regexp")).alias("c")))
    assert col_table.column("c").to_pylist() == [3, 0]
    assert lit_table.column("c").to_pylist() == [0, 0]


def test_regexp_instr_str_is_column_name(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("ababab", "ab"), ("ababab", "xy")], ["str", "regexp"])
    col_table = _table(frame.select(F.regexp_instr("str", "regexp").alias("i")))
    lit_table = _table(frame.select(F.regexp_instr("str", F.lit("regexp")).alias("i")))
    assert col_table.column("i").to_pylist() == [1, 0]
    assert lit_table.column("i").to_pylist() == [0, 0]


def test_split_part_str_is_column_name(spark: ReparkSession) -> None:
    """G2/P5: delimiter and partNum are ColumnOrName."""
    frame = spark.createDataFrame(
        [("a.b.c", ".", 2), ("a-b-c", "-", 2)],
        ["src", "delimiter", "n"],
    )
    table = _table(frame.select(F.split_part("src", "delimiter", "n").alias("p")))
    assert table.column("p").to_pylist() == ["b", "b"]


def test_bit_get_pos_is_column_name(spark: ReparkSession) -> None:
    """G4/P5: pos is ColumnOrName — ``F.bit_get('x', 'pos')`` reads the column."""
    frame = spark.createDataFrame([(6, 1), (6, 0)], ["x", "pos"])
    table = _table(
        frame.select(
            F.bit_get("x", "pos").alias("g"),
            F.getbit("x", "pos").alias("a"),
        )
    )
    assert table.column("g").to_pylist() == [1, 0]
    assert table.column("a").to_pylist() == [1, 0]


def test_regexp_instr_idx_matches_live_spark(spark: ReparkSession) -> None:
    """G6: idx NULL-propagates; the value is ignored (always first-match start).

    Discriminator: ``b(c)d`` on ``abcde`` with idx=1 is 2 (match start), NOT 3
    (group-1 start). idx=99 / idx=0 match idx omitted. No match → 0.
    """
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.regexp_instr(F.lit("1a 2b 14m"), F.lit(r"\d+(a|b|m)")).alias("omitted"),
            F.regexp_instr(F.lit("1a 2b 14m"), F.lit(r"\d+(a|b|m)"), 0).alias("i0"),
            F.regexp_instr(F.lit("1a 2b 14m"), F.lit(r"\d+(a|b|m)"), 1).alias("i1"),
            F.regexp_instr(F.lit("1a 2b 14m"), F.lit(r"\d+(a|b|m)"), 99).alias("i99"),
            F.regexp_instr(F.lit("abcde"), F.lit("b(c)d"), 0).alias("g0"),
            F.regexp_instr(F.lit("abcde"), F.lit("b(c)d"), 1).alias("g1"),
            F.regexp_instr(F.lit("abcde"), F.lit("zzz")).alias("nomatch"),
            F.regexp_instr(F.lit("1a 2b 14m"), F.lit(r"\d+(a|b|m)"), F.lit(None).cast("int")).alias(
                "nidx"
            ),
        )
    )
    assert table.column("omitted").to_pylist() == [1]
    assert table.column("i0").to_pylist() == [1]
    assert table.column("i1").to_pylist() == [1]
    assert table.column("i99").to_pylist() == [1]
    assert table.column("g0").to_pylist() == [2]
    assert table.column("g1").to_pylist() == [2]
    assert table.column("nomatch").to_pylist() == [0]
    assert table.column("nidx").to_pylist() == [None]
    assert table.schema.field("i0").type == pa.int32()


def test_bit_length_and_octet_length(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.bit_length(F.lit("ab")).alias("b"),
            F.octet_length(F.lit("ab")).alias("o"),
            F.bit_length(F.lit("🐈")).alias("bc"),
            F.octet_length(F.lit("🐈")).alias("oc"),
            F.bit_length(F.unhex(F.lit("6162"))).alias("bbin"),
            F.octet_length(F.unhex(F.lit("6162"))).alias("obin"),
            F.octet_length(F.unhex(F.lit("C3"))).alias("oc3"),
            F.bit_length(F.unhex(F.lit("C3"))).alias("bc3"),
            F.bit_length(F.lit(12)).alias("bi"),
            F.octet_length(F.lit(12)).alias("oi"),
        )
    )
    assert table.column("b").to_pylist() == [16]
    assert table.column("o").to_pylist() == [2]
    assert table.column("bc").to_pylist() == [32]
    assert table.column("oc").to_pylist() == [4]
    assert table.column("bbin").to_pylist() == [16]
    assert table.column("obin").to_pylist() == [2]
    # C3 is invalid UTF-8 (1 byte). A Utf8 stringify would be U+FFFD (3 bytes).
    assert table.column("oc3").to_pylist() == [1]
    assert table.column("bc3").to_pylist() == [8]
    assert table.column("bi").to_pylist() == [16]
    assert table.column("oi").to_pylist() == [2]
    assert table.schema.field("b").type == pa.int32()
    assert table.schema.field("o").type == pa.int32()


def test_utf8_valid(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.is_valid_utf8(F.lit("ok")).alias("v"),
            F.make_valid_utf8(F.lit("ok")).alias("m"),
        )
    )
    assert table.column("v").to_pylist() == [True]
    assert table.schema.field("v").type == pa.bool_()
    assert table.column("m").to_pylist() == ["ok"]
    assert table.schema.field("m").type in (pa.string(), pa.large_string())


def test_utf8_invalid_bytes(spark: ReparkSession) -> None:
    """P4: actually-invalid UTF-8 via unhex — is_valid False, make_valid repairs to U+FFFD."""
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.is_valid_utf8(F.unhex(F.lit("C3"))).alias("iv"),
            F.make_valid_utf8(F.unhex(F.lit("C3"))).alias("mv"),
            F.is_valid_utf8(F.unhex(F.lit("C3A9"))).alias("ok"),
            F.make_valid_utf8(F.unhex(F.lit("C3A9"))).alias("e9"),
        )
    )
    assert table.column("iv").to_pylist() == [False]
    assert table.column("mv").to_pylist() == ["\ufffd"]
    assert table.column("ok").to_pylist() == [True]
    assert table.column("e9").to_pylist() == ["é"]


def test_null_inputs(spark: ReparkSession) -> None:
    """P1: NULL-in / Spark-oracle-out for every GT1 name. regexp_count NULL is NULL, not 0."""
    ints = spark.createDataFrame([(None,)], schema="n int")
    longs = spark.createDataFrame([(None,)], schema="n bigint")
    strings = spark.createDataFrame([(None,)], schema="s string")
    doubles = spark.createDataFrame([(None,)], schema="x double")
    assert _table(ints.select(F.bin("n").alias("v"))).column("v").to_pylist() == [None]
    assert _table(ints.select(F.hex("n").alias("v"))).column("v").to_pylist() == [None]
    assert _table(strings.select(F.unhex("s").alias("v"))).column("v").to_pylist() == [None]
    assert _table(ints.select(F.factorial("n").alias("v"))).column("v").to_pylist() == [None]
    assert _table(doubles.select(F.rint("x").alias("v"))).column("v").to_pylist() == [None]
    assert _table(ints.select(F.bit_count("n").alias("v"))).column("v").to_pylist() == [None]
    assert _table(strings.select(F.bit_length("s").alias("v"))).column("v").to_pylist() == [None]
    assert _table(strings.select(F.octet_length("s").alias("v"))).column("v").to_pylist() == [None]
    assert _table(strings.select(F.is_valid_utf8("s").alias("v"))).column("v").to_pylist() == [None]
    assert _table(strings.select(F.make_valid_utf8("s").alias("v"))).column("v").to_pylist() == [
        None
    ]
    two = spark.createDataFrame([(None, 1), (6, None)], schema="x int, pos int")
    assert _table(two.select(F.bit_get("x", "pos").alias("v"))).column("v").to_pylist() == [
        None,
        None,
    ]
    assert _table(two.select(F.getbit("x", "pos").alias("v"))).column("v").to_pylist() == [
        None,
        None,
    ]
    assert _table(ints.select(F.shiftleft("n", 1).alias("v"))).column("v").to_pylist() == [None]
    assert _table(ints.select(F.shiftright("n", 1).alias("v"))).column("v").to_pylist() == [None]
    assert _table(ints.select(F.shiftrightunsigned("n", 1).alias("v"))).column("v").to_pylist() == [
        None
    ]
    re_frame = spark.createDataFrame(
        [(None, "ab"), ("ababab", None)], schema="str string, regexp string"
    )
    assert _table(re_frame.select(F.regexp_count("str", "regexp").alias("v"))).column(
        "v"
    ).to_pylist() == [None, None]
    assert _table(re_frame.select(F.regexp_instr("str", "regexp").alias("v"))).column(
        "v"
    ).to_pylist() == [None, None]
    sp = spark.createDataFrame(
        [(None, ".", 2), ("a.b.c", None, 2), ("a.b.c", ".", None)],
        schema="src string, d string, n int",
    )
    assert _table(sp.select(F.split_part("src", "d", "n").alias("v"))).column("v").to_pylist() == [
        None,
        None,
        None,
    ]
    wb = spark.createDataFrame(
        [(None, 0.0, 10.0, 5), (5.0, None, 10.0, 5)],
        schema="v double, mn double, mx double, nb int",
    )
    assert _table(wb.select(F.width_bucket("v", "mn", "mx", "nb").alias("w"))).column(
        "w"
    ).to_pylist() == [None, None]
    assert _table(longs.select(F.factorial("n").alias("v"))).column("v").to_pylist() == [None]


def test_unary_str_is_column_name(spark: ReparkSession) -> None:
    """P5: remaining unary names resolve a bare str as a column, not a literal."""
    frame = spark.createDataFrame(
        [(13, "48656C6C6F", 5, 1.5, 7, "ab", "ok")],
        ["n", "hexs", "f", "x", "bits", "s", "u"],
    )
    table = _table(
        frame.select(
            F.bin("n").alias("bin"),
            F.hex("n").alias("hex"),
            F.unhex("hexs").alias("unhex"),
            F.factorial("f").alias("fact"),
            F.rint("x").alias("rint"),
            F.bit_count("bits").alias("bc"),
            F.shiftleft("n", 1).alias("sl"),
            F.shiftright("n", 1).alias("sr"),
            F.shiftrightunsigned("n", 1).alias("sru"),
            F.bit_length("s").alias("bl"),
            F.octet_length("s").alias("ol"),
            F.is_valid_utf8("u").alias("iv"),
            F.make_valid_utf8("u").alias("mv"),
        )
    )
    assert table.column("bin").to_pylist() == ["1101"]
    assert table.column("hex").to_pylist() == ["D"]
    assert bytes(table.column("unhex").to_pylist()[0]) == b"Hello"
    assert table.column("fact").to_pylist() == [120]
    assert table.column("rint").to_pylist() == [2.0]
    assert table.column("bc").to_pylist() == [3]
    assert table.column("sl").to_pylist() == [26]
    assert table.column("sr").to_pylist() == [6]
    assert table.column("sru").to_pylist() == [6]
    assert table.column("bl").to_pylist() == [16]
    assert table.column("ol").to_pylist() == [2]
    assert table.column("iv").to_pylist() == [True]
    assert table.column("mv").to_pylist() == ["ok"]
    # Literal reading of the same Python strings is a different answer (or an error).
    lit_hex = _table(frame.select(F.hex(F.lit("n")).alias("h")))
    assert lit_hex.column("h").to_pylist() != table.column("hex").to_pylist()


def test_bin_and_rint_string_coercion(spark: ReparkSession) -> None:
    """G5: numeric-strings reach bin/rint the way Spark's CAST does."""
    frame = spark.createDataFrame([("13", "1.5")], ["n", "x"])
    table = _table(frame.select(F.bin("n").alias("b"), F.rint("x").alias("r")))
    assert table.column("b").to_pylist() == ["1101"]
    assert table.column("r").to_pylist() == [2.0]


def test_gt1_docstring_examples_execute(spark: ReparkSession) -> None:
    """F3: every GT1 docstring example is a form live PySpark accepts, and runs here."""
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.bin(F.lit(13)).alias("bin"),
            F.hex(F.lit(17)).alias("hex"),
            F.unhex(F.lit("48656C6C6F")).alias("unhex"),
            F.factorial(F.lit(5)).alias("fact"),
            F.factorial(F.lit(21)).alias("fact21"),
            F.rint(F.lit(1.5)).alias("rint"),
            F.width_bucket(F.lit(5.0), F.lit(0.0), F.lit(10.0), 5).alias("wb"),
            F.bit_count(F.lit(7)).alias("bc"),
            F.bit_get(F.lit(6), F.lit(1)).alias("bg"),
            F.shiftleft(F.lit(2), 1).alias("sl"),
            F.shiftright(F.lit(-2), 1).alias("sr"),
            F.shiftrightunsigned(F.lit(8), 1).alias("sru"),
            F.split_part(F.lit("a.b.c"), F.lit("."), F.lit(2)).alias("sp"),
            F.regexp_count(F.lit("ababab"), F.lit("ab")).alias("rc"),
            F.regexp_instr(F.lit("abcde"), F.lit("c")).alias("ri"),
            F.bit_length(F.lit("ab")).alias("bl"),
            F.octet_length(F.lit("ab")).alias("ol"),
            F.is_valid_utf8(F.lit("ok")).alias("iv"),
            F.make_valid_utf8(F.lit("ok")).alias("mv"),
        )
    )
    assert table.column("bin").to_pylist() == ["1101"]
    assert table.column("hex").to_pylist() == ["11"]
    assert bytes(table.column("unhex").to_pylist()[0]) == b"Hello"
    assert table.column("fact").to_pylist() == [120]
    assert table.column("fact21").to_pylist() == [None]
    assert table.column("rint").to_pylist() == [2.0]
    assert table.column("wb").to_pylist() == [3]
    assert table.column("bc").to_pylist() == [3]
    assert table.column("bg").to_pylist() == [1]
    assert table.column("sl").to_pylist() == [4]
    assert table.column("sr").to_pylist() == [-1]
    assert table.column("sru").to_pylist() == [4]
    assert table.column("sp").to_pylist() == ["b"]
    assert table.column("rc").to_pylist() == [3]
    assert table.column("ri").to_pylist() == [3]
    assert table.column("bl").to_pylist() == [16]
    assert table.column("ol").to_pylist() == [2]
    assert table.column("iv").to_pylist() == [True]
    assert table.column("mv").to_pylist() == ["ok"]


def test_gt1_docstring_examples_are_not_bare_literals() -> None:
    """F3 mutation pin: restoring F.bin(13) in the docstring reds this."""
    spark_dir = Path(__file__).resolve().parents[1] / "src" / "repark" / "spark"
    math_text = (spark_dir / "functions_math.py").read_text(encoding="utf-8")
    bitwise_text = (spark_dir / "functions_bitwise.py").read_text(encoding="utf-8")
    expr_text = (spark_dir / "functions_expr.py").read_text(encoding="utf-8")
    assert "F.bin(13)" not in math_text
    assert "F.bin(F.lit(13))" in math_text
    assert "F.hex(17)" not in math_text
    assert "F.factorial(5)" not in math_text
    assert "F.rint(1.5)" not in math_text
    assert "F.bit_count(7)" not in bitwise_text
    assert "F.bit_get(6, 1)" not in bitwise_text
    assert "F.shiftleft(2, 1)" not in bitwise_text
    assert "F.shiftright(-2, 1)" not in bitwise_text
    assert "F.shiftrightunsigned(8, 1)" not in bitwise_text
    assert "F.width_bucket(5.0, 0.0, 10.0, 5)" not in math_text
    assert "F.regexp_count(F.lit('ababab'), 'ab')" not in expr_text
    assert "F.regexp_count(F.lit('ababab'), F.lit('ab'))" in expr_text


def test_sql_door_bit_length_stringifies(spark: ReparkSession) -> None:
    """G5: string::functions() overwrite must win on the Spark SQL door (not only F.*)."""
    table = _table(spark.sql("SELECT bit_length(12) AS b, octet_length(12) AS o"))
    assert table.column("b").to_pylist() == [16]
    assert table.column("o").to_pylist() == [2]
    assert table.schema.field("b").type == pa.int32()


def test_bin_bool_over_accepts_where_spark_refuses(spark: ReparkSession) -> None:
    """G5 residual: facade CAST(long) lets BOOLEAN through; Spark analysis-refuses."""
    table = _table(
        spark.range(1).select(F.bin(F.lit(True)).alias("t"), F.bin(F.lit(False)).alias("f"))
    )
    assert table.column("t").to_pylist() == ["1"]
    assert table.column("f").to_pylist() == ["0"]


def test_sql_door_regexp_count_null_is_still_zero(spark: ReparkSession) -> None:
    """SQL-door residual: DF regexp_count(NULL) is 0. Facade P1 is NULL."""
    table = _table(spark.sql("SELECT regexp_count(CAST(NULL AS VARCHAR), 'ab') AS c"))
    assert table.column("c").to_pylist() == [0]


def test_sql_door_regexp_instr_third_arg_is_df_start(spark: ReparkSession) -> None:
    """SQL-door residual: 3rd arg is DF start, not Spark idx. Facade G6 is 2."""
    table = _table(spark.sql("SELECT regexp_instr('abcde', 'b(c)d', 99) AS i"))
    assert table.column("i").to_pylist() == [0]
