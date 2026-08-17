"""DS-4 — facade pins for the five torture-dataset families (conductor-18).

Each family is generated at its seeded ``small()`` scale (64 rows, seed 42 — never the
1_000_000 CLI default, so CI stays fast), written under ``tmp_path``, and read back
**through the repark facade**. Assertions run on the Arrow path (``to_arrow``), value
AND type — never ``show``.

The generators under ``python/repark-parity/datasets/<family>/`` are frozen surfaces:
this module imports and reads them, it never edits them. The generator table is the
oracle — expected values are derived from ``small()``, not hand-typed.

Two marker vocabularies appear in the pins below, both meaning "this lane pins current
behavior and reports; it does not change the engine" (``task/c18-datasets-ledger.md``):

* ``POLICY`` — documented, intended behavior that surprises (inference sampling misses,
  the >38-digit decimal demotion, leading zeros lost to int inference).
* ``BUG-CANDIDATE`` — behavior that looks wrong and is reported, not fixed here. If one
  of these reds because the behavior was fixed, that is the fix landing: re-point the
  pin at the corrected behavior and move the ledger row.

The nested family carries the U-DF-1 regression corpus: the capitalized-``Legs``
string-form ``explode`` and ``dynamicFlatten`` pins here are the DS-1 held pins, landed
now that the mixed-case generator bind (#154) is on main.
"""

from __future__ import annotations

import importlib
import sys
import types
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812
from repark.spark.session import _reset_active_session_for_tests

# --------------------------------------------------------------------------------------------
# Generator loading (bench sys.modules loader — the datasets tree is not a hatch package)
# --------------------------------------------------------------------------------------------

# This file lives at python/repark/tests/ → the datasets tree is a peer under repark-parity.
_DATASETS_DIR = Path(__file__).resolve().parents[2] / "repark-parity" / "datasets"

#: A9 scale: the CI door is 64 rows at seed 42. The 1M CLI default is for local runs.
ROWS = 64
SEED = 42
#: schema_inference at test scale: the int32→int64 / string→float shift sits inside the budget.
CONFLICT_AT = 32
#: Inference budget that deliberately stops short of ``CONFLICT_AT`` (the sampling miss).
SAMPLING_ROWS = 16


def _load_datasets() -> None:
    """Register ``python/repark-parity/datasets`` as ``repark_datasets`` (bench loader)."""
    package_name = "repark_datasets"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(_DATASETS_DIR)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package


def _datagen(family: str) -> Any:
    """Import one family's ``datagen`` module."""
    _load_datasets()
    return importlib.import_module(f"repark_datasets.{family}.datagen")


def _write_family(family: str, out: Path, **kwargs: Any) -> Path:
    """Write one family at CI scale into ``out`` (never the repository, never the 1M default)."""
    datagen = _datagen(family)
    return datagen.write_files(rows=ROWS, seed=SEED, out=out, **kwargs)


def _is_arrow_string(data_type: pa.DataType) -> bool:
    """True for utf8 / large_string / string_view (DataFusion may emit any of these)."""
    return (
        pa.types.is_string(data_type)
        or pa.types.is_large_string(data_type)
        or pa.types.is_string_view(data_type)
    )


def _is_arrow_list(data_type: pa.DataType) -> bool:
    """True for list / large_list."""
    return pa.types.is_list(data_type) or pa.types.is_large_list(data_type)


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """Isolated facade session (no AWS, no catalog)."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-datasets-facade").getOrCreate()
    try:
        yield session
    finally:
        session.stop()
        _reset_active_session_for_tests()


# ==================================================================================================
# nested — capitalized explode + dynamicFlatten (the DS-1 held pins; U-DF-1 corpus)
# ==================================================================================================


def _nested_truth() -> list[dict[str, Any]]:
    """Generator typed truth for the nested family at CI scale."""
    return _datagen("nested").small(rows=ROWS, seed=SEED).to_pylist()


def _nested_leg_rows(rows: list[dict[str, Any]]) -> list[tuple[int, int, str]]:
    """Oracle for ``explode(Legs)``: one ``(id, leg_id, side)`` per leg, id/leg order."""
    return sorted(
        (row["id"], leg["leg_id"], leg["side"]) for row in rows for leg in (row["Legs"] or [])
    )


def _nested_full_flatten_rows(rows: list[dict[str, Any]]) -> int:
    """Oracle row count for a full ``dynamicFlatten``.

    Every list on the path is exploded, and ``explode`` drops the row when a list is
    null or empty — so a branch survives only when every list it passes through is
    non-empty. The count is the nested cartesian product of the surviving branches.
    """
    total = 0
    for row in rows:
        legs_branch = 0
        for leg in row["Legs"] or []:
            for fill in leg["Fills"] or []:
                meta = fill["Meta"] or {}
                extra = meta.get("Extra") or {}
                legs_branch += len(meta.get("Tags") or []) * len(extra.get("Flags") or [])
        total += legs_branch * len(row["Tags"] or []) * len(row["Scores"] or [])
    return total


def test_nested_parquet_read_keeps_capitalized_nested_schema(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """Parquet read preserves the capitalized nested shape (value AND Arrow type)."""
    written = _write_family("nested", tmp_path / "nested")
    frame = spark.read.parquet(str(written / "data.parquet"))
    assert frame.columns == ["id", "Legs", "Tags", "Scores", "user_properties"]

    table = frame.orderBy("id").to_arrow()
    assert table.num_rows == ROWS
    assert table.schema.field("id").type == pa.int64()

    legs_type = table.schema.field("Legs").type
    assert _is_arrow_list(legs_type)
    leg_struct = legs_type.value_type
    assert pa.types.is_struct(leg_struct)
    assert [field.name for field in leg_struct] == ["leg_id", "side", "Fills"]
    assert leg_struct.field("leg_id").type == pa.int64()

    tags_type = table.schema.field("Tags").type
    assert _is_arrow_list(tags_type)
    assert _is_arrow_string(tags_type.value_type)
    assert _is_arrow_list(table.schema.field("Scores").type)
    assert table.schema.field("Scores").type.value_type == pa.int32()
    # Null-typed list survives the round trip as a list of nulls (array<void>).
    properties_type = table.schema.field("user_properties").type
    assert _is_arrow_list(properties_type)
    assert pa.types.is_null(properties_type.value_type)

    rows = table.to_pylist()
    truth = _nested_truth()
    assert [row["id"] for row in rows] == [row["id"] for row in truth]
    # Deep value pin: the whole nested value of the first row, straight from the generator.
    assert rows[0]["Legs"] == truth[0]["Legs"]
    assert rows[0]["Tags"] == truth[0]["Tags"]


def test_nested_explode_string_form_capitalized_legs(spark: ReparkSession, tmp_path: Path) -> None:
    """U-DF-1 (held DS-1 pin): ``F.explode('Legs')`` string form binds the mixed-case field."""
    written = _write_family("nested", tmp_path / "nested")
    frame = spark.read.parquet(str(written / "data.parquet"))

    exploded = frame.select(frame.id, F.explode("Legs").alias("leg")).orderBy("id")
    table = exploded.to_arrow()
    truth = _nested_truth()
    expected = _nested_leg_rows(truth)
    assert table.num_rows == len(expected)

    leg_type = table.schema.field("leg").type
    assert pa.types.is_struct(leg_type)
    assert leg_type.field("leg_id").type == pa.int64()
    assert _is_arrow_string(leg_type.field("side").type)
    assert _is_arrow_list(leg_type.field("Fills").type)

    observed = sorted(
        (row["id"], row["leg"]["leg_id"], row["leg"]["side"]) for row in table.to_pylist()
    )
    assert observed == expected


def test_nested_explode_casefold_and_column_forms_agree(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """``explode('LEGS')`` / ``explode(F.col('Legs'))`` resolve the same capitalized field."""
    written = _write_family("nested", tmp_path / "nested")
    frame = spark.read.parquet(str(written / "data.parquet"))
    expected = _nested_leg_rows(_nested_truth())

    for generator in (F.explode("LEGS"), F.explode(F.col("Legs")), F.explode(frame["Legs"])):
        table = frame.select(frame.id, generator.alias("leg")).to_arrow()
        assert table.num_rows == len(expected)
        assert pa.types.is_struct(table.schema.field("leg").type)
        observed = sorted(
            (row["id"], row["leg"]["leg_id"], row["leg"]["side"]) for row in table.to_pylist()
        )
        assert observed == expected


def test_nested_explode_outer_keeps_null_and_empty_list_rows(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """``explode_outer`` on the scalar-element lists keeps the null-list and empty-list rows."""
    written = _write_family("nested", tmp_path / "nested")
    frame = spark.read.parquet(str(written / "data.parquet"))
    truth = _nested_truth()

    for column, element_check in (("Tags", _is_arrow_string), ("Scores", pa.types.is_int32)):
        elements = sum(len(row[column] or []) for row in truth)
        missing = [row["id"] for row in truth if not row[column]]
        assert elements and missing  # both halves are really present at this scale

        table = frame.select(frame.id, F.explode_outer(column).alias("element")).to_arrow()
        # One row per element, plus one null-element row for every null/empty list.
        assert table.num_rows == elements + len(missing), column
        assert element_check(table.schema.field("element").type), column
        rows = table.to_pylist()
        null_ids = sorted({row["id"] for row in rows if row["element"] is None})
        assert null_ids == sorted(missing), column

        # Plain explode drops those rows instead of keeping them (the outer/inner contrast).
        plain = frame.select(frame.id, F.explode(column).alias("element")).to_arrow()
        assert plain.num_rows == elements, column


def test_nested_explode_outer_on_array_of_struct_refuses_loud(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """BUG-CANDIDATE — ``explode_outer`` refuses on ``array<struct>`` while ``explode`` works.

    Plain ``explode('Legs')`` unnests the list-of-struct fine (pinned above). Its outer
    twin needs an SQL element type for the null/empty guard and has no spelling for a
    struct element, so it refuses instead of keeping the null rows. The asymmetry is
    reported, not fixed here: the null/empty-keep behavior is pinned on the scalar-element
    lists in the previous test. If this reds because the guard learned struct elements,
    that is the fix — replace it with the value pin the previous test uses.
    """
    from repark.errors import AnalysisException

    written = _write_family("nested", tmp_path / "nested")
    frame = spark.read.parquet(str(written / "data.parquet"))

    with pytest.raises(AnalysisException, match=r"explode_outer cannot resolve SQL element type"):
        frame.select(frame.id, F.explode_outer("Legs").alias("leg")).to_arrow()

    # The refusal names the element type it could not spell, not a generic failure.
    with pytest.raises(AnalysisException, match=r"array<struct<leg_id:bigint"):
        frame.select(frame.id, F.explode_outer("Legs").alias("leg")).to_arrow()

    # Contrast: the plain generator on the very same column still unnests.
    plain = frame.select(frame.id, F.explode("Legs").alias("leg")).to_arrow()
    assert plain.num_rows == len(_nested_leg_rows(_nested_truth()))


def test_nested_dynamic_flatten_unnests_struct_columns(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """``dynamicFlatten`` flattens the exploded leg struct with parent-path prefixes."""
    written = _write_family("nested", tmp_path / "nested")
    frame = spark.read.parquet(str(written / "data.parquet"))

    legs = frame.select(frame.id, F.explode("Legs").alias("leg"))
    flat = legs.dynamicFlatten(explode_lists=False).orderBy("id", "leg_leg_id")
    assert flat.columns == ["id", "leg_leg_id", "leg_side", "leg_Fills"]

    table = flat.to_arrow()
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("leg_leg_id").type == pa.int64()
    assert _is_arrow_string(table.schema.field("leg_side").type)
    # explode_lists=False leaves the inner list-of-struct in place (no silent explode).
    assert _is_arrow_list(table.schema.field("leg_Fills").type)

    observed = [(row["id"], row["leg_leg_id"], row["leg_side"]) for row in table.to_pylist()]
    assert observed == _nested_leg_rows(_nested_truth())


def test_nested_dynamic_flatten_full_depth_column_order(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """Full-depth ``dynamicFlatten``: in-place expansion order + the drop of ``array<void>``."""
    written = _write_family("nested", tmp_path / "nested")
    frame = spark.read.parquet(str(written / "data.parquet"))

    flat = frame.dynamicFlatten()
    # Expansions are in place (schema position preserved); the null-typed list is dropped,
    # never exploded (drop_null_lists default).
    assert flat.columns == [
        "id",
        "Legs_leg_id",
        "Legs_side",
        "Legs_Fills_fill_id",
        "Legs_Fills_px",
        "Legs_Fills_qty",
        "Legs_Fills_Meta_venue",
        "Legs_Fills_Meta_Tags",
        "Legs_Fills_Meta_Extra_Deep_level",
        "Legs_Fills_Meta_Extra_Deep_note",
        "Legs_Fills_Meta_Extra_Flags",
        "Tags",
        "Scores",
    ]
    assert "user_properties" not in flat.columns

    table = flat.to_arrow()
    assert table.num_rows == _nested_full_flatten_rows(_nested_truth())
    assert table.schema.field("Legs_Fills_px").type == pa.float64()

    assert table.schema.field("Legs_Fills_Meta_Extra_Deep_level").type == pa.int32()
    assert table.schema.field("Legs_Fills_Meta_Extra_Flags").type == pa.bool_()
    assert table.schema.field("Scores").type == pa.int32()
    assert _is_arrow_string(table.schema.field("Legs_Fills_Meta_Extra_Deep_note").type)


def test_nested_dynamic_flatten_count_action_refuses_loud(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """BUG-CANDIDATE — ``count()`` on the full-depth flatten plan trips an optimizer rule.

    The very same plan exports fine through ``to_arrow`` (pinned above, 140 rows at this
    scale), but the ``count()`` action reds inside ``push_down_leaf_projections``: the
    multi-pass explode leaves a qualified ``<explode-alias>."Legs"`` beside the
    unqualified ``Legs`` and the rule calls the pair ambiguous. So the row count is
    reachable and correct on the export path while the cheapest way to ask for it fails.

    Reported, not fixed. The shallow one-pass flatten and a plain explode both count
    fine, which is what makes this specific rather than "counts do not work". If this
    reds because the optimizer stopped tripping, that is the fix — swap it for the
    ``count() == to_arrow().num_rows`` equality.
    """
    from repark.errors import AnalysisException

    written = _write_family("nested", tmp_path / "nested")
    frame = spark.read.parquet(str(written / "data.parquet"))
    expected = _nested_full_flatten_rows(_nested_truth())

    deep = frame.dynamicFlatten()
    assert deep.to_arrow().num_rows == expected  # the export path is correct
    with pytest.raises(AnalysisException, match=r"push_down_leaf_projections"):
        deep.count()

    # Narrow, not general: one explode pass counts fine on the same corpus.
    legs = frame.select(frame.id, F.explode("Legs").alias("leg"))
    assert legs.count() == len(_nested_leg_rows(_nested_truth()))
    assert legs.dynamicFlatten(explode_lists=False).count() == len(
        _nested_leg_rows(_nested_truth())
    )


# ==================================================================================================
# schema_inference — the sampling-miss POLICY pins
# ==================================================================================================


def _schema_inference_truth() -> list[dict[str, Any]]:
    return (
        _datagen("schema_inference")
        .small(rows=ROWS, seed=SEED, conflict_at=CONFLICT_AT)
        .to_pylist()
    )


def _columns_by_name(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {column["name"]: column for column in report["columns"]}


def test_schema_inference_sampling_miss_is_policy(spark: ReparkSession, tmp_path: Path) -> None:
    """POLICY — an under-sampled read misses the int32→int64 widening past the cap.

    smartCsv infers from at most ``samplingRows`` data rows and always materialises the
    whole file. The generator puts the widening at ``conflict_at``; a cap below it means
    inference never sees an int64 value, and the column resolves ``int32``. That is the
    documented contract (raise ``samplingRows`` to scan more), pinned here at test scale
    — this lane never "fixes" inference.
    """
    written = _write_family("schema_inference", tmp_path / "inference", conflict_at=CONFLICT_AT)
    csv_path = str(written / "data.csv")
    truth = _schema_inference_truth()
    # The widening really is in the file, past the cap — the miss is a sampling fact.
    assert max(row["int_widens"] for row in truth) > 2**31 - 1
    assert all(row["int_widens"] <= 2**31 - 1 for row in truth[:SAMPLING_ROWS])

    capped = spark.read.smartCsv(csv_path, sep=",", samplingRows=SAMPLING_ROWS)
    report = capped.describe_ingest()
    assert report["source"] == "smartCsv"
    assert report["delimiter"] == ","
    assert report["data_row_count"] == ROWS
    assert report["inference_rows_scanned"] == SAMPLING_ROWS
    assert report["inference_capped"] is True
    assert report["sampling_rows_limit"] == SAMPLING_ROWS
    capped_columns = _columns_by_name(report)
    assert capped_columns["int_widens"]["resolved_type"] == "int32"
    assert capped_columns["int_widens"]["sample_count"] == SAMPLING_ROWS

    # Positive control: the full scan sees the widening and resolves int64.
    full = spark.read.smartCsv(csv_path, sep=",")
    full_report = full.describe_ingest()
    assert full_report["inference_capped"] is False
    assert full_report["inference_rows_scanned"] == ROWS
    full_columns = _columns_by_name(full_report)
    assert full_columns["int_widens"]["resolved_type"] == "int64"
    # The string-vs-float half of the conflict stays string under both budgets (widest rung).
    assert capped_columns["str_or_float"]["resolved_type"] == "string"
    assert full_columns["str_or_float"]["resolved_type"] == "string"


def test_schema_inference_labeled_classes_resolve_as_documented(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """POLICY — per-class inference outcomes for the labeled conflict corpus (full scan).

    Two of these are the honest torture points, not defects to fix here:
    ``leading_zero_id`` is a zero-padded identifier and inference reads it as ``int32``
    (the leading zeros are gone), and ``empty_or_null`` counts every recognized null
    spelling as null so only the one real token survives the sample.
    """
    written = _write_family("schema_inference", tmp_path / "inference", conflict_at=CONFLICT_AT)
    csv_path = written / "data.csv"
    csv_text = csv_path.read_text(encoding="utf-8")
    assert "000000" in csv_text  # the ids really are zero-padded in the file

    report = spark.read.smartCsv(str(csv_path), sep=",").describe_ingest()
    columns = _columns_by_name(report)
    assert columns["leading_zero_id"]["resolved_type"] == "int32"  # POLICY: zeros lost
    assert columns["boolish_int"]["resolved_type"] == "int32"  # 0/1 are ints, never bool
    assert columns["bool_spelling"]["resolved_type"] == "bool"
    assert columns["dateish"]["resolved_type"] == "string"  # mixed date / non-date
    assert columns["currency"]["resolved_type"] == "string"  # currency marks stay text
    assert columns["scientific"]["resolved_type"] == "float64"
    assert columns["ts_looking"]["resolved_type"] == "timestamp"
    assert columns["euro_decimal"]["resolved_type"].startswith("decimal128")
    empty_or_null = columns["empty_or_null"]
    assert empty_or_null["resolved_type"] == "string"
    assert empty_or_null["null_count"] + empty_or_null["sample_count"] == ROWS
    assert empty_or_null["null_count"] > empty_or_null["sample_count"]


def test_schema_inference_undersampled_cast_refuses_loud(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """POLICY — the under-widened column fails LOUD on materialisation, never silently.

    This is the other half of the sampling contract: inference under-widens (the pin
    above), and the cast that follows refuses rather than truncating or nulling the
    values it cannot hold. The fix is the documented one — raise ``samplingRows`` — so
    the pin also proves the widened read materialises the same column cleanly.
    """
    from repark.errors import PySparkException

    written = _write_family("schema_inference", tmp_path / "inference", conflict_at=CONFLICT_AT)
    csv_path = str(written / "data.csv")

    capped = spark.read.smartCsv(csv_path, sep=",", samplingRows=SAMPLING_ROWS)
    with pytest.raises(PySparkException, match=r"Cannot cast string '2147483648'"):
        capped.select("id", "int_widens").to_arrow()

    widened = spark.read.smartCsv(csv_path, sep=",", samplingRows=ROWS)
    table = widened.select("id", "int_widens").orderBy("id").to_arrow()
    assert table.schema.field("int_widens").type == pa.int64()
    truth = _schema_inference_truth()
    assert [row["int_widens"] for row in table.to_pylist()] == [row["int_widens"] for row in truth]


# ==================================================================================================
# extreme_types — decimal round trip + the >38-digit demotion POLICY
# ==================================================================================================


def _extreme_truth() -> list[dict[str, Any]]:
    return _datagen("extreme_types").small(rows=ROWS, seed=SEED).to_pylist()


def test_extreme_types_decimal_round_trips_through_parquet(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """decimal128(24,21) survives the parquet round trip exactly (value AND type)."""
    written = _write_family("extreme_types", tmp_path / "extreme")
    frame = spark.read.parquet(str(written / "data.parquet"))
    assert frame.columns == [field.name for field in _datagen("extreme_types").SCHEMA]

    table = frame.orderBy("id").to_arrow()
    decimal_type = table.schema.field("decimal_hi").type
    assert pa.types.is_decimal(decimal_type)
    assert decimal_type.precision == 24
    assert decimal_type.scale == 21
    assert table.schema.field("id").type == pa.int64()
    assert _is_arrow_string(table.schema.field("beyond_38").type)

    rows = table.to_pylist()
    truth = _extreme_truth()
    assert [row["decimal_hi"] for row in rows] == [row["decimal_hi"] for row in truth]
    assert isinstance(rows[0]["decimal_hi"], Decimal)
    assert rows[0]["uuid_col"] == truth[0]["uuid_col"]
    assert rows[0]["beyond_38"] == truth[0]["beyond_38"]


def test_extreme_types_beyond_38_digits_demote_to_float64(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """POLICY — the smartCsv ladder demotes p>38 to float64 (documented, not a defect).

    ``beyond_38`` carries 40 integer digits plus one fractional digit: precision 41,
    above the decimal128 cap of 38. The ladder therefore falls to the next rung
    (float64) and the exact digits are gone — the pin records the loss instead of
    hiding it. ``decimal_hi`` in the same file stays exact at decimal128(24,21).
    """
    written = _write_family("extreme_types", tmp_path / "extreme")
    frame = spark.read.smartCsv(str(written / "data.csv"), sep=",")
    report = frame.describe_ingest()
    columns = _columns_by_name(report)
    assert columns["beyond_38"]["resolved_type"] == "float64"
    assert columns["decimal_hi"]["resolved_type"] == "decimal128(24,21)"
    assert columns["uuid_col"]["resolved_type"] == "string"
    assert columns["paragraph"]["resolved_type"] == "string"
    assert columns["html_fragment"]["resolved_type"] == "string"

    table = frame.orderBy("id").to_arrow()
    assert table.schema.field("beyond_38").type == pa.float64()
    decimal_type = table.schema.field("decimal_hi").type
    assert pa.types.is_decimal(decimal_type)
    assert (decimal_type.precision, decimal_type.scale) == (24, 21)
    # CSV inference types ids int32 where the parquet truth is int64 — same values, two doors.
    assert table.schema.field("id").type == pa.int32()

    rows = table.to_pylist()
    truth = _extreme_truth()
    assert rows[0]["beyond_38"] == pytest.approx(float(truth[0]["beyond_38"]))
    assert rows[0]["beyond_38"] == pytest.approx(1e39)
    assert rows[0]["decimal_hi"] == truth[0]["decimal_hi"]
    assert rows[0]["uuid_col"] == truth[0]["uuid_col"]
    assert rows[0]["html_fragment"] == truth[0]["html_fragment"]


# ==================================================================================================
# secrets — reads behave NORMALLY (no redaction of data columns today)
# ==================================================================================================


def _secrets_truth() -> list[dict[str, Any]]:
    return _datagen("secrets").small(rows=ROWS, seed=SEED).to_pylist()


def test_secrets_parquet_read_is_unredacted(spark: ReparkSession, tmp_path: Path) -> None:
    """Credential-named columns read back as ordinary data — values pass through verbatim.

    Opt-in flagging of secret-shaped DATA columns is a roadmap feature this fixture
    predates; ``prop_key_is_secret`` today governs CONFIGURATION keys
    (``test_a3_secrets_redaction.py``), not table columns. This pin is the standing
    detector for that: if a redaction feature ever lands silently, the values here stop
    matching the generator truth and this reds.
    """
    written = _write_family("secrets", tmp_path / "secrets")
    frame = spark.read.parquet(str(written / "data.parquet"))
    datagen = _datagen("secrets")
    assert frame.columns == [field.name for field in datagen.SCHEMA]

    table = frame.orderBy("id").to_arrow()
    rows = table.to_pylist()
    truth = _secrets_truth()
    for column, _class_id in datagen.SECRET_COLUMNS:
        assert _is_arrow_string(table.schema.field(column).type), column
        assert [row[column] for row in rows] == [row[column] for row in truth], column
    assert rows[0]["apiKey"] == truth[0]["apiKey"]
    assert rows[0]["apiKey"].startswith(datagen.FAKE_PREFIX)
    # The one nullable credential column keeps its nulls (a null secret is still a secret).
    null_rows = [row["id"] for row in rows if row[datagen.NULLABLE_SECRET_COLUMN] is None]
    assert null_rows == [row["id"] for row in truth if row[datagen.NULLABLE_SECRET_COLUMN] is None]
    assert null_rows  # the fixture really does carry nulls
    # Nothing is masked: no redaction marker appears anywhere in the values.
    values = [row[column] or "" for row in rows for column, _class_id in datagen.SECRET_COLUMNS]
    assert not any("***" in value for value in values)
    assert all(value == "" or value.startswith(datagen.FAKE_PREFIX) for value in values)


def test_secrets_smart_csv_keeps_camel_case_headers(spark: ReparkSession, tmp_path: Path) -> None:
    """The CSV door reads the same fixture unredacted, mixed-case headers intact."""
    written = _write_family("secrets", tmp_path / "secrets")
    frame = spark.read.smartCsv(str(written / "data.csv"), sep=",")
    datagen = _datagen("secrets")
    # Header case is never normalized silently (normalizeHeaderCase is opt-in).
    assert frame.columns == [field.name for field in datagen.SCHEMA]
    assert "apiKey" in frame.columns
    assert "accessKey" in frame.columns

    table = frame.orderBy("id").to_arrow()
    rows = table.to_pylist()
    truth = _secrets_truth()
    assert table.schema.field("id").type == pa.int32()
    assert _is_arrow_string(table.schema.field("apiKey").type)
    assert [row["apiKey"] for row in rows] == [row["apiKey"] for row in truth]
    assert [row["access_token"] for row in rows] == [row["access_token"] for row in truth]
    # The documented negative control keeps its object-key value (the `bucket` carve-out).
    assert rows[0][datagen.CARVE_OUT_COLUMN] == truth[0][datagen.CARVE_OUT_COLUMN]


# ==================================================================================================
# smartcsv — the delimiter zoo, null tokens, bool spellings, ragged rows
# ==================================================================================================


def _smartcsv_truth() -> list[dict[str, Any]]:
    return _datagen("smartcsv").small(rows=ROWS, seed=SEED).to_pylist()


#: The CSV header names 12 columns; long rows carry a 13th unlabeled cell, so the reader
#: pads the header with a synthesized ``_c12`` and every narrower row counts as ragged.
_SMARTCSV_OVERFLOW_COLUMN = "_c12"


def test_smartcsv_delimiter_zoo_reads_every_scheme(spark: ReparkSession, tmp_path: Path) -> None:
    """Every delimiter scheme reads to the same shape when the delimiter is declared."""
    written = _write_family("smartcsv", tmp_path / "smartcsv")
    datagen = _datagen("smartcsv")
    # The deduped header names are exactly the typed-truth schema names.
    expected_columns = [field.name for field in datagen.SCHEMA]
    assert datagen.CSV_HEADER.count(datagen.DUPLICATE_HEADER_NAME) == 2

    for scheme, delimiter in datagen.DELIMITERS.items():
        path = written / datagen.csv_file_name(scheme)
        frame = spark.read.smartCsv(str(path), sep=delimiter)
        report = frame.describe_ingest()
        assert report["delimiter"] == delimiter, scheme
        assert report["bom_stripped"] is True, scheme
        assert report["skipped_lines"] == len(datagen.PREAMBLE_LINES), scheme
        assert report["header_row_index"] == len(datagen.PREAMBLE_LINES), scheme
        assert report["data_row_count"] == ROWS, scheme
        # Duplicate header name deduped deterministically (dup_label → dup_label_2).
        assert frame.columns == [*expected_columns, _SMARTCSV_OVERFLOW_COLUMN], scheme
        columns = _columns_by_name(report)
        assert columns["flag"]["resolved_type"] == "bool", scheme
        assert columns["yes_no"]["resolved_type"] == "string", scheme
        assert columns["amount_currency"]["resolved_type"] == "string", scheme
        assert columns["amount_wide"]["resolved_type"] == "decimal128(15,5)", scheme
        assert columns["euro_decimal"]["resolved_type"] == "decimal128(5,2)", scheme


def test_smartcsv_ragged_rows_pad_and_overflow_column(spark: ReparkSession, tmp_path: Path) -> None:
    """Short rows null the trailing cells; long rows land in a synthesized overflow column."""
    written = _write_family("smartcsv", tmp_path / "smartcsv")
    datagen = _datagen("smartcsv")
    frame = spark.read.smartCsv(str(written / datagen.csv_file_name("comma")), sep=",")
    report = frame.describe_ingest()
    columns = _columns_by_name(report)

    short_rows = [index for index in range(ROWS) if datagen.is_short_row(index)]
    long_rows = [index for index in range(ROWS) if datagen.is_long_row(index)]
    assert short_rows and long_rows  # both directions really are present at this scale
    # Width is the widest row (13), so every narrower row — including the plain 12-cell
    # ones — is counted as padded.
    assert report["ragged_rows_padded"] == ROWS - len(long_rows)
    assert columns["ragged_tail_1"]["null_count"] == len(short_rows)
    assert columns["ragged_tail_2"]["null_count"] == len(short_rows)
    assert columns[_SMARTCSV_OVERFLOW_COLUMN]["null_count"] == ROWS - len(long_rows)
    assert columns[_SMARTCSV_OVERFLOW_COLUMN]["sample_count"] == len(long_rows)


def test_smartcsv_null_tokens_and_bool_spellings(spark: ReparkSession, tmp_path: Path) -> None:
    """Null-token vocabulary and bool spellings match the generator's typed truth.

    The projection leaves out ``euro_decimal`` on purpose: that column's cast refuses
    loud (pinned in ``test_smartcsv_euro_comma_decimal_cast_refuses_loud``), and this
    pin is about the token vocabularies, not that refusal.
    """
    written = _write_family("smartcsv", tmp_path / "smartcsv")
    datagen = _datagen("smartcsv")
    frame = spark.read.smartCsv(str(written / datagen.csv_file_name("comma")), sep=",")

    table = (
        frame.select(
            "id",
            "flag",
            "yes_no",
            "nullable_note",
            "embedded_delims",
            "dup_label",
            "dup_label_2",
            "ragged_tail_1",
            _SMARTCSV_OVERFLOW_COLUMN,
        )
        .orderBy("id")
        .to_arrow()
    )
    assert table.schema.field("flag").type == pa.bool_()
    assert _is_arrow_string(table.schema.field("yes_no").type)
    assert _is_arrow_string(table.schema.field("nullable_note").type)

    rows = table.to_pylist()
    truth = _smartcsv_truth()
    # Recognized null spellings become NULL; unrecognized ones stay literal strings.
    assert [row["nullable_note"] for row in rows] == [row["nullable_note"] for row in truth]
    assert any(row["nullable_note"] is None for row in rows)
    assert any(row["nullable_note"] is not None for row in rows)
    # true/TRUE/t/T and false/FALSE/f/F all resolve to bool; yes/no/Y/N stay strings.
    assert [row["flag"] for row in rows] == [row["flag"] for row in truth]
    assert [row["yes_no"] for row in rows] == [row["yes_no"] for row in truth]
    # Quoting round trip: the value carrying all four candidate delimiters comes back whole.
    assert [row["embedded_delims"] for row in rows] == [row["embedded_delims"] for row in truth]
    assert [row["ragged_tail_1"] for row in rows] == [row["ragged_tail_1"] for row in truth]
    assert [row["dup_label"] for row in rows] == [row["dup_label"] for row in truth]
    assert [row["dup_label_2"] for row in rows] == [row["dup_label_2"] for row in truth]
    # The overflow cells of the long rows land in the synthesized column, in row order.
    overflow = [row[_SMARTCSV_OVERFLOW_COLUMN] for row in rows]
    assert [value for value in overflow if value is not None] == [
        f"overflow-{index:04d}" for index in range(ROWS) if datagen.is_long_row(index)
    ]


def test_smartcsv_decimal_widths_materialize(spark: ReparkSession, tmp_path: Path) -> None:
    """The decimal-width class resolves to one union type and materialises exactly."""
    written = _write_family("smartcsv", tmp_path / "smartcsv")
    datagen = _datagen("smartcsv")
    frame = spark.read.smartCsv(str(written / datagen.csv_file_name("comma")), sep=",")

    table = frame.select("id", "amount_wide").orderBy("id").to_arrow()
    amount_type = table.schema.field("amount_wide").type
    assert pa.types.is_decimal(amount_type)
    # Union of the widths: max integer digits (10) + max scale (5).
    assert (amount_type.precision, amount_type.scale) == (15, 5)
    rows = table.to_pylist()
    truth = _smartcsv_truth()
    # Signs and leading zeros do not survive as text — the numeric value is what round-trips.
    assert [row["amount_wide"] for row in rows] == [
        Decimal(row["amount_wide"]).quantize(Decimal("0.00001")) for row in truth
    ]


def test_smartcsv_euro_comma_decimal_cast_refuses_loud(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """BUG-CANDIDATE — a comma-decimal column infers ``decimal128`` and then refuses the cast.

    The inference ladder normalizes ``760,35`` to a fixed-point value and resolves the
    column to ``decimal128(5,2)``. The cast that materialises it is handed the **raw**
    cell text, and the engine cannot parse a comma decimal — so a whole-frame read of
    either corpus that carries this class refuses loud. Reported, not fixed: the refusal
    is honest (no silent corruption), but the resolved type promises a value the read
    cannot deliver.

    Both corpora that carry the class are exercised, so the pin is about the class and
    not one file. If this reds because the cast learned the comma form, that is the fix —
    replace it with a value pin against the generator truth.
    """
    from repark.errors import PySparkException

    smart_dir = _write_family("smartcsv", tmp_path / "smartcsv")
    inference_dir = _write_family(
        "schema_inference", tmp_path / "inference", conflict_at=CONFLICT_AT
    )
    datagen = _datagen("smartcsv")

    smart_frame = spark.read.smartCsv(str(smart_dir / datagen.csv_file_name("comma")), sep=",")
    # The column really did resolve to a decimal rung — the promise the cast breaks.
    assert (
        _columns_by_name(smart_frame.describe_ingest())["euro_decimal"]["resolved_type"]
        == "decimal128(5,2)"
    )
    with pytest.raises(PySparkException, match=r"Cannot cast string '\d+,\d+'"):
        smart_frame.to_arrow()

    inference_frame = spark.read.smartCsv(str(inference_dir / "data.csv"), sep=",")
    with pytest.raises(PySparkException, match=r"Cannot cast string '\d+,\d+'"):
        inference_frame.select("id", "euro_decimal").to_arrow()


def test_smartcsv_delimiter_autodetect_picks_a_rival_delimiter(tmp_path: Path) -> None:
    """BUG-CANDIDATE / known-limit — auto-detect picks the wrong delimiter.

    ``detect_delimiter`` scores candidates by how many lines agree on a field
    count, and ``csv.reader`` only honors a quote that starts a field. In the
    comma-scheme file the ``embedded_delims`` value is quoted for the comma, so
    a rival candidate (``;``) sees that quote mid-field, treats it as literal,
    and splits every data line into exactly two fields — perfect agreement, and
    it beats the correct 12-field split. The header line then fails the
    field-count vote too, so one data row is eaten as the header.

    B4 rounds 1-3 tried to close this class and each regressed a named
    counterexample (field-count-first inverted 2-col files; one-splitter
    corrupted declared-sep values). Round 4 descopes: document the miss,
    declare ``sep`` (European-locale files: ``sep=';'``). The pin asserts
    the documented origin/main behavior, plus the correct read with ``sep``.

    Pinned at the preprocessing surface (no engine) because the mis-split
    header names are not usable identifiers.
    """
    from repark.spark._csv_smart import prepare_messy_csv

    written = _write_family("smartcsv", tmp_path / "smartcsv")
    datagen = _datagen("smartcsv")
    misdetected = {"comma": ";", "semicolon": "\t", "tab": ";", "pipe": ";"}

    for scheme, delimiter in datagen.DELIMITERS.items():
        path = written / datagen.csv_file_name(scheme)
        auto = prepare_messy_csv(path)
        assert auto.report.delimiter == misdetected[scheme], scheme
        assert auto.report.delimiter != delimiter, scheme
        # One data row is consumed as the header row under the wrong split.
        assert auto.report.data_row_count == ROWS - 1, scheme

        declared = prepare_messy_csv(path, sep=delimiter)
        assert declared.report.delimiter == delimiter, scheme
        assert declared.report.data_row_count == ROWS, scheme
        expected_headers = [field.name for field in datagen.SCHEMA]
        assert declared.headers == [*expected_headers, _SMARTCSV_OVERFLOW_COLUMN], scheme
