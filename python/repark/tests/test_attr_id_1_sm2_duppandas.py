from __future__ import annotations

from pathlib import Path
from typing import Any

import _sm2_shared as sm2
import pytest

pytest.importorskip("pandas")

from repark.errors import AnalysisException
from repark.spark import functions as spark_functions


def _params_of(error: Exception) -> dict[str, str]:
    getter = getattr(error, "getMessageParameters", None)
    if callable(getter):
        try:
            return dict(getter() or {})
        except Exception:
            return {}
    return {}


def _expected_ambiguous_message(name: str, references: str) -> str:
    return (
        f"[AMBIGUOUS_REFERENCE] Reference `{name}` is ambiguous, "
        f"could be: [{references}]. SQLSTATE: 42704"
    )


def _seeing_map(seen: dict[str, Any]) -> Any:
    def cap_map(batches: Any) -> Any:
        import pandas

        for pdf in batches:
            seen.setdefault("cols", []).append(list(pdf.columns))
        yield pandas.DataFrame({"a": [1]})

    return cap_map


def _folded_join(session: Any) -> Any:
    upper = session.createDataFrame([(1, "x"), (3, "y")], ["ID", "T"])
    lower = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    return upper.alias("r").join(
        lower.alias("l"),
        spark_functions.col("r.ID") == spark_functions.col("l.id"),
    )


def test_map_in_pandas_over_self_join_refuses_ambiguous_reference(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-map-self")
    frame = sm2._self_join(session)
    seen: dict[str, Any] = {}
    refused = sm2._refusal_of(
        lambda: frame.mapInPandas(_seeing_map(seen), schema="a long").collect()
    )
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "AMBIGUOUS_REFERENCE"
    assert sm2._sql_state_of(refused) == "42704"
    assert str(refused).splitlines()[0] == _expected_ambiguous_message("id", "`l`.`id`, `r`.`id`")
    assert _params_of(refused) == {"name": "`id`", "referenceNames": "[`l`.`id`, `r`.`id`]"}
    assert seen == {}
    session.stop()


def test_map_in_pandas_over_mixed_join_refuses_ambiguous_reference(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-map-mixed")
    frame = sm2._mixed_join(session)
    seen: dict[str, Any] = {}
    refused = sm2._refusal_of(
        lambda: frame.mapInPandas(_seeing_map(seen), schema="a long").collect()
    )
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "AMBIGUOUS_REFERENCE"
    assert sm2._sql_state_of(refused) == "42704"
    assert str(refused).splitlines()[0] == _expected_ambiguous_message("id", "`l`.`id`, `r`.`id`")
    assert seen == {}
    session.stop()


def test_map_in_pandas_over_using_join_names_first_duplicate_key(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-map-using")
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    frame = left.alias("l").join(left.alias("r"), "id", "inner")
    assert frame.columns == ["id", "s", "v", "s", "v"]
    seen: dict[str, Any] = {}
    refused = sm2._refusal_of(
        lambda: frame.mapInPandas(_seeing_map(seen), schema="a long").collect()
    )
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "AMBIGUOUS_REFERENCE"
    assert sm2._sql_state_of(refused) == "42704"
    assert str(refused).splitlines()[0] == _expected_ambiguous_message("s", "`l`.`s`, `r`.`s`")
    assert _params_of(refused) == {"name": "`s`", "referenceNames": "[`l`.`s`, `r`.`s`]"}
    assert seen == {}
    session.stop()


def test_map_in_pandas_over_cross_join_lists_bare_candidates(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-map-cross")
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    frame = left.crossJoin(left)
    seen: dict[str, Any] = {}
    refused = sm2._refusal_of(
        lambda: frame.mapInPandas(_seeing_map(seen), schema="a long").collect()
    )
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "AMBIGUOUS_REFERENCE"
    assert sm2._sql_state_of(refused) == "42704"
    assert str(refused).splitlines()[0] == _expected_ambiguous_message("id", "`id`, `id`")
    assert seen == {}
    session.stop()


def test_map_in_pandas_over_qualified_select_refuses_ambiguous_reference(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-map-qualsel")
    frame = sm2._self_join(session).select(spark_functions.col("l.id"), spark_functions.col("r.id"))
    assert frame.columns == ["id", "id"]
    seen: dict[str, Any] = {}
    refused = sm2._refusal_of(
        lambda: frame.mapInPandas(_seeing_map(seen), schema="a long").collect()
    )
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "AMBIGUOUS_REFERENCE"
    assert sm2._sql_state_of(refused) == "42704"
    assert str(refused).splitlines()[0] == _expected_ambiguous_message("id", "`l`.`id`, `r`.`id`")
    assert seen == {}
    session.stop()


def test_map_in_pandas_over_star_select_refuses_ambiguous_reference(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-map-star")
    frame = sm2._self_join(session).select("*")
    seen: dict[str, Any] = {}
    refused = sm2._refusal_of(
        lambda: frame.mapInPandas(_seeing_map(seen), schema="a long").collect()
    )
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "AMBIGUOUS_REFERENCE"
    assert sm2._sql_state_of(refused) == "42704"
    assert str(refused).splitlines()[0] == _expected_ambiguous_message("id", "`l`.`id`, `r`.`id`")
    assert seen == {}
    session.stop()


def test_map_in_pandas_over_duplicate_literals_refuses_ambiguous_reference(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-map-litdup")
    left = session.createDataFrame([(1, "a", 10)], ["id", "s", "v"])
    frame = left.select(spark_functions.lit(1).alias("a"), spark_functions.lit(2).alias("a"))
    seen: dict[str, Any] = {}
    refused = sm2._refusal_of(
        lambda: frame.mapInPandas(_seeing_map(seen), schema="a long").collect()
    )
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "AMBIGUOUS_REFERENCE"
    assert sm2._sql_state_of(refused) == "42704"
    assert str(refused).splitlines()[0] == _expected_ambiguous_message("a", "`a`, `a`")
    assert seen == {}
    session.stop()


def test_map_in_pandas_over_folded_join_reports_first_spelling(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-map-fold")
    frame = _folded_join(session)
    assert frame.columns == ["ID", "T", "id", "s", "v"]
    seen: dict[str, Any] = {}
    refused = sm2._refusal_of(
        lambda: frame.mapInPandas(_seeing_map(seen), schema="a long").collect()
    )
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "AMBIGUOUS_REFERENCE"
    assert sm2._sql_state_of(refused) == "42704"
    assert str(refused).splitlines()[0] == _expected_ambiguous_message("ID", "`l`.`ID`, `r`.`ID`")
    assert _params_of(refused) == {"name": "`ID`", "referenceNames": "[`l`.`ID`, `r`.`ID`]"}
    assert seen == {}
    session.stop()


def test_map_in_arrow_over_self_join_refuses_ambiguous_reference(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-mia-self")
    frame = sm2._self_join(session)
    refused = sm2._refusal_of(lambda: frame.mapInArrow(lambda batches: batches, schema="a long"))
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "AMBIGUOUS_REFERENCE"
    assert sm2._sql_state_of(refused) == "42704"
    assert str(refused).splitlines()[0] == _expected_ambiguous_message("id", "`l`.`id`, `r`.`id`")
    session.stop()


def test_map_in_pandas_refusal_fires_before_the_function_runs(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-map-eager")
    frame = sm2._self_join(session)
    seen: dict[str, Any] = {}
    refused = sm2._refusal_of(lambda: frame.mapInPandas(_seeing_map(seen), schema="a long"))
    assert isinstance(refused, AnalysisException)
    assert sm2._condition_of(refused) == "AMBIGUOUS_REFERENCE"
    assert seen == {}
    session.stop()


def test_map_in_pandas_over_same_origin_duplicates_runs(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-map-dupselect")
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    frame = left.select("id", "id")
    seen: dict[str, Any] = {}
    rows = frame.mapInPandas(_seeing_map(seen), schema="a long").collect()
    assert [row["a"] for row in rows] == [1]
    assert seen["cols"] == [["id", "id"]]
    for columns in seen["cols"]:
        assert not any("__repark_" in name for name in columns)
    session.stop()


def test_map_in_pandas_same_origin_duplicate_input_names_divergence(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-map-divergence")
    left = session.createDataFrame([(1, "a", 10)], ["id", "s", "v"])
    frame = left.select("id", "id")
    seen: dict[str, Any] = {}
    frame.mapInPandas(_seeing_map(seen), schema="a long").collect()
    assert seen["cols"] == [["id", "id"]]
    session.stop()


def test_map_in_pandas_over_plain_frame_runs_with_display_columns(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-map-plain")
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    seen: dict[str, Any] = {}
    rows = left.mapInPandas(_seeing_map(seen), schema="a long").collect()
    assert [row["a"] for row in rows] == [1]
    assert seen["cols"] == [["id", "s", "v"]]
    session.stop()


def test_map_in_pandas_over_folded_join_runs_when_case_sensitive(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-map-sensitive")
    session.conf.set("spark.sql.caseSensitive", "true")
    try:
        frame = _folded_join(session)
        assert frame.columns == ["ID", "T", "id", "s", "v"]
        seen: dict[str, Any] = {}
        rows = frame.mapInPandas(_seeing_map(seen), schema="a long").collect()
        assert [row["a"] for row in rows] == [1]
        assert seen["cols"] == [["ID", "T", "id", "s", "v"]]
    finally:
        session.conf.set("spark.sql.caseSensitive", "false")
        session.stop()


def test_map_in_pandas_still_runs_after_pandas_udf_select(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-map-after-pudf")
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    projected = left.select(
        spark_functions.pandas_udf(lambda pdf: pdf + 1, "long")("id").alias("p"), "s"
    )
    assert projected.columns == ["p", "s"]
    seen: dict[str, Any] = {}
    rows = projected.mapInPandas(_seeing_map(seen), schema="a long").collect()
    assert [row["a"] for row in rows] == [1]
    assert seen["cols"] == [["p", "s"]]
    session.stop()


def test_scalar_pandas_udf_input_series_is_positional_over_plain_frame(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-pudf-plain")
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    seen: dict[str, Any] = {}
    rows = (
        left.select(
            spark_functions.pandas_udf(
                lambda pdf: seen.update(name=str(pdf.name)) or pdf + 1, "long"
            )("id").alias("p")
        )
        .orderBy("p")
        .collect()
    )
    assert [row["p"] for row in rows] == [2, 3]
    assert seen["name"] == "_0"
    session.stop()


def test_scalar_pandas_udf_input_series_is_positional_over_qualified_twin(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-pudf-twin")
    frame = sm2._self_join(session)
    seen: dict[str, Any] = {}
    rows = (
        frame.select(
            spark_functions.pandas_udf(
                lambda pdf: seen.update(name=str(pdf.name)) or pdf + 1, "long"
            )(spark_functions.col("l.id")).alias("p")
        )
        .orderBy("p")
        .collect()
    )
    assert [row["p"] for row in rows] == [2, 3]
    assert seen["name"] == "_0"
    session.stop()


def test_scalar_pandas_udf_input_series_carry_no_internal_names(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-pudf-names")
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    seen: dict[str, Any] = {}
    rows = (
        left.select(
            spark_functions.pandas_udf(
                lambda first, second: (
                    seen.update(names=[str(first.name), str(second.name)]) or first + second
                ),
                "long",
            )("id", "v").alias("p")
        )
        .orderBy("p")
        .collect()
    )
    assert [row["p"] for row in rows] == [11, 22]
    assert seen["names"][0] == "_0"
    assert not any("__repark_" in name for name in seen["names"])
    session.stop()


def test_grouped_agg_pandas_udf_input_series_is_positional(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-gagg-plain")
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    seen: dict[str, Any] = {}
    rows = (
        left.groupBy("s")
        .agg(
            spark_functions.pandas_udf(
                lambda pdf: seen.update(name=str(pdf.name)) or pdf.max(),
                "long",
                spark_functions.PandasUDFType.GROUPED_AGG,
            )("id").alias("m")
        )
        .orderBy("s")
        .collect()
    )
    assert [(row["s"], row["m"]) for row in rows] == [("a", 1), ("b", 2)]
    assert seen["name"] == "_0"
    session.stop()


def test_to_pandas_over_self_join_carries_display_names(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-topandas-self")
    frame = sm2._self_join(session)
    pdf = frame.toPandas()
    assert list(pdf.columns) == ["id", "s", "v", "id", "s", "v"]
    assert pdf.values.tolist() == [[1, "a", 10, 1, "a", 10], [2, "b", 20, 2, "b", 20]]
    session.stop()


def test_to_pandas_over_mixed_join_carries_display_names(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-topandas-mixed")
    frame = sm2._mixed_join(session)
    pdf = frame.toPandas()
    assert list(pdf.columns) == ["id", "s", "v", "id", "t"]
    session.stop()


def test_to_pandas_over_same_origin_duplicates_carries_display_names(
    tmp_path: Path,
) -> None:
    session = sm2._open(tmp_path, "sm2-duppandas-topandas-dupselect")
    left = session.createDataFrame([(1, "a", 10)], ["id", "s", "v"])
    pdf = left.select("id", "id").toPandas()
    assert list(pdf.columns) == ["id", "id"]
    session.stop()
