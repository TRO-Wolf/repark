"""R-ML-SKELETON oracles — Param / Pipeline / uid / persistence / vectors.

Live-pyspark differentials ``importorskip`` when Java/pyspark cannot launch (facade CI is
JVM-free by design). EXPECTED-ERROR pins never skip.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, IllegalArgumentException, PySparkTypeError
from repark.spark.ml import (
    Model,
    Param,
    Params,
    Pipeline,
    PipelineModel,
    Vectors,
)
from repark.spark.ml.linalg import DenseVector, SparseVector, VectorUDT
from repark.spark.ml.pipeline import (
    REPARK_ML_FORMAT,
    REPARK_ML_VERSION,
    _ConstantColumnEstimator,
    _ConstantColumnModel,
)
from repark.spark.ml.util import _random_uid

# Helpers


def _session() -> ReparkSession:
    return ReparkSession.builder.appName("ml-skeleton-test").getOrCreate()


def _maybe_live_spark():
    """Return a live SparkSession or skip (JVM / pyspark missing)."""
    pytest.importorskip("pyspark")
    java_home = os.environ.get("JAVA_HOME", "")
    # Prefer Java 17+ when available (Spark 4.x needs class file 61).
    if not java_home or "11" in java_home:
        for candidate in (
            "/usr/lib/jvm/zulu-17-amd64",
            "/usr/lib/jvm/java-17-openjdk-amd64",
            "/usr/lib/jvm/java-21-openjdk-amd64",
        ):
            if Path(candidate).is_dir():
                os.environ["JAVA_HOME"] = candidate
                os.environ["PATH"] = f"{candidate}/bin:" + os.environ.get("PATH", "")
                break
    try:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[1]")
            .appName("repark-ml-oracle")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        return spark
    except Exception as error:
        pytest.skip(f"live pyspark unavailable: {error}")


# Uid + Param semantics


def test_random_uid_spark_shape() -> None:
    """Uid is ClassName + underscore + 8 hex chars."""
    uid = _random_uid("StringIndexer")
    assert re.fullmatch(r"StringIndexer_[0-9a-f]{8}", uid), uid


def test_param_get_or_default_and_explain() -> None:
    """getOrDefault / explainParams / copy surface."""

    class Toy(Params):
        def __init__(self) -> None:
            super().__init__()
            self.maxIter = Param(self, "maxIter", "max iterations.", typeConverter=int)
            self._setDefault(maxIter=10)

    toy = Toy()
    assert toy.getOrDefault(toy.maxIter) == 10
    toy._set(maxIter=3)
    assert toy.getOrDefault("maxIter") == 3
    explained = toy.explainParams()
    assert "maxIter:" in explained
    assert "max iterations" in explained
    assert "current: 3" in explained
    copied = toy.copy()
    assert copied.getOrDefault("maxIter") == 3
    copied2 = toy.copy(extra={toy.maxIter: 9})
    assert copied2.getOrDefault("maxIter") == 9


def test_param_undefined_raises() -> None:
    """Undefined param raises IllegalArgumentException (EXPECTED-ERROR)."""

    class Toy(Params):
        def __init__(self) -> None:
            super().__init__()
            self.seed = Param(self, "seed", "rng seed.")

    toy = Toy()
    with pytest.raises(IllegalArgumentException, match="does not have a default"):
        toy.getOrDefault(toy.seed)


# fit/transform gate — Repark DataFrame only


def test_fit_refuses_foreign_dataframe() -> None:
    """Estimator.fit refuses non-repark frames loud (EXPECTED-ERROR)."""
    est = _ConstantColumnEstimator(output_col="c", value=2.0)
    with pytest.raises(PySparkTypeError, match=r"repark\.dataframe\.DataFrame"):
        est.fit([("not", "a", "frame")])


def test_transform_refuses_pandas_name() -> None:
    """Transformer.transform names the foreign type (EXPECTED-ERROR)."""
    model = _ConstantColumnModel(output_col="c", value=1.0)

    class FakePandas:
        pass

    with pytest.raises(PySparkTypeError, match="FakePandas"):
        model.transform(FakePandas())


# Pipeline fit/transform ordering


def test_pipeline_fit_transform_ordering() -> None:
    """Stages apply left-to-right; estimator becomes model."""
    spark = _session()
    try:
        df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        pipe = Pipeline(
            stages=[
                _ConstantColumnEstimator(output_col="a", value=10.0),
                _ConstantColumnEstimator(output_col="b", value=20.0),
            ]
        )
        model = pipe.fit(df)
        assert isinstance(model, PipelineModel)
        assert len(model.stages) == 2
        assert all(isinstance(stage, Model) for stage in model.stages)
        out = model.transform(df).collect()
        assert sorted(row.asDict()["id"] for row in out) == [1, 2, 3]
        assert all(row.asDict()["a"] == 10.0 for row in out)
        assert all(row.asDict()["b"] == 20.0 for row in out)
    finally:
        spark.stop()


def test_pipeline_passthrough_transformer() -> None:
    """Already-fitted transformers in the stage list pass through without re-fit."""
    spark = _session()
    try:
        df = spark.createDataFrame([(1,)], ["id"])
        pre = _ConstantColumnModel(output_col="pre", value=7.0)
        pipe = Pipeline(stages=[pre, _ConstantColumnEstimator(output_col="post", value=8.0)])
        model = pipe.fit(df)
        row = model.transform(df).collect()[0].asDict()
        assert row["pre"] == 7.0
        assert row["post"] == 8.0
    finally:
        spark.stop()


# Persistence repark-ml v1


def test_pipeline_model_save_load_round_trip() -> None:
    """save → load → identical transform output; layout pins."""
    spark = _session()
    tmp = tempfile.mkdtemp(prefix="repark-ml-")
    try:
        df = spark.createDataFrame([(1,), (2,)], ["id"])
        model = Pipeline(stages=[_ConstantColumnEstimator(output_col="c", value=3.5)]).fit(df)
        path = str(Path(tmp) / "pipe")
        model.write().overwrite().save(path)

        meta = json.loads((Path(path) / "metadata.json").read_text(encoding="utf-8"))
        assert meta["format"] == REPARK_ML_FORMAT
        assert meta["version"] == REPARK_ML_VERSION
        assert meta["kind"] == "PipelineModel"
        assert "pyspark_version" in meta
        stage_dirs = list((Path(path) / "stages").iterdir())
        assert len(stage_dirs) == 1
        assert (stage_dirs[0] / "metadata.json").is_file()
        assert (stage_dirs[0] / "fitted" / "params.parquet").is_file()

        loaded = PipelineModel.load(path)
        original = sorted(
            (row.asDict()["id"], row.asDict()["c"]) for row in model.transform(df).collect()
        )
        restored = sorted(
            (row.asDict()["id"], row.asDict()["c"]) for row in loaded.transform(df).collect()
        )
        assert original == restored == [(1, 3.5), (2, 3.5)]
    finally:
        spark.stop()
        shutil.rmtree(tmp, ignore_errors=True)


def test_pipeline_model_save_atomic_overwrite() -> None:
    """M7: overwrite is write-new + rename — never rmtree-then-write (M4 C2-SAF-001).

    After a successful overwrite the path is a complete repark-ml tree; a failed
    mid-write must not leave the destination deleted (staging aborted, original kept
    until commit renames).
    """
    spark = _session()
    tmp = tempfile.mkdtemp(prefix="repark-ml-atomic-")
    try:
        df = spark.createDataFrame([(1,), (2,)], ["id"])
        model_a = Pipeline(stages=[_ConstantColumnEstimator(output_col="c", value=1.0)]).fit(df)
        model_b = Pipeline(stages=[_ConstantColumnEstimator(output_col="c", value=9.0)]).fit(df)
        path = Path(tmp) / "pipe"
        model_a.write().save(str(path))
        assert (path / "metadata.json").is_file()
        marker = path / "stages"
        assert marker.is_dir()
        # Overwrite replaces with model_b; no hollow empty dir window observable after return.
        model_b.write().overwrite().save(str(path))
        assert (path / "metadata.json").is_file()
        # No leftover staging/aside siblings from a successful commit.
        siblings = [p.name for p in path.parent.iterdir()]
        assert not any(".repark-ml-staging-" in name for name in siblings)
        assert not any(".repark-ml-aside-" in name for name in siblings)
        loaded = PipelineModel.load(str(path))
        values = sorted(row.asDict()["c"] for row in loaded.transform(df).collect())
        assert values == [9.0, 9.0]
        # Refuse without overwrite leaves existing tree intact.
        with pytest.raises(IllegalArgumentException, match=r"already exists"):
            model_a.write().save(str(path))
        assert (path / "metadata.json").is_file()
        still = PipelineModel.load(str(path))
        assert sorted(row.asDict()["c"] for row in still.transform(df).collect()) == [9.0, 9.0]
        # File target overwrite: replace file with directory tree (TOCTOU residual M7 C5).
        file_path = Path(tmp) / "as_file"
        file_path.write_text("not-a-dir", encoding="utf-8")
        model_a.write().overwrite().save(str(file_path))
        assert file_path.is_dir()
        assert (file_path / "metadata.json").is_file()
    finally:
        spark.stop()
        shutil.rmtree(tmp, ignore_errors=True)


def test_pipeline_model_save_race_aside_cleanup() -> None:
    """M7 octo C1: failed publish after move-aside must not leak aside when target reoccupied.

    Simulates concurrent-overwrite race via rename hook: after target→aside, peer recreates
    target so staging→target fails → commit cleans aside (no `.repark-ml-aside-*` sibling).
    """
    from repark.spark.ml.pipeline import _begin_atomic_save, _commit_atomic_save

    tmp = tempfile.mkdtemp(prefix="repark-ml-race-")
    try:
        target = Path(tmp) / "pipe"
        target.mkdir()
        (target / "metadata.json").write_text("ORIGINAL\n", encoding="utf-8")
        staging = _begin_atomic_save(target, overwrite=True)
        (staging / "metadata.json").write_text("NEW\n", encoding="utf-8")

        real_rename = Path.rename

        def _rename_with_peer(self: Path, new: Path | str) -> Path:
            destination = Path(new)
            result = real_rename(self, destination)
            # After commit moves target → aside, peer re-occupies the original path.
            if ".repark-ml-aside-" in destination.name:
                original = Path(tmp) / "pipe"
                original.mkdir(exist_ok=True)
                (original / "metadata.json").write_text("PEER\n", encoding="utf-8")
                (original / "blocker").write_text("x", encoding="utf-8")
            return result

        # Patch only during commit so begin/abort paths stay normal.
        Path.rename = _rename_with_peer  # type: ignore[method-assign]
        try:
            with pytest.raises(IllegalArgumentException, match=r"atomic save could not publish"):
                _commit_atomic_save(staging, target, overwrite=True)
        finally:
            Path.rename = real_rename  # type: ignore[method-assign]

        # Peer tree remains; our aside + staging must not linger.
        assert target.is_dir()
        assert (target / "metadata.json").read_text(encoding="utf-8") == "PEER\n"
        siblings = [p.name for p in Path(tmp).iterdir()]
        assert not any(".repark-ml-aside-" in name for name in siblings), siblings
        assert not any(".repark-ml-staging-" in name for name in siblings), siblings

        # overwrite=false TOCTOU: target created between begin and commit → refuse, keep old.
        staging2 = _begin_atomic_save(Path(tmp) / "fresh", overwrite=False)
        (staging2 / "metadata.json").write_text("x\n", encoding="utf-8")
        raced = Path(tmp) / "fresh"
        raced.mkdir()
        (raced / "metadata.json").write_text("old\n", encoding="utf-8")
        with pytest.raises(IllegalArgumentException, match=r"already exists"):
            _commit_atomic_save(staging2, raced, overwrite=False)
        assert (raced / "metadata.json").read_text(encoding="utf-8") == "old\n"
        siblings2 = [p.name for p in Path(tmp).iterdir()]
        assert not any(".repark-ml-staging-" in name for name in siblings2), siblings2
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_persistence_never_writes_training_rows() -> None:
    """Hard pin: no file under the save path contains input data values."""
    spark = _session()
    tmp = tempfile.mkdtemp(prefix="repark-ml-rows-")
    try:
        secret = "TRAINING_ROW_SECRET_XYZ_9911"
        df = spark.createDataFrame([(secret,), ("other",)], ["label"])
        model = Pipeline(stages=[_ConstantColumnEstimator(output_col="c", value=1.0)]).fit(df)
        path = Path(tmp) / "pipe"
        model.write().overwrite().save(str(path))
        for file_path in path.rglob("*"):
            if not file_path.is_file():
                continue
            raw = file_path.read_bytes()
            assert secret.encode("utf-8") not in raw, f"training data leaked into {file_path}"
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                continue
            assert secret not in text, f"training data leaked into {file_path}"
    finally:
        spark.stop()
        shutil.rmtree(tmp, ignore_errors=True)


# Vectors + createDataFrame / collect


def test_vectors_constructors() -> None:
    """Vectors.dense / sparse constructors and value equality."""
    dense = Vectors.dense(1.0, 2.0, 3.0)
    assert isinstance(dense, DenseVector)
    assert dense.toArray() == [1.0, 2.0, 3.0]
    sparse = Vectors.sparse(5, [1, 3], [1.0, 2.0])
    assert isinstance(sparse, SparseVector)
    assert sparse.toArray() == [0.0, 1.0, 0.0, 2.0, 0.0]
    assert VectorUDT().simpleString() == "vector"


def test_dense_vector_create_dataframe_round_trip() -> None:
    """Dense vectors via createDataFrame → FixedSizeList Arrow → collect values."""
    spark = _session()
    try:
        df = spark.createDataFrame(
            [(Vectors.dense(1.0, 2.0),), (Vectors.dense(3.0, 4.0),)],
            ["features"],
        )
        table = df.to_arrow()
        # FixedSizeList of width 2
        field = table.schema.field("features")
        assert str(field.type).startswith("fixed_size_list") or "list" in str(field.type).lower()
        rows = df.collect()
        values = [list(row.asDict()["features"]) for row in rows]
        assert values == [[1.0, 2.0], [3.0, 4.0]]
    finally:
        spark.stop()


def test_sparse_vector_create_dataframe_round_trip() -> None:
    """Sparse vectors via createDataFrame → struct Arrow → collect dict values."""
    spark = _session()
    try:
        df = spark.createDataFrame(
            [(Vectors.sparse(4, [0, 2], [1.5, 2.5]),)],
            ["features"],
        )
        table = df.to_arrow()
        field = table.schema.field("features")
        assert "struct" in str(field.type).lower() or hasattr(field.type, "names")
        row = df.collect()[0].asDict()["features"]
        assert row["size"] == 4
        assert list(row["indices"]) == [0, 2]
        assert list(row["values"]) == [1.5, 2.5]
    finally:
        spark.stop()


def test_mixed_dense_widths_loud() -> None:
    """Mixed dense widths in one column → AnalysisException (EXPECTED-ERROR)."""
    spark = _session()
    try:
        with pytest.raises(AnalysisException, match="fixed-width"):
            spark.createDataFrame(
                [(Vectors.dense(1.0, 2.0),), (Vectors.dense(1.0, 2.0, 3.0),)],
                ["features"],
            )
    finally:
        spark.stop()


# Live pyspark Param oracle (optional)


def test_live_pyspark_uid_and_explain_shape() -> None:
    """Live oracle: uid regex + explainParams key names (importorskip JVM)."""
    spark = _maybe_live_spark()
    try:
        from pyspark.ml.feature import StringIndexer

        indexer = StringIndexer(inputCol="a", outputCol="b")
        assert re.fullmatch(r"StringIndexer_[0-9a-f]{8,}", indexer.uid), indexer.uid
        explained = indexer.explainParams()
        assert "inputCol:" in explained
        assert "outputCol:" in explained
        assert "stringOrderType:" in explained
    finally:
        spark.stop()
