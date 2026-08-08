"""M2 R-ML-FEATURE oracles — plan-built transformers + pipeline e2e.

Live-pyspark differentials importorskip when JVM unavailable. EXPECTED-ERROR never skip.
Float bar: 1e-12 relative where noted; exact otherwise.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, IllegalArgumentException, UnsupportedOperationException
from repark.ml import Pipeline
from repark.ml.feature import (
    IDF,
    QUANTILE_FAMILY_STATUS,
    Binarizer,
    Bucketizer,
    CountVectorizer,
    Imputer,
    IndexToString,
    MaxAbsScaler,
    MinMaxScaler,
    OneHotEncoder,
    PolynomialExpansion,
    QuantileDiscretizer,
    RegexTokenizer,
    RobustScaler,
    SQLTransformer,
    StandardScaler,
    StopWordsRemover,
    StringIndexer,
    Tokenizer,
    VectorAssembler,
)


def _session() -> ReparkSession:
    return ReparkSession.builder.appName("ml-feature-test").getOrCreate()


def _close(rel: float, a: float, b: float, tol: float = 1e-12) -> bool:
    if a == b:
        return True
    denom = max(abs(a), abs(b), 1e-15)
    return abs(a - b) / denom <= tol


def test_vector_assembler_basic() -> None:
    """Assembler stacks numeric cols into dense array."""
    spark = _session()
    try:
        df = spark.createDataFrame([(1.0, 2.0), (3.0, 4.0)], ["a", "b"])
        out = VectorAssembler(inputCols=["a", "b"], outputCol="features").transform(df).collect()
        rows = [list(row.asDict()["features"]) for row in out]
        assert rows == [[1.0, 2.0], [3.0, 4.0]]
    finally:
        spark.stop()


def test_vector_assembler_null_error() -> None:
    """handleInvalid=error on nulls (EXPECTED-ERROR)."""
    spark = _session()
    try:
        df = spark.createDataFrame([(1.0, None), (2.0, 3.0)], ["a", "b"])
        with pytest.raises(AnalysisException, match="NULL"):
            VectorAssembler(inputCols=["a", "b"], outputCol="f", handleInvalid="error").transform(
                df
            )
    finally:
        spark.stop()


def test_vector_assembler_sparse_output() -> None:
    """M7: sparseOutput=True emits {size,indices,values} omitting zeros."""
    spark = _session()
    try:
        df = spark.createDataFrame([(0.0, 1.5, 0.0, 2.0)], ["a", "b", "c", "d"])
        out = (
            VectorAssembler(
                inputCols=["a", "b", "c", "d"],
                outputCol="f",
                sparseOutput=True,
            )
            .transform(df)
            .collect()[0]
            .asDict()["f"]
        )
        assert out["size"] == 4
        assert list(out["indices"]) == [1, 3]
        assert list(out["values"]) == pytest.approx([1.5, 2.0])
        # Default remains dense make_array.
        dense = (
            VectorAssembler(inputCols=["a", "b"], outputCol="d2")
            .transform(df.select("a", "b"))
            .collect()[0]
            .asDict()["d2"]
        )
        assert list(dense) == pytest.approx([0.0, 1.5])
    finally:
        spark.stop()


def test_string_indexer_keep_one_hot_encoder_drop_last_matrix() -> None:
    """M7: StringIndexer handleInvalid=keep x OHE dropLast interaction matrix.

    SI keep maps unseen/null → numLabels; OHE keep reserves invalid bucket *before*
    dropLast (M4 C4-L-001). Pins both dropLast True/False with SI-invalid indices.
    """
    spark = _session()
    try:
        train = spark.createDataFrame([("a",), ("b",), ("a",)], ["cat"])
        test = spark.createDataFrame([("a",), ("z",), (None,)], ["cat"])
        si = StringIndexer(inputCol="cat", outputCol="idx", handleInvalid="keep").fit(train)
        assert set(si.labels) == {"a", "b"}
        indexed = si.transform(test)
        # a → 0; unseen z / null → numLabels=2
        idx_by = {}
        for row in indexed.collect():
            payload = row.asDict()
            idx_by[payload["cat"]] = payload["idx"]
        assert idx_by["a"] == 0.0
        assert idx_by["z"] == 2.0
        assert idx_by[None] == 2.0

        # Fit on train-indexed so category_size reflects {0,1}.
        train_idx = si.transform(train)
        # dropLast=False + OHE keep: expanded category_size+1 for invalid bucket.
        ohe_false = OneHotEncoder(
            inputCol="idx",
            outputCol="oh",
            dropLast=False,
            handleInvalid="keep",
        ).fit(train_idx)
        # category_size=2 (indices 0,1); keep → size=3; invalid at index 2.
        assert ohe_false.category_size == 2
        rows_false = {
            row.asDict()["cat"]: row.asDict()["oh"]
            for row in ohe_false.transform(indexed).collect()
        }
        assert rows_false["a"]["size"] == 3
        assert list(rows_false["a"]["indices"]) == [0]
        assert rows_false["z"]["size"] == 3
        assert list(rows_false["z"]["indices"]) == [2]
        assert rows_false[None]["size"] == 3
        assert list(rows_false[None]["indices"]) == [2]

        # dropLast=True + keep: invalid bucket dropped → empty sparse for SI-invalid.
        ohe_true = OneHotEncoder(
            inputCol="idx",
            outputCol="oh",
            dropLast=True,
            handleInvalid="keep",
        ).fit(train_idx)
        rows_true = {
            row.asDict()["cat"]: row.asDict()["oh"] for row in ohe_true.transform(indexed).collect()
        }
        # expanded=3, dropLast → size=2; invalid index 2 dropped → empty.
        assert rows_true["a"]["size"] == 2
        assert list(rows_true["a"]["indices"]) == [0]
        assert rows_true["z"]["size"] == 2
        assert list(rows_true["z"]["indices"]) == []
        assert rows_true[None]["size"] == 2
        assert list(rows_true[None]["indices"]) == []

        # Illegal SI handleInvalid refuses loud at fit (not silent keep).
        with pytest.raises(IllegalArgumentException, match=r"handleInvalid"):
            StringIndexer(inputCol="cat", outputCol="idx", handleInvalid="bogus").fit(train)
    finally:
        spark.stop()


def test_string_indexer_frequency_desc() -> None:
    """Labels ordered by frequency desc; transform indices."""
    spark = _session()
    try:
        df = spark.createDataFrame([("a",), ("b",), ("a",), ("c",), ("a",), ("b",)], ["cat"])
        model = StringIndexer(inputCol="cat", outputCol="idx").fit(df)
        assert model.labels[0] == "a"  # most frequent
        assert set(model.labels) == {"a", "b", "c"}
        out = model.transform(df).collect()
        by_cat = {row.asDict()["cat"]: row.asDict()["idx"] for row in out}
        assert by_cat["a"] == 0.0
    finally:
        spark.stop()


def test_string_indexer_unseen_error() -> None:
    """Unseen label with handleInvalid=error (EXPECTED-ERROR)."""
    spark = _session()
    try:
        train = spark.createDataFrame([("a",), ("b",)], ["cat"])
        model = StringIndexer(inputCol="cat", outputCol="idx").fit(train)
        test = spark.createDataFrame([("z",)], ["cat"])
        with pytest.raises(AnalysisException, match=r"unseen|invalid"):
            model.transform(test)
    finally:
        spark.stop()


def test_index_to_string_round_trip() -> None:
    """IndexToString restores labels."""
    spark = _session()
    try:
        df = spark.createDataFrame([("x",), ("y",), ("x",)], ["cat"])
        model = StringIndexer(inputCol="cat", outputCol="idx").fit(df)
        indexed = model.transform(df)
        restored = IndexToString(inputCol="idx", outputCol="cat2", labels=model.labels).transform(
            indexed
        )
        pairs = [(row.asDict()["cat"], row.asDict()["cat2"]) for row in restored.collect()]
        assert all(left == right for left, right in pairs)
    finally:
        spark.stop()


def test_one_hot_encoder_sparse() -> None:
    """OHE emits sparse struct; dropLast shrinks size."""
    spark = _session()
    try:
        df = spark.createDataFrame([(0.0,), (1.0,), (0.0,)], ["idx"])
        model = OneHotEncoder(inputCol="idx", outputCol="oh", dropLast=True).fit(df)
        # category_size=2 → size=1 with dropLast
        assert model.category_size == 2
        row = model.transform(df).collect()[0].asDict()["oh"]
        assert row["size"] == 1
        assert "indices" in row and "values" in row
    finally:
        spark.stop()


def test_standard_scaler_with_mean_std() -> None:
    """StandardScaler centers and scales (1e-12)."""
    spark = _session()
    try:
        base = spark.createDataFrame([(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)], ["a", "b"])
        assembled = VectorAssembler(inputCols=["a", "b"], outputCol="f").transform(base)
        model = StandardScaler(inputCol="f", outputCol="s", withMean=True, withStd=True).fit(
            assembled
        )
        out = model.transform(assembled).collect()
        # Mean of a is 3, std samp of [1,3,5] = 2.0
        first = list(out[0].asDict()["s"])
        assert _close(1e-12, first[0], (1.0 - 3.0) / 2.0)
    finally:
        spark.stop()


def test_minmax_scaler_01() -> None:
    """MinMax to [0,1]."""
    spark = _session()
    try:
        base = spark.createDataFrame([(0.0,), (5.0,), (10.0,)], ["a"])
        assembled = VectorAssembler(inputCols=["a"], outputCol="f").transform(base)
        model = MinMaxScaler(inputCol="f", outputCol="m").fit(assembled)
        vals = [next(iter(row.asDict()["m"])) for row in model.transform(assembled).collect()]
        assert vals[0] == 0.0
        assert vals[-1] == 1.0
        assert _close(1e-12, vals[1], 0.5)
    finally:
        spark.stop()


def test_max_abs_scaler() -> None:
    """MaxAbs scales by max absolute value."""
    spark = _session()
    try:
        base = spark.createDataFrame([(2.0,), (-4.0,), (1.0,)], ["a"])
        assembled = VectorAssembler(inputCols=["a"], outputCol="f").transform(base)
        model = MaxAbsScaler(inputCol="f", outputCol="m").fit(assembled)
        vals = [next(iter(row.asDict()["m"])) for row in model.transform(assembled).collect()]
        assert _close(1e-12, vals[0], 0.5)
        assert _close(1e-12, vals[1], -1.0)
    finally:
        spark.stop()


def test_minmax_maxabs_nan_tolerant() -> None:
    """octo F-Q1-012: MinMax/MaxAbs exclude NaNs in fit; transform stays plan-valid."""
    spark = _session()
    try:
        base = spark.sql(
            "SELECT make_array(x) AS f FROM (VALUES (1.0), (CAST('NaN' AS DOUBLE)), (3.0)) t(x)"
        )
        mm = MinMaxScaler(inputCol="f", outputCol="m").fit(base)
        assert mm.original_min[0] == 1.0 and mm.original_max[0] == 3.0
        mm_vals = [next(iter(row.asDict()["m"])) for row in mm.transform(base).collect()]
        assert mm_vals[0] == 0.0
        assert mm_vals[2] == 1.0
        ma = MaxAbsScaler(inputCol="f", outputCol="a").fit(base)
        assert ma.max_abs[0] == 3.0
        ma_vals = [next(iter(row.asDict()["a"])) for row in ma.transform(base).collect()]
        assert _close(1e-12, ma_vals[0], 1.0 / 3.0)
    finally:
        spark.stop()


def test_bucketizer_splits() -> None:
    """Bucketizer assigns bucket indices."""
    spark = _session()
    try:
        df = spark.createDataFrame([(-0.5,), (0.5,), (1.5,)], ["x"])
        out = Bucketizer(
            splits=[-float("inf"), 0.0, 1.0, float("inf")],
            inputCol="x",
            outputCol="b",
        ).transform(df)
        buckets = [row.asDict()["b"] for row in out.collect()]
        assert buckets == [0.0, 1.0, 2.0]
    finally:
        spark.stop()


def test_imputer_mean() -> None:
    """Imputer mean fills nulls."""
    spark = _session()
    try:
        df = spark.createDataFrame([(1.0,), (None,), (3.0,)], ["x"])
        model = Imputer(inputCols=["x"], outputCols=["x_out"], strategy="mean").fit(df)
        vals = [row.asDict()["x_out"] for row in model.transform(df).collect()]
        assert vals[0] == 1.0
        assert vals[2] == 3.0
        assert _close(1e-12, vals[1], 2.0)
    finally:
        spark.stop()


def test_imputer_median() -> None:
    """Q1: Imputer(strategy=median) fills nulls with approx p50 (bounds-window)."""
    spark = _session()
    try:
        df = spark.createDataFrame([(1.0,), (None,), (3.0,), (5.0,)], ["x"])
        model = Imputer(inputCols=["x"], outputCols=["x_out"], strategy="median").fit(df)
        vals = [row.asDict()["x_out"] for row in model.transform(df).collect()]
        assert vals[0] == 1.0
        assert vals[2] == 3.0
        assert vals[3] == 5.0
        # Exact median of {1,3,5} is 3.0; t-digest neighbor window [1,5].
        assert 1.0 <= float(vals[1]) <= 5.0
        assert "SHIPPED" in QUANTILE_FAMILY_STATUS
    finally:
        spark.stop()


def test_imputer_median_nan_missing() -> None:
    """Q1/octo F-Q1-002: default missingValue=NaN treats NaN as missing (not crash)."""
    spark = _session()
    try:
        df = spark.sql("SELECT * FROM (VALUES (1.0), (CAST('NaN' AS DOUBLE)), (3.0), (5.0)) t(x)")
        model = Imputer(inputCols=["x"], outputCols=["x_out"], strategy="median").fit(df)
        # Fit must exclude NaNs → median of {1,3,5} window [1,5], finite replacement.
        rep = float(model.replacements["x"])
        assert rep == rep  # not NaN
        assert 1.0 <= rep <= 5.0
        vals = [row.asDict()["x_out"] for row in model.transform(df).collect()]
        # NaN row replaced with fitted median.
        assert vals[0] == 1.0
        assert vals[1] == rep
        assert vals[2] == 3.0
        assert vals[3] == 5.0
    finally:
        spark.stop()


def test_imputer_missing_value_sentinel() -> None:
    """Q1/octo F-Q1-002: non-NaN missingValue is excluded from fit and replaced."""
    spark = _session()
    try:
        df = spark.sql("SELECT * FROM (VALUES (1.0), (0.0), (3.0)) t(x)")
        model = Imputer(
            inputCols=["x"], outputCols=["x_out"], strategy="mean", missingValue=0.0
        ).fit(df)
        # Mean of {1,3} only.
        assert abs(float(model.replacements["x"]) - 2.0) < 1e-12
        vals = [row.asDict()["x_out"] for row in model.transform(df).collect()]
        assert vals == [1.0, 2.0, 3.0]
    finally:
        spark.stop()


def test_imputer_fit_temp_view_cleaned_on_error() -> None:
    """Q1/octo F-Q1-005: fit exception must not leak TEMPORARY views."""
    spark = _session()
    try:
        before = {table.name for table in spark.catalog.listTables()}
        with pytest.raises(AnalysisException):
            Imputer(inputCols=["nope"], outputCols=["o"], strategy="median").fit(
                spark.sql("SELECT 1.0 AS x")
            )
        after = {table.name for table in spark.catalog.listTables()}
        leaked = {name for name in after - before if name.startswith("__repark_")}
        assert not leaked, f"temp views leaked after fit error: {leaked}"
    finally:
        spark.stop()


def test_string_indexer_fit_temp_view_cleaned_on_error() -> None:
    """octo F-Q1-014: StringIndexer fit error path drops temp views."""
    spark = _session()
    try:
        before = {table.name for table in spark.catalog.listTables()}
        with pytest.raises(AnalysisException):
            StringIndexer(inputCol="nope", outputCol="i").fit(spark.sql("SELECT 1 AS x"))
        after = {table.name for table in spark.catalog.listTables()}
        leaked = {name for name in after - before if name.startswith("__repark_")}
        assert not leaked, f"temp views leaked after fit error: {leaked}"
    finally:
        spark.stop()


def test_robust_scaler_basic() -> None:
    """Q1: RobustScaler centers on median / scales by IQR (plan-built)."""
    spark = _session()
    try:
        base = spark.createDataFrame([(1.0,), (2.0,), (3.0,), (4.0,), (100.0,)], ["a"])
        assembled = VectorAssembler(inputCols=["a"], outputCol="f").transform(base)
        model = RobustScaler(
            inputCol="f",
            outputCol="s",
            withCentering=True,
            withScaling=True,
        ).fit(assembled)
        out = model.transform(assembled).collect()
        scaled = [next(iter(row.asDict()["s"])) for row in out]
        # Median ~3 on {1,2,3,4,100}; IQR ~ p75-p25. Centered values straddle 0.
        assert min(scaled) < 0.0 < max(scaled)
        # Outlier 100 maps far positive after robust scale.
        assert scaled[-1] > scaled[2]
    finally:
        spark.stop()


def test_quantile_discretizer_basic() -> None:
    """Q1: QuantileDiscretizer fit → Bucketizer with quantile splits."""
    spark = _session()
    try:
        df = spark.createDataFrame([(float(i),) for i in range(10)], ["x"])
        model = QuantileDiscretizer(
            inputCol="x",
            outputCol="b",
            numBuckets=2,
            relativeError=0.001,
        ).fit(df)
        assert isinstance(model, Bucketizer)
        buckets = sorted(row.asDict()["b"] for row in model.transform(df).collect())
        # Two buckets → labels 0 and 1 present on a 0..9 linear fixture.
        assert set(buckets) <= {0.0, 1.0}
        assert 0.0 in buckets and 1.0 in buckets
    finally:
        spark.stop()


def test_regex_tokenizer_gaps_true() -> None:
    """Q1: RegexTokenizer gaps=True splits on pattern; minTokenLength filters."""
    spark = _session()
    try:
        df = spark.createDataFrame([("Hello   World a BB",)], ["text"])
        out = (
            RegexTokenizer(
                inputCol="text",
                outputCol="words",
                pattern=r"\s+",
                gaps=True,
                minTokenLength=2,
                toLowercase=True,
            )
            .transform(df)
            .collect()
        )
        words = list(out[0].asDict()["words"])
        assert words == ["hello", "world", "bb"]
    finally:
        spark.stop()


def test_regex_tokenizer_preserves_row_order() -> None:
    """Q1/octo F-Q1-004/009: rid materialized — tokens stay associated with row ids."""
    spark = _session()
    try:
        # UNION ALL stresses non-deterministic scan order (VALUES alone can hide CTE bugs).
        union_sql = " UNION ALL ".join(
            f"SELECT {index} AS id, 'tok{index} extra' AS text" for index in range(20)
        )
        df = spark.sql(union_sql)
        out = RegexTokenizer(inputCol="text", outputCol="w").transform(df).collect()
        assert len(out) == 20
        for row in out:
            payload = row.asDict()
            token = next(iter(payload["w"]))
            assert token == f"tok{payload['id']}", f"assoc broken: {payload}"
    finally:
        spark.stop()


def test_imputer_same_input_output_col() -> None:
    """Q1/octo F-Q1-010: in-place impute to the same column name (Spark-compatible)."""
    spark = _session()
    try:
        df = spark.sql("SELECT * FROM (VALUES (1.0), (CAST(NULL AS DOUBLE)), (3.0)) t(x)")
        model = Imputer(inputCols=["x"], outputCols=["x"], strategy="mean").fit(df)
        vals = sorted(
            (row.asDict()["x"] if row.asDict()["x"] is not None else -1.0)
            for row in model.transform(df).collect()
        )
        assert vals == [1.0, 2.0, 3.0]
    finally:
        spark.stop()


def test_regex_tokenizer_gaps_false_stop() -> None:
    """gaps=False is loud STOP (no regexp_extract_all)."""
    spark = _session()
    try:
        df = spark.createDataFrame([("ab12",)], ["text"])
        with pytest.raises(UnsupportedOperationException, match="gaps=False"):
            RegexTokenizer(inputCol="text", outputCol="w", gaps=False).transform(df)
    finally:
        spark.stop()


def test_count_vectorizer_and_idf() -> None:
    """Q1: CountVectorizer vocab + counts; IDF smooth scaling."""
    spark = _session()
    try:
        # Build token arrays via Tokenizer (createDataFrame list-cols not yet Arrow-bound).
        text = spark.createDataFrame(
            [("a b a",), ("b c",), ("a c c",)],
            ["text"],
        )
        df = Tokenizer(inputCol="text", outputCol="tokens").transform(text)
        cv_model = CountVectorizer(inputCol="tokens", outputCol="tf", vocabSize=10).fit(df)
        assert set(cv_model.vocabulary) == {"a", "b", "c"}
        tf = cv_model.transform(df)
        rows = [list(row.asDict()["tf"]) for row in tf.collect()]
        # Vocabulary order is frequency-desc then token; check bag sums.
        assert all(
            abs(sum(vec) - expected) < 1e-9
            for vec, expected in zip(rows, [3.0, 2.0, 3.0], strict=True)
        )
        idf_model = IDF(inputCol="tf", outputCol="tfidf").fit(tf)
        assert len(idf_model.idf) == len(cv_model.vocabulary)
        assert all(weight >= 0.0 for weight in idf_model.idf)
        tfidf_rows = idf_model.transform(tf).collect()
        assert len(tfidf_rows) == 3
        from repark.ml.feature import COUNT_VECTORIZER_STATUS, IDF_STATUS

        assert "SHIPPED" in COUNT_VECTORIZER_STATUS
        assert "SHIPPED" in IDF_STATUS
    finally:
        spark.stop()


def test_count_vectorizer_fractional_min_tf() -> None:
    """Q1/octo F-Q1-006: minTF in [0,1) is a fraction of document tokens."""
    spark = _session()
    try:
        # Doc0: a a a b → 4 tokens; minTF=0.5 → threshold 2 → keep a(3), drop b(1)
        text = spark.sql("SELECT * FROM (VALUES ('a a a b'), ('a b c')) t(text)")
        df = Tokenizer(inputCol="text", outputCol="tokens").transform(text)
        model = CountVectorizer(inputCol="tokens", outputCol="tf", minTF=0.5).fit(df)
        # Force vocab order for assertion: transform uses fitted vocabulary order.
        rows = [list(row.asDict()["tf"]) for row in model.transform(df).collect()]
        # vocabulary frequency-desc: a (2 docs), b (2), c (1) → ['a','b','c'] or a/b tie by name
        assert "a" in model.vocabulary and "b" in model.vocabulary
        index_a = model.vocabulary.index("a")
        index_b = model.vocabulary.index("b")
        assert rows[0][index_a] == 3.0
        assert rows[0][index_b] == 0.0  # 1/4 < 0.5
    finally:
        spark.stop()


def test_tokenizer_basic() -> None:
    """Tokenizer lowercases and splits on space."""
    spark = _session()
    try:
        df = spark.createDataFrame([("Hello World",)], ["text"])
        out = Tokenizer(inputCol="text", outputCol="words").transform(df).collect()
        words = list(out[0].asDict()["words"])
        assert words == ["hello", "world"]
    finally:
        spark.stop()


def test_sql_transformer() -> None:
    """SQLTransformer substitutes __THIS__."""
    spark = _session()
    try:
        df = spark.createDataFrame([(1,), (2,)], ["id"])
        out = (
            SQLTransformer(statement="SELECT id, id * 2 AS doubled FROM __THIS__")
            .transform(df)
            .collect()
        )
        assert sorted(row.asDict()["doubled"] for row in out) == [2, 4]
    finally:
        spark.stop()


def test_binarizer() -> None:
    """Binarizer threshold."""
    spark = _session()
    try:
        df = spark.createDataFrame([(0.2,), (0.8,)], ["x"])
        out = Binarizer(threshold=0.5, inputCol="x", outputCol="b").transform(df)
        vals = [row.asDict()["b"] for row in out.collect()]
        assert vals == [0.0, 1.0]
    finally:
        spark.stop()


def test_polynomial_expansion_degree2_width2() -> None:
    """PolynomialExpansion degree 2 width 2."""
    spark = _session()
    try:
        base = spark.createDataFrame([(2.0, 3.0)], ["a", "b"])
        assembled = VectorAssembler(inputCols=["a", "b"], outputCol="f").transform(base)
        out = PolynomialExpansion(degree=2, inputCol="f", outputCol="p").transform(assembled)
        poly = list(out.collect()[0].asDict()["p"])
        # [x, x^2, y, x*y, y^2] = [2, 4, 3, 6, 9]
        assert poly == [2.0, 4.0, 3.0, 6.0, 9.0]
    finally:
        spark.stop()


def test_pipeline_e2e_assembler_indexer_encoder_scaler() -> None:
    """Multi-stage pipeline vs values+types on collect path."""
    spark = _session()
    try:
        df = spark.createDataFrame(
            [
                ("a", 1.0, 10.0),
                ("b", 2.0, 20.0),
                ("a", 3.0, 30.0),
                ("c", 4.0, 40.0),
            ],
            ["cat", "x", "y"],
        )
        pipe = Pipeline(
            stages=[
                StringIndexer(inputCol="cat", outputCol="cat_idx"),
                OneHotEncoder(inputCol="cat_idx", outputCol="cat_oh", dropLast=True),
                VectorAssembler(inputCols=["x", "y"], outputCol="num"),
                StandardScaler(
                    inputCol="num", outputCol="num_scaled", withMean=False, withStd=True
                ),
            ]
        )
        model = pipe.fit(df)
        out = model.transform(df)
        rows = out.collect()
        assert len(rows) == 4
        # types: cat_idx double, num list, num_scaled list, cat_oh struct
        sample = rows[0].asDict()
        assert isinstance(sample["cat_idx"], float)
        assert isinstance(sample["num"], list)
        assert isinstance(sample["num_scaled"], list)
        assert isinstance(sample["cat_oh"], dict)
        assert set(sample["cat_oh"].keys()) >= {"size", "indices", "values"}
        table = out.to_arrow()
        assert "cat_idx" in table.column_names
        assert "num_scaled" in table.column_names
    finally:
        spark.stop()


def test_live_string_indexer_labels_oracle() -> None:
    """Live pyspark StringIndexer label order (importorskip JVM)."""
    pytest.importorskip("pyspark")
    java_home = os.environ.get("JAVA_HOME", "")
    if not java_home or "11" in java_home:
        for candidate in (
            "/usr/lib/jvm/zulu-17-amd64",
            "/usr/lib/jvm/java-21-openjdk-amd64",
        ):
            if Path(candidate).is_dir():
                os.environ["JAVA_HOME"] = candidate
                os.environ["PATH"] = f"{candidate}/bin:" + os.environ.get("PATH", "")
                break
    try:
        from pyspark.ml.feature import StringIndexer as SparkStringIndexer
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[1]")
            .appName("ml-feat-oracle")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as error:
        pytest.skip(f"live pyspark unavailable: {error}")
    try:
        data = [("a",), ("b",), ("a",), ("c",), ("a",), ("b",)]
        pdf = spark.createDataFrame(data, ["cat"])
        spark_labels = SparkStringIndexer(inputCol="cat", outputCol="idx").fit(pdf).labels
        # repark
        rs = _session()
        try:
            rdf = rs.createDataFrame(data, ["cat"])
            repark_labels = StringIndexer(inputCol="cat", outputCol="idx").fit(rdf).labels
        finally:
            rs.stop()
        assert list(spark_labels) == list(repark_labels)
    finally:
        spark.stop()


def test_sql_transformer_refuses_non_select() -> None:
    """SQLTransformer refuses multi-statement / non-SELECT (octo C1-SEC-001)."""
    spark = _session()
    try:
        df = spark.createDataFrame([(1,)], ["id"])
        with pytest.raises(Exception, match=r"SELECT|single statement"):
            SQLTransformer(statement="DELETE FROM __THIS__").transform(df)
        with pytest.raises(Exception, match="single statement"):
            SQLTransformer(statement="SELECT * FROM __THIS__; SELECT 1").transform(df)
    finally:
        spark.stop()


def test_standard_scaler_single_row_std_fallback() -> None:
    """n=1 stddev_samp is NULL -> scale factor 1.0 (octo C1-L-002)."""
    spark = _session()
    try:
        base = spark.createDataFrame([(3.0, 4.0)], ["a", "b"])
        assembled = VectorAssembler(inputCols=["a", "b"], outputCol="f").transform(base)
        model = StandardScaler(inputCol="f", outputCol="s", withMean=False, withStd=True).fit(
            assembled
        )
        out = list(model.transform(assembled).collect()[0].asDict()["s"])
        assert out == [3.0, 4.0]
    finally:
        spark.stop()


def test_stop_words_remover_after_tokenizer() -> None:
    """StopWordsRemover filters via unnest plan (octo c2)."""
    spark = _session()
    try:
        df = spark.createDataFrame([("the cat and dog",)], ["text"])
        tok = Tokenizer(inputCol="text", outputCol="words").transform(df)
        out = StopWordsRemover(inputCol="words", outputCol="f").transform(tok).collect()
        assert list(out[0].asDict()["f"]) == ["cat", "dog"]
    finally:
        spark.stop()


def test_pipeline_string_indexer_save_load() -> None:
    """Fitted StringIndexer survives PipelineModel save/load (octo c2)."""
    import shutil
    import tempfile
    from pathlib import Path

    from repark.ml import PipelineModel

    spark = _session()
    tmp = tempfile.mkdtemp()
    try:
        df = spark.createDataFrame([("a",), ("b",), ("a",)], ["c"])
        model = Pipeline(stages=[StringIndexer(inputCol="c", outputCol="i")]).fit(df)
        path = str(Path(tmp) / "p")
        model.write().overwrite().save(path)
        loaded = PipelineModel.load(path)
        orig = sorted((r.asDict()["c"], r.asDict()["i"]) for r in model.transform(df).collect())
        got = sorted((r.asDict()["c"], r.asDict()["i"]) for r in loaded.transform(df).collect())
        assert orig == got
    finally:
        spark.stop()
        shutil.rmtree(tmp, ignore_errors=True)


def test_vector_assembler_output_collision() -> None:
    """outputCol already present -> AnalysisException (octo c3)."""
    spark = _session()
    try:
        df = spark.createDataFrame([(1.0, 2.0)], ["a", "features"])
        with pytest.raises(AnalysisException, match=r"already exists"):
            VectorAssembler(inputCols=["a"], outputCol="features").transform(df)
    finally:
        spark.stop()


def test_feature_fit_refuses_list() -> None:
    """Feature estimators refuse non-repark frames (octo c4)."""
    with pytest.raises(Exception, match=r"repark\.dataframe\.DataFrame"):
        StringIndexer(inputCol="c", outputCol="i").fit([("a",)])


def test_one_hot_encoder_all_null_fit() -> None:
    """All-null index col fit -> category_size 0; keep maps nulls to empty sparse (octo c5)."""
    spark = _session()
    try:
        df = spark.createDataFrame([(None,), (None,)], ["idx"])
        model = OneHotEncoder(
            inputCol="idx", outputCol="oh", dropLast=True, handleInvalid="keep"
        ).fit(df)
        assert model.category_size == 0
        row = model.transform(df).collect()[0].asDict()["oh"]
        assert row["size"] == 0
        assert list(row["indices"]) == []
    finally:
        spark.stop()
