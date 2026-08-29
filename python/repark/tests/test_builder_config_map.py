"""R-TAIL — ``ReparkSession.Builder.config(..., map=...)`` PySpark 3.4+ parity.

Oracle: live PySpark 4.1.2 under zulu-17 (2026-07-28). Signature::

    config(key=None, value=None, conf=None, *, map=None) -> Builder

Live precedence (verbatim):

* ``conf is not None`` → apply ``conf.getAll()``; ignore map / key / value (no error).
* else ``map is not None`` → apply ``map.items()``; ignore key / value (no error).
* else store ``key → to_str(value)`` (Spark ``pyspark.sql.utils.to_str``: bool → lowercase,
  ``None`` stays ``None``, else ``str(...)``).

"""

from __future__ import annotations

import pytest

from repark import ReparkSession
from repark.errors import IllegalArgumentException


def test_config_map_sets_multiple_keys() -> None:
    builder = ReparkSession.builder.config(
        map={
            "spark.app.name": "from-map",
            "foo.bar": "baz",
            "spark.sql.shuffle.partitions": "7",
        }
    )
    assert builder._config["spark.app.name"] == "from-map"
    assert builder._config["foo.bar"] == "baz"
    assert builder._config["spark.sql.shuffle.partitions"] == "7"
    spark = builder.getOrCreate()
    try:
        # Arrow path proves the session is live (shuffle=7 is accepted).
        table = spark.sql("SELECT 1 AS n").to_arrow()
        assert table.column("n").to_pylist() == [1]
    finally:
        spark.stop()


def test_config_map_coerces_int_values_to_str() -> None:
    # Oracle: map={'a': 7} → _options {'a': '7'}
    builder = ReparkSession.builder.config(map={"a": 7})
    assert builder._config["a"] == "7"


def test_config_to_str_bool_and_none() -> None:
    """Spark to_str: True→'true', False→'false', None→None (not 'None'/'True').

    Mutation guard (C1-Q-001): bare ``str(True)`` yields ``'True'`` and ``str(None)`` yields
    ``'None'`` — both fail this pin. A None value on an int knob must not break ``_lookup_int``
    with ``int('None')``; session still builds (key treated as unset).
    """
    builder = ReparkSession.builder.config(
        map={
            "spark.some.flag": True,
            "spark.other.flag": False,
            "spark.sql.shuffle.partitions": None,
        }
    )
    assert builder._config["spark.some.flag"] == "true"
    assert builder._config["spark.other.flag"] == "false"
    assert builder._config["spark.sql.shuffle.partitions"] is None
    builder_kv = ReparkSession.builder.config("spark.some.flag", True)
    assert builder_kv._config["spark.some.flag"] == "true"
    # getOrCreate with None on a load-bearing key: unset (default), not IllegalArgumentException.
    spark = ReparkSession.builder.config(map={"spark.sql.shuffle.partitions": None}).getOrCreate()
    try:
        table = spark.sql("SELECT 1 AS n").to_arrow()
        assert table.column("n").to_pylist() == [1]
    finally:
        spark.stop()


def test_config_map_empty_is_noop() -> None:
    builder = ReparkSession.builder.config(map={})
    assert builder._config == {}


def test_config_map_and_conf_merge_into_existing_builder_config() -> None:
    """Sequential map/conf update-merge into builder._config (C4-Q-001).

    A wholesale replace mutation (``self._config = {k: _to_str(v) for …}``) on the map or
    conf arm passes same-key overwrite pins; these disjoint-key keep+add and empty
    map/conf-after-prior pins fail it: prior entries stay, new ones are added.
    """
    builder_kv_map = ReparkSession.builder.config("spark.app.name", "keep").config(
        map={"foo.bar": "baz"}
    )
    assert builder_kv_map._config == {
        "spark.app.name": "keep",
        "foo.bar": "baz",
    }

    builder_map_map = ReparkSession.builder.config(map={"a": "1"}).config(map={"b": "2"})
    assert builder_map_map._config == {"a": "1", "b": "2"}

    builder_empty_map = ReparkSession.builder.config("spark.app.name", "keep").config(map={})
    assert builder_empty_map._config == {"spark.app.name": "keep"}

    class _FakeConf:
        def getAll(self) -> list[tuple[str, str]]:  # noqa: N802 — SparkConf surface
            return [("from-conf", "1")]

    builder_kv_conf = ReparkSession.builder.config("spark.app.name", "keep").config(
        conf=_FakeConf()
    )
    assert builder_kv_conf._config == {
        "spark.app.name": "keep",
        "from-conf": "1",
    }

    class _EmptyConf:
        def getAll(self) -> list[tuple[str, str]]:  # noqa: N802
            return []

    builder_empty_conf = ReparkSession.builder.config("spark.app.name", "keep").config(
        conf=_EmptyConf()
    )
    assert builder_empty_conf._config == {"spark.app.name": "keep"}


def test_config_map_then_kv_overwrite() -> None:
    builder = ReparkSession.builder.config(map={"spark.app.name": "from-map"}).config(
        "spark.app.name", "from-kv"
    )
    assert builder._config["spark.app.name"] == "from-kv"


def test_config_kv_then_map_overwrite() -> None:
    builder = ReparkSession.builder.config("spark.app.name", "from-kv").config(
        map={"spark.app.name": "from-map"}
    )
    assert builder._config["spark.app.name"] == "from-map"


def test_config_kv_and_map_then_conf_same_key_overwrite() -> None:
    """Sequential conf arm same-key overwrite (C7-Q-001).

    A residual ``setdefault`` / ``if key not in self._config`` mutation on conf stays green
    under the C4-Q-001 disjoint-merge and map/kv overwrite pins. These pins fail it: conf
    must replace the prior value for the same key; prior disjoint conf keys stay.
    """

    class _FakeConf:
        def getAll(self) -> list[tuple[str, str]]:  # noqa: N802 — SparkConf surface
            return [("spark.app.name", "via-conf")]

    builder_kv_conf = ReparkSession.builder.config("spark.app.name", "from-kv").config(
        conf=_FakeConf()
    )
    assert builder_kv_conf._config == {"spark.app.name": "via-conf"}
    assert "from-kv" not in builder_kv_conf._config.values()

    builder_map_conf = ReparkSession.builder.config(map={"spark.app.name": "from-map"}).config(
        conf=_FakeConf()
    )
    assert builder_map_conf._config == {"spark.app.name": "via-conf"}
    assert "from-map" not in builder_map_conf._config.values()

    class _FirstConf:
        def getAll(self) -> list[tuple[str, str]]:  # noqa: N802
            return [("spark.app.name", "first-conf"), ("keep.key", "stay")]

    class _SecondConf:
        def getAll(self) -> list[tuple[str, str]]:  # noqa: N802
            return [("spark.app.name", "second-conf")]

    builder_conf_conf = ReparkSession.builder.config(conf=_FirstConf()).config(conf=_SecondConf())
    assert builder_conf_conf._config == {
        "spark.app.name": "second-conf",
        "keep.key": "stay",
    }
    assert "first-conf" not in builder_conf_conf._config.values()


def test_config_map_plus_key_value_map_wins_no_error() -> None:
    """map ≻ key/value is exclusive apply (C2-Q-001), not merge-then-overwrite.

    Oracle: key+value+map together succeeds; only map is applied. Non-overlapping keys fail a
    merge-then-overwrite mutation: kv's ``k1`` must not leak into ``_config``.
    """
    builder = ReparkSession.builder.config(
        key="k1",
        value="v1",
        map={"k2": "v2"},
    )
    assert builder._config == {"k2": "v2"}
    assert "k1" not in builder._config
    assert "v1" not in builder._config.values()
    builder_overlap = ReparkSession.builder.config(
        key="k1",
        value="v1",
        map={"k2": "v2", "k1": "from-map"},
    )
    assert builder_overlap._config == {"k2": "v2", "k1": "from-map"}


def test_config_map_not_dict_raises_attribute_error() -> None:
    # Oracle: AttributeError: 'str' object has no attribute 'items'
    with pytest.raises(AttributeError, match=r"items"):
        ReparkSession.builder.config(map="not-a-dict")  # type: ignore[arg-type]
    with pytest.raises(AttributeError, match=r"items"):
        ReparkSession.builder.config(map=[("a", "1")])  # type: ignore[arg-type]


def test_config_conf_missing_get_all_raises_attribute_error() -> None:
    """conf without getAll → AttributeError matching getAll (C8-Q-003).

    A residual soft-skip mutation (``hasattr(conf, 'getAll')`` / try/except → empty apply)
    stays green under the FakeConf happy-path pins; this pin fails it: fail loud, even when
    map is also present — conf must not soft-empty and fall through to map.
    """
    with pytest.raises(AttributeError, match=r"getAll"):
        ReparkSession.builder.config(conf=object())
    with pytest.raises(AttributeError, match=r"getAll"):
        ReparkSession.builder.config(map={"from-map": "1"}, conf=object())


def test_config_positional_kv_still_works() -> None:
    builder = ReparkSession.builder.config("spark.app.name", "positional")
    assert builder._config["spark.app.name"] == "positional"


def test_config_conf_duck_typed_get_all() -> None:
    class _FakeConf:
        def getAll(self) -> list[tuple[str, str]]:  # noqa: N802 — SparkConf surface
            return [("spark.app.name", "via-conf"), ("foo", "1")]

    builder = ReparkSession.builder.config(conf=_FakeConf())
    assert builder._config["spark.app.name"] == "via-conf"
    assert builder._config["foo"] == "1"


def test_config_conf_to_str_bool_and_none() -> None:
    """conf arm applies Spark ``to_str`` (C3-Q-001); map/kv alone do not pin this branch.

    Mutation: store raw ``conf_value`` instead of ``_to_str(conf_value)``. String-only FakeConf
    fixtures stay green. Duck-typed ``getAll() → [('spark.sql.shuffle.partitions', True)]`` then
    yields ``int(True) == 1`` and ``getOrCreate`` silently builds with partitions=1; correct
    ``to_str`` stores ``'true'`` and ``_lookup_int`` raises ``IllegalArgumentException``.
    """

    class _FakeConf:
        def getAll(self) -> list[tuple[str, object]]:  # noqa: N802 — SparkConf surface
            return [
                ("spark.some.flag", True),
                ("spark.other.flag", False),
                ("spark.unset.knob", None),
            ]

    builder = ReparkSession.builder.config(conf=_FakeConf())
    assert builder._config["spark.some.flag"] == "true"
    assert builder._config["spark.other.flag"] == "false"
    assert builder._config["spark.unset.knob"] is None

    # None on a load-bearing key via conf: treated as unset (same as map path).
    class _FakeConfNonePartitions:
        def getAll(self) -> list[tuple[str, object]]:  # noqa: N802
            return [("spark.sql.shuffle.partitions", None)]

    spark = ReparkSession.builder.config(conf=_FakeConfNonePartitions()).getOrCreate()
    try:
        table = spark.sql("SELECT 1 AS n").to_arrow()
        assert table.column("n").to_pylist() == [1]
    finally:
        spark.stop()

    # Load-bearing True must not build: int(True)==1 would silently succeed without to_str.
    class _FakeConfTruePartitions:
        def getAll(self) -> list[tuple[str, object]]:  # noqa: N802
            return [("spark.sql.shuffle.partitions", True)]

    with pytest.raises(IllegalArgumentException, match=r"must be an integer"):
        ReparkSession.builder.config(conf=_FakeConfTruePartitions()).getOrCreate()


def test_config_conf_takes_precedence_over_map() -> None:
    # Oracle: conf branch wins; map ignored when conf is not None.
    class _FakeConf:
        def getAll(self) -> list[tuple[str, str]]:  # noqa: N802
            return [("from-conf", "1")]

    builder = ReparkSession.builder.config(
        map={"from-map": "2"},
        conf=_FakeConf(),
    )
    assert builder._config == {"from-conf": "1"}


def test_config_empty_conf_still_wins_over_map_same_call() -> None:
    """Empty getAll() conf still excludes map in the same call (C5-Q-002).

    ``if conf is not None`` is exclusive: empty getAll is a no-op on keys, not a fall-through
    to map. A residual non-empty-only mutation stays green under the non-empty conf≻map and
    C4-Q-001 pins; these pins fail it.
    """

    class _EmptyConf:
        def getAll(self) -> list[tuple[str, str]]:  # noqa: N802 — SparkConf surface
            return []

    builder = ReparkSession.builder.config(
        map={"from-map": "2", "spark.app.name": "via-map"},
        conf=_EmptyConf(),
    )
    assert builder._config == {}
    assert "from-map" not in builder._config
    assert "via-map" not in builder._config.values()

    builder_prior = ReparkSession.builder.config("prior.key", "keep").config(
        map={"from-map": "2"},
        conf=_EmptyConf(),
    )
    assert builder_prior._config == {"prior.key": "keep"}
    assert "from-map" not in builder_prior._config


def test_config_empty_map_and_empty_conf_still_exclude_kv_same_call() -> None:
    """Empty map={} / empty conf still exclude key/value in the same call (C6-Q-001).

    ``conf is not None`` and ``map is not None`` are exclusive even when empty: empty is a
    no-op on keys, not a fall-through to kv. A residual ``if map:`` / ``if conf.getAll():``
    mutation stays green under the C2-Q-001, C1-Q-002, C5-Q-002, and C4-Q-001 pins; these
    pins fail it.
    """

    class _EmptyConf:
        def getAll(self) -> list[tuple[str, str]]:  # noqa: N802 — SparkConf surface
            return []

    builder_empty_map = ReparkSession.builder.config(
        key="k1",
        value="v1",
        map={},
    )
    assert builder_empty_map._config == {}
    assert "k1" not in builder_empty_map._config
    assert "v1" not in builder_empty_map._config.values()

    builder_empty_conf = ReparkSession.builder.config(
        key="k1",
        value="v1",
        conf=_EmptyConf(),
    )
    assert builder_empty_conf._config == {}
    assert "k1" not in builder_empty_conf._config
    assert "v1" not in builder_empty_conf._config.values()

    builder_prior_map = ReparkSession.builder.config("prior.key", "keep").config(
        key="k1",
        value="v1",
        map={},
    )
    assert builder_prior_map._config == {"prior.key": "keep"}
    assert "k1" not in builder_prior_map._config

    builder_prior_conf = ReparkSession.builder.config("prior.key", "keep").config(
        key="k1",
        value="v1",
        conf=_EmptyConf(),
    )
    assert builder_prior_conf._config == {"prior.key": "keep"}
    assert "k1" not in builder_prior_conf._config


def test_config_conf_takes_precedence_over_kv() -> None:
    """conf ≻ key/value in the same call (C1-Q-002).

    Without this pin conf≻kv can be dropped while conf≻map and map≻kv pins stay green.
    """

    class _FakeConf:
        def getAll(self) -> list[tuple[str, str]]:  # noqa: N802
            return [("spark.app.name", "via-conf")]

    builder = ReparkSession.builder.config(
        key="spark.app.name",
        value="via-kv",
        conf=_FakeConf(),
    )
    assert builder._config == {"spark.app.name": "via-conf"}
    assert "via-kv" not in builder._config.values()


def test_config_map_load_bearing_knob_validated_at_get_or_create() -> None:
    # map= path must still hit SAF-006 shuffle validation.
    with pytest.raises(IllegalArgumentException, match=r"INVALID_CONF_VALUE\.REQUIREMENT"):
        ReparkSession.builder.config(map={"spark.sql.shuffle.partitions": "0"}).getOrCreate()


def test_config_starstar_dict_is_not_the_api() -> None:
    # ``**dict`` unpacking is NOT PySpark API — keyword args must be the named parameters.
    with pytest.raises(TypeError):
        ReparkSession.builder.config(**{"spark.app.name": "nope"})  # type: ignore[arg-type]
