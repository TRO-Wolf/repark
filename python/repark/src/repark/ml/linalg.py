"""Vector types — dense :class:`DenseVector` / sparse :class:`SparseVector` / :class:`Vectors`.

Campaign design decision 1 (docs/ml-design.md):

* dense → Arrow ``FixedSizeList<float64>[n]`` (fixed width per column)
* sparse → struct ``{size: int32, indices: list<int32>, values: list<float64>}``
* mixed dense widths in one column → loud :class:`~repark.errors.AnalysisException`
  naming the v1 limitation (do **not** fall back to variable ``List<float64>``)

``Vectors.dense`` / ``Vectors.sparse`` constructors mirror Spark. ``VectorUDT`` is a
schema marker for createDataFrame / display parity (simpleString ``vector``).
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from repark.errors import IllegalArgumentException, PySparkTypeError
from repark.types import (
    ArrayType,
    DataType,
    DoubleType,
    IntegerType,
    StructField,
    StructType,
)


class Vector:
    """Base class for dense and sparse vectors."""

    def size(self) -> int:
        """Dimensionality."""
        raise NotImplementedError

    def toArray(self) -> list[float]:
        """Dense float list of length ``size()``."""
        raise NotImplementedError

    def numNonzeros(self) -> int:
        """Count of non-zero entries."""
        raise NotImplementedError


class DenseVector(Vector):
    """Dense vector backed by a fixed-length list of floats."""

    def __init__(self, values: Sequence[float] | Iterable[float]) -> None:
        """Store values as ``list[float]``."""
        self._values = [float(item) for item in values]

    def size(self) -> int:
        """Length of the dense array."""
        return len(self._values)

    def toArray(self) -> list[float]:
        """Copy of underlying values."""
        return list(self._values)

    def numNonzeros(self) -> int:
        """Count of non-zeros."""
        return sum(1 for value in self._values if value != 0.0)

    def __len__(self) -> int:
        """Same as :meth:`size`."""
        return len(self._values)

    def __getitem__(self, index: int) -> float:
        """Index into the dense array."""
        return self._values[index]

    def __eq__(self, other: object) -> bool:
        """Value equality with dense or sparse (via dense array)."""
        if isinstance(other, DenseVector):
            return self._values == other._values
        if isinstance(other, SparseVector):
            return self._values == other.toArray()
        if isinstance(other, (list, tuple)):
            return self._values == [float(item) for item in other]
        return NotImplemented

    def __repr__(self) -> str:
        """Spark-like ``[1.0,2.0]`` form."""
        body = ",".join(str(value) for value in self._values)
        return f"[{body}]"

    def __str__(self) -> str:
        """Same as repr for display."""
        return repr(self)


class SparseVector(Vector):
    """Sparse vector: size + sorted indices + values."""

    def __init__(
        self,
        size: int,
        *args: Any,
    ) -> None:
        """Construct from ``(size, indices, values)`` or ``(size, {index: value})``."""
        if size < 0:
            raise IllegalArgumentException(f"SparseVector size must be >= 0, got {size}")
        self._size = int(size)
        if len(args) == 1 and isinstance(args[0], dict):
            items = sorted((int(index), float(value)) for index, value in args[0].items())
            self._indices = [index for index, _ in items]
            self._values = [value for _, value in items]
        elif len(args) == 2:
            indices, values = args
            self._indices = [int(index) for index in indices]
            self._values = [float(value) for value in values]
            if len(self._indices) != len(self._values):
                raise IllegalArgumentException(
                    "SparseVector indices and values must have the same length"
                )
        else:
            raise PySparkTypeError("SparseVector expects (size, indices, values) or (size, dict)")
        for index in self._indices:
            if index < 0 or index >= self._size:
                raise IllegalArgumentException(
                    f"SparseVector index {index} out of range for size {self._size}"
                )
        if self._indices != sorted(self._indices):
            # Spark requires sorted unique indices — sort for convenience but refuse dups.
            pairs = sorted(zip(self._indices, self._values, strict=True))
            self._indices = [index for index, _ in pairs]
            self._values = [value for _, value in pairs]
        if len(set(self._indices)) != len(self._indices):
            raise IllegalArgumentException("SparseVector indices must be unique")

    def size(self) -> int:
        """Declared dimensionality."""
        return self._size

    def toArray(self) -> list[float]:
        """Materialize dense form."""
        result = [0.0] * self._size
        for index, value in zip(self._indices, self._values, strict=True):
            result[index] = value
        return result

    def numNonzeros(self) -> int:
        """Count of stored non-zeros (values may include explicit 0.0)."""
        return sum(1 for value in self._values if value != 0.0)

    @property
    def indices(self) -> list[int]:
        """Non-zero indices."""
        return list(self._indices)

    @property
    def values(self) -> list[float]:
        """Non-zero values."""
        return list(self._values)

    def __len__(self) -> int:
        """Same as :meth:`size`."""
        return self._size

    def __getitem__(self, index: int) -> float:
        """Element at ``index`` (0.0 when absent — sparse zero)."""
        position = int(index)
        if position < 0 or position >= self._size:
            raise IndexError(f"SparseVector index {position} out of range for size {self._size}")
        for stored_index, value in zip(self._indices, self._values, strict=True):
            if stored_index == position:
                return value
            if stored_index > position:
                break
        return 0.0

    def __eq__(self, other: object) -> bool:
        """Value equality via dense array."""
        if isinstance(other, (DenseVector, SparseVector)):
            return self.toArray() == other.toArray()
        return NotImplemented

    def __repr__(self) -> str:
        """Spark-like ``(5,[1,3],[1.0,2.0])`` form."""
        return f"({self._size},{self._indices},{self._values})"

    def __str__(self) -> str:
        """Same as repr."""
        return repr(self)

    def as_struct_dict(self) -> dict[str, Any]:
        """Arrow/JSON-friendly sparse struct payload."""
        return {
            "size": self._size,
            "indices": list(self._indices),
            "values": list(self._values),
        }


class Vectors:
    """Factory for dense and sparse vectors (Spark ``Vectors``)."""

    @staticmethod
    def dense(*args: Any) -> DenseVector:
        """Build a :class:`DenseVector` from values or a single sequence."""
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            return DenseVector(args[0])
        return DenseVector(args)

    @staticmethod
    def sparse(size: int, *args: Any) -> SparseVector:
        """Build a :class:`SparseVector``."""
        return SparseVector(size, *args)

    @staticmethod
    def zeros(size: int) -> DenseVector:
        """Dense zero vector of ``size``."""
        return DenseVector([0.0] * size)


class VectorUDT(DataType):
    """Schema marker for ML vector columns (Spark ``VectorUDT``).

    ``simpleString()`` is ``vector``. createDataFrame binding expands dense cells to
    FixedSizeList and sparse cells to the sparse struct (see session hooks).
    """

    def _engine_type(self) -> str:
        """Logical type tag (not a SQL cast target)."""
        return "vector"

    def simpleString(self) -> str:
        """Spark display ``vector``."""
        return "vector"

    def typeName(self) -> str:
        """Type name ``vector``."""
        return "vector"

    def jsonValue(self) -> dict[str, object]:
        """JSON descriptor."""
        return {"type": "vector", "class": "repark.ml.linalg.VectorUDT"}

    def sqlType(self) -> StructType:
        """Underlying SQL struct used by Spark's VectorUDT (for reference)."""
        return StructType(
            [
                StructField("type", IntegerType(), False),
                StructField("size", IntegerType(), True),
                StructField("indices", ArrayType(IntegerType(), False), True),
                StructField("values", ArrayType(DoubleType(), False), True),
            ]
        )

    def __repr__(self) -> str:
        """``VectorUDT()``."""
        return "VectorUDT()"


def is_vector(value: Any) -> bool:
    """True if ``value`` is a repark or duck-typed Spark vector."""
    return isinstance(value, Vector) or (
        hasattr(value, "toArray") and hasattr(value, "size") and callable(value.toArray)
    )


def sparse_struct_type() -> StructType:
    """Arrow/SQL struct schema for sparse vectors."""
    return StructType(
        [
            StructField("size", IntegerType(), False),
            StructField("indices", ArrayType(IntegerType(), False), False),
            StructField("values", ArrayType(DoubleType(), False), False),
        ]
    )


__all__ = [
    "ArrayType",
    "DenseVector",
    "SparseVector",
    "Vector",
    "VectorUDT",
    "Vectors",
    "is_vector",
    "sparse_struct_type",
]
