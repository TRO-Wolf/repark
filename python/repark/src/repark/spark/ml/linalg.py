"""Dense and sparse vectors with Spark-compatible logical schema markers.

Dense columns use Arrow ``FixedSizeList<float64>[n]``. Sparse columns use
``{size: int32, indices: list<int32>, values: list<float64>}``. Mixed dense widths are refused.
``vector`` is a logical schema tag, not a SQL cast target.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from repark.errors import IllegalArgumentException, PySparkTypeError
from repark.spark.types import (
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
        """Return the vector dimension."""
        raise NotImplementedError

    def toArray(self) -> list[float]:
        """Return dense float values."""
        raise NotImplementedError

    def numNonzeros(self) -> int:
        """Return the count of non-zero entries."""
        raise NotImplementedError


class DenseVector(Vector):
    """Vector backed by a fixed-length float list."""

    def __init__(self, values: Sequence[float] | Iterable[float]) -> None:
        """Copy values into a float list."""
        self._values = [float(item) for item in values]

    def size(self) -> int:
        """Return the vector dimension."""
        return len(self._values)

    def toArray(self) -> list[float]:
        """Return a copy of the values."""
        return list(self._values)

    def numNonzeros(self) -> int:
        """Return the count of non-zero values."""
        return sum(1 for value in self._values if value != 0.0)

    def __len__(self) -> int:
        """Return the vector dimension."""
        return len(self._values)

    def __getitem__(self, index: int) -> float:
        """Return the value at ``index``."""
        return self._values[index]

    def __eq__(self, other: object) -> bool:
        """Compare values with another vector or sequence."""
        if isinstance(other, DenseVector):
            return self._values == other._values
        if isinstance(other, SparseVector):
            return self._values == other.toArray()
        if isinstance(other, (list, tuple)):
            return self._values == [float(item) for item in other]
        return NotImplemented

    def __repr__(self) -> str:
        """Return Spark-like bracketed values."""
        body = ",".join(str(value) for value in self._values)
        return f"[{body}]"

    def __str__(self) -> str:
        """Return the display representation."""
        return repr(self)


class SparseVector(Vector):
    """Sparse vector represented by size, sorted indices, and values."""

    def __init__(
        self,
        size: int,
        *args: Any,
    ) -> None:
        """Construct from indices and values or an index-to-value mapping."""
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
            # Spark requires sorted indices. Preserve input values while sorting.
            pairs = sorted(zip(self._indices, self._values, strict=True))
            self._indices = [index for index, _ in pairs]
            self._values = [value for _, value in pairs]
        if len(set(self._indices)) != len(self._indices):
            raise IllegalArgumentException("SparseVector indices must be unique")

    def size(self) -> int:
        """Return the declared dimension."""
        return self._size

    def toArray(self) -> list[float]:
        """Return dense values with absent entries set to zero."""
        result = [0.0] * self._size
        for index, value in zip(self._indices, self._values, strict=True):
            result[index] = value
        return result

    def numNonzeros(self) -> int:
        """Return the count of non-zero stored values."""
        return sum(1 for value in self._values if value != 0.0)

    @property
    def indices(self) -> list[int]:
        """Return a copy of stored indices."""
        return list(self._indices)

    @property
    def values(self) -> list[float]:
        """Return a copy of stored values."""
        return list(self._values)

    def __len__(self) -> int:
        """Return the vector dimension."""
        return self._size

    def __getitem__(self, index: int) -> float:
        """Return the value at ``index``, or zero when it is absent."""
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
        """Compare vector values through their dense forms."""
        if isinstance(other, (DenseVector, SparseVector)):
            return self.toArray() == other.toArray()
        return NotImplemented

    def __repr__(self) -> str:
        """Return Spark-like tuple values."""
        return f"({self._size},{self._indices},{self._values})"

    def __str__(self) -> str:
        """Return the display representation."""
        return repr(self)

    def as_struct_dict(self) -> dict[str, Any]:
        """Return an Arrow- and JSON-friendly sparse payload."""
        return {
            "size": self._size,
            "indices": list(self._indices),
            "values": list(self._values),
        }


class Vectors:
    """Factory for dense and sparse vectors."""

    @staticmethod
    def dense(*args: Any) -> DenseVector:
        """Build a dense vector from variadic values or one list or tuple."""
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            return DenseVector(args[0])
        return DenseVector(args)

    @staticmethod
    def sparse(size: int, *args: Any) -> SparseVector:
        """Build a sparse vector."""
        return SparseVector(size, *args)

    @staticmethod
    def zeros(size: int) -> DenseVector:
        """Build a dense zero vector of ``size``."""
        return DenseVector([0.0] * size)


class VectorUDT(DataType):
    """Schema marker for ML vector columns with Spark ``vector`` display type.

    ``sqlType`` includes ``type: int32`` plus nullable ``size``, ``indices``, and ``values``.
    The tag is logical metadata, not a SQL cast target.
    """

    def _engine_type(self) -> str:
        """Return the logical vector type tag."""
        return "vector"

    def simpleString(self) -> str:
        """Return the Spark display name."""
        return "vector"

    def typeName(self) -> str:
        """Return the type name."""
        return "vector"

    def jsonValue(self) -> dict[str, object]:
        """Return the JSON type descriptor."""
        return {"type": "vector", "class": "repark.spark.ml.linalg.VectorUDT"}

    def sqlType(self) -> StructType:
        """Return the SQL struct with ``type`` and nullable vector fields."""
        return StructType(
            [
                StructField("type", IntegerType(), False),
                StructField("size", IntegerType(), True),
                StructField("indices", ArrayType(IntegerType(), False), True),
                StructField("values", ArrayType(DoubleType(), False), True),
            ]
        )

    def __repr__(self) -> str:
        """Return the constructor representation."""
        return "VectorUDT()"


def is_vector(value: Any) -> bool:
    """Return whether ``value`` is a Repark or vector-shaped object."""
    return isinstance(value, Vector) or (
        hasattr(value, "toArray") and hasattr(value, "size") and callable(value.toArray)
    )


def sparse_struct_type() -> StructType:
    """Return sparse fields with these types:

    ``size: int32``, ``indices: list<int32>``, and ``values: list<float64>``.
    """
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
