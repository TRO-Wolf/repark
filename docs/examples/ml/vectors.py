"""Dense and sparse vectors: construction, values, indexing, and the vector type tag.

pins: ex-27-ml/C-002
"""

from __future__ import annotations

from repark.spark import ml

COVERS: list[str] = [
    "ml.DenseVector",
    "ml.SparseVector",
    "ml.Vector",
    "ml.VectorUDT",
    "ml.Vectors",
]


def expect(label: str, got: object, wanted: object) -> None:
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def main() -> None:
    """Run the Spark-measured vector contents, indexing, and type-tag answers."""
    dense = ml.Vectors.dense(1.0, 0.0, 3.0)
    expect("dense.class", type(dense).__name__, "DenseVector")
    expect("dense.isinstance.DenseVector", isinstance(dense, ml.DenseVector), True)
    expect("dense.isinstance.Vector", isinstance(dense, ml.Vector), True)
    expect("dense.toArray", [float(value) for value in dense.toArray()], [1.0, 0.0, 3.0])
    expect("dense.numNonzeros", int(dense.numNonzeros()), 2)
    expect("dense[0]", float(dense[0]), 1.0)
    expect("dense[1]", float(dense[1]), 0.0)
    expect("dense.str", str(dense), "[1.0,0.0,3.0]")
    expect(
        "Vectors.zeros", [float(value) for value in ml.Vectors.zeros(3).toArray()], [0.0, 0.0, 0.0]
    )

    sparse = ml.Vectors.sparse(5, [1, 3], [1.0, 2.0])
    expect("sparse.class", type(sparse).__name__, "SparseVector")
    expect("sparse.isinstance.SparseVector", isinstance(sparse, ml.SparseVector), True)
    expect("sparse.isinstance.Vector", isinstance(sparse, ml.Vector), True)
    expect(
        "sparse.toArray",
        [float(value) for value in sparse.toArray()],
        [0.0, 1.0, 0.0, 2.0, 0.0],
    )
    expect("sparse.numNonzeros", int(sparse.numNonzeros()), 2)
    expect("sparse[0]", float(sparse[0]), 0.0)
    expect("sparse[1]", float(sparse[1]), 1.0)
    expect("sparse.indices", [int(index) for index in sparse.indices], [1, 3])
    expect("sparse.values", [float(value) for value in sparse.values], [1.0, 2.0])
    expect(
        "sparse.from.dict",
        [float(value) for value in ml.Vectors.sparse(4, {1: 1.0, 3: 5.5}).toArray()],
        [0.0, 1.0, 0.0, 5.5],
    )
    expect("dense.eq.sparse", ml.Vectors.dense(0.0, 1.0, 0.0, 2.0, 0.0) == sparse, True)

    constructed = ml.DenseVector([1.0, 0.0, 3.0])
    expect(
        "DenseVector.ctor.toArray",
        [float(value) for value in constructed.toArray()],
        [1.0, 0.0, 3.0],
    )
    sparse_ctor = ml.SparseVector(5, [1, 3], [1.0, 2.0])
    expect(
        "SparseVector.ctor.toArray",
        [float(value) for value in sparse_ctor.toArray()],
        [0.0, 1.0, 0.0, 2.0, 0.0],
    )

    udt = ml.VectorUDT()
    expect("VectorUDT.simpleString", udt.simpleString(), "vector")
    expect("VectorUDT.repr", repr(udt), "VectorUDT()")


if __name__ == "__main__":
    main()
