"""Spark-parity differential test harness for repark.

The guarantee "DataFrame ops map one-to-one with PySpark" is only credible if it is *checked*. This
package compares the engine's output against a reference and fails on any divergence.

Two modes (see :mod:`repark_parity.compare` for the comparison core):

* **check** (routine CI, no JVM): compare repark output against recorded Spark *golden*
  fixtures stored in the test corpus.
* **record** (occasional, needs ``pyspark`` + a JVM): run the same operation on real Spark and
  refresh the golden fixtures. Gated behind the ``record`` optional dependency so the no-JVM
  promise holds for everyone else.
"""

from __future__ import annotations

from repark_parity.compare import FrameMismatchError, assert_frames_equal

__all__ = ["FrameMismatchError", "assert_frames_equal"]
