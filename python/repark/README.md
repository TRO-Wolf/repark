# repark

A near-drop-in **PySpark** API on a pure-Rust, **no-JVM** Apache Iceberg engine.

```python
from repark import ReparkSession   # was: from pyspark.sql import SparkSession

# Drop-in alias for existing scripts:
# from repark import SparkSession
```

Compute runs in Rust (Apache DataFusion + iceberg-rust + Arrow); data crosses the Python boundary
as Apache Arrow, zero-copy. See the [repository root](../../README.md) for architecture and status.
