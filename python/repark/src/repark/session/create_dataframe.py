"""createDataFrame / inference region marker (r26 T1).

Implementation free functions live in ``_funcs`` and bind into class modules
via ``from repark.session._funcs import *``. Aliases historically defined next
to createDataFrame helpers are re-exported here.
"""

from __future__ import annotations

from repark.session.session_core import ReparkSession

SparkSession = ReparkSession
ReParkSession = ReparkSession
