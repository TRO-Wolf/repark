"""SQL-HARDEN-1 measured matrix - the verdict per program, and the two engine halves it joins.

pins: sql-harden-1-cutover-shapes/C-001, C-003
"""

from __future__ import annotations

from _sql_harden_cutover_repark import REPARK
from _sql_harden_cutover_spark import SPARK

VERDICTS: dict[str, str] = {
    "s1-ctas-if-fresh": "DIVERGES",
    "s2-merge-idempotent": "DIVERGES",
    "s3-dedup-coalesce-cast": "EQUAL",
    "s4-overwrite-partitions": "DIVERGES",
    "s5-maintenance-calls": "DIVERGES",
    "s6-gold-incremental": "DIVERGES",
    "s7-ctas-if-fresh": "DIVERGES",
    "s7-merge-idempotent": "DIVERGES",
    "s7-overwrite-partitions": "DIVERGES",
    "s8-ctas-cow": "DIVERGES",
    "s8-merge-idempotent-cow": "DIVERGES",
    "s8-overwrite-partitions-cow": "DIVERGES",
    "s9-ctas-cow": "DIVERGES",
    "s9-merge-idempotent-cow": "DIVERGES",
    "s9-overwrite-partitions-cow": "DIVERGES",
}

REGISTRY: dict[str, str] = {
    "s1-ctas-if-fresh": "CUTOVER-CTAS-REQ-1",
    "s2-merge-idempotent": "CUTOVER-MERGE-FILES-1",
    "s3-dedup-coalesce-cast": "CUTOVER-DEDUP-SCHEMA-1",
    "s4-overwrite-partitions": "V3-COV-7",
    "s5-maintenance-calls": "CUTOVER-CTAS-REQ-1",
    "s6-gold-incremental": "V3-COV-7",
    "s7-ctas-if-fresh": "CUTOVER-CTAS-REQ-1",
    "s7-merge-idempotent": "CUTOVER-MERGE-FILES-1",
    "s7-overwrite-partitions": "V3-COV-7",
    "s8-ctas-cow": "CUTOVER-CTAS-REQ-1",
    "s8-merge-idempotent-cow": "CUTOVER-CTAS-REQ-1",
    "s8-overwrite-partitions-cow": "V3-COV-7",
    "s9-ctas-cow": "CUTOVER-CTAS-REQ-1",
    "s9-merge-idempotent-cow": "CUTOVER-CTAS-REQ-1",
    "s9-overwrite-partitions-cow": "V3-COV-7",
}

__all__ = ["REGISTRY", "REPARK", "SPARK", "VERDICTS"]
