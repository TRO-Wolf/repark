"""SQL-HARDEN-1 measured matrix - the repark half, recorded 2026-09-04.

pins: sql-harden-1-cutover-shapes/C-001
"""

from __future__ import annotations

from typing import Any

REPARK: dict[str, Any] = {
    "s1-ctas-if-fresh": {
        "statements": [["OK", None]],
        "probes": [
            [
                "OK",
                [
                    ["A", "1.2500", 1, "first", 10],
                    ["A", None, None, None, 10],
                    ["B", "2.5000", 2, "keep", 20],
                ],
            ],
            [
                "OK",
                [
                    ["id", "string", False],
                    ["amount", "decimal128(10, 4)", True],
                    ["units", "int32", True],
                    ["note", "string", True],
                    ["part", "int32", False],
                ],
            ],
            ["OK", [["append"]]],
            ["DEL", []],
            [
                "META",
                [
                    ["format-version", 2],
                    [
                        "schema",
                        [
                            ["id", "string", True],
                            ["ingestion_timestamp", "timestamp", True],
                            ["amount", "decimal(10,4)", False],
                            ["units", "int", False],
                            ["note", "string", False],
                            ["part", "int", True],
                        ],
                    ],
                    [
                        "write-properties",
                        [
                            ["write.delete.mode", "merge-on-read"],
                            ["write.merge.mode", "merge-on-read"],
                            ["write.target-file-size-bytes", "268435456"],
                            ["write.update.mode", "merge-on-read"],
                        ],
                    ],
                    ["next-row-id", None],
                ],
            ],
        ],
    },
    "s2-merge-idempotent": {
        "statements": [["OK", None], ["OK", None], ["OK", None]],
        "probes": [
            ["OK", [["A", "0.0000", 0, "unknown", 10], ["B", "2.5000", 2, "keep", 20]]],
            [
                "OK",
                [
                    ["id", "string", False],
                    ["amount", "decimal128(10, 4)", False],
                    ["units", "int32", False],
                    ["note", "string", False],
                    ["part", "int32", False],
                ],
            ],
            ["OK", [["append"], ["overwrite"], ["overwrite"]]],
            ["DEL", [[1, "PARQUET"]]],
            [
                "META",
                [
                    ["format-version", 2],
                    [
                        "schema",
                        [
                            ["id", "string", True],
                            ["ingestion_timestamp", "timestamp", True],
                            ["amount", "decimal(10,4)", True],
                            ["units", "int", True],
                            ["note", "string", True],
                            ["part", "int", True],
                        ],
                    ],
                    [
                        "write-properties",
                        [
                            ["write.delete.mode", "merge-on-read"],
                            ["write.merge.mode", "merge-on-read"],
                            ["write.target-file-size-bytes", "268435456"],
                            ["write.update.mode", "merge-on-read"],
                        ],
                    ],
                    ["next-row-id", None],
                ],
            ],
            [
                "IDEM",
                [
                    ["OK", [["A", "0.0000", 0, "unknown", 10], ["B", "2.5000", 2, "keep", 20]]],
                    ["OK", [["A", "0.0000", 0, "unknown", 10], ["B", "2.5000", 2, "keep", 20]]],
                    True,
                ],
            ],
        ],
    },
    "s3-dedup-coalesce-cast": {
        "statements": [["OK", [["A", "0.0000", 0, "unknown", 10], ["B", "2.5000", 2, "keep", 20]]]],
        "probes": [
            ["OK", [["A", "0.0000", 0, "unknown", 10], ["B", "2.5000", 2, "keep", 20]]],
            [
                "OK",
                [
                    ["id", "string_view", False],
                    ["amount", "decimal128(10, 4)", False],
                    ["units", "int32", False],
                    ["note", "string", False],
                    ["part", "int32", False],
                ],
            ],
        ],
    },
    "s4-overwrite-partitions": {
        "statements": [["OK", None], ["OK", None], ["OK", None]],
        "probes": [
            ["OK", [["B", "y", 20], ["C", "z", 10]]],
            ["OK", [["id", "string", True], ["note", "string", True], ["part", "int32", True]]],
            ["OK", [["append"], ["overwrite"]]],
            [
                "META",
                [
                    ["format-version", 2],
                    [
                        "schema",
                        [
                            ["id", "string", False],
                            ["note", "string", False],
                            ["part", "int", False],
                        ],
                    ],
                    [
                        "write-properties",
                        [
                            ["write.delete.mode", "merge-on-read"],
                            ["write.merge.mode", "merge-on-read"],
                            ["write.target-file-size-bytes", "268435456"],
                            ["write.update.mode", "merge-on-read"],
                        ],
                    ],
                    ["next-row-id", None],
                ],
            ],
        ],
    },
    "s5-maintenance-calls": {
        "statements": [
            ["OK", None],
            ["OK", None],
            ["OK", None],
            ["OK", [[0, 0, 0, 0, 0, 0]]],
            ["OK", [[0, 0, 0, 0, 0]]],
            ["OK", []],
            ["OK", [[0, 0, 0, 0]]],
        ],
        "probes": [
            [
                "OK",
                [
                    ["A", "0.0000", 0, "unknown", 10],
                    ["B", "2.5000", 2, "keep", 20],
                    ["C", "2.5000", 2, "keep", 30],
                    ["D", "2.5000", 2, "keep", 40],
                ],
            ],
            [
                "OK",
                [
                    ["id", "string", False],
                    ["amount", "decimal128(10, 4)", False],
                    ["units", "int32", False],
                    ["note", "string", False],
                    ["part", "int32", False],
                ],
            ],
            ["OK", [["append"], ["append"], ["append"]]],
            ["DEL", []],
            [
                "META",
                [
                    ["format-version", 2],
                    [
                        "schema",
                        [
                            ["id", "string", True],
                            ["ingestion_timestamp", "timestamp", True],
                            ["amount", "decimal(10,4)", True],
                            ["units", "int", True],
                            ["note", "string", True],
                            ["part", "int", True],
                        ],
                    ],
                    [
                        "write-properties",
                        [
                            ["write.delete.mode", "merge-on-read"],
                            ["write.merge.mode", "merge-on-read"],
                            ["write.target-file-size-bytes", "268435456"],
                            ["write.update.mode", "merge-on-read"],
                        ],
                    ],
                    ["next-row-id", None],
                ],
            ],
        ],
    },
    "s6-gold-incremental": {
        "statements": [
            ["OK", None],
            ["OK", None],
            ["OK", None],
            ["OK", None],
            ["OK", None],
            ["OK", None],
            ["OK", None],
            ["OK", None],
            ["OK", None],
            ["OK", None],
            ["ERROR", "Error during planning: Invalid function 'date'."],
            ["ERROR", "Error during planning: table 'ice.cut.s6_gold_incremental_fct' not found"],
            ["OK", None],
            ["ERROR", "Error during planning: Invalid function 'date'."],
        ],
        "probes": [
            ["ERROR", "Error during planning: table 'ice.cut.s6_gold_incremental_fct' not found"],
            ["ERROR", "Error during planning: table 'ice.cut.s6_gold_incremental_fct' not found"],
            ["ERROR", "Error during planning: table 'ice.cut.s6_gold_incremental_agg' not found"],
            ["ERROR", "Error during planning: table 'ice.cut.s6_gold_incremental_agg' not found"],
            ["META", ["ABSENT"]],
        ],
    },
    "s7-ctas-if-fresh": {
        "statements": [["OK", None]],
        "probes": [
            [
                "OK",
                [
                    ["A", "1.2500", 1, "first", 10],
                    ["A", None, None, None, 10],
                    ["B", "2.5000", 2, "keep", 20],
                ],
            ],
            [
                "OK",
                [
                    ["id", "string", False],
                    ["amount", "decimal128(10, 4)", True],
                    ["units", "int32", True],
                    ["note", "string", True],
                    ["part", "int32", False],
                ],
            ],
            ["OK", [["append"]]],
            ["DEL", []],
            [
                "META",
                [
                    ["format-version", 3],
                    [
                        "schema",
                        [
                            ["id", "string", True],
                            ["ingestion_timestamp", "timestamp", True],
                            ["amount", "decimal(10,4)", False],
                            ["units", "int", False],
                            ["note", "string", False],
                            ["part", "int", True],
                        ],
                    ],
                    [
                        "write-properties",
                        [
                            ["write.delete.mode", "merge-on-read"],
                            ["write.merge.mode", "merge-on-read"],
                            ["write.target-file-size-bytes", "268435456"],
                            ["write.update.mode", "merge-on-read"],
                        ],
                    ],
                    ["next-row-id", 3],
                ],
            ],
        ],
    },
    "s7-merge-idempotent": {
        "statements": [["OK", None], ["OK", None], ["OK", None]],
        "probes": [
            ["OK", [["A", "0.0000", 0, "unknown", 10], ["B", "2.5000", 2, "keep", 20]]],
            [
                "OK",
                [
                    ["id", "string", False],
                    ["amount", "decimal128(10, 4)", False],
                    ["units", "int32", False],
                    ["note", "string", False],
                    ["part", "int32", False],
                ],
            ],
            ["OK", [["append"], ["overwrite"], ["overwrite"]]],
            ["DEL", [[1, "PUFFIN"]]],
            [
                "META",
                [
                    ["format-version", 3],
                    [
                        "schema",
                        [
                            ["id", "string", True],
                            ["ingestion_timestamp", "timestamp", True],
                            ["amount", "decimal(10,4)", True],
                            ["units", "int", True],
                            ["note", "string", True],
                            ["part", "int", True],
                        ],
                    ],
                    [
                        "write-properties",
                        [
                            ["write.delete.mode", "merge-on-read"],
                            ["write.merge.mode", "merge-on-read"],
                            ["write.target-file-size-bytes", "268435456"],
                            ["write.update.mode", "merge-on-read"],
                        ],
                    ],
                    ["next-row-id", 6],
                ],
            ],
            [
                "IDEM",
                [
                    ["OK", [["A", "0.0000", 0, "unknown", 10], ["B", "2.5000", 2, "keep", 20]]],
                    ["OK", [["A", "0.0000", 0, "unknown", 10], ["B", "2.5000", 2, "keep", 20]]],
                    True,
                ],
            ],
        ],
    },
    "s7-overwrite-partitions": {
        "statements": [["OK", None], ["OK", None], ["OK", None]],
        "probes": [
            ["OK", [["B", "y", 20], ["C", "z", 10]]],
            ["OK", [["id", "string", True], ["note", "string", True], ["part", "int32", True]]],
            ["OK", [["append"], ["overwrite"]]],
            [
                "META",
                [
                    ["format-version", 3],
                    [
                        "schema",
                        [
                            ["id", "string", False],
                            ["note", "string", False],
                            ["part", "int", False],
                        ],
                    ],
                    [
                        "write-properties",
                        [
                            ["write.delete.mode", "merge-on-read"],
                            ["write.merge.mode", "merge-on-read"],
                            ["write.target-file-size-bytes", "268435456"],
                            ["write.update.mode", "merge-on-read"],
                        ],
                    ],
                    ["next-row-id", 4],
                ],
            ],
        ],
    },
}
