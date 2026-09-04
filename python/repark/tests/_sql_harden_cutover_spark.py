"""SQL-HARDEN-1 measured matrix - the spark half, recorded 2026-09-04.

pins: sql-harden-1-cutover-shapes/C-001
"""

from __future__ import annotations

from typing import Any

SPARK: dict[str, Any] = {
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
                    ["id", "string", True],
                    ["amount", "decimal128(10, 4)", True],
                    ["units", "int32", True],
                    ["note", "string", True],
                    ["part", "int32", True],
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
                            ["id", "string", False],
                            ["ingestion_timestamp", "timestamp", False],
                            ["amount", "decimal(10,4)", False],
                            ["units", "int", False],
                            ["note", "string", False],
                            ["part", "int", False],
                        ],
                    ],
                    [
                        "write-properties",
                        [
                            ["write.delete.mode", "merge-on-read"],
                            ["write.merge.mode", "merge-on-read"],
                            ["write.parquet.compression-codec", "zstd"],
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
                    ["id", "string", True],
                    ["amount", "decimal128(10, 4)", True],
                    ["units", "int32", True],
                    ["note", "string", True],
                    ["part", "int32", True],
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
                            ["id", "string", False],
                            ["ingestion_timestamp", "timestamp", False],
                            ["amount", "decimal(10,4)", False],
                            ["units", "int", False],
                            ["note", "string", False],
                            ["part", "int", False],
                        ],
                    ],
                    [
                        "write-properties",
                        [
                            ["write.delete.mode", "merge-on-read"],
                            ["write.merge.mode", "merge-on-read"],
                            ["write.parquet.compression-codec", "zstd"],
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
                    ["id", "string", True],
                    ["amount", "decimal128(10, 4)", True],
                    ["units", "int32", False],
                    ["note", "string", False],
                    ["part", "int32", True],
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
                            ["write.parquet.compression-codec", "zstd"],
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
                    ["id", "string", True],
                    ["amount", "decimal128(10, 4)", True],
                    ["units", "int32", True],
                    ["note", "string", True],
                    ["part", "int32", True],
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
                            ["id", "string", False],
                            ["ingestion_timestamp", "timestamp", False],
                            ["amount", "decimal(10,4)", False],
                            ["units", "int", False],
                            ["note", "string", False],
                            ["part", "int", False],
                        ],
                    ],
                    [
                        "write-properties",
                        [
                            ["write.delete.mode", "merge-on-read"],
                            ["write.merge.mode", "merge-on-read"],
                            ["write.parquet.compression-codec", "zstd"],
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
            ["OK", None],
            ["OK", None],
            ["OK", None],
            ["OK", None],
        ],
        "probes": [
            ["OK", [["s1", 10, 15], ["s2", 20, 40], ["s3", 10, 15]]],
            [
                "OK",
                [
                    ["survey_id", "string", True],
                    ["clinic_id", "int32", True],
                    ["wait_time_minutes", "int32", True],
                    ["provider_name", "string", True],
                ],
            ],
            ["OK", [[10, "2026-01-01", "Thursday", 1, 1], [20, "2026-01-02", "Friday", 1, 1]]],
            [
                "OK",
                [
                    ["clinic_id", "int32", True],
                    ["calendar_date", "date32[day]", True],
                    ["day_of_week", "string", True],
                    ["num_surveys", "int64", True],
                ],
            ],
            [
                "META",
                [
                    ["format-version", 2],
                    [
                        "schema",
                        [
                            ["calendar_date", "date", False],
                            ["year_month", "int", False],
                            ["year", "int", False],
                            ["year_quarter", "string", False],
                            ["survey_id", "string", False],
                            ["patient_visit_id", "string", False],
                            ["gene_prissy_score", "int", False],
                            ["experience_score", "int", False],
                            ["provider_name", "string", False],
                            ["appointment_datetime", "timestamptz", False],
                            ["clinic_id", "int", False],
                            ["provider_seen_time", "timestamptz", False],
                            ["checkin_time", "timestamptz", False],
                            ["wait_time_minutes", "int", False],
                        ],
                    ],
                    [
                        "write-properties",
                        [
                            ["write.delete.mode", "merge-on-read"],
                            ["write.merge.mode", "merge-on-read"],
                            ["write.parquet.compression-codec", "zstd"],
                            ["write.target-file-size-bytes", "268435456"],
                            ["write.update.mode", "merge-on-read"],
                        ],
                    ],
                    ["next-row-id", None],
                ],
            ],
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
                    ["id", "string", True],
                    ["amount", "decimal128(10, 4)", True],
                    ["units", "int32", True],
                    ["note", "string", True],
                    ["part", "int32", True],
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
                            ["id", "string", False],
                            ["ingestion_timestamp", "timestamp", False],
                            ["amount", "decimal(10,4)", False],
                            ["units", "int", False],
                            ["note", "string", False],
                            ["part", "int", False],
                        ],
                    ],
                    [
                        "write-properties",
                        [
                            ["write.delete.mode", "merge-on-read"],
                            ["write.merge.mode", "merge-on-read"],
                            ["write.parquet.compression-codec", "zstd"],
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
                    ["id", "string", True],
                    ["amount", "decimal128(10, 4)", True],
                    ["units", "int32", True],
                    ["note", "string", True],
                    ["part", "int32", True],
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
                            ["id", "string", False],
                            ["ingestion_timestamp", "timestamp", False],
                            ["amount", "decimal(10,4)", False],
                            ["units", "int", False],
                            ["note", "string", False],
                            ["part", "int", False],
                        ],
                    ],
                    [
                        "write-properties",
                        [
                            ["write.delete.mode", "merge-on-read"],
                            ["write.merge.mode", "merge-on-read"],
                            ["write.parquet.compression-codec", "zstd"],
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
                            ["write.parquet.compression-codec", "zstd"],
                            ["write.target-file-size-bytes", "268435456"],
                            ["write.update.mode", "merge-on-read"],
                        ],
                    ],
                    ["next-row-id", 4],
                ],
            ],
        ],
    },
    "s8-ctas-cow": {
        "statements": [
            ["OK", None],
        ],
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
                    ["id", "string", True],
                    ["amount", "decimal128(10, 4)", True],
                    ["units", "int32", True],
                    ["note", "string", True],
                    ["part", "int32", True],
                ],
            ],
            [
                "OK",
                [
                    ["append"],
                ],
            ],
            [
                "DEL",
                [],
            ],
            [
                "META",
                [
                    ["format-version", 2],
                    [
                        "schema",
                        [
                            ["id", "string", False],
                            ["ingestion_timestamp", "timestamp", False],
                            ["amount", "decimal(10,4)", False],
                            ["units", "int", False],
                            ["note", "string", False],
                            ["part", "int", False],
                        ],
                    ],
                    [
                        "write-properties",
                        [
                            ["write.delete.mode", "copy-on-write"],
                            ["write.merge.mode", "copy-on-write"],
                            ["write.parquet.compression-codec", "zstd"],
                            ["write.target-file-size-bytes", "268435456"],
                            ["write.update.mode", "copy-on-write"],
                        ],
                    ],
                    ["next-row-id", None],
                ],
            ],
        ],
    },
    "s8-merge-idempotent-cow": {
        "statements": [
            ["OK", None],
            ["OK", None],
            ["OK", None],
        ],
        "probes": [
            [
                "OK",
                [
                    ["A", "0.0000", 0, "unknown", 10],
                    ["B", "2.5000", 2, "keep", 20],
                ],
            ],
            [
                "OK",
                [
                    ["id", "string", True],
                    ["amount", "decimal128(10, 4)", True],
                    ["units", "int32", True],
                    ["note", "string", True],
                    ["part", "int32", True],
                ],
            ],
            [
                "OK",
                [
                    ["append"],
                    ["overwrite"],
                    ["overwrite"],
                ],
            ],
            [
                "DEL",
                [],
            ],
            [
                "META",
                [
                    ["format-version", 2],
                    [
                        "schema",
                        [
                            ["id", "string", False],
                            ["ingestion_timestamp", "timestamp", False],
                            ["amount", "decimal(10,4)", False],
                            ["units", "int", False],
                            ["note", "string", False],
                            ["part", "int", False],
                        ],
                    ],
                    [
                        "write-properties",
                        [
                            ["write.delete.mode", "copy-on-write"],
                            ["write.merge.mode", "copy-on-write"],
                            ["write.parquet.compression-codec", "zstd"],
                            ["write.target-file-size-bytes", "268435456"],
                            ["write.update.mode", "copy-on-write"],
                        ],
                    ],
                    ["next-row-id", None],
                ],
            ],
            [
                "IDEM",
                [
                    [
                        "OK",
                        [
                            ["A", "0.0000", 0, "unknown", 10],
                            ["B", "2.5000", 2, "keep", 20],
                        ],
                    ],
                    [
                        "OK",
                        [
                            ["A", "0.0000", 0, "unknown", 10],
                            ["B", "2.5000", 2, "keep", 20],
                        ],
                    ],
                    True,
                ],
            ],
            ["FILES", 1],
        ],
    },
    "s8-overwrite-partitions-cow": {
        "statements": [
            ["OK", None],
            ["OK", None],
            ["OK", None],
        ],
        "probes": [
            [
                "OK",
                [
                    ["B", "y", 20],
                    ["C", "z", 10],
                ],
            ],
            [
                "OK",
                [
                    ["id", "string", True],
                    ["note", "string", True],
                    ["part", "int32", True],
                ],
            ],
            [
                "OK",
                [
                    ["append"],
                    ["overwrite"],
                ],
            ],
            [
                "DEL",
                [],
            ],
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
                            ["write.delete.mode", "copy-on-write"],
                            ["write.merge.mode", "copy-on-write"],
                            ["write.parquet.compression-codec", "zstd"],
                            ["write.target-file-size-bytes", "268435456"],
                            ["write.update.mode", "copy-on-write"],
                        ],
                    ],
                    ["next-row-id", None],
                ],
            ],
        ],
    },
    "s9-ctas-cow": {
        "statements": [
            ["OK", None],
        ],
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
                    ["id", "string", True],
                    ["amount", "decimal128(10, 4)", True],
                    ["units", "int32", True],
                    ["note", "string", True],
                    ["part", "int32", True],
                ],
            ],
            [
                "OK",
                [
                    ["append"],
                ],
            ],
            [
                "DEL",
                [],
            ],
            [
                "META",
                [
                    ["format-version", 3],
                    [
                        "schema",
                        [
                            ["id", "string", False],
                            ["ingestion_timestamp", "timestamp", False],
                            ["amount", "decimal(10,4)", False],
                            ["units", "int", False],
                            ["note", "string", False],
                            ["part", "int", False],
                        ],
                    ],
                    [
                        "write-properties",
                        [
                            ["write.delete.mode", "copy-on-write"],
                            ["write.merge.mode", "copy-on-write"],
                            ["write.parquet.compression-codec", "zstd"],
                            ["write.target-file-size-bytes", "268435456"],
                            ["write.update.mode", "copy-on-write"],
                        ],
                    ],
                    ["next-row-id", 3],
                ],
            ],
        ],
    },
    "s9-merge-idempotent-cow": {
        "statements": [
            ["OK", None],
            ["OK", None],
            ["OK", None],
        ],
        "probes": [
            [
                "OK",
                [
                    ["A", "0.0000", 0, "unknown", 10],
                    ["B", "2.5000", 2, "keep", 20],
                ],
            ],
            [
                "OK",
                [
                    ["id", "string", True],
                    ["amount", "decimal128(10, 4)", True],
                    ["units", "int32", True],
                    ["note", "string", True],
                    ["part", "int32", True],
                ],
            ],
            [
                "OK",
                [
                    ["append"],
                    ["overwrite"],
                    ["overwrite"],
                ],
            ],
            [
                "DEL",
                [],
            ],
            [
                "META",
                [
                    ["format-version", 3],
                    [
                        "schema",
                        [
                            ["id", "string", False],
                            ["ingestion_timestamp", "timestamp", False],
                            ["amount", "decimal(10,4)", False],
                            ["units", "int", False],
                            ["note", "string", False],
                            ["part", "int", False],
                        ],
                    ],
                    [
                        "write-properties",
                        [
                            ["write.delete.mode", "copy-on-write"],
                            ["write.merge.mode", "copy-on-write"],
                            ["write.parquet.compression-codec", "zstd"],
                            ["write.target-file-size-bytes", "268435456"],
                            ["write.update.mode", "copy-on-write"],
                        ],
                    ],
                    ["next-row-id", 6],
                ],
            ],
            [
                "IDEM",
                [
                    [
                        "OK",
                        [
                            ["A", "0.0000", 0, "unknown", 10],
                            ["B", "2.5000", 2, "keep", 20],
                        ],
                    ],
                    [
                        "OK",
                        [
                            ["A", "0.0000", 0, "unknown", 10],
                            ["B", "2.5000", 2, "keep", 20],
                        ],
                    ],
                    True,
                ],
            ],
            ["FILES", 1],
        ],
    },
    "s9-overwrite-partitions-cow": {
        "statements": [
            ["OK", None],
            ["OK", None],
            ["OK", None],
        ],
        "probes": [
            [
                "OK",
                [
                    ["B", "y", 20],
                    ["C", "z", 10],
                ],
            ],
            [
                "OK",
                [
                    ["id", "string", True],
                    ["note", "string", True],
                    ["part", "int32", True],
                ],
            ],
            [
                "OK",
                [
                    ["append"],
                    ["overwrite"],
                ],
            ],
            [
                "DEL",
                [],
            ],
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
                            ["write.delete.mode", "copy-on-write"],
                            ["write.merge.mode", "copy-on-write"],
                            ["write.parquet.compression-codec", "zstd"],
                            ["write.target-file-size-bytes", "268435456"],
                            ["write.update.mode", "copy-on-write"],
                        ],
                    ],
                    ["next-row-id", 4],
                ],
            ],
        ],
    },
}
