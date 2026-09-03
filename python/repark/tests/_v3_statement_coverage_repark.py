"""V3-COV measured matrix — the repark half, recorded 2026-09-03.

pins: v3-cov-statement-coverage/C-003
"""

from __future__ import annotations

from typing import Any

REPARK: dict[str, Any] = {
    "create-v3-flat": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", []],
            [
                "META",
                [
                    ["format-version", 3],
                    ["schema", [["id", "int", False], ["name", "string", False]]],
                    ["partition-fields", []],
                ],
            ],
        ],
    },
    "create-v3-partitioned": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", []],
            [
                "META",
                [
                    ["format-version", 3],
                    [
                        "schema",
                        [["id", "int", False], ["name", "string", False], ["part", "int", False]],
                    ],
                    ["partition-fields", [["part", "identity"]]],
                ],
            ],
        ],
    },
    "create-v3-bucket-transform": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", []],
            [
                "META",
                [
                    ["format-version", 3],
                    [
                        "schema",
                        [["id", "int", False], ["name", "string", False], ["part", "int", False]],
                    ],
                    ["partition-fields", [["id_bucket", "bucket[4]"]]],
                ],
            ],
        ],
    },
    "create-v3-write-order": {
        "statements": [
            [
                "ERROR",
                'SQL error: ParserError("Expected: end of statement, found: USING at Line: 1,',
            ]
        ],
        "probes": [
            ["ERROR", "Error during planning: table 'ice.cov.create_v3_write_order' not found"]
        ],
    },
    "create-v3-properties": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", []],
            [
                "META",
                [
                    ["format-version", 3],
                    ["schema", [["id", "int", False], ["name", "string", False]]],
                    ["partition-fields", []],
                    [
                        "write-properties",
                        [
                            ["write.delete.mode", "merge-on-read"],
                            ["write.merge.mode", "merge-on-read"],
                            ["write.update.mode", "merge-on-read"],
                        ],
                    ],
                ],
            ],
        ],
    },
    "ctas-v3": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"]]],
            [
                "META",
                [
                    ["format-version", 3],
                    ["schema", [["id", "long", True], ["name", "string", True]]],
                    ["partition-fields", []],
                ],
            ],
        ],
    },
    "insert-into": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [2, "b"], [3, "c"], [4, "d"], [5, "e"]]],
            ["OK", [[1, 0, 1], [2, 1, 1], [3, 2, 1], [4, 3, 1], [5, 4, 2]]],
        ],
    },
    "insert-into-select": {
        "statements": [["OK", None]],
        "probes": [
            [
                "OK",
                [
                    [1, "a"],
                    [11, "a"],
                    [12, "b"],
                    [13, "c"],
                    [14, "d"],
                    [2, "b"],
                    [3, "c"],
                    [4, "d"],
                ],
            ],
            [
                "OK",
                [
                    [1, 0, 1],
                    [11, 4, 2],
                    [12, 5, 2],
                    [13, 6, 2],
                    [14, 7, 2],
                    [2, 1, 1],
                    [3, 2, 1],
                    [4, 3, 1],
                ],
            ],
        ],
    },
    "insert-overwrite-table": {
        "statements": [["OK", None]],
        "probes": [["OK", [[9, "z"]]], ["OK", [[9, 4, 2]]]],
    },
    "insert-overwrite-partition-static-values": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[3, "c", 20], [4, "d", 20], [7, "g", 10]]],
            ["OK", [[3, 2, 1], [4, 3, 1], [7, 4, 2]]],
        ],
    },
    "insert-overwrite-partition-static-select": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a", 10], [3, "c", 20], [4, "d", 20]]],
            ["OK", [[1, 4, 2], [3, 2, 1], [4, 3, 1]]],
        ],
    },
    "insert-overwrite-partition-dynamic": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[3, "c", 20], [4, "d", 20], [7, "g", 10]]],
            ["OK", [[3, 2, 1], [4, 3, 1], [7, 4, 2]]],
        ],
    },
    "delete-where-mor": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [3, 2, 1], [4, 3, 1]]],
            ["OK", [[1, "PUFFIN", 1]]],
        ],
    },
    "delete-where-cow": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [3, 2, 1], [4, 3, 1]]],
        ],
    },
    "delete-where-partitioned-mor": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a", 10], [3, "c", 20], [4, "d", 20]]],
            ["OK", [[1, 0, 1], [3, 2, 1], [4, 3, 1]]],
            ["OK", [[1, "PUFFIN", 1]]],
        ],
    },
    "delete-where-partitioned-cow": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a", 10], [3, "c", 20], [4, "d", 20]]],
            ["OK", [[1, 0, 1], [3, 2, 1], [4, 3, 1]]],
        ],
    },
    "delete-in-subquery-mor": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [3, 2, 1], [4, 3, 1]]],
            ["OK", [[1, "PUFFIN", 1]]],
        ],
    },
    "delete-not-in-subquery-mor": {
        "statements": [["OK", None]],
        "probes": [["OK", [[2, "b"]]], ["OK", [[2, 1, 1]]], ["OK", [[1, "PUFFIN", 3]]]],
    },
    "delete-exists-subquery-mor": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [3, 2, 1], [4, 3, 1]]],
            ["OK", [[1, "PUFFIN", 1]]],
        ],
    },
    "delete-not-exists-subquery-mor": {
        "statements": [["OK", None]],
        "probes": [["OK", [[2, "b"]]], ["OK", [[2, 1, 1]]], ["OK", [[1, "PUFFIN", 3]]]],
    },
    "delete-in-subquery-cow": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [3, 2, 1], [4, 3, 1]]],
        ],
    },
    "delete-all-rows-mor": {
        "statements": [["OK", None]],
        "probes": [["OK", []], ["OK", []], ["OK", [[1, "PUFFIN", 4]]], ["OK", [[0, 4], [1, 4]]]],
    },
    "update-where-mor": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [2, "z"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [2, 1, 2], [3, 2, 1], [4, 3, 1]]],
            ["OK", [[1, "PUFFIN", 1]]],
        ],
    },
    "update-where-cow": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [2, "z"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [2, 1, 2], [3, 2, 1], [4, 3, 1]]],
        ],
    },
    "update-where-partitioned-mor": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a", 10], [2, "z", 10], [3, "c", 20], [4, "d", 20]]],
            ["OK", [[1, 0, 1], [2, 1, 2], [3, 2, 1], [4, 3, 1]]],
            ["OK", [[1, "PUFFIN", 1]]],
        ],
    },
    "update-in-subquery-mor": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [2, "z"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [2, 1, 2], [3, 2, 1], [4, 3, 1]]],
            ["OK", [[1, "PUFFIN", 1]]],
        ],
    },
    "update-not-in-subquery-mor": {
        "statements": [
            [
                "ERROR",
                "Error during planning: UPDATE with a subquery in its WHERE clause is refused",
            ]
        ],
        "probes": [
            ["OK", [[1, "a"], [2, "b"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [2, 1, 1], [3, 2, 1], [4, 3, 1]]],
            ["OK", []],
        ],
    },
    "update-exists-subquery-mor": {
        "statements": [
            [
                "ERROR",
                "Error during planning: UPDATE with a subquery in its WHERE clause is refused",
            ]
        ],
        "probes": [
            ["OK", [[1, "a"], [2, "b"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [2, 1, 1], [3, 2, 1], [4, 3, 1]]],
            ["OK", []],
        ],
    },
    "update-partition-key-cow": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a", 10], [2, "b", 30], [3, "c", 20], [4, "d", 20]]],
            ["OK", [[1, 0, 1], [2, 1, 2], [3, 2, 1], [4, 3, 1]]],
        ],
    },
    "merge-matched-update-mor": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [2, "z"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [2, 1, 2], [3, 2, 1], [4, 3, 1]]],
            ["OK", [[1, "PUFFIN", 1]]],
        ],
    },
    "merge-matched-update-cow": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [2, "z"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [2, 1, 2], [3, 2, 1], [4, 3, 1]]],
        ],
    },
    "merge-matched-delete-mor": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [3, 2, 1], [4, 3, 1]]],
            ["OK", [[1, "PUFFIN", 1]]],
        ],
    },
    "merge-matched-delete-cow": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [3, 2, 1], [4, 3, 1]]],
        ],
    },
    "merge-not-matched-insert": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [2, "b"], [3, "c"], [4, "d"], [9, "n"]]],
            ["OK", [[1, 0, 1], [2, 1, 1], [3, 2, 1], [4, 3, 1], [9, 4, 2]]],
            ["OK", []],
        ],
    },
    "merge-not-matched-by-source-delete": {
        "statements": [["OK", None]],
        "probes": [["OK", [[2, "b"]]], ["OK", [[2, 1, 1]]], ["OK", [[1, "PUFFIN", 3]]]],
    },
    "merge-not-matched-by-source-update": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "z"], [2, "b"], [3, "z"], [4, "z"]]],
            ["OK", [[1, 0, 2], [2, 1, 1], [3, 2, 2], [4, 3, 2]]],
            ["OK", [[1, "PUFFIN", 3]]],
        ],
    },
    "merge-mixed-arms": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[2, "z"], [9, "n"]]],
            ["OK", [[2, 1, 2], [9, 5, 2]]],
            ["OK", [[1, "PUFFIN", 4]]],
        ],
    },
    "merge-matched-conditional": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [2, "z"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [2, 1, 2], [3, 2, 1], [4, 3, 1]]],
            ["OK", [[1, "PUFFIN", 1]]],
        ],
    },
    "merge-partitioned-mor": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a", 10], [3, "c", 20], [4, "d", 20]]],
            ["OK", [[1, 0, 1], [3, 2, 1], [4, 3, 1]]],
            ["OK", [[1, "PUFFIN", 1]]],
        ],
    },
    "merge-source-table-mor": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [2, "z"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [2, 1, 2], [3, 2, 1], [4, 3, 1]]],
            ["OK", [[1, "PUFFIN", 1]]],
        ],
    },
    "alter-add-column": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a", None], [2, "b", None], [3, "c", None], [4, "d", None]]],
            ["OK", [[1, 0, 1], [2, 1, 1], [3, 2, 1], [4, 3, 1]]],
        ],
    },
    "alter-drop-column": {
        "statements": [["OK", None]],
        "probes": [["OK", [[1, 10], [2, 10], [3, 20], [4, 20]]]],
    },
    "alter-rename-column": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [2, "b"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [2, 1, 1], [3, 2, 1], [4, 3, 1]]],
        ],
    },
    "alter-alter-column-type": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [2, "b"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [2, 1, 1], [3, 2, 1], [4, 3, 1]]],
        ],
    },
    "alter-add-partition-field": {
        "statements": [["OK", None], ["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [2, "b"], [3, "c"], [4, "d"], [5, "e"]]],
            ["OK", [[1, 0, 1], [2, 1, 1], [3, 2, 1], [4, 3, 1], [5, 4, 2]]],
            ["OK", [[0, 4], [1, 1]]],
        ],
    },
    "alter-drop-partition-field": {
        "statements": [["OK", None], ["OK", None]],
        "probes": [
            ["OK", [[1, "a", 10], [2, "b", 10], [3, "c", 20], [4, "d", 20], [5, "e", 30]]],
            ["OK", [[0, 2], [0, 2], [1, 1]]],
        ],
    },
    "alter-replace-partition-field": {
        "statements": [["OK", None], ["OK", None]],
        "probes": [
            ["OK", [[1, "a", 10], [2, "b", 10], [3, "c", 20], [4, "d", 20], [5, "e", 30]]],
            ["OK", [[0, 2], [0, 2], [1, 1]]],
            ["META", [["partition-fields", [["id_bucket_2", "bucket[2]"]]]]],
        ],
    },
    "alter-add-column-partitioned": {
        "statements": [["OK", None]],
        "probes": [
            [
                "OK",
                [[1, "a", 10, None], [2, "b", 10, None], [3, "c", 20, None], [4, "d", 20, None]],
            ],
            ["OK", [[1, 0, 1], [2, 1, 1], [3, 2, 1], [4, 3, 1]]],
        ],
    },
    "alter-set-tblproperties": {
        "statements": [["OK", None], ["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [3, 2, 1], [4, 3, 1]]],
            ["OK", [[1, "PUFFIN", 1]]],
        ],
    },
    "alter-unset-tblproperties": {
        "statements": [["OK", None], ["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [3, 2, 1], [4, 3, 1]]],
            ["OK", []],
        ],
    },
    "alter-write-ordered-by": {
        "statements": [
            [
                "ERROR",
                "This feature is not implemented: ALTER TABLE WRITE ORDERED BY / WRITE DISTRI",
            ]
        ],
        "probes": [
            ["OK", [[1, "a"], [2, "b"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [2, 1, 1], [3, 2, 1], [4, 3, 1]]],
        ],
    },
    "alter-set-format-version-3": {
        "statements": [["OK", None], ["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 2], [3, 1, 2], [4, 2, 2]]],
            ["OK", []],
        ],
    },
    "alter-set-format-version-3-mor": {
        "statements": [["OK", None], ["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [3, 2, 1], [4, 3, 1]]],
            ["OK", [[1, "PUFFIN", 1]]],
        ],
    },
    "truncate-table": {
        "statements": [["OK", None]],
        "probes": [["OK", []], ["OK", [["append"], ["delete"]]]],
    },
    "drop-table": {
        "statements": [["OK", None]],
        "probes": [["ERROR", "Error during planning: table 'ice.cov.drop_table' not found"]],
    },
    "meta-snapshots": {"statements": [["OK", None]], "probes": [["OK", [["append"], ["delete"]]]]},
    "meta-files": {"statements": [["OK", None]], "probes": [["OK", [[0, 4], [1, 1]]]]},
    "meta-delete-files": {"statements": [["OK", None]], "probes": [["OK", [[1, "PUFFIN", 1]]]]},
    "meta-manifests": {
        "statements": [["OK", None]],
        "probes": [["OK", [[0, 1, 0, 0], [1, 0, 0, 0]]]],
    },
    "meta-history": {"statements": [["OK", None]], "probes": [["OK", [[True], [True]]]]},
    "meta-refs": {
        "statements": [["OK", None]],
        "probes": [["OK", [["main", "BRANCH", None], ["t1", "TAG", None]]]],
    },
    "meta-partitions": {"statements": [["OK", None]], "probes": [["OK", [[2, 1, 0], [2, 1, 1]]]]},
    "meta-entries": {"statements": [["OK", None]], "probes": [["OK", [[1], [1]]]]},
    "meta-all-data-files": {"statements": [["OK", None]], "probes": [["OK", [[0, 4]]]]},
    "meta-position-deletes": {
        "statements": [["OK", None]],
        "probes": [
            [
                "ERROR",
                "External error: FeatureUnsupported => position_deletes metadata table scan i",
            ]
        ],
    },
    "lineage-projection": {
        "statements": [["OK", None]],
        "probes": [
            ["OK", [[1, 0, 1], [2, 1, 2], [3, 2, 1], [4, 3, 1]]],
            ["OK", [[0], [1], [2], [3]]],
        ],
    },
    "time-travel-version-as-of": {
        "statements": [["OK", None]],
        "probes": [["OK", [[1, "a"], [2, "b"], [3, "c"], [4, "d"]]]],
    },
    "time-travel-timestamp-as-of": {
        "statements": [["OK", None]],
        "probes": [["OK", [[1, "a"], [2, "b"], [3, "c"], [4, "d"]]]],
    },
    "branch-create-and-read": {
        "statements": [["OK", None], ["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [2, "b"], [3, "c"], [4, "d"]]],
            ["OK", [[1, "a"], [3, "c"], [4, "d"]]],
        ],
    },
    "branch-write": {
        "statements": [["OK", None], ["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [2, "b"], [3, "c"], [4, "d"], [5, "e"]]],
            ["OK", [[1, "a"], [2, "b"], [3, "c"], [4, "d"]]],
        ],
    },
    "branch-create-replace-and-drop": {
        "statements": [["OK", None], ["OK", None], ["OK", None]],
        "probes": [["OK", [["main", "BRANCH"]]]],
    },
    "tag-create-and-read": {
        "statements": [["OK", None], ["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [2, "b"], [3, "c"], [4, "d"]]],
            ["OK", [[1, "a"], [3, "c"], [4, "d"]]],
        ],
    },
    "tag-retention": {
        "statements": [["OK", None]],
        "probes": [["OK", [["main", "BRANCH", None], ["t1", "TAG", 864000000]]]],
    },
    "branch-retention": {
        "statements": [["OK", None]],
        "probes": [["OK", [["b1", "BRANCH", 2], ["main", "BRANCH", None]]]],
    },
    "call-expire-snapshots": {
        "statements": [["OK", None], ["OK", [[0, 0, 0, 0, 1]]]],
        "probes": [["OK", [[1, "a"], [3, "c"], [4, "d"]]], ["OK", [["delete"]]]],
    },
    "call-remove-orphan-files": {
        "statements": [["OK", []]],
        "probes": [["OK", [[1, "a"], [2, "b"], [3, "c"], [4, "d"]]]],
    },
    "call-rewrite-data-files": {
        "statements": [["OK", None], ["OK", [[0, 0, 0]]]],
        "probes": [
            ["OK", [[1, "a"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [3, 2, 1], [4, 3, 1]]],
            ["OK", [[1, "PUFFIN", 1]]],
        ],
    },
    "call-rewrite-manifests": {
        "statements": [["OK", [[0, 0]]]],
        "probes": [["OK", [[1, "a"], [2, "b"], [3, "c"], [4, "d"]]]],
    },
    "call-rewrite-position-delete-files": {
        "statements": [
            ["OK", None],
            [
                "ERROR",
                "This feature is not implemented: CALL rewrite_position_delete_files found 1 ",
            ],
        ],
        "probes": [["OK", [[1, "a"], [3, "c"], [4, "d"]]], ["OK", [[1, "PUFFIN", 1]]]],
    },
    "call-rollback-to-snapshot": {
        "statements": [["OK", None], ["OK", None]],
        "probes": [
            ["OK", [[1, "a"], [2, "b"], [3, "c"], [4, "d"]]],
            ["OK", [[1, 0, 1], [2, 1, 1], [3, 2, 1], [4, 3, 1]]],
        ],
    },
    "call-register-table": {
        "statements": [["OK", None]],
        "probes": [["OK", [[1, "a"], [2, "b"], [3, "c"], [4, "d"]]]],
    },
}
