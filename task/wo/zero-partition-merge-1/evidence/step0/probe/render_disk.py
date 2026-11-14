from __future__ import annotations

import json
import sys
from pathlib import Path

NEG = ("8000000000000000", "80000000")
POS = ("0000000000000000", "00000000")
SIGNS = {1: NEG, 2: POS, 5: POS, 6: NEG, 7: NEG, 8: POS}
CROSS = {10: NEG, 1: POS}


def want(table: str, ident: int) -> tuple[str, str] | None:
    if "_cross_" in table:
        return CROSS.get(ident)
    return SIGNS.get(ident)


def summarise(path: Path) -> dict[str, int]:
    tally = {
        "files_read": 0,
        "files_not_read_avro": 0,
        "read_errors": 0,
        "rows_with_changed_stored_bits": 0,
        "files_holding_both_zeros": 0,
        "files_where_partition_value_differs_from_a_stored_zero": 0,
    }
    for line in path.read_text().splitlines():
        record = json.loads(line)
        table = record["table"]
        col = 0 if table.endswith("_d") else 1
        for partition, _count, rows in record["disk"]:
            if isinstance(rows, str):
                key = "files_not_read_avro" if rows == "not-read" else "read_errors"
                tally[key] += 1
                continue
            tally["files_read"] += 1
            signs = set()
            stored_zero_bits = set()
            for ident, stored in rows.items():
                expected = want(table, int(ident))
                if expected is None:
                    continue
                if tuple(stored) != expected:
                    tally["rows_with_changed_stored_bits"] += 1
                signs.add(expected)
                stored_zero_bits.add(stored[col])
            if len(signs) > 1:
                tally["files_holding_both_zeros"] += 1
            if stored_zero_bits and any(bits != partition for bits in stored_zero_bits):
                tally["files_where_partition_value_differs_from_a_stored_zero"] += 1
    return tally


def main() -> int:
    print("| run | " + " | ".join(summarise(Path(sys.argv[1])).keys()) + " |")
    print("|---" * 7 + "|")
    for arg in sys.argv[1:]:
        tally = summarise(Path(arg))
        print(f"| {Path(arg).stem} | " + " | ".join(str(v) for v in tally.values()) + " |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
