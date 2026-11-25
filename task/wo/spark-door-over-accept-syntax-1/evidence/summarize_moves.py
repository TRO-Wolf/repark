from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

from compare_over_accept import build_rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=Path, required=True)
    parser.add_argument("--head", required=True)
    arguments = parser.parse_args()
    moved: dict[str, list[str]] = defaultdict(list)
    unlike: dict[str, list[str]] = defaultdict(list)
    for mode in ("true", "false"):
        spark = json.loads((arguments.dir / f"spark-{mode}.json").read_text())
        main_report = json.loads((arguments.dir / f"main-{mode}.json").read_text())
        head = json.loads((arguments.dir / f"{arguments.head}-{mode}.json").read_text())
        for row in build_rows(spark, main_report, head):
            key = f"{row['id']} {row['door']} ansi={mode}"
            if row["main_verdict"] != row["head_verdict"]:
                moved[f"{row['role']} {row['main_verdict']} -> {row['head_verdict']}"].append(key)
            if row["head_verdict"] != "EQ":
                text = f"`{row['text']}`: Spark {row['spark']}; head {row['head']}"
                unlike[f"{row['id']} {row['head_verdict']}"].append(f"{row['door']}/{mode} {text}")
    print("## Moved (main -> head)")
    for key, cells in sorted(moved.items()):
        print(f"- **{key}** ({len(cells)}): {', '.join(cells)}")
    print("\n## Still unlike Spark at head")
    for key, cells in sorted(unlike.items()):
        doors = sorted({cell.split(" ")[0] for cell in cells})
        print(f"- **{key}** [{', '.join(doors)}]: {cells[0][len(cells[0].split(' ')[0]) + 1 :]}")


if __name__ == "__main__":
    main()
