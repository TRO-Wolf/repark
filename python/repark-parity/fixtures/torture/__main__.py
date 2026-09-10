"""The `python -m repark_parity.torture` entry point: generate one family's files."""

from __future__ import annotations

import argparse
from pathlib import Path

from repark_parity.torture import FAMILIES
from repark_parity.torture.nested import NESTED_FAMILY
from repark_parity.torture.tiers import DEFAULT_SEED


def build_parser() -> argparse.ArgumentParser:
    """Build the generate CLI parser."""
    parser = argparse.ArgumentParser(
        prog="repark_parity.torture",
        description="Generate a torture family's Parquet and CSV files.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    generate = subparsers.add_parser("generate", help="write one family's Parquet and CSV files")
    generate.add_argument("family", choices=sorted(FAMILIES))
    generate.add_argument("--rows", type=int, required=True)
    generate.add_argument("--seed", type=int, default=DEFAULT_SEED)
    generate.add_argument("--out", type=Path, required=True)
    generate.add_argument("--depth", type=int, default=None)
    generate.add_argument("--width", type=int, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Parse argv, generate the requested family, and print what was written."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if (args.depth is not None or args.width is not None) and args.family != "nested":
        parser.error("--depth/--width belong to the nested family")
    if args.family == "nested":
        output = NESTED_FAMILY.generate(
            rows=args.rows,
            seed=args.seed,
            out=args.out,
            depth=args.depth,
            width=args.width,
        )
    else:
        output = FAMILIES[args.family].generate(rows=args.rows, seed=args.seed, out=args.out)
    print(f"{args.family}: {output.rows} rows -> {output.parquet_path} {output.csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
