"""List a repark.toml-declared source and show ping() refusing its incomplete specification.

pins: cfg-2/C-013 · pins: c-2/C-099
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from repark.errors import IllegalArgumentException
from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.source",
    "SparkSession.sources",
]


def main() -> None:
    """Declare one source in a repark.toml and exercise the listing and the handle."""
    with tempfile.TemporaryDirectory() as directory:
        config_path = Path(directory) / "repark.toml"
        config_path.write_text(
            "[default.database.postgres.company_db]\n"
            'host = "db.example.com"\n'
            'password = "s3cr3t"\n',
            encoding="utf-8",
        )
        repark = ReparkSession.builder.configFile(str(config_path)).getOrCreate()
        try:
            rows = repark.sources()
            if [row.name for row in rows] != ["company_db"]:
                raise SystemExit(f"sources() {rows!r} unexpected")
            if rows[0].properties["password"] != "***":
                raise SystemExit("source password must stay masked")
            handle = repark.source("company_db")
            if handle.key_path != "default.database.postgres.company_db":
                raise SystemExit(f"key_path {handle.key_path!r} unexpected")
            try:
                handle.ping()
            except IllegalArgumentException as error:
                text = str(error)
                if handle.key_path not in text or "`user` is required" not in text:
                    raise SystemExit(f"ping refusal {error!r} unexpected") from error
                if "s3cr3t" in text:
                    raise SystemExit("ping refusal must not carry the password") from error
            else:
                raise SystemExit("ping() must refuse an incomplete specification")
        finally:
            repark.stop()


if __name__ == "__main__":
    main()
