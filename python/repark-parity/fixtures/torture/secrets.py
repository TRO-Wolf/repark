"""The secrets torture family: credential-shaped column names carrying fake plaintext."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq

from repark_parity.torture.family import (
    CSV_NAME,
    PARQUET_NAME,
    FamilyOutput,
    refuse_bad_rows,
    refuse_bad_seed,
    refuse_repository_output,
)

FAKE_PREFIX = "repark-fake-"
FORBIDDEN_VALUE_PREFIXES = ("AKIA", "ASIA", "ghp_", "gho_", "sk-", "xoxb-")
NULLABLE_SECRET_COLUMN = "session_token"
NULL_EVERY = 7

FLAGGED_COLUMN_NAMES = (
    "password",
    "api_token",
    "session_token",
    "client_secret",
    "aws_secret_key",
    "private_key",
    "access_key_id",
    "service_access_key",
    "connection_string",
    "credential_id",
    "user_info",
    "bearer",
    "api_key",
)
ORDINARY_COLUMN_NAMES = ("id", "name", "note", "bucket_key")
COLUMN_NAMES = (
    "id",
    "password",
    "name",
    "api_token",
    "note",
    "client_secret",
    "aws_secret_key",
    "private_key",
    "access_key_id",
    "service_access_key",
    "connection_string",
    "credential_id",
    "user_info",
    "bearer",
    "api_key",
    "session_token",
    "bucket_key",
)

DECLARED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        *(pa.field(name, pa.string()) for name in COLUMN_NAMES if name != "id"),
    ]
)


def _flagged_value(column: str, index: int, seed: int) -> str:
    """One obviously-fake credential value; seed moves every byte."""
    value = f"{FAKE_PREFIX}{column}-{(index * 37 + seed) % 1_000_000:06d}"
    if value.startswith(FORBIDDEN_VALUE_PREFIXES):
        raise ValueError(f"secret value must not look real: {value}")
    return value


def _resolved_columns(rows: int, seed: int) -> dict[str, pa.Array]:
    """Build every column once; the CSV text renders the same values as the Parquet leg."""
    columns: dict[str, list[object]] = {name: [] for name in COLUMN_NAMES}
    for index in range(rows):
        columns["id"].append(index)
        columns["name"].append(f"user-{(index + seed) % 10_000:05d}")
        columns["note"].append("" if index % 5 == 0 else f"note-{(index + seed) % 997}")
        for column in FLAGGED_COLUMN_NAMES:
            if column == NULLABLE_SECRET_COLUMN and index % NULL_EVERY == 0:
                columns[column].append(None)
            else:
                columns[column].append(_flagged_value(column, index, seed))
        columns["bucket_key"].append(f"warehouse/table/part-{(index + seed) % 100_000:05d}.parquet")
    return {
        name: pa.array(values, type=DECLARED_SCHEMA.field(name).type)
        for name, values in columns.items()
    }


def _csv_text(columns: dict[str, pa.Array]) -> str:
    """Render the secrets CSV from the resolved values, one plain line per row."""
    values = {name: columns[name].to_pylist() for name in COLUMN_NAMES}
    lines = [",".join(COLUMN_NAMES)]
    for index in range(len(values["id"])):
        lines.append(
            ",".join(
                "" if values[name][index] is None else str(values[name][index])
                for name in COLUMN_NAMES
            )
        )
    return "\n".join(lines) + "\n"


class SecretsFamily:
    """The secrets family implementor: credential-named columns beside ordinary ones."""

    name = "secrets"

    def generate(self, rows: int, seed: int, out: Path) -> FamilyOutput:
        """Write the secrets Parquet and CSV under out."""
        refuse_bad_rows(rows)
        refuse_bad_seed(seed)
        refuse_repository_output(out)
        out.mkdir(parents=True, exist_ok=True)
        columns = _resolved_columns(rows, seed)
        (out / CSV_NAME).write_text(_csv_text(columns), encoding="utf-8")
        table = pa.table(columns, schema=DECLARED_SCHEMA)
        pq.write_table(table, out / PARQUET_NAME)
        return FamilyOutput(rows=rows, parquet_path=out / PARQUET_NAME, csv_path=out / CSV_NAME)

    def expected_read_schema(self, fmt: Literal["parquet", "csv"]) -> pa.Schema | None:
        """Return the declared expectation; both formats carry the same schema."""
        if fmt == "parquet" or fmt == "csv":
            return DECLARED_SCHEMA
        return None


SECRETS_FAMILY = SecretsFamily()
