from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import quote, unquote

ALGORITHM = "AWS4-HMAC-SHA256"
UNRESERVED = "-_.~"


@dataclass(frozen=True)
class Credentials:
    access_key: str
    secret_key: str
    token: str | None = None


@dataclass(frozen=True)
class Signed:
    canonical_request: str
    string_to_sign: str
    signature: str
    headers: tuple[tuple[str, str], ...]


def amz_date(moment: datetime | None = None) -> str:
    stamp = moment if moment is not None else datetime.now(UTC)
    return stamp.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def normalize_path(path: str) -> str:
    segments: list[str] = []
    for segment in path.split("/"):
        if segment in ("", "."):
            continue
        if segment == "..":
            if segments:
                segments.pop()
            continue
        segments.append(segment)
    normalized = "/" + "/".join(segments)
    if path.endswith("/") and normalized != "/":
        normalized += "/"
    return normalized


def canonical_uri(path: str, *, normalize: bool = True) -> str:
    raw = normalize_path(path) if normalize else path
    return quote(raw or "/", safe="/" + UNRESERVED, encoding="utf-8")


def canonical_query(query: str) -> str:
    pairs: list[tuple[str, str]] = []
    for part in query.split("&"):
        if not part:
            continue
        key, _, value = part.partition("=")
        pairs.append(
            (
                quote(unquote(key), safe=UNRESERVED, encoding="utf-8"),
                quote(unquote(value), safe=UNRESERVED, encoding="utf-8"),
            )
        )
    return "&".join(f"{key}={value}" for key, value in sorted(pairs))


def canonical_headers(headers: list[tuple[str, str]]) -> tuple[str, str]:
    merged: dict[str, list[str]] = {}
    for name, value in headers:
        merged.setdefault(name.strip().lower(), []).append(" ".join(value.split()))
    names = sorted(merged)
    text = "".join(f"{name}:{','.join(merged[name])}\n" for name in names)
    return text, ";".join(names)


def signing_key(secret_key: str, date: str, region: str, service: str) -> bytes:
    key = f"AWS4{secret_key}".encode()
    for part in (date, region, service, "aws4_request"):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    return key


def sign(
    *,
    method: str,
    path: str,
    query: str,
    headers: list[tuple[str, str]],
    body: bytes,
    credentials: Credentials,
    region: str,
    service: str,
    stamp: str,
    normalize: bool = True,
) -> Signed:
    signed_headers = [*headers, ("X-Amz-Date", stamp)]
    if credentials.token is not None:
        signed_headers.append(("X-Amz-Security-Token", credentials.token))
    header_text, header_names = canonical_headers(signed_headers)
    canonical_request = "\n".join(
        [
            method,
            canonical_uri(path, normalize=normalize),
            canonical_query(query),
            header_text,
            header_names,
            hashlib.sha256(body).hexdigest(),
        ]
    )
    date = stamp[:8]
    scope = f"{date}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join(
        [ALGORITHM, stamp, scope, hashlib.sha256(canonical_request.encode()).hexdigest()]
    )
    signature = hmac.new(
        signing_key(credentials.secret_key, date, region, service),
        string_to_sign.encode(),
        hashlib.sha256,
    ).hexdigest()
    authorization = (
        f"{ALGORITHM} Credential={credentials.access_key}/{scope}, "
        f"SignedHeaders={header_names}, Signature={signature}"
    )
    out = [(name, value) for name, value in signed_headers]
    out.append(("Authorization", authorization))
    return Signed(canonical_request, string_to_sign, signature, tuple(out))
