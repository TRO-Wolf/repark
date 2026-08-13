#!/usr/bin/env bash
# Bump the TRO-Wolf/iceberg-rust [patch.crates-io] pin. The whole iceberg* family moves
# together — all five `rev = "…"` lines in the root Cargo.toml stay identical (the
# single-writer-per-pin invariant). Contract: docs/fork-sync.md — the bump rides its OWN PR,
# gated by `make preflight`, and fork main must be green before its rev is pinnable.
# Usage: scripts/bump_fork_pin.sh <40-hex-rev | branch-name>   (or: make bump-fork-pin REV=…)
set -euo pipefail

FORK_URL="https://github.com/TRO-Wolf/iceberg-rust"
REF="${1:?usage: bump_fork_pin.sh <rev|branch>}"

cd "$(git rev-parse --show-toplevel)"

mapfile -t distinct < <(grep -oE 'rev = "[0-9a-f]{40}"' Cargo.toml | sort -u)
if [ "${#distinct[@]}" -ne 1 ]; then
    echo "pin invariant broken: ${#distinct[@]} distinct revs in Cargo.toml — fix by hand first" >&2
    exit 1
fi
old="${distinct[0]#rev = \"}"
old="${old%\"}"

if [[ "$REF" =~ ^[0-9a-f]{40}$ ]]; then
    new="$REF"
else
    new="$(git ls-remote "$FORK_URL" "refs/heads/$REF" | cut -f1)"
    if [ -z "$new" ]; then
        echo "branch '$REF' not found on $FORK_URL" >&2
        exit 1
    fi
fi

if [ "$new" = "$old" ]; then
    echo "already pinned at $old"
    exit 0
fi

n="$(grep -c "rev = \"$old\"" Cargo.toml)"
if [ "$n" -ne 5 ]; then
    echo "expected 5 rev lines, found $n — Cargo.toml [patch] layout changed; bump by hand" >&2
    exit 1
fi
sed -i "s/rev = \"$old\"/rev = \"$new\"/g" Cargo.toml

cargo update -p iceberg -p iceberg-datafusion -p iceberg-catalog-glue \
    -p iceberg-catalog-s3tables -p iceberg-storage-opendal

echo "pin: $old -> $new (5 lines rewritten; Cargo.lock updated)"
echo "changelog for the PR body: $FORK_URL/compare/$old...$new"
echo "next: make preflight — the bump rides its own PR (docs/fork-sync.md)"
