#!/usr/bin/env bash
export COORDINATOR_MODEL=grok-4.7
exec "$(dirname "$(readlink -f "$0")")/engine-grok.sh" "$@"
