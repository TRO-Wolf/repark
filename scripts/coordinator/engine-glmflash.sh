#!/usr/bin/env bash
export COORDINATOR_MODEL=zai/glm-5.3-flash
exec "$(dirname "$(readlink -f "$0")")/engine-glm.sh" "$@"
