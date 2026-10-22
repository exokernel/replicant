# shellcheck shell=bash
#
# docker compose helpers. Sourced after scripts/lib/stack.sh.

COMPOSE_FILE=deploy/docker/compose.generated.yaml

# Runs `docker compose` against the generated compose file.
compose() {
    docker compose -f "$COMPOSE_FILE" "$@"
}

# Writes the compose file for $1 replicas backed by CRDT library $2.
#
# gen-compose.py needs pyyaml. The analysis .venv has it, from
# analysis/requirements.txt.
compose_generate() {
    "$(python_bin)" deploy/docker/gen-compose.py "$1" --crdt "$2" > "$COMPOSE_FILE"
}
