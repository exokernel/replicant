# shellcheck shell=bash
#
# Puts the pinned Node and npm first on PATH. Sourced by the scripts in this
# directory. It sets `here` to this directory.
#
# mise installs the pinned version if it is missing. The trust setting applies to
# this one command, and it names this directory only.

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

if ! command -v mise >/dev/null; then
    echo "error: these tools need mise, to pin the Node version" >&2
    exit 1
fi
node_path=$(MISE_TRUSTED_CONFIG_PATHS="$here" mise exec --cd "$here" -- which node)
PATH="$(dirname "$node_path"):$PATH"
export PATH
