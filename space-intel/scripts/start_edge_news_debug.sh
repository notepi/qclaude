#!/bin/zsh

set -euo pipefail

PROFILE_DIR="$HOME/.edge-codex-debug"

mkdir -p "$PROFILE_DIR"
open -na "Microsoft Edge" --args --remote-debugging-port=9222 --user-data-dir="$PROFILE_DIR"
