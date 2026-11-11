#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BIN_PATH="${ROOT_DIR}/artifacts/linux/simple-chat"

if [ ! -f "${BIN_PATH}" ]; then
  echo "Linuxバイナリが見つかりません。先に以下を実行してビルドしてください:"
  echo "  docker compose run --rm build-linux"
  exit 1
fi

if [ ! -x "${BIN_PATH}" ]; then
  chmod +x "${BIN_PATH}" 2>/dev/null || true
fi

# 第1引数にプロファイル名（例: ./scripts/run-linux.sh sub）を渡すと
# 別設定ファイル (config-sub.json) で2人目のテスト起動が可能です。
if [ $# -ge 1 ]; then
  export SIMPLE_CHAT_PROFILE="$1"
  shift
fi

# NVIDIA + Wayland (KDE Plasma等) 環境で透過・最前面・ドラッグ移動を確実に機能させる設定
if [ "${XDG_SESSION_TYPE:-}" = "wayland" ] && [ -n "${DISPLAY:-}" ]; then
  export GDK_BACKEND="${GDK_BACKEND:-x11}"
fi
export WEBKIT_DISABLE_DMABUF_RENDERER="${WEBKIT_DISABLE_DMABUF_RENDERER:-1}"

exec "${BIN_PATH}" "$@"
