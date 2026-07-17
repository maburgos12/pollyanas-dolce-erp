#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "--write" ]]; then
  echo "Uso: bash scripts/git_workspace_preflight.sh --write" >&2
  echo "Valida un worktree antes de modificar archivos." >&2
  exit 2
fi

repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || {
  echo "ERROR: la carpeta actual no pertenece a un repositorio Git." >&2
  exit 1
}
common_dir="$(git rev-parse --git-common-dir)"
if [[ "$common_dir" != /* ]]; then
  common_dir="$repo_root/$common_dir"
fi
base_checkout="$(cd "$common_dir/.." && pwd -P)"
current_branch="$(git branch --show-current)"

if [[ -z "$current_branch" ]]; then
  echo "ERROR: detached HEAD. Crea un worktree nuevo desde origin/main." >&2
  exit 1
fi

if [[ "$repo_root" == "$base_checkout" ]]; then
  echo "ERROR: el checkout base es de solo lectura; trabaja en codex_worktrees/." >&2
  exit 1
fi

if [[ "$current_branch" == "main" ]]; then
  echo "ERROR: no se permite implementar directamente sobre main." >&2
  exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "ERROR: el worktree ya contiene cambios o archivos sin seguimiento." >&2
  git status --short --branch >&2
  exit 1
fi

git fetch origin main --quiet
read -r behind ahead < <(git rev-list --left-right --count origin/main...HEAD)
if (( behind > 0 )); then
  echo "ERROR: la rama está $behind commit(s) detrás de origin/main." >&2
  exit 1
fi

if git worktree list --porcelain | grep -q '^prunable '; then
  echo "AVISO: existen worktrees prunable; revisar con git worktree prune --dry-run." >&2
fi

echo "OK: worktree limpio, rama $current_branch, detrás=0, adelante=$ahead."
