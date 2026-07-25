#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
source "$SCRIPT_DIR/lib/task_workspace_common.sh"

repo="" worktree="" task="" owner="" scope=""
while (( $# )); do
  case "$1" in
    --repo|--worktree|--task|--owner|--scope)
      key="${1#--}"; require_value "$1" "${2:-}"; printf -v "$key" '%s' "$2"; shift 2 ;;
    *) die "opción desconocida: $1" ;;
  esac
done
for value in repo worktree task owner scope; do
  [[ -n "${!value}" ]] || die "falta --$value"
done
validate_id task "$task"
validate_id owner "$owner"
repo="$(cd "$repo" && pwd -P)"
worktree="$(cd "$worktree" && pwd -P)"
[[ "$worktree" != "$repo" ]] || die "no se puede adoptar el checkout base"
git -C "$repo" worktree list --porcelain | grep -Fxq "worktree $worktree" \
  || die "la ruta no es un worktree registrado por Git"
branch="$(git -C "$worktree" branch --show-current)"
[[ -n "$branch" ]] || die "no se puede adoptar detached HEAD; crear rama de rescate"
[[ "$branch" != "main" ]] || die "no se puede adoptar main"
[[ -z "$(git -C "$worktree" status --porcelain)" ]] \
  || die "el worktree debe estar limpio antes de adoptarlo"

registry="$(init_registry "$repo")"
lock="$registry/locks/global.lock"
acquire_lock "$lock"
trap 'release_lock "$lock"' EXIT
find_record "$registry" "$task" >/dev/null 2>&1 \
  && die "la tarea ya está registrada: $task"
while IFS= read -r record; do
  [[ "$(json_field "$record" worktree)" != "$worktree" ]] \
    || die "el worktree ya tiene propietario"
  [[ "$(json_field "$record" branch)" != "$branch" ]] \
    || die "la rama ya pertenece a otra tarea"
done < <(find "$registry/active" "$registry/delivered" "$registry/blocked" \
  -maxdepth 1 -type f -name '*.json' 2>/dev/null)
base="$(git -C "$repo" merge-base "$branch" origin/main)"
write_record "$registry/active/$task.json" "$task" "$owner" "$branch" \
  "$worktree" "$base" "$scope" active "worktree heredado adoptado"
printf '{"event":"adopt","task":"%s","at":"%s"}\n' \
  "$(json_escape "$task")" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  >>"$registry/audit-log.jsonl"
echo "OK: tarea=$task adoptada rama=$branch worktree=$worktree"
