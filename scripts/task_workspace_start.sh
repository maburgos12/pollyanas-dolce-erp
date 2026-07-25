#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=lib/task_workspace_common.sh
source "$SCRIPT_DIR/lib/task_workspace_common.sh"

repo="" root="" task="" branch="" owner="" scope=""
while (( $# )); do
  case "$1" in
    --repo|--root|--task|--branch|--owner|--scope)
      key="${1#--}"; require_value "$1" "${2:-}"; printf -v "$key" '%s' "$2"; shift 2 ;;
    *) die "opción desconocida: $1" ;;
  esac
done

for value in repo root task branch owner scope; do
  [[ -n "${!value}" ]] || die "falta --$value"
done
validate_id task "$task"
validate_id owner "$owner"
[[ "$branch" =~ ^codex/[A-Za-z0-9._/-]+$ ]] || die "la rama debe iniciar con codex/"

repo="$(cd "$repo" && pwd -P)"
mkdir -p "$root"
root="$(cd "$root" && pwd -P)"
worktree="$root/$task"
[[ "$worktree" == "$root/"* ]] || die "ruta de worktree fuera de la raíz autorizada"
[[ ! -e "$worktree" ]] || die "el worktree ya existe: $worktree"

registry="$(init_registry "$repo")"
lock="$registry/locks/global.lock"
acquire_lock "$lock"
trap 'release_lock "$lock"' EXIT

if find_record "$registry" "$task" >/dev/null; then
  die "la tarea ya está registrada: $task"
fi
git -C "$repo" fetch origin main --quiet
[[ "$(git -C "$repo" branch --show-current)" == "main" ]] \
  || die "el checkout base debe permanecer en main"
[[ -z "$(git -C "$repo" status --porcelain)" ]] \
  || die "el checkout base contiene cambios"
git -C "$repo" show-ref --verify --quiet "refs/heads/$branch" \
  && die "la rama local ya existe: $branch"

base="$(git -C "$repo" rev-parse origin/main)"
created_branch=0
rollback() {
  if (( created_branch )); then
    git -C "$repo" worktree remove "$worktree" --force >/dev/null 2>&1 || true
    git -C "$repo" branch -D "$branch" >/dev/null 2>&1 || true
  fi
}
trap 'status=$?; (( status == 0 )) || rollback; release_lock "$lock"; exit "$status"' EXIT

git -C "$repo" worktree add -b "$branch" "$worktree" "$base" >/dev/null
created_branch=1
write_record "$registry/active/$task.json" "$task" "$owner" "$branch" \
  "$worktree" "$base" "$scope" active
printf '{"event":"start","task":"%s","at":"%s"}\n' \
  "$(json_escape "$task")" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$registry/audit-log.jsonl"
created_branch=0
echo "OK: tarea=$task rama=$branch worktree=$worktree base=$base"
