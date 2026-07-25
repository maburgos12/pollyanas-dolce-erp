#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
source "$SCRIPT_DIR/lib/task_workspace_common.sh"

repo="" task="" state=""
while (( $# )); do
  case "$1" in
    --repo|--task|--state)
      key="${1#--}"; require_value "$1" "${2:-}"; printf -v "$key" '%s' "$2"; shift 2 ;;
    *) die "opción desconocida: $1" ;;
  esac
done
for value in repo task state; do [[ -n "${!value}" ]] || die "falta --$value"; done
[[ "$state" == "merged" || "$state" == "discarded" ]] || die "estado inválido"
registry="$(init_registry "$repo")"
if [[ -f "$registry/closed/$task.json" || -f "$registry/discarded/$task.json" ]]; then
  echo "OK: tarea=$task ya estaba cerrada"
  exit 0
fi
record="$(find_record "$registry" "$task")" || die "tarea no registrada: $task"
case "$record" in "$registry/active/"*|"$registry/delivered/"*|"$registry/blocked/"*) ;; *) die "estado no cerrable";; esac

lock="$registry/locks/global.lock"
acquire_lock "$lock"
trap 'release_lock "$lock"' EXIT
branch="$(json_field "$record" branch)"
worktree="$(json_field "$record" worktree)"
base="$(json_field "$record" base_commit)"
scope="$(json_field "$record" scope)"
owner="$(json_field "$record" owner)"
[[ -d "$worktree" ]] || die "worktree ausente; auditar manualmente antes de cerrar"
[[ -z "$(git -C "$worktree" status --porcelain)" ]] || die "worktree con cambios sin guardar"

git -C "$repo" fetch origin main --quiet
head="$(git -C "$worktree" rev-parse HEAD)"
if [[ "$state" == "merged" ]]; then
  git -C "$repo" merge-base --is-ancestor "$head" origin/main \
    || die "HEAD contiene commits ausentes de origin/main"
  if git -C "$repo" ls-remote --exit-code --heads origin "$branch" >/dev/null 2>&1; then
    git -C "$repo" fetch origin "$branch:refs/remotes/origin/$branch" --quiet
    git -C "$repo" merge-base --is-ancestor "refs/remotes/origin/$branch" origin/main \
      || die "la rama remota contiene commits ausentes de origin/main"
  fi
else
  recovery="$registry/recovery/${task}-$(date -u +%Y%m%dT%H%M%SZ).bundle"
  git -C "$repo" bundle create "$recovery" "$branch" >/dev/null
fi

if [[ "$state" == "merged" ]] \
  && git -C "$repo" show-ref --verify --quiet "refs/remotes/origin/$branch"; then
  git -C "$repo" push origin --delete "$branch" >/dev/null
fi
git -C "$repo" worktree remove "$worktree"
if [[ "$state" == "discarded" ]]; then
  git -C "$repo" branch -D "$branch" >/dev/null
else
  git -C "$repo" branch -d "$branch" >/dev/null
fi
destination="$registry/closed/$task.json"
[[ "$state" == "discarded" ]] && destination="$registry/discarded/$task.json"
write_record "$destination" "$task" "$owner" "$branch" "$worktree" "$base" \
  "$scope" "$state" "cierre corroborado"
rm "$record"
git -C "$repo" fetch --prune origin --quiet
git -C "$repo" worktree prune
printf '{"event":"close","task":"%s","state":"%s","at":"%s"}\n' \
  "$(json_escape "$task")" "$state" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  >>"$registry/audit-log.jsonl"
echo "OK: tarea=$task estado=$state"
