#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
source "$SCRIPT_DIR/lib/task_workspace_common.sh"

repo="" task="" to="" status="" note=""
while (( $# )); do
  case "$1" in
    --repo|--task|--to|--status|--note)
      key="${1#--}"; require_value "$1" "${2:-}"; printf -v "$key" '%s' "$2"; shift 2 ;;
    *) die "opción desconocida: $1" ;;
  esac
done
for value in repo task to status; do [[ -n "${!value}" ]] || die "falta --$value"; done
validate_id task "$task"
registry="$(init_registry "$repo")"
lock="$registry/locks/global.lock"
acquire_lock "$lock"
trap 'release_lock "$lock"' EXIT
source_file="$registry/active/$task.json"
[[ -f "$source_file" ]] || die "la tarea no está activa: $task"
branch="$(json_field "$source_file" branch)"
worktree="$(json_field "$source_file" worktree)"
base="$(json_field "$source_file" base_commit)"
scope="$(json_field "$source_file" scope)"
destination="$registry/delivered/$task.json"
[[ "$status" == "blocked" ]] && destination="$registry/blocked/$task.json"
write_record "$destination" "$task" "$to" "$branch" \
  "$worktree" "$base" "$scope" "$status" "$note"
rm "$source_file"
printf '{"event":"handoff","task":"%s","to":"%s","at":"%s"}\n' \
  "$(json_escape "$task")" "$(json_escape "$to")" \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$registry/audit-log.jsonl"
echo "OK: tarea=$task entregada_a=$to estado=$status"
