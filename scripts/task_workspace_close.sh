#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
source "$SCRIPT_DIR/lib/task_workspace_common.sh"
script_path="$SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")"
original_args=("$@")

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
repo="$(cd "$repo" && pwd -P)"
registry="$(init_registry "$repo")"
lock="$registry/locks/global.lock"
acquire_lock "$lock"
trap 'release_lock "$lock"' EXIT
sync_root_main "$repo" "$registry"
root_script="$repo/scripts/task_workspace_close.sh"
if [[ "$ROOT_SYNC_CHANGED" == "1" && "$script_path" == "$root_script" \
  && "${TASK_WORKSPACE_CLOSE_REEXEC_SHA:-}" != "$ROOT_SYNC_SHA" ]]; then
  TASK_WORKSPACE_CLOSE_REEXEC_SHA="$ROOT_SYNC_SHA" \
    exec "$root_script" "${original_args[@]}"
fi
if [[ -f "$registry/closed/$task.json" || -f "$registry/discarded/$task.json" ]]; then
  echo "OK: tarea=$task ya estaba cerrada"
  exit 0
fi
record="$(find_record "$registry" "$task")" || die "tarea no registrada: $task"
case "$record" in "$registry/active/"*|"$registry/delivered/"*|"$registry/blocked/"*) ;; *) die "estado no cerrable";; esac

branch="$(json_field "$record" branch)"
worktree="$(json_field "$record" worktree)"
base="$(json_field "$record" base_commit)"
scope="$(json_field "$record" scope)"
owner="$(json_field "$record" owner)"
# Un cierre interrumpido puede dejar el worktree ya removido y la tarea todavía
# activa. Ese reintento debe poder completarse: solo se exige auditoría manual
# cuando Git aún lo lista, porque ahí sí hay un estado inconsistente que revisar.
worktree_presente=1
if [[ ! -d "$worktree" ]]; then
  if git -C "$repo" worktree list --porcelain | grep -Fxq "worktree $worktree"; then
    die "worktree ausente del disco pero aún registrado en Git; auditar manualmente antes de cerrar"
  fi
  git -C "$repo" show-ref --verify --quiet "refs/heads/$branch" \
    || die "worktree y rama ausentes; auditar manualmente antes de cerrar"
  worktree_presente=0
fi
if (( worktree_presente )); then
  [[ -z "$(git -C "$worktree" status --porcelain)" ]] || die "worktree con cambios sin guardar"
fi

if (( worktree_presente )); then
  head="$(git -C "$worktree" rev-parse HEAD)"
else
  head="$(git -C "$repo" rev-parse "$branch")"
fi
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

if (( worktree_presente )); then
  git -C "$repo" worktree remove "$worktree"
fi
# La rama local se borra ANTES que la remota. `git branch -d` valida contra el
# upstream, y `push --delete` también borra refs/remotes/origin/<rama>: si la
# remota se va primero, `-d` se queda sin referencia, cae en el HEAD del checkout
# base —que suele estar atrasado respecto a origin/main— y falla con "not fully
# merged", dejando la tarea a medio cerrar. La corroboración real ya ocurrió
# arriba contra origin/main.
if [[ "$state" == "discarded" ]]; then
  git -C "$repo" branch -D "$branch" >/dev/null
else
  git -C "$repo" branch -d "$branch" >/dev/null
fi
if [[ "$state" == "merged" ]] \
  && git -C "$repo" show-ref --verify --quiet "refs/remotes/origin/$branch"; then
  git -C "$repo" push origin --delete "$branch" >/dev/null
fi
destination="$registry/closed/$task.json"
[[ "$state" == "discarded" ]] && destination="$registry/discarded/$task.json"
write_record "$destination" "$task" "$owner" "$branch" "$worktree" "$base" \
  "$scope" "$state" "cierre corroborado"
rm "$record"
git -C "$repo" fetch --prune origin --quiet
sync_root_main "$repo" "$registry"
git -C "$repo" worktree prune
printf '{"event":"close","task":"%s","state":"%s","at":"%s"}\n' \
  "$(json_escape "$task")" "$state" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  >>"$registry/audit-log.jsonl"
echo "OK: tarea=$task estado=$state"
