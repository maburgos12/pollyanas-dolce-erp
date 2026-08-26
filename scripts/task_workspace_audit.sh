#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
source "$SCRIPT_DIR/lib/task_workspace_common.sh"
export GIT_OPTIONAL_LOCKS=0

repo=""
while (( $# )); do
  case "$1" in
    --repo) require_value "$1" "${2:-}"; repo="$2"; shift 2 ;;
    *) die "opción desconocida: $1" ;;
  esac
done
[[ -n "$repo" ]] || die "falta --repo"
repo="$(base_checkout_path "$repo")"
registry="$(registry_root "$repo")"
count() {
  if [[ -d "$registry/$1" ]]; then
    find "$registry/$1" -maxdepth 1 -type f -name '*.json' | wc -l | tr -d ' '
  else
    printf '0'
  fi
}
worktrees="$(git -C "$repo" worktree list --porcelain | awk '/^worktree /{n++} END{print n+0}')"
detached="$(git -C "$repo" worktree list --porcelain | awk '/^detached$/{n++} END{print n+0}')"
branches="$(git -C "$repo" for-each-ref --format='%(refname)' refs/heads | wc -l | tr -d ' ')"
unregistered=0
while IFS= read -r worktree; do
  [[ "$worktree" == "$(cd "$repo" && pwd -P)" ]] && continue
  registered=0
  if [[ -d "$registry" ]]; then
    while IFS= read -r record; do
      [[ "$(json_field "$record" worktree)" == "$worktree" ]] && {
        registered=1
        break
      }
    done < <(find "$registry/active" "$registry/delivered" "$registry/blocked" \
      -maxdepth 1 -type f -name '*.json' 2>/dev/null)
  fi
  (( registered == 1 )) || unregistered=$((unregistered + 1))
done < <(git -C "$repo" worktree list --porcelain | sed -n 's/^worktree //p')

unique_branches=0
while IFS= read -r branch; do
  [[ "$branch" == "main" ]] && continue
  if ! git -C "$repo" merge-base --is-ancestor "$branch" origin/main 2>/dev/null; then
    unique_branches=$((unique_branches + 1))
  fi
done < <(git -C "$repo" for-each-ref --format='%(refname:short)' refs/heads)

root_branch="$(git -C "$repo" branch --show-current)"
root_clean="SI"
[[ -z "$(git -C "$repo" status --porcelain)" ]] || root_clean="NO"
root_head="$(git -C "$repo" rev-parse main 2>/dev/null || printf 'AUSENTE')"
origin_head="$(git -C "$repo" rev-parse origin/main 2>/dev/null || printf 'AUSENTE')"
root_ahead="N/D"
root_behind="N/D"
root_state="BLOQUEADO_REFERENCIAS"
if [[ "$root_head" != "AUSENTE" && "$origin_head" != "AUSENTE" ]]; then
  read -r root_ahead root_behind < <(
    git -C "$repo" rev-list --left-right --count main...origin/main
  )
  if [[ "$root_branch" != "main" ]]; then
    root_state="BLOQUEADO_RAMA"
  elif [[ "$root_clean" != "SI" ]]; then
    root_state="BLOQUEADO_CAMBIOS"
  elif (( root_ahead == 0 && root_behind == 0 )); then
    root_state="SINCRONIZADO"
  elif (( root_ahead == 0 && root_behind > 0 )); then
    root_state="REQUIERE_FAST_FORWARD"
  else
    root_state="DIVERGENTE"
  fi
fi

echo "ROOT_MAIN: $root_state"
echo "ROOT_BRANCH: ${root_branch:-DETACHED}"
echo "ROOT_LIMPIO: $root_clean"
echo "ROOT_AHEAD: $root_ahead"
echo "ROOT_BEHIND: $root_behind"
echo "ROOT_HEAD: $root_head"
echo "ORIGIN_MAIN_SHA: $origin_head"
echo "WORKTREES: $worktrees"
echo "RAMAS_LOCALES: $branches"
echo "DETACHED: $detached"
echo "WORKTREES_SIN_REGISTRO: $unregistered"
echo "RAMAS_CON_COMMITS_UNICOS: $unique_branches"
echo "ACTIVOS: $(count active)"
echo "ENTREGADOS: $(count delivered)"
echo "BLOQUEADOS: $(count blocked)"
echo "CERRADOS: $(count closed)"
echo "DESCARTADOS: $(count discarded)"
