#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
source "$SCRIPT_DIR/lib/task_workspace_common.sh"

repo=""
while (( $# )); do
  case "$1" in
    --repo) require_value "$1" "${2:-}"; repo="$2"; shift 2 ;;
    *) die "opción desconocida: $1" ;;
  esac
done
[[ -n "$repo" ]] || die "falta --repo"
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
