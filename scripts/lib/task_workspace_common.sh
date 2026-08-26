#!/usr/bin/env bash

die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_value() {
  [[ -n "${2:-}" ]] || die "falta valor para $1"
}

validate_id() {
  [[ "$2" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] \
    || die "$1 inválido: usa letras, números, punto, guion o guion bajo"
}

json_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  printf '%s' "$value"
}

repo_common_dir() {
  local repo="$1" common
  common="$(git -C "$repo" rev-parse --git-common-dir 2>/dev/null)" \
    || die "$repo no es un repositorio Git"
  if [[ "$common" != /* ]]; then
    common="$(cd "$repo" && cd "$common" && pwd -P)"
  fi
  printf '%s\n' "$common"
}

base_checkout_path() {
  local common
  common="$(repo_common_dir "$1")"
  cd "$common/.." && pwd -P
}

registry_root() {
  printf '%s/task-workspaces\n' "$(repo_common_dir "$1")"
}

init_registry() {
  local root
  root="$(registry_root "$1")"
  mkdir -p "$root"/{active,delivered,blocked,closed,discarded,locks,recovery}
  touch "$root/audit-log.jsonl"
  printf '%s\n' "$root"
}

find_record() {
  local root="$1" task="$2" state
  for state in active delivered blocked closed discarded; do
    if [[ -f "$root/$state/$task.json" ]]; then
      printf '%s\n' "$root/$state/$task.json"
      return 0
    fi
  done
  return 1
}

json_field() {
  local file="$1" key="$2"
  sed -n "s/.*\"$key\": \"\\([^\"]*\\)\".*/\\1/p" "$file" | head -1
}

write_record() {
  local file="$1" task="$2" owner="$3" branch="$4" worktree="$5"
  local base="$6" scope="$7" status="$8" note="${9:-}"
  local tmp="${file}.tmp.$$"
  umask 077
  {
    printf '{\n'
    printf '  "task_id": "%s",\n' "$(json_escape "$task")"
    printf '  "owner": "%s",\n' "$(json_escape "$owner")"
    printf '  "branch": "%s",\n' "$(json_escape "$branch")"
    printf '  "worktree": "%s",\n' "$(json_escape "$worktree")"
    printf '  "base_commit": "%s",\n' "$(json_escape "$base")"
    printf '  "scope": "%s",\n' "$(json_escape "$scope")"
    printf '  "status": "%s",\n' "$(json_escape "$status")"
    printf '  "note": "%s",\n' "$(json_escape "$note")"
    printf '  "updated_at": "%s"\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '}\n'
  } >"$tmp"
  mv "$tmp" "$file"
}

acquire_lock() {
  local lock="$1"
  if ! mkdir "$lock" 2>/dev/null; then
    if [[ -f "$lock/pid" && "$(cat "$lock/pid")" == "$$" ]]; then
      return 0
    fi
    die "otra operación controla este registro: $lock"
  fi
  printf '%s\n' "$$" >"$lock/pid"
}

release_lock() {
  rm -f "$1/pid"
  rmdir "$1" 2>/dev/null || true
}

sync_root_main() {
  local repo="$1" registry="$2" base_checkout branch
  local before after remote ahead behind
  repo="$(cd "$repo" && pwd -P)"
  base_checkout="$(base_checkout_path "$repo")"
  [[ "$repo" == "$base_checkout" ]] \
    || die "--repo debe apuntar al checkout raíz: $base_checkout"

  branch="$(git -C "$repo" branch --show-current)"
  [[ "$branch" == "main" ]] || die "el checkout raíz debe permanecer en main"
  [[ -z "$(git -C "$repo" status --porcelain)" ]] \
    || die "el checkout raíz contiene cambios; no se sincronizó"

  git -C "$repo" fetch origin main --quiet
  git -C "$repo" show-ref --verify --quiet refs/heads/main \
    || die "falta la rama local main"
  git -C "$repo" show-ref --verify --quiet refs/remotes/origin/main \
    || die "falta la referencia origin/main"

  read -r ahead behind < <(
    git -C "$repo" rev-list --left-right --count main...origin/main
  )
  (( ahead == 0 )) \
    || die "main diverge de origin/main: adelante=$ahead atrás=$behind"

  before="$(git -C "$repo" rev-parse main)"
  if (( behind > 0 )); then
    git -C "$repo" merge --ff-only --quiet origin/main
  fi
  after="$(git -C "$repo" rev-parse main)"
  remote="$(git -C "$repo" rev-parse origin/main)"
  [[ "$after" == "$remote" ]] \
    || die "main no quedó sincronizada con origin/main"

  ROOT_SYNC_BEFORE="$before"
  ROOT_SYNC_SHA="$after"
  ROOT_SYNC_CHANGED=0
  [[ "$before" == "$after" ]] || ROOT_SYNC_CHANGED=1

  printf '{"event":"root-sync","before":"%s","after":"%s","behind":%s,"at":"%s"}\n' \
    "$before" "$after" "$behind" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    >>"$registry/audit-log.jsonl"
}
