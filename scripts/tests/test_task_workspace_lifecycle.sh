#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
START="$PROJECT_ROOT/scripts/task_workspace_start.sh"
PREFLIGHT="$PROJECT_ROOT/scripts/git_workspace_preflight.sh"
HANDOFF="$PROJECT_ROOT/scripts/task_workspace_handoff.sh"
AUDIT="$PROJECT_ROOT/scripts/task_workspace_audit.sh"
CLOSE="$PROJECT_ROOT/scripts/task_workspace_close.sh"
ADOPT="$PROJECT_ROOT/scripts/task_workspace_adopt.sh"
COMMON="$PROJECT_ROOT/scripts/lib/task_workspace_common.sh"

passed=0
failed=0

fail() {
  echo "FAIL: $*" >&2
  failed=$((failed + 1))
}

pass() {
  echo "PASS: $*"
  passed=$((passed + 1))
}

assert_success() {
  local name="$1"
  shift
  if "$@" >"$TEST_TMP/output" 2>&1; then pass "$name"; else
    cat "$TEST_TMP/output" >&2
    fail "$name"
  fi
}

assert_failure() {
  local name="$1"
  shift
  if "$@" >"$TEST_TMP/output" 2>&1; then
    cat "$TEST_TMP/output" >&2
    fail "$name"
  else
    pass "$name"
  fi
}

setup_repo() {
  TEST_TMP="$(mktemp -d)"
  export TEST_TMP
  git init --bare "$TEST_TMP/origin.git" >/dev/null
  git init -b main "$TEST_TMP/repo" >/dev/null
  git -C "$TEST_TMP/repo" config user.name Test
  git -C "$TEST_TMP/repo" config user.email test@example.com
  echo base >"$TEST_TMP/repo/README.md"
  git -C "$TEST_TMP/repo" add README.md
  git -C "$TEST_TMP/repo" commit -m base >/dev/null
  git -C "$TEST_TMP/repo" remote add origin "$TEST_TMP/origin.git"
  git -C "$TEST_TMP/repo" push -u origin main >/dev/null
  mkdir -p "$TEST_TMP/worktrees"
}

cleanup_repo() {
  rm -rf "$TEST_TMP"
}

advance_remote_main() {
  git clone "$TEST_TMP/origin.git" "$TEST_TMP/publisher" >/dev/null 2>&1
  git -C "$TEST_TMP/publisher" config user.name Publisher
  git -C "$TEST_TMP/publisher" config user.email publisher@example.com
  echo remote >>"$TEST_TMP/publisher/README.md"
  git -C "$TEST_TMP/publisher" add README.md
  git -C "$TEST_TMP/publisher" commit -m remote >/dev/null
  git -C "$TEST_TMP/publisher" push origin main >/dev/null
}

test_root_sync_on_start() {
  setup_repo
  advance_remote_main
  remote_head="$(git --git-dir="$TEST_TMP/origin.git" rev-parse main)"
  [[ "$(git -C "$TEST_TMP/repo" rev-parse main)" != "$remote_head" ]] \
    && pass "fixture leaves base checkout behind remote main" \
    || fail "fixture leaves base checkout behind remote main"

  assert_success "start fast-forwards clean base checkout" \
    "$START" --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task synced --branch codex/synced --owner test --scope scripts
  [[ "$(git -C "$TEST_TMP/repo" rev-parse main)" == "$remote_head" ]] \
    && pass "start leaves root main synchronized" \
    || fail "start leaves root main synchronized"
  [[ "$(git -C "$TEST_TMP/worktrees/synced" rev-parse HEAD)" == "$remote_head" ]] \
    && pass "start pins worktree to synchronized remote head" \
    || fail "start pins worktree to synchronized remote head"
  cleanup_repo
}

test_start_reexecutes_after_self_update() {
  setup_repo
  mkdir -p "$TEST_TMP/repo/scripts/lib"
  cp "$START" "$TEST_TMP/repo/scripts/task_workspace_start.sh"
  cp "$COMMON" "$TEST_TMP/repo/scripts/lib/task_workspace_common.sh"
  chmod +x "$TEST_TMP/repo/scripts/task_workspace_start.sh"
  git -C "$TEST_TMP/repo" add scripts
  git -C "$TEST_TMP/repo" commit -m lifecycle >/dev/null
  git -C "$TEST_TMP/repo" push origin main >/dev/null

  git clone "$TEST_TMP/origin.git" "$TEST_TMP/publisher" >/dev/null 2>&1
  git -C "$TEST_TMP/publisher" config user.name Publisher
  git -C "$TEST_TMP/publisher" config user.email publisher@example.com
  perl -0pi -e 's/set -euo pipefail/set -euo pipefail\nprintf reexecuted >"\$REEXEC_MARKER"/' \
    "$TEST_TMP/publisher/scripts/task_workspace_start.sh"
  git -C "$TEST_TMP/publisher" add scripts/task_workspace_start.sh
  git -C "$TEST_TMP/publisher" commit -m lifecycle-v2 >/dev/null
  git -C "$TEST_TMP/publisher" push origin main >/dev/null

  assert_success "start reexecutes the version received by fast-forward" \
    env REEXEC_MARKER="$TEST_TMP/reexecuted" \
    "$TEST_TMP/repo/scripts/task_workspace_start.sh" \
    --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task reexec --branch codex/reexec --owner test --scope scripts
  [[ -f "$TEST_TMP/reexecuted" ]] \
    && pass "updated start script executed in the same operation" \
    || fail "updated start script executed in the same operation"
  [[ ! -d "$TEST_TMP/repo/.git/task-workspaces/locks/global.lock" ]] \
    && pass "self reexec releases lifecycle lock" \
    || fail "self reexec releases lifecycle lock"
  cleanup_repo
}

test_close_reexecutes_after_self_update() {
  setup_repo
  mkdir -p "$TEST_TMP/repo/scripts/lib"
  cp "$START" "$TEST_TMP/repo/scripts/task_workspace_start.sh"
  cp "$CLOSE" "$TEST_TMP/repo/scripts/task_workspace_close.sh"
  cp "$COMMON" "$TEST_TMP/repo/scripts/lib/task_workspace_common.sh"
  chmod +x "$TEST_TMP/repo/scripts/task_workspace_start.sh" \
    "$TEST_TMP/repo/scripts/task_workspace_close.sh"
  git -C "$TEST_TMP/repo" add scripts
  git -C "$TEST_TMP/repo" commit -m lifecycle >/dev/null
  git -C "$TEST_TMP/repo" push origin main >/dev/null

  "$TEST_TMP/repo/scripts/task_workspace_start.sh" \
    --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task close-reexec --branch codex/close-reexec --owner test --scope scripts \
    >/dev/null
  git -C "$TEST_TMP/worktrees/close-reexec" push -u origin codex/close-reexec \
    >/dev/null

  git clone "$TEST_TMP/origin.git" "$TEST_TMP/publisher" >/dev/null 2>&1
  git -C "$TEST_TMP/publisher" config user.name Publisher
  git -C "$TEST_TMP/publisher" config user.email publisher@example.com
  perl -0pi -e 's/set -euo pipefail/set -euo pipefail\nprintf reexecuted >"\$REEXEC_MARKER"/' \
    "$TEST_TMP/publisher/scripts/task_workspace_close.sh"
  git -C "$TEST_TMP/publisher" add scripts/task_workspace_close.sh
  git -C "$TEST_TMP/publisher" commit -m lifecycle-v2 >/dev/null
  git -C "$TEST_TMP/publisher" push origin main >/dev/null

  assert_success "close reexecutes the version received by fast-forward" \
    env REEXEC_MARKER="$TEST_TMP/close-reexecuted" \
    "$TEST_TMP/repo/scripts/task_workspace_close.sh" \
    --repo "$TEST_TMP/repo" --task close-reexec --state merged
  [[ -f "$TEST_TMP/close-reexecuted" ]] \
    && pass "updated close script executed before destructive steps" \
    || fail "updated close script executed before destructive steps"
  [[ ! -d "$TEST_TMP/worktrees/close-reexec" ]] \
    && pass "reexecuted close removes corroborated worktree" \
    || fail "reexecuted close removes corroborated worktree"
  [[ ! -d "$TEST_TMP/repo/.git/task-workspaces/locks/global.lock" ]] \
    && pass "close self reexec releases lifecycle lock" \
    || fail "close self reexec releases lifecycle lock"
  cleanup_repo
}

test_start_uses_pinned_sync_sha() {
  setup_repo
  advance_remote_main
  git -C "$TEST_TMP/publisher" checkout -b future >/dev/null
  echo future >>"$TEST_TMP/publisher/README.md"
  git -C "$TEST_TMP/publisher" add README.md
  git -C "$TEST_TMP/publisher" commit -m future >/dev/null
  git -C "$TEST_TMP/publisher" push origin future >/dev/null
  git -C "$TEST_TMP/repo" fetch origin future:refs/remotes/origin/future --quiet
  synced_sha="$(git --git-dir="$TEST_TMP/origin.git" rev-parse main)"
  future_sha="$(git -C "$TEST_TMP/repo" rev-parse origin/future)"

  real_git="$(command -v git)"
  mkdir -p "$TEST_TMP/bin"
  printf '%s\n' \
    '#!/usr/bin/env bash' \
    'if [[ "$*" == *"show-ref --verify --quiet refs/heads/codex/race"* ]]; then' \
    '  "$REAL_GIT" -C "$RACE_REPO" update-ref refs/remotes/origin/main "$RACE_SHA"' \
    'fi' \
    'exec "$REAL_GIT" "$@"' >"$TEST_TMP/bin/git"
  chmod +x "$TEST_TMP/bin/git"

  assert_success "start survives origin/main moving after synchronization" \
    env PATH="$TEST_TMP/bin:$PATH" REAL_GIT="$real_git" \
    RACE_REPO="$TEST_TMP/repo" RACE_SHA="$future_sha" \
    "$START" --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task race --branch codex/race --owner test --scope scripts
  [[ "$(git -C "$TEST_TMP/repo" rev-parse main)" == "$synced_sha" ]] \
    && pass "race fixture keeps root at synchronized SHA" \
    || fail "race fixture keeps root at synchronized SHA"
  [[ "$(git -C "$TEST_TMP/worktrees/race" rev-parse HEAD)" == "$synced_sha" ]] \
    && pass "start pins worktree to root synchronized SHA" \
    || fail "start pins worktree to root synchronized SHA"
  cleanup_repo
}

test_root_divergence_blocks_start() {
  setup_repo
  echo local >>"$TEST_TMP/repo/README.md"
  git -C "$TEST_TMP/repo" add README.md
  git -C "$TEST_TMP/repo" commit -m local >/dev/null
  advance_remote_main

  assert_failure "start rejects divergent root main" \
    "$START" --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task divergent --branch codex/divergent --owner test --scope scripts
  [[ ! -e "$TEST_TMP/worktrees/divergent" ]] \
    && pass "divergence creates no task worktree" \
    || fail "divergence creates no task worktree"
  cleanup_repo
}

test_start_and_preflight() {
  setup_repo
  assert_success "start creates registered worktree" \
    "$START" --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task demo --branch codex/demo --owner test --scope scripts
  [[ -f "$TEST_TMP/repo/.git/task-workspaces/active/demo.json" ]] \
    && pass "start writes registry" || fail "start writes registry"
  assert_success "registered worktree passes preflight" \
    bash -c "cd '$TEST_TMP/worktrees/demo' && '$PREFLIGHT' --write"
  mkdir "$TEST_TMP/repo/.git/task-workspaces/locks/global.lock"
  assert_failure "concurrent lifecycle operation is rejected" \
    "$START" --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task concurrent --branch codex/concurrent --owner test --scope scripts
  rmdir "$TEST_TMP/repo/.git/task-workspaces/locks/global.lock"
  assert_failure "duplicate task is rejected" \
    "$START" --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task demo --branch codex/demo-2 --owner test --scope scripts
  cleanup_repo
}

test_preflight_guards() {
  setup_repo
  assert_failure "base checkout is rejected" \
    bash -c "cd '$TEST_TMP/repo' && '$PREFLIGHT' --write"
  "$START" --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task dirty --branch codex/dirty --owner test --scope scripts >/dev/null
  echo dirty >"$TEST_TMP/worktrees/dirty/dirty.txt"
  assert_failure "dirty worktree is rejected" \
    bash -c "cd '$TEST_TMP/worktrees/dirty' && '$PREFLIGHT' --write"
  git -C "$TEST_TMP/repo" worktree add --detach "$TEST_TMP/worktrees/detached" origin/main >/dev/null
  assert_failure "detached worktree is rejected" \
    bash -c "cd '$TEST_TMP/worktrees/detached' && '$PREFLIGHT' --write"
  git -C "$TEST_TMP/repo" worktree add -b codex/unregistered \
    "$TEST_TMP/worktrees/unregistered" origin/main >/dev/null
  assert_failure "unregistered worktree is rejected once registry exists" \
    bash -c "cd '$TEST_TMP/worktrees/unregistered' && '$PREFLIGHT' --write"
  cleanup_repo
}

test_handoff_and_audit() {
  setup_repo
  assert_success "audit works before registry exists" \
    "$AUDIT" --repo "$TEST_TMP/repo"
  [[ ! -e "$TEST_TMP/repo/.git/task-workspaces" ]] \
    && pass "audit does not create registry" || fail "audit does not create registry"
  "$START" --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task handoff --branch codex/handoff --owner codex --scope scripts >/dev/null
  assert_success "handoff changes state" \
    "$HANDOFF" --repo "$TEST_TMP/repo" --task handoff --to claude \
    --status ready-for-review --note "tests pending"
  [[ -f "$TEST_TMP/repo/.git/task-workspaces/delivered/handoff.json" ]] \
    && pass "handoff moves registry" || fail "handoff moves registry"
  "$START" --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task blocked --branch codex/blocked --owner codex --scope scripts >/dev/null
  assert_success "handoff can mark blocked state" \
    "$HANDOFF" --repo "$TEST_TMP/repo" --task blocked --to codex \
    --status blocked --note "external dependency"
  [[ -f "$TEST_TMP/repo/.git/task-workspaces/blocked/blocked.json" ]] \
    && pass "blocked task has dedicated registry" || fail "blocked task has dedicated registry"
  git -C "$TEST_TMP/repo" worktree add -b codex/orphan \
    "$TEST_TMP/worktrees/orphan" origin/main >/dev/null
  echo unique >"$TEST_TMP/worktrees/orphan/unique.txt"
  git -C "$TEST_TMP/worktrees/orphan" add unique.txt
  git -C "$TEST_TMP/worktrees/orphan" commit -m unique >/dev/null
  assert_success "audit is read only and classifies registry" \
    "$AUDIT" --repo "$TEST_TMP/repo"
  grep -q "ENTREGADOS: 1" "$TEST_TMP/output" \
    && pass "audit reports delivered count" || fail "audit reports delivered count"
  grep -q "WORKTREES_SIN_REGISTRO: 1" "$TEST_TMP/output" \
    && pass "audit detects unregistered worktree" || fail "audit detects unregistered worktree"
  grep -Eq "RAMAS_CON_COMMITS_UNICOS: [1-9]" "$TEST_TMP/output" \
    && pass "audit detects unique branches" || fail "audit detects unique branches"
  cleanup_repo
}

test_audit_reports_root_state() {
  setup_repo
  assert_success "audit reports synchronized root" \
    "$AUDIT" --repo "$TEST_TMP/repo"
  grep -q "ROOT_MAIN: SINCRONIZADO" "$TEST_TMP/output" \
    && pass "audit classifies synchronized root" \
    || fail "audit classifies synchronized root"
  grep -q "ROOT_AHEAD: 0" "$TEST_TMP/output" \
    && pass "audit reports zero commits ahead" \
    || fail "audit reports zero commits ahead"
  grep -q "ROOT_BEHIND: 0" "$TEST_TMP/output" \
    && pass "audit reports zero commits behind" \
    || fail "audit reports zero commits behind"

  advance_remote_main
  git -C "$TEST_TMP/repo" fetch origin main --quiet
  assert_success "audit reports fast-forwardable root" \
    "$AUDIT" --repo "$TEST_TMP/repo"
  grep -q "ROOT_MAIN: REQUIERE_FAST_FORWARD" "$TEST_TMP/output" \
    && pass "audit classifies fast-forwardable root" \
    || fail "audit classifies fast-forwardable root"
  grep -q "ROOT_BEHIND: 1" "$TEST_TMP/output" \
    && pass "audit counts root lag" \
    || fail "audit counts root lag"
  cleanup_repo

  setup_repo
  echo dirty >"$TEST_TMP/repo/dirty.txt"
  assert_success "audit reports dirty root without mutating it" \
    "$AUDIT" --repo "$TEST_TMP/repo"
  grep -q "ROOT_MAIN: BLOQUEADO_CAMBIOS" "$TEST_TMP/output" \
    && pass "audit classifies dirty root" \
    || fail "audit classifies dirty root"
  [[ -f "$TEST_TMP/repo/dirty.txt" ]] \
    && pass "audit preserves dirty root file" \
    || fail "audit preserves dirty root file"
  cleanup_repo

  setup_repo
  echo local >>"$TEST_TMP/repo/README.md"
  git -C "$TEST_TMP/repo" add README.md
  git -C "$TEST_TMP/repo" commit -m local >/dev/null
  advance_remote_main
  git -C "$TEST_TMP/repo" fetch origin main --quiet
  assert_success "audit reports divergent root" \
    "$AUDIT" --repo "$TEST_TMP/repo"
  grep -q "ROOT_MAIN: DIVERGENTE" "$TEST_TMP/output" \
    && pass "audit classifies divergent root" \
    || fail "audit classifies divergent root"
  grep -q "ROOT_AHEAD: 1" "$TEST_TMP/output" \
    && pass "audit counts divergent local commit" \
    || fail "audit counts divergent local commit"
  grep -q "ROOT_BEHIND: 1" "$TEST_TMP/output" \
    && pass "audit counts divergent remote commit" \
    || fail "audit counts divergent remote commit"
  cleanup_repo
}

test_audit_uses_root_and_disables_optional_locks() {
  setup_repo
  "$START" --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task audit-child --branch codex/audit-child --owner test --scope scripts >/dev/null
  echo dirty >"$TEST_TMP/worktrees/audit-child/dirty.txt"
  assert_success "audit derives root when invoked with a linked worktree" \
    "$AUDIT" --repo "$TEST_TMP/worktrees/audit-child"
  grep -q "ROOT_BRANCH: main" "$TEST_TMP/output" \
    && pass "audit reports actual root branch" \
    || fail "audit reports actual root branch"
  grep -q "ROOT_LIMPIO: SI" "$TEST_TMP/output" \
    && pass "audit ignores linked worktree dirtiness for root state" \
    || fail "audit ignores linked worktree dirtiness for root state"

  touch -t 203001010101 "$TEST_TMP/repo/README.md"
  trace="$TEST_TMP/audit-trace.json"
  assert_success "audit succeeds with trace enabled" \
    env GIT_TRACE2_EVENT="$trace" "$AUDIT" --repo "$TEST_TMP/repo"
  if grep -q "do_write_index" "$trace"; then
    fail "audit disables optional index writes"
  else
    pass "audit disables optional index writes"
  fi
  cleanup_repo
}

test_adopt_legacy_worktree() {
  setup_repo
  git -C "$TEST_TMP/repo" worktree add -b codex/legacy \
    "$TEST_TMP/worktrees/legacy" origin/main >/dev/null
  assert_success "adopt registers a legacy worktree" \
    "$ADOPT" --repo "$TEST_TMP/repo" --worktree "$TEST_TMP/worktrees/legacy" \
    --task legacy --owner test --scope scripts
  assert_success "adopted worktree passes preflight" \
    bash -c "cd '$TEST_TMP/worktrees/legacy' && '$PREFLIGHT' --write"
  assert_failure "adopt rejects base checkout" \
    "$ADOPT" --repo "$TEST_TMP/repo" --worktree "$TEST_TMP/repo" \
    --task base --owner test --scope scripts
  cleanup_repo
}

test_safe_close() {
  setup_repo
  "$START" --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task unique --branch codex/unique --owner test --scope scripts >/dev/null
  echo unique >"$TEST_TMP/worktrees/unique/unique.txt"
  git -C "$TEST_TMP/worktrees/unique" add unique.txt
  git -C "$TEST_TMP/worktrees/unique" commit -m unique >/dev/null
  assert_failure "close rejects commits absent from main" \
    "$CLOSE" --repo "$TEST_TMP/repo" --task unique --state merged
  [[ -d "$TEST_TMP/worktrees/unique" ]] \
    && pass "failed close preserves worktree" || fail "failed close preserves worktree"

  "$START" --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task merged --branch codex/merged --owner test --scope scripts >/dev/null
  git -C "$TEST_TMP/worktrees/merged" push -u origin codex/merged >/dev/null
  assert_success "close removes corroborated empty branch" \
    "$CLOSE" --repo "$TEST_TMP/repo" --task merged --state merged
  [[ ! -d "$TEST_TMP/worktrees/merged" ]] \
    && pass "close removes worktree" || fail "close removes worktree"
  [[ -f "$TEST_TMP/repo/.git/task-workspaces/closed/merged.json" ]] \
    && pass "close archives registry" || fail "close archives registry"
  if git -C "$TEST_TMP/repo" ls-remote --exit-code --heads origin codex/merged >/dev/null 2>&1; then
    fail "close removes merged remote branch"
  else
    pass "close removes merged remote branch"
  fi
  assert_success "close is idempotent" \
    "$CLOSE" --repo "$TEST_TMP/repo" --task merged --state merged

  "$START" --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task remote-advanced --branch codex/remote-advanced --owner test --scope scripts >/dev/null
  echo remote >"$TEST_TMP/worktrees/remote-advanced/remote.txt"
  git -C "$TEST_TMP/worktrees/remote-advanced" add remote.txt
  git -C "$TEST_TMP/worktrees/remote-advanced" commit -m remote >/dev/null
  git -C "$TEST_TMP/worktrees/remote-advanced" push -u origin codex/remote-advanced >/dev/null
  git -C "$TEST_TMP/worktrees/remote-advanced" reset --hard origin/main >/dev/null
  assert_failure "close rejects remote commits absent from main" \
    "$CLOSE" --repo "$TEST_TMP/repo" --task remote-advanced --state merged
  git -C "$TEST_TMP/repo" ls-remote --exit-code --heads origin codex/remote-advanced >/dev/null \
    && pass "failed close preserves remote branch" || fail "failed close preserves remote branch"

  "$START" --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task discarded --branch codex/discarded --owner test --scope scripts >/dev/null
  echo discard >"$TEST_TMP/worktrees/discarded/discard.txt"
  git -C "$TEST_TMP/worktrees/discarded" add discard.txt
  git -C "$TEST_TMP/worktrees/discarded" commit -m discard >/dev/null
  assert_success "discard creates recovery and removes worktree" \
    "$CLOSE" --repo "$TEST_TMP/repo" --task discarded --state discarded
  recovery_count="$(find "$TEST_TMP/repo/.git/task-workspaces/recovery" \
    -type f -name 'discarded-*.bundle' | wc -l | tr -d ' ')"
  [[ "$recovery_count" == "1" ]] \
    && pass "discard preserves git bundle" || fail "discard preserves git bundle"
  cleanup_repo
}

test_close_with_stale_base_checkout() {
  setup_repo
  "$START" --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task stale --branch codex/stale --owner test --scope scripts >/dev/null
  echo stale >"$TEST_TMP/worktrees/stale/stale.txt"
  git -C "$TEST_TMP/worktrees/stale" add stale.txt
  git -C "$TEST_TMP/worktrees/stale" commit -m stale >/dev/null
  git -C "$TEST_TMP/worktrees/stale" push -u origin codex/stale >/dev/null
  # El commit llega a main en el remoto (equivale al merge del PR) mientras el
  # checkout base se queda atrasado. Es el caso real: si la rama remota se borra
  # antes que la local, `branch -d` valida contra ese main viejo y falla.
  git -C "$TEST_TMP/worktrees/stale" push origin HEAD:main >/dev/null
  assert_success "close works when base checkout is behind origin/main" \
    "$CLOSE" --repo "$TEST_TMP/repo" --task stale --state merged
  [[ "$(git -C "$TEST_TMP/repo" rev-parse main)" == \
    "$(git -C "$TEST_TMP/repo" rev-parse origin/main)" ]] \
    && pass "close leaves root main synchronized" \
    || fail "close leaves root main synchronized"
  git -C "$TEST_TMP/repo" show-ref --verify --quiet refs/heads/codex/stale \
    && fail "close removes local branch when base is behind" \
    || pass "close removes local branch when base is behind"
  if git -C "$TEST_TMP/repo" ls-remote --exit-code --heads origin codex/stale >/dev/null 2>&1; then
    fail "close removes remote branch when base is behind"
  else
    pass "close removes remote branch when base is behind"
  fi
  cleanup_repo
}

test_close_resumes_after_missing_worktree() {
  setup_repo
  "$START" --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task resumed --branch codex/resumed --owner test --scope scripts >/dev/null
  echo resumed >"$TEST_TMP/worktrees/resumed/resumed.txt"
  git -C "$TEST_TMP/worktrees/resumed" add resumed.txt
  git -C "$TEST_TMP/worktrees/resumed" commit -m resumed >/dev/null
  git -C "$TEST_TMP/worktrees/resumed" push -u origin codex/resumed >/dev/null
  git -C "$TEST_TMP/worktrees/resumed" push origin HEAD:main >/dev/null
  # Simula un cierre interrumpido justo despues de remover el worktree.
  git -C "$TEST_TMP/repo" worktree remove "$TEST_TMP/worktrees/resumed"
  assert_success "close resumes when worktree was already removed" \
    "$CLOSE" --repo "$TEST_TMP/repo" --task resumed --state merged
  [[ -f "$TEST_TMP/repo/.git/task-workspaces/closed/resumed.json" ]] \
    && pass "resumed close archives registry" || fail "resumed close archives registry"

  "$START" --repo "$TEST_TMP/repo" --root "$TEST_TMP/worktrees" \
    --task ghost --branch codex/ghost --owner test --scope scripts >/dev/null
  # Borrar la carpeta a mano deja a Git listando un worktree inexistente: eso si
  # es un estado inconsistente y debe frenar el cierre.
  rm -rf "$TEST_TMP/worktrees/ghost"
  assert_failure "close rejects worktree missing from disk but tracked by git" \
    "$CLOSE" --repo "$TEST_TMP/repo" --task ghost --state merged
  cleanup_repo
}

test_start_and_preflight
test_root_sync_on_start
test_start_reexecutes_after_self_update
test_close_reexecutes_after_self_update
test_start_uses_pinned_sync_sha
test_root_divergence_blocks_start
test_preflight_guards
test_handoff_and_audit
test_audit_reports_root_state
test_audit_uses_root_and_disables_optional_locks
test_adopt_legacy_worktree
test_safe_close
test_close_with_stale_base_checkout
test_close_resumes_after_missing_worktree

echo "RESULT: passed=$passed failed=$failed"
(( failed == 0 ))
