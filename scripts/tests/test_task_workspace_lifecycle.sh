#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
START="$PROJECT_ROOT/scripts/task_workspace_start.sh"
PREFLIGHT="$PROJECT_ROOT/scripts/git_workspace_preflight.sh"
HANDOFF="$PROJECT_ROOT/scripts/task_workspace_handoff.sh"
AUDIT="$PROJECT_ROOT/scripts/task_workspace_audit.sh"
CLOSE="$PROJECT_ROOT/scripts/task_workspace_close.sh"
ADOPT="$PROJECT_ROOT/scripts/task_workspace_adopt.sh"

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

test_start_and_preflight
test_preflight_guards
test_handoff_and_audit
test_adopt_legacy_worktree
test_safe_close

echo "RESULT: passed=$passed failed=$failed"
(( failed == 0 ))
