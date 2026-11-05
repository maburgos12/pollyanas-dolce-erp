"""Factory contracts against disposable Git repositories, never the ERP database."""
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "task_factory.py"


class FactoryTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(SCRIPT.exists(), "factory coordinator must exist")
        spec = importlib.util.spec_from_file_location("task_factory", SCRIPT)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.git("init", "-b", "main")
        self.git("config", "user.email", "test@example.com")
        self.git("config", "user.name", "Test")
        (self.repo / "source.txt").write_text("original\n")
        self.git("add", ".")
        self.git("commit", "-m", "base")
        self.git("checkout", "-b", "codex/example")
        self.sha = self.git("rev-parse", "HEAD")
        self.registry = self.repo / ".git/task-workspaces"
        (self.registry / "active").mkdir(parents=True)
        (self.registry / "locks").mkdir()
        self.record = self.registry / "active/example.json"
        self.record.write_text(json.dumps(dict(task_id="example", owner="test",
            branch="codex/example", worktree=str(self.repo), base_commit=self.sha,
            scope="source.txt", status="active")))
        self.factory = self.module.Factory(self.repo, "example")
        self.factory.initialize("Keep source behavior", "User approved pilot", "tooling")

    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.repo), *args], text=True,
                                       stderr=subprocess.DEVNULL).strip()

    def run_stage(self, stage, code="print('verified')", timeout=5):
        return self.factory.run(stage, [sys.executable, "-c", code], timeout,
                                "Observed acceptance criterion")

    def pr(self, **changes):
        return dict(headRefOid=self.sha, baseRefName="main", state="OPEN", mergeCommit=None,
                    url="https://github.com/test/repo/pull/1", statusCheckRollup=[
                        dict(name="test", status="COMPLETED", conclusion="SUCCESS")]) | changes

    def through_ci(self):
        self.run_stage("local")
        self.run_stage("review")
        with patch.object(self.factory, "github_pr", return_value=self.pr()):
            self.factory.check_ci("1")

    def through_integration(self):
        self.through_ci()
        self.git("update-ref", "refs/remotes/origin/main", self.sha)
        with patch.object(self.factory, "github_pr", return_value=self.pr(
                state="MERGED", mergeCommit={"oid": self.sha})), \
                patch.object(self.factory, "fetch_main"):
            self.factory.integration()

    def test_status_is_read_only_and_reports_next_step(self):
        before = self.factory.path.read_bytes()
        self.assertEqual(self.factory.status()["next_step"], "local")
        self.assertEqual(before, self.factory.path.read_bytes())

    def test_reinitialize_cannot_downgrade_delivery_or_authorization(self):
        with self.assertRaises(ValueError):
            self.factory.initialize("Changed", "Changed", "production")

    def test_unknown_and_traversal_tasks_are_rejected(self):
        for task in ("../example", "missing"):
            with self.subTest(task=task), self.assertRaises(ValueError):
                self.module.Factory(self.repo, task)

    def test_success_is_logged_and_resume_selects_review(self):
        self.assertTrue(self.run_stage("local"))
        state = self.factory.status()
        self.assertEqual(state["next_step"], "review")
        event = state["events"][-1]
        self.assertEqual(event["sha"], self.sha)
        self.assertIn("verified", Path(event["log"]).read_text())

    def test_failure_blocks_later_steps_and_limits_retries(self):
        for _ in range(3):
            self.assertFalse(self.run_stage("local", "raise SystemExit(4)"))
        with self.assertRaisesRegex(ValueError, "limit"):
            self.run_stage("local")
        with self.assertRaises(ValueError):
            self.run_stage("review")
        self.assertEqual(self.factory.status()["next_step"], "local")

    def test_timeout_records_failure(self):
        self.assertFalse(self.run_stage("local", "import time; time.sleep(5)", timeout=1))
        self.assertEqual(self.factory.status()["events"][-1]["exit_code"], 124)

    def test_dirty_tree_and_changed_revision_invalidate_evidence(self):
        self.run_stage("local")
        (self.repo / "source.txt").write_text("changed\n")
        with self.assertRaises(ValueError):
            self.run_stage("review")
        self.git("add", ".")
        self.git("commit", "-m", "change")
        self.assertEqual(self.factory.status()["next_step"], "local")
        with self.assertRaises(ValueError):
            self.run_stage("review")

    def test_command_modifying_source_does_not_pass(self):
        self.assertFalse(self.run_stage("local",
            "from pathlib import Path; Path('source.txt').write_text('changed')"))

    def test_ci_rejects_stale_failed_missing_or_pending_checks(self):
        self.run_stage("local")
        self.run_stage("review")
        for changes in (
            {"headRefOid": "0" * 40}, {"baseRefName": "other"},
            {"statusCheckRollup": []},
            {"statusCheckRollup": [dict(name="test", status="IN_PROGRESS", conclusion=None)]},
            {"statusCheckRollup": [dict(name="test", status="COMPLETED", conclusion="FAILURE")]},
        ):
            with self.subTest(changes=changes), \
                    patch.object(self.factory, "github_pr", return_value=self.pr(**changes)), \
                    self.assertRaises(ValueError):
                self.factory.check_ci("1")
        self.assertEqual(self.factory.status()["next_step"], "ci")

    def test_integration_requires_actual_merged_pr(self):
        self.through_ci()
        with patch.object(self.factory, "github_pr", return_value=self.pr()), \
                self.assertRaises(ValueError):
            self.factory.integration()

    def test_tooling_requires_verification_and_cleanup_before_close(self):
        self.through_integration()
        with self.assertRaises(ValueError):
            self.factory.check_close(refresh=False)
        self.run_stage("verify")
        with self.assertRaises(ValueError):
            self.factory.check_close(refresh=False)
        self.run_stage("cleanup")
        self.factory.check_close(refresh=False)

    def test_production_cannot_skip_deploy_and_runtime_revision(self):
        self.factory.path.unlink()
        self.factory.initialize("Keep behavior", "User approved production", "production")
        self.through_integration()
        with self.assertRaises(ValueError):
            self.run_stage("verify")
        with self.assertRaisesRegex(ValueError, "revision"):
            self.run_stage("deploy")
        self.assertTrue(self.factory.run("deploy", [sys.executable, "-c", "print('deployed')"],
            5, "Official deploy", revision=self.sha))
        self.assertFalse(self.factory.run("verify", [sys.executable, "-c", "print('old version')"],
            5, "Runtime SHA and acceptance", revision=self.sha))
        self.assertTrue(self.factory.run("verify", [sys.executable, "-c", f"print('{self.sha}')"],
            5, "Runtime SHA and acceptance", revision=self.sha))

    def test_global_lock_prevents_concurrent_mutation(self):
        lock = self.registry / "locks/global.lock"
        lock.mkdir()
        with self.assertRaises(ValueError):
            self.run_stage("local")

    def test_close_refreshes_github_evidence(self):
        self.through_integration()
        self.run_stage("verify")
        self.run_stage("cleanup")
        with patch.object(self.factory, "github_pr", return_value=self.pr(headRefOid="0" * 40)), \
                self.assertRaises(ValueError):
            self.factory.check_close()

    def test_enrolled_task_cannot_bypass_gate_using_legacy_close(self):
        origin = self.root / "origin.git"
        subprocess.run(["git", "init", "--bare", str(origin)], check=True, capture_output=True)
        self.git("remote", "add", "origin", str(origin))
        self.git("push", "origin", "main")
        self.git("checkout", "main")
        worktree = self.root / "task"
        self.git("worktree", "add", str(worktree), "codex/example")
        record = json.loads(self.record.read_text())
        record["worktree"] = str(worktree)
        self.record.write_text(json.dumps(record))
        result = subprocess.run(["bash", str(SCRIPT.with_name("task_workspace_close.sh")),
            "--repo", str(self.repo), "--task", "example", "--state", "merged"],
            capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing current evidence", result.stderr)
        self.assertTrue(worktree.exists())
        self.assertTrue(self.record.exists())

    def test_removed_or_tampered_log_invalidates_evidence(self):
        self.run_stage("local")
        event = self.factory.status()["events"][-1]
        Path(event["log"]).write_text("replaced")
        self.assertEqual(self.factory.status()["next_step"], "local")
        Path(event["log"]).unlink()
        self.assertEqual(self.factory.status()["next_step"], "local")

    def test_close_can_resume_after_worktree_removal(self):
        self.through_integration()
        self.run_stage("verify")
        self.run_stage("cleanup")
        self.factory.worktree = self.root / "already-removed"
        self.factory.check_close(refresh=False)


if __name__ == "__main__":
    unittest.main()
