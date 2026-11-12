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

    def test_failed_ci_refresh_invalidates_previous_success(self):
        self.through_ci()
        with patch.object(self.factory, "github_pr", return_value=self.pr(statusCheckRollup=[])), \
                self.assertRaises(ValueError):
            self.factory.check_ci("1")
        self.assertEqual(self.factory.status()["next_step"], "ci")
        self.assertEqual(self.factory.status()["events"][-1]["result"], "failed")

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

    def test_production_execution_cannot_overlap_another_task(self):
        self.factory.path.unlink()
        self.factory.initialize("Keep behavior", "Approved production", "production")
        self.through_integration()
        environment_lock = self.registry / "locks/factory-production.lock"
        environment_lock.mkdir()
        with self.assertRaisesRegex(ValueError, "production"):
            self.factory.run("deploy", [sys.executable, "-c", "print('deploy')"],
                             5, "Official deploy", revision=self.sha)
        self.assertTrue(environment_lock.exists())
        self.assertFalse(self.factory.lock_path.exists())

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

    def test_closed_task_has_no_next_step(self):
        self.through_integration()
        self.run_stage("verify")
        self.run_stage("cleanup")
        closed = self.registry / "closed"
        closed.mkdir()
        self.record.rename(closed / self.record.name)
        state = self.module.Factory(self.repo, "example").status()
        self.assertEqual(state["lifecycle"], "closed")
        self.assertIsNone(state["next_step"])

    def prepare_successor(self):
        self.through_integration()
        self.assertFalse(self.run_stage("verify", "raise SystemExit(9)"))
        record = json.loads(self.record.read_text()) | {"task_id": "successor"}
        successor_record = self.registry / "active/successor.json"
        successor_record.write_text(json.dumps(record))
        successor = self.module.Factory(self.repo, "successor")
        successor.initialize("Includes original acceptance", "Approved successor", "tooling")
        data = successor.read()
        data["events"] = self.factory.read()["events"][:-1]
        for event in data["events"]:
            if "pr" in event:
                event["pr"] = "https://github.com/test/repo/pull/2"
        successor.save(data)
        for stage in ("verify", "cleanup"):
            self.assertTrue(successor.run(stage, [sys.executable, "-c", "print('observed')"],
                                         5, "Successor acceptance and resource handoff"))
        closed = self.registry / "closed"
        closed.mkdir()
        record["status"] = "merged"
        successor_record.write_text(json.dumps(record))
        successor_record.rename(closed / successor_record.name)
        self.successor = self.module.Factory(self.repo, "successor")
        self.proof = self.root / "coverage.json"
        self.proof.write_text(json.dumps({"acceptance": "Keep source behavior",
            "coverage": "Verified original behavior in successor log; reviewed by operator",
            "resources": {"owner": "test", "review_date": "2026-11-13",
                          "inventory": ["original volume preserved, backup verified"]}}))

    def record_supersession(self, **changes):
        args = dict(successor_task="successor", authorization="Explicit user approval",
                    evidence=self.proof) | changes
        with patch.object(self.module.Factory, "github_pr", side_effect=lambda pr: self.pr(
                state="MERGED", mergeCommit={"oid": self.sha}, url=pr)), \
                patch.object(self.module.Factory, "fetch_main"):
            self.factory.supersede(**args)

    def test_supersession_preserves_failure_and_seals_original_execution(self):
        self.prepare_successor()
        before = self.factory.read()["events"]
        self.record_supersession()
        self.assertEqual(before, self.factory.read()["events"])
        self.assertEqual(self.factory.read()["events"][-1]["result"], "failed")
        self.assertEqual(self.factory.status()["next_step"], "close")
        self.assertNotIn("verify", self.factory.status()["completed"])
        self.factory.check_close(refresh=False)
        with self.assertRaisesRegex(ValueError, "sealed"):
            self.run_stage("verify")
        with self.assertRaisesRegex(ValueError, "overwritten"):
            self.record_supersession()

    def test_supersession_requires_authorization_and_acceptance_coverage(self):
        self.prepare_successor()
        with self.assertRaises(ValueError):
            self.record_supersession(authorization=" ")
        for change in ({"acceptance": "different"}, {"coverage": ""}, {"resources": {}}):
            original = self.proof.read_text()
            self.proof.write_text(json.dumps(json.loads(original) | change))
            with self.assertRaises(ValueError):
                self.record_supersession()
            self.proof.write_text(original)
        with self.assertRaises(ValueError):
            self.record_supersession(successor_task="example")

    def test_supersession_rejects_unclosed_locked_or_chained_successor(self):
        self.prepare_successor()
        closed = self.successor.record_path
        active = self.registry / "active/successor.json"
        closed.rename(active)
        with self.assertRaisesRegex(ValueError, "closed"):
            self.record_supersession()
        active.rename(closed)
        self.successor.lock_path.mkdir()
        with self.assertRaisesRegex(ValueError, "unlocked"):
            self.record_supersession()
        self.successor.lock_path.rmdir()
        data = self.successor.read()
        self.successor.save(data | {"supersession": {"successor": "example"}})
        with self.assertRaisesRegex(ValueError, "chains"):
            self.record_supersession()

    def test_supersession_rejects_incomplete_or_changed_successor_evidence(self):
        self.prepare_successor()
        data = self.successor.read()
        log = Path(data["events"][-1]["log"])
        original = log.read_bytes()
        log.write_text("tampered")
        with self.assertRaisesRegex(ValueError, "complete"):
            self.record_supersession()
        log.write_bytes(original)
        self.record_supersession()
        log.write_text("tampered after registration")
        with self.assertRaises(ValueError):
            self.factory.check_close(refresh=False)
        log.write_bytes(original)
        self.successor.save(data | {"acceptance": "changed"})
        with self.assertRaisesRegex(ValueError, "successor evidence changed"):
            self.factory.check_close(refresh=False)

    def test_supersession_revalidates_coverage_revision_and_main_ancestry(self):
        self.prepare_successor()
        self.record_supersession()
        entry = self.factory.read()["supersession"]
        saved = Path(entry["evidence"])
        original = saved.read_bytes()
        saved.write_text("tampered")
        with self.assertRaisesRegex(ValueError, "evidence changed"):
            self.factory.check_close(refresh=False)
        saved.write_bytes(original)
        self.git("update-ref", "-d", "refs/remotes/origin/main")
        with self.assertRaises(subprocess.CalledProcessError):
            self.factory.check_close(refresh=False)
        self.git("update-ref", "refs/remotes/origin/main", self.sha)
        (self.repo / "source.txt").write_text("changed")
        with self.assertRaises(ValueError):
            self.factory.check_close(refresh=False)

    def test_supersession_cannot_hide_stale_original_integration_or_changed_pr(self):
        self.prepare_successor()
        original = self.factory.read()
        changed = self.factory.read()
        changed["events"][0]["result"] = "failed"
        self.factory.save(changed)
        with self.assertRaisesRegex(ValueError, "original integration"):
            self.record_supersession()
        self.factory.save(original)
        self.record_supersession()
        with patch.object(self.module.Factory, "fetch_main"), \
                patch.object(self.module.Factory, "github_pr", return_value=self.pr(state="OPEN")), \
                self.assertRaisesRegex(ValueError, "merge evidence changed"):
            self.factory.check_close()

    def test_supersession_rejects_delivery_downgrade_and_revision_change(self):
        self.prepare_successor()
        data = self.factory.read()
        self.factory.save(data | {"delivery": "production"})
        with self.assertRaisesRegex(ValueError, "downgrades"):
            self.record_supersession()
        self.factory.save(data)
        self.record_supersession()
        (self.repo / "source.txt").write_text("new revision")
        self.git("add", ".")
        self.git("commit", "-m", "new revision")
        with self.assertRaisesRegex(ValueError, "revision"):
            self.factory.check_close(refresh=False)

    def test_supersession_passes_official_close_without_rewriting_stage_history(self):
        self.prepare_successor()
        self.record_supersession()
        origin = self.root / "origin.git"
        subprocess.run(["git", "init", "--bare", str(origin)], check=True, capture_output=True)
        self.git("config", f"url.{origin}.insteadOf", "https://github.com/test/repo.git")
        self.git("remote", "add", "origin", "https://github.com/test/repo.git")
        self.git("push", "origin", "main")
        self.git("checkout", "main")
        wt = self.root / "worktree"
        self.git("worktree", "add", str(wt), "codex/example")
        record = json.loads(self.record.read_text()) | {"worktree": str(wt)}
        self.record.write_text(json.dumps(record))
        # GitHub identity/API are simulated; Git transport uses the disposable bare repo.
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        gh = bin_dir / "gh"
        gh.write_text(f"#!{sys.executable}\nimport json,sys\np={self.pr(state='MERGED', mergeCommit={'oid': self.sha})!r}\np['url']=sys.argv[3]\nprint(json.dumps(p))\n")
        gh.chmod(0o700)
        import os
        import shutil
        git = bin_dir / "git"
        git.write_text(f"#!{sys.executable}\nimport os,sys\nif sys.argv[1:]==['remote','get-url','origin']:\n print('https://github.com/test/repo.git')\nelse:\n os.execv({shutil.which('git')!r}, ['git', *sys.argv[1:]])\n")
        git.chmod(0o700)
        result = subprocess.run(["bash", str(SCRIPT.with_name("task_workspace_close.sh")),
            "--repo", str(self.repo), "--task", "example", "--state", "merged"],
            env=os.environ | {"PATH": str(bin_dir) + os.pathsep + os.environ["PATH"]},
            capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(wt.exists())
        self.assertTrue((self.registry / "closed/example.json").exists())
        self.assertEqual(self.factory.read()["events"][-1]["result"], "failed")


if __name__ == "__main__":
    unittest.main()
