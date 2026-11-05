#!/usr/bin/env python3
"""Evidence and resumable delivery for registered workspaces; no ERP DB or LLM calls."""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import uuid


def now():
    return datetime.now(timezone.utc).isoformat()


def command(args, cwd):
    return subprocess.check_output(args, cwd=cwd, text=True, stderr=subprocess.PIPE,
                                   timeout=60).strip()


class Factory:
    def __init__(self, repo, task):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", task):
            raise ValueError("invalid task id")
        self.repo = Path(repo).resolve()
        common = Path(command(["git", "rev-parse", "--git-common-dir"], self.repo))
        self.registry = (self.repo / common).resolve() / "task-workspaces"
        records = [self.registry / state / f"{task}.json" for state in
                   ("active", "delivered", "blocked", "closed", "discarded")]
        records = [p for p in records if p.is_file()]
        if len(records) != 1:
            raise ValueError("task must have exactly one lifecycle record")
        self.record_path = records[0]
        self.record = json.loads(self.record_path.read_text())
        self.worktree = Path(self.record["worktree"])
        self.task = task
        self.path = self.registry / "factory" / f"{task}.json"
        self.lock_path = self.registry / "locks" / f"factory-{task}.lock"

    @contextmanager
    def lock(self, production=False):
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        environment_lock = self.registry / "locks/factory-production.lock"
        environment_owned = False
        try:
            self.lock_path.mkdir()
        except FileExistsError:
            raise ValueError("task execution locked; inspect owner before recovering") from None
        try:
            (self.lock_path / "pid").write_text(str(os.getpid()))
            if (self.registry / "locks/global.lock").exists():
                raise ValueError("lifecycle operation in progress")
            if not self.record_path.exists() or self.record_path.parent.name != "active":
                raise ValueError("only active tasks can advance; resume lifecycle ownership first")
            if production:
                try:
                    environment_lock.mkdir()
                except FileExistsError:
                    raise ValueError("production execution locked by another task") from None
                environment_owned = True
                (environment_lock / "owner").write_text(f"{self.task} pid={os.getpid()}")
            yield
        finally:
            if environment_owned:
                (environment_lock / "owner").unlink(missing_ok=True)
                environment_lock.rmdir()
            (self.lock_path / "pid").unlink(missing_ok=True)
            self.lock_path.rmdir()

    def read(self):
        return json.loads(self.path.read_text())

    def save(self, data):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(f".{os.getpid()}.tmp")
        with temporary.open("w") as stream:
            os.chmod(temporary, 0o600)
            json.dump(data, stream, indent=2, ensure_ascii=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(self.path)

    def git(self, *args):
        return command(["git", *args], self.worktree)

    def head(self, clean=False):
        if not self.worktree.exists():
            return command(["git", "rev-parse", "refs/heads/" + self.record["branch"]], self.repo)
        if self.git("branch", "--show-current") != self.record["branch"]:
            raise ValueError("workspace branch differs from registered owner")
        if clean and self.git("status", "--porcelain"):
            raise ValueError("workspace must be clean to record evidence")
        return self.git("rev-parse", "HEAD")

    def initialize(self, acceptance, authorization, delivery):
        with self.lock():
            if self.path.exists():
                raise ValueError("already enrolled; acceptance and authorization cannot be overwritten")
            if not acceptance.strip() or not authorization.strip():
                raise ValueError("acceptance and authorization are required")
            if delivery not in ("tooling", "production"):
                raise ValueError("invalid delivery")
            self.save(dict(schema=1, task=self.task, acceptance=acceptance,
                           authorization=authorization, delivery=delivery,
                           created_at=now(), events=[]))

    def stages(self):
        return ["local", "review", "ci", "integration"] + (
            ["deploy"] if self.read()["delivery"] == "production" else []) + ["verify", "cleanup"]

    def valid_events(self, sha):
        stages = self.stages()
        valid = {}
        for event in self.read()["events"]:
            if event["sha"] != sha:
                continue
            index = stages.index(event["stage"])
            for stage in stages[index:]:
                valid.pop(stage, None)
            log_valid = True
            if event.get("log"):
                path = Path(event["log"])
                log_valid = path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == event.get("log_sha256")
            if event["result"] == "passed" and log_valid and all(s in valid for s in stages[:index]):
                valid[event["stage"]] = event
        return valid

    def status(self):
        data = self.read()
        terminal = self.record_path.parent.name in ("closed", "discarded")
        sha = (data["events"][-1]["sha"] if data["events"] else None) if terminal else self.head()
        valid = self.valid_events(sha)
        dirty = False if terminal else bool(self.git("status", "--porcelain"))
        next_step = next((s for s in self.stages() if s not in valid), "close")
        return data | dict(lifecycle=self.record_path.parent.name, sha=sha, dirty=dirty,
                           next_step="local" if dirty else next_step,
                           executing=self.lock_path.exists(), completed=list(valid))

    def require(self, stage, sha):
        stages = self.stages()
        if stage not in stages:
            raise ValueError("stage does not apply to this delivery")
        valid = self.valid_events(sha)
        missing = [s for s in stages[:stages.index(stage)] if s not in valid]
        if missing:
            raise ValueError("missing current evidence: " + ", ".join(missing))
        return valid

    def append(self, event):
        data = self.read()
        data["events"].append(event)
        self.save(data)

    def run(self, stage, args, timeout, note, revision=None):
        if stage not in ("local", "review", "deploy", "verify", "cleanup"):
            raise ValueError("CI and integration must be observed through GitHub")
        if not args or not note.strip() or not 1 <= timeout <= 3600:
            raise ValueError("command, observation note and timeout 1..3600 are required")
        with self.lock(production=stage in ("deploy", "verify") and self.read()["delivery"] == "production"):
            sha = self.head(clean=True)
            valid = self.require(stage, sha)
            production = self.read()["delivery"] == "production"
            if production and stage in ("deploy", "verify"):
                if revision != valid["integration"]["release_sha"]:
                    raise ValueError("release revision must match observed merge commit")
            attempts = sum(e["sha"] == sha and e["stage"] == stage and e["result"] != "passed"
                           for e in self.read()["events"])
            if attempts >= 3:
                raise ValueError("retry limit reached; diagnose before a new revision")
            logs = self.path.parent / self.task
            logs.mkdir(exist_ok=True)
            log = logs / f"{stage}-{uuid.uuid4().hex}.log"
            event = dict(stage=stage, sha=sha, result="running", started_at=now(),
                         command=args, note=note, log=str(log), release_sha=revision)
            self.append(event)
            started = time.monotonic()
            exit_code = 127
            with log.open("w") as output:
                os.chmod(log, 0o600)
                try:
                    process = subprocess.Popen(args, cwd=self.worktree, stdout=output,
                                               stderr=subprocess.STDOUT, start_new_session=True)
                    try:
                        exit_code = process.wait(timeout=timeout)
                    except (subprocess.TimeoutExpired, KeyboardInterrupt):
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
                        exit_code = 124
                except OSError as error:
                    output.write(str(error))
            try:
                unchanged = self.head(clean=True) == sha
            except ValueError:
                unchanged = False
            runtime_ok = not (production and stage == "verify") or revision in log.read_text(errors="replace")
            passed = exit_code == 0 and unchanged and runtime_ok
            data = self.read()
            data["events"][-1].update(result="passed" if passed else "failed", exit_code=exit_code,
                finished_at=now(), duration_seconds=round(time.monotonic() - started, 3),
                log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),
                unchanged=unchanged, runtime_revision_observed=runtime_ok if production and stage == "verify" else None)
            self.save(data)
            return passed

    def github_pr(self, pr):
        origin = command(["git", "remote", "get-url", "origin"], self.repo)
        match = re.fullmatch(r"(?:https://github.com/|git@github.com:)([^/]+/[^/]+?)(?:\.git)?", origin)
        if not match:
            raise ValueError("origin must identify the GitHub repository")
        observed = json.loads(command(["gh", "pr", "view", str(pr), "--repo", match[1], "--json",
            "headRefOid,baseRefName,state,mergeCommit,url,statusCheckRollup"], self.repo))
        if not observed["url"].startswith(f"https://github.com/{match[1]}/pull/"):
            raise ValueError("PR belongs to a different repository")
        return observed

    @staticmethod
    def validate_ci(pr, sha):
        if pr["headRefOid"] != sha or pr["baseRefName"] != "main":
            raise ValueError("PR revision or base differs from task")
        checks = pr["statusCheckRollup"] or []
        required = [c for c in checks if c.get("name") == "test"]
        if len(required) != 1 or required[0].get("conclusion") != "SUCCESS":
            raise ValueError("required test check must succeed on current SHA")
        for check in checks:
            if "conclusion" in check:
                ok = check.get("status") == "COMPLETED" and check["conclusion"] in ("SUCCESS", "SKIPPED", "NEUTRAL")
            else:
                ok = check.get("state") == "SUCCESS"
            if not ok:
                raise ValueError("CI contains failed or pending checks")

    def check_ci(self, pr):
        with self.lock():
            sha = self.head(clean=True)
            self.require("ci", sha)
            observed = self.github_pr(pr)
            self.validate_ci(observed, sha)
            self.append(dict(stage="ci", sha=sha, result="passed", finished_at=now(),
                             pr=observed["url"], checks=observed["statusCheckRollup"]))

    def fetch_main(self):
        command(["git", "fetch", "origin", "main", "--quiet"], self.repo)

    def integration(self):
        with self.lock():
            sha = self.head(clean=True)
            valid = self.require("integration", sha)
            observed = self.github_pr(valid["ci"]["pr"])
            self.validate_ci(observed, sha)
            if observed["state"] != "MERGED" or not observed["mergeCommit"]:
                raise ValueError("PR has not been merged")
            release = observed["mergeCommit"]["oid"]
            self.fetch_main()
            command(["git", "merge-base", "--is-ancestor", release, "origin/main"], self.repo)
            self.append(dict(stage="integration", sha=sha, result="passed", finished_at=now(),
                             release_sha=release, pr=observed["url"]))

    def check_close(self, refresh=True):
        if self.lock_path.exists():
            raise ValueError("task execution is still locked")
        sha = self.head(clean=True)
        valid = self.require("cleanup", sha)
        if "cleanup" not in self.valid_events(sha):
            raise ValueError("cleanup evidence required")
        if refresh:
            observed = self.github_pr(valid["ci"]["pr"])
            self.validate_ci(observed, sha)
            if observed["state"] != "MERGED" or not observed["mergeCommit"] or \
                    observed["mergeCommit"]["oid"] != valid["integration"]["release_sha"]:
                raise ValueError("merge evidence changed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--task", required=True)
    sub = parser.add_subparsers(dest="action", required=True)
    init = sub.add_parser("init")
    init.add_argument("--acceptance", required=True)
    init.add_argument("--authorization", required=True)
    init.add_argument("--delivery", choices=("tooling", "production"), required=True)
    sub.add_parser("status")
    sub.add_parser("resume", help="Read-only: show current evidence and next permitted step")
    run = sub.add_parser("run")
    run.add_argument("--stage", choices=("local", "review", "deploy", "verify", "cleanup"), required=True)
    run.add_argument("--timeout", type=int, default=600)
    run.add_argument("--note", required=True)
    run.add_argument("--revision")
    run.add_argument("command", nargs=argparse.REMAINDER)
    ci = sub.add_parser("ci")
    ci.add_argument("--pr", required=True)
    sub.add_parser("integration")
    sub.add_parser("check-close")
    args = parser.parse_args()
    try:
        factory = Factory(args.repo, args.task)
        if args.action == "init":
            factory.initialize(args.acceptance, args.authorization, args.delivery)
        elif args.action == "run":
            argv = args.command[1:] if args.command[:1] == ["--"] else args.command
            if not factory.run(args.stage, argv, args.timeout, args.note, args.revision):
                print(json.dumps(factory.status(), ensure_ascii=False, indent=2))
                return 1
        elif args.action == "ci":
            factory.check_ci(args.pr)
        elif args.action == "integration":
            factory.integration()
        elif args.action == "check-close":
            factory.check_close()
            return 0
        print(json.dumps(factory.status(), ensure_ascii=False, indent=2))
        return 0
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
