"""Repeatable monthly controller; NAS receipt and authenticated HTTP gate retirement."""

import fcntl
import hashlib
from datetime import date
from importlib import import_module
from io import StringIO
import json
import os
from pathlib import Path
import re
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from core.management.commands.archive_bitacora_media import (
    MAX_BYTES, atomic_json, digest_file, local_file, verify_archive_mount,
)
from core.media_archive import _open_read, load_archive_entry, open_archive
from core.private_operational_media import _can_access_archived_bitacora_media


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def private_directory(value):
    path = Path(os.path.abspath(value))
    media = Path(settings.MEDIA_ROOT).resolve()
    if path == media or path.is_relative_to(media) or media.is_relative_to(path):
        raise CommandError("Use directorios privados separados de MEDIA_ROOT.")
    for configured in (getattr(settings, "MEDIA_ARCHIVE_ROOT", ""), getattr(settings, "MEDIA_ARCHIVE_INDEX_ROOT", "")):
        if configured:
            reserved = Path(os.path.abspath(configured))
            if path == reserved or path.is_relative_to(reserved) or reserved.is_relative_to(path):
                raise CommandError("Plan y staging deben estar separados del NAS y su índice.")
    if any(item.is_symlink() for item in (path, *path.parents)):
        raise CommandError("Los directorios privados no admiten enlaces simbólicos.")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.stat().st_mode & 0o077:
        raise CommandError("Los directorios privados deben tener permisos 0700.")
    return path


def matching_stat(path, entry):
    with _open_read(path) as source:
        metadata = os.fstat(source.fileno())
        if metadata.st_size != entry["bytes"]:
            return None
        digest = hashlib.sha256()
        remaining = entry["bytes"] + 1
        while remaining:
            chunk = source.read(min(remaining, 64 * 1024))
            if not chunk:
                break
            remaining -= len(chunk)
            digest.update(chunk)
        if remaining != 1 or digest.hexdigest() != entry["sha256"]:
            return None
        return metadata


class Command(BaseCommand):
    help = "Sincroniza un mes cerrado; espera NAS y solo retira tras validar todas las fotos por HTTP."

    def add_arguments(self, parser):
        parser.add_argument("--month", required=True)
        parser.add_argument("--plan-dir", required=True)
        parser.add_argument("--stage-root", required=True)
        parser.add_argument("--verification-user", default="admin")
        parser.add_argument("--verification-origin", required=True)
        parser.add_argument("--verification-host", default="erp.pollyanasdolce.com")

    def handle(self, *args, **options):
        state_path = None
        plan_sha = None
        state_trusted = False
        lock = None
        try:
            origin = urlsplit(options["verification_origin"])
            if (origin.scheme != "http" or origin.hostname not in {"localhost", "127.0.0.1"}
                    or origin.port is None or origin.port < 1 or origin.username or origin.password
                    or origin.path not in {"", "/"} or origin.query or origin.fragment
                    or options["verification_host"] != "erp.pollyanasdolce.com"):
                raise CommandError("La verificación exige HTTP loopback con puerto y Host del ERP.")
            month = options["month"]
            if date.fromisoformat(month + "-01").strftime("%Y-%m") != month:
                raise CommandError("Mes inválido; use YYYY-MM.")
            plan_dir = private_directory(options["plan_dir"])
            stage = private_directory(options["stage_root"])
            if plan_dir == stage or plan_dir.is_relative_to(stage) or stage.is_relative_to(plan_dir):
                raise CommandError("Plan y staging deben estar separados.")
            # Stable inode: do not remove this file when releasing the lock.
            lock_fd = os.open(plan_dir / "sync.lock", os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            lock = os.fdopen(lock_fd, "w")
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                self.result("WAIT_LOCK")
                return
            plan_path = plan_dir / f"{month}.plan.json"
            state_path = plan_dir / f"{month}.state.json"
            if plan_path.is_symlink() or state_path.is_symlink():
                raise CommandError("Plan o estado inseguro.")
            if not plan_path.exists():
                self.archive_command(options, "plan", plan_path)
            directory_fd = os.open(plan_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            plan_sha = digest_file(plan_path)
            plan = json.loads(plan_path.read_text())
            if plan.get("version") != 1 or plan.get("month") != month or not isinstance(plan.get("entries"), list):
                raise CommandError("Plan incompatible.")
            entries = plan["entries"]
            if len({entry["name"] for entry in entries}) != len(entries):
                raise CommandError("Plan con nombres duplicados.")
            for entry in entries:
                local_file(entry["name"])
                if (type(entry["bytes"]) is not int or not 0 < entry["bytes"] <= MAX_BYTES
                        or not isinstance(entry["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", entry["sha256"])):
                    raise CommandError("Tamaño inválido en el plan.")
            previous = json.loads(state_path.read_text()) if state_path.exists() else {}
            if previous and previous.get("plan_sha256") != plan_sha:
                raise CommandError("Cambió la identidad del plan; conserve los originales.")
            state_trusted = True
            if previous.get("completed") is True:
                if not all(self.index_matches(entry, plan_sha) for entry in entries):
                    raise CommandError("El lote retirado no tiene todos sus índices verificados.")
                if not self.nas_ready(entries):
                    self.result("WAIT_NAS", files=len(entries))
                    return
                self.cleanup_staging(entries, stage, plan_sha)
                self.result("COMPLETE", files=len(entries))
                return
            user = get_user_model().objects.get(**{get_user_model().USERNAME_FIELD: options["verification_user"]})
            if not user.is_active or any(not _can_access_archived_bitacora_media(user, entry["name"]) for entry in entries):
                raise CommandError("El usuario existente no tiene acceso a todas las evidencias.")
            remaining = []
            for entry in entries:
                if local_file(entry["name"]).exists():
                    remaining.append(entry)
                elif not self.index_matches(entry, plan_sha):
                    raise CommandError("Falta original sin índice verificado del plan; no se continúa.")
            if remaining:
                stage_plan = plan_path
                if len(remaining) != len(entries):
                    stage_plan = plan_dir / f"{month}.stage.json"
                    if stage_plan.is_symlink():
                        raise CommandError("Subplan inseguro.")
                    atomic_json(stage_plan, {**plan, "entries": remaining})
                self.archive_command(options, "stage", stage_plan, stage_root=str(stage))
            if not self.nas_ready(entries):
                self.state(state_path, plan_sha, "WAIT_NAS")
                self.result("WAIT_NAS", files=len(entries))
                return
            self.archive_command(options, "verify", plan_path)
            self.verify_http(user, entries, options)
            self.archive_command(options, "retire", plan_path, confirm_plan=plan_sha)
            self.state(state_path, plan_sha, "COMPLETE", completed=True)
            self.cleanup_staging(entries, stage, plan_sha)
            self.result("COMPLETE", files=len(entries))
        except (OSError, ValueError, KeyError, TypeError, CommandError, get_user_model().DoesNotExist):
            if state_trusted:
                try:
                    previous = json.loads(state_path.read_text()) if state_path.exists() else {}
                    if previous.get("completed") is not True:
                        self.state(state_path, plan_sha, "ERROR")
                except (OSError, ValueError):
                    pass
            self.result("ERROR")
            raise CommandError("Sincronización no completada; conserve originales y reintente tras revisar el estado.") from None
        finally:
            if lock is not None:
                lock.close()

    def archive_command(self, options, mode, plan, **kwargs):
        call_command("archive_bitacora_media", month=options["month"], mode=mode, plan=str(plan),
                     actor=f"automatic:{options['verification_user']}", stdout=StringIO(), **kwargs)

    def state(self, path, plan_sha, status, *, completed=False):
        atomic_json(path, {"status": status, "completed": completed, "plan_sha256": plan_sha,
                           "updated_at": timezone.now().isoformat()})
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def result(self, status, **values):
        self.stdout.write(json.dumps({"status": status, **values}))

    def index_matches(self, entry, plan_sha):
        indexed = load_archive_entry(entry["name"])
        return indexed is not None and indexed.get("plan_sha256") == plan_sha and all(
            indexed.get(key) == entry[key] for key in ("bytes", "sha256")
        )

    def nas_ready(self, entries):
        try:
            verify_archive_mount()
            return all(matching_stat(Path(os.path.abspath(settings.MEDIA_ARCHIVE_ROOT)) / entry["name"], entry) for entry in entries)
        except (OSError, CommandError):
            return False

    def verify_http(self, user, entries, options):
        session = import_module(settings.SESSION_ENGINE).SessionStore()
        try:
            session["_auth_user_id"] = str(user.pk)
            session["_auth_user_backend"] = settings.AUTHENTICATION_BACKENDS[0]
            session["_auth_user_hash"] = user.get_session_auth_hash()
            session.set_expiry(15 * 60)
            session.save()
            opener = build_opener(ProxyHandler({}), NoRedirect())
            for entry in entries:
                request = Request(options["verification_origin"].rstrip("/") + "/media/" + quote(entry["name"], safe="/") + "?archive=1", headers={
                    "Cookie": f"{settings.SESSION_COOKIE_NAME}={session.session_key}",
                    "Host": options["verification_host"], "X-Forwarded-Proto": "https",
                })
                with opener.open(request, timeout=10) as response:
                    controls = {token.strip().lower() for token in response.headers.get("Cache-Control", "").split(",")}
                    if response.status != 200 or not {"private", "no-store"}.issubset(controls):
                        raise CommandError("HTTP no confirmó lectura privada.")
                    content = response.read(entry["bytes"] + 1)
                    if len(content) != entry["bytes"] or hashlib.sha256(content).hexdigest() != entry["sha256"]:
                        raise CommandError("HTTP no confirmó contenido íntegro.")
        finally:
            if session.session_key:
                session.delete(session.session_key)

    def cleanup_staging(self, entries, stage, plan_sha):
        verify_archive_mount()
        for entry in entries:
            target = stage / entry["name"]
            if not target.exists() or local_file(entry["name"]).exists():
                continue
            if not self.index_matches(entry, plan_sha):
                raise CommandError("Staging conservado: índice no coincide.")
            with open_archive(entry["name"]):
                pass
            original_stat = matching_stat(target, entry)
            current_stat = target.lstat()
            if original_stat is None or any(getattr(current_stat, key) != getattr(original_stat, key) for key in ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_nlink")):
                raise CommandError("Staging conservado: contenido cambió.")
            if not local_file(entry["name"]).exists():
                target.unlink()
