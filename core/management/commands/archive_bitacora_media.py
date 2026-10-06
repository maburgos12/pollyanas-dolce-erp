"""Archive one closed month; receipt and retirement are separate explicit steps."""

import hashlib
import json
import os
import shutil
from datetime import date, timedelta
from pathlib import Path

from django.apps import apps
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import models, transaction
from django.utils import timezone

from core.media_archive import ArchiveUnavailable, load_archive_entry, open_archive, write_archive_entry
from logistica.models import BitacoraSalidaLlegada

FIELDS = ("foto_tablero_salida", "foto_tablero_llegada", "foto_ticket_combustible")
MAX_BYTES = 20 * 1024 * 1024


def verify_archive_mount():
    """A matching local directory is not proof of receipt by the NAS."""
    root = Path(os.path.abspath(settings.MEDIA_ARCHIVE_ROOT))
    expected_source = getattr(settings, "MEDIA_ARCHIVE_MOUNT_SOURCE", "")
    if not expected_source or not expected_source.startswith("//"):
        raise CommandError("Configure la identidad del recurso NAS, no un directorio de staging.")
    try:
        lines = Path("/proc/self/mountinfo").read_text().splitlines()
    except OSError as exc:
        raise CommandError("No se puede corroborar el montaje NAS; conserve originales.") from exc
    matches = []
    for line in lines:
        head, _, tail = line.partition(" - ")
        left, right = head.split(), tail.split()
        if len(left) < 6 or len(right) < 3:
            continue
        mountpoint = Path(left[4].replace("\\040", " "))
        if root.is_relative_to(mountpoint):
            matches.append((len(mountpoint.parts), right[0], right[1], set(left[5].split(",")) | set(right[2].split(","))))
    if not matches:
        raise CommandError("El destino no está montado desde el NAS.")
    _, filesystem, source, flags = max(matches, key=lambda match: match[0])
    if filesystem != "cifs" or source != expected_source or flags & {"rw", "hard"} or not {"ro", "nosuid", "nodev", "noexec", "soft"}.issubset(flags):
        raise CommandError("NAS debe coincidir y montarse ro,nosuid,nodev,noexec,soft; conserve originales.")


def digest_file(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def local_file(name):
    from pathlib import PurePosixPath

    relative = PurePosixPath(name)
    if not name.startswith("bitacora/") or relative.as_posix() != name or relative.is_absolute() or ".." in relative.parts or "\\" in name:
        raise CommandError("Ruta de evidencia inválida.")
    root = Path(settings.MEDIA_ROOT).resolve()
    path = root.joinpath(*relative.parts)
    if any(item.is_symlink() for item in (path, *path.parents) if item != root and root in item.parents):
        raise CommandError("No se archivan enlaces simbólicos.")
    if not path.resolve().is_relative_to(root):
        raise CommandError("Ruta fuera de MEDIA_ROOT.")
    return path


def atomic_json(path, value):
    from tempfile import NamedTemporaryFile

    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as stream:
        temp = Path(stream.name)
        try:
            json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        except BaseException:
            temp.unlink(missing_ok=True)
            raise
    os.replace(temp, path)


def references(names):
    """Check every FileField so shared evidence is never retired as an orphan."""
    result = {name: [] for name in names}
    for model in apps.get_models():
        fields = [field.name for field in model._meta.fields if isinstance(field, models.FileField)]
        if not fields:
            continue
        query = models.Q()
        for field in fields:
            query |= models.Q(**{f"{field}__in": names})
        for row in model.objects.filter(query).values("pk", *fields).iterator(chunk_size=500):
            for field in fields:
                name = row[field]
                if name in result:
                    result[name].append({"model": model._meta.label_lower, "id": row["pk"], "field": field})
    return {name: sorted(refs, key=lambda ref: (ref["model"], ref["id"], ref["field"])) for name, refs in result.items()}


def audit_event(plan_path, event):
    audit_path = plan_path.with_suffix(".audit.jsonl")
    with audit_path.open("a", encoding="utf-8") as log:
        os.chmod(audit_path, 0o600)
        log.write(json.dumps({**event, "at": timezone.now().isoformat()}) + "\n")
        log.flush()
        os.fsync(log.fileno())


class Command(BaseCommand):
    help = "Plan/stage/verify/retire/restore de fotos de un mes cerrado; nunca modifica registros."

    def add_arguments(self, parser):
        parser.add_argument("--month", required=True, help="YYYY-MM; al menos 90 días de antigüedad")
        parser.add_argument("--mode", choices=("plan", "stage", "verify", "retire", "restore"), default="plan")
        parser.add_argument("--plan", required=True, help="JSON privado fuera de MEDIA_ROOT")
        parser.add_argument("--stage-root", help="Destino local de publicación HBS; nunca equivale a recepción NAS")
        parser.add_argument("--confirm-plan", help="SHA-256 exacto del plan para retirar originales")
        parser.add_argument("--actor", help="Responsable de la operación; obligatorio fuera de plan")

    def handle(self, *args, **options):
        try:
            self.run(options)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise CommandError("Archivo histórico no disponible o plan inválido; no continúe con retiros.") from exc

    def run(self, options):
        month_text = options["month"]
        try:
            month = date.fromisoformat(f"{month_text}-01")
        except ValueError as exc:
            raise CommandError("Mes inválido; use YYYY-MM.") from exc
        end = (month.replace(day=28) + timedelta(days=4)).replace(day=1)
        if month.strftime("%Y-%m") != month_text or end > timezone.localdate() - timedelta(days=90):
            raise CommandError("El mes debe estar cerrado y completo fuera de los últimos 90 días.")
        plan_path = Path(options["plan"]).resolve()
        media_root = Path(settings.MEDIA_ROOT).resolve()
        if plan_path.is_relative_to(media_root):
            raise CommandError("El plan debe permanecer fuera del almacenamiento público.")
        mode = options["mode"]
        if mode == "plan":
            if plan_path.exists():
                raise CommandError("El plan existe; conserve su identidad o use otro nombre.")
            records = list(BitacoraSalidaLlegada.objects.filter(fecha__gte=month, fecha__lt=end, cerrada=True).values("pk", *FIELDS))
            allowed_ids = {row["pk"] for row in records}
            names = sorted({str(row[field]) for row in records for field in FIELDS if row[field]})
            refs = references(names) if names else {}
            entries, skipped = [], []
            for name in names:
                if any(ref["model"] != "logistica.bitacorasalidallegada" or ref["field"] not in FIELDS or ref["id"] not in allowed_ids for ref in refs[name]):
                    skipped.append({"name": name, "reason": "shared_or_ineligible_reference"})
                    continue
                path = local_file(name)
                if not path.is_file() or path.stat().st_nlink != 1:
                    skipped.append({"name": name, "reason": "missing_or_hardlinked"})
                    continue
                size = path.stat().st_size
                if not 0 < size <= MAX_BYTES:
                    skipped.append({"name": name, "reason": "unsupported_size"})
                    continue
                entries.append({"name": name, "bytes": size, "sha256": digest_file(path), "source_refs": refs[name]})
            atomic_json(plan_path, {"version": 1, "month": month_text, "entries": entries, "skipped": skipped, "created_at": timezone.now().isoformat()})
            self.stdout.write(json.dumps({"files": len(entries), "bytes": sum(e["bytes"] for e in entries), "skipped": len(skipped), "plan_sha256": digest_file(plan_path)}))
            return
        if not options["actor"]:
            raise CommandError("Indique --actor para registrar responsabilidad.")
        plan_sha = digest_file(plan_path)
        plan = json.loads(plan_path.read_text())
        if plan.get("version") != 1 or plan.get("month") != month_text or not isinstance(plan.get("entries"), list):
            raise CommandError("Plan incompatible.")
        entries = plan["entries"]
        names = [entry["name"] for entry in entries]
        if len(set(names)) != len(names):
            raise CommandError("El plan contiene duplicados.")
        if mode == "retire" and options["confirm_plan"] != plan_sha:
            raise CommandError("Retirar exige --confirm-plan con el SHA-256 exacto del plan revisado.")
        if mode == "stage" and not options["stage_root"]:
            raise CommandError("Publicar exige --stage-root.")
        if mode in {"verify", "retire", "restore"} and not settings.MEDIA_ARCHIVE_ROOT:
            raise CommandError("No hay acceso permanente configurado al NAS; conservar originales.")
        if mode in {"verify", "retire", "restore"}:
            verify_archive_mount()
        for entry in entries:
            refs = entry.get("source_refs")
            if not refs or any(ref.get("model") != "logistica.bitacorasalidallegada" or ref.get("field") not in FIELDS or type(ref.get("id")) is not int for ref in refs):
                raise CommandError("Cada evidencia debe pertenecer a bitácoras cerradas del lote.")
        completed = 0
        for entry in entries:
            name = entry["name"]
            path = local_file(name)
            # Pin all source references while checking eligibility and removing a source.
            with transaction.atomic():
                expected_refs = entry["source_refs"]
                ids = {ref["id"] for ref in expected_refs}
                records = list(BitacoraSalidaLlegada.objects.select_for_update().filter(pk__in=ids, cerrada=True, fecha__gte=month, fecha__lt=end).values_list("pk", flat=True))
                if set(records) != ids or references([name])[name] != expected_refs:
                    raise CommandError("Cambió una referencia o la bitácora; vuelva a planificar sin retirar.")
                if path.exists() and (path.stat().st_size != entry["bytes"] or digest_file(path) != entry["sha256"] or path.stat().st_nlink != 1):
                    raise CommandError("Cambió el original; se conserva y debe revisarse.")
                original_stat = path.stat() if path.exists() else None
                audit = {"mode": mode, "actor": options["actor"], "name_hash": hashlib.sha256(name.encode()).hexdigest(), "plan_sha256": plan_sha, "bytes": entry["bytes"]}
                audit_event(plan_path, {**audit, "state": "started"})
                if mode == "stage":
                    if not path.is_file():
                        raise CommandError("No existe original para publicar.")
                    target_root = Path(options["stage_root"]).resolve()
                    if target_root.is_relative_to(media_root) or media_root.is_relative_to(target_root):
                        raise CommandError("Staging debe estar separado de MEDIA_ROOT.")
                    target = target_root / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    if target.is_symlink() or not target.resolve().is_relative_to(target_root):
                        raise CommandError("Staging inseguro.")
                    if not target.exists():
                        shutil.copyfile(path, target)
                    if target.stat().st_size != entry["bytes"] or digest_file(target) != entry["sha256"]:
                        raise CommandError("Staging no coincide; conserve originales.")
                elif mode == "verify":
                    # Direct receipt is checked before publishing the trusted local index.
                    root = Path(settings.MEDIA_ARCHIVE_ROOT).resolve()
                    archived = root / name
                    if archived.is_symlink() or not archived.resolve().is_relative_to(root) or archived.stat().st_size != entry["bytes"] or digest_file(archived) != entry["sha256"]:
                        raise CommandError("No hay recepción íntegra en NAS.")
                    write_archive_entry({**entry, "status": "verified", "month": month_text, "verified_at": timezone.now().isoformat(), "actor": options["actor"], "plan_sha256": plan_sha})
                    with open_archive(name):
                        pass
                else:
                    indexed = load_archive_entry(name)
                    if not indexed or any(indexed.get(key) != entry[key] for key in ("bytes", "sha256")) or indexed.get("plan_sha256") != plan_sha:
                        raise CommandError("No hay índice verificado para este plan.")
                    with open_archive(name) as source:
                        if mode == "restore" and not path.exists():
                            from tempfile import NamedTemporaryFile

                            path.parent.mkdir(parents=True, exist_ok=True)
                            with NamedTemporaryFile(dir=path.parent, delete=False) as restored:
                                temporary = Path(restored.name)
                                try:
                                    shutil.copyfileobj(source, restored)
                                    restored.flush()
                                    os.fsync(restored.fileno())
                                    if temporary.stat().st_size != entry["bytes"] or digest_file(temporary) != entry["sha256"]:
                                        raise CommandError("Restauración incompleta; se conserva lectura NAS.")
                                    os.link(temporary, path)  # Atomic publication without overwriting.
                                finally:
                                    temporary.unlink(missing_ok=True)
                        elif mode == "retire" and path.exists():
                            current_stat = path.stat()
                            if original_stat is None or any(getattr(current_stat, key) != getattr(original_stat, key) for key in ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_nlink")):
                                raise CommandError("El original cambió durante la verificación; no se retira.")
                            path.unlink()
                completed += 1
                audit_event(plan_path, {**audit, "state": "completed"})
        self.stdout.write(json.dumps({"mode": mode, "files": completed, "plan_sha256": plan_sha, "originals_retired": mode == "retire"}))
