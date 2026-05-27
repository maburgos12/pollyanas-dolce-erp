from __future__ import annotations

import json
from datetime import date, datetime, time

from django.contrib.auth import get_user_model
from django.db import transaction
from django.utils import timezone
from django.utils.dateparse import parse_date, parse_datetime, parse_time

from recetas.utils.normalizacion import normalizar_nombre
from rrhh.models import Empleado
from seguimiento.models import SeguimientoChecklistItem, SeguimientoItem


AGENTE_DG_SOURCE_TABLE_TYPES = {
    "commitments": SeguimientoItem.TIPO_COMPROMISO,
    "minute_agreements": SeguimientoItem.TIPO_MINUTA,
    "minute_projects": SeguimientoItem.TIPO_PROYECTO,
}


def _tokens(value: str) -> set[str]:
    return {token for token in normalizar_nombre(value or "").replace(".", " ").split() if len(token) > 1}


def _user_area_hints(user) -> set[str]:
    hints = set()
    for group in getattr(user, "groups").all():
        hints.update(_tokens(group.name))
    profile = getattr(user, "userprofile", None)
    if profile:
        hints.update(_tokens(getattr(getattr(profile, "departamento", None), "nombre", "")))
        hints.update(_tokens(getattr(getattr(profile, "sucursal", None), "nombre", "")))
    return hints


def _score_empleado_para_usuario(empleado: Empleado, user, user_tokens: set[str], area_hints: set[str]) -> int:
    empleado_tokens = _tokens(empleado.nombre_normalizado or empleado.nombre)
    if not user_tokens or not empleado_tokens:
        return 0
    overlap = user_tokens.intersection(empleado_tokens)
    if not overlap:
        return 0

    score = len(overlap) * 10
    if user_tokens.issubset(empleado_tokens):
        score += 30
    if empleado_tokens == user_tokens:
        score += 50

    area_tokens = _tokens(f"{empleado.area} {empleado.puesto} {empleado.sucursal}")
    if area_hints and area_tokens.intersection(area_hints):
        score += 20
    return score


def empleado_de_usuario(user):
    if not user or not user.is_authenticated:
        return None

    email = (getattr(user, "email", "") or "").strip()
    if email:
        empleado = Empleado.objects.filter(activo=True, email__iexact=email).first()
        if empleado:
            return empleado

    nombre = (user.get_full_name() or user.username or "").strip()
    nombre_norm = normalizar_nombre(nombre)
    if not nombre_norm:
        return None
    empleado = Empleado.objects.filter(activo=True, nombre_normalizado=nombre_norm).first()
    if empleado:
        return empleado

    user_tokens = _tokens(f"{user.get_full_name()} {user.username}")
    area_hints = _user_area_hints(user)
    scored = []
    for candidato in Empleado.objects.filter(activo=True).only(
        "id",
        "nombre",
        "nombre_normalizado",
        "email",
        "area",
        "puesto",
        "sucursal",
        "activo",
    ):
        score = _score_empleado_para_usuario(candidato, user, user_tokens, area_hints)
        if score:
            scored.append((score, candidato.id, candidato))
    if not scored:
        return None
    scored.sort(key=lambda item: (-item[0], item[1]))
    return scored[0][2]


def usuarios_para_empleado(empleado):
    if not empleado:
        return get_user_model().objects.none()

    User = get_user_model()
    qs = User.objects.none()
    if empleado.email:
        qs = qs | User.objects.filter(email__iexact=empleado.email)
    nombre_norm = empleado.nombre_normalizado or normalizar_nombre(empleado.nombre or "")
    if nombre_norm:
        matches = [user.pk for user in User.objects.all() if normalizar_nombre(user.get_full_name() or user.username) == nombre_norm]
        qs = qs | User.objects.filter(pk__in=matches)
    return qs.distinct()


def agente_dg_status_a_erp(raw_status: str | None) -> str:
    status = str(raw_status or "").upper()
    if status in {"SUBMITTED", "IN_REVIEW"}:
        return SeguimientoItem.ESTATUS_EN_REVISION
    if status in {"REVIEWED", "CLOSED", "COMPLETED", "APPROVED", "ENTREGADO_A_TIEMPO", "ENTREGADO_TARDE"}:
        return SeguimientoItem.ESTATUS_COMPLETADO
    if status in {"BLOCKED", "AT_RISK", "NO_ENTREGADO"}:
        return SeguimientoItem.ESTATUS_BLOQUEADO
    if status in {"CANCELLED", "CANCELED"}:
        return SeguimientoItem.ESTATUS_CANCELADO
    if status in {"IN_PROGRESS", "DUE_SOON", "DUE_TODAY", "READY", "OVERDUE", "POSTPONED", "PAUSED"}:
        return SeguimientoItem.ESTATUS_EN_PROCESO
    return SeguimientoItem.ESTATUS_PENDIENTE


def agente_dg_as_datetime(value, due_time=None):
    if not value:
        return None
    parsed_due_time = due_time
    if isinstance(due_time, str):
        parsed_due_time = parse_time(due_time)
    if isinstance(value, datetime):
        dt_value = value
    elif isinstance(value, date):
        dt_value = datetime.combine(value, parsed_due_time or time(18, 0))
    elif isinstance(value, str):
        dt_value = parse_datetime(value)
        if not dt_value:
            parsed_date = parse_date(value)
            if not parsed_date:
                return None
            dt_value = datetime.combine(parsed_date, parsed_due_time or time(18, 0))
    else:
        return None
    if timezone.is_naive(dt_value):
        dt_value = timezone.make_aware(dt_value, timezone.get_current_timezone())
    return dt_value


def agente_dg_checklist_from_json(raw_value: str | None) -> list[str]:
    if not raw_value:
        return []
    try:
        payload = json.loads(raw_value)
    except (TypeError, ValueError):
        return []
    if isinstance(payload, list):
        values = []
        for item in payload:
            if isinstance(item, str):
                values.append(item)
            elif isinstance(item, dict):
                values.append(item.get("title") or item.get("texto") or item.get("label") or "")
        return [value.strip() for value in values if value and value.strip()]
    return []


class AgenteDGSeguimientoImporter:
    def _resolve_user(self, row):
        User = get_user_model()
        email = (row.get("user_email") or "").strip()
        name = (row.get("user_name") or "").strip()
        if email:
            user = User.objects.filter(email__iexact=email).first()
            if user:
                return user
            user = User.objects.filter(username__iexact=email).first()
            if user:
                return user
        name_norm = normalizar_nombre(name)
        for user in User.objects.filter(is_active=True):
            if normalizar_nombre(user.get_full_name() or user.username) == name_norm:
                return user
        return None

    def _build_project_participants(self, steps, participants):
        by_project = {}

        def add(project_id, user_id, email, name, role):
            if not project_id or not user_id:
                return
            by_project.setdefault(project_id, {})
            by_project[project_id][user_id] = {
                "user_id": user_id,
                "user_email": email or "",
                "user_name": name or "",
                "role": role or "",
            }

        for step in steps:
            add(
                step.get("project_id"),
                step.get("owner_user_id"),
                step.get("owner_user_email"),
                step.get("owner_user_name"),
                "STEP_OWNER",
            )
            add(
                step.get("project_id"),
                step.get("approver_user_id"),
                step.get("approver_user_email"),
                step.get("approver_user_name"),
                "STEP_APPROVER",
            )
        for participant in participants:
            add(
                participant.get("project_id"),
                participant.get("user_id"),
                participant.get("user_email"),
                participant.get("user_name"),
                participant.get("role"),
            )
        return {project_id: list(values.values()) for project_id, values in by_project.items()}

    def _upsert_item(self, row, source_table: str, tipo: str, counters: dict[str, int], checklist=None, participants=None):
        if not row.get("id") or not row.get("titulo"):
            counters["skipped"] += 1
            return

        user = self._resolve_user(row)
        empleado = empleado_de_usuario(user) if user else None
        participant_users, participant_empleados, participant_payload = self._resolve_participants(participants or [])
        participant_users = [participant for participant in participant_users if not user or participant.pk != user.pk]
        participant_empleados = [participant for participant in participant_empleados if not empleado or participant.pk != empleado.pk]
        metadata = {
            "source": "agente_dg",
            "source_table": source_table,
            "source_id": row["id"],
            "source_user_id": row.get("user_id"),
            "source_user_email": row.get("user_email") or "",
            "source_user_name": row.get("user_name") or "",
            "source_status": str(row.get("status") or ""),
            "source_participants": participant_payload,
        }
        defaults = {
            "tipo": tipo,
            "titulo": row["titulo"],
            "descripcion": row.get("descripcion") or "",
            "entregable_esperado": row.get("expected_deliverable") or "",
            "responsable_user": user,
            "responsable_empleado": empleado,
            "area": row.get("area_name") or "",
            "fecha_limite": agente_dg_as_datetime(row.get("due_at") or row.get("target_date") or row.get("due_date"), row.get("due_time")),
            "estatus": agente_dg_status_a_erp(row.get("status")),
            "origen": "Agente DG",
            "referencia_externa": f"{source_table}:{row['id']}",
            "metadata": metadata,
        }
        item = SeguimientoItem.objects.filter(
            metadata__source="agente_dg",
            metadata__source_table=source_table,
            metadata__source_id=row["id"],
        ).first()
        if item:
            for key, value in defaults.items():
                setattr(item, key, value)
            item.save()
            counters["updated"] += 1
        else:
            item = SeguimientoItem.objects.create(**defaults)
            counters["created"] += 1

        item.participantes_user.set(participant_users)
        item.participantes_empleado.set(participant_empleados)

        checklist_payload = checklist
        if checklist_payload is None:
            checklist_payload = [
                {"titulo": title, "descripcion": "", "completado": False}
                for title in agente_dg_checklist_from_json(row.get("checklist_items_json"))
            ]
        self._sync_checklist(item, checklist_payload)

    def _resolve_participants(self, participants):
        users = []
        empleados = []
        payload = []
        seen_users = set()
        seen_empleados = set()
        for participant in participants:
            user = self._resolve_user(participant)
            empleado = empleado_de_usuario(user) if user else None
            if user and user.pk not in seen_users:
                users.append(user)
                seen_users.add(user.pk)
            if empleado and empleado.pk not in seen_empleados:
                empleados.append(empleado)
                seen_empleados.add(empleado.pk)
            payload.append(
                {
                    "source_user_id": participant.get("user_id"),
                    "source_user_email": participant.get("user_email") or "",
                    "source_user_name": participant.get("user_name") or "",
                    "role": participant.get("role") or "",
                    "erp_user_id": user.pk if user else None,
                    "erp_empleado_id": empleado.pk if empleado else None,
                }
            )
        return users, empleados, payload

    def _sync_checklist(self, item: SeguimientoItem, checklist_payload):
        if not checklist_payload:
            item.checklist.all().delete()
            return
        existing = {check.orden: check for check in item.checklist.all()}
        desired_orders = set()
        for index, payload in enumerate(checklist_payload, start=1):
            titulo = (payload.get("titulo") or "").strip()
            if not titulo:
                continue
            desired_orders.add(index)
            check = existing.get(index)
            defaults = {
                "titulo": titulo,
                "descripcion": payload.get("descripcion") or "",
                "completado": bool(payload.get("completado")),
            }
            if check:
                for key, value in defaults.items():
                    setattr(check, key, value)
                if not check.completado:
                    check.completado_por = None
                    check.completado_at = None
                check.save()
            else:
                SeguimientoChecklistItem.objects.create(seguimiento=item, orden=index, **defaults)
        item.checklist.exclude(orden__in=desired_orders).delete()


def upsert_agente_dg_payload(payload: dict) -> dict[str, int]:
    source_table = payload.get("source_table")
    source_id = payload.get("source_id")
    action = payload.get("action") or "upsert"
    record = dict(payload.get("record") or {})
    counters = {"created": 0, "updated": 0, "skipped": 0}

    if action != "upsert":
        counters["skipped"] += 1
        return counters
    if source_table not in AGENTE_DG_SOURCE_TABLE_TYPES:
        raise ValueError("source_table no soportado")
    if source_id and not record.get("id"):
        record["id"] = source_id

    importer = AgenteDGSeguimientoImporter()
    with transaction.atomic():
        importer._upsert_item(
            record,
            source_table,
            AGENTE_DG_SOURCE_TABLE_TYPES[source_table],
            counters,
            checklist=payload.get("checklist"),
            participants=payload.get("participants"),
        )
    return counters
