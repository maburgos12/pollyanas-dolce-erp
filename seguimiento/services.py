from __future__ import annotations

import json
from datetime import date, datetime, time

from django.contrib.auth import get_user_model
from django.db import transaction
from django.utils import timezone
from django.utils.dateparse import parse_date, parse_datetime, parse_time

from django.db.models import Q

from recetas.utils.normalizacion import normalizar_nombre
from rrhh.models import Empleado
from seguimiento.models import (
    SeguimientoChecklistItem,
    SeguimientoItem,
    SeguimientoProrrogaSolicitud,
)


def items_pendientes_revision_dg():
    """Acuerdos que requieren acción del DG: en revisión o con prórroga pendiente.

    Independiente de filtros de UI — siempre devuelve todo lo accionable.
    """
    return (
        SeguimientoItem.objects.filter(
            Q(estatus=SeguimientoItem.ESTATUS_EN_REVISION)
            | Q(prorrogas__estatus=SeguimientoProrrogaSolicitud.ESTATUS_PENDIENTE)
        )
        .select_related("responsable_user", "responsable_empleado")
        .prefetch_related("evidencias__usuario", "comentarios", "prorrogas", "checklist")
        .distinct()
        .order_by("fecha_limite", "-updated_at")
    )


def _responsable_nombre(item) -> str:
    if item.responsable_user:
        return item.responsable_user.get_full_name() or item.responsable_user.username
    if item.responsable_empleado:
        return item.responsable_empleado.nombre
    return "Sin asignar"


AGENTE_DG_SOURCE_TABLE_TYPES = {
    "commitments": SeguimientoItem.TIPO_COMPROMISO,
    "minute_agreements": SeguimientoItem.TIPO_MINUTA,
    "minute_projects": SeguimientoItem.TIPO_PROYECTO,
}
CHECKLIST_TITULO_MAX_LENGTH = 220
_PRESERVE_CHECKLIST = object()


def _truncate_for_charfield(value: str, max_length: int) -> str:
    value = (value or "").strip()
    if len(value) <= max_length:
        return value
    return value[: max_length - 3].rstrip() + "..."


def _json_datetime(value) -> str:
    if not value:
        return ""
    dt_value = agente_dg_as_datetime(value)
    if dt_value:
        return dt_value.isoformat()
    return str(value)


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

    empleado = Empleado.objects.filter(activo=True, usuario_erp=user).first()
    if empleado:
        return empleado

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
                values.append(item.get("title") or item.get("text") or item.get("texto") or item.get("label") or "")
        return [value.strip() for value in values if value and value.strip()]
    return []


def agente_dg_checklist_payload_from_json(raw_value: str | None) -> list[dict]:
    if not raw_value:
        return []
    try:
        payload = json.loads(raw_value)
    except (TypeError, ValueError):
        return []
    if not isinstance(payload, list):
        return []

    items = []
    for item in payload:
        if isinstance(item, str):
            titulo = item.strip()
            completado = False
            completado_at = None
        elif isinstance(item, dict):
            titulo = (item.get("title") or item.get("text") or item.get("texto") or item.get("label") or "").strip()
            completado = bool(item.get("completed") or item.get("completado") or item.get("done") or item.get("checked"))
            completado_at = item.get("completed_at") or item.get("completado_at")
        else:
            continue
        if titulo:
            items.append(
                {
                    "titulo": titulo,
                    "descripcion": "",
                    "completado": completado,
                    "completado_at": completado_at,
                }
            )
    return items


def _texto_a_puntos(texto: str) -> list[str]:
    """Desglosa un entregable en puntos: por líneas o por separadores '-' / '•'."""
    if not texto:
        return []
    bruto = texto.replace("•", "\n").replace("; ", "\n")
    lineas = [l for l in bruto.splitlines()]
    if len(lineas) <= 1:
        # Una sola línea tipo "- A - B - C": separar por " - "
        unica = lineas[0] if lineas else texto
        partes = unica.split(" - ")
        lineas = partes if len(partes) > 1 else [unica]
    puntos = []
    for linea in lineas:
        limpio = linea.strip().lstrip("-*•").strip()
        if limpio:
            puntos.append(limpio)
    return puntos


def sub_checklist_de_paso(checklist_items_json: str | None, deliverable_text: str | None) -> list[dict]:
    """Devuelve [{titulo, completado}] del checklist operativo del paso.

    Usa el estado real de checklist_items_json si existe; si no, desglosa el entregable
    (todos pendientes) para que al menos se vean los puntos.
    """
    items: list[dict] = []
    raw = (checklist_items_json or "").strip()
    if raw:
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            payload = None
        if isinstance(payload, list):
            for it in payload:
                if isinstance(it, str):
                    titulo = it.strip()
                    completado = False
                elif isinstance(it, dict):
                    titulo = (it.get("title") or it.get("titulo") or it.get("text") or it.get("label") or "").strip()
                    estado = it.get("status") or it.get("estado")
                    completado = bool(
                        it.get("done")
                        or it.get("completed")
                        or it.get("completado")
                        or it.get("checked")
                        or (isinstance(estado, str) and estado.upper() in {"DONE", "COMPLETED", "RESUELTO", "OK"})
                    )
                else:
                    continue
                if titulo:
                    items.append({"titulo": titulo, "completado": completado})
    if not items:
        items = [{"titulo": p, "completado": False} for p in _texto_a_puntos(deliverable_text or "")]
    return items


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
            "source_archived_at": _json_datetime(row.get("archived_at")),
            "source_completed_at": _json_datetime(row.get("completed_at")),
            "synced_at": timezone.now().isoformat(),
            "source_participants": participant_payload,
        }
        # Solo minutas y proyectos pasan por aprobación del DG. Los compromisos son
        # actividades de desempeño que el colaborador gestiona y cierra por su cuenta.
        requiere_aprobacion = tipo in (SeguimientoItem.TIPO_MINUTA, SeguimientoItem.TIPO_PROYECTO)

        estatus = agente_dg_status_a_erp(row.get("status"))
        # El Agente DG manda los compromisos como SUBMITTED → EN_REVISION; como no
        # requieren tu visto bueno, no deben atorarse en tu bandeja: quedan en proceso.
        if not requiere_aprobacion and estatus == SeguimientoItem.ESTATUS_EN_REVISION:
            estatus = SeguimientoItem.ESTATUS_EN_PROCESO

        defaults = {
            "tipo": tipo,
            "titulo": row["titulo"],
            "descripcion": row.get("descripcion") or "",
            "entregable_esperado": row.get("expected_deliverable") or "",
            "responsable_user": user,
            "responsable_empleado": empleado,
            "area": row.get("area_name") or "",
            "fecha_limite": agente_dg_as_datetime(row.get("due_at") or row.get("target_date") or row.get("due_date"), row.get("due_time")),
            "estatus": estatus,
            "requiere_aprobacion": requiere_aprobacion,
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

        cierre_fuente = agente_dg_status_a_erp(row.get("status")) in {
            SeguimientoItem.ESTATUS_COMPLETADO,
            SeguimientoItem.ESTATUS_CANCELADO,
        }
        cierre_at = agente_dg_as_datetime(row.get("archived_at") or row.get("completed_at"))
        if cierre_fuente and cierre_at and item.aprobado_at != cierre_at:
            item.aprobado_at = cierre_at
            item.save(update_fields=["aprobado_at", "updated_at"])
        elif not cierre_fuente and item.aprobado_at:
            item.aprobado_at = None
            item.save(update_fields=["aprobado_at", "updated_at"])

        item.participantes_user.set(participant_users)
        item.participantes_empleado.set(participant_empleados)

        if checklist is _PRESERVE_CHECKLIST:
            return
        checklist_payload = checklist
        if checklist_payload is None:
            checklist_payload = agente_dg_checklist_payload_from_json(row.get("checklist_items_json"))
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

    def _resolver_aprobador_user(self, email: str, nombre: str):
        """Intenta resolver el aprobador de un paso a un usuario del ERP.

        Busca primero por e-mail (exacto), luego por nombre completo normalizado.
        Devuelve None si no se puede resolver.
        """
        User = get_user_model()
        if email:
            user = User.objects.filter(email__iexact=email.strip(), is_active=True).first()
            if user:
                return user
        if nombre:
            nombre_norm = normalizar_nombre(nombre.strip())
            for u in User.objects.filter(is_active=True):
                if normalizar_nombre(u.get_full_name() or u.username) == nombre_norm:
                    return u
        return None

    def _sync_checklist(self, item: SeguimientoItem, checklist_payload):
        if not checklist_payload:
            item.checklist.all().delete()
            return
        existing_checks = list(item.checklist.all())
        existing_by_order = {check.orden: check for check in existing_checks}
        existing_by_step = {
            check.origen_step_id: check
            for check in existing_checks
            if check.origen_step_id
        }
        desired_check_ids = set()
        for index, payload in enumerate(checklist_payload, start=1):
            titulo_completo = (payload.get("titulo") or "").strip()
            if not titulo_completo:
                continue
            titulo = _truncate_for_charfield(titulo_completo, CHECKLIST_TITULO_MAX_LENGTH)
            origen_step_id = payload.get("origen_step_id")
            check = existing_by_step.get(origen_step_id) if origen_step_id else None
            if check and check.pk in desired_check_ids:
                check = None
            if not check:
                check = existing_by_order.get(index)
                if check and check.pk in desired_check_ids:
                    check = None
            # Resolver el aprobador del ERP desde e-mail o nombre (datos del Agente DG)
            aprobador_user = self._resolver_aprobador_user(
                payload.get("aprobador_email") or "",
                payload.get("aprobador_nombre") or "",
            )
            defaults = {
                "titulo": titulo,
                "origen_step_id": payload.get("origen_step_id"),
                "descripcion": payload.get("descripcion") or (titulo_completo if titulo_completo != titulo else ""),
                "completado": bool(payload.get("completado")),
                "entregable": payload.get("entregable") or "",
                "responsable_nombre": payload.get("responsable_nombre") or "",
                "aprobador_nombre": payload.get("aprobador_nombre") or "",
                "aprobador_user": aprobador_user,
                "requiere_aprobacion": bool(payload.get("requiere_aprobacion")),
                "vence": agente_dg_as_datetime(payload.get("vence")) if payload.get("vence") else None,
                "prioridad": payload.get("prioridad") or "",
                "tipo": payload.get("tipo") or "",
                "estatus_origen": payload.get("estatus_origen") or "",
                "sub_checklist": sub_checklist_de_paso(payload.get("checklist_items_json"), payload.get("entregable")),
            }
            if check:
                check.orden = index
                for key, value in defaults.items():
                    setattr(check, key, value)
                if check.completado:
                    check.completado_at = agente_dg_as_datetime(payload.get("completado_at") or payload.get("completed_at")) or check.completado_at or timezone.now()
                else:
                    check.completado_por = None
                    check.completado_at = None
                check.save()
                desired_check_ids.add(check.pk)
            else:
                if defaults["completado"]:
                    defaults["completado_at"] = agente_dg_as_datetime(payload.get("completado_at") or payload.get("completed_at")) or timezone.now()
                check = SeguimientoChecklistItem.objects.create(seguimiento=item, orden=index, **defaults)
                desired_check_ids.add(check.pk)
        item.checklist.exclude(pk__in=desired_check_ids).delete()


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
    checklist = payload.get("checklist", _PRESERVE_CHECKLIST)
    if checklist is _PRESERVE_CHECKLIST and "checklist_items_json" in record:
        checklist = None
    with transaction.atomic():
        importer._upsert_item(
            record,
            source_table,
            AGENTE_DG_SOURCE_TABLE_TYPES[source_table],
            counters,
            checklist=checklist,
            participants=payload.get("participants"),
        )
    return counters
