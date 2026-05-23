from __future__ import annotations

import json
import os
from datetime import date, datetime, time

import psycopg2
from psycopg2.extras import RealDictCursor
from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from recetas.utils.normalizacion import normalizar_nombre
from seguimiento.models import SeguimientoChecklistItem, SeguimientoItem
from seguimiento.services import empleado_de_usuario


COMMITMENT_QUERY = """
    SELECT
        c.id,
        COALESCE(c.titulo, c.title) AS titulo,
        COALESCE(c.descripcion, c.description) AS descripcion,
        c.expected_deliverable,
        c.status,
        c.due_date,
        c.due_time,
        c.assigned_to AS user_id,
        u.email AS user_email,
        u.name AS user_name,
        a.name AS area_name
    FROM commitments c
    LEFT JOIN users u ON u.id = c.assigned_to
    LEFT JOIN areas a ON a.id = c.area_id
    ORDER BY c.id
"""

MINUTE_QUERY = """
    SELECT
        m.id,
        m.title AS titulo,
        m.agreement_text AS descripcion,
        m.checklist_items_json,
        m.status,
        m.due_at,
        m.collaborator_user_id AS user_id,
        u.email AS user_email,
        u.name AS user_name,
        m.meeting_label AS area_name
    FROM minute_agreements m
    LEFT JOIN users u ON u.id = m.collaborator_user_id
    WHERE m.archived_at IS NULL
    ORDER BY m.id
"""

PROJECT_QUERY = """
    SELECT
        p.id,
        p.title AS titulo,
        COALESCE(p.objective, p.description) AS descripcion,
        p.status,
        p.target_date,
        p.owner_user_id AS user_id,
        u.email AS user_email,
        u.name AS user_name,
        'Proyecto' AS area_name
    FROM minute_projects p
    LEFT JOIN users u ON u.id = p.owner_user_id
    ORDER BY p.id
"""

PROJECT_STEPS_QUERY = """
    SELECT
        s.id,
        s.project_id,
        s.owner_user_id,
        owner.email AS owner_user_email,
        owner.name AS owner_user_name,
        s.approver_user_id,
        approver.email AS approver_user_email,
        approver.name AS approver_user_name,
        s.title,
        s.description,
        s.deliverable_text,
        s.status,
        s.order_index,
        s.completed_at
    FROM minute_project_steps s
    LEFT JOIN users owner ON owner.id = s.owner_user_id
    LEFT JOIN users approver ON approver.id = s.approver_user_id
    ORDER BY s.project_id, s.order_index, s.id
"""

PROJECT_PARTICIPANTS_QUERY = """
    SELECT
        s.project_id,
        spp.step_id,
        spp.user_id,
        u.email AS user_email,
        u.name AS user_name,
        spp.role
    FROM minute_project_step_participants spp
    JOIN minute_project_steps s ON s.id = spp.step_id
    LEFT JOIN users u ON u.id = spp.user_id
    ORDER BY s.project_id, spp.step_id, spp.id
"""


def _status_agente_a_erp(raw_status: str | None) -> str:
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


def _as_datetime(value, due_time=None):
    if not value:
        return None
    if isinstance(value, datetime):
        dt_value = value
    elif isinstance(value, date):
        dt_value = datetime.combine(value, due_time or time(18, 0))
    else:
        return None
    if timezone.is_naive(dt_value):
        dt_value = timezone.make_aware(dt_value, timezone.get_current_timezone())
    return dt_value


def _checklist_from_json(raw_value: str | None) -> list[str]:
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


class Command(BaseCommand):
    help = "Importa compromisos, minutas y proyectos de Agente DG hacia Seguimiento del ERP."

    def add_arguments(self, parser):
        parser.add_argument(
            "--database-url",
            default=os.getenv("AGENTE_DG_SYNC_DATABASE_URL") or os.getenv("AGENTE_DG_DATABASE_URL"),
            help="URL PostgreSQL sync de Agente DG. También puede venir de AGENTE_DG_SYNC_DATABASE_URL.",
        )
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--limit", type=int, default=0)

    def handle(self, *args, **options):
        database_url = options["database_url"]
        if not database_url:
            raise CommandError("Falta --database-url o AGENTE_DG_SYNC_DATABASE_URL.")

        with psycopg2.connect(database_url) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                commitments = self._fetch_optional(cursor, "commitments", COMMITMENT_QUERY, options["limit"])
                minutes = self._fetch_optional(cursor, "minute_agreements", MINUTE_QUERY, options["limit"])
                projects = self._fetch_optional(cursor, "minute_projects", PROJECT_QUERY, options["limit"])
                steps = self._fetch_optional(cursor, "minute_project_steps", PROJECT_STEPS_QUERY, 0)
                participants = self._fetch_optional(
                    cursor,
                    "minute_project_step_participants",
                    PROJECT_PARTICIPANTS_QUERY,
                    0,
                )

        project_steps = {}
        for step in steps:
            project_steps.setdefault(step["project_id"], []).append(step)
        project_participants = self._build_project_participants(steps, participants)

        counters = {"created": 0, "updated": 0, "skipped": 0}
        with transaction.atomic():
            for row in commitments:
                self._upsert_item(row, "commitments", SeguimientoItem.TIPO_COMPROMISO, counters)
            for row in minutes:
                self._upsert_item(row, "minute_agreements", SeguimientoItem.TIPO_MINUTA, counters)
            for row in projects:
                self._upsert_item(
                    row,
                    "minute_projects",
                    SeguimientoItem.TIPO_PROYECTO,
                    counters,
                    checklist=[
                        {
                            "titulo": step["title"],
                            "descripcion": step.get("description") or step.get("deliverable_text") or "",
                            "completado": _status_agente_a_erp(step.get("status")) == SeguimientoItem.ESTATUS_COMPLETADO,
                        }
                        for step in project_steps.get(row["id"], [])
                    ],
                    participants=project_participants.get(row["id"], []),
                )
            if options["dry_run"]:
                transaction.set_rollback(True)

        suffix = " (dry-run, sin guardar)" if options["dry_run"] else ""
        self.stdout.write(self.style.SUCCESS(f"created={counters['created']} updated={counters['updated']} skipped={counters['skipped']}{suffix}"))

    def _fetch(self, cursor, query: str, limit: int):
        cursor.execute(f"{query} LIMIT %s" if limit else query, [limit] if limit else None)
        return list(cursor.fetchall())

    def _fetch_optional(self, cursor, table_name: str, query: str, limit: int):
        cursor.execute("SELECT to_regclass(%s)", [f"public.{table_name}"])
        if not cursor.fetchone()["to_regclass"]:
            self.stdout.write(self.style.WARNING(f"Tabla ausente en Agente DG: {table_name}; se omite."))
            return []
        return self._fetch(cursor, query, limit)

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
            "fecha_limite": _as_datetime(row.get("due_at") or row.get("target_date") or row.get("due_date"), row.get("due_time")),
            "estatus": _status_agente_a_erp(row.get("status")),
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
            checklist_payload = [{"titulo": title, "descripcion": "", "completado": False} for title in _checklist_from_json(row.get("checklist_items_json"))]
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
