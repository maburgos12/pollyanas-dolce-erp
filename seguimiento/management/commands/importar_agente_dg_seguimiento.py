from __future__ import annotations

import json
import os

import psycopg2
from psycopg2.extras import RealDictCursor
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from seguimiento.models import SeguimientoItem
from seguimiento.services import (
    AgenteDGSeguimientoImporter,
    agente_dg_as_datetime as _as_datetime,
    agente_dg_checklist_from_json as _checklist_from_json,
    agente_dg_status_a_erp as _status_agente_a_erp,
)


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
        m.archived_at,
        m.due_at,
        m.collaborator_user_id AS user_id,
        u.email AS user_email,
        u.name AS user_name,
        m.meeting_label AS area_name
    FROM minute_agreements m
    LEFT JOIN users u ON u.id = m.collaborator_user_id
    WHERE
        m.archived_at IS NULL
        OR UPPER(COALESCE(m.status::text, '')) IN ('COMPLETED', 'CLOSED', 'CANCELLED', 'CANCELED')
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
        s.step_type,
        s.priority,
        s.requires_approval,
        s.due_at,
        s.checklist_items_json,
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


class Command(AgenteDGSeguimientoImporter, BaseCommand):
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
                            "origen_step_id": step.get("id"),
                            "descripcion": step.get("description") or "",
                            "completado": _status_agente_a_erp(step.get("status")) == SeguimientoItem.ESTATUS_COMPLETADO,
                            "entregable": step.get("deliverable_text") or "",
                            "responsable_nombre": step.get("owner_user_name") or "",
                            "aprobador_nombre": step.get("approver_user_name") or "",
                            # Email del aprobador para resolución exacta contra usuarios del ERP
                            "aprobador_email": step.get("approver_user_email") or "",
                            "requiere_aprobacion": bool(step.get("requires_approval")),
                            "vence": step.get("due_at"),
                            "prioridad": step.get("priority") or "",
                            "tipo": str(step.get("step_type") or ""),
                            "estatus_origen": str(step.get("status") or ""),
                            "checklist_items_json": step.get("checklist_items_json") or "",
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
