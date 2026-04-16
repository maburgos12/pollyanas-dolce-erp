from __future__ import annotations

import os
from dataclasses import dataclass

from django.conf import settings
from django.core.management import BaseCommand, CommandError
from django.db import connection

from core.management.commands.ejecutar_rutina_diaria_erp import _prefer_public_database_url_if_needed


CRITICAL_TABLES = (
    "pos_bridge_daily_sales",
    "ventas_ventaautoritativapoint",
    "orquestacion_orchestrationrun",
    "recetas_movimientoproductocedis",
)

KNOWN_NON_LIVE_DB_NAMES = {
    "pollyana_db",
}


@dataclass(frozen=True)
class TableStatus:
    name: str
    exists: bool
    count: int | None


def _count_rows(table_name: str) -> int:
    with connection.cursor() as cursor:
        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        return int(cursor.fetchone()[0])


def _collect_table_statuses() -> list[TableStatus]:
    tables = set(connection.introspection.table_names())
    statuses: list[TableStatus] = []
    for table_name in CRITICAL_TABLES:
        exists = table_name in tables
        count = _count_rows(table_name) if exists else None
        statuses.append(TableStatus(name=table_name, exists=exists, count=count))
    return statuses


def _collect_context() -> dict:
    db = settings.DATABASES["default"]
    statuses = _collect_table_statuses()
    table_counts = {status.name: status.count for status in statuses if status.exists and status.count is not None}
    return {
        "engine": str(db.get("ENGINE") or ""),
        "name": str(db.get("NAME") or ""),
        "host": str(db.get("HOST") or ""),
        "port": str(db.get("PORT") or ""),
        "user": str(db.get("USER") or ""),
        "database_url_present": bool(str(os.environ.get("DATABASE_URL") or "").strip()),
        "database_public_url_present": bool(str(os.environ.get("DATABASE_PUBLIC_URL") or "").strip()),
        "table_statuses": statuses,
        "critical_counts": table_counts,
    }


def _evaluate_context(context: dict, *, require_data: bool, required_db_name: str | None) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    engine = context["engine"]
    db_name = context["name"]
    statuses: list[TableStatus] = context["table_statuses"]

    if engine != "django.db.backends.postgresql":
        errors.append("El ERP debe correr sobre PostgreSQL; el backend activo no es PostgreSQL.")

    missing_tables = [status.name for status in statuses if not status.exists]
    if missing_tables:
        errors.append(
            "Faltan tablas críticas del ERP en la base activa: " + ", ".join(missing_tables)
        )

    if required_db_name and db_name != required_db_name:
        errors.append(
            f"La base activa es '{db_name}' y no coincide con la base requerida '{required_db_name}'."
        )

    if db_name in KNOWN_NON_LIVE_DB_NAMES:
        warnings.append(
            f"La base activa '{db_name}' está documentada como local/riesgosa y no debe asumirse como base viva del ERP."
        )

    if require_data:
        zero_or_missing = [
            status.name
            for status in statuses
            if (not status.exists) or status.count in (None, 0)
        ]
        if zero_or_missing:
            errors.append(
                "La base activa no demuestra actividad defendible en capas críticas: "
                + ", ".join(zero_or_missing)
            )

    return errors, warnings


class Command(BaseCommand):
    help = (
        "Diagnostica el contexto runtime del ERP y valida si la base activa es defendible "
        "para trabajo operativo."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--strict",
            action="store_true",
            help="Falla si hay errores o warnings de contexto riesgoso.",
        )
        parser.add_argument(
            "--require-data",
            action="store_true",
            help="Exige evidencia mínima de datos en tablas críticas, no solo esquema.",
        )
        parser.add_argument(
            "--require-db-name",
            default="",
            help="Nombre exacto de la base que debe estar activa.",
        )

    def handle(self, *args, **options):
        db_fallback_msg = _prefer_public_database_url_if_needed()
        context = _collect_context()
        required_db_name = str(options.get("require_db_name") or "").strip() or None
        errors, warnings = _evaluate_context(
            context,
            require_data=bool(options["require_data"]),
            required_db_name=required_db_name,
        )

        self.stdout.write("Diagnóstico rápido de contexto ERP")
        if db_fallback_msg:
            self.stdout.write(f"db_fallback={db_fallback_msg}")
        self.stdout.write(f"ENGINE={context['engine']}")
        self.stdout.write(f"NAME={context['name']}")
        self.stdout.write(f"HOST={context['host']}")
        self.stdout.write(f"PORT={context['port']}")
        self.stdout.write(f"USER={context['user']}")
        self.stdout.write(f"DATABASE_URL={'SET' if context['database_url_present'] else 'EMPTY'}")
        self.stdout.write(
            f"DATABASE_PUBLIC_URL={'SET' if context['database_public_url_present'] else 'EMPTY'}"
        )
        for status in context["table_statuses"]:
            count = "MISSING" if status.count is None else status.count
            self.stdout.write(f"{status.name}|exists={status.exists}|count={count}")

        for warning in warnings:
            self.stdout.write(self.style.WARNING(f"WARNING: {warning}"))
        for error in errors:
            self.stdout.write(self.style.ERROR(f"ERROR: {error}"))

        if errors:
            raise CommandError("Contexto DB del ERP no defendible.")
        if warnings and options["strict"]:
            raise CommandError("Contexto DB del ERP riesgoso en modo estricto.")

        self.stdout.write(self.style.SUCCESS("Contexto DB del ERP diagnosticado."))
