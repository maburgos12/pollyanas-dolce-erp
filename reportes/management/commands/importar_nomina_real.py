from __future__ import annotations

import csv
import unicodedata
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from reportes.models import CategoriaGasto, CentroCosto, GastoOperativoMensual
from reportes.services_operating_finance import OperatingFinanceBootstrapService


AREA_CENTRO_COSTO = {
    "produccion": "PROD",
    "producción": "PROD",
    "prod": "PROD",
}


def _parse_period(value: str) -> date:
    raw = str(value or "").strip()
    try:
        if len(raw) == 7:
            year, month = raw.split("-", 1)
            return date(int(year), int(month), 1)
        parsed = date.fromisoformat(raw)
        return date(parsed.year, parsed.month, 1)
    except ValueError as exc:
        raise CommandError(f"Periodo inválido '{value}'. Usa YYYY-MM.") from exc


def _money(value: str) -> Decimal:
    raw = str(value or "").strip().replace("$", "").replace(",", "")
    if not raw:
        return Decimal("0")
    try:
        return Decimal(raw).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError) as exc:
        raise CommandError(f"Monto inválido '{value}'.") from exc


def _key(value: str) -> str:
    raw = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    without_accents = "".join(char for char in raw if not unicodedata.combining(char))
    return "_".join(without_accents.split())[:60]


class Command(BaseCommand):
    help = "Importa nómina real mensual desde CSV: area, periodo, concepto, monto."

    def add_arguments(self, parser):
        parser.add_argument("--archivo", required=True, help="CSV con columnas area, periodo, concepto, monto.")
        parser.add_argument("--dry-run", action="store_true", help="Valida y resume sin persistir.")

    def handle(self, *args, **options):
        path = Path(options["archivo"]).expanduser().resolve()
        dry_run = bool(options.get("dry_run"))
        if not path.exists():
            raise CommandError(f"No existe archivo: {path}")

        OperatingFinanceBootstrapService().bootstrap()
        centro_por_codigo = {row.codigo: row for row in CentroCosto.objects.filter(codigo__in=set(AREA_CENTRO_COSTO.values()))}
        categoria = CategoriaGasto.objects.get(codigo="MANO_OBRA_PROD")

        rows = []
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            headers = {str(header or "").strip().lower() for header in (reader.fieldnames or [])}
            required = {"area", "periodo", "concepto", "monto"}
            missing = required - headers
            if missing:
                raise CommandError(f"CSV sin columnas requeridas: {', '.join(sorted(missing))}")
            for index, row in enumerate(reader, start=2):
                area = str(row.get("area") or "").strip().lower()
                centro_codigo = AREA_CENTRO_COSTO.get(area)
                if not centro_codigo:
                    raise CommandError(f"Fila {index}: area no soportada '{row.get('area')}'.")
                period = _parse_period(str(row.get("periodo") or ""))
                concept = str(row.get("concepto") or "").strip()
                amount = _money(str(row.get("monto") or ""))
                external_key = f"NOMINA_REAL|{centro_codigo}|{period.isoformat()}|{_key(concept) or index}"
                rows.append(
                    {
                        "external_key": external_key,
                        "periodo": period,
                        "centro_costo": centro_por_codigo[centro_codigo],
                        "categoria_gasto": categoria,
                        "monto": amount,
                        "tipo_dato": GastoOperativoMensual.TIPO_DATO_REAL,
                        "fuente": GastoOperativoMensual.FUENTE_IMPORTADA,
                        "es_estimado": False,
                        "comentario": concept,
                        "archivo_soporte": str(path),
                    }
                )

        self.stdout.write(f"Archivo: {path.name}")
        self.stdout.write(f"Filas válidas: {len(rows)}")
        self.stdout.write(f"Total: ${sum((row['monto'] for row in rows), Decimal('0')):,.2f}")
        if dry_run:
            self.stdout.write(self.style.WARNING("Dry-run: no se modificaron datos."))
            return

        created = 0
        updated = 0
        with transaction.atomic():
            for row in rows:
                _, was_created = GastoOperativoMensual.objects.update_or_create(
                    external_key=row["external_key"],
                    defaults=row,
                )
                created += int(was_created)
                updated += int(not was_created)
        self.stdout.write(self.style.SUCCESS(f"Nómina real importada: created={created} updated={updated}"))
