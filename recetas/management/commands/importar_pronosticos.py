from __future__ import annotations

import csv
from decimal import Decimal
from pathlib import Path
from typing import Any

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from openpyxl import load_workbook

from recetas.models import PronosticoVenta, Receta
from recetas.utils.normalizacion import normalizar_nombre


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    try:
        raw = str(value).strip().replace(",", ".")
        if raw == "":
            return Decimal("0")
        return Decimal(raw)
    except Exception:
        return Decimal("0")


def _normalize_period(raw: Any) -> str:
    txt = str(raw or "").strip()
    if not txt:
        return ""
    txt = txt.replace("/", "-")
    if len(txt) >= 7:
        return txt[:7]
    return txt


def _load_rows(filepath: Path) -> list[dict[str, Any]]:
    suffix = filepath.suffix.lower()
    if suffix == ".csv":
        with filepath.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            return [{(k or "").strip().lower(): v for k, v in row.items()} for row in reader]

    if suffix in {".xlsx", ".xlsm"}:
        wb = load_workbook(filepath, data_only=True, read_only=True)
        ws = wb.active
        rows = list(ws.values)
        if not rows:
            return []
        headers = [str(h or "").strip().lower() for h in rows[0]]
        out: list[dict[str, Any]] = []
        for r in rows[1:]:
            row = {}
            for i, h in enumerate(headers):
                if not h:
                    continue
                row[h] = r[i] if i < len(r) else None
            out.append(row)
        return out

    raise CommandError("Formato no soportado. Usa CSV o XLSX.")


class Command(BaseCommand):
    help = "Importa pronósticos de venta (receta, periodo, cantidad) desde CSV/XLSX"

    def add_arguments(self, parser):
        parser.add_argument("filepath", type=str)
        parser.add_argument("--replace", action="store_true", help="Reemplaza cantidad existente para receta+periodo")
        parser.add_argument("--fuente", type=str, default="IMPORT_PRONOSTICO", help="Etiqueta de fuente")

    @transaction.atomic
    def handle(self, *args, **options):
        filepath = Path(options["filepath"])
        if not filepath.exists():
            raise CommandError(f"No existe archivo: {filepath}")

        rows = _load_rows(filepath)
        if not rows:
            self.stdout.write(self.style.WARNING("Archivo sin filas para importar."))
            return

        created = 0
        updated = 0
        skipped = 0

        for row in rows:
            receta_raw = row.get("receta") or row.get("producto") or row.get("nombre") or ""
            periodo_raw = row.get("periodo") or row.get("mes") or ""
            cantidad_raw = row.get("cantidad") or row.get("pronostico") or row.get("forecast") or 0

            receta_name = str(receta_raw).strip()
            periodo = _normalize_period(periodo_raw)
            cantidad = _to_decimal(cantidad_raw)

            if not receta_name or not periodo:
                skipped += 1
                continue

            receta = (
                Receta.objects.filter(nombre_normalizado=normalizar_nombre(receta_name)).order_by("id").first()
                or Receta.objects.filter(codigo_point=receta_name).order_by("id").first()
            )
            if not receta:
                skipped += 1
                continue

            pronostico = PronosticoVenta.objects.filter(receta=receta, periodo=periodo).first()
            if pronostico:
                if options["replace"]:
                    pronostico.cantidad = cantidad
                else:
                    pronostico.cantidad = Decimal(str(pronostico.cantidad or 0)) + cantidad
                pronostico.fuente = (options["fuente"] or "IMPORT_PRONOSTICO")[:40]
                pronostico.save(update_fields=["cantidad", "fuente", "actualizado_en"])
                updated += 1
            else:
                PronosticoVenta.objects.create(
                    receta=receta,
                    periodo=periodo,
                    cantidad=cantidad,
                    fuente=(options["fuente"] or "IMPORT_PRONOSTICO")[:40],
                )
                created += 1

        self.stdout.write("Importación pronósticos completada")
        self.stdout.write(f"  - filas leídas: {len(rows)}")
        self.stdout.write(f"  - creados: {created}")
        self.stdout.write(f"  - actualizados: {updated}")
        self.stdout.write(f"  - omitidos: {skipped}")
