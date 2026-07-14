"""Siembra las reglas rubro→fuente para consolidar el gasto/ingreso real.

Idempotente: solo administra reglas con origen=SEED; nunca toca las de
origen=ADMIN (si un rubro tiene regla ADMIN, el seed lo respeta y lo omite).
El mapeo vive en ``reportes/data/mapeo_rubros_fuentes.csv``; los ingresos de
Ventas se generan programáticamente desde el concepto "CATEGORÍA · PRODUCTO".
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from reportes.models import CategoriaGasto, ReglaFuenteRubro, RubroPresupuesto
from reportes.services_presupuesto_maestro import normalize_header_text

CSV_DEFAULT = Path(__file__).resolve().parents[2] / "data" / "mapeo_rubros_fuentes.csv"
TIPOS_VALIDOS = {choice[0] for choice in ReglaFuenteRubro.FUENTE_CHOICES}


class Command(BaseCommand):
    help = "Crea/actualiza reglas de fuente por rubro desde el CSV de mapeo."

    def add_arguments(self, parser):
        parser.add_argument("--csv", default=str(CSV_DEFAULT))
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument(
            "--sin-ventas",
            action="store_true",
            help="No generar reglas VENTA_POS para los rubros de ingresos de Ventas.",
        )

    def handle(self, *args, **options):
        csv_path = Path(options["csv"])
        if not csv_path.exists():
            raise CommandError(f"No existe el CSV de mapeo: {csv_path}")
        dry_run = options["dry_run"]

        planes: dict[int, list[dict]] = {}  # rubro_id -> [kwargs de regla]
        avisos: list[str] = []

        # --- filas del CSV -------------------------------------------------
        with csv_path.open(newline="", encoding="utf-8") as fh:
            for idx, row in enumerate(csv.DictReader(fh), start=2):
                area = (row.get("area") or "").strip()
                concepto = normalize_header_text(row.get("concepto") or "")
                tipo = (row.get("tipo_fuente") or "").strip().upper()
                if tipo not in TIPOS_VALIDOS:
                    raise CommandError(f"fila {idx}: tipo_fuente inválido '{tipo}'")
                categoria = None
                if (row.get("categoria_gasto") or "").strip():
                    codigo = row["categoria_gasto"].strip()
                    categoria = CategoriaGasto.objects.filter(codigo=codigo).first()
                    if categoria is None:
                        avisos.append(f"fila {idx}: categoria_gasto '{codigo}' no existe; fila omitida")
                        continue
                try:
                    filtros = json.loads(row["filtros"]) if (row.get("filtros") or "").strip() else {}
                except json.JSONDecodeError as exc:
                    raise CommandError(f"fila {idx}: filtros JSON inválido: {exc}")

                rubros = [
                    r
                    for r in RubroPresupuesto.objects.filter(area__codigo=area, activo=True)
                    if normalize_header_text(r.concepto) == concepto
                ]
                if not rubros:
                    avisos.append(f"fila {idx}: sin rubros para area={area} concepto='{row.get('concepto')}'")
                    continue
                for rubro in rubros:
                    planes.setdefault(rubro.id, []).append(
                        {
                            "tipo_fuente": tipo,
                            "categoria_gasto": categoria,
                            "filtros": filtros,
                            "notas": (row.get("notas") or "").strip()[:200],
                        }
                    )

        # --- ingresos de Ventas: CATEGORÍA · PRODUCTO ----------------------
        if not options["sin_ventas"]:
            ventas = RubroPresupuesto.objects.filter(
                area__codigo="ventas", tipo=RubroPresupuesto.TIPO_INGRESO, activo=True
            ).select_related("sucursal")
            for rubro in ventas:
                partes = [p.strip() for p in rubro.concepto.split("·")]
                filtros = {"categoria_pos": partes[0], "campo_monto": "total_venta"}
                if len(partes) > 1 and partes[1]:
                    filtros["producto_pos"] = partes[1]
                planes.setdefault(rubro.id, []).append(
                    {
                        "tipo_fuente": ReglaFuenteRubro.FUENTE_VENTA_POS,
                        "categoria_gasto": None,
                        "filtros": filtros,
                        "notas": "Ventas POS por categoría/producto",
                    }
                )

        # --- aplicar --------------------------------------------------------
        creadas = 0
        omitidos_admin = 0
        rubros_con_admin = set(
            ReglaFuenteRubro.objects.filter(
                rubro_id__in=planes.keys(), origen=ReglaFuenteRubro.ORIGEN_ADMIN
            ).values_list("rubro_id", flat=True)
        )
        with transaction.atomic():
            for rubro_id, reglas in planes.items():
                if rubro_id in rubros_con_admin:
                    omitidos_admin += 1
                    continue
                if not dry_run:
                    ReglaFuenteRubro.objects.filter(
                        rubro_id=rubro_id, origen=ReglaFuenteRubro.ORIGEN_SEED
                    ).delete()
                    for kwargs in reglas:
                        ReglaFuenteRubro.objects.create(
                            rubro_id=rubro_id, origen=ReglaFuenteRubro.ORIGEN_SEED, **kwargs
                        )
                creadas += len(reglas)

            # Reconciliación: reglas SEED de rubros que salieron del mapeo se
            # eliminan para que la base converja al estado declarado en el CSV.
            # No toca reglas ADMIN ni rubros con regla ADMIN (ahí manda admin).
            obsoletas_qs = ReglaFuenteRubro.objects.filter(
                origen=ReglaFuenteRubro.ORIGEN_SEED
            ).exclude(rubro_id__in=planes.keys()).exclude(rubro_id__in=rubros_con_admin)
            if options["sin_ventas"]:
                # Corrida parcial: las reglas de Ventas las administra la corrida completa.
                obsoletas_qs = obsoletas_qs.exclude(rubro__area__codigo="ventas")
            obsoletas = obsoletas_qs.count()
            if not dry_run and obsoletas:
                obsoletas_qs.delete()
            if dry_run:
                transaction.set_rollback(True)

        # --- reporte de cobertura -------------------------------------------
        total = RubroPresupuesto.objects.filter(activo=True).count()
        con_regla = len(planes) - omitidos_admin + len(rubros_con_admin)
        modo = "DRY-RUN" if dry_run else "APLICADO"
        self.stdout.write(f"[{modo}] reglas: {creadas} en {len(planes)} rubros "
                          f"(admin respetados: {omitidos_admin}, seed obsoletas eliminadas: {obsoletas})")
        self.stdout.write(f"Cobertura: {con_regla}/{total} rubros activos con regla")
        for area_codigo in (
            RubroPresupuesto.objects.filter(activo=True)
            .values_list("area__codigo", flat=True)
            .distinct()
        ):
            area_ids = set(
                RubroPresupuesto.objects.filter(activo=True, area__codigo=area_codigo).values_list(
                    "id", flat=True
                )
            )
            self.stdout.write(f"  {area_codigo}: {len(area_ids & set(planes))}/{len(area_ids)}")
        for aviso in avisos:
            self.stdout.write(self.style.WARNING(f"AVISO {aviso}"))
