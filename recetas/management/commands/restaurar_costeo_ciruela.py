from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from maestros.models import UnidadMedida
from recetas.models import Receta
from recetas.utils.costeo_semanal import snapshot_weekly_costs
from recetas.utils.costeo_versionado import asegurar_version_costeo
from recetas.utils.derived_insumos import sync_preparacion_insumo
from recetas.utils.rendimientos_protegidos import RENDIMIENTOS_PROTEGIDOS_CIRUELA


PREPARATION_ORDER = ("01CC07", "03JUC64", "02MC08")
FINAL_RECIPE_CODES = ("0113", "0112", "0111", "0114")
VERSION_SOURCE = "RESTAURA_RENDIMIENTO_CIRUELA"


class Command(BaseCommand):
    help = "Restaura rendimientos protegidos de ciruela y refresca costos/versiones/snapshots relacionados."

    def add_arguments(self, parser):
        parser.add_argument(
            "--anchor-date",
            default="",
            help="Fecha YYYY-MM-DD para actualizar el snapshot semanal. Default: hoy.",
        )
        parser.add_argument(
            "--historical-period",
            default="",
            help="Mes YYYY-MM para reconstruir costeo historico mensual. Default: mes actual.",
        )
        parser.add_argument(
            "--skip-historical",
            action="store_true",
            help="No refrescar filas historicas mensuales de las recetas afectadas.",
        )

    def handle(self, *args, **options):
        anchor_date = self._parse_date(options.get("anchor_date") or "")
        historical_period = self._parse_period(options.get("historical_period") or "")
        if historical_period is None:
            today = date.today()
            historical_period = date(today.year, today.month, 1)

        protected_by_code = {item.codigo_point.upper(): item for item in RENDIMIENTOS_PROTEGIDOS_CIRUELA}
        target_codes = tuple(PREPARATION_ORDER) + tuple(FINAL_RECIPE_CODES)
        recetas_by_code = {
            (receta.codigo_point or "").strip().upper(): receta
            for receta in Receta.objects.filter(codigo_point__in=target_codes).select_related("rendimiento_unidad")
        }
        missing = sorted(set(target_codes) - set(recetas_by_code))
        if missing:
            raise CommandError(f"No se encontraron recetas por codigo Point: {', '.join(missing)}")

        updated_yields: list[dict[str, object]] = []
        with transaction.atomic():
            for code in PREPARATION_ORDER:
                receta = Receta.objects.select_for_update().get(pk=recetas_by_code[code].pk)
                protected = protected_by_code[code]
                unidad = UnidadMedida.objects.get(codigo=protected.unidad_codigo)
                before = {
                    "cantidad": str(receta.rendimiento_cantidad or ""),
                    "unidad": receta.rendimiento_unidad.codigo if receta.rendimiento_unidad_id else "",
                }
                receta.rendimiento_cantidad = protected.cantidad
                receta.rendimiento_unidad = unidad
                receta.save(update_fields=["rendimiento_cantidad", "rendimiento_unidad"])
                updated_yields.append(
                    {
                        "receta_id": receta.id,
                        "codigo_point": code,
                        "nombre": receta.nombre,
                        "before": before,
                        "after": {
                            "cantidad": str(protected.cantidad),
                            "unidad": protected.unidad_codigo,
                        },
                    }
                )

        derived_sync: list[dict[str, object]] = []
        for code in PREPARATION_ORDER:
            receta = Receta.objects.get(pk=recetas_by_code[code].pk)
            stats = sync_preparacion_insumo(receta)
            derived_sync.append(
                {
                    "receta_id": receta.id,
                    "codigo_point": code,
                    "costos_creados": stats.costos_creados,
                    "insumos_actualizados": stats.insumos_actualizados,
                }
            )

        version_rows: list[dict[str, object]] = []
        ordered_codes = tuple(PREPARATION_ORDER) + tuple(FINAL_RECIPE_CODES)
        for code in ordered_codes:
            receta = Receta.objects.get(pk=recetas_by_code[code].pk)
            version, created = asegurar_version_costeo(receta, fuente=VERSION_SOURCE)
            version_rows.append(
                {
                    "receta_id": receta.id,
                    "codigo_point": code,
                    "nombre": receta.nombre,
                    "version": version.version_num,
                    "created": created,
                    "costo_total": str(version.costo_total),
                    "costo_por_unidad_rendimiento": str(version.costo_por_unidad_rendimiento or ""),
                }
            )

        receta_ids = [recetas_by_code[code].id for code in ordered_codes]
        weekly = snapshot_weekly_costs(anchor_date=anchor_date, receta_ids=receta_ids, include_addons=False)

        historical_payload = None
        if not options.get("skip_historical"):
            historical_payload = self._sync_target_historical_rows(
                period_start=historical_period,
                receta_ids=receta_ids,
            )

        payload = {
            "updated_yields": updated_yields,
            "derived_sync": derived_sync,
            "versions": version_rows,
            "weekly_snapshot": {
                "week_start": weekly.week_start.isoformat(),
                "week_end": weekly.week_end.isoformat(),
                "recipes_created": weekly.recipes_created,
                "recipes_updated": weekly.recipes_updated,
                "total_items": weekly.total_items,
            },
            "historical_snapshot": historical_payload,
        }
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2))

    def _parse_date(self, value: str) -> date | None:
        raw = (value or "").strip()
        if not raw:
            return None
        try:
            return date.fromisoformat(raw)
        except ValueError as exc:
            raise CommandError("anchor-date debe venir en formato YYYY-MM-DD.") from exc

    def _parse_period(self, value: str) -> date | None:
        raw = (value or "").strip()
        if not raw:
            return None
        try:
            year, month = raw.split("-", 1)
            return date(int(year), int(month), 1)
        except (TypeError, ValueError) as exc:
            raise CommandError("historical-period debe venir en formato YYYY-MM.") from exc

    def _sync_target_historical_rows(self, *, period_start: date, receta_ids: list[int]) -> dict[str, object]:
        from reportes.models import RecetaCostoHistoricoMensual

        rows: list[dict[str, object]] = []
        for receta in Receta.objects.filter(id__in=receta_ids).select_related("rendimiento_unidad").order_by("id"):
            lineas_totales = receta.lineas.exclude(tipo_linea="SUBSECCION").count()
            row, _created = RecetaCostoHistoricoMensual.objects.update_or_create(
                periodo=period_start,
                receta=receta,
                defaults={
                    "costo_total": receta.costo_total_estimado_decimal,
                    "costo_por_unidad_rendimiento": receta.costo_por_unidad_rendimiento,
                    "lineas_costeadas": lineas_totales,
                    "lineas_totales": lineas_totales,
                    "coverage_pct": Decimal("100.000000") if lineas_totales else Decimal("0.000000"),
                    "metadata": {
                        "source": VERSION_SOURCE,
                        "bom_basis": "CURRENT_RECIPE_STRUCTURE_TARGETED_RESTORE",
                        "rendimiento_cantidad": str(receta.rendimiento_cantidad or ""),
                        "rendimiento_unidad": receta.rendimiento_unidad.codigo if receta.rendimiento_unidad_id else "",
                    },
                },
            )
            rows.append(
                {
                    "receta_id": receta.id,
                    "nombre": receta.nombre,
                    "costo_total": str(row.costo_total),
                    "costo_por_unidad_rendimiento": str(row.costo_por_unidad_rendimiento or ""),
                }
            )
        return {
            "period_start": period_start.isoformat(),
            "receta_rows_updated": len(rows),
            "rows": rows,
        }
