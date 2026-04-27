from __future__ import annotations

import json
from pathlib import Path

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError

from pos_bridge.services.monthly_finished_goods_comparison_service import MonthlyFinishedGoodsComparisonService


class Command(BaseCommand):
    help = (
        "Construye la comparativa mensual de producto terminado comercial. "
        "Primero usa PostgreSQL; si faltan movimientos del mes puede traer ventas/produccion/merma desde Point."
    )

    def add_arguments(self, parser):
        parser.add_argument("--month", help="Mes a procesar en formato YYYY-MM.")
        parser.add_argument("--period", help="Alias de --month en formato YYYY-MM.")
        parser.add_argument(
            "--output-dir",
            default="output/monthly_finished_goods_comparison",
            help="Directorio donde se guardará el XLSX.",
        )
        parser.add_argument(
            "--skip-point-fallback",
            action="store_true",
            help="No intentar extracción desde Point si PostgreSQL no trae filas del mes.",
        )
        parser.add_argument(
            "--rebuild",
            action="store_true",
            help="Reconstruye el cierre aun si ya existe uno para el mes.",
        )
        parser.add_argument(
            "--branch",
            default="",
            help="Filtro opcional de sucursal Point para troubleshooting.",
        )
        parser.add_argument("--actor-username", default="", help="Usuario ERP para registrar la ejecución.")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Previsualiza cobertura y cierre teórico sin persistir ni exportar XLSX.",
        )

    def handle(self, *args, **options):
        actor = None
        actor_username = (options.get("actor_username") or "").strip()
        if actor_username:
            actor = get_user_model().objects.filter(username=actor_username).first()
            if actor is None:
                raise CommandError(f"No existe actor_username '{actor_username}'.")

        try:
            month = (options.get("month") or options.get("period") or "").strip()
            if not month:
                raise CommandError("Usa --month YYYY-MM o --period YYYY-MM.")
            service = MonthlyFinishedGoodsComparisonService()
            if options.get("dry_run"):
                result = service.dry_run(month=month)
            else:
                result = service.run(
                    month=month,
                    output_dir=Path(options["output_dir"]),
                    triggered_by=actor,
                    rebuild=bool(options.get("rebuild")),
                    fallback_to_point=not bool(options.get("skip_point_fallback")),
                    branch_filter=(options.get("branch") or "").strip() or None,
                )
        except ValueError as exc:
            raise CommandError("month debe tener formato YYYY-MM.") from exc
        except Exception as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(json.dumps(result, ensure_ascii=False, indent=2, default=str))
