from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Count, Sum

from reportes.models import LineaPresupuestoMensual, RubroPresupuesto
from reportes.services_presupuesto_maestro import is_totalizer_budget_concept


class Command(BaseCommand):
    help = "Detecta y elimina rubros totalizadores del Presupuesto Maestro."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Muestra qué borraría sin modificar datos.")
        parser.add_argument("--ejecutar", action="store_true", help="Ejecuta el borrado de rubros y líneas.")

    def handle(self, *args, **options):
        ejecutar = bool(options.get("ejecutar"))
        dry_run = bool(options.get("dry_run")) or not ejecutar

        candidates = []
        qs = RubroPresupuesto.objects.select_related("area").annotate(
            line_count=Count("lineas_mensuales"),
            total_presupuesto=Sum("lineas_mensuales__monto_presupuesto"),
        )
        for rubro in qs.order_by("area__codigo", "concepto", "id"):
            total = rubro.total_presupuesto or 0
            is_empty = rubro.line_count == 0 or total == 0
            is_totalizer = is_totalizer_budget_concept(rubro.concepto, area_code=rubro.area.codigo)
            if is_totalizer or is_empty:
                candidates.append(rubro)

        line_count = sum(int(rubro.line_count or 0) for rubro in candidates)

        self.stdout.write(f"Modo: {'DRY-RUN' if dry_run else 'EJECUCIÓN'}")
        self.stdout.write(f"Rubros candidatos: {len(candidates)}")
        self.stdout.write(f"Líneas candidatas: {line_count}")
        for rubro in candidates[:80]:
            self.stdout.write(
                f"- {rubro.id} | {rubro.area.codigo} | {rubro.concepto} | líneas={rubro.line_count} | total={rubro.total_presupuesto or 0}"
            )
        if len(candidates) > 80:
            self.stdout.write(f"... {len(candidates) - 80} rubro(s) más")

        if dry_run:
            self.stdout.write(self.style.WARNING("No se modificaron datos. Usa --ejecutar para borrar."))
            return

        candidate_ids = [rubro.id for rubro in candidates]
        with transaction.atomic():
            deleted_lines, _ = LineaPresupuestoMensual.objects.filter(rubro_id__in=candidate_ids).delete()
            deleted_rubros, _ = RubroPresupuesto.objects.filter(id__in=candidate_ids).delete()

        self.stdout.write(self.style.SUCCESS("Limpieza ejecutada"))
        self.stdout.write(f"Rubros eliminados: {deleted_rubros}")
        self.stdout.write(f"Líneas eliminadas: {deleted_lines}")
