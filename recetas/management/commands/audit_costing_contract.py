from __future__ import annotations

from collections import Counter
from decimal import Decimal
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection
from django.db.models import OuterRef, Q, Subquery

from maestros.models import CostoInsumo, Insumo
from recetas.models import LineaReceta, Receta, RecetaCostoVersion
from recetas.services.costing_contract import CostContext, resolve_line_cost, resolve_recipe_cost_map


OLD_PROBE_ARTIFACTS = [
    "scripts/cantidad_a_producir.xlsx",
    "scripts/resolver_probe.py",
    "scripts/match_probe_remote.py",
    "storage/insumos_requeridos_costeo_corregido_20260520.xlsx",
    "storage/insumos_requeridos_vps_point_sin_faltantes_20260520.xlsx",
    "storage/test_codex_file.xlsx",
    "storage/rentabilidad_diagnostico_preview.html",
]


class Command(BaseCommand):
    help = "Audita en modo lectura el contrato unico de costeo contra la base ERP/VPS."

    def add_arguments(self, parser):
        parser.add_argument(
            "--show-unresolved",
            action="store_true",
            help="Muestra lineas de receta que siguen sin costo resoluble.",
        )
        parser.add_argument(
            "--show-version-diffs",
            action="store_true",
            help="Muestra diferencias entre ultima version guardada y costo vivo VPS.",
        )

    def handle(self, *args, **options):
        self._print_database_context()
        self._print_core_counts()
        self._print_line_resolution(show_unresolved=bool(options["show_unresolved"]))
        self._print_version_diffs(show_diffs=bool(options["show_version_diffs"]))
        self._print_old_artifacts()

    def _print_database_context(self) -> None:
        db_settings = settings.DATABASES.get("default", {})
        self.stdout.write("DATABASE_CONTEXT")
        self.stdout.write(f"  alias_keys={','.join(sorted(settings.DATABASES.keys()))}")
        self.stdout.write(f"  engine={db_settings.get('ENGINE', '')}")
        self.stdout.write(f"  name={connection.settings_dict.get('NAME', '')}")
        self.stdout.write(f"  host={connection.settings_dict.get('HOST', '')}")
        self.stdout.write(f"  port={connection.settings_dict.get('PORT', '')}")
        if "sqlite" in str(db_settings.get("ENGINE", "")).lower():
            self.stdout.write(self.style.ERROR("  status=INVALID_SQLITE_OPERATIONAL_DB"))
        else:
            self.stdout.write(self.style.SUCCESS("  status=POSTGRES_OPERATIONAL_DB"))

    def _print_core_counts(self) -> None:
        active_lines = self._active_recipe_lines()
        self.stdout.write("CORE_COUNTS")
        self.stdout.write(f"  recetas_total={Receta.objects.count()}")
        self.stdout.write(f"  recetas_final={Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL).count()}")
        self.stdout.write(f"  recetas_preparacion={Receta.objects.filter(tipo=Receta.TIPO_PREPARACION).count()}")
        self.stdout.write(f"  lineas_total={LineaReceta.objects.count()}")
        self.stdout.write(f"  lineas_activas={active_lines.count()}")
        self.stdout.write(
            "  lineas_activas_sin_insumo="
            f"{active_lines.filter(insumo__isnull=True).count()}"
        )
        self.stdout.write(
            "  lineas_activas_insumo_sin_snapshot="
            f"{active_lines.filter(insumo__isnull=False).filter(Q(costo_unitario_snapshot__isnull=True) | Q(costo_unitario_snapshot__lte=0)).count()}"
        )
        self.stdout.write(f"  costo_insumo_rows={CostoInsumo.objects.count()}")
        self.stdout.write(f"  insumos_activos={Insumo.objects.filter(activo=True).count()}")
        self.stdout.write(
            "  insumos_activos_con_costo="
            f"{Insumo.objects.filter(activo=True, costoinsumo__isnull=False).distinct().count()}"
        )
        self.stdout.write(f"  receta_costo_version_rows={RecetaCostoVersion.objects.count()}")
        self.stdout.write(
            "  receta_costo_version_recetas="
            f"{RecetaCostoVersion.objects.values('receta_id').distinct().count()}"
        )
        self.stdout.write(
            "  receta_costo_version_positive_recetas="
            f"{RecetaCostoVersion.objects.filter(costo_total__gt=0).values('receta_id').distinct().count()}"
        )

    def _print_line_resolution(self, *, show_unresolved: bool) -> None:
        source_counts: Counter[str] = Counter()
        unresolved_rows: list[dict[str, object]] = []
        checked = 0
        for line in self._active_recipe_lines().filter(insumo__isnull=False).select_related(
            "receta",
            "insumo",
            "unidad",
            "insumo__unidad_base",
        ).order_by("receta_id", "id"):
            checked += 1
            resolution = resolve_line_cost(line, context=CostContext.CURRENT_LIVE)
            source_counts[resolution.source] += 1
            if resolution.unresolved:
                unresolved_rows.append(
                    {
                        "linea_id": line.id,
                        "receta_id": line.receta_id,
                        "receta": line.receta.nombre if line.receta else "",
                        "insumo_id": line.insumo_id,
                        "insumo": line.insumo.nombre if line.insumo else "",
                        "source": resolution.source,
                        "reason": resolution.unresolved_reason,
                    }
                )

        self.stdout.write("LINE_RESOLUTION")
        self.stdout.write(f"  context={CostContext.CURRENT_LIVE.value}")
        self.stdout.write(f"  checked={checked}")
        self.stdout.write(f"  unresolved={len(unresolved_rows)}")
        self.stdout.write(f"  source_counts={sorted(source_counts.items())}")
        if show_unresolved:
            for row in unresolved_rows:
                self.stdout.write(
                    "  unresolved_line="
                    f"{row['linea_id']}|receta={row['receta_id']} {row['receta']}|"
                    f"insumo={row['insumo_id']} {row['insumo']}|source={row['source']}|reason={row['reason']}"
                )

    def _print_version_diffs(self, *, show_diffs: bool) -> None:
        latest_version_id = (
            RecetaCostoVersion.objects.filter(receta_id=OuterRef("receta_id"))
            .order_by("-version_num", "-creado_en", "-id")
            .values("id")[:1]
        )
        latest_versions = list(
            RecetaCostoVersion.objects.filter(id__in=Subquery(latest_version_id))
            .select_related("receta")
            .filter(receta__isnull=False)
        )
        live_costs = resolve_recipe_cost_map(
            [version.receta_id for version in latest_versions],
            context=CostContext.CURRENT_LIVE,
        )
        diffs: list[dict[str, object]] = []
        for version in latest_versions:
            live = live_costs.get(version.receta_id)
            if live is None or live.unresolved:
                continue
            stored = Decimal(str(version.costo_total or 0)).quantize(Decimal("0.000001"))
            current = live.total_cost.quantize(Decimal("0.000001"))
            if stored != current:
                diffs.append(
                    {
                        "receta_id": version.receta_id,
                        "receta": version.receta.nombre,
                        "version_id": version.id,
                        "stored": stored,
                        "current": current,
                        "source": live.source,
                    }
                )

        self.stdout.write("VERSION_VS_CURRENT_LIVE")
        self.stdout.write(f"  latest_versions={len(latest_versions)}")
        self.stdout.write(f"  diffs={len(diffs)}")
        if show_diffs:
            for row in diffs:
                self.stdout.write(
                    "  diff="
                    f"{row['receta_id']}|{row['receta']}|version={row['version_id']}|"
                    f"stored={row['stored']}|current={row['current']}|source={row['source']}"
                )

    def _print_old_artifacts(self) -> None:
        base_dir = Path(settings.BASE_DIR)
        found = [relative for relative in OLD_PROBE_ARTIFACTS if (base_dir / relative).exists()]
        self.stdout.write("OLD_PROBE_ARTIFACTS")
        self.stdout.write(f"  found={len(found)}")
        for relative in found:
            self.stdout.write(f"  artifact={relative}")
        if found:
            self.stdout.write(
                "  action=report_only_no_delete "
                "cleanup_requires_explicit_approval"
            )

    def _active_recipe_lines(self):
        return LineaReceta.objects.exclude(match_status=LineaReceta.STATUS_REJECTED).exclude(
            tipo_linea=LineaReceta.TIPO_SUBSECCION
        )
