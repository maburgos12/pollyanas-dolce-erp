from django.core.management.base import BaseCommand
from django.db.models import Q

from recetas.models import LineaReceta, Receta
from recetas.utils.matching import clasificar_match, match_insumo
from recetas.utils.template_loader import (
    _get_or_create_component_insumo,
    _latest_cost_by_insumo,
    _should_autocreate_component,
    _unit_from_text,
)


class Command(BaseCommand):
    help = (
        "Reintenta matching de líneas de receta en estado REJECTED/NO_MATCH "
        "usando el motor vigente (alias/exact/contains/fuzzy)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Aplica cambios. Sin esta bandera corre en dry-run.",
        )
        parser.add_argument(
            "--receta",
            type=str,
            default="",
            help="Filtra por nombre de receta (contains).",
        )
        parser.add_argument(
            "--include-needs-review",
            action="store_true",
            help="Incluye también líneas en NEEDS_REVIEW (default: solo REJECTED/NO_MATCH).",
        )

    def handle(self, *args, **options):
        qs = LineaReceta.objects.select_related("receta", "insumo", "unidad").order_by("receta__nombre", "posicion")
        receta_filter = (options.get("receta") or "").strip()
        if receta_filter:
            qs = qs.filter(receta__nombre__icontains=receta_filter)

        if options.get("include_needs_review"):
            qs = qs.filter(
                Q(match_status=LineaReceta.STATUS_REJECTED)
                | Q(match_status=LineaReceta.STATUS_NEEDS_REVIEW)
                | Q(match_method=LineaReceta.MATCH_NONE)
                | Q(insumo__isnull=True)
            )
        else:
            qs = qs.filter(
                Q(match_status=LineaReceta.STATUS_REJECTED)
                | Q(match_method=LineaReceta.MATCH_NONE)
                | Q(insumo__isnull=True)
            )

        total = qs.count()
        updates = []
        promoted_auto = 0
        promoted_review = 0
        still_rejected = 0
        autocreated_components = 0
        touched_snapshots = 0

        for linea in qs.iterator():
            ingrediente = (linea.insumo_texto or "").strip()
            if not ingrediente:
                still_rejected += 1
                continue

            insumo, score, method = match_insumo(ingrediente)
            status = clasificar_match(score)

            if insumo is None and _should_autocreate_component(
                recipe_type=linea.receta.tipo,
                tipo_linea=linea.tipo_linea,
                ingrediente=ingrediente,
                costo_linea_value=linea.costo_linea_excel,
            ):
                unidad = linea.unidad or _unit_from_text(linea.unidad_texto)
                before_exists = bool(linea.insumo_id)
                insumo = _get_or_create_component_insumo(ingrediente, unidad)
                score = 100.0
                method = "AUTO_COMPONENTE"
                status = LineaReceta.STATUS_AUTO
                if not before_exists:
                    autocreated_components += 1

            if linea.tipo_linea == LineaReceta.TIPO_SUBSECCION and insumo is None:
                score = 100.0
                method = LineaReceta.MATCH_SUBSECTION
                status = LineaReceta.STATUS_AUTO

            if status == LineaReceta.STATUS_AUTO:
                promoted_auto += 1
            elif status == LineaReceta.STATUS_NEEDS_REVIEW:
                promoted_review += 1
            else:
                still_rejected += 1

            next_insumo = insumo if status != LineaReceta.STATUS_REJECTED else None
            next_snapshot = linea.costo_unitario_snapshot
            if next_insumo and (next_snapshot is None or next_snapshot <= 0):
                latest = _latest_cost_by_insumo(next_insumo.id)
                if latest and latest > 0:
                    next_snapshot = latest
                    touched_snapshots += 1

            updates.append(
                (
                    linea,
                    next_insumo,
                    score,
                    method,
                    status,
                    next_snapshot,
                )
            )

        self.stdout.write("Rematch de líneas de receta")
        self.stdout.write(f"  - líneas evaluadas: {total}")
        self.stdout.write(f"  - auto aprobadas: {promoted_auto}")
        self.stdout.write(f"  - quedan en revisión: {promoted_review}")
        self.stdout.write(f"  - siguen rechazadas: {still_rejected}")
        self.stdout.write(f"  - componentes auto-creados: {autocreated_components}")
        self.stdout.write(f"  - snapshots completados por costo base: {touched_snapshots}")

        if updates:
            self.stdout.write("  - muestra:")
            for linea, next_insumo, score, method, status, _ in updates[:15]:
                insumo_label = next_insumo.nombre if next_insumo else "None"
                self.stdout.write(
                    f"    * {linea.receta.nombre} | pos={linea.posicion} | {linea.insumo_texto} -> "
                    f"{insumo_label} | {method} {score:.1f} | {status}"
                )

        if not options["apply"]:
            self.stdout.write("Dry-run: no se actualizaron líneas. Usa --apply para confirmar.")
            return

        applied = 0
        for linea, next_insumo, score, method, status, snapshot in updates:
            linea.insumo = next_insumo
            linea.match_score = score
            linea.match_method = method
            linea.match_status = status
            if snapshot is not None and snapshot > 0:
                linea.costo_unitario_snapshot = snapshot
                linea.save(
                    update_fields=[
                        "insumo",
                        "match_score",
                        "match_method",
                        "match_status",
                        "costo_unitario_snapshot",
                    ]
                )
            else:
                linea.save(update_fields=["insumo", "match_score", "match_method", "match_status"])
            applied += 1

        # Regenera versiones de costeo solo para recetas tocadas (idempotente).
        touched_recipe_ids = {linea.receta_id for linea, *_ in updates}
        if touched_recipe_ids:
            from recetas.utils.costeo_versionado import asegurar_version_costeo

            for receta in Receta.objects.filter(id__in=touched_recipe_ids):
                asegurar_version_costeo(receta, fuente="REMATCH_LINEAS")

        self.stdout.write(self.style.SUCCESS(f"Líneas actualizadas: {applied}"))
