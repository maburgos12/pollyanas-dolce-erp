from django.core.management.base import BaseCommand
from django.db.models import Q

from recetas.models import LineaReceta, Receta
from recetas.utils.matching import clasificar_match, match_insumo
from recetas.utils.template_loader import (
    _get_or_create_component_insumo,
    _latest_cost_by_insumos,
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
        parser.add_argument(
            "--limit",
            type=int,
            default=500,
            help="Máximo de líneas a evaluar por corrida (default: 500).",
        )
        parser.add_argument(
            "--offset",
            type=int,
            default=0,
            help="Offset de líneas para paginar corridas (default: 0).",
        )
        parser.add_argument(
            "--progress-every",
            type=int,
            default=100,
            help="Imprime progreso cada N líneas evaluadas (default: 100).",
        )

    def handle(self, *args, **options):
        qs = LineaReceta.objects.select_related("receta", "insumo", "unidad").order_by("receta__nombre", "posicion", "id")
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

        total_universe = qs.count()
        limit = max(1, int(options.get("limit") or 500))
        offset = max(0, int(options.get("offset") or 0))
        progress_every = max(1, int(options.get("progress_every") or 100))
        qs = qs[offset : offset + limit]
        total = qs.count()
        updates = []
        promoted_auto = 0
        promoted_review = 0
        still_rejected = 0
        autocreated_components = 0
        touched_snapshots = 0

        evaluated = 0
        for linea in qs.iterator(chunk_size=200):
            evaluated += 1
            if evaluated % progress_every == 0:
                self.stdout.write(f"  ...progreso: {evaluated}/{total}")
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
            updates.append(
                (
                    linea,
                    next_insumo,
                    score,
                    method,
                    status,
                    linea.costo_unitario_snapshot,
                )
            )

        # Completa snapshots faltantes en lote para evitar 1 query por línea.
        insumo_ids_for_snapshot = {
            next_insumo.id
            for _, next_insumo, _, _, status, snapshot in updates
            if status != LineaReceta.STATUS_REJECTED and next_insumo and (snapshot is None or snapshot <= 0)
        }
        latest_costs = _latest_cost_by_insumos(insumo_ids_for_snapshot)
        hydrated_updates = []
        for linea, next_insumo, score, method, status, snapshot in updates:
            next_snapshot = snapshot
            if (
                status != LineaReceta.STATUS_REJECTED
                and next_insumo
                and (next_snapshot is None or next_snapshot <= 0)
            ):
                latest = latest_costs.get(next_insumo.id)
                if latest and latest > 0:
                    next_snapshot = latest
                    touched_snapshots += 1
            hydrated_updates.append((linea, next_insumo, score, method, status, next_snapshot))
        updates = hydrated_updates

        self.stdout.write("Rematch de líneas de receta")
        self.stdout.write(f"  - universo filtrado: {total_universe}")
        self.stdout.write(f"  - página evaluada: offset={offset}, limit={limit}, total={total}")
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
        changed_lines = []
        for linea, next_insumo, score, method, status, snapshot in updates:
            changed = False
            if linea.insumo_id != (next_insumo.id if next_insumo else None):
                linea.insumo = next_insumo
                changed = True
            if (linea.match_score or 0) != (score or 0):
                linea.match_score = score
                changed = True
            if (linea.match_method or "") != (method or ""):
                linea.match_method = method
                changed = True
            if (linea.match_status or "") != (status or ""):
                linea.match_status = status
                changed = True
            if snapshot is not None and snapshot > 0 and (linea.costo_unitario_snapshot or 0) != snapshot:
                linea.costo_unitario_snapshot = snapshot
                changed = True
            if changed:
                changed_lines.append(linea)
                applied += 1

        if changed_lines:
            LineaReceta.objects.bulk_update(
                changed_lines,
                ["insumo", "match_score", "match_method", "match_status", "costo_unitario_snapshot"],
                batch_size=500,
            )

        # Regenera versiones de costeo solo para recetas tocadas (idempotente).
        touched_recipe_ids = {linea.receta_id for linea in changed_lines}
        if touched_recipe_ids:
            from recetas.utils.costeo_versionado import asegurar_version_costeo

            for receta in Receta.objects.filter(id__in=touched_recipe_ids):
                asegurar_version_costeo(receta, fuente="REMATCH_LINEAS")

        self.stdout.write(self.style.SUCCESS(f"Líneas actualizadas: {applied}"))
