from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Count, Q
from django.utils import timezone

from maestros.models import Insumo
from pos_bridge.models import PointRecipeNode
from recetas.models import LineaReceta, Receta, RecetaEquivalencia
from recetas.utils.normalizacion import normalizar_nombre


@dataclass
class LineMapping:
    point_code: str
    point_name: str
    quantity: Decimal | None
    unit_text: str
    insumo: Insumo | None
    method: str


@dataclass
class RecipeMapping:
    receta: Receta
    node: PointRecipeNode | None
    lines_total: int = 0
    mapped: list[LineMapping] = field(default_factory=list)
    unmapped: list[LineMapping] = field(default_factory=list)
    skipped_reason: str = ""


class Command(BaseCommand):
    help = "Une PointRecipeNodeLine con LineaReceta para recetas sin BOM directo."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Muestra qué se crearía sin persistir. Es el default.")
        parser.add_argument("--ejecutar", action="store_true", help="Persiste las LineaReceta mapeadas.")
        parser.add_argument("--receta", default="", help="Nombre o codigo_point específico para probar.")
        parser.add_argument(
            "--solo-vendidas",
            action="store_true",
            help="Procesa solo recetas vendidas en PointDailySale durante los últimos 30 días.",
        )
        parser.add_argument("--days", type=int, default=30, help="Ventana usada con --solo-vendidas.")

    def handle(self, *args, **options):
        dry_run = bool(options["dry_run"])
        ejecutar = bool(options["ejecutar"])
        if dry_run and ejecutar:
            raise CommandError("Usa solo una opción: --dry-run o --ejecutar.")
        if not dry_run and not ejecutar:
            dry_run = True

        days = int(options.get("days") or 30)
        if days <= 0:
            raise CommandError("--days debe ser mayor a cero.")

        receta_filter = (options.get("receta") or "").strip()
        mappings = self._build_mappings(
            receta_filter=receta_filter,
            solo_vendidas=bool(options["solo_vendidas"]),
            days=days,
        )

        created = 0
        with transaction.atomic():
            if ejecutar:
                created = self._persist(mappings)
            if dry_run:
                transaction.set_rollback(True)

        self._print_report(
            mappings=mappings,
            dry_run=dry_run,
            ejecutar=ejecutar,
            solo_vendidas=bool(options["solo_vendidas"]),
            days=days,
            created=created,
        )

    def _build_mappings(self, *, receta_filter: str, solo_vendidas: bool, days: int) -> list[RecipeMapping]:
        queryset = self._candidate_recipes(receta_filter=receta_filter, solo_vendidas=solo_vendidas, days=days)
        mappings = []
        for receta in queryset:
            node = self._latest_node_for_recipe(receta)
            mapping = RecipeMapping(receta=receta, node=node)
            if node is None:
                mapping.skipped_reason = "SIN_NODO_POINT"
                mappings.append(mapping)
                continue

            lines = list(node.lines.select_related("erp_insumo", "unit").order_by("position", "id"))
            mapping.lines_total = len(lines)
            if not lines:
                mapping.skipped_reason = "NODO_SIN_LINEAS"
                mappings.append(mapping)
                continue

            for line in lines:
                insumo, method = self._resolve_insumo(line)
                line_mapping = LineMapping(
                    point_code=(line.point_code or "").strip(),
                    point_name=(line.point_name or "").strip(),
                    quantity=line.quantity,
                    unit_text=(line.unit.codigo if line.unit_id else line.unit_text or "").strip(),
                    insumo=insumo,
                    method=method,
                )
                if insumo is None:
                    mapping.unmapped.append(line_mapping)
                else:
                    mapping.mapped.append(line_mapping)
            mappings.append(mapping)
        return mappings

    def _candidate_recipes(self, *, receta_filter: str, solo_vendidas: bool, days: int):
        queryset = (
            Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL)
            .annotate(nlineas=Count("lineas"))
            .filter(nlineas=0)
            .exclude(codigo_point="")
            .order_by("nombre", "id")
        )
        # Compute ids explicitly because the table is small and the command runs
        # offline; this keeps the business rule readable.
        equivalent_child_ids = [
            eq.receta_porcion_id
            for eq in RecetaEquivalencia.objects.filter(activo=True).only("receta_porcion_id", "receta_padre_id")
            if eq.receta_porcion_id != eq.receta_padre_id
        ]
        if equivalent_child_ids:
            queryset = queryset.exclude(id__in=equivalent_child_ids)
        if receta_filter:
            queryset = queryset.filter(Q(nombre__icontains=receta_filter) | Q(codigo_point__iexact=receta_filter))
        if solo_vendidas:
            cutoff = timezone.localdate() - timedelta(days=days)
            queryset = queryset.filter(point_daily_sales__sale_date__gte=cutoff).distinct()
        return queryset

    def _latest_node_for_recipe(self, receta: Receta) -> PointRecipeNode | None:
        code = (receta.codigo_point or "").strip()
        if not code:
            return None
        node = (
            PointRecipeNode.objects.filter(point_code__iexact=code)
            .select_related("run")
            .prefetch_related("lines")
            .order_by("-run__created_at", "-id")
            .first()
        )
        return node

    def _resolve_insumo(self, line) -> tuple[Insumo | None, str]:
        if line.erp_insumo_id:
            return line.erp_insumo, "POINT_NODE_ERP_INSUMO"

        point_code = (line.point_code or "").strip()
        if point_code:
            insumo = Insumo.objects.filter(codigo_point__iexact=point_code).order_by("id").first()
            if insumo is not None:
                return insumo, "CODIGO_POINT"

        name_norm = normalizar_nombre(line.normalized_name or line.point_name or "")
        if name_norm:
            insumo = Insumo.objects.filter(nombre_normalizado=name_norm).order_by("id").first()
            if insumo is not None:
                return insumo, "NOMBRE_NORMALIZADO"
            insumo = Insumo.objects.filter(nombre_normalizado__icontains=name_norm).order_by("id").first()
            if insumo is not None:
                return insumo, "NOMBRE_CONTIENE"
            for token in [part for part in name_norm.split() if len(part) >= 4][:3]:
                insumo = Insumo.objects.filter(nombre_normalizado__icontains=token).order_by("id").first()
                if insumo is not None:
                    return insumo, f"TOKEN:{token}"

        return None, "NO_MAPEADO"

    def _persist(self, mappings: list[RecipeMapping]) -> int:
        created = 0
        for mapping in mappings:
            if mapping.node is None or not mapping.mapped:
                continue
            for position, line in enumerate(mapping.mapped, start=1):
                _, was_created = LineaReceta.objects.get_or_create(
                    receta=mapping.receta,
                    posicion=position,
                    tipo_linea=LineaReceta.TIPO_NORMAL,
                    defaults={
                        "insumo": line.insumo,
                        "insumo_texto": line.point_name or line.point_code or f"Insumo {position}",
                        "cantidad": line.quantity,
                        "unidad_texto": line.unit_text,
                        "unidad": line.insumo.unidad_base if line.insumo else None,
                        "match_score": 100 if line.method in {"POINT_NODE_ERP_INSUMO", "CODIGO_POINT", "NOMBRE_NORMALIZADO"} else 80,
                        "match_method": LineaReceta.MATCH_EXACT if line.method in {"POINT_NODE_ERP_INSUMO", "CODIGO_POINT", "NOMBRE_NORMALIZADO"} else LineaReceta.MATCH_CONTAINS,
                        "match_status": LineaReceta.STATUS_AUTO if line.method != "NO_MAPEADO" else LineaReceta.STATUS_REJECTED,
                    },
                )
                if was_created:
                    created += 1
        return created

    def _print_report(
        self,
        *,
        mappings: list[RecipeMapping],
        dry_run: bool,
        ejecutar: bool,
        solo_vendidas: bool,
        days: int,
        created: int,
    ) -> None:
        total_mapped = sum(len(mapping.mapped) for mapping in mappings)
        total_unmapped = sum(len(mapping.unmapped) for mapping in mappings)
        sin_nodo = sum(1 for mapping in mappings if mapping.node is None)
        skipped = Counter(mapping.skipped_reason for mapping in mappings if mapping.skipped_reason)

        self.stdout.write(
            f"unir_bom_point_erp · dry_run={dry_run} · ejecutar={ejecutar} · "
            f"solo_vendidas={solo_vendidas} · days={days}"
        )
        self.stdout.write(f"Recetas candidatas: {len(mappings)}")
        self.stdout.write(f"Recetas sin nodo en Point: {sin_nodo}")
        self.stdout.write(f"LineaReceta {'crearía' if dry_run else 'creadas'}: {total_mapped if dry_run else created}")
        self.stdout.write(f"Insumos no mapeados: {total_unmapped}")
        if skipped:
            self.stdout.write("Omisiones:")
            for reason, count in sorted(skipped.items()):
                self.stdout.write(f"  {reason}: {count}")

        for mapping in mappings:
            receta = mapping.receta
            prefix = "[DRY-RUN]" if dry_run else "[EJECUTADO]"
            self.stdout.write("")
            self.stdout.write(f"{prefix} {receta.nombre} (codigo_point: {receta.codigo_point})")
            if mapping.node is None:
                self.stdout.write("  Nodo: NO ENCONTRADO")
                continue
            self.stdout.write(
                f"  Nodo: {mapping.node.id} "
                f"(run {mapping.node.run_id}, {mapping.node.run.created_at:%Y-%m-%d})"
            )
            self.stdout.write(f"  Líneas en Point: {mapping.lines_total}")
            self.stdout.write(f"  Mapearían a LineaReceta: {len(mapping.mapped)}")
            self.stdout.write(f"  Sin mapeo: {len(mapping.unmapped)}")
            for line in mapping.mapped[:20]:
                self.stdout.write(
                    f"    OK {line.point_code or '-'} | {line.point_name} | "
                    f"qty: {line.quantity} {line.unit_text} -> {line.insumo.nombre if line.insumo else '-'} ({line.method})"
                )
            if mapping.unmapped:
                self.stdout.write("  No mapeados:")
                for line in mapping.unmapped:
                    self.stdout.write(
                        f"    - {line.point_code or '-'} | {line.point_name} | qty: {line.quantity} {line.unit_text}"
                    )
