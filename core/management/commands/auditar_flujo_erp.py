import json
from django.core.management.base import BaseCommand
from django.db.models import Count, Q

from compras.models import OrdenCompra, RecepcionCompra, SolicitudCompra
from inventario.models import ExistenciaInsumo, MovimientoInventario
from maestros.models import CostoInsumo, Insumo, InsumoAlias, PointPendingMatch, Proveedor
from recetas.models import LineaReceta, Receta, SolicitudReabastoCedis, SolicitudReabastoCedisLinea


class Command(BaseCommand):
    help = "Audita consistencia global del ERP (catálogos, recetas, inventario, compras y reabasto)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--json",
            action="store_true",
            help="Salida JSON (útil para monitoreo/CI).",
        )

    def handle(self, *args, **options):
        metrics = self._build_metrics()
        if options["json"]:
            self.stdout.write(json.dumps(metrics, ensure_ascii=False, indent=2))
            return

        self.stdout.write(self.style.MIGRATE_HEADING("Auditoría de Flujo ERP"))
        self.stdout.write("  -- Volumen --")
        self.stdout.write(f"    insumos: {metrics['volumen']['insumos']}")
        self.stdout.write(f"    proveedores: {metrics['volumen']['proveedores']}")
        self.stdout.write(f"    recetas: {metrics['volumen']['recetas']}")
        self.stdout.write(f"    lineas_receta: {metrics['volumen']['lineas_receta']}")
        self.stdout.write(f"    existencias: {metrics['volumen']['existencias']}")
        self.stdout.write(f"    movimientos: {metrics['volumen']['movimientos']}")
        self.stdout.write(f"    solicitudes_compra: {metrics['volumen']['solicitudes_compra']}")
        self.stdout.write(f"    ordenes_compra: {metrics['volumen']['ordenes_compra']}")
        self.stdout.write(f"    recepciones: {metrics['volumen']['recepciones']}")
        self.stdout.write(f"    reabasto_solicitudes: {metrics['volumen']['reabasto_solicitudes']}")
        self.stdout.write(f"    reabasto_lineas: {metrics['volumen']['reabasto_lineas']}")

        self.stdout.write("  -- Duplicados --")
        self.stdout.write(
            f"    insumos_nombre_normalizado_dup: {metrics['duplicados']['insumos_nombre_normalizado_dup']}"
        )
        self.stdout.write(
            f"    recetas_nombre_normalizado_dup: {metrics['duplicados']['recetas_nombre_normalizado_dup']}"
        )
        self.stdout.write(f"    recetas_codigo_point_dup: {metrics['duplicados']['recetas_codigo_point_dup']}")
        self.stdout.write(f"    solicitudes_compra_dup_key: {metrics['duplicados']['solicitudes_compra_dup_key']}")

        self.stdout.write("  -- Calidad receta/match --")
        self.stdout.write(f"    lineas_sin_match: {metrics['calidad_receta']['lineas_sin_match']}")
        self.stdout.write(f"    lineas_needs_review: {metrics['calidad_receta']['lineas_needs_review']}")
        self.stdout.write(
            f"    lineas_ligadas_sin_cantidad: {metrics['calidad_receta']['lineas_ligadas_sin_cantidad']}"
        )
        self.stdout.write(
            "    lineas_ligadas_sin_costo_snapshot: "
            f"{metrics['calidad_receta']['lineas_ligadas_sin_costo_snapshot']}"
        )

        self.stdout.write("  -- Calidad inventario/compras --")
        self.stdout.write(f"    insumos_sin_existencia: {metrics['calidad_stock']['insumos_sin_existencia']}")
        self.stdout.write(f"    costos_sin_proveedor: {metrics['calidad_stock']['costos_sin_proveedor']}")
        self.stdout.write(f"    aliases_sin_insumo_activo: {metrics['calidad_stock']['aliases_sin_insumo_activo']}")
        self.stdout.write(f"    point_pending_total: {metrics['calidad_stock']['point_pending_total']}")

        alertas = metrics["alertas"]
        if alertas:
            self.stdout.write(self.style.WARNING("  -- Alertas --"))
            for alerta in alertas:
                self.stdout.write(self.style.WARNING(f"    - {alerta}"))
        else:
            self.stdout.write(self.style.SUCCESS("  Sin alertas críticas detectadas."))

    def _build_metrics(self) -> dict:
        volumen = {
            "insumos": Insumo.objects.count(),
            "proveedores": Proveedor.objects.count(),
            "recetas": Receta.objects.count(),
            "lineas_receta": LineaReceta.objects.count(),
            "solicitudes_compra": SolicitudCompra.objects.count(),
            "ordenes_compra": OrdenCompra.objects.count(),
            "recepciones": RecepcionCompra.objects.count(),
            "existencias": ExistenciaInsumo.objects.count(),
            "movimientos": MovimientoInventario.objects.count(),
            "reabasto_solicitudes": SolicitudReabastoCedis.objects.count(),
            "reabasto_lineas": SolicitudReabastoCedisLinea.objects.count(),
        }

        duplicados = {
            "insumos_nombre_normalizado_dup": Insumo.objects.values("nombre_normalizado")
            .annotate(c=Count("id"))
            .filter(c__gt=1)
            .count(),
            "recetas_nombre_normalizado_dup": Receta.objects.values("nombre_normalizado")
            .annotate(c=Count("id"))
            .filter(c__gt=1)
            .count(),
            "recetas_codigo_point_dup": Receta.objects.exclude(codigo_point="")
            .values("codigo_point")
            .annotate(c=Count("id"))
            .filter(c__gt=1)
            .count(),
            "solicitudes_compra_dup_key": SolicitudCompra.objects.values(
                "area",
                "solicitante",
                "insumo_id",
                "fecha_requerida",
                "estatus",
            )
            .annotate(c=Count("id"))
            .filter(c__gt=1)
            .count(),
        }

        lineas_principales = LineaReceta.objects.exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        conceptos_no_materiales = [
            "presentación",
            "presentacion",
            "armado",
            "subtotal",
            "margen",
            "rendimiento",
        ]
        lineas_materiales = lineas_principales
        for concepto in conceptos_no_materiales:
            lineas_materiales = lineas_materiales.exclude(insumo_texto__iexact=concepto)

        calidad_receta = {
            "lineas_sin_match": lineas_materiales.filter(
                match_status=LineaReceta.STATUS_REJECTED
            ).count(),
            "lineas_needs_review": lineas_materiales.filter(
                match_status=LineaReceta.STATUS_NEEDS_REVIEW
            ).count(),
            "lineas_ligadas_sin_cantidad": LineaReceta.objects.filter(insumo__isnull=False)
            .filter(Q(cantidad__isnull=True) | Q(cantidad__lte=0))
            .count(),
            "lineas_ligadas_sin_costo_snapshot": LineaReceta.objects.filter(insumo__isnull=False)
            .filter(Q(costo_unitario_snapshot__isnull=True) | Q(costo_unitario_snapshot__lte=0))
            .count(),
        }

        calidad_stock = {
            "insumos_sin_existencia": Insumo.objects.filter(activo=True, existenciainsumo__isnull=True).count(),
            "costos_sin_proveedor": CostoInsumo.objects.filter(proveedor__isnull=True).count(),
            "aliases_sin_insumo_activo": InsumoAlias.objects.filter(insumo__activo=False).count(),
            "point_pending_total": PointPendingMatch.objects.count(),
        }

        alertas = []
        if duplicados["insumos_nombre_normalizado_dup"] > 0:
            alertas.append("Hay insumos duplicados por nombre normalizado.")
        if duplicados["recetas_nombre_normalizado_dup"] > 0:
            alertas.append("Hay recetas duplicadas por nombre normalizado.")
        if duplicados["recetas_codigo_point_dup"] > 0:
            alertas.append("Hay códigos Point repetidos entre recetas.")
        if calidad_receta["lineas_sin_match"] > 0:
            alertas.append("Existen líneas de receta sin match.")
        if calidad_receta["lineas_ligadas_sin_cantidad"] > 0:
            alertas.append("Existen líneas ligadas sin cantidad válida.")
        if calidad_receta["lineas_ligadas_sin_costo_snapshot"] > 0:
            alertas.append("Existen líneas ligadas sin costo unitario snapshot.")
        if calidad_stock["insumos_sin_existencia"] > 0:
            alertas.append("Hay insumos activos sin registro de existencia.")
        if calidad_stock["point_pending_total"] > 0:
            alertas.append("Hay pendientes de homologación Point por resolver.")

        return {
            "volumen": volumen,
            "duplicados": duplicados,
            "calidad_receta": calidad_receta,
            "calidad_stock": calidad_stock,
            "alertas": alertas,
        }
