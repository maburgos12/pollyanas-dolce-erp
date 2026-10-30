"""Costos materiales por unidad vendida; nunca sustituye faltantes por porcentajes."""
from decimal import Decimal


def _costos_recetas_para_periodo(receta_ids, periodo, fin_periodo):
    if not receta_ids:
        return {}

    from django.db.models import Case, IntegerField, When
    from recetas.models import RecetaCostoVersion
    from reportes.models import RecetaCostoHistoricoMensual

    costos = {}
    historicos = RecetaCostoHistoricoMensual.objects.filter(
        receta_id__in=receta_ids,
        periodo=periodo,
    )
    for historico in historicos:
        costos[historico.receta_id] = Decimal(historico.costo_por_unidad_rendimiento or historico.costo_total or 0) if historico.coverage_pct >= 100 else Decimal("0")

    recetas_sin_historico = [receta_id for receta_id in receta_ids if receta_id not in costos]
    if recetas_sin_historico:
        versiones = (
            RecetaCostoVersion.objects
            .filter(receta_id__in=recetas_sin_historico, creado_en__date__lte=fin_periodo)
            .annotate(
                fuente_prioridad=Case(
                    When(fuente="POINT_PRODUCTION_REPORT", then=0),
                    When(fuente="POINT_COST_CAPTURE", then=1),
                    When(fuente="POINT_COST_CAPTURE_FIX", then=2),
                    default=3,
                    output_field=IntegerField(),
                )
            )
            .order_by("receta_id", "fuente_prioridad", "-version_num")
        )
        for version in versiones:
            if version.receta_id not in costos:
                costos[version.receta_id] = (Decimal(version.costo_total or 0) - Decimal(version.costo_mo or 0) - Decimal(version.costo_indirecto or 0)) / Decimal(version.rendimiento_cantidad or 1)
    return costos


def _costos_reventa_para_periodo(producto_ids, periodo, fin_periodo):
    if not producto_ids:
        return {}

    from reportes.models import ProductoReventaCosto, ProductoReventaCostoHistoricoMensual

    costos = {}
    historicos = ProductoReventaCostoHistoricoMensual.objects.filter(
        producto_point_id__in=producto_ids,
        periodo=periodo,
    )
    for historico in historicos:
        costos[historico.producto_point_id] = Decimal(historico.costo_promedio or 0)

    productos_sin_historico = [producto_id for producto_id in producto_ids if producto_id not in costos]
    if productos_sin_historico:
        vigentes = (
            ProductoReventaCosto.objects
            .filter(producto_point_id__in=productos_sin_historico, fecha_vigencia__lte=fin_periodo)
            .order_by("producto_point_id", "-fecha_vigencia", "-id")
        )
        for vigente in vigentes:
            if vigente.producto_point_id not in costos:
                costos[vigente.producto_point_id] = Decimal(vigente.costo_unitario or 0)
    return costos
