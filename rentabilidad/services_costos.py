"""Costos materiales por unidad vendida; nunca sustituye faltantes por porcentajes."""
from decimal import Decimal


def _costos_recetas_para_periodo(receta_ids, periodo, fin_periodo):
    return {pk: fuente["cost"] for pk, fuente in _fuentes_recetas_para_periodo(receta_ids, periodo, fin_periodo).items()}


def _fuentes_recetas_para_periodo(receta_ids, periodo, fin_periodo):
    if not receta_ids:
        return {}

    from django.db.models import Case, IntegerField, When
    from django.utils.timezone import localdate
    from recetas.models import RecetaCostoVersion
    from reportes.models import RecetaCostoHistoricoMensual

    costos = {}
    historicos = RecetaCostoHistoricoMensual.objects.filter(
        receta_id__in=receta_ids,
        periodo=periodo,
    )
    for historico in historicos:
        costos[historico.receta_id] = {"cost": Decimal(historico.costo_por_unidad_rendimiento or historico.costo_total or 0) if historico.coverage_pct >= 100 else Decimal("0"), "period": historico.periodo}

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
                costos[version.receta_id] = {"cost": (Decimal(version.costo_total or 0) - Decimal(version.costo_mo or 0) - Decimal(version.costo_indirecto or 0)) / Decimal(version.rendimiento_cantidad or 1), "period": localdate(version.creado_en).replace(day=1)}
    return costos


def _costos_reventa_para_periodo(producto_ids, periodo, fin_periodo):
    return {pk: fuente["cost"] for pk, fuente in _fuentes_reventa_para_periodo(producto_ids, periodo, fin_periodo).items()}


def _fuentes_reventa_para_periodo(producto_ids, periodo, fin_periodo):
    """Conserva la vigencia junto al costo para auditar arrastres entre meses."""
    if not producto_ids:
        return {}

    from reportes.models import ProductoReventaCosto, ProductoReventaCostoHistoricoMensual
    from pos_bridge.services.point_cost_validation import resale_cost_matches_product

    costos = {}
    historicos = list(ProductoReventaCostoHistoricoMensual.objects.filter(
        producto_point_id__in=producto_ids,
        periodo=periodo,
    ))
    source_ids = {pk for h in historicos for pk in (h.metadata or {}).get("source_rows", [])}
    fuentes = {c.pk: c for c in ProductoReventaCosto.objects.filter(pk__in=source_ids).select_related("producto_point")}
    for historico in historicos:
        ids = (historico.metadata or {}).get("source_rows", [])
        if ids and any(pk not in fuentes or fuentes[pk].producto_point_id != historico.producto_point_id or fuentes[pk].fecha_vigencia > fin_periodo or not resale_cost_matches_product(fuentes[pk]) for pk in ids):
            continue
        fecha = max((fuentes[pk].fecha_vigencia for pk in ids), default=historico.source_date or periodo)
        if fecha > fin_periodo:
            continue
        costos[historico.producto_point_id] = {"cost": Decimal(historico.costo_promedio or 0), "period": fecha.replace(day=1)}

    productos_sin_historico = [producto_id for producto_id in producto_ids if producto_id not in costos]
    if productos_sin_historico:
        vigentes = (
            ProductoReventaCosto.objects
            .filter(producto_point_id__in=productos_sin_historico, fecha_vigencia__lte=fin_periodo, costo_unitario__gt=0)
            .select_related("producto_point")
            .order_by("producto_point_id", "-fecha_vigencia", "-id")
        )
        for vigente in vigentes:
            if vigente.producto_point_id not in costos and resale_cost_matches_product(vigente):
                costos[vigente.producto_point_id] = {"cost": Decimal(vigente.costo_unitario or 0), "period": vigente.fecha_vigencia.replace(day=1)}
    return costos


def asignar_recetas_identificadas(filas):
    """Reutiliza identidades existentes sin crear relaciones ni modificar ventas."""
    from collections import defaultdict
    from recetas.models import RecetaAgrupacionAddon
    from pos_bridge.models import PointRecipeNode
    from recetas.utils.normalizacion import normalizar_nombre

    aprobados = defaultdict(set)
    for regla in RecetaAgrupacionAddon.objects.filter(activo=True, status=RecetaAgrupacionAddon.STATUS_APPROVED).exclude(addon_receta_id=None).select_related("addon_receta"):
        nombre = normalizar_nombre(regla.addon_nombre_point)
        if nombre == normalizar_nombre(regla.addon_receta.nombre):
            aprobados[(regla.addon_codigo_point, nombre)].add(regla.addon_receta_id)
    codigos = {fila["product__sku"] for fila in filas if fila["receta_id"] is None}
    for nodo in PointRecipeNode.objects.filter(source_type="PRODUCT", point_code__in=codigos, erp_recipe__isnull=False).select_related("erp_recipe"):
        nombre = normalizar_nombre(nodo.point_name)
        if nombre == normalizar_nombre(nodo.erp_recipe.nombre):
            aprobados[(nodo.point_code, nombre)].add(nodo.erp_recipe_id)
    for fila in filas:
        ids = aprobados.get((fila["product__sku"], normalizar_nombre(fila["product__name"])), set())
        if fila["receta_id"] is None and len(ids) == 1:
            # La base ya tiene su venta: esta fila consume únicamente el complemento aprobado.
            fila["receta_id"] = next(iter(ids))
