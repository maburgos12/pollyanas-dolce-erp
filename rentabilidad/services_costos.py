"""Costos materiales por unidad vendida; nunca sustituye faltantes por porcentajes."""
from calendar import monthrange
from decimal import Decimal


def productos_nativos_por_sku(filas):
    """Candidatos por clave exacta; conserva colisiones para no elegir el primero."""
    from collections import defaultdict
    from pos_bridge.models import PointProduct
    from recetas.utils.normalizacion import normalizar_nombre

    codigos = {fila["product__sku"] for fila in filas if not fila["product_id"] and fila["product__sku"]}
    candidatos = defaultdict(lambda: defaultdict(set))
    for producto in PointProduct.objects.filter(sku__in=codigos).only("id", "external_id", "sku", "name"):
        if producto.external_id.isdigit():
            candidatos[producto.sku][normalizar_nombre(producto.name)].add(producto.pk)
    return candidatos


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
        costos[historico.receta_id] = {"cost": Decimal(historico.costo_por_unidad_rendimiento or historico.costo_total or 0) if historico.coverage_pct >= 100 else Decimal("0"), "period": historico.periodo, "through": historico.periodo.replace(day=monthrange(historico.periodo.year, historico.periodo.month)[1])}

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

    # Las decisiones DG explícitas tienen precedencia; un cero común sigue desconocido.
    decisiones = {}
    for row in ProductoReventaCosto.objects.filter(
        producto_point_id__in=producto_ids, fecha_vigencia__lte=fin_periodo,
        fuente=ProductoReventaCosto.FUENTE_MANUAL,
    ).select_related("producto_point").order_by("producto_point_id", "-fecha_vigencia", "-id"):
        meta = row.metadata or {}
        treatment = meta.get("cost_treatment")
        if (row.producto_point_id not in decisiones
                and treatment in {"NO_DIRECT_COST", "SUPPLIER_GIFT", "UNALLOCATED_SERVICE", "PURCHASE_REFERENCE"}
                and meta.get("authorization")
                and meta.get("point_external_id") == row.producto_point.external_id
                and meta.get("point_name") == row.producto_point.name
                and ((treatment == "PURCHASE_REFERENCE" and row.costo_unitario > 0)
                     or (treatment != "PURCHASE_REFERENCE" and row.costo_unitario == 0))):
            decisiones[row.producto_point_id] = {
                "cost": row.costo_unitario, "period": row.fecha_vigencia.replace(day=1),
                "treatment": treatment, "authorized": True,
            }
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
    costos.update(decisiones)
    return costos


def asignar_recetas_identificadas(filas, *, incluir_aliases=False):
    """Reutiliza identidades existentes sin crear relaciones ni modificar ventas."""
    from collections import defaultdict
    from recetas.models import RecetaAgrupacionAddon
    from pos_bridge.models import PointRecipeNode
    from recetas.utils.normalizacion import normalizar_nombre

    aprobados = defaultdict(set)
    for regla in RecetaAgrupacionAddon.objects.filter(activo=True, status=RecetaAgrupacionAddon.STATUS_APPROVED).exclude(addon_receta_id=None).select_related("addon_receta"):
        nombre = normalizar_nombre(regla.addon_nombre_point)
        if regla.addon_codigo_point and nombre == normalizar_nombre(regla.addon_receta.nombre):
            aprobados[(regla.addon_codigo_point, nombre)].add(regla.addon_receta_id)
    aprobados_explicitos = {key: set(ids) for key, ids in aprobados.items()}
    codigos = {fila["product__sku"] for fila in filas if fila["receta_id"] is None and fila["product__sku"]}
    if incluir_aliases:
        from recetas.models import RecetaCodigoPointAlias

        for alias in RecetaCodigoPointAlias.objects.filter(activo=True, codigo_point__in=codigos).select_related("receta"):
            nombre = normalizar_nombre(alias.nombre_point)
            if nombre and nombre == normalizar_nombre(alias.receta.nombre):
                aprobados[(alias.codigo_point, nombre)].add(alias.receta_id)
    for nodo in PointRecipeNode.objects.filter(source_type="PRODUCT", point_code__in=codigos, erp_recipe__isnull=False).select_related("erp_recipe"):
        nombre = normalizar_nombre(nodo.point_name)
        if nombre == normalizar_nombre(nodo.erp_recipe.nombre):
            aprobados[(nodo.point_code, nombre)].add(nodo.erp_recipe_id)
    for fila in filas:
        ids = aprobados.get((fila["product__sku"], normalizar_nombre(fila["product__name"])), set())
        if len(ids) == 1 and (fila["receta_id"] is None or ids == aprobados_explicitos.get((fila["product__sku"], normalizar_nombre(fila["product__name"])))):
            # La base ya tiene su venta: esta fila consume únicamente el complemento aprobado.
            receta_id = next(iter(ids))
            fila["recipe_identity_corrected"] = bool(fila["receta_id"] and fila["receta_id"] != receta_id)
            fila["receta_id"] = receta_id


def _asignar_recetas_historicas_2025(filas, periodo):
    """Recupera la identidad que acredita el costo documental del mismo mes."""
    if not filas:
        return
    from collections import defaultdict
    from django.db.models import Q
    from pos_bridge.models import PointProduct
    from recetas.utils.normalizacion import normalizar_nombre
    from reportes.models import RecetaCostoHistoricoMensual

    productos = dict(PointProduct.objects.filter(
        pk__in={f["product_id"] for f in filas if f["product_id"]},
    ).values_list("pk", "external_id"))
    identidades = defaultdict(set)
    for historico in RecetaCostoHistoricoMensual.objects.filter(
        Q(metadata__source="POINT_PRODUCTION_UNIT_VALUATION_2025",
          metadata__unit_of_analysis="ONE_NATIVE_FINISHED_PRODUCT_UNIT")
        | Q(metadata__source="POINT_CONVERSION_UNIT_VALUATION_2025",
            metadata__unit_of_analysis="ONE_NATIVE_CONVERTED_PRODUCT_UNIT"),
        periodo=periodo, coverage_pct__gte=100,
    ).select_related("receta"):
        meta = historico.metadata
        codigo, nombre = meta.get("historical_sku"), normalizar_nombre(meta.get("historical_name") or "")
        externo = meta.get("point_product_external_id")
        if (codigo and nombre and externo and meta.get("source_rows")
                and codigo == historico.receta.codigo_point
                and nombre == normalizar_nombre(historico.receta.nombre)
                and (historico.costo_por_unidad_rendimiento or historico.costo_total) > 0):
            identidades[(externo, codigo, nombre)].add(historico.receta_id)
    for fila in filas:
        ids = identidades.get((productos.get(fila["product_id"]), fila["product__sku"],
                               normalizar_nombre(fila["product__name"])), set())
        if len(ids) == 1:
            fila["receta_id"] = next(iter(ids))


def asignar_costos_filas(filas, periodo, fin_periodo):
    """Misma precedencia de receta, adquisición y decisión DG en todos los lectores."""
    from recetas.models import Receta
    from recetas.utils.normalizacion import normalizar_nombre
    # shortcut: regla revisada solo para 2025; ampliar tras auditar otro ejercicio.
    if periodo.year == 2025:
        productos = productos_nativos_por_sku(filas)
        for fila in filas:
            ids = productos.get(fila["product__sku"], {}).get(normalizar_nombre(fila["product__name"]), set())
            if not fila["product_id"] and len(ids) == 1:
                fila["product_id"] = next(iter(ids))
    asignar_recetas_identificadas(filas)
    resale = _fuentes_reventa_para_periodo({f["product_id"] for f in filas if f["product_id"]}, periodo, fin_periodo)
    # shortcut: identidades documentales 2025 y aliases ene-sep2026; otros periodos requieren auditoría.
    if periodo.year == 2025 or (periodo.year == 2026 and periodo.month <= 9):
        pendientes = [f for f in filas if f["receta_id"] is None and not (
            resale.get(f["product_id"]) and (resale[f["product_id"]]["cost"] > 0
                                             or resale[f["product_id"]].get("authorized")))]
        if periodo.year == 2025:
            _asignar_recetas_historicas_2025(pendientes, periodo)
        else:
            asignar_recetas_identificadas(pendientes, incluir_aliases=True)
    ids = {f["receta_id"] for f in filas if f["receta_id"]}
    recipes = _fuentes_recetas_para_periodo(ids, periodo, fin_periodo)
    resale_recipes = set(Receta.objects.filter(pk__in=ids, modo_costeo=Receta.MODO_COSTEO_REVENTA).values_list("pk", flat=True))
    for row in filas:
        evidence = recipes.get(row["receta_id"])
        acquisition = resale.get(row["product_id"])
        is_resale = not row["receta_id"] or bool(acquisition and (acquisition.get("authorized") or (row["receta_id"] in resale_recipes and acquisition["cost"] > 0)))
        row["cost_evidence"] = acquisition if is_resale else evidence
        row["cost_is_resale"] = is_resale
