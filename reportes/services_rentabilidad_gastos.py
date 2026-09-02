"""Lectura de gastos mensuales reales; nunca consolida ni modifica sus fuentes."""
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_HALF_UP

from django.db.models import Q

from .models import (GastoOperativoMensual, GastoRecurrenteVersion,
                     LineaPresupuestoMensual, ObligacionGasto, ReglaFuenteRubro)

CENTAVO = Decimal("0.01")
FAMILIAS = {"renta", "electricidad", "telefono", "sistemas", "alarmas", "mantenimiento", "otros"}
# Códigos existentes en el catálogo ERP; no se clasifica por fragmentos de texto.
FAMILIA_CODIGO = {"RENTA": "renta", "RENTA_SUC": "renta", "LUZ_SUC": "electricidad",
                  "TELEFONO_SUC": "telefono", "SISTEMAS_CORP": "sistemas",
                  "MANTENIMIENTO": "mantenimiento", "AGUA_SUC": "otros"}
FAMILIA_CONCEPTO = {"arrendamiento local": "renta", "energía eléctrica": "electricidad",
                    "teléfono e internet": "telefono", "mantenimiento equipo/maquinaria": "mantenimiento",
                    "licencias y servicios de sistemas": "sistemas",
                    "servicio de monitoreo de alarmas y seguridad": "alarmas"}
PERSONAL_FUENTES = {"NOMINA", "NOMINA_CONCEPTO", "BONO_PRODUCCION", "BONO_VENTAS"}
PERSONAL_CODIGOS = {"NOMINA", "NOMINA_SUC", "NOMINA_PROD", "NOMINA_CORP", "SIPARE", "IMSS", "INFONAVIT"}


def _familia(categoria=None, rubro=None):
    if categoria and (categoria.codigo in PERSONAL_CODIGOS or categoria.bucket == "MANO_OBRA_PROD"):
        return None
    for objeto in (categoria, rubro):
        valor = (getattr(objeto, "metadata", None) or {}).get("familia")
        if valor in FAMILIAS:
            return valor
    if categoria and categoria.codigo in FAMILIA_CODIGO:
        return FAMILIA_CODIGO[categoria.codigo]
    return FAMILIA_CONCEPTO.get(str(getattr(rubro, "concepto", "")).strip().lower())


def _fecha_filtro(valor):
    if not valor:
        return None
    texto = str(valor)
    return date.fromisoformat(texto + "-01" if len(texto) == 7 else texto)


def _vigente(regla, periodo):
    filtros = regla.filtros or {}
    try:
        desde, hasta = _fecha_filtro(filtros.get("desde")), _fecha_filtro(filtros.get("hasta"))
    except (TypeError, ValueError):
        return False
    return (desde is None or desde <= periodo) and (hasta is None or periodo <= hasta)


def _sucursal(regla):
    return regla.sucursal_id or regla.rubro.sucursal_id


def _reglas_del_mes(reglas, periodo):
    vigentes = [r for r in reglas if _vigente(r, periodo)]
    exactas = [r for r in vigentes if r.tipo_fuente == "OBLIGACION_GASTO"]
    return sorted(exactas or vigentes, key=lambda r: r.pk)


def _repartir(monto, centro, reglas):
    asignaciones = []
    for regla in reglas:
        directo = centro.tipo == "SUCURSAL_VENTA" and centro.sucursal_id == _sucursal(regla)
        try:
            porcentaje = Decimal("100") if directo else Decimal(str((regla.filtros or {}).get("porcentaje", 100)))
            if not porcentaje.is_finite() or not Decimal("0") < porcentaje <= Decimal("100"):
                raise ValueError
        except (ValueError, InvalidOperation):
            raise ValueError("Porcentaje de reparto inválido.") from None
        asignaciones.append((regla, porcentaje, (monto * porcentaje / 100).quantize(CENTAVO, rounding=ROUND_HALF_UP),
                             regla.rubro.area.codigo, _sucursal(regla)))
    total = sum(a[1] for a in asignaciones)
    if total > 100 or (len(asignaciones) > 1 and any(a[0].modo_asignacion != "DISTRIBUCION" for a in asignaciones)):
        raise ValueError("Reglas superpuestas: riesgo de contar dos veces el mismo origen.")
    if total == 100:
        ultima = asignaciones[-1]
        asignaciones[-1] = (*ultima[:2], monto - sum(a[2] for a in asignaciones[:-1]), *ultima[3:])
    return asignaciones, total != 100


def _asignar_cobertura(monto, centro, reglas, inicio, fin, periodo):
    """Conserva el recibo completo, incluso si cambia su reparto entre meses."""
    meses = [date((inicio.year * 12 + inicio.month - 1 + n) // 12,
                  (inicio.year * 12 + inicio.month - 1 + n) % 12 + 1, 1)
             for n in range((fin.year - inicio.year) * 12 + fin.month - inicio.month + 1)]
    juegos = {}
    for mes in meses:
        vigentes = _reglas_del_mes(reglas, mes)
        if not vigentes:
            raise ValueError("Falta historia de reparto para un mes de la cobertura.")
        for regla in vigentes:
            directo = centro.tipo == "SUCURSAL_VENTA" and centro.sucursal_id == _sucursal(regla)
            if not directo and not (regla.filtros or {}).get("desde") and regla.actualizado_en.date().replace(day=1) > inicio:
                raise ValueError("Sin historia verificable para aplicar el reparto actual.")
        juegos[mes] = _repartir(monto, centro, vigentes)
    # Cambiar solo la identidad/version de una regla no cambia sus proporciones.
    def firma(asignaciones):
        return sorted((a[3], a[4] or 0, a[1], a[0].signo) for a in asignaciones)
    actual, incompleto = juegos[periodo]
    cambio = any(firma(asignaciones) != firma(actual) for asignaciones, _ in juegos.values())
    if cambio:
        if any(parcial for _, parcial in juegos.values()):
            raise ValueError("La historia de reparto no cubre el 100% de cada mes del recibo.")
        actual, incompleto = _repartir(_importe_mensual(monto, inicio, fin, periodo), centro,
                                       _reglas_del_mes(reglas, periodo))
    return actual, cambio, incompleto


def _coincide(regla, centro, categoria_id, obligacion=None):
    if regla.tipo_fuente == "OBLIGACION_GASTO":
        return obligacion is not None and regla.rubro_id == obligacion.rubro_id
    if regla.tipo_fuente != "GASTO_OPERATIVO" or regla.categoria_gasto_id != categoria_id:
        return False
    if regla.centro_costo_id:
        return centro.pk == regla.centro_costo_id
    centro_tipo = (regla.filtros or {}).get("centro_tipo")
    if centro_tipo:
        return centro.tipo == centro_tipo
    return _sucursal(regla) is None or centro.sucursal_id == _sucursal(regla)


def _fila(origen, registro_id, *, sucursal_id=None, area="", familia=None, concepto="",
          monto=None, inicio=None, fin=None, soporte=None, regla=None):
    return dict(origen=origen, registro_id=registro_id, clave=f"{origen}:{registro_id}",
                sucursal_id=sucursal_id, area=area, familia=familia, concepto=concepto,
                monto_original=monto, monto_mensual=None, cobertura_inicio=inicio,
                cobertura_fin=fin, regla_id=regla.pk if regla else None,
                version_recurrente_id=None, vigencia_inicio=None, vigencia_fin=None, base_asignada=None,
                porcentaje=None, soporte=soporte or {}, estado="PENDIENTE", detalle="")


def _pendiente(resultado, fila, motivo):
    resultado["pendientes"].append({**fila, "monto_mensual": None, "estado": "PENDIENTE", "detalle": motivo})


def _pendiente_destinos(resultado, fila, motivo, reglas):
    """Una fuente compartida incompleta afecta a cada destino, no solo al centro."""
    if not reglas:
        _pendiente(resultado, fila, motivo)
        return
    for regla in reglas:
        _pendiente(resultado, {**fila, "regla_id": regla.pk, "sucursal_id": _sucursal(regla),
                              "area": regla.rubro.area.codigo}, motivo)


def _importe_mensual(monto, inicio, fin, periodo):
    meses = (fin.year - inicio.year) * 12 + fin.month - inicio.month + 1
    base = (monto / meses).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    return monto - base * (meses - 1) if periodo == fin else base


def leer_gastos_mensuales(periodo):
    """Devuelve filas económicas y pendientes; importes originales siempre intactos.

    ``filas`` contiene importes sumables COMPLETO/PARCIAL. ``pendientes`` nunca
    contiene un importe mensual sumable. Las fechas de vencimiento/pago son
    evidencia, no determinan el devengo. No invoca consolidación ni recálculos.
    """
    periodo = periodo.replace(day=1)
    resultado = {"filas": [], "pendientes": []}
    reglas = list(ReglaFuenteRubro.objects.filter(activa=True, rubro__activo=True)
                  .select_related("rubro__area", "rubro__sucursal", "categoria_gasto", "centro_costo"))
    personal_rubros = {r.rubro_id for r in reglas if r.tipo_fuente in PERSONAL_FUENTES}
    reglas = [r for r in reglas if r.modo_asignacion != "CONTROL" and r.rubro_id not in personal_rubros
              and r.tipo_fuente in {"GASTO_OPERATIVO", "OBLIGACION_GASTO", "MANUAL"}]
    versiones = list(GastoRecurrenteVersion.objects.filter(
        gasto_recurrente__activo=True, vigencia_inicio__lte=periodo)
        .filter(Q(vigencia_fin__isnull=True) | Q(vigencia_fin__gte=periodo))
        .select_related("gasto_recurrente__area", "gasto_recurrente__rubro",
                        "gasto_recurrente__categoria_gasto", "gasto_recurrente__centro_costo"))
    gastos = list(GastoOperativoMensual.objects.filter(tipo_dato="REAL", es_estimado=False)
                  .filter(Q(periodo=periodo) | Q(cobertura_mes_inicio__lte=periodo, cobertura_mes_fin__gte=periodo))
                  .exclude(obligacion_gasto__estado="CANCELADO")
                  .select_related("categoria_gasto", "centro_costo", "obligacion_gasto__rubro",
                                  "obligacion_gasto__area", "obligacion_gasto__version_recurrente")
                  .prefetch_related("obligacion_gasto__pagos")
                  .order_by("pk"))
    fuentes = [(g, getattr(g, "obligacion_gasto", None)) for g in gastos]
    fuentes += [(None, o) for o in ObligacionGasto.objects.filter(periodo=periodo, gasto_operativo__isnull=True)
                .exclude(estado="CANCELADO").select_related("categoria_gasto", "centro_costo", "rubro", "area", "version_recurrente")
                .prefetch_related("pagos")]
    vistas_reglas, vistas_versiones, rubros_con_fuente = set(), set(), set()
    for gasto, obligacion in fuentes:
        fuente = gasto or obligacion
        categoria, centro = fuente.categoria_gasto, fuente.centro_costo
        if (categoria.codigo in PERSONAL_CODIGOS | {"INVERSION", "CAPEX"}
                or categoria.bucket in {"MANO_OBRA_PROD", "EMPAQUE_PROD"}
                or categoria.capa_objetivo == "FABRICACION"):
            continue
        if categoria.codigo == "OPEX_TOTAL_SUC":
            continue  # Agregado de otras fuentes, nunca una fuente adicional.
        rubro = obligacion.rubro if obligacion else None
        if rubro and rubro.pk in personal_rubros:
            continue
        historicas = [r for r in reglas if _coincide(r, centro, categoria.pk, obligacion)]
        candidatas = _reglas_del_mes(historicas, periodo)
        familia = _familia(categoria, rubro)
        if not familia:
            familias = {_familia(categoria, r.rubro) for r in candidatas}
            familias.discard(None)
            if len(familias) == 1:
                familia = familias.pop()
        inicio = gasto.cobertura_mes_inicio if gasto else None
        fin = gasto.cobertura_mes_fin if gasto else None
        monto = gasto.monto if gasto else obligacion.monto_reconocido
        archivo = (gasto.archivo_soporte if gasto else "") or (obligacion.archivo_soporte if obligacion else "")
        soporte = {"archivo": archivo, "gasto_operativo_id": gasto.pk if gasto else None}
        if obligacion:
            soporte.update(obligacion_id=obligacion.pk, monto_obligacion=obligacion.monto_reconocido,
                           fecha_vencimiento=obligacion.fecha_vencimiento, estado_pago=obligacion.estado,
                           pagos=[{"id": p.pk, "fecha_pago": p.fecha_pago, "monto": p.monto,
                                   "metodo_pago": p.metodo_pago, "referencia": p.referencia}
                                  for p in obligacion.pagos.all()])
        fila = _fila("GASTO_OPERATIVO" if gasto else "OBLIGACION_GASTO", fuente.pk,
                     sucursal_id=centro.sucursal_id, area=obligacion.area.codigo if obligacion else "",
                     familia=familia, concepto=obligacion.concepto if obligacion else categoria.nombre,
                     monto=monto, inicio=inicio or fuente.periodo, fin=fin or fuente.periodo, soporte=soporte)
        if not fila["familia"]:
            _pendiente_destinos(resultado, fila, "Categoría sin clasificación económica inequívoca.", candidatas)
            continue
        contratos = [v for v in versiones if v.gasto_recurrente.categoria_gasto_id == categoria.pk
                     and v.gasto_recurrente.centro_costo_id == centro.pk
                     and (not rubro or v.gasto_recurrente.rubro_id == rubro.pk)]
        version = obligacion.version_recurrente if obligacion else None
        version_fuente = version or (contratos[0] if not obligacion and len(contratos) == 1 else None)
        if version_fuente:
            fila.update(version_recurrente_id=version_fuente.pk, vigencia_inicio=version_fuente.vigencia_inicio,
                        vigencia_fin=version_fuente.vigencia_fin)
        if (inicio is None) != (fin is None) or (inicio and (inicio > fin or inicio.day != 1 or fin.day != 1)):
            _pendiente_destinos(resultado, fila, "Cobertura inválida o incompleta.", candidatas)
            continue
        if not inicio and ((version and version.periodicidad_meses == 2) or any(v.periodicidad_meses == 2 for v in contratos)):
            _pendiente_destinos(resultado, fila, "Servicio bimestral conocido sin cobertura explícita.", candidatas)
            continue
        inicio, fin = inicio or fuente.periodo, fin or fuente.periodo
        if not inicio <= periodo <= fin:
            continue
        directo_sucursal = centro.tipo == "SUCURSAL_VENTA" and centro.sucursal_id is not None
        if not candidatas and len(contratos) != 1 and not directo_sucursal:
            _pendiente_destinos(resultado, fila, "Falta regla aplicable o contrato recurrente inequívoco.", historicas)
            continue
        reparto_incompleto = False
        asignacion_mensualizada = False
        if not candidatas and len(contratos) == 1:
            contrato = contratos[0].gasto_recurrente
            fila.update(area=contrato.area.codigo, sucursal_id=contrato.rubro.sucursal_id or centro.sucursal_id)
            asignaciones = [(None, Decimal("100"), monto, fila["area"], fila["sucursal_id"])]
        elif not candidatas:
            asignaciones = [(None, Decimal("100"), monto, "gastos-venta", centro.sucursal_id)]
        else:
            try:
                asignaciones, asignacion_mensualizada, reparto_incompleto = _asignar_cobertura(
                    monto, centro, historicas, inicio, fin, periodo)
            except ValueError as exc:
                _pendiente_destinos(resultado, fila, str(exc), candidatas)
                continue
            if reparto_incompleto:
                _pendiente_destinos(resultado, fila, "El reparto registrado no cubre el 100% del origen.", candidatas)
        for regla, porcentaje, asignado, area, sucursal_id in asignaciones:
            salida = {**fila, "area": area, "sucursal_id": sucursal_id,
                      "regla_id": regla.pk if regla else None, "porcentaje": porcentaje,
                      "clave": f"{fila['clave']}:REGLA:{regla.pk if regla else 'CONTRATO'}"}
            if area == "produccion":
                salida["sucursal_id"] = None
            elif area == "gastos-venta" and sucursal_id is None:
                _pendiente(resultado, salida, "Sucursal sin resolver; no se reparte entre sucursales.")
                continue
            if regla:
                salida.update(vigencia_inicio=_fecha_filtro((regla.filtros or {}).get("desde")),
                              vigencia_fin=_fecha_filtro((regla.filtros or {}).get("hasta")))
            mensual = asignado if asignacion_mensualizada else _importe_mensual(asignado, inicio, fin, periodo)
            salida.update(monto_mensual=mensual * (regla.signo if regla else 1), base_asignada=asignado,
                          estado="COMPLETO" if archivo else "PARCIAL",
                          detalle="" if archivo else "Importe registrado sin archivo de soporte verificable.")
            if obligacion and obligacion.monto_reconocido != monto:
                salida.update(estado="PARCIAL", detalle="El gasto y la obligación difieren; se conserva el gasto y el importe original de la obligación como evidencia.")
            if reparto_incompleto:
                salida.update(estado="PARCIAL", detalle="El reparto registrado no cubre el 100% del origen.")
            resultado["filas"].append(salida)
            if regla:
                vistas_reglas.add(regla.pk)
                rubros_con_fuente.add(regla.rubro_id)
                if regla.tipo_fuente == "OBLIGACION_GASTO":
                    # El gasto enlazado satisface también su regla genérica si
                    # representa exactamente el mismo destino y proporción.
                    # No se agrega una segunda fila ni se cubren otras categorías.
                    for espejo in historicas:
                        if (espejo.tipo_fuente != "GASTO_OPERATIVO" or not _vigente(espejo, periodo)
                                or _sucursal(espejo) != sucursal_id or espejo.rubro.area.codigo != area
                                or espejo.signo != regla.signo):
                            continue
                        try:
                            reparto_espejo, _ = _repartir(monto, centro, [espejo])
                        except ValueError:
                            continue
                        if reparto_espejo[0][1] == porcentaje:
                            vistas_reglas.add(espejo.pk)
            if rubro:
                rubros_con_fuente.add(rubro.pk)
            if version_fuente:
                vistas_versiones.add(version_fuente.pk)
                rubros_con_fuente.add(version_fuente.gasto_recurrente.rubro_id)

    for linea in (LineaPresupuestoMensual.objects.filter(periodo=periodo, version="ORIGINAL", rubro__activo=True)
                  .exclude(monto_real__isnull=True).select_related("rubro__area")):
        rubro = linea.rubro
        if rubro.pk in personal_rubros or rubro.tipo not in {"EGRESO", "COSTO"}:
            continue
        familia = _familia(rubro=rubro)
        if not familia:
            continue
        fila = _fila("LINEA_PRESUPUESTO", linea.pk, sucursal_id=rubro.sucursal_id,
                     area=rubro.area.codigo, familia=familia, concepto=rubro.concepto,
                     monto=linea.monto_real, inicio=periodo, fin=periodo,
                     soporte={"fuente_real": linea.fuente_real, "rubro_id": rubro.pk})
        if linea.fuente_real.startswith("AUTO:"):
            if rubro.pk not in rubros_con_fuente:
                _pendiente(resultado, fila, "Espejo automático sin fuente económica comprobada; no se suma.")
            continue
        posible_espejo = rubro.pk in rubros_con_fuente or any(
            f["sucursal_id"] == rubro.sucursal_id and f["familia"] == familia for f in resultado["filas"])
        if posible_espejo:
            _pendiente(resultado, fila, "Captura manual posiblemente reflejada en otra fuente; requiere conciliación.")
        elif not linea.fuente_real.startswith("MANUAL:"):
            _pendiente(resultado, fila, "Fuente consolidada no inequívoca.")
        elif rubro.area.codigo == "gastos-venta" and not rubro.sucursal_id:
            _pendiente(resultado, fila, "Sucursal sin resolver en captura manual.")
        else:
            fila.update(monto_mensual=linea.monto_real, porcentaje=Decimal("100"), estado="PARCIAL",
                        detalle="Real manual identificado; falta comprobar soporte documental.")
            if rubro.area.codigo == "produccion":
                fila["sucursal_id"] = None
            resultado["filas"].append(fila)
            rubros_con_fuente.add(rubro.pk)

    for regla in reglas:
        familia = _familia(regla.categoria_gasto, regla.rubro)
        if (not _vigente(regla, periodo) or not familia or regla.pk in vistas_reglas
                or (regla.tipo_fuente == "MANUAL" and regla.rubro_id in rubros_con_fuente)):
            continue
        fila = _fila("REGLA_FUENTE", regla.pk, sucursal_id=_sucursal(regla), area=regla.rubro.area.codigo,
                     familia=familia, concepto=regla.rubro.concepto, inicio=periodo, fin=periodo, regla=regla)
        _pendiente(resultado, fila, "Falta fuente real o recibo del mes para la regla aplicable.")
    for version in versiones:
        recurrente = version.gasto_recurrente
        familia = _familia(recurrente.categoria_gasto, recurrente.rubro)
        if not familia or version.pk in vistas_versiones:
            continue
        fila = _fila("GASTO_RECURRENTE_VERSION", version.pk, sucursal_id=recurrente.rubro.sucursal_id or recurrente.centro_costo.sucursal_id,
                     area=recurrente.area.codigo, familia=familia, concepto=recurrente.concepto,
                     monto=version.monto, inicio=periodo, fin=periodo)
        _pendiente(resultado, fila, "Contrato vigente sin recibo u obligación real que cubra el mes.")
    return resultado
