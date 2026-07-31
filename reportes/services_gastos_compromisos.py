from __future__ import annotations

import calendar
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP

from django.core.exceptions import PermissionDenied, ValidationError
from django.db import models, transaction

from core.access import can_manage_module

from .models import (
    AreaPresupuesto,
    AreaPresupuestoResponsable,
    CategoriaGasto,
    CentroCosto,
    GastoOperativoMensual,
    GastoRecurrente,
    GastoRecurrenteVersion,
    ObligacionGasto,
    PagoObligacionGasto,
    ParcialidadObligacionGasto,
    ReglaFuenteRubro,
    RubroPresupuesto,
)

CENTAVO = Decimal("0.01")


def usuario_puede_capturar_area(usuario, area: AreaPresupuesto) -> bool:
    if not getattr(usuario, "is_authenticated", False):
        return False
    if can_manage_module(usuario, "reportes"):
        return True
    return AreaPresupuestoResponsable.objects.filter(
        area=area,
        usuario=usuario,
        puede_capturar=True,
    ).exists()


def _autorizar_area(usuario, area: AreaPresupuesto) -> None:
    if not usuario_puede_capturar_area(usuario, area):
        raise PermissionDenied("No eres responsable de esta área de presupuesto.")


def _monto(valor) -> Decimal:
    try:
        monto = Decimal(str(valor)).quantize(CENTAVO, rounding=ROUND_HALF_UP)
    except Exception as exc:  # noqa: BLE001
        raise ValidationError("Captura un monto válido.") from exc
    if monto <= 0:
        raise ValidationError("El monto debe ser mayor que cero.")
    return monto


def _validar_clasificacion(*, area, rubro, condicion_pago, tipo_credito, plazo_cantidad, plazo_unidad, numero_parcialidades):
    if rubro.area_id != area.id:
        raise ValidationError("El rubro no pertenece al área seleccionada.")
    if condicion_pago == ObligacionGasto.CONDICION_CONTADO:
        return "", None, "", 1
    if condicion_pago != ObligacionGasto.CONDICION_CREDITO:
        raise ValidationError("Selecciona si la compra fue de contado o a crédito.")
    if tipo_credito not in {ObligacionGasto.CREDITO_UNICO, ObligacionGasto.CREDITO_DIFERIDO}:
        raise ValidationError("Selecciona el tipo de crédito.")
    if not plazo_cantidad or int(plazo_cantidad) < 1:
        raise ValidationError("Indica el plazo del crédito.")
    if plazo_unidad not in {ObligacionGasto.PLAZO_DIAS, ObligacionGasto.PLAZO_MESES}:
        raise ValidationError("Indica si el plazo está expresado en días o meses.")
    parcialidades = int(numero_parcialidades or 1)
    if tipo_credito == ObligacionGasto.CREDITO_DIFERIDO and parcialidades < 2:
        raise ValidationError("Un crédito diferido debe tener al menos dos parcialidades.")
    if tipo_credito == ObligacionGasto.CREDITO_UNICO:
        parcialidades = 1
    return tipo_credito, int(plazo_cantidad), plazo_unidad, parcialidades


def _regla_gasto_coincide(regla, *, centro_costo, categoria_gasto) -> bool:
    if regla.categoria_gasto_id != categoria_gasto.id:
        return False
    if regla.centro_costo_id:
        return regla.centro_costo_id == centro_costo.id
    centro_tipo = (regla.filtros or {}).get("centro_tipo")
    if centro_tipo and centro_tipo != centro_costo.tipo:
        return False
    sucursal = regla.sucursal_efectiva()
    return sucursal is None or sucursal.id == centro_costo.sucursal_id


def _asegurar_fuente_rubro(*, rubro, centro_costo, categoria_gasto) -> None:
    reglas = list(rubro.reglas_fuente.filter(activa=True).exclude(tipo_fuente=ReglaFuenteRubro.FUENTE_MANUAL))
    if any(regla.tipo_fuente == ReglaFuenteRubro.FUENTE_OBLIGACION_GASTO for regla in reglas):
        return
    reglas_gasto = [
        regla for regla in reglas if regla.tipo_fuente == ReglaFuenteRubro.FUENTE_GASTO_OPERATIVO
    ]
    otras = [
        regla
        for regla in reglas
        if regla.tipo_fuente
        not in {ReglaFuenteRubro.FUENTE_GASTO_OPERATIVO, ReglaFuenteRubro.FUENTE_OBLIGACION_GASTO}
    ]
    if otras:
        raise ValidationError(
            "Este rubro ya se llena desde otra fuente automática; registra el gasto en su módulo de origen."
        )
    if reglas_gasto:
        if not any(
            _regla_gasto_coincide(
                regla, centro_costo=centro_costo, categoria_gasto=categoria_gasto
            )
            for regla in reglas_gasto
        ):
            raise ValidationError(
                "La categoría o el centro de costo no coincide con la fuente configurada para este rubro."
            )
        return
    ReglaFuenteRubro.objects.create(
        rubro=rubro,
        tipo_fuente=ReglaFuenteRubro.FUENTE_OBLIGACION_GASTO,
        origen=ReglaFuenteRubro.ORIGEN_ADMIN,
        notas="Fuente creada al habilitar captura estructurada de obligaciones.",
    )


def _sumar_meses(fecha: date, meses: int) -> date:
    indice = fecha.month - 1 + meses
    anio = fecha.year + indice // 12
    mes = indice % 12 + 1
    dia = min(fecha.day, calendar.monthrange(anio, mes)[1])
    return date(anio, mes, dia)


def _fecha_parcialidad(obligacion: ObligacionGasto, indice: int) -> date:
    if indice == 0:
        return obligacion.fecha_vencimiento
    cantidad = int(obligacion.plazo_cantidad or obligacion.numero_parcialidades)
    paso = max(1, cantidad // obligacion.numero_parcialidades)
    if obligacion.plazo_unidad == ObligacionGasto.PLAZO_MESES:
        return _sumar_meses(obligacion.fecha_vencimiento, paso * indice)
    return obligacion.fecha_vencimiento + timedelta(days=paso * indice)


def _crear_parcialidades(obligacion: ObligacionGasto) -> None:
    if not (
        obligacion.condicion_pago == ObligacionGasto.CONDICION_CREDITO
        and obligacion.tipo_credito == ObligacionGasto.CREDITO_DIFERIDO
    ):
        return
    base = (obligacion.monto_reconocido / obligacion.numero_parcialidades).quantize(
        CENTAVO, rounding=ROUND_HALF_UP
    )
    acumulado = Decimal("0.00")
    filas = []
    for indice in range(obligacion.numero_parcialidades):
        monto = base
        if indice == obligacion.numero_parcialidades - 1:
            monto = obligacion.monto_reconocido - acumulado
        acumulado += monto
        filas.append(
            ParcialidadObligacionGasto(
                obligacion=obligacion,
                numero=indice + 1,
                fecha_vencimiento=_fecha_parcialidad(obligacion, indice),
                monto=monto,
            )
        )
    ParcialidadObligacionGasto.objects.bulk_create(filas)


def _reconocer_gasto(obligacion: ObligacionGasto) -> GastoOperativoMensual:
    gasto = GastoOperativoMensual.objects.create(
        periodo=obligacion.periodo,
        centro_costo=obligacion.centro_costo,
        categoria_gasto=obligacion.categoria_gasto,
        external_key=f"OBLIGACION-GASTO-{obligacion.pk}",
        monto=obligacion.monto_reconocido,
        tipo_dato=GastoOperativoMensual.TIPO_DATO_REAL,
        fuente=GastoOperativoMensual.FUENTE_MANUAL,
        comentario=f"{obligacion.concepto} · obligación #{obligacion.pk}",
        archivo_soporte=obligacion.archivo_soporte,
        capturado_por=obligacion.creado_por,
    )
    obligacion.gasto_operativo = gasto
    obligacion.save(update_fields=["gasto_operativo", "actualizado_en"])
    return gasto


def _crear_obligacion(*, usuario, origen, area, rubro, centro_costo, categoria_gasto, concepto, periodo,
                      fecha_gasto, fecha_vencimiento, monto, proveedor="", condicion_pago=ObligacionGasto.CONDICION_CONTADO,
                      metodo_pago_previsto="", tipo_credito="", plazo_cantidad=None, plazo_unidad="",
                      numero_parcialidades=1, gasto_recurrente=None, version_recurrente=None,
                      archivo_soporte="", notas="") -> ObligacionGasto:
    _autorizar_area(usuario, area)
    tipo_credito, plazo_cantidad, plazo_unidad, numero_parcialidades = _validar_clasificacion(
        area=area,
        rubro=rubro,
        condicion_pago=condicion_pago,
        tipo_credito=tipo_credito,
        plazo_cantidad=plazo_cantidad,
        plazo_unidad=plazo_unidad,
        numero_parcialidades=numero_parcialidades,
    )
    if not concepto.strip():
        raise ValidationError("Escribe el concepto del gasto.")
    if fecha_vencimiento < fecha_gasto:
        raise ValidationError("La fecha de vencimiento no puede ser anterior a la fecha del gasto.")
    _asegurar_fuente_rubro(
        rubro=rubro,
        centro_costo=centro_costo,
        categoria_gasto=categoria_gasto,
    )
    obligacion = ObligacionGasto.objects.create(
        origen=origen,
        gasto_recurrente=gasto_recurrente,
        version_recurrente=version_recurrente,
        area=area,
        rubro=rubro,
        centro_costo=centro_costo,
        categoria_gasto=categoria_gasto,
        concepto=concepto.strip(),
        proveedor=proveedor.strip(),
        periodo=periodo.replace(day=1),
        fecha_gasto=fecha_gasto,
        fecha_vencimiento=fecha_vencimiento,
        monto_reconocido=_monto(monto),
        condicion_pago=condicion_pago,
        metodo_pago_previsto=metodo_pago_previsto,
        tipo_credito=tipo_credito,
        plazo_cantidad=plazo_cantidad,
        plazo_unidad=plazo_unidad,
        numero_parcialidades=numero_parcialidades,
        archivo_soporte=archivo_soporte,
        notas=notas,
        creado_por=usuario,
    )
    _crear_parcialidades(obligacion)
    _reconocer_gasto(obligacion)
    return obligacion


@transaction.atomic
def crear_gasto_variable(**kwargs) -> ObligacionGasto:
    return _crear_obligacion(origen=ObligacionGasto.ORIGEN_VARIABLE, **kwargs)


@transaction.atomic
def crear_gasto_recurrente(*, usuario, area, rubro, centro_costo, categoria_gasto, concepto,
                           vigencia_inicio, monto, dia_vencimiento, condicion_pago,
                           metodo_pago_previsto="", proveedor="", tipo_credito="",
                           plazo_cantidad=None, plazo_unidad="", numero_parcialidades=1,
                           motivo="") -> GastoRecurrente:
    _autorizar_area(usuario, area)
    tipo_credito, plazo_cantidad, plazo_unidad, numero_parcialidades = _validar_clasificacion(
        area=area,
        rubro=rubro,
        condicion_pago=condicion_pago,
        tipo_credito=tipo_credito,
        plazo_cantidad=plazo_cantidad,
        plazo_unidad=plazo_unidad,
        numero_parcialidades=numero_parcialidades,
    )
    if not 1 <= int(dia_vencimiento) <= 31:
        raise ValidationError("El día de vencimiento debe estar entre 1 y 31.")
    if not concepto.strip():
        raise ValidationError("Escribe el concepto del gasto fijo.")
    recurrente = GastoRecurrente.objects.create(
        area=area,
        rubro=rubro,
        centro_costo=centro_costo,
        categoria_gasto=categoria_gasto,
        concepto=concepto.strip(),
        proveedor=proveedor.strip(),
        creado_por=usuario,
    )
    GastoRecurrenteVersion.objects.create(
        gasto_recurrente=recurrente,
        vigencia_inicio=vigencia_inicio,
        monto=_monto(monto),
        dia_vencimiento=int(dia_vencimiento),
        condicion_pago=condicion_pago,
        metodo_pago_previsto=metodo_pago_previsto,
        tipo_credito=tipo_credito,
        plazo_cantidad=plazo_cantidad,
        plazo_unidad=plazo_unidad,
        numero_parcialidades=numero_parcialidades,
        motivo=motivo.strip(),
        creado_por=usuario,
    )
    return recurrente


@transaction.atomic
def editar_gasto_recurrente(*, usuario, recurrente, vigencia_inicio, monto, dia_vencimiento,
                            condicion_pago, metodo_pago_previsto="", tipo_credito="",
                            plazo_cantidad=None, plazo_unidad="", numero_parcialidades=1,
                            motivo="") -> GastoRecurrenteVersion:
    _autorizar_area(usuario, recurrente.area)
    if not motivo.strip():
        raise ValidationError("Explica el motivo de la modificación.")
    if not 1 <= int(dia_vencimiento) <= 31:
        raise ValidationError("El día de vencimiento debe estar entre 1 y 31.")
    actual = (
        GastoRecurrenteVersion.objects.select_for_update()
        .filter(gasto_recurrente=recurrente, vigencia_fin__isnull=True)
        .order_by("-vigencia_inicio", "-id")
        .first()
    )
    if actual is None:
        raise ValidationError("El gasto fijo no tiene una versión vigente.")
    if vigencia_inicio <= actual.vigencia_inicio:
        raise ValidationError("La nueva vigencia debe ser posterior a la versión actual.")
    tipo_credito, plazo_cantidad, plazo_unidad, numero_parcialidades = _validar_clasificacion(
        area=recurrente.area,
        rubro=recurrente.rubro,
        condicion_pago=condicion_pago,
        tipo_credito=tipo_credito,
        plazo_cantidad=plazo_cantidad,
        plazo_unidad=plazo_unidad,
        numero_parcialidades=numero_parcialidades,
    )
    actual.vigencia_fin = vigencia_inicio - timedelta(days=1)
    actual.save(update_fields=["vigencia_fin"])
    return GastoRecurrenteVersion.objects.create(
        gasto_recurrente=recurrente,
        vigencia_inicio=vigencia_inicio,
        monto=_monto(monto),
        dia_vencimiento=int(dia_vencimiento),
        condicion_pago=condicion_pago,
        metodo_pago_previsto=metodo_pago_previsto,
        tipo_credito=tipo_credito,
        plazo_cantidad=plazo_cantidad,
        plazo_unidad=plazo_unidad,
        numero_parcialidades=numero_parcialidades,
        motivo=motivo.strip(),
        creado_por=usuario,
    )


@transaction.atomic
def generar_obligacion_recurrente(*, usuario, recurrente: GastoRecurrente, periodo: date):
    _autorizar_area(usuario, recurrente.area)
    periodo = periodo.replace(day=1)
    existente = ObligacionGasto.objects.filter(gasto_recurrente=recurrente, periodo=periodo).first()
    if existente:
        return existente, False
    version = (
        recurrente.versiones.filter(vigencia_inicio__lte=periodo)
        .filter(models.Q(vigencia_fin__isnull=True) | models.Q(vigencia_fin__gte=periodo))
        .order_by("-vigencia_inicio")
        .first()
    )
    if version is None:
        raise ValidationError("No existe una versión vigente para el periodo solicitado.")
    dia = min(version.dia_vencimiento, calendar.monthrange(periodo.year, periodo.month)[1])
    obligacion = _crear_obligacion(
        usuario=usuario,
        origen=ObligacionGasto.ORIGEN_RECURRENTE,
        gasto_recurrente=recurrente,
        version_recurrente=version,
        area=recurrente.area,
        rubro=recurrente.rubro,
        centro_costo=recurrente.centro_costo,
        categoria_gasto=recurrente.categoria_gasto,
        concepto=recurrente.concepto,
        proveedor=recurrente.proveedor,
        periodo=periodo,
        fecha_gasto=periodo,
        fecha_vencimiento=date(periodo.year, periodo.month, dia),
        monto=version.monto,
        condicion_pago=version.condicion_pago,
        metodo_pago_previsto=version.metodo_pago_previsto,
        tipo_credito=version.tipo_credito,
        plazo_cantidad=version.plazo_cantidad,
        plazo_unidad=version.plazo_unidad,
        numero_parcialidades=version.numero_parcialidades,
    )
    return obligacion, True


@transaction.atomic
def registrar_pago(*, usuario, obligacion: ObligacionGasto, fecha_pago: date, monto,
                   metodo_pago: str, referencia="", notas="", parcialidad=None) -> PagoObligacionGasto:
    _autorizar_area(usuario, obligacion.area)
    obligacion = ObligacionGasto.objects.select_for_update().get(pk=obligacion.pk)
    if obligacion.estado == ObligacionGasto.ESTADO_CANCELADO:
        raise ValidationError("No se puede pagar una obligación cancelada.")
    monto = _monto(monto)
    if monto > obligacion.saldo_pendiente:
        raise ValidationError("El pago excede el saldo pendiente.")
    if metodo_pago not in dict(PagoObligacionGasto.METODO_CHOICES):
        raise ValidationError("Selecciona un medio de pago válido.")
    pago = PagoObligacionGasto.objects.create(
        obligacion=obligacion,
        parcialidad=parcialidad,
        fecha_pago=fecha_pago,
        monto=monto,
        metodo_pago=metodo_pago,
        referencia=referencia.strip(),
        notas=notas.strip(),
        registrado_por=usuario,
    )
    saldo = obligacion.monto_reconocido - obligacion.total_pagado
    obligacion.estado = ObligacionGasto.ESTADO_PAGADO if saldo <= 0 else ObligacionGasto.ESTADO_PARCIAL
    obligacion.save(update_fields=["estado", "actualizado_en"])
    return pago
