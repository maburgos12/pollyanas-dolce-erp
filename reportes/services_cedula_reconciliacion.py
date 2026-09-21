"""Cruces adicionales de expedientes aplicados, sin recalcular el control patronal."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace

from django.db import transaction
from django.utils import timezone

from core.audit import log_event

from .models import DetalleCedulaIMSS, DocumentoCedulaIMSS, ExpedienteCedulaIMSS, LineaPresupuestoMensual
from .services_cedula_expediente import _bloquear_familia, _cruzar_detalles
from .services_cedula_imss import CedulaParseada, FUENTE_SIPARE, TrabajadorCedula, _rubro_destino


ZERO = Decimal("0.00")
CENT = Decimal("0.01")


@dataclass(frozen=True)
class ResultadoCruces:
    expediente_id: int
    nuevos_cruces: int
    pendientes: int
    monto_atribuido: Decimal
    aplicado: bool


def _importes_mensuales(total: Decimal, meses: list) -> tuple[Decimal, ...]:
    primera = (total / len(meses)).quantize(CENT)
    return tuple([primera] * (len(meses) - 1) + [total - primera * (len(meses) - 1)])


def _totales_atribuidos(detalles) -> dict[tuple[str, int | None], Decimal]:
    totales: dict[tuple[str, int | None], Decimal] = {}
    for detalle in detalles:
        if detalle.empleado_id is None:
            continue
        clave = (detalle.area_codigo, detalle.sucursal_id)
        totales[clave] = totales.get(clave, ZERO) + detalle.cuota_patronal
    return totales


def _reconciliar(expediente_id: int, *, aplicar: bool, usuario) -> ResultadoCruces:
    queryset = ExpedienteCedulaIMSS.objects
    if aplicar:
        queryset = queryset.select_for_update()
    expediente = queryset.get(pk=expediente_id)
    if expediente.estado != ExpedienteCedulaIMSS.ESTADO_APLICADO:
        raise ValueError("Solo se reconcilian expedientes aplicados.")
    suas = list(expediente.documentos.filter(clase=DocumentoCedulaIMSS.CLASE_SUA_XLS))
    if len(suas) != 1:
        raise ValueError("El expediente no tiene un SUA único.")
    documento = suas[0]
    detalles_qs = documento.detalles.order_by("pk")
    if aplicar:
        detalles_qs = detalles_qs.select_for_update()
    guardados = list(detalles_qs)
    if (
        len(guardados) != expediente.trabajadores
        or sum((d.cuota_patronal for d in guardados), ZERO).quantize(CENT) != expediente.total_patronal
        or sum(d.empleado_id is not None for d in guardados) != expediente.cruzados
    ):
        raise ValueError("El detalle guardado no concilia con el expediente aplicado.")
    parseada = CedulaParseada(
        tipo=expediente.tipo,
        periodo=expediente.periodo,
        registro_patronal=expediente.registro_patronal,
        trabajadores=[
            TrabajadorCedula(
                nss=d.nss, nombre=d.nombre_origen, patronal=d.cuota_patronal,
                dias=d.dias, sdi=d.sdi, retiro=d.retiro,
                cesantia_patronal=d.cesantia_patronal,
                aportacion_vivienda=d.aportacion_vivienda,
            ) for d in guardados
        ],
    )
    if aplicar:
        _bloquear_familia(SimpleNamespace(parseada=parseada))
    candidatos, duplicados = _cruzar_detalles(parseada, bloquear=aplicar)
    if duplicados:
        raise ValueError("Hay NSS duplicados entre empleados vigentes en el periodo.")
    nuevos = []
    for guardado, candidato in zip(guardados, candidatos, strict=True):
        if guardado.empleado_id is not None:
            if candidato.empleado_id not in (None, guardado.empleado_id):
                raise ValueError("Un cruce previo ahora apunta a otra identidad; no se alteró.")
            continue
        if candidato.empleado_id is not None:
            nuevos.append((guardado, candidato))
    if not nuevos:
        return ResultadoCruces(expediente.pk, 0, expediente.sin_cruce, ZERO, aplicar)

    anterior = _totales_atribuidos(guardados)
    posterior = dict(anterior)
    for _, candidato in nuevos:
        clave = (candidato.area_codigo, candidato.sucursal_id)
        posterior[clave] = posterior.get(clave, ZERO) + candidato.cuota_patronal
    conceptos = ["imss"] if expediente.tipo == ExpedienteCedulaIMSS.TIPO_MENSUAL else [
        "infonavit rcv", "infonavit"
    ]
    avisos: list[str] = []
    cambios = []
    for clave in sorted(posterior, key=lambda item: (item[0], item[1] or 0)):
        rubro = _rubro_destino(clave[0], clave[1], conceptos, avisos)
        if rubro is None:
            raise ValueError("Falta rubro presupuestal para atribuir un cruce histórico.")
        importe_anterior = _importes_mensuales(anterior.get(clave, ZERO), parseada.meses)
        importe_nuevo = _importes_mensuales(posterior[clave], parseada.meses)
        for mes, monto_anterior, monto_nuevo in zip(
            parseada.meses, importe_anterior, importe_nuevo, strict=True
        ):
            linea = LineaPresupuestoMensual.objects.filter(
                rubro=rubro,
                periodo=mes,
                version=LineaPresupuestoMensual.VERSION_ORIGINAL,
            ).first()
            if aplicar and linea is not None:
                linea = LineaPresupuestoMensual.objects.select_for_update().get(pk=linea.pk)
            if monto_anterior:
                if (
                    linea is None
                    or linea.fuente_real != FUENTE_SIPARE
                    or linea.monto_real != monto_anterior
                    or (linea.metadata or {}).get("expediente_cedula_imss_id") != expediente.pk
                ):
                    raise ValueError("La atribución previa cambió o no pertenece al expediente; no se alteró.")
            elif monto_nuevo != monto_anterior and linea is not None and (
                linea.monto_real is not None
                or str(linea.fuente_real or "")
            ):
                raise ValueError("El destino del nuevo cruce ya tiene un monto; no se alteró.")
            if monto_nuevo != monto_anterior:
                cambios.append((rubro, mes, linea, monto_nuevo))

    if aplicar:
        ahora = timezone.now()
        for guardado, candidato in nuevos:
            guardado.empleado_id = candidato.empleado_id
            guardado.area_codigo = candidato.area_codigo
            guardado.sucursal_id = candidato.sucursal_id
            guardado.cruce_estado = DetalleCedulaIMSS.CRUCE_CRUZADO
            guardado.save(update_fields=["empleado", "area_codigo", "sucursal", "cruce_estado"])
        for rubro, mes, linea, monto in cambios:
            if linea is None:
                linea = LineaPresupuestoMensual(
                    rubro=rubro, periodo=mes,
                    version=LineaPresupuestoMensual.VERSION_ORIGINAL,
                    monto_presupuesto=ZERO,
                )
            metadata = dict(linea.metadata or {})
            metadata.update({
                "expediente_cedula_imss_id": expediente.pk,
                "documento_cedula_imss_id": documento.pk,
                "cedula_imss": {
                    "tipo": expediente.tipo,
                    "registro_patronal": expediente.registro_patronal,
                    "trabajadores": expediente.cruzados + len(nuevos),
                    "cruce_historico_en": ahora.isoformat(),
                },
            })
            linea.monto_real = monto
            linea.fuente_real = FUENTE_SIPARE
            linea.metadata = metadata
            linea.actualizado_en = ahora
            linea.save()
        expediente.cruzados += len(nuevos)
        expediente.sin_cruce -= len(nuevos)
        expediente.metadata = {
            **(expediente.metadata or {}),
            "cruces_historicos": {
                "detalle_ids": [guardado.pk for guardado, _ in nuevos],
                "actualizado_en": ahora.isoformat(),
            },
        }
        expediente.save(update_fields=["cruzados", "sin_cruce", "metadata"])
        log_event(
            usuario, "CEDULA_IMSS_CRUCES_HISTORICOS",
            "reportes.ExpedienteCedulaIMSS", str(expediente.pk),
            {
                "documento_sua_id": documento.pk,
                "sha256_sua": documento.sha256,
                "detalle_ids": [guardado.pk for guardado, _ in nuevos],
                "nuevos_cruces": len(nuevos),
                "monto_atribuido": str(sum((c.cuota_patronal for _, c in nuevos), ZERO)),
            },
        )
    return ResultadoCruces(
        expediente.pk, len(nuevos), expediente.sin_cruce if aplicar else expediente.sin_cruce - len(nuevos),
        sum((c.cuota_patronal for _, c in nuevos), ZERO), aplicar,
    )


def reconciliar_cruces_aplicados(
    expediente_id: int, *, aplicar: bool = False, usuario=None
) -> ResultadoCruces:
    """Previsualiza o aplica solo cruces nuevos; nunca modifica el total corporativo."""
    if aplicar:
        with transaction.atomic():
            return _reconciliar(expediente_id, aplicar=True, usuario=usuario)
    return _reconciliar(expediente_id, aplicar=False, usuario=usuario)
