from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from django.core import signing
from django.core.exceptions import PermissionDenied, ValidationError

from logistica.models import PuntoLogistico, RutaCargaChecklistLinea, RutaEntrega
from logistica.services_carga_ruta import lineas_tramo_operativo_actual


CONTEXT_SALT = "logistica.contexto-operativo.v1"
CONTEXT_MAX_AGE_SECONDS = 12 * 60 * 60


class ContextoOperativoObsoleto(ValidationError):
    def __init__(self, mensaje: str, *, codigo: str = "contexto_obsoleto", productos_afectados=()):
        super().__init__(mensaje, code=codigo)
        self.codigo = codigo
        self.productos_afectados = tuple(sorted({int(value) for value in productos_afectados}))


@dataclass(frozen=True)
class ContextoOperativo:
    ruta_id: int
    chofer_autorizado_id: int
    unidad_id: int
    tramo_id: str
    parada_cedis_origen_id: int
    version_checklist: str
    sucursales_permitidas: tuple[int, ...]
    productos_permitidos: tuple[int, ...]
    acciones_permitidas: tuple[str, ...]
    token: str


def _linea_fact(linea: RutaCargaChecklistLinea) -> tuple:
    return (
        linea.id,
        linea.parada_id,
        linea.source_hash,
        str(linea.cantidad_enviada_esperada),
        linea.estatus,
    )


def _version_checklist(lineas: tuple[RutaCargaChecklistLinea, ...]) -> str:
    payload = json.dumps([_linea_fact(linea) for linea in lineas], separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _tramo(ruta: RutaEntrega, lineas: tuple[RutaCargaChecklistLinea, ...]):
    paradas = list(ruta.paradas.select_related("punto", "punto__sucursal").order_by("orden", "id"))
    ordenes_lineas = [linea.parada.orden for linea in lineas]
    orden_inicio_productos = min(ordenes_lineas) if ordenes_lineas else None
    cedis = [parada for parada in paradas if parada.punto.tipo == PuntoLogistico.TIPO_CEDIS]
    candidatos_origen = [
        parada for parada in cedis if orden_inicio_productos is None or parada.orden < orden_inicio_productos
    ]
    if not candidatos_origen:
        raise ValidationError("La ruta no tiene una parada CEDIS de origen para el tramo operativo.")
    origen = candidatos_origen[-1]
    siguiente = next((parada for parada in cedis if parada.orden > origen.orden), None)
    tramo_id = f"cedis-{origen.id}:hasta-{siguiente.id if siguiente else 'fin'}"
    return origen, tramo_id


def _payload(contexto: ContextoOperativo, lineas: tuple[RutaCargaChecklistLinea, ...]) -> dict:
    return {
        "ruta_id": contexto.ruta_id,
        "chofer_autorizado_id": contexto.chofer_autorizado_id,
        "unidad_id": contexto.unidad_id,
        "tramo_id": contexto.tramo_id,
        "parada_cedis_origen_id": contexto.parada_cedis_origen_id,
        "version_checklist": contexto.version_checklist,
        "sucursales_permitidas": list(contexto.sucursales_permitidas),
        "productos_permitidos": list(contexto.productos_permitidos),
        "acciones_permitidas": list(contexto.acciones_permitidas),
        "lineas": {str(linea.id): list(_linea_fact(linea)[1:]) for linea in lineas},
    }


def _ruta_actual(ruta: RutaEntrega, *, bloquear: bool) -> RutaEntrega:
    queryset = RutaEntrega.objects.select_related("repartidor", "repartidor__user", "unidad_operativa")
    if bloquear:
        queryset = queryset.select_for_update(of=("self",))
    return queryset.get(pk=ruta.pk)


def _construir_contexto(*, ruta: RutaEntrega, actor, firmar: bool, bloquear: bool = False):
    ruta = _ruta_actual(ruta, bloquear=bloquear)
    if not ruta.repartidor_id or ruta.repartidor.user_id != getattr(actor, "id", None):
        raise PermissionDenied("Solo el chofer titular puede operar esta ruta.")
    if not ruta.unidad_operativa_id:
        raise ValidationError("La ruta no tiene unidad operativa asignada.")

    lineas = tuple(
        lineas_tramo_operativo_actual(ruta)
        .exclude(estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        .select_related("parada", "parada__punto", "erp_destination_branch")
        .order_by("parada__orden", "item_name", "id")
    )
    origen, tramo_id = _tramo(ruta, lineas)
    sucursales = tuple(
        sorted(
            {
                linea.erp_destination_branch_id or linea.parada.punto.sucursal_id
                for linea in lineas
                if linea.erp_destination_branch_id or linea.parada.punto.sucursal_id
            }
        )
    )
    contexto = ContextoOperativo(
        ruta_id=ruta.id,
        chofer_autorizado_id=ruta.repartidor_id,
        unidad_id=ruta.unidad_operativa_id,
        tramo_id=tramo_id,
        parada_cedis_origen_id=origen.id,
        version_checklist=_version_checklist(lineas),
        sucursales_permitidas=sucursales,
        productos_permitidos=tuple(linea.id for linea in lineas),
        acciones_permitidas=("guardar_carga_sucursal", "salir_a_ruta"),
        token="",
    )
    if not firmar:
        return contexto, lineas
    token = signing.dumps(_payload(contexto, lineas), salt=CONTEXT_SALT, compress=True)
    return ContextoOperativo(**{**contexto.__dict__, "token": token})


def construir_contexto_operativo(*, ruta: RutaEntrega, actor) -> ContextoOperativo:
    return _construir_contexto(ruta=ruta, actor=actor, firmar=True)


def validar_contexto_operativo(*, token: str, ruta: RutaEntrega, actor, bloquear: bool = False) -> ContextoOperativo:
    try:
        firmado = signing.loads(token, salt=CONTEXT_SALT, max_age=CONTEXT_MAX_AGE_SECONDS)
    except signing.BadSignature as exc:
        raise ContextoOperativoObsoleto("El contexto operativo no es válido.") from exc

    actual, lineas = _construir_contexto(ruta=ruta, actor=actor, firmar=False, bloquear=bloquear)
    if firmado.get("ruta_id") != actual.ruta_id or firmado.get("chofer_autorizado_id") != actual.chofer_autorizado_id:
        raise ContextoOperativoObsoleto("La ruta o el chofer autorizado cambiaron.")
    if firmado.get("unidad_id") != actual.unidad_id:
        raise ContextoOperativoObsoleto("La unidad operativa de la ruta cambió.")
    if firmado.get("tramo_id") != actual.tramo_id:
        raise ContextoOperativoObsoleto("La ruta avanzó a otro tramo.", codigo="tramo_cambiado")
    if firmado.get("version_checklist") != actual.version_checklist:
        anteriores = firmado.get("lineas") or {}
        actuales = {str(linea.id): list(_linea_fact(linea)[1:]) for linea in lineas}
        afectados = {
            int(linea_id)
            for linea_id in set(anteriores) | set(actuales)
            if anteriores.get(linea_id) != actuales.get(linea_id)
        }
        raise ContextoOperativoObsoleto(
            "La carga de Point o el checklist cambiaron.",
            codigo="checklist_actualizado",
            productos_afectados=afectados,
        )
    return ContextoOperativo(**{**actual.__dict__, "token": token})
