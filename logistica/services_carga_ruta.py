from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import json

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.db.models import Count, Prefetch, Q
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from core.access import can_manage_submodule
from core.models import Notificacion, Sucursal
from core.notificaciones import crear_notificaciones
from pos_bridge.models import PointTransferLine
from pos_bridge.services.movement_sync_service import PointMovementSyncService
from pos_bridge.services.open_transfer_sync_service import (
    OpenTransferSyncService,
    is_cedis_like_name,
    resolve_requesting_erp_branch,
)
from pos_bridge.utils.helpers import normalize_text
from recetas.models import SolicitudReabastoCedis, SolicitudReabastoCedisLinea

from .domain_ruta import parada_resuelta_operativamente, point_transfer_enviada
from .models import (
    EventoRuta,
    ParadaEntregaEvidencia,
    ParadaRuta,
    PuntoLogistico,
    RutaCargaChecklist,
    RutaCargaChecklistLinea,
    RutaEntrega,
)
from .services_rutas_control import repartidor_participa_en_ruta

POINT_PENDIENTE_ENVIO_NOTA = (
    "La carga aún no aparece enviada en Point. "
    "Pide a logística que atienda o actualice la transferencia en Point."
)


class RecargaCedisPointError(ValidationError):
    estado_sync = "ERROR_POINT"
    http_status = 503
    jefe_notificado = True


class PointSyncUnavailableError(ValidationError):
    def __init__(self, message, *, sync_job=None):
        super().__init__(message)
        self.sync_job = sync_job


class RecargaCedisPendienteEnviado(ValidationError):
    estado_sync = "PENDIENTE_ENVIADO"
    http_status = 409
    jefe_notificado = True


class RecargaCedisSinLineasPoint(ValidationError):
    estado_sync = "SIN_LINEAS_POINT"
    http_status = 409
    jefe_notificado = True


class RecargaCedisSnapshotObsoleto(ValidationError):
    estado_sync = "SNAPSHOT_OBSOLETO"
    http_status = 409
    jefe_notificado = False


@dataclass(frozen=True)
class ChecklistCargaResumen:
    checklist: RutaCargaChecklist
    creadas: int = 0
    actualizadas: int = 0
    omitidas: int = 0


@dataclass(frozen=True)
class RecepcionPointResumen:
    ruta: RutaEntrega
    evidencias_creadas: int = 0
    evidencias_existentes: int = 0
    paradas_actualizadas: int = 0
    lineas_recibidas: int = 0
    lineas_pendientes_point: int = 0


def _cantidad_esperada(line: PointTransferLine) -> Decimal:
    return Decimal(str(line.sent_quantity or 0))


def _identidad_producto_carga(*, item_code: str, item_name: str, unit: str) -> tuple[str, str, str]:
    """Identidad estable: código y unidad; nombre sólo cuando Point no aporta código."""
    code = " ".join(str(item_code or "").split()).upper()
    name = " ".join(str(item_name or "").split()).upper()
    normalized_unit = " ".join(str(unit or "").split()).upper()
    return (code, "" if code else name, normalized_unit)


def _paradas_por_sucursal(ruta: RutaEntrega) -> dict[int, ParadaRuta]:
    paradas = ruta.paradas.select_related("punto", "punto__sucursal").order_by("orden", "id")
    result = {}
    sucursales = list(Sucursal.objects.filter(activa=True).only("id", "nombre"))
    for parada in paradas:
        if parada.punto.tipo == PuntoLogistico.TIPO_CEDIS:
            continue
        if parada.punto.sucursal_id and parada.punto.sucursal_id not in result:
            result[parada.punto.sucursal_id] = parada
            normalized = normalize_text(parada.punto.sucursal.nombre)
            aliases = [sucursal for sucursal in sucursales if sucursal.id != parada.punto.sucursal_id and normalize_text(sucursal.nombre) in normalized]
            if len(aliases) == 1:
                result.setdefault(aliases[0].id, parada)
    return result


def _cantidad_referencia_entrega(linea: RutaCargaChecklistLinea) -> Decimal:
    if linea.cantidad_cargada is not None:
        return Decimal(str(linea.cantidad_cargada or 0))
    return Decimal(str(linea.cantidad_enviada_esperada or 0))


def _linea_pendiente_envio_point(linea: RutaCargaChecklistLinea) -> bool:
    return (
        linea.estatus == RutaCargaChecklistLinea.ESTATUS_PENDIENTE
        and linea.point_transfer_line_id is not None
        and not point_transfer_enviada(linea.point_transfer_line)
    )


def _estatus_carga_para_cantidades(*, cargada: Decimal, esperada: Decimal) -> str:
    if cargada == esperada:
        return RutaCargaChecklistLinea.ESTATUS_CARGADA
    if cargada == 0:
        return RutaCargaChecklistLinea.ESTATUS_FALTANTE
    if cargada < esperada:
        return RutaCargaChecklistLinea.ESTATUS_PARCIAL
    return RutaCargaChecklistLinea.ESTATUS_SOBRANTE


def obtener_checklist_carga(ruta: RutaEntrega) -> RutaCargaChecklist:
    checklist, _ = RutaCargaChecklist.objects.get_or_create(
        ruta=ruta,
        defaults={"estatus": RutaCargaChecklist.ESTATUS_PENDIENTE},
    )
    return checklist


def _ids_paradas_con_recarga_cedis(ruta: RutaEntrega) -> set[int]:
    return set(
        EventoRuta.objects.filter(ruta=ruta)
        .filter(
            Q(tipo=EventoRuta.TIPO_RECARGA_CEDIS)
            | Q(
                tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL,
                metadata__tipo__in=["recarga_cedis", "recarga_cedis_pwa"],
            )
        )
        .exclude(parada_id__isnull=True)
        .values_list("parada_id", flat=True)
    )


def _ordenes_tramo_carga_actual(ruta: RutaEntrega) -> set[int] | None:
    paradas = list(ruta.paradas.select_related("punto").order_by("orden", "id"))
    cedis = [parada for parada in paradas if parada.punto and parada.punto.tipo == PuntoLogistico.TIPO_CEDIS]
    if not cedis:
        return None

    cedis_con_recarga = _ids_paradas_con_recarga_cedis(ruta)
    inicio = cedis[0].orden if cedis[0].orden == 1 else None
    for cedis_parada in cedis:
        if inicio is not None and cedis_parada.orden <= inicio:
            continue
        if cedis_parada.id in cedis_con_recarga:
            inicio = cedis_parada.orden
            continue
        break
    fin = next((parada.orden for parada in cedis if inicio is None or parada.orden > inicio), None)

    return {
        parada.orden
        for parada in paradas
        if parada.punto
        and parada.punto.tipo != PuntoLogistico.TIPO_CEDIS
        and (inicio is None or parada.orden > inicio)
        and (fin is None or parada.orden < fin)
    }


def obtener_checklist_carga_detallado(
    ruta: RutaEntrega, *, solo_tramo_actual: bool = False, excluir_superadas: bool = False
) -> RutaCargaChecklist:
    checklist = obtener_checklist_carga(ruta)
    lineas_qs = RutaCargaChecklistLinea.objects.select_related(
        "parada",
        "point_transfer_line",
        "validado_por",
        "validado_por__empleado_rrhh",
    ).order_by("parada__orden", "item_name", "id")
    if solo_tramo_actual:
        ordenes = _ordenes_tramo_carga_actual(ruta)
        if ordenes is not None:
            lineas_qs = lineas_qs.filter(parada__orden__in=ordenes)
    if excluir_superadas:
        lineas_qs = lineas_qs.exclude(estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA)
    return (
        RutaCargaChecklist.objects.select_related("ruta")
        .prefetch_related(Prefetch("lineas", queryset=lineas_qs))
        .get(pk=checklist.pk)
    )


def _limpiar_pendientes_antes_tramo_actual(*, ruta: RutaEntrega, checklist: RutaCargaChecklist) -> int:
    ordenes = _ordenes_tramo_carga_actual(ruta)
    if not ordenes:
        return 0
    return checklist.lineas.filter(
        estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE,
        parada__orden__lt=min(ordenes),
    ).delete()[0]


def _superar_versiones_snapshot_point_anteriores(
    *, lineas: list[RutaCargaChecklistLinea]
) -> int:
    """Conserva snapshots anteriores del mismo folio sólo como auditoría."""
    actuales_por_folio: dict[str, list[RutaCargaChecklistLinea]] = {}
    for linea in lineas:
        point_line = linea.point_transfer_line
        if (
            linea.estatus != RutaCargaChecklistLinea.ESTATUS_SUPERADA
            and point_line is not None
            and point_line.is_current_snapshot
        ):
            actuales_por_folio.setdefault(linea.transfer_external_id, []).append(linea)

    superadas = 0
    for anterior in lineas:
        point_line = anterior.point_transfer_line
        if (
            anterior.estatus == RutaCargaChecklistLinea.ESTATUS_SUPERADA
            or point_line is None
            or point_line.is_current_snapshot
        ):
            continue
        actuales = actuales_por_folio.get(anterior.transfer_external_id, [])
        if not actuales:
            continue
        identidad_anterior = _identidad_producto_carga(
            item_code=anterior.item_code,
            item_name=anterior.item_name,
            unit=anterior.unit,
        )
        equivalentes = [
            actual
            for actual in actuales
            if _identidad_producto_carga(
                item_code=actual.item_code,
                item_name=actual.item_name,
                unit=actual.unit,
            )
            == identidad_anterior
        ]
        anterior.estatus = RutaCargaChecklistLinea.ESTATUS_SUPERADA
        anterior.superada_por = equivalentes[0] if len(equivalentes) == 1 else None
        nota = "Snapshot anterior del mismo folio Point; datos y evidencia conservados para auditoría."
        if nota not in anterior.notas:
            anterior.notas = " ".join(
                value for value in [anterior.notas.strip(), nota] if value
            )
        anterior.save(
            update_fields=["estatus", "superada_por", "notas", "actualizado_en"]
        )
        superadas += 1
    return superadas


def _superar_lineas_point_canceladas(
    *, lineas: list[RutaCargaChecklistLinea]
) -> int:
    superadas = 0
    for linea in lineas:
        point_line = linea.point_transfer_line
        if (
            linea.estatus == RutaCargaChecklistLinea.ESTATUS_SUPERADA
            or point_line is None
            or not point_line.is_cancelled
        ):
            continue
        linea.estatus = RutaCargaChecklistLinea.ESTATUS_SUPERADA
        linea.superada_por = None
        nota = "Point canceló este folio; datos y evidencia conservados para auditoría."
        if nota not in linea.notas:
            linea.notas = " ".join(
                value for value in [linea.notas.strip(), nota] if value
            )
        linea.save(
            update_fields=["estatus", "superada_por", "notas", "actualizado_en"]
        )
        superadas += 1
    return superadas


def _sincronizar_lineas_point_para_ruta(*, ruta: RutaEntrega, checklist: RutaCargaChecklist, solo_abiertas: bool = False) -> tuple[int, int, int]:
    paradas_by_branch = _paradas_por_sucursal(ruta)
    if not paradas_by_branch:
        return 0, 0, 0

    branch_ids = set(paradas_by_branch)
    candidates_qs = (
        PointTransferLine.objects.select_related("erp_origin_branch", "erp_destination_branch", "origin_branch", "destination_branch")
        .filter(
            is_cancelled=False,
            is_current_snapshot=True,
            registered_at__date__in=[ruta.fecha_ruta - timedelta(days=1), ruta.fecha_ruta],
        )
        .filter(erp_destination_branch_id__in=branch_ids)
        .order_by("transfer_external_id", "detail_external_id", "id")
    )

    # _actualizar_checklist_carga_desde_point abre la transacción que contiene esta
    # función. Bloquear primero las líneas Point serializa dos rutas que intenten
    # reclamar el mismo detalle, incluso si una conserva un source_hash CEDIS.
    candidates = list(candidates_qs.select_for_update(of=("self",)))
    candidate_ids = [line.id for line in candidates]
    candidate_hashes = [line.source_hash for line in candidates]
    hashes_ocupados_otras_rutas = set(
        RutaCargaChecklistLinea.objects.filter(source_hash__in=candidate_hashes)
        .exclude(checklist=checklist)
        .values_list("source_hash", flat=True)
    )
    reservas_activas = list(
        RutaCargaChecklistLinea.objects.filter(
            Q(source_hash__in=candidate_hashes) | Q(point_transfer_line_id__in=candidate_ids)
        )
        .exclude(estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        .order_by("point_transfer_line_id", "creado_en", "id")
        .values_list("source_hash", "point_transfer_line_id", "checklist_id")
    )
    propietario_por_point_id = {}
    for _, point_id, checklist_id in reservas_activas:
        if point_id is not None:
            propietario_por_point_id.setdefault(point_id, checklist_id)
    hashes_reservados = {
        source_hash
        for source_hash, point_id, checklist_id in reservas_activas
        if checklist_id != checklist.id
        and (
            point_id is None
            or propietario_por_point_id.get(point_id) != checklist.id
        )
    }
    point_ids_reservados = {
        point_id
        for point_id, checklist_id in propietario_por_point_id.items()
        if checklist_id != checklist.id
    }
    checklist_lines = list(
        checklist.lineas.select_for_update(of=("self",)).select_related(
            "point_transfer_line",
            "point_transfer_line__destination_branch",
        )
    )
    lineas_superadas = 0
    activas_por_point_id: dict[int, list[RutaCargaChecklistLinea]] = {}
    for linea in checklist_lines:
        if (
            linea.point_transfer_line_id is not None
            and linea.estatus != RutaCargaChecklistLinea.ESTATUS_SUPERADA
        ):
            activas_por_point_id.setdefault(linea.point_transfer_line_id, []).append(linea)
    for duplicadas in activas_por_point_id.values():
        if len(duplicadas) <= 1:
            continue
        point_line = duplicadas[0].point_transfer_line
        propietaria = max(
            duplicadas,
            key=lambda linea: (
                linea.source_hash == point_line.source_hash,
                linea.detail_external_id == point_line.detail_external_id,
                linea.transfer_external_id == point_line.transfer_external_id,
                -linea.id,
            ),
        )
        for linea in duplicadas:
            if linea.id == propietaria.id:
                continue
            linea.estatus = RutaCargaChecklistLinea.ESTATUS_SUPERADA
            linea.superada_por = propietaria
            linea.notas = " ".join(
                value
                for value in [
                    linea.notas.strip(),
                    "Fila duplicada de la misma línea Point; se conserva solo para auditoría.",
                ]
                if value
            )
            linea.save(
                update_fields=[
                    "estatus",
                    "superada_por",
                    "notas",
                    "actualizado_en",
                ]
            )
            lineas_superadas += 1
    for linea in checklist_lines:
        point_line = linea.point_transfer_line
        retorno_a_cedis = bool(
            point_line
            and point_line.erp_origin_branch_id in branch_ids
            and point_line.erp_destination_branch_id not in branch_ids
            and point_line.destination_branch_id
            and is_cedis_like_name(point_line.destination_branch.name)
        )
        reservada_por_otra_ruta = (
            linea.point_transfer_line_id in point_ids_reservados
            and linea.estatus != RutaCargaChecklistLinea.ESTATUS_SUPERADA
        )
        if retorno_a_cedis or reservada_por_otra_ruta:
            if linea.estatus == RutaCargaChecklistLinea.ESTATUS_SUPERADA:
                continue
            linea.estatus = RutaCargaChecklistLinea.ESTATUS_SUPERADA
            nota_auditoria = (
                "Transferencia de regreso a CEDIS; se conserva solo para auditoría."
                if retorno_a_cedis
                else "Línea Point ya asignada a otra ruta; se conserva solo para auditoría."
            )
            linea.notas = " ".join(
                value
                for value in [
                    linea.notas.strip(),
                    nota_auditoria,
                ]
                if value
            )
            linea.save(update_fields=["estatus", "notas", "actualizado_en"])
            lineas_superadas += 1
    lineas_por_source = {
        linea.source_hash: linea
        for linea in checklist_lines
        if linea.estatus != RutaCargaChecklistLinea.ESTATUS_SUPERADA
    }
    lineas_por_point_id = {
        linea.point_transfer_line_id: linea
        for linea in checklist_lines
        if linea.point_transfer_line_id is not None
        and linea.estatus != RutaCargaChecklistLinea.ESTATUS_SUPERADA
    }
    todas_por_source = {linea.source_hash: linea for linea in checklist_lines}
    todas_por_point_id = {
        linea.point_transfer_line_id: linea
        for linea in checklist_lines
        if linea.point_transfer_line_id is not None
    }

    creadas = 0
    actualizadas = lineas_superadas
    omitidas = 0
    for line in candidates:
        branch = resolve_requesting_erp_branch(line)
        if branch is None or branch.id not in paradas_by_branch:
            omitidas += 1
            continue
        if line.source_hash in hashes_reservados or line.id in point_ids_reservados:
            omitidas += 1
            continue
        cantidad_esperada = _cantidad_esperada(line)
        enviada = point_transfer_enviada(line)
        parada = paradas_by_branch[branch.id]
        producto_key = _point_producto_key(line)
        cedis_line = None
        if producto_key:
            for existing in checklist_lines:
                if (
                    existing.parada_id == parada.id
                    and existing.source_hash.startswith("cedis-reabasto-")
                    and existing.point_transfer_line_id is None
                    and _linea_producto_key(existing) == producto_key
                ):
                    cedis_line = existing
                    break
        if cedis_line:
            esperada_anterior = Decimal(str(cedis_line.cantidad_enviada_esperada or 0))
            cedis_line.point_transfer_line = line
            cedis_line.transfer_external_id = line.transfer_external_id
            cedis_line.detail_external_id = line.detail_external_id
            cedis_line.unit = line.unit
            cedis_line.erp_origin_branch = line.erp_origin_branch
            cedis_line.erp_destination_branch = line.erp_destination_branch
            cedis_line.cantidad_enviada_esperada = cantidad_esperada
            if cedis_line.estatus in {
                RutaCargaChecklistLinea.ESTATUS_PENDIENTE,
                RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED,
            }:
                if not enviada:
                    cedis_line.cantidad_cargada = None
                    cedis_line.estatus = RutaCargaChecklistLinea.ESTATUS_PENDIENTE
                    cedis_line.notas = "Point aún no registra Enviado para esta solicitud."
                elif cantidad_esperada <= 0:
                    cedis_line.cantidad_cargada = Decimal("0")
                    cedis_line.estatus = RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED
                    cedis_line.notas = "Point confirmó enviado final en cero; no requiere captura."
                else:
                    cedis_line.cantidad_cargada = None
                    cedis_line.estatus = RutaCargaChecklistLinea.ESTATUS_PENDIENTE
                    cedis_line.notas = ""
            elif cedis_line.cantidad_cargada is not None and esperada_anterior != cantidad_esperada:
                cargada = Decimal(str(cedis_line.cantidad_cargada))
                cedis_line.estatus = _estatus_carga_para_cantidades(cargada=cargada, esperada=cantidad_esperada)
                if not (cedis_line.validado_por_id or cedis_line.validado_en or cedis_line.client_event_id):
                    cedis_line.motivo_diferencia = (
                        ""
                        if cedis_line.estatus == RutaCargaChecklistLinea.ESTATUS_CARGADA
                        else RutaCargaChecklistLinea.MOTIVO_OTRO
                    )
                nota_cambio = f"Point actualizó enviado de {esperada_anterior} a {cantidad_esperada}; captura conservada en {cargada}."
                cedis_line.notas = " ".join(value for value in [cedis_line.notas.strip(), nota_cambio] if value)
            cedis_line.save(
                update_fields=[
                    "point_transfer_line",
                    "transfer_external_id",
                    "detail_external_id",
                    "unit",
                    "erp_origin_branch",
                    "erp_destination_branch",
                    "cantidad_enviada_esperada",
                    "cantidad_cargada",
                    "estatus",
                    "motivo_diferencia",
                    "notas",
                    "actualizado_en",
                ]
            )
            lineas_por_point_id[line.id] = cedis_line
            actualizadas += 1
            continue
        existing = (
            lineas_por_point_id.get(line.id)
            or lineas_por_source.get(line.source_hash)
            or todas_por_point_id.get(line.id)
            or todas_por_source.get(line.source_hash)
        )
        defaults = {
            "parada": parada,
            "point_transfer_line": line,
            "transfer_external_id": line.transfer_external_id,
            "detail_external_id": line.detail_external_id,
            "item_code": line.item_code,
            "item_name": line.item_name,
            "unit": line.unit,
            "erp_origin_branch": line.erp_origin_branch,
            "erp_destination_branch": line.erp_destination_branch,
            "cantidad_solicitada": line.requested_quantity,
            "cantidad_enviada_esperada": cantidad_esperada,
            "notas": (
                "Point aún no registra Enviado para esta solicitud."
                if not enviada
                else "Point confirmó enviado final en cero; no requiere captura."
                if cantidad_esperada <= 0
                else ""
            ),
        }
        if existing:
            # Esta misma PointTransferLine ya está adjunta a una fila de este checklist
            # (por source_hash directo o porque un resync previo la fusionó con un
            # placeholder de CEDIS). Nunca se crea una fila nueva para la misma línea de
            # Point: siempre se actualiza la que ya existe, sin importar su estatus actual.
            esperada_anterior = Decimal(str(existing.cantidad_enviada_esperada or 0))
            existing.point_transfer_line = line
            existing.transfer_external_id = line.transfer_external_id
            existing.detail_external_id = line.detail_external_id
            existing.cantidad_solicitada = line.requested_quantity
            existing.cantidad_enviada_esperada = cantidad_esperada
            update_fields = [
                "point_transfer_line",
                "transfer_external_id",
                "detail_external_id",
                "cantidad_solicitada",
                "cantidad_enviada_esperada",
                "actualizado_en",
            ]
            tiene_captura_humana = bool(
                existing.validado_por_id
                or existing.validado_en
                or existing.client_event_id
                or existing.evidencias_entrega.exists()
            )
            reactivada = existing.estatus == RutaCargaChecklistLinea.ESTATUS_SUPERADA and enviada
            if reactivada:
                existing.superada_por = None
                if tiene_captura_humana and existing.cantidad_cargada is not None:
                    existing.estatus = _estatus_carga_para_cantidades(
                        cargada=Decimal(str(existing.cantidad_cargada)),
                        esperada=cantidad_esperada,
                    )
                else:
                    existing.cantidad_cargada = Decimal("0") if cantidad_esperada <= 0 else None
                    existing.estatus = (
                        RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED
                        if cantidad_esperada <= 0
                        else RutaCargaChecklistLinea.ESTATUS_PENDIENTE
                    )
                nota_reactivacion = "Point confirmó posteriormente este folio como Enviado; línea reactivada."
                if nota_reactivacion not in existing.notas:
                    existing.notas = " ".join(
                        value for value in [existing.notas.strip(), nota_reactivacion] if value
                    )
                update_fields.extend(["superada_por", "cantidad_cargada", "estatus", "notas"])
                lineas_por_point_id[line.id] = existing
                lineas_por_source[existing.source_hash] = existing
            if not reactivada and existing.estatus in {
                RutaCargaChecklistLinea.ESTATUS_PENDIENTE,
                RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED,
            }:
                if tiene_captura_humana and existing.cantidad_cargada is not None:
                    if enviada:
                        cargada = Decimal(str(existing.cantidad_cargada))
                        existing.estatus = _estatus_carga_para_cantidades(
                            cargada=cargada,
                            esperada=cantidad_esperada,
                        )
                        nota_cambio = (
                            f"Point actualizó enviado de {esperada_anterior} a "
                            f"{cantidad_esperada}; captura conservada en {cargada}."
                        )
                        existing.notas = " ".join(
                            value for value in [existing.notas.strip(), nota_cambio] if value
                        )
                        update_fields.extend(["estatus", "notas"])
                elif not enviada:
                    existing.cantidad_cargada = None
                    existing.estatus = RutaCargaChecklistLinea.ESTATUS_PENDIENTE
                    existing.notas = "Point aún no registra Enviado para esta solicitud."
                elif cantidad_esperada <= 0:
                    existing.cantidad_cargada = Decimal("0")
                    existing.estatus = RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED
                    existing.notas = "Point confirmó enviado final en cero; no requiere captura."
                else:
                    existing.cantidad_cargada = None
                    existing.estatus = RutaCargaChecklistLinea.ESTATUS_PENDIENTE
                    existing.notas = ""
                if not tiene_captura_humana or existing.cantidad_cargada is None:
                    update_fields.extend(["cantidad_cargada", "estatus", "notas"])
            elif (
                not reactivada
                and existing.cantidad_cargada is not None
                and esperada_anterior != cantidad_esperada
            ):
                cargada = Decimal(str(existing.cantidad_cargada))
                existing.estatus = _estatus_carga_para_cantidades(cargada=cargada, esperada=cantidad_esperada)
                nota_cambio = f"Point actualizó enviado de {esperada_anterior} a {cantidad_esperada}; captura conservada en {cargada}."
                existing.notas = " ".join(value for value in [existing.notas.strip(), nota_cambio] if value)
                update_fields.extend(["estatus", "notas"])
                if not (existing.validado_por_id or existing.validado_en or existing.client_event_id):
                    existing.motivo_diferencia = (
                        ""
                        if existing.estatus == RutaCargaChecklistLinea.ESTATUS_CARGADA
                        else RutaCargaChecklistLinea.MOTIVO_OTRO
                    )
                    update_fields.append("motivo_diferencia")
            existing.save(
                update_fields=update_fields
            )
            actualizadas += 1
            continue
        defaults.update(
            cantidad_cargada=Decimal("0") if enviada and cantidad_esperada <= 0 else None,
            estatus=(
                RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED
                if enviada and cantidad_esperada <= 0
                else RutaCargaChecklistLinea.ESTATUS_PENDIENTE
            ),
        )
        source_hash_nuevo = line.source_hash
        if source_hash_nuevo in hashes_ocupados_otras_rutas:
            source_hash_nuevo = f"point-ruta-{ruta.id}-{line.id}"
        nueva_linea = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            source_hash=source_hash_nuevo,
            **defaults,
        )
        checklist_lines.append(nueva_linea)
        lineas_por_source[nueva_linea.source_hash] = nueva_linea
        lineas_por_point_id[line.id] = nueva_linea
        creadas += 1
    actualizadas += _superar_versiones_snapshot_point_anteriores(
        lineas=checklist_lines,
    )
    actualizadas += _superar_lineas_point_canceladas(
        lineas=checklist_lines,
    )
    return creadas, actualizadas, omitidas


def ruta_tiene_movimiento_point_nuevo(*, fecha, puntos: list[PuntoLogistico]) -> bool:
    branch_ids = {punto.sucursal_id for punto in puntos if punto.tipo != PuntoLogistico.TIPO_CEDIS and punto.sucursal_id}
    if not branch_ids:
        return True
    base_qs = (
        PointTransferLine.objects.filter(
            is_cancelled=False,
            is_current_snapshot=True,
            registered_at__date__in=[fecha - timedelta(days=1), fecha],
        )
        .exclude(source_hash__in=RutaCargaChecklistLinea.objects.exclude(source_hash="").values("source_hash"))
    )
    sucursales_con_movimiento = set()
    for linea in base_qs.iterator():
        if not point_transfer_enviada(linea):
            continue
        if linea.erp_origin_branch_id:
            sucursales_con_movimiento.add(linea.erp_origin_branch_id)
        if linea.erp_destination_branch_id:
            sucursales_con_movimiento.add(linea.erp_destination_branch_id)
    return branch_ids.issubset(sucursales_con_movimiento)


def _sincronizar_lineas_consolidado_para_ruta(*, ruta: RutaEntrega, checklist: RutaCargaChecklist) -> tuple[int, int, int]:
    paradas_by_branch = _paradas_por_sucursal(ruta)
    if not paradas_by_branch:
        return 0, 0, 0

    lineas = (
        SolicitudReabastoCedisLinea.objects.select_related("solicitud", "solicitud__sucursal", "receta")
        .filter(
            solicitud__fecha_operacion=ruta.fecha_ruta,
            solicitud__sucursal_id__in=set(paradas_by_branch),
            solicitud__estado__in=[
                SolicitudReabastoCedis.ESTADO_ENVIADA,
                SolicitudReabastoCedis.ESTADO_ATENDIDA,
            ],
        )
        .order_by("solicitud__sucursal__codigo", "receta__nombre", "id")
    )
    creadas = 0
    actualizadas = 0
    omitidas = 0
    for linea in lineas:
        cantidad = Decimal(str(linea.solicitado or 0))
        source_hash = f"cedis-reabasto-{ruta.fecha_ruta:%Y%m%d}-{linea.solicitud.sucursal_id}-{linea.receta_id}"
        if cantidad <= 0:
            RutaCargaChecklistLinea.objects.filter(
                checklist=checklist,
                source_hash=source_hash,
                estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE,
            ).delete()
            omitidas += 1
            continue
        if RutaCargaChecklistLinea.objects.filter(source_hash=source_hash).exclude(checklist=checklist).exists():
            omitidas += 1
            continue
        existing = RutaCargaChecklistLinea.objects.filter(checklist=checklist, source_hash=source_hash).first()
        if existing and existing.estatus != RutaCargaChecklistLinea.ESTATUS_PENDIENTE:
            omitidas += 1
            continue
        receta = linea.receta
        _, created = RutaCargaChecklistLinea.objects.update_or_create(
            checklist=checklist,
            source_hash=source_hash,
            defaults={
                "parada": paradas_by_branch[linea.solicitud.sucursal_id],
                "point_transfer_line": None,
                "transfer_external_id": linea.solicitud.folio,
                "detail_external_id": str(linea.id),
                "item_code": receta.codigo_point or "",
                "item_name": receta.nombre,
                "unit": "",
                "erp_origin_branch": None,
                "erp_destination_branch": linea.solicitud.sucursal,
                "cantidad_solicitada": cantidad,
                "cantidad_enviada_esperada": Decimal("0"),
                "notas": POINT_PENDIENTE_ENVIO_NOTA,
            },
        )
        if created:
            creadas += 1
        else:
            actualizadas += 1
    return creadas, actualizadas, omitidas


def _producto_key(value: str) -> str:
    return (value or "").strip().lower()


def _linea_producto_key(linea: RutaCargaChecklistLinea) -> str:
    return _producto_key(linea.item_code or linea.item_name)


def _point_producto_key(line: PointTransferLine) -> str:
    return _producto_key(line.item_code or line.item_name)


def _point_recibidas_por_ruta(ruta: RutaEntrega) -> dict[tuple[int, str], list[PointTransferLine]]:
    paradas_by_branch = _paradas_por_sucursal(ruta)
    if not paradas_by_branch:
        return {}
    branch_ids = set(paradas_by_branch)
    result: dict[tuple[int, str], list[PointTransferLine]] = {}
    lines = (
        PointTransferLine.objects.select_related("erp_origin_branch", "erp_destination_branch", "origin_branch", "destination_branch")
        .filter(
            is_cancelled=False,
            is_current_snapshot=True,
            is_received=True,
            registered_at__date__in=[ruta.fecha_ruta - timedelta(days=1), ruta.fecha_ruta],
        )
        .filter(Q(erp_origin_branch_id__in=branch_ids) | Q(erp_destination_branch_id__in=branch_ids))
        .order_by("received_at", "id")
    )
    for line in lines:
        branch = resolve_requesting_erp_branch(line)
        key = _point_producto_key(line)
        if branch is None or branch.id not in branch_ids or not key:
            continue
        result.setdefault((branch.id, key), []).append(line)
    return result


def _evidencia_point(linea: RutaCargaChecklistLinea) -> ParadaEntregaEvidencia | None:
    return (
        ParadaEntregaEvidencia.objects.filter(linea_carga=linea, client_event_id__startswith="point-recepcion-")
        .order_by("-capturado_en", "-id")
        .first()
    )


def _cantidad_recibida_linea(linea: RutaCargaChecklistLinea) -> Decimal | None:
    evidencia = _evidencia_point(linea)
    if evidencia is not None:
        return Decimal(str(evidencia.cantidad_entregada or 0))
    if linea.point_transfer_line_id and linea.point_transfer_line.is_received:
        return Decimal(str(linea.point_transfer_line.received_quantity or 0))
    return None


def _confirmacion_completa_parada(parada: ParadaRuta) -> ParadaEntregaEvidencia | None:
    return (
        parada.evidencias_entrega.filter(linea_carga__isnull=True, tipo=ParadaEntregaEvidencia.TIPO_CONFIRMACION)
        .order_by("-capturado_en", "-id")
        .first()
    )


def _tiene_cierre_local(parada: ParadaRuta) -> bool:
    if parada.entrega_estado == ParadaRuta.ENTREGA_PENDIENTE:
        return False
    return parada.evidencias_entrega.exclude(client_event_id__startswith="point-recepcion-").exists()


def _cantidad_recibida_con_respaldo_pwa(
    linea: RutaCargaChecklistLinea,
    confirmacion: ParadaEntregaEvidencia | None,
) -> Decimal | None:
    recibida = _cantidad_recibida_linea(linea)
    if recibida is not None:
        return recibida
    if confirmacion is not None:
        return _cantidad_referencia_entrega(linea)
    return None


def _elegir_recepcion_point(linea: RutaCargaChecklistLinea, candidates: list[PointTransferLine]) -> list[PointTransferLine]:
    if len(candidates) <= 1:
        return candidates
    esperado = _cantidad_referencia_entrega(linea)
    exactas = [line for line in candidates if Decimal(str(line.received_quantity or 0)) == esperado]
    if not exactas:
        return []
    return [max(exactas, key=lambda line: (line.received_at or line.updated_at, line.id))]


def sincronizar_checklist_carga_desde_point(*, ruta: RutaEntrega, user=None, ejecutar_sync: bool = True) -> ChecklistCargaResumen:
    ruta = RutaEntrega.objects.get(pk=ruta.pk)
    if ruta.estatus not in {RutaEntrega.ESTATUS_PLANEADA, RutaEntrega.ESTATUS_EN_RUTA}:
        raise ValidationError("La carga solo se puede sincronizar mientras la ruta está planeada o en ruta.")
    if not _paradas_por_sucursal(ruta):
        raise ValidationError("La ruta no tiene paradas ligadas a sucursales para relacionar transferencias Point.")

    sync_job = None
    if ejecutar_sync:
        service = OpenTransferSyncService()
        for fecha in [ruta.fecha_ruta - timedelta(days=1), ruta.fecha_ruta]:
            sync_job = service.sync_open_transfers(fecha=fecha, triggered_by=user)
            if sync_job.status != sync_job.STATUS_SUCCESS:
                raise PointSyncUnavailableError(
                    "No se pudo sincronizar Point para generar la carga esperada.",
                    sync_job=sync_job,
                )

    return _actualizar_checklist_carga_desde_point(ruta=ruta, user=user, sync_job=sync_job)


def sincronizar_checklist_recarga_desde_point(*, ruta: RutaEntrega, user=None) -> ChecklistCargaResumen:
    ruta = RutaEntrega.objects.get(pk=ruta.pk)
    if ruta.estatus not in {RutaEntrega.ESTATUS_PLANEADA, RutaEntrega.ESTATUS_EN_RUTA}:
        raise ValidationError("La carga solo se puede sincronizar mientras la ruta está planeada o en ruta.")
    if not _paradas_por_sucursal(ruta):
        raise ValidationError("La ruta no tiene paradas ligadas a sucursales para relacionar transferencias Point.")

    service = PointMovementSyncService()
    sync_job = None
    for fecha in [ruta.fecha_ruta - timedelta(days=1), ruta.fecha_ruta]:
        sync_job = service.run_transfer_sync(
            start_date=fecha,
            end_date=fecha,
            triggered_by=user,
        )
        if sync_job.status != sync_job.STATUS_SUCCESS:
            raise PointSyncUnavailableError(
                "No se pudo completar la recarga de transferencias Point.",
                sync_job=sync_job,
            )

    return _actualizar_checklist_carga_desde_point(
        ruta=ruta,
        user=user,
        sync_job=sync_job,
        solo_abiertas=False,
    )


@transaction.atomic
def _actualizar_checklist_carga_desde_point(
    *,
    ruta: RutaEntrega,
    user=None,
    sync_job=None,
    solo_abiertas: bool = True,
) -> ChecklistCargaResumen:
    ruta = RutaEntrega.objects.select_for_update().get(pk=ruta.pk)
    if ruta.estatus not in {RutaEntrega.ESTATUS_PLANEADA, RutaEntrega.ESTATUS_EN_RUTA}:
        raise ValidationError("La carga solo se puede sincronizar mientras la ruta está planeada o en ruta.")

    paradas_by_branch = _paradas_por_sucursal(ruta)
    if not paradas_by_branch:
        raise ValidationError("La ruta no tiene paradas ligadas a sucursales para relacionar transferencias Point.")
    checklist = RutaCargaChecklist.objects.select_for_update().filter(ruta=ruta).first()
    checklist = checklist or obtener_checklist_carga(ruta)
    hechos_point_antes = dict(checklist.lineas.values_list("source_hash", "cantidad_enviada_esperada"))
    checklist.point_sync_job = sync_job or checklist.point_sync_job
    sync_fields = ["point_sync_job", "estatus", "actualizado_en"]
    if sync_job is not None:
        checklist.sincronizado_en = timezone.now()
        sync_fields.append("sincronizado_en")
    if checklist.estatus == RutaCargaChecklist.ESTATUS_PENDIENTE:
        checklist.estatus = RutaCargaChecklist.ESTATUS_EN_REVISION
    checklist.save(update_fields=sync_fields)
    omitidas = checklist.lineas.filter(parada__punto__tipo=PuntoLogistico.TIPO_CEDIS, estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE).delete()[0]

    creadas, actualizadas, omitidas_consolidado = _sincronizar_lineas_consolidado_para_ruta(ruta=ruta, checklist=checklist)
    omitidas += omitidas_consolidado
    if creadas or actualizadas:
        checklist.lineas.filter(point_transfer_line__isnull=False, estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE).exclude(
            source_hash__startswith="cedis-reabasto-"
        ).delete()
        checklist.notas = "Carga esperada generada desde consolidado CEDIS."
        checklist.save(update_fields=["notas", "actualizado_en"])
    creadas_point, actualizadas_point, omitidas_point = _sincronizar_lineas_point_para_ruta(
        ruta=ruta,
        checklist=checklist,
        solo_abiertas=solo_abiertas,
    )
    creadas += creadas_point
    actualizadas += actualizadas_point
    omitidas += omitidas_point
    omitidas += _limpiar_pendientes_antes_tramo_actual(ruta=ruta, checklist=checklist)
    hechos_point_despues = dict(checklist.lineas.values_list("source_hash", "cantidad_enviada_esperada"))
    hechos_point_cambiaron = hechos_point_antes != hechos_point_despues

    if checklist.lineas.exists():
        if checklist.estatus == RutaCargaChecklist.ESTATUS_BLOQUEADA:
            checklist.estatus = RutaCargaChecklist.ESTATUS_EN_REVISION
            checklist.notas = ""
            checklist.save(update_fields=["estatus", "notas", "actualizado_en"])
        elif checklist.lineas.filter(estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE).exists():
            checklist.estatus = RutaCargaChecklist.ESTATUS_EN_REVISION
            checklist.save(update_fields=["estatus", "actualizado_en"])
        elif not checklist.lineas.exclude(
            estatus__in=[
                RutaCargaChecklistLinea.ESTATUS_CARGADA,
                RutaCargaChecklistLinea.ESTATUS_NO_APLICA,
                RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED,
                RutaCargaChecklistLinea.ESTATUS_SUPERADA,
            ]
        ).exists():
            checklist.estatus = RutaCargaChecklist.ESTATUS_CONFIRMADA
            checklist.confirmado_por = user
            checklist.confirmado_en = timezone.now()
            checklist.save(update_fields=["estatus", "confirmado_por", "confirmado_en", "actualizado_en"])
        else:
            checklist.estatus = RutaCargaChecklist.ESTATUS_CON_INCIDENCIA
            checklist.confirmado_por = None
            checklist.confirmado_en = None
            update_fields = ["estatus", "confirmado_por", "confirmado_en", "actualizado_en"]
            if hechos_point_cambiaron:
                checklist.motivo_override = ""
                update_fields.append("motivo_override")
            checklist.save(update_fields=update_fields)
    else:
        checklist.estatus = RutaCargaChecklist.ESTATUS_BLOQUEADA
        checklist.notas = (
            "No se encontraron transferencias abiertas de Point para las sucursales de esta ruta."
            if solo_abiertas
            else "No se encontraron transferencias de Point en el snapshot completo para las sucursales de esta ruta."
        )
        checklist.save(update_fields=["estatus", "notas", "actualizado_en"])

    return ChecklistCargaResumen(checklist=checklist, creadas=creadas, actualizadas=actualizadas, omitidas=omitidas)


def validar_usuario_puede_operar_checklist(*, user, ruta: RutaEntrega, repartidor) -> None:
    if ruta.estatus not in {RutaEntrega.ESTATUS_PLANEADA, RutaEntrega.ESTATUS_EN_RUTA}:
        raise ValidationError("La ruta no permite confirmar carga en este estatus.")
    if can_manage_submodule(user, "logistica", "rutas"):
        return
    if not repartidor_participa_en_ruta(ruta=ruta, repartidor=repartidor):
        raise PermissionDenied("No tienes permiso para confirmar carga de esta ruta.")


@transaction.atomic
def validar_linea_carga(
    *,
    user,
    ruta: RutaEntrega,
    repartidor,
    linea_id: int,
    cantidad_cargada,
    motivo_diferencia: str = "",
    notas: str = "",
    client_event_id: str = "",
    source_hash: str = "",
    transfer_external_id: str = "",
    detail_external_id: str = "",
    parada_id: int | None = None,
) -> RutaCargaChecklistLinea:
    validar_usuario_puede_operar_checklist(user=user, ruta=ruta, repartidor=repartidor)
    checklist = obtener_checklist_carga(ruta)
    lineas_actuales = RutaCargaChecklistLinea.objects.select_for_update().select_related("checklist", "parada").filter(
        checklist=checklist,
    )
    linea = lineas_actuales.filter(pk=linea_id).first()
    if not linea:
        candidatos = lineas_actuales.exclude(estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        source_hash = (source_hash or "").strip()
        transfer_external_id = (transfer_external_id or "").strip()
        detail_external_id = (detail_external_id or "").strip()
        if source_hash:
            candidatos = candidatos.filter(source_hash=source_hash)
        elif transfer_external_id and detail_external_id:
            candidatos = candidatos.filter(
                transfer_external_id=transfer_external_id,
                detail_external_id=detail_external_id,
            )
        else:
            raise ValidationError("La carga se actualizó desde Point. Actualiza la pantalla y vuelve a confirmar este producto.")
        if parada_id:
            candidatos = candidatos.filter(parada_id=parada_id)
        linea = candidatos.first()
        if not linea:
            raise ValidationError("La carga se actualizó desde Point y este producto ya no pertenece al tramo actual.")
    if linea.estatus == RutaCargaChecklistLinea.ESTATUS_SUPERADA:
        raise ValidationError("Esta línea fue superada por una transferencia de Point más reciente y ya no admite captura.")

    client_event_id = (client_event_id or "").strip()
    if client_event_id and linea.client_event_id == client_event_id and linea.estatus != RutaCargaChecklistLinea.ESTATUS_PENDIENTE:
        return linea

    try:
        cantidad = Decimal(str(cantidad_cargada))
    except (InvalidOperation, TypeError, ValueError):
        raise ValidationError("Captura una cantidad cargada válida.")
    if cantidad < 0:
        raise ValidationError("La cantidad cargada no puede ser negativa.")

    esperada = Decimal(str(linea.cantidad_enviada_esperada or 0))
    if esperada <= 0:
        raise ValidationError(POINT_PENDIENTE_ENVIO_NOTA)
    if cantidad == esperada:
        estatus = RutaCargaChecklistLinea.ESTATUS_CARGADA
        motivo_diferencia = ""
    elif cantidad == 0:
        estatus = RutaCargaChecklistLinea.ESTATUS_FALTANTE
    elif cantidad < esperada:
        estatus = RutaCargaChecklistLinea.ESTATUS_PARCIAL
    else:
        estatus = RutaCargaChecklistLinea.ESTATUS_SOBRANTE

    if estatus != RutaCargaChecklistLinea.ESTATUS_CARGADA and not motivo_diferencia:
        raise ValidationError("Selecciona el motivo de la diferencia antes de guardar.")

    linea.cantidad_cargada = cantidad
    linea.estatus = estatus
    linea.motivo_diferencia = motivo_diferencia
    linea.notas = notas or ""
    linea.client_event_id = client_event_id
    linea.validado_por = user
    linea.validado_en = timezone.now()
    linea.save(
        update_fields=[
            "cantidad_cargada",
            "estatus",
            "motivo_diferencia",
            "notas",
            "client_event_id",
            "validado_por",
            "validado_en",
            "actualizado_en",
        ]
    )

    estatus_previo = checklist.estatus
    pendientes = checklist.lineas.filter(estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE).exists()
    diferencias = checklist.lineas.exclude(
        estatus__in=[
            RutaCargaChecklistLinea.ESTATUS_CARGADA,
            RutaCargaChecklistLinea.ESTATUS_NO_APLICA,
            RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED,
            RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        ]
    ).exists()
    if pendientes:
        checklist.estatus = RutaCargaChecklist.ESTATUS_EN_REVISION
    elif diferencias:
        checklist.estatus = RutaCargaChecklist.ESTATUS_CON_INCIDENCIA
    else:
        checklist.estatus = RutaCargaChecklist.ESTATUS_CONFIRMADA
        checklist.confirmado_por = user
        checklist.confirmado_en = timezone.now()
    checklist.save(update_fields=["estatus", "confirmado_por", "confirmado_en", "actualizado_en"])

    if checklist.estatus == RutaCargaChecklist.ESTATUS_CON_INCIDENCIA and estatus_previo != RutaCargaChecklist.ESTATUS_CON_INCIDENCIA:
        crear_notificaciones(
            _usuarios_diferencia_carga(ruta),
            titulo=f"Diferencia de carga: {ruta.folio}",
            mensaje="La carga quedó completa con al menos una línea con diferencia. Logística debe autorizar la ruta con la diferencia.",
            url=f"/logistica/rutas/{ruta.id}/",
            tipo=Notificacion.TIPO_SISTEMA,
            prioridad=Notificacion.PRIORIDAD_ALTA,
            actor=user,
            objeto_tipo="logistica.RutaEntrega",
            objeto_id=ruta.id,
            excluir=user,
        )
    return linea


@transaction.atomic
def validar_producto_tramo_carga(
    *,
    user,
    ruta: RutaEntrega,
    repartidor,
    item_code: str,
    item_name: str,
    unit: str,
    cantidad_cargada,
    client_event_id: str = "",
) -> list[RutaCargaChecklistLinea]:
    """Confirma en una sola captura el total físico de un producto del tramo actual."""
    validar_usuario_puede_operar_checklist(user=user, ruta=ruta, repartidor=repartidor)
    checklist = obtener_checklist_carga(ruta)
    key = _identidad_producto_carga(item_code=item_code, item_name=item_name, unit=unit)
    lineas = [
        linea
        for linea in lineas_tramo_operativo_actual(ruta, checklist=checklist)
        .select_for_update()
        .select_related("parada")
        .exclude(estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        .order_by("parada__orden", "id")
        if _identidad_producto_carga(
            item_code=linea.item_code,
            item_name=linea.item_name,
            unit=linea.unit,
        ) == key
    ]
    if not lineas:
        raise ValidationError("El producto ya no pertenece al tramo de carga actual. Actualiza la pantalla.")

    try:
        cantidad = Decimal(str(cantidad_cargada))
    except (InvalidOperation, TypeError, ValueError):
        raise ValidationError("Captura una cantidad total cargada válida.")
    if cantidad < 0:
        raise ValidationError("La cantidad total cargada no puede ser negativa.")

    esperada = sum((Decimal(str(linea.cantidad_enviada_esperada or 0)) for linea in lineas), Decimal("0"))
    if cantidad != esperada:
        raise ValidationError(
            "El total cargado no coincide con la sumatoria enviada. "
            "Abre Ver desglose para registrar la diferencia por sucursal sin inventar su distribución."
        )

    base_event_id = (client_event_id or "").strip()
    for linea in lineas:
        if linea.estatus != RutaCargaChecklistLinea.ESTATUS_PENDIENTE:
            continue
        cantidad_linea = Decimal(str(linea.cantidad_enviada_esperada or 0))
        if cantidad_linea <= 0:
            continue
        validar_linea_carga(
            user=user,
            ruta=ruta,
            repartidor=repartidor,
            linea_id=linea.id,
            cantidad_cargada=cantidad_linea,
            client_event_id=f"{base_event_id}:{linea.id}" if base_event_id else "",
        )
        linea.refresh_from_db()
    return lineas


def checklist_bloquea_salida(ruta: RutaEntrega) -> str | None:
    checklist = getattr(ruta, "checklist_carga", None)
    if not checklist or not checklist.lineas.exists():
        return None
    if ruta.paradas.filter(punto__tipo=PuntoLogistico.TIPO_CEDIS).exists():
        lineas_salida = lineas_tramo_operativo_actual(
            ruta,
            checklist=checklist,
        ).select_related("point_transfer_line")
        if not lineas_salida.exists():
            return None
        if any(_linea_pendiente_envio_point(linea) for linea in lineas_salida):
            return POINT_PENDIENTE_ENVIO_NOTA
        if lineas_salida.filter(estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE).exists():
            return "confirma todas las líneas de carga antes de liberar la ruta"
        return None
    lineas = checklist.lineas.select_related("point_transfer_line").all()
    if any(_linea_pendiente_envio_point(linea) for linea in lineas):
        return POINT_PENDIENTE_ENVIO_NOTA
    if checklist.estatus == RutaCargaChecklist.ESTATUS_CONFIRMADA:
        return None
    if checklist.estatus == RutaCargaChecklist.ESTATUS_CON_INCIDENCIA:
        if checklist.motivo_override:
            return None
        return "logística debe autorizar la ruta con la diferencia"
    return "confirma todas las líneas de carga antes de liberar la ruta"


def lineas_tramo_operativo_actual(ruta: RutaEntrega, *, checklist: RutaCargaChecklist | None = None):
    checklist = checklist or getattr(ruta, "checklist_carga", None)
    if not checklist:
        return RutaCargaChecklistLinea.objects.none()
    lineas = checklist.lineas.all()
    ordenes = _ordenes_tramo_carga_actual(ruta)
    if ordenes is None:
        return lineas
    return lineas.filter(parada__orden__in=ordenes)


@transaction.atomic
def autorizar_diferencia_checklist_carga(*, ruta: RutaEntrega, user, autorizado: bool, notas: str = "") -> RutaCargaChecklist:
    checklist = RutaCargaChecklist.objects.select_for_update().filter(ruta=ruta).first()
    if not checklist:
        raise ValidationError("La ruta no tiene checklist de carga.")
    if checklist.estatus != RutaCargaChecklist.ESTATUS_CON_INCIDENCIA:
        raise ValidationError("Esta ruta no tiene una diferencia de carga pendiente de autorizar.")

    if autorizado:
        checklist.motivo_override = notas or "Diferencia de carga autorizada por logística."
        checklist.save(update_fields=["motivo_override", "actualizado_en"])
        descripcion = f"Logística autorizó liberar la ruta con diferencia de carga. {notas}".strip()
        severidad = EventoRuta.SEVERIDAD_INFO
    else:
        descripcion = f"Logística rechazó liberar la ruta con diferencia de carga. {notas}".strip()
        severidad = EventoRuta.SEVERIDAD_ALERTA

    EventoRuta.objects.create(
        ruta=ruta,
        tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL,
        severidad=severidad,
        descripcion=descripcion,
        metadata={"tipo": "autorizacion_diferencia_carga", "autorizado": autorizado, "notas": notas},
        creado_por=user,
    )
    return checklist


def _evento_recarga_existente(*, ruta_id: int, parada_id: int | None) -> EventoRuta | None:
    if parada_id is None:
        return None
    return (
        EventoRuta.objects.filter(ruta_id=ruta_id, parada_id=parada_id)
        .filter(
            Q(tipo=EventoRuta.TIPO_RECARGA_CEDIS)
            | Q(tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL, metadata__tipo__in=["recarga_cedis", "recarga_cedis_pwa"])
        )
        .order_by("id")
        .first()
    )


def _paradas_siguiente_tramo(ruta: RutaEntrega, parada: ParadaRuta):
    siguiente_cedis = (
        ruta.paradas.filter(punto__tipo=PuntoLogistico.TIPO_CEDIS, orden__gt=parada.orden)
        .order_by("orden", "id")
        .first()
    )
    paradas = ruta.paradas.filter(orden__gt=parada.orden).exclude(
        punto__tipo=PuntoLogistico.TIPO_CEDIS,
    )
    if siguiente_cedis is not None:
        paradas = paradas.filter(orden__lt=siguiente_cedis.orden)
    return paradas.select_related("punto", "punto__sucursal").order_by("orden", "id")


def _lineas_siguiente_tramo(ruta: RutaEntrega, parada: ParadaRuta):
    lineas = RutaCargaChecklistLinea.objects.filter(
        checklist__ruta=ruta,
        parada_id__in=_paradas_siguiente_tramo(ruta, parada).values("id"),
    ).exclude(estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA)
    return lineas.select_related(
        "point_transfer_line",
        "parada",
        "parada__punto",
        "parada__punto__sucursal",
    ).order_by("parada__orden", "id")


def _snapshot_siguiente_tramo(ruta: RutaEntrega, parada: ParadaRuta, *, actor) -> dict:
    lineas = []
    sucursales = {}
    checklist = (
        RutaCargaChecklist.objects.select_related("point_sync_job")
        .filter(ruta=ruta)
        .first()
    )
    for parada_tramo in _paradas_siguiente_tramo(ruta, parada):
        sucursal = parada_tramo.punto.sucursal
        if sucursal is not None:
            sucursales[sucursal.id] = {"id": sucursal.id, "nombre": sucursal.nombre}
    for linea in _lineas_siguiente_tramo(ruta, parada):
        point_line = linea.point_transfer_line
        lineas.append(
            {
                "linea_checklist_id": linea.id,
                "parada_id": linea.parada_id,
                "point_transfer_line_id": linea.point_transfer_line_id,
                "detail_external_id": linea.detail_external_id,
                "enviado": bool(point_line and point_transfer_enviada(point_line)),
                "cantidad_solicitada": str(linea.cantidad_solicitada),
                "cantidad_enviada": str(linea.cantidad_enviada_esperada),
            }
        )
    contenido_autorizable = {
        "ruta_id": ruta.id,
        "parada_id": parada.id,
        "sucursales": [sucursales[key] for key in sorted(sucursales)],
        "lineas": lineas,
    }
    metadata_sync = {
        "checklist_sincronizado_en": (
            checklist.sincronizado_en.isoformat()
            if checklist and checklist.sincronizado_en
            else None
        ),
        "point_sync_job_id": checklist.point_sync_job_id if checklist else None,
        "point_sync_job_status": (
            checklist.point_sync_job.status
            if checklist and checklist.point_sync_job_id
            else None
        ),
    }
    payload = {
        **contenido_autorizable,
        **metadata_sync,
        "actor_id": getattr(actor, "id", None),
        "capturado_en": timezone.now().isoformat(),
    }
    payload["snapshot_hash"] = hashlib.sha256(
        json.dumps(contenido_autorizable, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return payload


def _snapshot_obsoleto(*, expected_snapshot_hash: str, snapshot_actual: dict):
    if snapshot_actual["snapshot_hash"] == expected_snapshot_hash:
        return
    exc = RecargaCedisSnapshotObsoleto(
        "La carga del siguiente tramo cambió desde la revisión; actualiza y vuelve a revisar antes de autorizar."
    )
    exc.snapshot_hash = snapshot_actual["snapshot_hash"]
    raise exc


def _error_recarga_con_snapshot(exc: ValidationError, snapshot: dict) -> ValidationError:
    exc.snapshot_hash = snapshot["snapshot_hash"]
    return exc


def _snapshot_tiene_sync_externo_valido(snapshot: dict) -> bool:
    return bool(
        snapshot.get("checklist_sincronizado_en")
        and snapshot.get("point_sync_job_id")
        and snapshot.get("point_sync_job_status") == "SUCCESS"
    )


ESTADOS_ALERTA_RECARGA_REVISABLE = (
    "ERROR_POINT",
    "PENDIENTE_ENVIADO",
    "SIN_LINEAS_POINT",
)


def _fecha_observacion_alerta_recarga(evento: EventoRuta):
    raw = (evento.metadata or {}).get("ultima_observacion_en")
    observada_en = parse_datetime(raw) if isinstance(raw, str) else None
    if observada_en is None:
        observada_en = evento.creado_en
    if timezone.is_naive(observada_en):
        observada_en = timezone.make_aware(observada_en, timezone.get_current_timezone())
    return observada_en


def ultima_alerta_recarga_cedis_revisable(
    *, ruta: RutaEntrega, parada: ParadaRuta, bloquear: bool = False
) -> EventoRuta | None:
    alertas = EventoRuta.objects.filter(
        ruta=ruta,
        parada=parada,
        tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL,
        metadata__tipo="alerta_recarga_cedis_sync",
        metadata__estado_sync__in=ESTADOS_ALERTA_RECARGA_REVISABLE,
    ).order_by("id")
    if bloquear:
        alertas = alertas.select_for_update()
    candidatas = list(alertas)
    return max(
        candidatas,
        key=lambda evento: (
            _fecha_observacion_alerta_recarga(evento),
            evento.creado_en,
            evento.id,
        ),
        default=None,
    )


def _hash_alerta_recarga(evento: EventoRuta | None) -> str:
    if evento is None:
        return ""
    return ((evento.metadata or {}).get("snapshot") or {}).get("snapshot_hash") or ""


@transaction.atomic
def _validar_alerta_recarga_revisada(
    *, ruta: RutaEntrega, parada: ParadaRuta, expected_snapshot_hash: str
) -> EventoRuta:
    alerta = ultima_alerta_recarga_cedis_revisable(
        ruta=ruta,
        parada=parada,
        bloquear=True,
    )
    if _hash_alerta_recarga(alerta) != expected_snapshot_hash:
        raise ValidationError("La alerta revisada ya no está disponible; actualiza la ruta antes de autorizar.")
    return alerta


@transaction.atomic
def _registrar_alerta_recarga_sync(
    *, ruta: RutaEntrega, parada: ParadaRuta, estado_sync: str, snapshot: dict, detalle: str, actor
) -> EventoRuta:
    snapshot_hash = snapshot["snapshot_hash"]
    clave = f"recarga-sync:{ruta.id}:{parada.id}:{estado_sync}:{snapshot_hash}"
    observada_en_inicial = timezone.now()
    evento, creado = EventoRuta.objects.get_or_create(
        clave_auditoria=clave,
        defaults={
            "ruta": ruta,
            "parada": parada,
            "tipo": EventoRuta.TIPO_INCIDENCIA_MANUAL,
            "severidad": EventoRuta.SEVERIDAD_ALERTA,
            "descripcion": detalle,
            "metadata": {
                "tipo": "alerta_recarga_cedis_sync",
                "estado_sync": estado_sync,
                "ruta_id": ruta.id,
                "parada_id": parada.id,
                "actor_id": getattr(actor, "id", None),
                "capturado_en": snapshot["capturado_en"],
                "ultima_observacion_en": observada_en_inicial.isoformat(),
                "ultima_observacion_actor_id": getattr(actor, "id", None),
                "sucursales": snapshot["sucursales"],
                "snapshot": snapshot,
            },
            "creado_por": actor,
        },
    )
    evento = EventoRuta.objects.select_for_update().get(pk=evento.pk)
    observada_en = timezone.now()
    metadata = dict(evento.metadata or {})
    metadata["ultima_observacion_en"] = observada_en.isoformat()
    metadata["ultima_observacion_actor_id"] = getattr(actor, "id", None)
    evento.metadata = metadata
    evento.save(update_fields=["metadata"])
    for usuario in _usuarios_diferencia_carga(ruta):
        if not usuario.is_active:
            continue
        Notificacion.objects.get_or_create(
            usuario=usuario,
            objeto_tipo="logistica.EventoRuta",
            objeto_id=str(evento.id),
            defaults={
                "actor": actor if getattr(actor, "is_authenticated", False) else None,
                "titulo": f"Recarga CEDIS requiere atención · {ruta.folio}",
                "mensaje": detalle,
                "url": f"/logistica/rutas/{ruta.id}/",
                "tipo": Notificacion.TIPO_SISTEMA,
                "prioridad": Notificacion.PRIORIDAD_ALTA,
            },
        )
    return evento


@transaction.atomic
def registrar_recarga_cedis(
    *, ruta: RutaEntrega, user, notas: str, parada: ParadaRuta | None, snapshot: dict | None,
    estado_sync: str, motivo_autorizacion: str, expected_snapshot_hash: str = "",
) -> EventoRuta:
    ruta = RutaEntrega.objects.select_for_update().get(pk=ruta.pk)
    if ruta.estatus not in {RutaEntrega.ESTATUS_PLANEADA, RutaEntrega.ESTATUS_EN_RUTA}:
        raise ValidationError("La recarga CEDIS solo aplica a rutas planeadas o en ruta.")
    checklist = RutaCargaChecklist.objects.select_for_update().filter(ruta=ruta).first()
    if ruta.estatus == RutaEntrega.ESTATUS_PLANEADA and (not checklist or not checklist.lineas.exists()):
        raise ValidationError("La ruta no tiene carga esperada para registrar recarga CEDIS.")
    cedis_punto = PuntoLogistico.objects.filter(tipo=PuntoLogistico.TIPO_CEDIS).order_by("id").first()
    if not cedis_punto:
        raise ValidationError("No hay punto logístico CEDIS configurado para registrar la recarga.")
    if parada is not None:
        parada = ParadaRuta.objects.select_for_update().select_related("punto").get(pk=parada.pk, ruta=ruta)
        if estado_sync == "AUTORIZADO":
            _validar_alerta_recarga_revisada(
                ruta=ruta,
                parada=parada,
                expected_snapshot_hash=expected_snapshot_hash,
            )
        existente = _evento_recarga_existente(ruta_id=ruta.id, parada_id=parada.id)
        if existente:
            return existente
        if ruta.estatus == RutaEntrega.ESTATUS_EN_RUTA:
            snapshot_actual = _snapshot_siguiente_tramo(ruta, parada, actor=user)
            if not snapshot or snapshot_actual["snapshot_hash"] != snapshot.get("snapshot_hash"):
                raise RecargaCedisSnapshotObsoleto(
                    "La carga del siguiente tramo cambió; actualiza y vuelve a revisar antes de confirmar."
                )

    lineas_tramo = lineas_tramo_operativo_actual(ruta, checklist=checklist)
    pendientes = lineas_tramo.filter(estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE).count()
    diferencias = lineas_tramo.exclude(
        estatus__in=[
            RutaCargaChecklistLinea.ESTATUS_CARGADA,
            RutaCargaChecklistLinea.ESTATUS_NO_APLICA,
            RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED,
            RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        ]
    ).count()
    if ruta.estatus == RutaEntrega.ESTATUS_PLANEADA:
        if pendientes:
            raise ValidationError("Primero valida cada línea como cargada, parcial o faltante.")
        if checklist.estatus != RutaCargaChecklist.ESTATUS_CON_INCIDENCIA:
            raise ValidationError("La salida parcial solo aplica cuando hay faltantes o parciales.")
        checklist.motivo_override = notas or "Salida parcial autorizada con recarga CEDIS programada."
        checklist.save(update_fields=["motivo_override", "actualizado_en"])

    numero = (
        EventoRuta.objects.filter(ruta=ruta)
        .filter(
            Q(tipo=EventoRuta.TIPO_RECARGA_CEDIS)
            | Q(tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL, metadata__tipo__in=["recarga_cedis", "recarga_cedis_pwa"])
        )
        .count()
        + 1
    )
    paradas_con_recarga = _ids_paradas_con_recarga_cedis(ruta)
    cedis_inicial_id = ruta.paradas.filter(
        punto__tipo=PuntoLogistico.TIPO_CEDIS,
        orden=1,
    ).values_list("id", flat=True).first()
    parada_cedis = (
        ruta.paradas.select_for_update()
        .filter(
            punto__tipo=PuntoLogistico.TIPO_CEDIS,
            estado__in=[
                ParadaRuta.ESTADO_PENDIENTE,
                ParadaRuta.ESTADO_OMITIDA,
                ParadaRuta.ESTADO_VISITADA,
            ],
        )
        .exclude(pk__in=paradas_con_recarga)
        .exclude(pk=cedis_inicial_id)
        .order_by("orden", "id")
        .first()
    )
    if parada is not None and (not parada_cedis or parada_cedis.id != parada.id):
        raise ValidationError("La recarga CEDIS no corresponde al siguiente tramo de la ruta.")
    if not parada_cedis:
        ultimo_orden = ruta.paradas.order_by("-orden").values_list("orden", flat=True).first() or 0
        parada_cedis = ParadaRuta.objects.create(ruta=ruta, punto=cedis_punto, orden=ultimo_orden + 1)
    now = timezone.now()
    parada_cedis.estado = ParadaRuta.ESTADO_VISITADA
    parada_cedis.hora_llegada_real = parada_cedis.hora_llegada_real or now
    parada_cedis.hora_salida_real = parada_cedis.hora_salida_real or now
    parada_cedis.notas = notas or parada_cedis.notas
    parada_cedis.save(update_fields=["estado", "hora_llegada_real", "hora_salida_real", "notas", "actualizado_en"])
    ruta.recompute_route_control()
    ruta.save(update_fields=["cumplimiento_porcentaje", "updated_at"])

    metadata = {
        "tipo": "recarga_cedis",
        "numero": numero,
        "pendientes": pendientes,
        "diferencias": diferencias,
        "notas": notas,
        "estado_sync": estado_sync,
        "snapshot": snapshot,
    }
    if estado_sync == "AUTORIZADO":
        metadata["autorizacion"] = {
            "actor_id": user.id,
            "motivo": motivo_autorizacion,
            "snapshot_hash": snapshot["snapshot_hash"],
            "parada_id": parada_cedis.id,
        }
    return EventoRuta.objects.create(
        ruta=ruta,
        parada=parada_cedis,
        tipo=EventoRuta.TIPO_RECARGA_CEDIS,
        severidad=EventoRuta.SEVERIDAD_INFO,
        descripcion=f"Recarga CEDIS {numero} registrada por logística.",
        metadata=metadata,
        creado_por=user,
    )


_confirmar_recarga_cedis_atomica = registrar_recarga_cedis


def _validar_parada_recarga_pre_sync(
    *, ruta: RutaEntrega, parada: ParadaRuta | None
) -> tuple[RutaEntrega, ParadaRuta | None]:
    ruta = RutaEntrega.objects.get(pk=ruta.pk)
    if ruta.estatus not in {RutaEntrega.ESTATUS_PLANEADA, RutaEntrega.ESTATUS_EN_RUTA}:
        raise ValidationError("La recarga CEDIS solo aplica a rutas planeadas o en ruta.")
    if ruta.estatus != RutaEntrega.ESTATUS_EN_RUTA:
        return ruta, parada

    paradas_con_recarga = _ids_paradas_con_recarga_cedis(ruta)
    cedis_inicial_id = ruta.paradas.filter(
        punto__tipo=PuntoLogistico.TIPO_CEDIS,
        orden=1,
    ).values_list("id", flat=True).first()
    estados_candidatos = [
        ParadaRuta.ESTADO_PENDIENTE,
        ParadaRuta.ESTADO_OMITIDA,
        ParadaRuta.ESTADO_VISITADA,
    ]
    siguiente = (
        ruta.paradas.select_related("punto")
        .filter(
            punto__tipo=PuntoLogistico.TIPO_CEDIS,
            estado__in=estados_candidatos,
        )
        .exclude(pk__in=paradas_con_recarga)
        .exclude(pk=cedis_inicial_id)
        .order_by("orden", "id")
        .first()
    )
    if parada is None:
        parada = siguiente
    else:
        parada = (
            ruta.paradas.select_related("punto")
            .filter(
                pk=parada.pk,
                punto__tipo=PuntoLogistico.TIPO_CEDIS,
                estado__in=estados_candidatos,
            )
            .exclude(pk__in=paradas_con_recarga)
            .exclude(pk=cedis_inicial_id)
            .first()
        )
    if parada is None:
        raise ValidationError("No hay una parada CEDIS pendiente para registrar la recarga.")
    if siguiente is None or siguiente.id != parada.id:
        raise ValidationError("La recarga CEDIS no corresponde al siguiente tramo de la ruta.")
    return ruta, parada


def _orquestar_recarga_cedis(
    *, ruta: RutaEntrega, user, notas: str = "", parada: ParadaRuta | None = None,
    autorizar_sin_sync: bool = False, motivo_autorizacion: str = "",
    expected_snapshot_hash: str = "",
) -> EventoRuta:
    ruta = RutaEntrega.objects.get(pk=ruta.pk)
    parada_id = getattr(parada, "id", None)
    existente = _evento_recarga_existente(ruta_id=ruta.id, parada_id=parada_id)
    if existente:
        return existente
    ruta, parada = _validar_parada_recarga_pre_sync(ruta=ruta, parada=parada)

    if autorizar_sin_sync:
        if not can_manage_submodule(user, "logistica", "rutas"):
            raise PermissionDenied("Solo una jefatura de logística puede autorizar el snapshot de recarga.")
        motivo_autorizacion = (motivo_autorizacion or "").strip()
        if not motivo_autorizacion:
            raise ValidationError("Captura el motivo de autorización del snapshot de recarga.")
        expected_snapshot_hash = (expected_snapshot_hash or "").strip()
        if not expected_snapshot_hash:
            raise ValidationError("Actualiza la ruta y revisa la alerta antes de autorizar la recarga.")
        _validar_alerta_recarga_revisada(
            ruta=ruta,
            parada=parada,
            expected_snapshot_hash=expected_snapshot_hash,
        )

    snapshot = None
    estado_sync = "ACTUALIZADO"
    if ruta.estatus == RutaEntrega.ESTATUS_EN_RUTA:
        try:
            sincronizar_checklist_recarga_desde_point(ruta=ruta, user=user)
        except PointSyncUnavailableError as exc:
            snapshot = _snapshot_siguiente_tramo(ruta, parada, actor=user)
            detalle_error = "; ".join(exc.messages) if hasattr(exc, "messages") else str(exc)
            snapshot["sync_error"] = detalle_error
            _registrar_alerta_recarga_sync(
                ruta=ruta,
                parada=parada,
                estado_sync="ERROR_POINT",
                snapshot=snapshot,
                detalle="Point no pudo sincronizar; el jefe fue notificado.",
                actor=user,
            )
            if autorizar_sin_sync:
                _snapshot_obsoleto(
                    expected_snapshot_hash=expected_snapshot_hash,
                    snapshot_actual=snapshot,
                )
                if _snapshot_tiene_sync_externo_valido(snapshot):
                    estado_sync = "AUTORIZADO"
                else:
                    raise _error_recarga_con_snapshot(
                        RecargaCedisPointError(
                            "Point no pudo sincronizar y no existe un snapshot externo previo válido para autorizar."
                        ),
                        snapshot,
                    ) from exc
            else:
                raise _error_recarga_con_snapshot(
                    RecargaCedisPointError("Point no pudo sincronizar; el jefe fue notificado."),
                    snapshot,
                ) from exc

        if snapshot is None:
            snapshot = _snapshot_siguiente_tramo(ruta, parada, actor=user)
        if autorizar_sin_sync and estado_sync != "AUTORIZADO":
            _snapshot_obsoleto(
                expected_snapshot_hash=expected_snapshot_hash,
                snapshot_actual=snapshot,
            )
        sin_lineas_para_sucursales = bool(snapshot["sucursales"] and not snapshot["lineas"])
        if sin_lineas_para_sucursales and estado_sync != "AUTORIZADO":
            if autorizar_sin_sync:
                estado_sync = "AUTORIZADO"
            else:
                _registrar_alerta_recarga_sync(
                    ruta=ruta,
                    parada=parada,
                    estado_sync="SIN_LINEAS_POINT",
                    snapshot=snapshot,
                    detalle=(
                        "El siguiente tramo tiene sucursales planeadas pero Point no devolvió líneas; "
                        "el jefe fue notificado."
                    ),
                    actor=user,
                )
                raise _error_recarga_con_snapshot(
                    RecargaCedisSinLineasPoint(
                        "Point no devolvió líneas para las sucursales planeadas del siguiente tramo; "
                        "el jefe fue notificado."
                    ),
                    snapshot,
                )
        pendientes = [linea for linea in snapshot["lineas"] if not linea["enviado"]]
        if pendientes and estado_sync != "AUTORIZADO":
            if autorizar_sin_sync:
                estado_sync = "AUTORIZADO"
            else:
                _registrar_alerta_recarga_sync(
                    ruta=ruta,
                    parada=parada,
                    estado_sync="PENDIENTE_ENVIADO",
                    snapshot=snapshot,
                    detalle="Hay solicitudes del siguiente tramo que aún no pasan a Enviado; el jefe fue notificado.",
                    actor=user,
                )
                raise _error_recarga_con_snapshot(
                    RecargaCedisPendienteEnviado(
                        "Hay solicitudes que aún no pasan a Enviado; el jefe fue notificado."
                    ),
                    snapshot,
                )

    return _confirmar_recarga_cedis_atomica(
        ruta=ruta,
        user=user,
        notas=notas,
        parada=parada,
        snapshot=snapshot,
        estado_sync=estado_sync,
        motivo_autorizacion=motivo_autorizacion,
        expected_snapshot_hash=expected_snapshot_hash,
    )


registrar_recarga_cedis = _orquestar_recarga_cedis


def _usuarios_logistica_rutas():
    User = get_user_model()
    usuarios = User.objects.filter(is_active=True).prefetch_related("groups", "module_access")
    return [usuario for usuario in usuarios if can_manage_submodule(usuario, "logistica", "rutas")]


def _usuarios_diferencia_carga(ruta: RutaEntrega):
    User = get_user_model()
    usuarios = {usuario.id: usuario for usuario in _usuarios_logistica_rutas()}
    for usuario in User.objects.filter(is_active=True, is_superuser=True):
        usuarios[usuario.id] = usuario
    empleado = getattr(getattr(ruta.repartidor, "user", None), "empleado_rrhh", None)
    jefe_usuario = getattr(getattr(empleado, "jefe_directo", None), "usuario_erp", None)
    if jefe_usuario and jefe_usuario.is_active:
        usuarios[jefe_usuario.id] = jefe_usuario
    return list(usuarios.values())


def paradas_con_entrega_requerida(ruta: RutaEntrega):
    return ruta.paradas.select_related("punto").exclude(punto__tipo=PuntoLogistico.TIPO_CEDIS)


def ruta_tiene_paradas_entregables_pendientes(ruta: RutaEntrega) -> bool:
    return any(
        not parada_resuelta_operativamente(parada)
        for parada in ruta.paradas.select_related("punto").order_by("orden", "id")
    )


def ruta_tiene_entregas_pendientes(ruta: RutaEntrega) -> bool:
    return paradas_con_entrega_requerida(ruta).filter(entrega_estado=ParadaRuta.ENTREGA_PENDIENTE).exists()


def ruta_tiene_diferencias_entrega(ruta: RutaEntrega) -> bool:
    return paradas_con_entrega_requerida(ruta).filter(
        entrega_estado__in=[ParadaRuta.ENTREGA_CON_DIFERENCIA, ParadaRuta.ENTREGA_NO_ENTREGADA]
    ).exists()


def _resumen_diferencias_cierre(ruta: RutaEntrega) -> list[dict]:
    checklist = getattr(ruta, "checklist_carga", None)
    resumen = []
    for parada in paradas_con_entrega_requerida(ruta).filter(
        entrega_estado__in=[ParadaRuta.ENTREGA_CON_DIFERENCIA, ParadaRuta.ENTREGA_NO_ENTREGADA]
    ).order_by("orden", "id"):
        item = {
            "parada_id": parada.id,
            "orden": parada.orden,
            "punto": parada.punto_nombre_snapshot,
            "entrega_estado": parada.entrega_estado,
            "productos": [],
        }
        if checklist:
            for linea in checklist.lineas.filter(parada=parada).select_related("point_transfer_line").order_by("item_name", "id"):
                esperado = _cantidad_referencia_entrega(linea)
                recibido = _cantidad_recibida_linea(linea)
                if recibido != esperado:
                    item["productos"].append(
                        {
                            "codigo": linea.item_code,
                            "producto": linea.item_name,
                            "esperado": str(esperado),
                            "recibido": str(recibido) if recibido is not None else None,
                        }
                    )
        resumen.append(item)
    return resumen


@transaction.atomic
def cerrar_ruta_con_diferencia_autorizada(*, ruta: RutaEntrega, user, notas: str = "") -> EventoRuta:
    ruta = RutaEntrega.objects.select_for_update().get(pk=ruta.pk)
    if ruta.estatus != RutaEntrega.ESTATUS_EN_RUTA:
        raise ValidationError("Solo puedes cerrar con diferencia una ruta en seguimiento.")
    if ruta_tiene_paradas_entregables_pendientes(ruta):
        raise ValidationError("No se puede cerrar: hay paradas pendientes por visitar u omitir.")
    if ruta_tiene_entregas_pendientes(ruta):
        raise ValidationError("No se puede cerrar: hay paradas sin entrega confirmada.")
    if not ruta_tiene_diferencias_entrega(ruta):
        raise ValidationError("Esta ruta no tiene diferencias; usa el cierre normal.")

    diferencias = _resumen_diferencias_cierre(ruta)
    ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
    ruta.hora_cierre_real = ruta.hora_cierre_real or timezone.now()
    ruta.save(update_fields=["estatus", "hora_cierre_real", "updated_at"])
    evento = EventoRuta.objects.create(
        ruta=ruta,
        tipo=EventoRuta.TIPO_CIERRE,
        severidad=EventoRuta.SEVERIDAD_ALERTA,
        descripcion="Ruta cerrada con diferencia autorizada para revisión de Logística.",
        metadata={
            "tipo": "cierre_con_diferencia_autorizada",
            "diferencias": diferencias,
            "notas": notas,
        },
        creado_por=user,
    )
    crear_notificaciones(
        _usuarios_logistica_rutas(),
        titulo=f"Ruta con diferencia: {ruta.folio}",
        mensaje="Cerrada operativamente con diferencia autorizada. Revisar evidencia y recepción Point.",
        url=f"/logistica/rutas/{ruta.id}/",
        tipo=Notificacion.TIPO_SISTEMA,
        prioridad=Notificacion.PRIORIDAD_ALTA,
        actor=user,
        objeto_tipo="logistica.RutaEntrega",
        objeto_id=ruta.id,
        excluir=user,
    )
    return evento


@transaction.atomic
def confirmar_checklist_carga_manual(*, ruta: RutaEntrega, user, notas: str = "") -> int:
    if ruta.estatus != RutaEntrega.ESTATUS_PLANEADA:
        raise ValidationError("Solo puedes confirmar carga manual en una ruta planeada.")
    checklist = RutaCargaChecklist.objects.select_for_update().filter(ruta=ruta).first()
    if not checklist or not checklist.lineas.exists():
        raise ValidationError("La ruta no tiene carga Point para confirmar.")
    now = timezone.now()
    lineas = list(
        checklist.lineas.select_related("point_transfer_line")
        .select_for_update(of=("self",))
        .filter(estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
    )
    if any(_linea_pendiente_envio_point(linea) for linea in lineas):
        raise ValidationError(POINT_PENDIENTE_ENVIO_NOTA)
    for linea in lineas:
        linea.cantidad_cargada = linea.cantidad_enviada_esperada
        linea.estatus = RutaCargaChecklistLinea.ESTATUS_CARGADA
        linea.motivo_diferencia = ""
        linea.notas = notas or "Carga confirmada manualmente por logística."
        linea.client_event_id = f"manual-carga-{ruta.id}-{linea.id}"
        linea.validado_por = user
        linea.validado_en = now
        linea.save(
            update_fields=[
                "cantidad_cargada",
                "estatus",
                "motivo_diferencia",
                "notas",
                "client_event_id",
                "validado_por",
                "validado_en",
                "actualizado_en",
            ]
        )
    diferencias = checklist.lineas.exclude(
        estatus__in=[
            RutaCargaChecklistLinea.ESTATUS_CARGADA,
            RutaCargaChecklistLinea.ESTATUS_NO_APLICA,
            RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED,
            RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        ]
    ).exists()
    checklist.estatus = RutaCargaChecklist.ESTATUS_CON_INCIDENCIA if diferencias else RutaCargaChecklist.ESTATUS_CONFIRMADA
    checklist.confirmado_por = None if diferencias else user
    checklist.confirmado_en = None if diferencias else now
    checklist.notas = notas or "Carga confirmada manualmente por logística."
    checklist.save(update_fields=["estatus", "confirmado_por", "confirmado_en", "notas", "actualizado_en"])
    if not diferencias:
        registrar_evento_checklist_confirmado(ruta=ruta, user=user)
    return len(lineas)


def registrar_evento_checklist_confirmado(*, ruta: RutaEntrega, user) -> None:
    if EventoRuta.objects.filter(ruta=ruta, tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL, metadata__tipo="checklist_carga").exists():
        return
    EventoRuta.objects.create(
        ruta=ruta,
        tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL,
        severidad=EventoRuta.SEVERIDAD_INFO,
        descripcion="Checklist de carga confirmado antes de salida.",
        metadata={"tipo": "checklist_carga"},
        creado_por=user,
    )


def sincronizar_recepcion_desde_point(*, ruta: RutaEntrega, user=None, ejecutar_sync: bool = True) -> RecepcionPointResumen:
    ruta = RutaEntrega.objects.get(pk=ruta.pk)
    if ruta.estatus not in {RutaEntrega.ESTATUS_EN_RUTA, RutaEntrega.ESTATUS_COMPLETADA}:
        raise ValidationError("La recepción Point solo aplica a rutas en seguimiento o completadas.")

    if ejecutar_sync:
        sync_job = PointMovementSyncService().run_transfer_sync(
            start_date=ruta.fecha_ruta,
            end_date=ruta.fecha_ruta,
            triggered_by=user,
        )
        if sync_job.status != sync_job.STATUS_SUCCESS:
            raise ValidationError("No se pudo sincronizar Point para confirmar recepción de transferencias.")

    return _actualizar_recepcion_desde_point(ruta=ruta, user=user)


@transaction.atomic
def _actualizar_recepcion_desde_point(*, ruta: RutaEntrega, user=None) -> RecepcionPointResumen:
    ruta = RutaEntrega.objects.select_for_update().get(pk=ruta.pk)
    if ruta.estatus not in {RutaEntrega.ESTATUS_EN_RUTA, RutaEntrega.ESTATUS_COMPLETADA}:
        raise ValidationError("La recepción Point solo aplica a rutas en seguimiento o completadas.")

    checklist = obtener_checklist_carga(ruta)

    if not checklist.lineas.exists():
        return RecepcionPointResumen(ruta=ruta)

    source_hashes = list(checklist.lineas.exclude(source_hash="").values_list("source_hash", flat=True))
    point_lines = {
        line.source_hash: line
        for line in PointTransferLine.objects.select_related("sync_job").filter(
            source_hash__in=source_hashes,
            is_cancelled=False,
            is_current_snapshot=True,
        )
    }
    point_recibidas = _point_recibidas_por_ruta(ruta)

    evidencias_creadas = 0
    evidencias_existentes = 0
    paradas_actualizadas = 0
    lineas_recibidas = 0
    lineas_pendientes_point = 0

    for linea in checklist.lineas.exclude(
        estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA,
    ).select_related("parada").order_by("parada__orden", "id"):
        point_line = point_lines.get(linea.source_hash)
        if point_line is not None and linea.point_transfer_line_id != point_line.id:
            linea.point_transfer_line = point_line
            linea.save(update_fields=["point_transfer_line", "actualizado_en"])
        received_lines = []
        if point_line is not None and point_line.is_received:
            received_lines = [point_line]
        elif linea.parada.punto.sucursal_id:
            candidates = point_recibidas.get((linea.parada.punto.sucursal_id, _linea_producto_key(linea)), [])
            received_lines = _elegir_recepcion_point(linea, candidates)
            if received_lines and linea.point_transfer_line_id != received_lines[0].id:
                linea.point_transfer_line = received_lines[0]
                linea.save(update_fields=["point_transfer_line", "actualizado_en"])
        if not received_lines and point_line is not None:
            if not point_transfer_enviada(point_line):
                continue
            if Decimal(str(point_line.sent_quantity or 0)) == Decimal("0"):
                continue
        if not received_lines:
            lineas_pendientes_point += 1
            continue

        lineas_recibidas += 1
        received_quantity = sum((Decimal(str(line.received_quantity or 0)) for line in received_lines), Decimal("0"))
        received_at_values = [line.received_at for line in received_lines if line.received_at]
        received_at = max(received_at_values) if received_at_values else timezone.now()
        client_event_id = f"point-recepcion-{linea.source_hash}"
        metadata = {
            "origen": "point_transfer",
            "transfer_external_id": received_lines[0].transfer_external_id,
            "detail_external_id": received_lines[0].detail_external_id,
            "received_by": ", ".join(sorted({line.received_by for line in received_lines if line.received_by})),
            "is_finalized": all(line.is_finalized for line in received_lines),
            "source_hashes": [line.source_hash for line in received_lines],
        }
        evidencia, created = ParadaEntregaEvidencia.objects.get_or_create(
            ruta=ruta,
            parada=linea.parada,
            linea_carga=linea,
            client_event_id=client_event_id,
            defaults={
                "tipo": ParadaEntregaEvidencia.TIPO_CONFIRMACION,
                "cantidad_entregada": received_quantity,
                "comentario": "Recepción confirmada desde Point.",
                "capturado_por": user,
                "capturado_en": received_at,
                "metadata": metadata,
            },
        )
        if created:
            evidencias_creadas += 1
        else:
            evidencias_existentes += 1
            evidencia.cantidad_entregada = received_quantity
            evidencia.capturado_por = user
            evidencia.capturado_en = received_at
            evidencia.metadata = metadata
            evidencia.save(update_fields=["cantidad_entregada", "capturado_por", "capturado_en", "metadata"])
    return RecepcionPointResumen(
        ruta=ruta,
        evidencias_creadas=evidencias_creadas,
        evidencias_existentes=evidencias_existentes,
        paradas_actualizadas=paradas_actualizadas,
        lineas_recibidas=lineas_recibidas,
        lineas_pendientes_point=lineas_pendientes_point,
    )


@dataclass(frozen=True)
class SuperacionHistoricaResumen:
    grupos_afectados: int = 0
    lineas_superadas: int = 0
    grupos_ambiguos: int = 0
    detalle_ambiguos: list = field(default_factory=list)


_ESTATUS_RESUELTOS_HISTORICO = {
    RutaCargaChecklistLinea.ESTATUS_CARGADA,
    RutaCargaChecklistLinea.ESTATUS_PARCIAL,
    RutaCargaChecklistLinea.ESTATUS_FALTANTE,
    RutaCargaChecklistLinea.ESTATUS_SOBRANTE,
}


def _resueltas_son_duplicado_equivalente(resueltas: list[RutaCargaChecklistLinea]) -> bool:
    """True si todas las líneas resueltas son la misma validación duplicada.

    Mismo detalle de Point (o mismo point_transfer_line), misma cantidad
    cargada/esperada y mismo validador: es la misma transferencia capturada
    dos veces por la Causa A, no dos entregas reales distintas.
    """
    primera = resueltas[0]
    return all(
        linea.detail_external_id == primera.detail_external_id
        and linea.point_transfer_line_id == primera.point_transfer_line_id
        and linea.cantidad_cargada == primera.cantidad_cargada
        and linea.cantidad_enviada_esperada == primera.cantidad_enviada_esperada
        and linea.validado_por_id == primera.validado_por_id
        and primera.validado_por_id is not None
        for linea in resueltas
    )


def _linea_historica_enviada_positiva(linea: RutaCargaChecklistLinea) -> bool:
    cantidad = Decimal(str(linea.cantidad_enviada_esperada or 0))
    if cantidad <= 0:
        return False
    if linea.point_transfer_line_id is None:
        return True
    return point_transfer_enviada(linea.point_transfer_line)


def _linea_historica_superable(linea: RutaCargaChecklistLinea) -> bool:
    if linea.validado_por_id or linea.validado_en:
        return False
    if Decimal(str(linea.cantidad_enviada_esperada or 0)) > 0:
        return False
    return True


def marcar_lineas_checklist_superadas_historicas(*, dry_run: bool = True) -> SuperacionHistoricaResumen:
    """Aplica retroactivamente la regla de SUPERADA a duplicados ya existentes.

    Agrupa por (checklist, parada, producto, unidad, folio) y nunca cruza folios
    ni unidades. Sólo una línea positiva/enviada o una validación inequívoca
    puede ser autoritativa; únicamente candidatos antiguos en cero y no
    validados se marcan SUPERADA, incluso si ese cero fue un Enviado válido.
    Dos detalles positivos reales permanecen independientes. Dos validaciones sólo se colapsan cuando
    comparten el mismo detalle/linaje Point, cantidad y validador; cualquier
    diferencia se reporta para revisión manual sin alterar el grupo.
    """
    grupos = (
        RutaCargaChecklistLinea.objects.exclude(estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        .values("checklist_id", "parada_id", "item_code", "item_name", "unit", "transfer_external_id")
        .annotate(n=Count("id"))
        .filter(n__gt=1)
    )

    grupos_afectados = 0
    lineas_superadas = 0
    grupos_ambiguos = 0
    detalle_ambiguos = []

    for grupo in grupos:
        lineas = list(
            RutaCargaChecklistLinea.objects.filter(
                checklist_id=grupo["checklist_id"],
                parada_id=grupo["parada_id"],
                item_code=grupo["item_code"],
                item_name=grupo["item_name"],
                unit=grupo["unit"],
                transfer_external_id=grupo["transfer_external_id"],
            )
            .exclude(estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA)
            .select_related("point_transfer_line")
        )
        resueltas = [
            linea for linea in lineas if linea.estatus in _ESTATUS_RESUELTOS_HISTORICO or linea.validado_por_id
        ]

        if len(resueltas) > 1 and not _resueltas_son_duplicado_equivalente(resueltas):
            grupos_ambiguos += 1
            detalle_ambiguos.append(
                {
                    "checklist_id": grupo["checklist_id"],
                    "parada_id": grupo["parada_id"],
                    "item_code": grupo["item_code"],
                    "item_name": grupo["item_name"],
                    "transfer_external_id": grupo["transfer_external_id"],
                    "lineas_resueltas": [linea.id for linea in resueltas],
                }
            )
            continue

        if len(resueltas) > 1:
            autoritativa = min(resueltas, key=lambda linea: (linea.creado_en, linea.id))
            a_superar = [
                linea
                for linea in lineas
                if linea.id != autoritativa.id
                and (linea in resueltas or _linea_historica_superable(linea))
            ]
        elif resueltas:
            autoritativa = min(resueltas, key=lambda linea: (linea.creado_en, linea.id))
            a_superar = [
                linea
                for linea in lineas
                if linea.id != autoritativa.id and _linea_historica_superable(linea)
            ]
        else:
            positivas = [linea for linea in lineas if _linea_historica_enviada_positiva(linea)]
            if len(positivas) != 1:
                continue
            autoritativa = positivas[0]
            a_superar = [
                linea
                for linea in lineas
                if linea.id != autoritativa.id and _linea_historica_superable(linea)
            ]

        if not a_superar:
            continue

        grupos_afectados += 1
        lineas_superadas += len(a_superar)
        if not dry_run:
            for linea in a_superar:
                linea.estatus = RutaCargaChecklistLinea.ESTATUS_SUPERADA
                linea.superada_por = autoritativa
                linea.save(update_fields=["estatus", "superada_por", "actualizado_en"])

    return SuperacionHistoricaResumen(
        grupos_afectados=grupos_afectados,
        lineas_superadas=lineas_superadas,
        grupos_ambiguos=grupos_ambiguos,
        detalle_ambiguos=detalle_ambiguos,
    )
