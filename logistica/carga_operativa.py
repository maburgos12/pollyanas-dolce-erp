from dataclasses import dataclass
from enum import StrEnum

from django.db import transaction
from django.db.models import Count

from .domain_ruta import point_transfer_enviada
from .models import RutaCargaChecklistLinea, RutaEntrega


NOTA_SOLICITUD_AUDITORIA = (
    "Solicitud sin Enviado Point; se conserva solo para auditoría."
)


class ClasificacionLineaPoint(StrEnum):
    ENVIADA = "ENVIADA"
    AUDITORIA_SOLICITUD = "AUDITORIA_SOLICITUD"


def clasificar_linea_point(linea) -> ClasificacionLineaPoint:
    """Clasifica Point sin inferir la transición a partir de cantidades."""

    if point_transfer_enviada(linea):
        return ClasificacionLineaPoint.ENVIADA
    return ClasificacionLineaPoint.AUDITORIA_SOLICITUD


def linea_point_es_operativa(linea) -> bool:
    return clasificar_linea_point(linea) == ClasificacionLineaPoint.ENVIADA


def archivar_linea_checklist(linea, *, nota: str, superada_por=None) -> bool:
    if linea.estatus == RutaCargaChecklistLinea.ESTATUS_SUPERADA:
        return False
    linea.estatus = RutaCargaChecklistLinea.ESTATUS_SUPERADA
    linea.superada_por = superada_por
    if nota not in linea.notas:
        linea.notas = " ".join(
            value for value in [linea.notas.strip(), nota] if value
        )
    linea.save(
        update_fields=["estatus", "superada_por", "notas", "actualizado_en"]
    )
    return True


def archivar_solicitudes_point(lineas) -> int:
    archivadas = 0
    for linea in lineas:
        point_line = linea.point_transfer_line
        if point_line is None or linea_point_es_operativa(point_line):
            continue
        archivadas += int(
            archivar_linea_checklist(
                linea,
                nota="Solicitud Point sin Enviado; se conserva solo para auditoría.",
            )
        )
    return archivadas


def resolver_duplicados_activos_point(lineas) -> int:
    activas_por_point_id = {}
    for linea in lineas:
        if (
            linea.point_transfer_line_id is not None
            and linea.estatus != RutaCargaChecklistLinea.ESTATUS_SUPERADA
        ):
            activas_por_point_id.setdefault(linea.point_transfer_line_id, []).append(linea)

    archivadas = 0
    for duplicadas in activas_por_point_id.values():
        if len(duplicadas) <= 1:
            continue
        point_line = duplicadas[0].point_transfer_line
        canonica = max(
            duplicadas,
            key=lambda linea: (
                linea.source_hash == point_line.source_hash,
                linea.detail_external_id == point_line.detail_external_id,
                linea.transfer_external_id == point_line.transfer_external_id,
                -linea.id,
            ),
        )
        for linea in duplicadas:
            if linea.id == canonica.id:
                continue
            archivadas += int(
                archivar_linea_checklist(
                    linea,
                    superada_por=canonica,
                    nota=(
                        "Fila duplicada de la misma línea Point; "
                        "se conserva solo para auditoría."
                    ),
                )
            )
    return archivadas


@dataclass(frozen=True)
class LimpiezaCargaOperativaResumen:
    rutas: int
    lineas_activas: int
    solicitudes_activas: int
    duplicados_activos: int
    ejecutada: bool
    lineas_activas_despues: int
    solicitudes_activas_despues: int
    duplicados_activos_despues: int


def _lineas_activas(ruta_ids):
    return (
        RutaCargaChecklistLinea.objects.filter(checklist__ruta_id__in=ruta_ids)
        .exclude(estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        .select_related("point_transfer_line")
    )


def _contar_solicitudes_activas(lineas) -> int:
    return sum(
        1
        for linea in lineas
        if linea.point_transfer_line is None
        or not linea_point_es_operativa(linea.point_transfer_line)
    )


def _contar_duplicados_activos(lineas) -> int:
    grupos = (
        lineas.exclude(point_transfer_line_id=None)
        .values("point_transfer_line_id")
        .annotate(total=Count("id"))
        .filter(total__gt=1)
    )
    return sum(grupo["total"] - 1 for grupo in grupos)


def limpiar_carga_operativa_rutas_abiertas(
    *,
    ruta_ids=None,
    ejecutar: bool = False,
) -> LimpiezaCargaOperativaResumen:
    """Audita o reconstruye rutas abiertas usando solo Enviado de Point."""

    rutas = RutaEntrega.objects.filter(
        estatus__in=[RutaEntrega.ESTATUS_PLANEADA, RutaEntrega.ESTATUS_EN_RUTA]
    ).order_by("id")
    if ruta_ids is not None:
        rutas = rutas.filter(id__in=ruta_ids)
    ids = list(rutas.values_list("id", flat=True))
    activas = _lineas_activas(ids)
    activas_antes = activas.count()
    solicitudes_antes = _contar_solicitudes_activas(activas)
    duplicados_antes = _contar_duplicados_activos(activas)

    if ejecutar:
        # Importación local: services_carga_ruta usa el clasificador de este módulo.
        from .services_carga_ruta import sincronizar_checklist_carga_desde_point

        for ruta_id in ids:
            with transaction.atomic():
                ruta = RutaEntrega.objects.select_for_update().get(id=ruta_id)
                placeholders = (
                    RutaCargaChecklistLinea.objects.select_for_update()
                    .filter(checklist__ruta=ruta, point_transfer_line__isnull=True)
                    .exclude(estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA)
                )
                for linea in placeholders:
                    linea.estatus = RutaCargaChecklistLinea.ESTATUS_SUPERADA
                    linea.superada_por = None
                    if NOTA_SOLICITUD_AUDITORIA not in linea.notas:
                        linea.notas = " ".join(
                            value
                            for value in [linea.notas.strip(), NOTA_SOLICITUD_AUDITORIA]
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
                sincronizar_checklist_carga_desde_point(
                    ruta=ruta,
                    ejecutar_sync=False,
                )

    activas_despues = _lineas_activas(ids)
    return LimpiezaCargaOperativaResumen(
        rutas=len(ids),
        lineas_activas=activas_antes,
        solicitudes_activas=solicitudes_antes,
        duplicados_activos=duplicados_antes,
        ejecutada=ejecutar,
        lineas_activas_despues=activas_despues.count(),
        solicitudes_activas_despues=_contar_solicitudes_activas(activas_despues),
        duplicados_activos_despues=_contar_duplicados_activos(activas_despues),
    )
