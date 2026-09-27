from dataclasses import dataclass
import re
import unicodedata

from django.db.models import F, OuterRef, Prefetch, Subquery
from django.utils import timezone

from fallas.models import BitacoraFalla, ReporteFalla
from operacion.models import RespuestaHigiene


@dataclass(frozen=True)
class PropuestaConsolidacion:
    principal_id: int
    repetido_id: int
    respuesta_principal_id: int
    respuesta_repetida_id: int
    sucursal: str
    punto: str
    fecha_principal: str
    fecha_repetida: str
    estatus_principal: str
    estatus_repetido: str
    observacion_principal: str
    observacion_repetida: str
    exacta: bool
    motivo: str


@dataclass(frozen=True)
class ResultadoPreview:
    exactas: tuple[PropuestaConsolidacion, ...]
    ambiguas: tuple[PropuestaConsolidacion, ...]

    @property
    def todas(self):
        return self.exactas + self.ambiguas


def normalizar_texto(value):
    folded = unicodedata.normalize("NFKD", value or "")
    ascii_text = "".join(char for char in folded if not unicodedata.combining(char))
    return re.sub(r"\s+", " ", ascii_text.casefold()).strip()


def clave_identidad(respuesta):
    reporte = respuesta.reporte_falla
    if respuesta.registro.sucursal_id != reporte.sucursal_id:
        return None
    if reporte.tipo_objetivo == ReporteFalla.OBJETIVO_EQUIPO:
        activo_id = reporte.activo_relacionado_id or 0
        area_instalacion = ""
    else:
        activo_id = 0
        area_instalacion = reporte.area_instalacion.strip().casefold()
    return (
        respuesta.registro.sucursal_id,
        respuesta.registro.tipo,
        respuesta.punto_clave,
        reporte.tipo_objetivo,
        reporte.categoria_id,
        activo_id,
        area_instalacion,
    )


ESTATUS_TERMINALES = frozenset(
    {
        ReporteFalla.ESTATUS_RESUELTO,
        ReporteFalla.ESTATUS_CERRADO,
        ReporteFalla.ESTATUS_CANCELADO,
    }
)
ESTATUS_ACTIVOS = frozenset(
    {
        ReporteFalla.ESTATUS_ABIERTO,
        ReporteFalla.ESTATUS_REVISION,
        ReporteFalla.ESTATUS_PROCESO,
    }
)


def estado_activo_conocido(estatus):
    if estatus in ESTATUS_ACTIVOS:
        return True
    if estatus in ESTATUS_TERMINALES:
        return False
    return None


def reporte_activo_en(reporte, momento):
    if momento < reporte.fecha_reporte:
        return False

    transiciones = [
        evento
        for evento in getattr(reporte, "_historial_consolidacion", ())
        if evento.estatus_nuevo and evento.estatus_nuevo != evento.estatus_anterior
    ]
    eventos = [
        (evento.timestamp, evento.id, estado_activo_conocido(evento.estatus_nuevo))
        for evento in transiciones
    ]
    fechas_terminales = [
        fecha
        for fecha in (reporte.fecha_resolucion, reporte.fecha_cierre)
        if fecha is not None
    ]
    eventos.extend((fecha, 0, False) for fecha in fechas_terminales)
    if not eventos and reporte.estatus not in ESTATUS_ACTIVOS:
        return False

    activo = True
    for timestamp, _, nuevo_activo in sorted(eventos):
        if timestamp > momento:
            break
        activo = nuevo_activo
    return activo is True


def fecha_local_iso(value):
    return timezone.localtime(value).date().isoformat()


def proponer_consolidacion_higiene():
    primera_respuesta = (
        RespuestaHigiene.objects.filter(reporte_falla_id=OuterRef("reporte_falla_id"))
        .order_by("id")
        .values("id")[:1]
    )
    respuestas_consultadas = list(
        RespuestaHigiene.objects.filter(
            reporte_falla__isnull=False,
            reporte_falla__duplicado_de__isnull=True,
            id=Subquery(primera_respuesta),
        )
        .select_related(
            "registro",
            "reporte_falla",
            "reporte_falla__categoria",
            "reporte_falla__sucursal",
        )
        .prefetch_related(
            Prefetch(
                "reporte_falla__bitacora",
                queryset=(
                    BitacoraFalla.objects.exclude(estatus_nuevo="")
                    .exclude(estatus_nuevo=F("estatus_anterior"))
                    .only(
                        "id",
                        "reporte_id",
                        "estatus_anterior",
                        "estatus_nuevo",
                        "timestamp",
                    )
                    .order_by("timestamp", "id")
                ),
                to_attr="_historial_consolidacion",
            )
        )
        .order_by("reporte_falla__fecha_reporte", "reporte_falla_id", "id")
    )

    grupos = {}
    for respuesta in respuestas_consultadas:
        identidad = clave_identidad(respuesta)
        if identidad is not None:
            grupos.setdefault(identidad, []).append(respuesta)

    exactas = []
    ambiguas = []
    for rows in grupos.values():
        principales = [rows[0]]
        for candidata in rows[1:]:
            principales_activas = [
                principal
                for principal in principales
                if reporte_activo_en(
                    principal.reporte_falla,
                    candidata.reporte_falla.fecha_reporte,
                )
            ]
            if len(principales_activas) != 1:
                principales.append(candidata)
                continue
            principal = principales_activas[0]

            exacta = normalizar_texto(principal.observacion) == normalizar_texto(
                candidata.observacion
            )
            propuesta = PropuestaConsolidacion(
                principal_id=principal.reporte_falla_id,
                repetido_id=candidata.reporte_falla_id,
                respuesta_principal_id=principal.id,
                respuesta_repetida_id=candidata.id,
                sucursal=principal.reporte_falla.sucursal.nombre,
                punto=principal.punto_revision,
                fecha_principal=fecha_local_iso(principal.reporte_falla.fecha_reporte),
                fecha_repetida=fecha_local_iso(candidata.reporte_falla.fecha_reporte),
                estatus_principal=principal.reporte_falla.get_estatus_display(),
                estatus_repetido=candidata.reporte_falla.get_estatus_display(),
                observacion_principal=principal.observacion,
                observacion_repetida=candidata.observacion,
                exacta=exacta,
                motivo=(
                    "Identidad y observación exactas"
                    if exacta
                    else "Identidad exacta; observación diferente"
                ),
            )
            (exactas if exacta else ambiguas).append(propuesta)

    return ResultadoPreview(tuple(exactas), tuple(ambiguas))
