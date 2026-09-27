from dataclasses import dataclass
import re
import unicodedata

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
    return (
        respuesta.registro.sucursal_id,
        respuesta.registro.tipo,
        respuesta.punto_clave,
        respuesta.tipo_objetivo,
        reporte.categoria_id,
        respuesta.activo_relacionado_id or 0,
        normalizar_texto(respuesta.area_instalacion),
    )


def fecha_fin(reporte):
    return reporte.fecha_cierre or reporte.fecha_resolucion


def proponer_consolidacion_higiene():
    respuestas_consultadas = list(
        RespuestaHigiene.objects.filter(
            reporte_falla__isnull=False,
            reporte_falla__duplicado_de__isnull=True,
        )
        .select_related(
            "registro",
            "registro__sucursal",
            "reporte_falla",
            "reporte_falla__categoria",
        )
        .order_by("reporte_falla__fecha_reporte", "reporte_falla_id", "id")
    )

    por_reporte = {}
    for respuesta in respuestas_consultadas:
        por_reporte.setdefault(respuesta.reporte_falla_id, respuesta)

    grupos = {}
    for respuesta in por_reporte.values():
        grupos.setdefault(clave_identidad(respuesta), []).append(respuesta)

    exactas = []
    ambiguas = []
    for rows in grupos.values():
        principal = rows[0]
        for candidata in rows[1:]:
            cierre = fecha_fin(principal.reporte_falla)
            if cierre and cierre < candidata.reporte_falla.fecha_reporte:
                principal = candidata
                continue

            exacta = normalizar_texto(principal.observacion) == normalizar_texto(
                candidata.observacion
            )
            propuesta = PropuestaConsolidacion(
                principal_id=principal.reporte_falla_id,
                repetido_id=candidata.reporte_falla_id,
                respuesta_principal_id=principal.id,
                respuesta_repetida_id=candidata.id,
                sucursal=principal.registro.sucursal.nombre,
                punto=principal.punto_revision,
                fecha_principal=principal.reporte_falla.fecha_reporte.date().isoformat(),
                fecha_repetida=candidata.reporte_falla.fecha_reporte.date().isoformat(),
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
