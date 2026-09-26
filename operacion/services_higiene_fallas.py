from __future__ import annotations

import hashlib
from dataclasses import dataclass

from django.core.exceptions import ValidationError
from django.db import connection, transaction

from activos.models import Activo
from fallas.models import BitacoraFalla, CategoriaFalla, ReporteFalla

from .higiene_catalog import punto_higiene
from .models import RespuestaHigiene


ESTATUS_ACTIVOS = (
    ReporteFalla.ESTATUS_ABIERTO,
    ReporteFalla.ESTATUS_REVISION,
    ReporteFalla.ESTATUS_PROCESO,
)


class FallaHigieneConflict(Exception):
    def __init__(self, candidatos, *, punto_clave=None):
        self.candidatos = list(candidatos)
        self.punto_clave = punto_clave
        super().__init__("Ya existe una falla activa para este punto.")


@dataclass(frozen=True)
class IdentidadFallaHigiene:
    sucursal_id: int
    tipo_checklist: str
    punto_clave: str
    tipo_objetivo: str
    categoria_id: int
    activo_id: int | None
    area_instalacion: str

    @property
    def lock_key(self) -> int:
        valor = "|".join(
            (
                str(self.sucursal_id),
                self.tipo_checklist.strip().upper(),
                self.punto_clave.strip(),
                self.tipo_objetivo.strip().upper(),
                str(self.categoria_id),
                str(self.activo_id or 0),
                self.area_instalacion.strip().casefold(),
            )
        )
        digest = hashlib.blake2b(valor.encode("utf-8"), digest_size=8).digest()
        return int.from_bytes(digest, byteorder="big", signed=True)


def _filtrar_identidad(queryset, identidad: IdentidadFallaHigiene):
    queryset = queryset.filter(
        sucursal_id=identidad.sucursal_id,
        categoria_id=identidad.categoria_id,
        tipo_objetivo=identidad.tipo_objetivo,
        constataciones_higiene__registro__tipo=identidad.tipo_checklist,
        constataciones_higiene__punto_clave=identidad.punto_clave,
    )
    if identidad.tipo_objetivo == ReporteFalla.OBJETIVO_EQUIPO:
        queryset = queryset.filter(activo_relacionado_id=identidad.activo_id)
    else:
        queryset = queryset.filter(
            activo_relacionado__isnull=True,
            area_instalacion__iexact=identidad.area_instalacion.strip(),
        )
    return queryset


def fallas_misma_identidad(identidad: IdentidadFallaHigiene):
    queryset = _filtrar_identidad(
        ReporteFalla.objects.filter(
            duplicado_de__isnull=True,
        ),
        identidad,
    )
    return queryset.distinct().order_by("fecha_reporte", "id")


def fallas_coincidentes(identidad: IdentidadFallaHigiene):
    return fallas_misma_identidad(identidad).filter(estatus__in=ESTATUS_ACTIVOS)


def id_positivo_estricto(value, *, campo: str, mensaje: str) -> int:
    if isinstance(value, bool):
        raise ValidationError({campo: mensaje})
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value and all("0" <= char <= "9" for char in value):
        parsed = int(value)
    else:
        raise ValidationError({campo: mensaje})
    if parsed <= 0:
        raise ValidationError({campo: mensaje})
    return parsed


def reporte_coincide_identidad(reporte: ReporteFalla, identidad: IdentidadFallaHigiene) -> bool:
    if (
        reporte.sucursal_id != identidad.sucursal_id
        or reporte.categoria_id != identidad.categoria_id
        or reporte.tipo_objetivo != identidad.tipo_objetivo
    ):
        return False
    if identidad.tipo_objetivo == ReporteFalla.OBJETIVO_EQUIPO:
        if reporte.activo_relacionado_id != identidad.activo_id:
            return False
    elif (
        reporte.activo_relacionado_id is not None
        or reporte.area_instalacion.strip().casefold()
        != identidad.area_instalacion.strip().casefold()
    ):
        return False
    return reporte.constataciones_higiene.filter(
        registro__tipo=identidad.tipo_checklist,
        punto_clave=identidad.punto_clave,
    ).exists()


def bloquear_reportes(reportes_ids) -> dict[int, ReporteFalla]:
    ids = sorted(set(reportes_ids))
    return {
        reporte.pk: reporte
        for reporte in ReporteFalla.objects.select_for_update()
        .filter(pk__in=ids)
        .order_by("pk")
    }


def bloquear_identidad(identidad: IdentidadFallaHigiene) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(%s)", [identidad.lock_key])


def identidad_desde_consulta(
    *,
    sucursal,
    params,
    categorias_cache: dict[int, CategoriaFalla | None] | None = None,
    activos_cache: dict[tuple[int, int], Activo | None] | None = None,
) -> IdentidadFallaHigiene:
    tipo_checklist = str(params.get("tipo_checklist") or params.get("tipo") or "").strip().upper()
    punto_clave = str(params.get("punto_clave") or params.get("key") or "").strip()
    if not tipo_checklist:
        raise ValidationError({"tipo": "Selecciona una bitácora válida."})
    if not punto_clave:
        raise ValidationError({"respuestas": "Identifica el punto de revisión."})
    if not punto_higiene(tipo_checklist, punto_clave)[1]:
        raise ValidationError(
            {punto_clave: "El punto no pertenece a la plantilla de higiene vigente."}
        )

    categoria_id = id_positivo_estricto(
        params.get("categoria_id"),
        campo=punto_clave,
        mensaje="Selecciona una categoría activa para el reporte.",
    )
    categorias_cache = categorias_cache if categorias_cache is not None else {}
    if categoria_id not in categorias_cache:
        categorias_cache[categoria_id] = CategoriaFalla.objects.filter(
            pk=categoria_id,
            activo=True,
        ).first()
    categoria = categorias_cache[categoria_id]
    if not categoria:
        raise ValidationError({punto_clave: "Selecciona una categoría activa para el reporte."})

    tipo_objetivo = str(params.get("tipo_objetivo") or "").strip().upper()
    activo_id = None
    area_instalacion = str(params.get("area_instalacion") or "").strip()
    if tipo_objetivo == ReporteFalla.OBJETIVO_EQUIPO:
        activo_id_consulta = id_positivo_estricto(
            params.get("activo_id"),
            campo=punto_clave,
            mensaje="Selecciona un equipo de tu sucursal.",
        )
        activos_cache = activos_cache if activos_cache is not None else {}
        activo_cache_key = (sucursal.pk, activo_id_consulta)
        if activo_cache_key not in activos_cache:
            activos_cache[activo_cache_key] = Activo.objects.filter(
                pk=activo_id_consulta,
                sucursal=sucursal,
                activo=True,
            ).first()
        activo = activos_cache[activo_cache_key]
        if not activo:
            raise ValidationError({punto_clave: "El equipo seleccionado no pertenece a tu sucursal."})
        if categoria.tipo != CategoriaFalla.TIPO_EQUIPO:
            raise ValidationError({punto_clave: "Selecciona una categoría de equipo."})
        activo_id = activo.pk
        area_instalacion = ""
    elif tipo_objetivo == ReporteFalla.OBJETIVO_INSTALACION:
        if not area_instalacion:
            raise ValidationError({punto_clave: "Indica el área de la instalación."})
        if categoria.tipo != CategoriaFalla.TIPO_INSTALACION:
            raise ValidationError({punto_clave: "Selecciona una categoría de instalaciones."})
    else:
        raise ValidationError(
            {punto_clave: "Clasifica el seguimiento como equipo o instalación."}
        )

    return IdentidadFallaHigiene(
        sucursal_id=sucursal.pk,
        tipo_checklist=tipo_checklist,
        punto_clave=punto_clave,
        tipo_objetivo=tipo_objetivo,
        categoria_id=categoria.pk,
        activo_id=activo_id,
        area_instalacion=area_instalacion,
    )


def registrar_constatacion(*, respuesta, reporte, decision, usuario) -> None:
    respuesta.reporte_falla = reporte
    respuesta.continuidad_falla = decision
    respuesta.requiere_seguimiento = True
    respuesta.save(
        update_fields=["reporte_falla", "continuidad_falla", "requiere_seguimiento"]
    )
    mensajes = {
        RespuestaHigiene.CONTINUIDAD_IGUAL: "Higiene constata que la falla sigue igual",
        RespuestaHigiene.CONTINUIDAD_CAMBIO: "Higiene constata que la falla cambió o empeoró",
        RespuestaHigiene.CONTINUIDAD_CORRECCION: "Higiene solicita validar una corrección",
        RespuestaHigiene.CONTINUIDAD_INICIAL: "Falla detectada inicialmente desde Higiene",
    }
    BitacoraFalla.objects.create(
        reporte=reporte,
        usuario=usuario,
        estatus_anterior=reporte.estatus,
        estatus_nuevo=reporte.estatus,
        comentario=(
            f"{mensajes.get(decision, 'Constatación registrada desde Higiene')}; "
            f"registro #{respuesta.registro_id}, punto {respuesta.punto_clave}."
        ),
    )

    from .services_fallas import notificar_evento_higiene

    transaction.on_commit(
        lambda: notificar_evento_higiene(reporte, respuesta, usuario),
        robust=True,
    )
