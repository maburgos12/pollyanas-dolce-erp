from __future__ import annotations

import hashlib
import logging
from decimal import Decimal, InvalidOperation

from django.core.exceptions import PermissionDenied, ValidationError
from django.db import connection, transaction
from django.utils import timezone

from core.access import can_manage_submodule, is_admin_or_dg, is_repartidor_only
from fallas.models import ReporteFalla
from mantenimiento.evidence_validation import EvidenceValidationError, validate_evidence_files

from .higiene_catalog import PLANTILLA_VERSION, plantilla_higiene, punto_higiene
from .models import RegistroHigiene, RespuestaHigiene
from .services_fallas import crear_reporte_falla
from .services_higiene_fallas import (
    ESTATUS_ACTIVOS,
    FallaHigieneConflict,
    bloquear_identidad,
    bloquear_reportes,
    fallas_misma_identidad,
    id_positivo_estricto,
    identidad_desde_consulta,
    registrar_constatacion,
    reporte_coincide_identidad,
)


DECISIONES_FALLA = {
    "AUTO",
    "MISMA",
    "CAMBIO",
    "DISTINTA",
    "CORRECCION_PENDIENTE",
}
DECISIONES_CON_REPORTE = {"MISMA", "CAMBIO", "CORRECCION_PENDIENTE"}
DECISIONES_CON_EVIDENCIA = {"AUTO", "DISTINTA", "CAMBIO", "CORRECCION_PENDIENTE"}
logger = logging.getLogger(__name__)


def sucursal_higiene_usuario(user):
    profile = getattr(user, "userprofile", None)
    sucursal = getattr(profile, "sucursal", None)
    return sucursal if sucursal and sucursal.esta_operativa() else None


def puede_supervisar_higiene(user) -> bool:
    return is_admin_or_dg(user) or can_manage_submodule(user, "ventas", "visitas_sucursal")


def puede_capturar_higiene(user) -> bool:
    if not user or not user.is_authenticated or is_repartidor_only(user):
        return False
    return bool(sucursal_higiene_usuario(user))


def require_higiene_access(user):
    if not puede_capturar_higiene(user) and not puede_supervisar_higiene(user):
        raise PermissionDenied("Tu sesión no tiene acceso a higiene y limpieza.")


def registros_higiene_autorizados(user):
    require_higiene_access(user)
    queryset = RegistroHigiene.objects.select_related("sucursal", "creado_por").prefetch_related(
        "respuestas__reporte_falla"
    )
    if puede_supervisar_higiene(user):
        return queryset
    return queryset.filter(sucursal=sucursal_higiene_usuario(user))


def _error(message: str, field: str | None = None):
    raise ValidationError({field: message} if field else message)


def _as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "si", "sí"}


def _bloquear_registro_higiene(*, sucursal_id, fecha, tipo, clave_instancia) -> None:
    valor = f"registro-higiene|{sucursal_id}|{fecha.isoformat()}|{tipo}|{clave_instancia}"
    lock_key = int.from_bytes(
        hashlib.blake2b(valor.encode("utf-8"), digest_size=8).digest(),
        byteorder="big",
        signed=True,
    )
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(%s)", [lock_key])


def _normalizar_respuestas(*, tipo, respuestas, registro_existente, archivos, sucursal):
    if not isinstance(respuestas, list) or not respuestas:
        _error("Captura al menos un punto de revisión.", "respuestas")

    normalizadas = []
    claves = set()
    existentes = {
        respuesta.punto_clave: respuesta
        for respuesta in (
            registro_existente.respuestas.select_related("reporte_falla").all()
            if registro_existente
            else []
        )
    }
    categorias_cache = {}
    activos_cache = {}
    for raw in respuestas:
        if not isinstance(raw, dict):
            _error("Cada punto de revisión debe tener una respuesta válida.", "respuestas")
        clave = str(raw.get("key") or "").strip()
        if not clave or clave in claves:
            _error("La captura contiene puntos repetidos o desconocidos.", "respuestas")
        claves.add(clave)
        orden, punto = punto_higiene(tipo, clave)
        if not punto:
            _error(f"El punto {clave} no pertenece a la plantilla vigente.", "respuestas")

        valor_numerico = None
        respuesta_valor = str(raw.get("respuesta") or "").strip().upper()
        if punto["tipo_respuesta"] == "NUMERICA":
            try:
                valor_numerico = Decimal(str(raw.get("valor_numerico")))
            except (InvalidOperation, TypeError, ValueError):
                _error(f"Captura un valor válido para {punto['etiqueta']}.", clave)
            opciones = {Decimal(opcion) for opcion in punto["opciones"]}
            if valor_numerico not in opciones:
                _error(f"Selecciona un valor permitido para {punto['etiqueta']}.", clave)
            respuesta_valor = ""
        elif respuesta_valor not in dict(RespuestaHigiene.RESPUESTA_CHOICES):
            _error(f"Indica si cumple, no cumple o no aplica en {punto['etiqueta']}.", clave)
        elif respuesta_valor == RespuestaHigiene.RESPUESTA_NA and not punto.get("admite_na"):
            _error(f"El punto {punto['etiqueta']} no admite No aplica.", clave)

        observacion = str(raw.get("observacion") or "").strip()
        corregido = _as_bool(raw.get("corregido"))
        seguimiento = _as_bool(raw.get("requiere_seguimiento"))
        falla_decision = str(raw.get("falla_decision") or "AUTO").strip().upper()
        if falla_decision not in DECISIONES_FALLA:
            _error("Selecciona una decisión válida para el seguimiento.", clave)
        reporte_falla_id = None
        if falla_decision in DECISIONES_CON_REPORTE:
            reporte_falla_id = id_positivo_estricto(
                raw.get("reporte_falla_id"),
                campo=clave,
                mensaje="Selecciona la falla a la que darás continuidad.",
            )
        existente = existentes.get(clave)
        reporte_existente = bool(existente and existente.reporte_falla_id)
        if reporte_existente:
            respuesta_valor = existente.respuesta
            observacion = existente.observacion
            corregido = False
            seguimiento = True
        if corregido and seguimiento:
            _error("Una corrección inmediata no puede enviarse también a seguimiento.", clave)
        if (corregido or seguimiento) and respuesta_valor != RespuestaHigiene.RESPUESTA_NO_CUMPLE:
            _error("Solo un punto No cumple puede registrar corrección o seguimiento.", clave)
        if respuesta_valor == RespuestaHigiene.RESPUESTA_NO_CUMPLE and not observacion:
            _error("Describe qué encontraste en cada punto que no cumple.", clave)

        archivo = None if reporte_existente else archivos.get(f"evidencia_{clave}")
        if archivo:
            try:
                archivo = validate_evidence_files([archivo], images_only=True)[0]
            except EvidenceValidationError as exc:
                _error(str(exc), clave)
        evidencia_disponible = archivo or (existente.evidencia if existente else None)

        categoria = None
        activo = None
        tipo_objetivo = str(raw.get("tipo_objetivo") or "").strip().upper()
        area_instalacion = str(raw.get("area_instalacion") or "").strip()
        prioridad = str(raw.get("prioridad") or ReporteFalla.PRIORIDAD_MEDIA).strip()
        if reporte_existente:
            tipo_objetivo = existente.tipo_objetivo
            area_instalacion = existente.area_instalacion
            activo = existente.activo_relacionado
        identidad = None
        if seguimiento and not (existente and existente.reporte_falla_id):
            if falla_decision in DECISIONES_CON_EVIDENCIA and not evidencia_disponible:
                _error("Agrega una foto para enviar el hallazgo a Mantenimiento.", clave)
            identidad = identidad_desde_consulta(
                sucursal=sucursal,
                params={
                    **raw,
                    "tipo_checklist": tipo,
                    "punto_clave": clave,
                    "tipo_objetivo": tipo_objetivo,
                    "area_instalacion": area_instalacion,
                },
                categorias_cache=categorias_cache,
                activos_cache=activos_cache,
            )
            categoria = categorias_cache[identidad.categoria_id]
            activo = (
                activos_cache[(sucursal.pk, identidad.activo_id)]
                if identidad.activo_id is not None
                else None
            )
            tipo_objetivo = identidad.tipo_objetivo
            area_instalacion = identidad.area_instalacion

        normalizadas.append(
            {
                "clave": clave,
                "orden": orden,
                "punto": punto,
                "respuesta": respuesta_valor,
                "valor_numerico": valor_numerico,
                "observacion": observacion,
                "corregido": corregido,
                "seguimiento": seguimiento,
                "archivo": archivo,
                "evidencia_disponible": evidencia_disponible,
                "categoria": categoria,
                "activo": activo,
                "tipo_objetivo": tipo_objetivo,
                "area_instalacion": area_instalacion,
                "prioridad": prioridad,
                "falla_decision": falla_decision,
                "reporte_falla_id": reporte_falla_id,
                "identidad": identidad,
            }
        )
    return normalizadas


def _preflight_fallas_higiene(*, normalizadas):
    identidades_por_lock = {
        item["identidad"].lock_key: item["identidad"]
        for item in normalizadas
        if item["seguimiento"] and item["identidad"] is not None
    }
    for lock_key in sorted(identidades_por_lock):
        bloquear_identidad(identidades_por_lock[lock_key])

    reportes_relevantes_ids = set()
    for identidad in identidades_por_lock.values():
        candidatos_ids = list(fallas_misma_identidad(identidad).values_list("pk", flat=True))
        reportes_relevantes_ids.update(candidatos_ids)
    reportes_relevantes_ids.update(
        item["reporte_falla_id"]
        for item in normalizadas
        if item["reporte_falla_id"] is not None
    )
    reportes_bloqueados = bloquear_reportes(reportes_relevantes_ids)

    planes = []
    for item in normalizadas:
        plan = {
            "reporte": None,
            "continuidad": None,
            "crear_reporte": False,
        }
        if not item["seguimiento"] or item["identidad"] is None:
            planes.append(plan)
            continue

        identidad = item["identidad"]
        candidatos = sorted(
            (
                reporte
                for reporte in reportes_bloqueados.values()
                if reporte.estatus in ESTATUS_ACTIVOS
                and reporte.duplicado_de_id is None
                and reporte_coincide_identidad(reporte, identidad)
            ),
            key=lambda reporte: (reporte.fecha_reporte, reporte.pk),
        )
        decision = item["falla_decision"]
        if decision in DECISIONES_CON_REPORTE:
            plan["reporte"] = next(
                (
                    candidato
                    for candidato in candidatos
                    if candidato.pk == item["reporte_falla_id"]
                ),
                None,
            )
            if plan["reporte"]:
                plan["continuidad"] = {
                    "MISMA": RespuestaHigiene.CONTINUIDAD_IGUAL,
                    "CAMBIO": RespuestaHigiene.CONTINUIDAD_CAMBIO,
                    "CORRECCION_PENDIENTE": RespuestaHigiene.CONTINUIDAD_CORRECCION,
                }[decision]
            else:
                solicitado = reportes_bloqueados.get(item["reporte_falla_id"])
                if not solicitado or solicitado.estatus != ReporteFalla.ESTATUS_CERRADO:
                    _error(
                        "La falla seleccionada no corresponde a este punto o ya no admite continuidad.",
                        item["clave"],
                    )
                if not reporte_coincide_identidad(solicitado, identidad):
                    _error(
                        "La falla cerrada no corresponde a la identidad de este hallazgo.",
                        item["clave"],
                    )
                if candidatos:
                    raise FallaHigieneConflict(candidatos, punto_clave=item["clave"])
                if not item["evidencia_disponible"]:
                    _error(
                        "Agrega una foto para registrar la reincidencia de la falla cerrada.",
                        item["clave"],
                    )
                plan["crear_reporte"] = True
        elif decision == "AUTO":
            if candidatos:
                raise FallaHigieneConflict(candidatos, punto_clave=item["clave"])
            plan["crear_reporte"] = True
        else:
            plan["crear_reporte"] = True
        planes.append(plan)
    return planes


def _datos_reporte_higiene(*, item, plantilla, sucursal, user):
    return {
        "sucursal": sucursal,
        "usuario": user,
        "categoria": item["categoria"],
        "tipo_objetivo": item["tipo_objetivo"],
        "activo_relacionado": item["activo"],
        "area_instalacion": item["area_instalacion"],
        "titulo": f"{plantilla['titulo']} · {item['punto']['etiqueta']}",
        "descripcion": (
            f"Hallazgo detectado en higiene diaria ({item['punto']['seccion']}): "
            f"{item['observacion']}"
        ),
        "prioridad": item["prioridad"],
    }


def _prevalidar_captura(
    *,
    normalizadas,
    planes_falla,
    plantilla,
    sucursal,
    user,
    fecha,
    tipo,
    clave_instancia,
    hora,
    tipo_bano,
    uso_bano,
    notas,
):
    registro_prueba = RegistroHigiene(
        sucursal=sucursal,
        fecha=fecha,
        tipo=tipo,
        clave_instancia=clave_instancia,
        hora=hora or None,
        plantilla_version=PLANTILLA_VERSION,
        plantilla_snapshot=plantilla,
        tipo_bano=str(tipo_bano or "").strip(),
        uso_bano=str(uso_bano or "").strip(),
        notas=str(notas or "").strip(),
        creado_por=user,
    )
    registro_prueba.full_clean(validate_unique=False, validate_constraints=False)

    for item, plan_falla in zip(normalizadas, planes_falla, strict=True):
        continuidad = plan_falla["continuidad"] or (
            RespuestaHigiene.CONTINUIDAD_INICIAL if plan_falla["crear_reporte"] else ""
        )
        respuesta_prueba = RespuestaHigiene(
            punto_clave=item["clave"],
            seccion=item["punto"]["seccion"],
            punto_revision=item["punto"]["etiqueta"],
            orden=item["orden"],
            respuesta=item["respuesta"],
            valor_numerico=item["valor_numerico"],
            observacion=item["observacion"],
            evidencia=item["archivo"] or item["evidencia_disponible"],
            corregido_en_momento=item["corregido"],
            requiere_seguimiento=item["seguimiento"],
            tipo_objetivo=item["tipo_objetivo"],
            activo_relacionado=item["activo"],
            area_instalacion=item["area_instalacion"],
            continuidad_falla=continuidad,
        )
        respuesta_prueba.full_clean(
            exclude=("registro", "reporte_falla"),
            validate_unique=False,
            validate_constraints=False,
        )
        if not plan_falla["crear_reporte"]:
            continue

        datos_reporte = _datos_reporte_higiene(
            item=item,
            plantilla=plantilla,
            sucursal=sucursal,
            user=user,
        )
        evidencia = item["archivo"] or item["evidencia_disponible"]
        reporte_prueba = ReporteFalla(
            sucursal=datos_reporte["sucursal"],
            reportado_por=datos_reporte["usuario"],
            categoria=datos_reporte["categoria"],
            tipo_objetivo=datos_reporte["tipo_objetivo"],
            activo_relacionado=datos_reporte["activo_relacionado"],
            area_instalacion=datos_reporte["area_instalacion"],
            titulo=datos_reporte["titulo"],
            descripcion=datos_reporte["descripcion"],
            prioridad=datos_reporte["prioridad"],
            foto_evidencia=evidencia,
        )
        reporte_prueba.full_clean()
        plan_falla["datos_reporte"] = datos_reporte


def _limpiar_blobs_creados(blobs_creados) -> None:
    eliminados = set()
    for storage, nombre in reversed(blobs_creados):
        clave = (id(storage), nombre)
        if not nombre or clave in eliminados:
            continue
        eliminados.add(clave)
        try:
            storage.delete(nombre)
        except Exception:
            logger.exception("No se pudo limpiar evidencia de higiene abortada: %s", nombre)


@transaction.atomic
def guardar_registro_higiene(
    *,
    user,
    tipo,
    clave_instancia,
    respuestas,
    archivos,
    hora=None,
    tipo_bano="",
    uso_bano="",
    notas="",
):
    if not puede_capturar_higiene(user):
        raise PermissionDenied("Tu sesión no tiene una sucursal operativa para capturar.")
    sucursal = sucursal_higiene_usuario(user)
    plantilla = plantilla_higiene(tipo)
    if not plantilla:
        _error("Selecciona una bitácora válida.", "tipo")
    clave_instancia = str(clave_instancia or "").strip()
    if not clave_instancia:
        _error("Identifica la toma o ronda.", "clave_instancia")

    fecha_registro = timezone.localdate()
    _bloquear_registro_higiene(
        sucursal_id=sucursal.pk,
        fecha=fecha_registro,
        tipo=tipo,
        clave_instancia=clave_instancia,
    )
    registro = RegistroHigiene.objects.select_for_update().filter(
        sucursal=sucursal,
        fecha=fecha_registro,
        tipo=tipo,
        clave_instancia=clave_instancia,
    ).first()
    creado = registro is None
    normalizadas = _normalizar_respuestas(
        tipo=tipo,
        respuestas=respuestas,
        registro_existente=registro,
        archivos=archivos,
        sucursal=sucursal,
    )
    planes_falla = _preflight_fallas_higiene(normalizadas=normalizadas)
    _prevalidar_captura(
        normalizadas=normalizadas,
        planes_falla=planes_falla,
        plantilla=plantilla,
        sucursal=sucursal,
        user=user,
        fecha=fecha_registro,
        tipo=tipo,
        clave_instancia=clave_instancia,
        hora=hora,
        tipo_bano=tipo_bano,
        uso_bano=uso_bano,
        notas=notas,
    )

    if creado:
        registro = RegistroHigiene.objects.create(
            sucursal=sucursal,
            fecha=fecha_registro,
            tipo=tipo,
            clave_instancia=clave_instancia,
            hora=hora or None,
            plantilla_version=PLANTILLA_VERSION,
            plantilla_snapshot=plantilla,
            tipo_bano=str(tipo_bano or "").strip(),
            uso_bano=str(uso_bano or "").strip(),
            notas=str(notas or "").strip(),
            creado_por=user,
        )
    else:
        registro.hora = hora or registro.hora
        registro.tipo_bano = str(tipo_bano or registro.tipo_bano).strip()
        registro.uso_bano = str(uso_bano or registro.uso_bano).strip()
        registro.notas = str(notas or registro.notas).strip()
        registro.save(update_fields=["hora", "tipo_bano", "uso_bano", "notas", "actualizado_en"])

    reporte_ids = []
    blobs_creados = []
    try:
        for item, plan_falla in zip(normalizadas, planes_falla, strict=True):
            respuesta, _ = RespuestaHigiene.objects.update_or_create(
                registro=registro,
                punto_clave=item["clave"],
                defaults={
                    "seccion": item["punto"]["seccion"],
                    "punto_revision": item["punto"]["etiqueta"],
                    "orden": item["orden"],
                    "respuesta": item["respuesta"],
                    "valor_numerico": item["valor_numerico"],
                    "observacion": item["observacion"],
                    "corregido_en_momento": item["corregido"],
                    "requiere_seguimiento": item["seguimiento"],
                    "tipo_objetivo": item["tipo_objetivo"],
                    "activo_relacionado": item["activo"],
                    "area_instalacion": item["area_instalacion"],
                },
            )
            if item["archivo"]:
                respuesta.evidencia = item["archivo"]
                try:
                    respuesta.save(update_fields=["evidencia"])
                finally:
                    evidencia_guardada = respuesta.evidencia
                    if getattr(evidencia_guardada, "_committed", False):
                        blobs_creados.append(
                            (evidencia_guardada.storage, evidencia_guardada.name)
                        )
            if plan_falla["reporte"]:
                registrar_constatacion(
                    respuesta=respuesta,
                    reporte=plan_falla["reporte"],
                    decision=plan_falla["continuidad"],
                    usuario=user,
                )
            elif plan_falla["crear_reporte"]:
                datos_reporte = plan_falla["datos_reporte"]
                evidencia = respuesta.evidencia.name if respuesta.evidencia else None
                reporte = crear_reporte_falla(
                    **datos_reporte,
                    evidencia=evidencia,
                    comentario_bitacora=(
                        f"Reporte creado automáticamente desde Higiene diaria, registro #{registro.pk}. "
                        "La evidencia se capturó una sola vez."
                    ),
                )
                registrar_constatacion(
                    respuesta=respuesta,
                    reporte=reporte,
                    decision=RespuestaHigiene.CONTINUIDAD_INICIAL,
                    usuario=user,
                )
            if respuesta.reporte_falla_id:
                reporte_ids.append(respuesta.reporte_falla_id)
    except BaseException:
        _limpiar_blobs_creados(blobs_creados)
        raise
    return registro, creado, sorted(set(reporte_ids))
