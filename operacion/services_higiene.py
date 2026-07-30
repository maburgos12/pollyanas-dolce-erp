from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.utils import timezone

from activos.models import Activo
from core.access import can_manage_submodule, is_admin_or_dg, is_mermas_only, is_repartidor_only
from fallas.models import CategoriaFalla, ReporteFalla
from mantenimiento.evidence_validation import EvidenceValidationError, validate_evidence_files

from .higiene_catalog import PLANTILLA_VERSION, plantilla_higiene, punto_higiene
from .models import RegistroHigiene, RespuestaHigiene
from .services_fallas import crear_reporte_falla


def sucursal_higiene_usuario(user):
    profile = getattr(user, "userprofile", None)
    sucursal = getattr(profile, "sucursal", None)
    return sucursal if sucursal and sucursal.esta_operativa() else None


def puede_supervisar_higiene(user) -> bool:
    return is_admin_or_dg(user) or can_manage_submodule(user, "ventas", "visitas_sucursal")


def puede_capturar_higiene(user) -> bool:
    if not user or not user.is_authenticated or is_repartidor_only(user) or is_mermas_only(user):
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
        if seguimiento and not (existente and existente.reporte_falla_id):
            if not evidencia_disponible:
                _error("Agrega una foto para enviar el hallazgo a Mantenimiento.", clave)
            categoria = CategoriaFalla.objects.filter(pk=raw.get("categoria_id"), activo=True).first()
            if not categoria:
                _error("Selecciona una categoría activa para el reporte.", clave)
            if tipo_objetivo == ReporteFalla.OBJETIVO_EQUIPO:
                activo = Activo.objects.filter(
                    pk=raw.get("activo_id"), sucursal=sucursal, activo=True
                ).first()
                if not activo:
                    _error("El equipo seleccionado no pertenece a tu sucursal.", clave)
                if categoria.tipo != CategoriaFalla.TIPO_EQUIPO:
                    _error("Selecciona una categoría de equipo.", clave)
                area_instalacion = ""
            elif tipo_objetivo == ReporteFalla.OBJETIVO_INSTALACION:
                if not area_instalacion:
                    _error("Indica el área de la instalación.", clave)
                if categoria.tipo != CategoriaFalla.TIPO_INSTALACION:
                    _error("Selecciona una categoría de instalaciones.", clave)
            else:
                _error("Clasifica el seguimiento como equipo o instalación.", clave)

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
            }
        )
    return normalizadas


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

    registro, creado = RegistroHigiene.objects.get_or_create(
        sucursal=sucursal,
        fecha=timezone.localdate(),
        tipo=tipo,
        clave_instancia=clave_instancia,
        defaults={
            "hora": hora or None,
            "plantilla_version": PLANTILLA_VERSION,
            "plantilla_snapshot": plantilla,
            "tipo_bano": str(tipo_bano or "").strip(),
            "uso_bano": str(uso_bano or "").strip(),
            "notas": str(notas or "").strip(),
            "creado_por": user,
        },
    )
    if not creado:
        registro = RegistroHigiene.objects.select_for_update().get(pk=registro.pk)
    normalizadas = _normalizar_respuestas(
        tipo=tipo,
        respuestas=respuestas,
        registro_existente=registro,
        archivos=archivos,
        sucursal=sucursal,
    )
    if not creado:
        registro.hora = hora or registro.hora
        registro.tipo_bano = str(tipo_bano or registro.tipo_bano).strip()
        registro.uso_bano = str(uso_bano or registro.uso_bano).strip()
        registro.notas = str(notas or registro.notas).strip()
        registro.save(update_fields=["hora", "tipo_bano", "uso_bano", "notas", "actualizado_en"])

    reporte_ids = []
    for item in normalizadas:
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
            respuesta.save(update_fields=["evidencia"])
        if item["seguimiento"] and not respuesta.reporte_falla_id:
            evidencia = respuesta.evidencia.name if respuesta.evidencia else None
            reporte = crear_reporte_falla(
                sucursal=sucursal,
                usuario=user,
                categoria=item["categoria"],
                tipo_objetivo=item["tipo_objetivo"],
                activo_relacionado=item["activo"],
                area_instalacion=item["area_instalacion"],
                titulo=f"{plantilla['titulo']} · {item['punto']['etiqueta']}",
                descripcion=(
                    f"Hallazgo detectado en higiene diaria ({item['punto']['seccion']}): "
                    f"{item['observacion']}"
                ),
                prioridad=item["prioridad"],
                evidencia=evidencia,
                comentario_bitacora=(
                    f"Reporte creado automáticamente desde Higiene diaria, registro #{registro.pk}. "
                    "La evidencia se capturó una sola vez."
                ),
            )
            respuesta.reporte_falla = reporte
            respuesta.save(update_fields=["reporte_falla"])
        if respuesta.reporte_falla_id:
            reporte_ids.append(respuesta.reporte_falla_id)
    return registro, creado, sorted(set(reporte_ids))
