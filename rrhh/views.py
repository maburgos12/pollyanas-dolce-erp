from __future__ import annotations

from calendar import monthrange
from datetime import date as dt_date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
import re
from tempfile import NamedTemporaryFile

from django.contrib import messages
from django.contrib.auth.models import Group
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.db.models import Avg, Count, Q, Sum
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone

from django.contrib.auth import get_user_model

from core.access import can_manage_rrhh, can_view_rrhh, can_view_submodule
from core.audit import log_event
from core.models import Sucursal

from .models import (
    AsistenciaEmpleado,
    BonoEsquema,
    CatalogoFuncionOperativa,
    Empleado,
    EmpleadoBaja,
    EmpleadoIdentidadPendiente,
    HoraExtra,
    ImportacionChecador,
    NominaConceptoLinea,
    NominaImportacion,
    NominaLinea,
    NominaPeriodo,
    PermisoSalida,
    PlantillaAutorizada,
    Prestamo,
    ReglamentoLaboral,
    ReglaLaboral,
    SolicitudVacaciones,
    VacanteRRHH,
)

LOGISTICA_TIPO_EMPLEADO_DOLCE = "empleado_dolce"
LOGISTICA_TIPO_CONDUCTOR_OCASIONAL = "empleado_conductor_ocasional"
LOGISTICA_TIPOS_RRHH = (
    ("", "Sin acceso a logística"),
    (LOGISTICA_TIPO_EMPLEADO_DOLCE, "Repartidor operativo"),
    (LOGISTICA_TIPO_CONDUCTOR_OCASIONAL, "Conductor ocasional autorizado"),
)
LOGISTICA_TIPOS_RRHH_VALUES = {value for value, _label in LOGISTICA_TIPOS_RRHH}
USUARIOS_ERP_EXCLUIDOS_RRHH = frozenset(
    {
        "ad_agent_service",
        "omnichannel_service",
        "fallas.sucursal.test",
        "debug",
    }
)
from .services_bonos import asegurar_esquemas_base, esquema_codigo, sincronizar_esquemas_bono
from .services_catalogos import (
    NIVEL_ORGANIZACIONAL_CHOICES,
    NIVEL_ORGANIZACIONAL_VALUES,
    area_division_choices,
    area_division_map,
    area_division_values,
    funciones_operativas_catalogo,
    puesto_operativo_choices,
    puesto_operativo_values,
)
from .services_identidad import (
    asegurar_identidad_operativa_empleado,
    normalizar_codigo_empleado,
    vincular_identidad_pendiente,
)
from .services.lista_raya import importar_lista_raya_nomina
from .services_personnel_normalization import build_personnel_normalization_plan
from .services_niveles import jefatura_q, liderazgo_q
from .services_permisos import can_authorize_direccion, resolver_permiso_direccion
from .api_views import empleado_de_usuario
from .services_vacaciones import (
    aprobar_solicitud_vacaciones_rrhh,
    can_gestionar_vacaciones_jefe,
    crear_solicitud_vacaciones,
    preautorizar_solicitud_vacaciones_jefe,
    rechazar_solicitud_vacaciones,
    saldo_vacaciones_empleado,
)
from .services_vacantes import (
    can_autorizar_vacante,
    can_solicitar_vacantes,
    can_ver_vacante,
    crear_solicitud_vacante,
)
from recetas.utils.normalizacion import normalizar_nombre


def _parse_decimal(raw: str | None) -> Decimal:
    try:
        return Decimal(str(raw or "0"))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def _parse_date(raw: str | None):
    value = (raw or "").strip()
    if not value:
        return None
    try:
        return dt_date.fromisoformat(value)
    except ValueError:
        return None


def _codigo_empleado_desde_post(post_data) -> str:
    return normalizar_codigo_empleado(post_data.get("codigo"))


def _codigo_catalogo_desde_valor(valor: str, fallback: str) -> str:
    base = normalizar_nombre(valor or fallback or "").upper()
    base = re.sub(r"[^A-Z0-9_ ]+", " ", base).strip()[:80]
    return re.sub(r"\s+", " ", base)


def _empleado_con_codigo_duplicado(codigo: str, empleado_id: int | None = None) -> Empleado | None:
    if not codigo:
        return None
    qs = Empleado.objects.filter(codigo__iexact=codigo)
    if empleado_id:
        qs = qs.exclude(pk=empleado_id)
    return qs.first()


def _guardar_upload_temporal(archivo) -> Path:
    suffix = Path(archivo.name or "").suffix.lower()
    with NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        for chunk in archivo.chunks():
            tmp.write(chunk)
        return Path(tmp.name)


def _catalogo_value(post_data, field_name: str, allowed_values: frozenset[str], current_value: str = "") -> str:
    value = (post_data.get(field_name) or "").strip()
    if value == "__otro__":
        raise ValidationError("Solo se permiten valores del catalogo oficial.")
    if value and value not in allowed_values and value != (current_value or ""):
        raise ValidationError("Selecciona un valor del catalogo oficial.")
    return value


def _organizacion_desde_post(post_data, empleado: Empleado | None = None) -> dict:
    area = _catalogo_value(post_data, "area", area_division_values(), empleado.area if empleado else "")
    defaults = area_division_map().get(area, {})
    puesto_operativo = (
        _catalogo_value(
            post_data,
            "puesto_operativo",
            puesto_operativo_values(),
            empleado.puesto_operativo if empleado else "",
        )
        or defaults.get("puesto_operativo", "")
    )
    nivel_organizacional = _catalogo_value(
        post_data,
        "nivel_organizacional",
        NIVEL_ORGANIZACIONAL_VALUES,
        empleado.nivel_organizacional if empleado else "",
    ) or defaults.get("nivel_organizacional", Empleado.NIVEL_COLABORADOR)
    departamento = (post_data.get("departamento") or defaults.get("departamento") or "").strip()
    departamento_origen = (post_data.get("departamento_origen") or defaults.get("departamento_origen") or departamento).strip()
    return {
        "area": area,
        "departamento": departamento,
        "departamento_origen": departamento_origen,
        "puesto_operativo": puesto_operativo,
        "nivel_organizacional": nivel_organizacional,
        "participa_bonos_ventas": post_data.get("participa_bonos_ventas") == "on",
        "participa_bonos_produccion": post_data.get("participa_bonos_produccion") == "on",
    }


def _resolver_jefe_directo_desde_post(post_data, organizacion: dict, empleado: Empleado | None = None) -> int | None:
    jefe_id = (post_data.get("jefe_directo") or "").strip()
    if not jefe_id:
        return None
    if not jefe_id.isdigit():
        raise ValidationError("Selecciona un jefe directo valido.")
    qs = Empleado.objects.filter(pk=int(jefe_id), activo=True).filter(liderazgo_q())
    if empleado:
        qs = qs.exclude(pk=empleado.pk)
    jefe = qs.first()
    if not jefe:
        raise ValidationError("El jefe directo debe ser una persona activa con nivel de liderazgo.")
    departamento = (organizacion.get("departamento") or "").strip().upper()
    jefe_departamento = (jefe.departamento or "").strip().upper()
    if departamento and jefe_departamento != departamento and jefe.nivel_organizacional != Empleado.NIVEL_DIRECCION:
        raise ValidationError("El jefe directo debe corresponder a la jerarquia del departamento.")
    return jefe.id


def _safe_int(raw: str | None) -> int | None:
    value = (raw or "").strip()
    return int(value) if value.isdigit() else None


def _crear_usuario_rrhh_para_empleado(request, empleado: Empleado):
    if request.POST.get("crear_usuario_erp") != "on":
        return None
    User = get_user_model()
    username = (request.POST.get("nuevo_usuario_username") or "").strip()
    password = (request.POST.get("nuevo_usuario_password") or "").strip()
    if not username:
        raise ValidationError("Captura el usuario para el acceso ERP/app.")
    if User.objects.filter(username__iexact=username).exists():
        raise ValidationError("Ese usuario ya existe. Selecciónalo en Usuario ERP o usa otro username.")
    if len(password) < 8:
        raise ValidationError("La contraseña temporal debe tener al menos 8 caracteres.")

    user = User.objects.create_user(
        username=username,
        email=empleado.email or "",
        password=password,
    )
    user.first_name = empleado.nombre.strip()
    user.last_name = ""
    user.is_active = True
    user.save(update_fields=["first_name", "last_name", "email", "is_active"])

    log_event(
        request.user,
        "CREATE",
        "auth.User",
        str(user.id),
        {
            "username": user.username,
            "source": "rrhh.empleados",
            "empleado": empleado.id,
            "password_created": True,
        },
    )
    return user


def _resolver_usuario_erp_desde_post(request, empleado: Empleado):
    crear_usuario = request.POST.get("crear_usuario_erp") == "on"
    usuario_erp_id = (request.POST.get("usuario_erp") or "").strip()
    if crear_usuario and usuario_erp_id:
        raise ValidationError("Elige crear usuario nuevo o vincular usuario existente, no ambos.")
    if crear_usuario and _logistica_tipo_desde_post(request.POST, empleado) and not _safe_int(request.POST.get("sucursal_app_id")):
        raise ValidationError("Selecciona la sucursal app para crear el usuario con acceso a logística.")
    if crear_usuario:
        return _crear_usuario_rrhh_para_empleado(request, empleado)
    if usuario_erp_id.isdigit():
        User = get_user_model()
        nuevo_user = User.objects.filter(pk=int(usuario_erp_id)).first()
        if nuevo_user and (
            not hasattr(nuevo_user, "empleado_rrhh")
            or nuevo_user.empleado_rrhh is None
            or nuevo_user.empleado_rrhh.pk == empleado.pk
        ):
            return nuevo_user
        if not nuevo_user:
            return None
    if usuario_erp_id == "":
        return empleado.usuario_erp
    return empleado.usuario_erp


def _usuarios_erp_disponibles_rrhh():
    return (
        get_user_model()
        .objects.filter(is_active=True, is_staff=False, is_superuser=False)
        .exclude(username__in=USUARIOS_ERP_EXCLUIDOS_RRHH)
        .order_by("username")
    )


def _logistica_tipo_desde_post(post_data, empleado: Empleado) -> str:
    value = (post_data.get("logistica_tipo_identidad") or "").strip()
    if value not in LOGISTICA_TIPOS_RRHH_VALUES:
        raise ValidationError("Selecciona un tipo de acceso logístico válido.")
    es_repartidor_operativo = (empleado.puesto_operativo or "").strip().upper() == "REPARTIDOR"
    if value == LOGISTICA_TIPO_CONDUCTOR_OCASIONAL and es_repartidor_operativo:
        raise ValidationError("Un puesto operativo repartidor debe quedar como Repartidor operativo.")
    if not value and es_repartidor_operativo:
        return LOGISTICA_TIPO_EMPLEADO_DOLCE
    return value


def _logistica_tipo_explicitado(post_data) -> bool:
    return bool((post_data.get("logistica_tipo_identidad") or "").strip())


def _sucursal_logistica_desde_post(request, empleado: Empleado):
    sucursal_app_id = _safe_int(request.POST.get("sucursal_app_id"))
    if sucursal_app_id:
        return Sucursal.objects.filter(pk=sucursal_app_id, activa=True).first()
    if not empleado.usuario_erp_id:
        return None
    try:
        return empleado.usuario_erp.repartidor_logistica.sucursal
    except Exception:
        pass
    try:
        return empleado.usuario_erp.userprofile.sucursal
    except Exception:
        return None


def _sincronizar_logistica_desde_post(request, empleado: Empleado) -> bool:
    tipo_identidad = _logistica_tipo_desde_post(request.POST, empleado)
    if not tipo_identidad:
        return False
    if not empleado.usuario_erp_id:
        if not _logistica_tipo_explicitado(request.POST):
            return False
        raise ValidationError("Vincula o crea usuario ERP/app antes de habilitar acceso logístico.")

    sucursal = _sucursal_logistica_desde_post(request, empleado)
    if not sucursal:
        raise ValidationError("Selecciona la sucursal en app para habilitar acceso logístico.")

    from logistica.models import Repartidor

    repartidor, created = Repartidor.objects.get_or_create(
        user=empleado.usuario_erp,
        defaults={
            "sucursal": sucursal,
            "telefono": empleado.telefono or "",
            "tipo_identidad": tipo_identidad,
        },
    )

    changed_fields: set[str] = set()
    if repartidor.sucursal_id != sucursal.id:
        repartidor.sucursal = sucursal
        changed_fields.add("sucursal")
    if empleado.telefono and repartidor.telefono != empleado.telefono:
        repartidor.telefono = empleado.telefono
        changed_fields.add("telefono")
    if repartidor.tipo_identidad != tipo_identidad:
        repartidor.tipo_identidad = tipo_identidad
        changed_fields.add("tipo_identidad")

    text_fields = {
        "motivo_autorizacion": "motivo_autorizacion",
        "autorizado_por": "autorizado_por",
        "notas_identidad": "notas_identidad",
    }
    for post_field, model_field in text_fields.items():
        value = (request.POST.get(post_field) or "").strip()
        if getattr(repartidor, model_field) != value:
            setattr(repartidor, model_field, value)
            changed_fields.add(model_field)

    field_map = {
        "numero_licencia": "numero_licencia",
        "licencia_expedicion": "licencia_expedicion",
        "licencia_expiracion": "licencia_expiracion",
    }
    for post_field, model_field in field_map.items():
        raw_value = (request.POST.get(post_field) or "").strip()
        value = _parse_date(raw_value) if model_field.startswith("licencia_") else raw_value
        if getattr(repartidor, model_field) != value:
            setattr(repartidor, model_field, value)
            changed_fields.add(model_field)

    archivo = request.FILES.get("archivo_licencia")
    if archivo:
        repartidor.archivo_licencia = archivo
        changed_fields.add("archivo_licencia")

    if changed_fields:
        repartidor.save(update_fields=sorted(changed_fields))

    grupo, _ = Group.objects.get_or_create(name="repartidor")
    if tipo_identidad == LOGISTICA_TIPO_EMPLEADO_DOLCE:
        empleado.usuario_erp.groups.add(grupo)
    elif (empleado.puesto_operativo or "").strip().upper() != "REPARTIDOR":
        empleado.usuario_erp.groups.remove(grupo)

    if created or changed_fields:
        log_event(
            request.user,
            "CREATE" if created else "UPDATE",
            "logistica.Repartidor",
            str(repartidor.id),
            {
                "empleado": empleado.id,
                "username": empleado.usuario_erp.username,
                "tipo_identidad": repartidor.tipo_identidad,
                "campos": sorted(changed_fields),
                "source": "rrhh.empleados",
            },
        )
    return created or bool(changed_fields)


RRHH_MODULE_TABS = [
    {"label": "Indicadores", "url_name": "rrhh:rrhh_indicadores", "key": "dashboard", "submodule": "dashboard"},
    {"label": "Organización", "url_name": "rrhh:rrhh_organizacion", "key": "organizacion", "submodule": "organizacion"},
    {"label": "Catálogos", "url_name": "rrhh:rrhh_catalogos", "key": "catalogos", "submodule": "catalogos"},
    {"label": "Empleados", "url_name": "rrhh:empleados", "key": "empleados", "submodule": "empleados"},
    {"label": "Permisos", "url_name": "rrhh:rrhh_permisos_list", "key": "permisos", "submodule": "permisos"},
    {"label": "Vacaciones", "url_name": "rrhh:rrhh_vacaciones_list", "key": "vacaciones", "submodule": "vacaciones"},
    {"label": "Horas extra", "url_name": "rrhh:rrhh_he_list", "key": "horas_extra", "submodule": "horas_extra"},
    {"label": "Asistencias", "url_name": "rrhh:rrhh_asistencias", "key": "asistencias", "submodule": "asistencias"},
    {"label": "Checador", "url_name": "rrhh:rrhh_importar", "key": "checador", "submodule": "importar_checador"},
    {"label": "Vacantes", "url_name": "rrhh:rrhh_vacantes", "key": "vacantes", "submodule": "vacantes"},
    {"label": "Préstamos", "url_name": "rrhh:rrhh_prestamos_lista", "key": "prestamos", "submodule": "prestamos"},
    {"label": "Nómina", "url_name": "rrhh:nomina", "key": "nomina", "submodule": "nomina"},
]


IDENTITY_PLANE_LABELS = {
    "accesos": "Accesos",
    "catalogos": "Catálogos",
    "personal": "Personal",
    "repartidores": "Repartidores",
    "usuarios": "Usuarios",
}

IDENTITY_SEVERITY_LABELS = {
    "risk": "Riesgo",
    "warning": "Revisión",
    "info": "Info",
}

IDENTITY_ACTION_LABELS = {
    "alinear_area_repartidor": "Alinear área de repartidor",
    "alinear_puesto_operativo_y_area": "Alinear área y puesto",
    "capturar_sucursal_requerida": "Capturar sucursal",
    "clasificar_cuenta_no_personal": "Clasificar cuenta técnica",
    "clasificar_usuario_sin_empleado": "Clasificar usuario sin empleado",
    "crear_departamento_core_faltante": "Crear departamento catálogo",
    "crear_perfil_desde_empleado_vinculado": "Crear perfil operativo",
    "definir_nivel_organizacional": "Definir nivel",
    "resolver_sucursal_legacy_no_mapeada": "Resolver sucursal no mapeada",
    "revisar_alias_grupo_canonico": "Revisar alias de grupo",
    "revisar_fusion_grupo_mayusculas": "Fusionar grupos por mayúsculas",
    "revisar_grupos_alias_en_usuario": "Revisar alias en usuario",
    "revisar_puesto_operativo_no_catalogado": "Revisar puesto no catalogado",
    "revisar_usuario_con_multiples_grupos": "Revisar múltiples grupos",
    "revisar_usuario_repartidor_sin_empleado": "Vincular usuario app a empleado",
    "separar_grupos_repartidor": "Separar rol repartidor",
    "separar_jefatura_de_puesto_operativo": "Separar jefatura de puesto",
    "validar_produccion_vs_embetunado": "Validar Producción vs Embetunado",
    "vincular_usuario_o_crear_perfil": "Vincular usuario o perfil",
    "vincular_usuario_repartidor": "Vincular usuario real de repartidor",
}


def _identity_map_context(limit: int = 80) -> dict:
    report = build_personnel_normalization_plan(limit=limit)
    rows = []
    for item in report["proposals"]:
        rows.append(
            {
                **item,
                "plane_label": IDENTITY_PLANE_LABELS.get(item["plane"], item["plane"]),
                "severity_label": IDENTITY_SEVERITY_LABELS.get(item["severity"], item["severity"]),
                "action_label": IDENTITY_ACTION_LABELS.get(item["action"], item["action"].replace("_", " ").title()),
            }
        )
    return {
        "summary": report["summary"],
        "rows": rows,
        "dry_run": report["dry_run"],
        "writes": report["writes"],
    }


def _has_rrhh_task_access(user, tab_key: str) -> bool:
    if not user or not getattr(user, "is_authenticated", False):
        return False
    if tab_key == "vacantes":
        return bool(
            can_solicitar_vacantes(user)
            or can_autorizar_vacante(user)
            or can_ver_vacante(user)
            or VacanteRRHH.objects.filter(Q(solicitado_por=user) | Q(creado_por=user)).exists()
        )
    if tab_key == "prestamos":
        return Prestamo.objects.filter(jefe_directo=user).exists()
    if tab_key == "horas_extra":
        return HoraExtra.objects.filter(jefe_directo=user).exists()
    if tab_key == "vacaciones":
        return SolicitudVacaciones.objects.filter(Q(jefe_directo=user) | Q(creado_por=user)).exists()
    return False


def _module_tabs(active: str, user=None) -> list[dict]:
    tabs = []
    for tab in RRHH_MODULE_TABS:
        if tab["key"] == "catalogos" and user is not None and not can_manage_rrhh(user):
            continue
        if user is not None and not (
            tab["key"] == "catalogos"
            or can_view_submodule(user, "rrhh", tab["submodule"])
            or _has_rrhh_task_access(user, tab["key"])
        ):
            continue
        tabs.append(
            {
                "label": tab["label"],
                "url_name": tab["url_name"],
                "active": active == tab["key"],
            }
        )
    return tabs


def _rrhh_enterprise_chain(
    *,
    empleados_total: int,
    empleados_activos: int,
    nominas_total: int,
    nominas_borrador: int,
    nominas_cerradas: int,
    nominas_pagadas: int,
) -> list[dict]:
    chain = [
        {
            "step": "01",
            "title": "Maestro de personal",
            "detail": "Altas activas, área y puesto del personal operativo.",
            "count": empleados_activos,
            "status": "Base activa" if empleados_activos else "Sin base activa",
            "tone": "success" if empleados_activos else "warning",
            "url": reverse("rrhh:empleados"),
            "cta": "Abrir empleados",
            "owner": "RRHH / Administración",
            "next_step": "Mantener plantilla activa y alineada con puesto, área y contrato.",
        },
        {
            "step": "02",
            "title": "Periodos de nómina",
            "detail": "Periodos creados para captura y control documental.",
            "count": nominas_total,
            "status": "Con periodos" if nominas_total else "Sin periodos",
            "tone": "success" if nominas_total else "warning",
            "url": reverse("rrhh:nomina"),
            "cta": "Abrir nómina",
            "owner": "RRHH / Nómina",
            "next_step": "Abrir periodos correctos y asegurar calendario de cálculo.",
        },
        {
            "step": "03",
            "title": "Cálculo y cierre",
            "detail": "Nóminas pendientes de cierre o validación.",
            "count": nominas_borrador,
            "status": "Sin borradores" if nominas_borrador == 0 else f"{nominas_borrador} en borrador",
            "tone": "success" if nominas_borrador == 0 else "danger",
            "url": reverse("rrhh:nomina") + f"?estatus={NominaPeriodo.ESTATUS_BORRADOR}",
            "cta": "Revisar borradores",
            "owner": "RRHH / Cálculo",
            "next_step": "Cerrar borradores y validar cálculo neto del periodo.",
        },
        {
            "step": "04",
            "title": "Pago y trazabilidad",
            "detail": "Nóminas cerradas o pagadas con control documental.",
            "count": nominas_pagadas,
            "status": "Pagadas" if nominas_pagadas else f"{nominas_cerradas} cerradas",
            "tone": "success" if nominas_pagadas or nominas_cerradas else "warning",
            "url": reverse("rrhh:nomina"),
            "cta": "Ver cierre",
            "owner": "RRHH / Auditoría",
            "next_step": "Documentar pago y resguardar evidencia de cierre del periodo.",
        },
    ]
    for index, item in enumerate(chain):
        previous = chain[index - 1] if index else None
        item["completion"] = 100 if item.get("tone") == "success" else (60 if item.get("tone") == "warning" else 25)
        item["depends_on"] = previous["title"] if previous else "Origen del módulo"
        if previous:
            item["dependency_status"] = (
                f"Condicionado por {previous['title'].lower()}"
                if previous.get("tone") != "success"
                else f"Listo desde {previous['title'].lower()}"
            )
        else:
            item["dependency_status"] = "Punto de arranque del módulo"
    return chain


def _month_bounds(raw_month: str | None):
    today = timezone.localdate()
    raw_month = (raw_month or today.strftime("%Y-%m")).strip()
    try:
        year, month = [int(part) for part in raw_month.split("-", 1)]
        start = dt_date(year, month, 1)
    except (TypeError, ValueError):
        start = dt_date(today.year, today.month, 1)
        raw_month = start.strftime("%Y-%m")
    end = dt_date(start.year, start.month, monthrange(start.year, start.month)[1])
    return raw_month, start, end


def _area_key(value: str | None) -> str:
    return (value or "SIN AREA").strip().upper() or "SIN AREA"


def _pct(numerator: Decimal | int, denominator: Decimal | int) -> Decimal:
    denominator = Decimal(str(denominator or "0"))
    if denominator == 0:
        return Decimal("0")
    return (Decimal(str(numerator or "0")) / denominator * Decimal("100")).quantize(Decimal("0.01"))


def _rrhh_document_stage_rows(
    *,
    empleados_total: int,
    empleados_activos: int,
    nominas_total: int,
    nominas_borrador: int,
    nominas_cerradas: int,
    nominas_pagadas: int,
) -> list[dict]:
    rows = [
        {
            "label": "Empleados activos",
            "open": empleados_activos,
            "closed": max(empleados_total - empleados_activos, 0),
            "detail": "Personal activo frente a inactivo.",
            "url": reverse("rrhh:empleados"),
            "owner": "RRHH / Administración",
            "next_step": "Regularizar plantilla activa y vigencia contractual.",
        },
        {
            "label": "Nóminas en borrador",
            "open": nominas_borrador,
            "closed": max(nominas_total - nominas_borrador, 0),
            "detail": "Periodos pendientes de cierre frente a periodos avanzados.",
            "url": reverse("rrhh:nomina") + f"?estatus={NominaPeriodo.ESTATUS_BORRADOR}",
            "owner": "RRHH / Nómina",
            "next_step": "Cerrar borradores y validar incidencias del periodo.",
        },
        {
            "label": "Nóminas cerradas",
            "open": nominas_cerradas,
            "closed": nominas_pagadas,
            "detail": "Periodos cerrados frente a pagados.",
            "url": reverse("rrhh:nomina") + f"?estatus={NominaPeriodo.ESTATUS_CERRADA}",
            "owner": "RRHH / Cálculo",
            "next_step": "Liberar pago y documentar cierre definitivo.",
        },
        {
            "label": "Nóminas pagadas",
            "open": nominas_pagadas,
            "closed": max(nominas_total - nominas_pagadas, 0),
            "detail": "Periodos ya liquidados frente a pendientes.",
            "url": reverse("rrhh:nomina") + f"?estatus={NominaPeriodo.ESTATUS_PAGADA}",
            "owner": "RRHH / Auditoría",
            "next_step": "Archivar evidencia y conciliar histórico del periodo.",
        },
    ]
    for row in rows:
        total = int(row["open"]) + int(row["closed"])
        row["completion"] = int(round((int(row["closed"]) / total) * 100)) if total else 100
    return rows


def _rrhh_operational_health_cards(
    *,
    focus: str,
    primary_open: int,
    secondary_open: int,
    ready_count: int,
) -> list[dict[str, object]]:
    if focus == "empleados":
        return [
            {
                "label": "Personal activo",
                "value": ready_count,
                "tone": "success" if ready_count else "warning",
                "detail": "Colaboradores activos listos para operación.",
            },
            {
                "label": "Nóminas abiertas",
                "value": primary_open,
                "tone": "warning" if primary_open else "success",
                "detail": "Periodos aún en captura o cierre.",
            },
            {
                "label": "Periodos pagados",
                "value": secondary_open,
                "tone": "primary",
                "detail": "Periodos ya liquidados con trazabilidad.",
            },
        ]
    if focus == "nomina":
        return [
            {
                "label": "Borradores activos",
                "value": primary_open,
                "tone": "warning" if primary_open else "success",
                "detail": "Nóminas que todavía requieren cierre.",
            },
            {
                "label": "Nóminas cerradas",
                "value": secondary_open,
                "tone": "primary",
                "detail": "Periodos validados pendientes de pago o archivados.",
            },
            {
                "label": "Nóminas pagadas",
                "value": ready_count,
                "tone": "success",
                "detail": "Periodos liquidados correctamente.",
            },
        ]
    return [
        {
            "label": "Líneas capturadas",
            "value": primary_open,
            "tone": "primary",
            "detail": "Partidas salariales registradas en el periodo.",
        },
        {
            "label": "Periodo abierto",
            "value": secondary_open,
            "tone": "warning" if secondary_open else "success",
            "detail": "Indica si el periodo sigue pendiente de cierre.",
        },
        {
            "label": "Periodo cerrado",
            "value": ready_count,
            "tone": "success",
            "detail": "Periodo con cierre o pago documentado.",
        },
    ]


def _rrhh_governance_rows(rows: list[dict], owner_default: str = "RRHH / Operación") -> list[dict]:
    governance_rows: list[dict] = []
    for row in rows:
        governance_rows.append(
            {
                "front": row.get("label", "Frente RRHH"),
                "owner": row.get("owner") or owner_default,
                "blockers": int(row.get("open") or 0),
                "completion": int(row.get("completion") or 0),
                "detail": row.get("detail", ""),
                "next_step": row.get("next_step") or "Seguir flujo RRHH",
                "url": row.get("url") or reverse("rrhh:empleados"),
                "cta": "Abrir",
            }
        )
    return governance_rows


def _rrhh_maturity_summary(*, chain: list[dict], default_url: str) -> dict[str, object]:
    completed_steps = sum(1 for item in chain if item.get("tone") == "success")
    attention_steps = len(chain) - completed_steps
    coverage_pct = int(round((completed_steps / len(chain)) * 100)) if chain else 0
    next_priority = next((item for item in chain if item.get("tone") != "success"), None)
    if not next_priority:
        next_priority = {
            "title": "Operación estabilizada",
            "detail": "No hay brechas críticas abiertas en RRHH.",
            "url": default_url,
            "cta": "Abrir RRHH",
        }
    return {
        "completed_steps": completed_steps,
        "attention_steps": attention_steps,
        "coverage_pct": coverage_pct,
        "next_priority_title": next_priority.get("title", "Operación estabilizada"),
        "next_priority_detail": next_priority.get("detail", "No hay brechas críticas abiertas en RRHH."),
        "next_priority_url": next_priority.get("url", default_url),
        "next_priority_cta": next_priority.get("cta", "Abrir RRHH"),
    }


def _rrhh_critical_path_rows(chain: list[dict[str, object]]) -> list[dict[str, object]]:
    severity_order = {"danger": 0, "warning": 1, "success": 2}
    ranked = sorted(
        chain,
        key=lambda item: (
            severity_order.get(str(item.get("tone") or "warning"), 9),
            -int(item.get("count") or 0),
            int(item.get("completion") or 0),
        ),
    )
    rows: list[dict[str, object]] = []
    for index, item in enumerate(ranked[:4], start=1):
        rows.append(
            {
                "rank": f"R{index}",
                "title": item.get("title", "Tramo RRHH"),
                "owner": item.get("owner", "RRHH / Operación"),
                "status": item.get("status", "Sin estado"),
                "tone": item.get("tone", "warning"),
                "count": int(item.get("count") or 0),
                "completion": int(item.get("completion") or 0),
                "depends_on": item.get("depends_on", "Origen del módulo"),
                "dependency_status": item.get("dependency_status", "Sin dependencia registrada"),
                "detail": item.get("detail", ""),
                "next_step": item.get("next_step", "Continuar flujo RRHH"),
                "url": item.get("url", reverse("rrhh:empleados")),
                "cta": item.get("cta", "Abrir"),
            }
        )
    return rows


def _rrhh_executive_radar_rows(
    governance_rows: list[dict[str, object]],
    *,
    default_owner: str = "RRHH / Operación",
    fallback_url: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in governance_rows[:4]:
        completion = int(row.get("completion") or 0)
        blockers = int(row.get("blockers") or 0)
        if blockers <= 0 and completion >= 90:
            tone = "success"
            status = "Controlado"
            dominant_blocker = "Sin bloqueo activo"
        elif completion >= 50:
            tone = "warning"
            status = "En seguimiento"
            dominant_blocker = row.get("detail", "") or "Brecha de RH en seguimiento"
        else:
            tone = "danger"
            status = "Con bloqueo"
            dominant_blocker = row.get("detail", "") or "Bloqueo de RH abierto"
        rows.append(
            {
                "phase": row.get("front", "Frente de RH"),
                "owner": row.get("owner") or default_owner,
                "status": status,
                "tone": tone,
                "blockers": blockers,
                "progress_pct": completion,
                "dominant_blocker": dominant_blocker,
                "depends_on": row.get("front", "Origen del módulo"),
                "dependency_status": row.get("next_step", "Sin dependencia registrada"),
                "next_step": row.get("next_step", "Abrir frente"),
                "url": row.get("url", fallback_url),
                "cta": row.get("cta", "Abrir"),
            }
        )
    return rows


def _rrhh_command_center(*, governance_rows: list[dict], maturity_summary: dict[str, object]) -> dict[str, object]:
    blockers = sum(int(row.get("blockers") or 0) for row in governance_rows)
    primary_row = max(governance_rows, key=lambda row: int(row.get("blockers") or 0), default={}) if governance_rows else {}
    tone = "success" if blockers == 0 else ("warning" if blockers <= 3 else "danger")
    status = "Listo para operar" if blockers == 0 else ("En atención" if blockers <= 3 else "Crítico")
    return {
        "owner": primary_row.get("owner") or "RRHH / Operación",
        "status": status,
        "tone": tone,
        "blockers": blockers,
        "next_step": maturity_summary.get("next_priority_detail") or "Continuar cierre documental del módulo.",
        "cta": maturity_summary.get("next_priority_cta") or primary_row.get("cta") or "Abrir",
        "url": maturity_summary.get("next_priority_url") or primary_row.get("url") or reverse("rrhh:empleados"),
    }


def _rrhh_release_gate_rows(
    *,
    empleados_total: int,
    empleados_activos: int,
    nominas_borrador: int,
    nominas_cerradas: int,
    nominas_pagadas: int,
    default_url: str,
) -> list[dict[str, object]]:
    return [
        {
            "step": "01",
            "title": "Maestro de colaboradores listo para operar",
            "detail": "Colaboradores activos y listos para operar en RRHH.",
            "completed": empleados_activos,
            "open_count": max(empleados_total - empleados_activos, 0),
            "total": max(empleados_total, 1),
            "tone": "success" if empleados_total and empleados_activos >= empleados_total else "warning",
            "url": default_url,
            "cta": "Revisar colaboradores",
        },
        {
            "step": "02",
            "title": "Periodo validado y cerrado",
            "detail": "Nóminas cerradas o listas para pago con cálculo completo.",
            "completed": nominas_cerradas + nominas_pagadas,
            "open_count": nominas_borrador,
            "total": max(nominas_borrador + nominas_cerradas + nominas_pagadas, 1),
            "tone": "success" if (nominas_cerradas + nominas_pagadas) else "warning",
            "url": reverse("rrhh:nomina"),
            "cta": "Revisar nóminas",
        },
        {
            "step": "03",
            "title": "Pago documentado y conciliado",
            "detail": "Periodos pagados con trazabilidad y cierre documental.",
            "completed": nominas_pagadas,
            "open_count": max((nominas_cerradas + nominas_pagadas) - nominas_pagadas, 0),
            "total": max(nominas_cerradas + nominas_pagadas, 1),
            "tone": "success" if nominas_pagadas else "warning",
            "url": reverse("rrhh:nomina"),
            "cta": "Ver cierres",
        },
    ]


def _rrhh_handoff_map(
    *,
    empleados_activos: int,
    nominas_borrador: int,
    nominas_cerradas: int,
    nominas_pagadas: int,
) -> list[dict[str, object]]:
    return [
        {
            "label": "Personal",
            "count": empleados_activos,
            "status": "Base activa" if empleados_activos else "Sin base activa",
            "detail": "Colaboradores listos para captura y cálculo de periodo.",
            "tone": "success" if empleados_activos else "warning",
            "url": reverse("rrhh:empleados"),
            "cta": "Abrir empleados",
            "owner": "RRHH / Administración",
            "depends_on": "Alta vigente del colaborador",
            "exit_criteria": "Todo colaborador activo debe tener datos listos para captura y cálculo.",
            "next_step": "Completar plantilla activa y regularizar cobertura de personal.",
            "completion": 100 if empleados_activos else 45,
        },
        {
            "label": "Captura",
            "count": nominas_borrador,
            "status": "En cálculo" if nominas_borrador else "Sin borradores",
            "detail": "Periodos todavía abiertos para captura o revisión.",
            "tone": "danger" if nominas_borrador else "success",
            "url": reverse("rrhh:nomina") + f"?estatus={NominaPeriodo.ESTATUS_BORRADOR}",
            "cta": "Revisar borradores",
            "owner": "RRHH / Nómina",
            "depends_on": "Plantilla y cobertura cerradas",
            "exit_criteria": "No dejar periodos abiertos antes de cierre documental.",
            "next_step": "Cerrar borradores y validar cálculo del periodo.",
            "completion": 100 if nominas_borrador == 0 else 55,
        },
        {
            "label": "Cierre",
            "count": nominas_cerradas,
            "status": "Cerradas" if nominas_cerradas else "Sin cierres",
            "detail": "Periodos validados pendientes de pago final o archivo.",
            "tone": "warning" if nominas_cerradas and not nominas_pagadas else "success",
            "url": reverse("rrhh:nomina") + f"?estatus={NominaPeriodo.ESTATUS_CERRADA}",
            "cta": "Ver cierres",
            "owner": "RRHH / Auditoría",
            "depends_on": "Captura y validación del periodo",
            "exit_criteria": "Todo periodo cerrado debe quedar con pago o evidencia de resguardo.",
            "next_step": "Documentar pago o cierre final del periodo.",
            "completion": 100 if nominas_cerradas == 0 or nominas_pagadas else 70,
        },
        {
            "label": "Pago",
            "count": nominas_pagadas,
            "status": "Pagadas" if nominas_pagadas else "Pendientes",
            "detail": "Periodos liquidados con trazabilidad documental.",
            "tone": "success" if nominas_pagadas else "warning",
            "url": reverse("rrhh:nomina") + f"?estatus={NominaPeriodo.ESTATUS_PAGADA}",
            "cta": "Ver pagos",
            "owner": "RRHH / Tesorería",
            "depends_on": "Periodo cerrado y autorizado",
            "exit_criteria": "Pago registrado y resguardado con evidencia trazable.",
            "next_step": "Confirmar liquidación y archivo documental del periodo.",
            "completion": 100 if nominas_pagadas else 60,
        },
    ]


def _rrhh_focus_cards(*, selected_focus: str) -> list[dict[str, object]]:
    cards = [
        {
            "key": "ACTIVOS",
            "label": "Personal activo",
            "count": Empleado.objects.filter(activo=True).count(),
            "detail": "Colaboradores habilitados para operación normal.",
            "url": reverse("rrhh:empleados") + "?enterprise_focus=ACTIVOS&estado=activos",
        },
        {
            "key": "INACTIVOS",
            "label": "Personal inactivo",
            "count": Empleado.objects.filter(activo=False).count(),
            "detail": "Colaboradores fuera de operación o con baja administrativa.",
            "url": reverse("rrhh:empleados") + "?enterprise_focus=INACTIVOS&estado=inactivos",
        },
        {
            "key": "SIN_AREA",
            "label": "Sin área asignada",
            "count": Empleado.objects.filter(Q(area__isnull=True) | Q(area="")).count(),
            "detail": "Expedientes que bloquean trazabilidad por estructura organizacional.",
            "url": reverse("rrhh:empleados") + "?enterprise_focus=SIN_AREA",
        },
        {
            "key": "SIN_SUCURSAL",
            "label": "Sin sucursal",
            "count": Empleado.objects.filter(Q(sucursal__isnull=True) | Q(sucursal="")).count(),
            "detail": "Registros sin centro operativo asignado para control y reporteo.",
            "url": reverse("rrhh:empleados") + "?enterprise_focus=SIN_SUCURSAL",
        },
    ]
    for card in cards:
        card["is_active"] = card["key"] == selected_focus
    return cards


def _rrhh_focus_summary(*, selected_focus: str, count: int) -> dict[str, object] | None:
    if not selected_focus:
        return None
    mapping = {
        "ACTIVOS": ("Personal activo", "Vista enfocada en colaboradores actualmente operativos."),
        "INACTIVOS": ("Personal inactivo", "Vista enfocada en expedientes fuera de operación."),
        "SIN_AREA": ("Sin área asignada", "Vista enfocada en expedientes que requieren estructura organizacional."),
        "SIN_SUCURSAL": ("Sin sucursal", "Vista enfocada en expedientes sin centro operativo asignado."),
    }
    title, detail = mapping.get(
        selected_focus,
        ("Foco RRHH", "Vista enfocada en un subconjunto operativo del módulo de RRHH."),
    )
    return {
        "title": title,
        "detail": detail,
        "count": count,
        "clear_url": reverse("rrhh:empleados"),
    }


@login_required
def empleados(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver RRHH")

    if request.method == "POST":
        if not can_manage_rrhh(request.user):
            raise PermissionDenied("No tienes permisos para gestionar RRHH")

        action = (request.POST.get("action") or "create").strip()
        nombre = (request.POST.get("nombre") or "").strip()
        if action == "vincular_identidad":
            pendiente = get_object_or_404(
                EmpleadoIdentidadPendiente,
                pk=request.POST.get("pendiente_id"),
                estado=EmpleadoIdentidadPendiente.ESTADO_PENDIENTE,
            )
            empleado = get_object_or_404(Empleado, pk=request.POST.get("empleado_id"))
            try:
                vincular_identidad_pendiente(pendiente, empleado, user=request.user)
            except ValueError as exc:
                messages.error(request, str(exc))
            else:
                messages.success(request, f"Código {pendiente.codigo_externo} vinculado a {empleado.nombre}.")
            return redirect("rrhh:empleados")
        if action == "baja":
            empleado = None
            empleado_id = (request.POST.get("empleado") or "").strip()
            if empleado_id.isdigit():
                empleado = Empleado.objects.filter(pk=int(empleado_id)).first()
            fecha_baja = _parse_date(request.POST.get("fecha_baja")) or timezone.localdate()
            fecha_ingreso = _parse_date(request.POST.get("fecha_ingreso"))
            baja = EmpleadoBaja.objects.create(
                empleado=empleado,
                nombre=(request.POST.get("nombre_baja") or (empleado.nombre if empleado else "")).strip(),
                area=_area_key(request.POST.get("area_baja") or (empleado.area if empleado else "")),
                puesto=(request.POST.get("puesto_baja") or (empleado.puesto if empleado else "")).strip(),
                tipo_contrato=(request.POST.get("tipo_contrato_baja") or (empleado.tipo_contrato if empleado else Empleado.CONTRATO_FIJO)).strip(),
                fecha_ingreso=fecha_ingreso or (empleado.fecha_ingreso if empleado else fecha_baja),
                fecha_baja=fecha_baja,
                motivo=(request.POST.get("motivo") or EmpleadoBaja.MOTIVO_OTRO).strip(),
                observacion=(request.POST.get("observacion") or "").strip(),
                creado_por=request.user,
            )
            if empleado and request.POST.get("marcar_inactivo") == "on":
                empleado.activo = False
                empleado.save(update_fields=["activo", "updated_at"])
            messages.success(request, f"Baja capturada para {baja.nombre}.")
            return redirect("rrhh:empleados")
        if action == "plantilla":
            anio = int(request.POST.get("anio") or timezone.localdate().year)
            mes_raw = (request.POST.get("mes_plantilla") or "").strip()
            mes_val = int(mes_raw) if mes_raw.isdigit() else None
            plantilla, _ = PlantillaAutorizada.objects.update_or_create(
                anio=anio,
                mes=mes_val,
                area=_area_key(request.POST.get("area_plantilla")),
                puesto=(request.POST.get("puesto_plantilla") or "").strip().upper(),
                defaults={
                    "cantidad": int(request.POST.get("cantidad") or 0),
                    "notas": (request.POST.get("notas") or "").strip(),
                    "actualizado_por": request.user,
                },
            )
            messages.success(request, f"Plantilla autorizada actualizada: {plantilla}.")
            return redirect("rrhh:empleados")
        codigo = _codigo_empleado_desde_post(request.POST)
        if not nombre:
            messages.error(request, "Nombre del empleado es obligatorio.")
        elif action == "update":
            empleado_id = (request.POST.get("empleado_id") or "").strip()
            empleado = get_object_or_404(Empleado, pk=int(empleado_id)) if empleado_id.isdigit() else None
            if not empleado:
                messages.error(request, "Selecciona un empleado válido para editar.")
            elif duplicado := _empleado_con_codigo_duplicado(codigo, empleado.id):
                messages.error(request, f"El código {codigo} ya pertenece a {duplicado.nombre}.")
            else:
                try:
                    organizacion = _organizacion_desde_post(request.POST, empleado)
                except ValidationError as exc:
                    messages.error(request, f"Organización inválida: {exc.messages[0]}")
                    return redirect("rrhh:empleados")
                if codigo:
                    empleado.codigo = codigo
                empleado.nombre = nombre
                empleado.rfc = (request.POST.get("rfc") or "").strip()
                empleado.curp = (request.POST.get("curp") or "").strip()
                empleado.nss = (request.POST.get("nss") or "").strip()
                empleado.area = organizacion["area"]
                empleado.puesto = (request.POST.get("puesto") or "").strip()
                empleado.departamento_origen = organizacion["departamento_origen"]
                empleado.departamento = organizacion["departamento"]
                empleado.puesto_operativo = organizacion["puesto_operativo"]
                empleado.nivel_organizacional = organizacion["nivel_organizacional"]
                try:
                    empleado.jefe_directo_id = _resolver_jefe_directo_desde_post(request.POST, organizacion, empleado)
                except ValidationError as exc:
                    messages.error(request, exc.messages[0])
                    return redirect("rrhh:empleados")
                empleado.tipo_personal = (request.POST.get("tipo_personal") or Empleado.TIPO_POLLYANA).strip()
                empleado.participa_bonos_ventas = organizacion["participa_bonos_ventas"]
                empleado.participa_bonos_produccion = organizacion["participa_bonos_produccion"]
                empleado.tipo_contrato = (request.POST.get("tipo_contrato") or Empleado.CONTRATO_FIJO).strip()
                empleado.fecha_ingreso = _parse_date(request.POST.get("fecha_ingreso")) or empleado.fecha_ingreso
                empleado.salario_diario = _parse_decimal(request.POST.get("salario_diario"))
                empleado.telefono = (request.POST.get("telefono") or "").strip()
                empleado.email = (request.POST.get("email") or "").strip()
                empleado.sucursal = (request.POST.get("sucursal") or "").strip()
                empleado.activo = request.POST.get("activo") == "on"
                try:
                    empleado.usuario_erp = _resolver_usuario_erp_desde_post(request, empleado)
                except ValidationError as exc:
                    messages.error(request, exc.messages[0])
                    return redirect("rrhh:empleados")
                empleado.save()
                sucursal_app_id = (request.POST.get("sucursal_app_id") or "").strip()
                asegurar_identidad_operativa_empleado(
                    empleado,
                    sucursal_app_id=int(sucursal_app_id) if sucursal_app_id.isdigit() else None,
                )
                try:
                    _sincronizar_logistica_desde_post(request, empleado)
                except ValidationError as exc:
                    messages.error(request, exc.messages[0])
                    return redirect("rrhh:empleados")
                sincronizar_esquemas_bono(empleado, request.POST, organizacion)
                log_event(
                    request.user,
                    "UPDATE",
                    "rrhh.Empleado",
                    str(empleado.id),
                    {
                        "codigo": empleado.codigo,
                        "nombre": empleado.nombre,
                        "activo": empleado.activo,
                    },
                )
                messages.success(request, f"Empleado {empleado.nombre} actualizado.")
                return redirect("rrhh:empleados")
        else:
            try:
                organizacion = _organizacion_desde_post(request.POST)
            except ValidationError as exc:
                messages.error(request, f"Organización inválida: {exc.messages[0]}")
                return redirect("rrhh:empleados")
            if duplicado := _empleado_con_codigo_duplicado(codigo):
                messages.error(request, f"El código {codigo} ya pertenece a {duplicado.nombre}.")
                return redirect("rrhh:empleados")
            try:
                jefe_directo_id = _resolver_jefe_directo_desde_post(request.POST, organizacion)
            except ValidationError as exc:
                messages.error(request, exc.messages[0])
                return redirect("rrhh:empleados")
            empleado = Empleado.objects.create(
                codigo=codigo,
                nombre=nombre,
                rfc=(request.POST.get("rfc") or "").strip(),
                curp=(request.POST.get("curp") or "").strip(),
                nss=(request.POST.get("nss") or "").strip(),
                area=organizacion["area"],
                puesto=(request.POST.get("puesto") or "").strip(),
                departamento_origen=organizacion["departamento_origen"],
                departamento=organizacion["departamento"],
                puesto_operativo=organizacion["puesto_operativo"],
                nivel_organizacional=organizacion["nivel_organizacional"],
                jefe_directo_id=jefe_directo_id,
                tipo_personal=(request.POST.get("tipo_personal") or Empleado.TIPO_POLLYANA).strip(),
                participa_bonos_ventas=organizacion["participa_bonos_ventas"],
                participa_bonos_produccion=organizacion["participa_bonos_produccion"],
                tipo_contrato=(request.POST.get("tipo_contrato") or Empleado.CONTRATO_FIJO).strip(),
                fecha_ingreso=request.POST.get("fecha_ingreso") or timezone.localdate(),
                salario_diario=_parse_decimal(request.POST.get("salario_diario")),
                telefono=(request.POST.get("telefono") or "").strip(),
                email=(request.POST.get("email") or "").strip(),
                sucursal=(request.POST.get("sucursal") or "").strip(),
            )
            try:
                empleado.usuario_erp = _resolver_usuario_erp_desde_post(request, empleado)
            except ValidationError as exc:
                empleado.delete()
                messages.error(request, exc.messages[0])
                return redirect("rrhh:empleados")
            if empleado.usuario_erp_id:
                empleado.save(update_fields=["usuario_erp"])
            sucursal_app_id = (request.POST.get("sucursal_app_id") or "").strip()
            asegurar_identidad_operativa_empleado(
                empleado,
                sucursal_app_id=int(sucursal_app_id) if sucursal_app_id.isdigit() else None,
            )
            try:
                _sincronizar_logistica_desde_post(request, empleado)
            except ValidationError as exc:
                empleado.delete()
                messages.error(request, exc.messages[0])
                return redirect("rrhh:empleados")
            sincronizar_esquemas_bono(empleado, request.POST, organizacion)
            log_event(
                request.user,
                "CREATE",
                "rrhh.Empleado",
                str(empleado.id),
                {
                    "codigo": empleado.codigo,
                    "nombre": empleado.nombre,
                    "salario_diario": str(empleado.salario_diario),
                },
            )
            messages.success(request, f"Empleado {empleado.nombre} registrado.")
            return redirect("rrhh:empleados")

    q = (request.GET.get("q") or "").strip()
    estado = (request.GET.get("estado") or "activos").strip().lower()
    enterprise_focus = (request.GET.get("enterprise_focus") or "").strip().upper()

    asegurar_esquemas_base()
    qs = Empleado.objects.all().prefetch_related("bonos_esquemas").annotate(total_lineas_nomina=Count("lineas_nomina"))
    if q:
        qs = qs.filter(
            Q(nombre__icontains=q)
            | Q(codigo__icontains=q)
            | Q(rfc__icontains=q)
            | Q(curp__icontains=q)
            | Q(nss__icontains=q)
            | Q(area__icontains=q)
            | Q(puesto__icontains=q)
            | Q(departamento__icontains=q)
            | Q(puesto_operativo__icontains=q)
            | Q(jefe_directo__nombre__icontains=q)
        )
    if estado == "activos":
        qs = qs.filter(activo=True)
    elif estado == "inactivos":
        qs = qs.filter(activo=False)
    if enterprise_focus == "ACTIVOS":
        qs = qs.filter(activo=True)
    elif enterprise_focus == "INACTIVOS":
        qs = qs.filter(activo=False)
    elif enterprise_focus == "SIN_AREA":
        qs = qs.filter(Q(area__isnull=True) | Q(area=""))
    elif enterprise_focus == "SIN_SUCURSAL":
        qs = qs.filter(Q(sucursal__isnull=True) | Q(sucursal=""))

    empleados_total = Empleado.objects.count()
    empleados_activos = Empleado.objects.filter(activo=True).count()
    empleados_inactivos = max(empleados_total - empleados_activos, 0)
    empleados_activos_con_usuario = Empleado.objects.filter(activo=True, usuario_erp__isnull=False).count()
    empleados_activos_sin_usuario = max(empleados_activos - empleados_activos_con_usuario, 0)
    identidades_pendientes_total = EmpleadoIdentidadPendiente.objects.filter(
        estado=EmpleadoIdentidadPendiente.ESTADO_PENDIENTE
    ).count()
    User = get_user_model()
    usuarios_activos_sin_empleado = (
        User.objects.filter(is_active=True, empleado_rrhh__isnull=True)
        .exclude(username__in=USUARIOS_ERP_EXCLUIDOS_RRHH)
        .count()
    )
    from logistica.models import Repartidor

    conductores_activos = Repartidor.objects.filter(user__is_active=True).count()
    conductores_ocasionales = Repartidor.objects.filter(
        user__is_active=True,
        tipo_identidad=LOGISTICA_TIPO_CONDUCTOR_OCASIONAL,
    ).count()
    nominas_total = NominaPeriodo.objects.count()
    nominas_borrador = NominaPeriodo.objects.filter(estatus=NominaPeriodo.ESTATUS_BORRADOR).count()
    nominas_cerradas = NominaPeriodo.objects.filter(estatus=NominaPeriodo.ESTATUS_CERRADA).count()
    nominas_pagadas = NominaPeriodo.objects.filter(estatus=NominaPeriodo.ESTATUS_PAGADA).count()
    enterprise_chain = _rrhh_enterprise_chain(
        empleados_total=empleados_total,
        empleados_activos=empleados_activos,
        nominas_total=nominas_total,
        nominas_borrador=nominas_borrador,
        nominas_cerradas=nominas_cerradas,
        nominas_pagadas=nominas_pagadas,
    )
    document_stage_rows = _rrhh_document_stage_rows(
        empleados_total=empleados_total,
        empleados_activos=empleados_activos,
        nominas_total=nominas_total,
        nominas_borrador=nominas_borrador,
        nominas_cerradas=nominas_cerradas,
        nominas_pagadas=nominas_pagadas,
    )
    maturity_summary = _rrhh_maturity_summary(
        chain=enterprise_chain,
        default_url=reverse("rrhh:empleados"),
    )
    handoff_map = _rrhh_handoff_map(
        empleados_activos=empleados_activos,
        nominas_borrador=nominas_borrador,
        nominas_cerradas=nominas_cerradas,
        nominas_pagadas=nominas_pagadas,
    )
    release_gate_rows = _rrhh_release_gate_rows(
        empleados_total=empleados_total,
        empleados_activos=empleados_activos,
        nominas_borrador=nominas_borrador,
        nominas_cerradas=nominas_cerradas,
        nominas_pagadas=nominas_pagadas,
        default_url=reverse("rrhh:empleados"),
    )

    empleados_page = list(qs.order_by("nombre")[:600])
    for empleado in empleados_page:
        empleado.bono_esquema_ids = {esquema.id for esquema in empleado.bonos_esquemas.all()}
        empleado.repartidor_logistica = None
        if empleado.usuario_erp_id:
            try:
                empleado.repartidor_logistica = empleado.usuario_erp.repartidor_logistica
            except Exception:
                empleado.repartidor_logistica = None

    identidades_pendientes = (
        EmpleadoIdentidadPendiente.objects.select_related("empleado_sugerido")
        .filter(estado=EmpleadoIdentidadPendiente.ESTADO_PENDIENTE)
        .order_by("-actualizado_en")[:20]
    )

    context = {
        "module_tabs": _module_tabs("empleados", request.user),
        "can_manage_rrhh": can_manage_rrhh(request.user),
        "empleados": empleados_page,
        "q": q,
        "estado": estado,
        "enterprise_focus": enterprise_focus,
        "total_empleados": empleados_total,
        "total_activos": empleados_activos,
        "total_inactivos": empleados_inactivos,
        "total_nominas": nominas_total,
        "identity_guard_cards": [
            {
                "label": "Fuente oficial",
                "value": empleados_activos,
                "status": "Controlado",
                "detail": "Empleado RRHH activo. Los módulos leen desde esta persona.",
            },
            {
                "label": "Con usuario/app",
                "value": empleados_activos_con_usuario,
                "status": "Ligados",
                "detail": "Credenciales ERP/app unidas al empleado.",
            },
            {
                "label": "Sin usuario/app",
                "value": empleados_activos_sin_usuario,
                "status": "Operativo",
                "detail": "Personal sin acceso digital requerido por ahora.",
            },
            {
                "label": "Nómina/Hikvision",
                "value": identidades_pendientes_total,
                "status": "Pendientes",
                "detail": "Códigos recibidos por vincular antes de duplicar.",
            },
            {
                "label": "Logística/unidades",
                "value": conductores_activos,
                "status": f"{conductores_ocasionales} ocasional(es)",
                "detail": "Repartidores y conductores ocasionales autorizados.",
            },
            {
                "label": "Usuarios sin empleado",
                "value": usuarios_activos_sin_empleado,
                "status": "Excepciones",
                "detail": "Solo deben quedar técnicos, externos o administradores.",
            },
        ],
        "identity_workflow_steps": [
            "Persona",
            "Organización",
            "Usuario/app",
            "Logística/documentos",
            "Bonos/permisos",
        ],
        "contrato_choices": Empleado.CONTRATO_CHOICES,
        "departamento_choices": Empleado.DEP_CHOICES,
        "area_division_choices": area_division_choices(),
        "area_division_values": area_division_values(),
        "puesto_operativo_choices": puesto_operativo_choices(),
        "puesto_operativo_values": puesto_operativo_values(),
        "logistica_tipo_identidad_choices": LOGISTICA_TIPOS_RRHH,
        "nivel_organizacional_choices": Empleado.NIVEL_ORGANIZACIONAL_CHOICES,
        "nivel_organizacional_values": NIVEL_ORGANIZACIONAL_VALUES,
        "tipo_personal_choices": Empleado.TIPO_PERSONAL_CHOICES,
        "bono_esquemas": BonoEsquema.objects.filter(activo=True).order_by("nombre"),
        "empleados_jefes": Empleado.objects.filter(activo=True).filter(liderazgo_q()).order_by("departamento", "nombre"),
        "usuarios_erp": _usuarios_erp_disponibles_rrhh(),
        "sucursales_app": Sucursal.objects.filter(activa=True).order_by("nombre"),
        "motivo_baja_choices": EmpleadoBaja.MOTIVO_CHOICES,
        "months": range(1, 13),
        "bajas_recientes": EmpleadoBaja.objects.select_related("empleado").order_by("-fecha_baja")[:8],
        "plantillas": PlantillaAutorizada.objects.order_by("-anio", "-mes", "area")[:8],
        "identidades_pendientes": identidades_pendientes,
        "enterprise_chain": enterprise_chain,
        "critical_path_rows": _rrhh_critical_path_rows(enterprise_chain),
        "document_stage_rows": document_stage_rows,
        "erp_governance_rows": _rrhh_governance_rows(document_stage_rows, owner_default="RRHH / Administración"),
        "executive_radar_rows": _rrhh_executive_radar_rows(
            _rrhh_governance_rows(document_stage_rows, owner_default="RRHH / Administración"),
            default_owner="RRHH / Administración",
            fallback_url=reverse("rrhh:empleados"),
        ),
        "maturity_summary": maturity_summary,
        "handoff_map": handoff_map,
        "release_gate_rows": release_gate_rows,
        "release_gate_completion": (
            int(
                (
                    sum(row["completed"] for row in release_gate_rows)
                    / sum(row["total"] for row in release_gate_rows)
                )
                * 100
            )
            if release_gate_rows and sum(row["total"] for row in release_gate_rows)
            else 0
        ),
        "focus_cards": _rrhh_focus_cards(selected_focus=enterprise_focus),
        "focus_summary": _rrhh_focus_summary(
            selected_focus=enterprise_focus,
            count=qs.count(),
        ),
        "operational_health_cards": _rrhh_operational_health_cards(
            focus="empleados",
            primary_open=nominas_borrador,
            secondary_open=nominas_pagadas,
            ready_count=empleados_activos,
        ),
    }
    context["erp_command_center"] = _rrhh_command_center(
        governance_rows=context["erp_governance_rows"],
        maturity_summary=maturity_summary,
    )
    return render(request, "rrhh/empleados.html", context)


@login_required
def nomina(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver RRHH")

    if request.method == "POST":
        if not can_manage_rrhh(request.user):
            raise PermissionDenied("No tienes permisos para gestionar RRHH")

        action = (request.POST.get("action") or "create_period").strip()
        if action == "import_lista_raya":
            archivo = request.FILES.get("archivo")
            replace = request.POST.get("replace") == "on"
            if not archivo:
                messages.error(request, "Selecciona el archivo XLS de lista de raya.")
                return redirect("rrhh:nomina")
            if Path(archivo.name or "").suffix.lower() != ".xls":
                messages.error(request, "La importación de lista de raya espera el archivo .xls de CONTPAQi.")
                return redirect("rrhh:nomina")

            temp_path = _guardar_upload_temporal(archivo)
            try:
                resultado = importar_lista_raya_nomina(
                    temp_path,
                    created_by=request.user,
                    replace=replace,
                    archivo_nombre=archivo.name,
                )
            except Exception as exc:
                messages.error(request, f"No se importó la lista de raya: {exc}")
                return redirect("rrhh:nomina")
            finally:
                temp_path.unlink(missing_ok=True)

            periodo = resultado["periodo"]
            importacion = resultado["importacion"]
            messages.success(
                request,
                f"Lista de raya importada: {importacion.empleados_detectados} empleados, "
                f"periodo {periodo.folio}, neto ${periodo.total_neto}.",
            )
            log_event(
                request.user,
                "CREATE",
                "rrhh.NominaImportacion",
                str(importacion.id),
                {
                    "archivo": importacion.archivo_nombre,
                    "periodo": periodo.folio,
                    "empleados": importacion.empleados_detectados,
                    "replace": replace,
                },
            )
            return redirect("rrhh:nomina_detail", pk=periodo.id)

        fecha_inicio = request.POST.get("fecha_inicio")
        fecha_fin = request.POST.get("fecha_fin")
        fecha_inicio_obj = _parse_date(fecha_inicio)
        fecha_fin_obj = _parse_date(fecha_fin)
        if not fecha_inicio_obj or not fecha_fin_obj:
            messages.error(request, "Fecha inicio y fin son obligatorias.")
        elif fecha_fin_obj < fecha_inicio_obj:
            messages.error(request, "La fecha fin no puede ser menor a fecha inicio.")
        else:
            nomina = NominaPeriodo.objects.create(
                tipo_periodo=(request.POST.get("tipo_periodo") or NominaPeriodo.TIPO_QUINCENAL).strip(),
                fecha_inicio=fecha_inicio_obj,
                fecha_fin=fecha_fin_obj,
                estatus=(request.POST.get("estatus") or NominaPeriodo.ESTATUS_BORRADOR).strip(),
                notas=(request.POST.get("notas") or "").strip(),
                created_by=request.user,
            )
            log_event(
                request.user,
                "CREATE",
                "rrhh.NominaPeriodo",
                str(nomina.id),
                {
                    "folio": nomina.folio,
                    "tipo_periodo": nomina.tipo_periodo,
                    "fecha_inicio": str(nomina.fecha_inicio),
                    "fecha_fin": str(nomina.fecha_fin),
                },
            )
            messages.success(request, f"Nómina {nomina.folio} creada.")
            return redirect("rrhh:nomina_detail", pk=nomina.id)

    estatus = (request.GET.get("estatus") or "").strip().upper()
    tipo = (request.GET.get("tipo") or "").strip().upper()

    nominas_qs = NominaPeriodo.objects.all()
    if estatus:
        nominas_qs = nominas_qs.filter(estatus=estatus)
    if tipo:
        nominas_qs = nominas_qs.filter(tipo_periodo=tipo)

    nominas_total = NominaPeriodo.objects.count()
    nominas_borrador = NominaPeriodo.objects.filter(estatus=NominaPeriodo.ESTATUS_BORRADOR).count()
    nominas_cerradas = NominaPeriodo.objects.filter(estatus=NominaPeriodo.ESTATUS_CERRADA).count()
    nominas_pagadas = NominaPeriodo.objects.filter(estatus=NominaPeriodo.ESTATUS_PAGADA).count()
    enterprise_chain = _rrhh_enterprise_chain(
        empleados_total=Empleado.objects.count(),
        empleados_activos=Empleado.objects.filter(activo=True).count(),
        nominas_total=nominas_total,
        nominas_borrador=nominas_borrador,
        nominas_cerradas=nominas_cerradas,
        nominas_pagadas=nominas_pagadas,
    )
    document_stage_rows = _rrhh_document_stage_rows(
        empleados_total=Empleado.objects.count(),
        empleados_activos=Empleado.objects.filter(activo=True).count(),
        nominas_total=nominas_total,
        nominas_borrador=nominas_borrador,
        nominas_cerradas=nominas_cerradas,
        nominas_pagadas=nominas_pagadas,
    )
    maturity_summary = _rrhh_maturity_summary(
        chain=enterprise_chain,
        default_url=reverse("rrhh:nomina"),
    )
    handoff_map = _rrhh_handoff_map(
        empleados_activos=Empleado.objects.filter(activo=True).count(),
        nominas_borrador=nominas_borrador,
        nominas_cerradas=nominas_cerradas,
        nominas_pagadas=nominas_pagadas,
    )
    release_gate_rows = _rrhh_release_gate_rows(
        empleados_total=Empleado.objects.count(),
        empleados_activos=Empleado.objects.filter(activo=True).count(),
        nominas_borrador=nominas_borrador,
        nominas_cerradas=nominas_cerradas,
        nominas_pagadas=nominas_pagadas,
        default_url=reverse("rrhh:nomina"),
    )

    context = {
        "module_tabs": _module_tabs("nomina", request.user),
        "can_manage_rrhh": can_manage_rrhh(request.user),
        "nominas": nominas_qs.order_by("-fecha_fin", "-id")[:120],
        "importaciones": NominaImportacion.objects.select_related("periodo", "created_by")[:10],
        "estatus": estatus,
        "tipo": tipo,
        "tipo_choices": NominaPeriodo.TIPO_CHOICES,
        "estatus_choices": NominaPeriodo.ESTATUS_CHOICES,
        "total_nominas": nominas_total,
        "nominas_borrador": nominas_borrador,
        "enterprise_chain": enterprise_chain,
        "critical_path_rows": _rrhh_critical_path_rows(enterprise_chain),
        "document_stage_rows": document_stage_rows,
        "erp_governance_rows": _rrhh_governance_rows(document_stage_rows, owner_default="RRHH / Nómina"),
        "executive_radar_rows": _rrhh_executive_radar_rows(
            _rrhh_governance_rows(document_stage_rows, owner_default="RRHH / Nómina"),
            default_owner="RRHH / Nómina",
            fallback_url=reverse("rrhh:nomina"),
        ),
        "maturity_summary": maturity_summary,
        "handoff_map": handoff_map,
        "release_gate_rows": release_gate_rows,
        "release_gate_completion": (
            int(
                (
                    sum(row["completed"] for row in release_gate_rows)
                    / sum(row["total"] for row in release_gate_rows)
                )
                * 100
            )
            if release_gate_rows and sum(row["total"] for row in release_gate_rows)
            else 0
        ),
        "operational_health_cards": _rrhh_operational_health_cards(
            focus="nomina",
            primary_open=nominas_borrador,
            secondary_open=nominas_cerradas,
            ready_count=nominas_pagadas,
        ),
    }
    context["erp_command_center"] = _rrhh_command_center(
        governance_rows=context["erp_governance_rows"],
        maturity_summary=maturity_summary,
    )
    return render(request, "rrhh/nomina.html", context)


@login_required
def nomina_detail(request, pk: int):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver RRHH")

    periodo = get_object_or_404(NominaPeriodo, pk=pk)

    if request.method == "POST":
        if not can_manage_rrhh(request.user):
            raise PermissionDenied("No tienes permisos para gestionar RRHH")

        action = (request.POST.get("action") or "add_line").strip()
        if action == "add_line":
            empleado_id = (request.POST.get("empleado_id") or "").strip()
            if not empleado_id.isdigit():
                messages.error(request, "Selecciona un empleado válido.")
                return redirect("rrhh:nomina_detail", pk=periodo.id)
            empleado = get_object_or_404(Empleado, pk=int(empleado_id), activo=True)
            with transaction.atomic():
                linea, _ = NominaLinea.objects.get_or_create(periodo=periodo, empleado=empleado)
                linea.dias_trabajados = _parse_decimal(request.POST.get("dias_trabajados"))
                linea.salario_base = _parse_decimal(request.POST.get("salario_base"))
                linea.bonos = _parse_decimal(request.POST.get("bonos"))
                linea.descuentos = _parse_decimal(request.POST.get("descuentos"))
                linea.observaciones = (request.POST.get("observaciones") or "").strip()
                linea.save()
                periodo.recompute_totals()
                periodo.save(update_fields=["total_bruto", "total_descuentos", "total_neto", "updated_at"])
            log_event(
                request.user,
                "UPDATE",
                "rrhh.NominaLinea",
                str(linea.id),
                {
                    "periodo": periodo.folio,
                    "empleado": empleado.nombre,
                    "neto": str(linea.neto_calculado),
                },
            )
            messages.success(request, f"Línea de nómina guardada para {empleado.nombre}.")
            return redirect("rrhh:nomina_detail", pk=periodo.id)

        if action == "delete_line":
            line_id = (request.POST.get("line_id") or "").strip()
            if line_id.isdigit():
                line = NominaLinea.objects.filter(pk=int(line_id), periodo=periodo).first()
                if line:
                    line.delete()
                    periodo.recompute_totals()
                    periodo.save(update_fields=["total_bruto", "total_descuentos", "total_neto", "updated_at"])
                    messages.success(request, "Línea eliminada.")
            return redirect("rrhh:nomina_detail", pk=periodo.id)

    lineas_total = periodo.lineas.count()
    enterprise_chain = [
        {
            "step": "01",
            "title": "Periodo abierto",
            "detail": "Periodo de nómina registrado para captura.",
            "count": 1,
            "status": periodo.folio,
            "tone": "success",
            "url": reverse("rrhh:nomina_detail", kwargs={"pk": periodo.id}),
            "cta": "Ver periodo",
            "owner": "RRHH / Nómina",
            "next_step": "Capturar líneas y validar colaboradores incluidos en el periodo.",
        },
        {
            "step": "02",
            "title": "Captura de líneas",
            "detail": "Empleados cargados y percepciones calculadas.",
            "count": lineas_total,
            "status": "Con líneas" if lineas_total else "Sin líneas",
            "tone": "success" if lineas_total else "warning",
            "url": reverse("rrhh:nomina_detail", kwargs={"pk": periodo.id}),
            "cta": "Capturar líneas",
            "owner": "RRHH / Captura",
            "next_step": "Completar percepciones, descuentos y observaciones por empleado.",
        },
        {
            "step": "03",
            "title": "Cierre del periodo",
            "detail": "Periodo validado y listo para liberar.",
            "count": 1 if periodo.estatus == NominaPeriodo.ESTATUS_CERRADA else 0,
            "status": periodo.get_estatus_display(),
            "tone": "success" if periodo.estatus in {NominaPeriodo.ESTATUS_CERRADA, NominaPeriodo.ESTATUS_PAGADA} else "warning",
            "url": reverse("rrhh:nomina_detail", kwargs={"pk": periodo.id}),
            "cta": "Validar cierre",
            "owner": "RRHH / Cálculo",
            "next_step": "Validar neto, descuentos y consistencia antes del cierre.",
        },
        {
            "step": "04",
            "title": "Pago documentado",
            "detail": "Periodo liquidado con trazabilidad documental.",
            "count": 1 if periodo.estatus == NominaPeriodo.ESTATUS_PAGADA else 0,
            "status": "Pagada" if periodo.estatus == NominaPeriodo.ESTATUS_PAGADA else "Pendiente",
            "tone": "success" if periodo.estatus == NominaPeriodo.ESTATUS_PAGADA else "warning",
            "url": reverse("rrhh:nomina_detail", kwargs={"pk": periodo.id}),
            "cta": "Revisar pago",
            "owner": "RRHH / Auditoría",
            "next_step": "Registrar pago y dejar evidencia terminal del periodo.",
        },
    ]
    for index, item in enumerate(enterprise_chain):
        previous = enterprise_chain[index - 1] if index else None
        item["completion"] = 100 if item.get("tone") == "success" else (60 if item.get("tone") == "warning" else 25)
        item["depends_on"] = previous["title"] if previous else "Origen del módulo"
        if previous:
            item["dependency_status"] = (
                f"Condicionado por {previous['title'].lower()}"
                if previous.get("tone") != "success"
                else f"Listo desde {previous['title'].lower()}"
            )
        else:
            item["dependency_status"] = "Punto de arranque del módulo"
    document_stage_rows = [
        {
            "label": "Líneas capturadas",
            "open": lineas_total,
            "closed": 0,
            "detail": "Registro documental del periodo.",
            "url": reverse("rrhh:nomina_detail", kwargs={"pk": periodo.id}),
            "owner": "RRHH / Nómina",
            "next_step": "Completar captura y validar totales del periodo.",
        },
        {
            "label": "Periodo en revisión",
            "open": 1 if periodo.estatus == NominaPeriodo.ESTATUS_BORRADOR else 0,
            "closed": 1 if periodo.estatus != NominaPeriodo.ESTATUS_BORRADOR else 0,
            "detail": "Borrador frente a periodos ya validados.",
            "url": reverse("rrhh:nomina_detail", kwargs={"pk": periodo.id}),
            "owner": "RRHH / Cálculo",
            "next_step": "Cerrar el periodo cuando el cálculo quede validado.",
        },
        {
            "label": "Periodo pagado",
            "open": 1 if periodo.estatus == NominaPeriodo.ESTATUS_PAGADA else 0,
            "closed": 1 if periodo.estatus != NominaPeriodo.ESTATUS_PAGADA else 0,
            "detail": "Liquidación documental del periodo.",
            "url": reverse("rrhh:nomina_detail", kwargs={"pk": periodo.id}),
            "owner": "RRHH / Auditoría",
            "next_step": "Resguardar evidencia de pago y conciliación final.",
        },
    ]
    for row in document_stage_rows:
        total = int(row["open"]) + int(row["closed"])
        row["completion"] = int(round((int(row["closed"]) / total) * 100)) if total else 100
    maturity_summary = _rrhh_maturity_summary(
        chain=enterprise_chain,
        default_url=reverse("rrhh:nomina_detail", kwargs={"pk": periodo.id}),
    )
    handoff_map = _rrhh_handoff_map(
        empleados_activos=Empleado.objects.filter(activo=True).count(),
        nominas_borrador=1 if periodo.estatus == NominaPeriodo.ESTATUS_BORRADOR else 0,
        nominas_cerradas=1 if periodo.estatus == NominaPeriodo.ESTATUS_CERRADA else 0,
        nominas_pagadas=1 if periodo.estatus == NominaPeriodo.ESTATUS_PAGADA else 0,
    )
    release_gate_rows = _rrhh_release_gate_rows(
        empleados_total=1,
        empleados_activos=1,
        nominas_borrador=1 if periodo.estatus == NominaPeriodo.ESTATUS_BORRADOR else 0,
        nominas_cerradas=1 if periodo.estatus == NominaPeriodo.ESTATUS_CERRADA else 0,
        nominas_pagadas=1 if periodo.estatus == NominaPeriodo.ESTATUS_PAGADA else 0,
        default_url=reverse("rrhh:nomina_detail", kwargs={"pk": periodo.id}),
    )

    context = {
        "module_tabs": _module_tabs("nomina", request.user),
        "can_manage_rrhh": can_manage_rrhh(request.user),
        "periodo": periodo,
        "lineas": periodo.lineas.select_related("empleado").order_by("empleado__nombre", "id"),
        "empleados": Empleado.objects.filter(activo=True).order_by("nombre")[:1000],
        "estatus_choices": NominaPeriodo.ESTATUS_CHOICES,
        "enterprise_chain": enterprise_chain,
        "critical_path_rows": _rrhh_critical_path_rows(enterprise_chain),
        "document_stage_rows": document_stage_rows,
        "erp_governance_rows": _rrhh_governance_rows(document_stage_rows, owner_default="RRHH / Operación"),
        "executive_radar_rows": _rrhh_executive_radar_rows(
            _rrhh_governance_rows(document_stage_rows, owner_default="RRHH / Operación"),
            default_owner="RRHH / Operación",
            fallback_url=reverse("rrhh:nomina_detail", kwargs={"pk": periodo.pk}),
        ),
        "maturity_summary": maturity_summary,
        "handoff_map": handoff_map,
        "release_gate_rows": release_gate_rows,
        "release_gate_completion": (
            int(
                (
                    sum(row["completed"] for row in release_gate_rows)
                    / sum(row["total"] for row in release_gate_rows)
                )
                * 100
            )
            if release_gate_rows and sum(row["total"] for row in release_gate_rows)
            else 0
        ),
        "operational_health_cards": _rrhh_operational_health_cards(
            focus="nomina_detail",
            primary_open=lineas_total,
            secondary_open=1 if periodo.estatus == NominaPeriodo.ESTATUS_BORRADOR else 0,
            ready_count=1 if periodo.estatus in {NominaPeriodo.ESTATUS_CERRADA, NominaPeriodo.ESTATUS_PAGADA} else 0,
        ),
    }
    context["erp_command_center"] = _rrhh_command_center(
        governance_rows=context["erp_governance_rows"],
        maturity_summary=maturity_summary,
    )
    return render(request, "rrhh/nomina_detail.html", context)


@login_required
def nomina_status(request, pk: int, estatus: str):
    if request.method != "POST":
        return redirect("rrhh:nomina")
    if not can_manage_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para gestionar RRHH")

    estatus = (estatus or "").strip().upper()
    if estatus not in {choice[0] for choice in NominaPeriodo.ESTATUS_CHOICES}:
        messages.error(request, "Estatus inválido.")
        return redirect("rrhh:nomina")

    periodo = get_object_or_404(NominaPeriodo, pk=pk)
    from_status = periodo.estatus
    if from_status == estatus:
        return redirect("rrhh:nomina_detail", pk=periodo.id)

    periodo.estatus = estatus
    periodo.save(update_fields=["estatus", "updated_at"])
    log_event(
        request.user,
        "UPDATE",
        "rrhh.NominaPeriodo",
        str(periodo.id),
        {"folio": periodo.folio, "from": from_status, "to": estatus},
    )
    messages.success(request, f"Nómina {periodo.folio} actualizada a {estatus}.")
    return redirect("rrhh:nomina_detail", pk=periodo.id)


@login_required
def indicadores_ch(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver indicadores de Capital Humano")

    mes, inicio, fin = _month_bounds(request.GET.get("mes") or request.POST.get("mes"))

    if request.method == "POST":
        if not can_manage_rrhh(request.user):
            raise PermissionDenied("No tienes permisos para capturar indicadores de Capital Humano")
        action = (request.POST.get("action") or "").strip()
        if action == "baja":
            empleado = None
            empleado_id = (request.POST.get("empleado") or "").strip()
            if empleado_id.isdigit():
                empleado = Empleado.objects.filter(pk=int(empleado_id)).first()
            fecha_baja = _parse_date(request.POST.get("fecha_baja")) or timezone.localdate()
            fecha_ingreso = _parse_date(request.POST.get("fecha_ingreso"))
            baja = EmpleadoBaja.objects.create(
                empleado=empleado,
                nombre=(request.POST.get("nombre") or (empleado.nombre if empleado else "")).strip(),
                area=_area_key(request.POST.get("area") or (empleado.area if empleado else "")),
                puesto=(request.POST.get("puesto") or (empleado.puesto if empleado else "")).strip(),
                tipo_contrato=(request.POST.get("tipo_contrato") or (empleado.tipo_contrato if empleado else Empleado.CONTRATO_FIJO)).strip(),
                fecha_ingreso=fecha_ingreso or (empleado.fecha_ingreso if empleado else fecha_baja),
                fecha_baja=fecha_baja,
                motivo=(request.POST.get("motivo") or EmpleadoBaja.MOTIVO_OTRO).strip(),
                observacion=(request.POST.get("observacion") or "").strip(),
                creado_por=request.user,
            )
            if empleado and request.POST.get("marcar_inactivo") == "on":
                empleado.activo = False
                empleado.save(update_fields=["activo", "updated_at"])
            messages.success(request, f"Baja capturada para {baja.nombre}.")
        elif action == "plantilla":
            anio = int(request.POST.get("anio") or inicio.year)
            mes_raw = (request.POST.get("mes_plantilla") or "").strip()
            mes_val = int(mes_raw) if mes_raw.isdigit() else None
            area = _area_key(request.POST.get("area"))
            puesto = (request.POST.get("puesto") or "").strip().upper()
            plantilla, _ = PlantillaAutorizada.objects.update_or_create(
                anio=anio,
                mes=mes_val,
                area=area,
                puesto=puesto,
                defaults={
                    "cantidad": int(request.POST.get("cantidad") or 0),
                    "notas": (request.POST.get("notas") or "").strip(),
                    "actualizado_por": request.user,
                },
            )
            messages.success(request, f"Plantilla autorizada actualizada: {plantilla}.")
        elif action == "vacante":
            vacante = crear_solicitud_vacante(
                area=_area_key(request.POST.get("area")),
                puesto=(request.POST.get("puesto") or "").strip().upper(),
                fecha_solicitada=_parse_date(request.POST.get("fecha_solicitada")) or timezone.localdate(),
                motivo_solicitud=(request.POST.get("motivo_no_cubierta") or "").strip(),
                sugerencias=(request.POST.get("sugerencias") or "").strip(),
                solicitado_por=request.user,
                creado_por=request.user,
            )
            messages.success(request, f"Vacante capturada: {vacante.area} · {vacante.puesto}.")
        return redirect(f"{reverse('rrhh:rrhh_indicadores')}?mes={mes}")

    periodos = NominaPeriodo.objects.filter(fecha_inicio__lte=fin, fecha_fin__gte=inicio).order_by("fecha_inicio")
    periodo_ids = list(periodos.values_list("id", flat=True))
    lineas = NominaLinea.objects.filter(periodo_id__in=periodo_ids).select_related("periodo", "empleado")
    payroll = periodos.aggregate(bruto=Sum("total_bruto"), descuentos=Sum("total_descuentos"), neto=Sum("total_neto"))

    first_period = periodos.first()
    last_period = periodos.last()
    plantilla_inicial = (
        NominaLinea.objects.filter(periodo=first_period).values("empleado_id").distinct().count()
        if first_period
        else 0
    )
    plantilla_final = (
        NominaLinea.objects.filter(periodo=last_period).values("empleado_id").distinct().count()
        if last_period
        else Empleado.objects.filter(activo=True).count()
    )

    altas = Empleado.objects.filter(fecha_ingreso__gte=inicio, fecha_ingreso__lte=fin).count()
    bajas = EmpleadoBaja.objects.filter(fecha_baja__gte=inicio, fecha_baja__lte=fin)
    bajas_count = bajas.count()
    promedio_base = (Decimal(plantilla_inicial) + Decimal(plantilla_final)) / Decimal("2") if plantilla_inicial or plantilla_final else Decimal("0")
    rotacion = _pct(bajas_count, promedio_base)

    he_conceptos = NominaConceptoLinea.objects.filter(linea__periodo_id__in=periodo_ids).filter(
        Q(codigo_concepto="4") | Q(nombre__icontains="Horas extras")
    )
    he_totales = he_conceptos.aggregate(horas=Sum("valor"), costo=Sum("importe"), registros=Count("id"))

    bajas_antiguedades = [baja.antiguedad_meses for baja in bajas]
    permanencia_promedio = (
        sum(bajas_antiguedades, Decimal("0")) / Decimal(len(bajas_antiguedades))
        if bajas_antiguedades
        else Decimal("0")
    ).quantize(Decimal("0.01"))
    bajas_prueba = sum(1 for baja in bajas if baja.en_periodo_prueba)

    plantilla_qs = PlantillaAutorizada.objects.filter(anio=inicio.year).filter(Q(mes=inicio.month) | Q(mes__isnull=True))
    plantilla_autorizada = plantilla_qs.aggregate(total=Sum("cantidad"))["total"] or 0
    vacantes = VacanteRRHH.objects.filter(fecha_solicitada__gte=inicio, fecha_solicitada__lte=fin)
    vacantes_count = vacantes.count()
    vacantes_cubiertas = vacantes.filter(estado=VacanteRRHH.ESTADO_CUBIERTA).count()
    vacantes_pendientes = vacantes.exclude(estado__in=[VacanteRRHH.ESTADO_CUBIERTA, VacanteRRHH.ESTADO_CANCELADA]).count()
    dias_cubiertas = [vacante.dias_en_cubrir for vacante in vacantes if vacante.dias_en_cubrir is not None]
    promedio_cobertura = sum(dias_cubiertas) / len(dias_cubiertas) if dias_cubiertas else 0

    areas = set()
    lineas_area = {
        _area_key(row["empleado__area"]): row
        for row in lineas.values("empleado__area").annotate(empleados=Count("empleado_id", distinct=True), neto=Sum("neto_calculado"))
    }
    he_area = {
        _area_key(row["linea__empleado__area"]): row
        for row in he_conceptos.values("linea__empleado__area").annotate(horas=Sum("valor"), costo=Sum("importe"))
    }
    bajas_area = {_area_key(row["area"]): row["total"] for row in bajas.values("area").annotate(total=Count("id"))}
    plantilla_area = {
        _area_key(row["area"]): row["total"] for row in plantilla_qs.values("area").annotate(total=Sum("cantidad"))
    }
    vacantes_area = {_area_key(row["area"]): row["total"] for row in vacantes.values("area").annotate(total=Count("id"))}
    for data in (lineas_area, he_area, bajas_area, plantilla_area, vacantes_area):
        areas.update(data.keys())
    area_rows = []
    for area in sorted(areas):
        line_data = lineas_area.get(area, {})
        he_data = he_area.get(area, {})
        autorizada = plantilla_area.get(area, 0)
        actual = line_data.get("empleados", 0)
        area_rows.append(
            {
                "area": area,
                "plantilla_actual": actual,
                "plantilla_autorizada": autorizada,
                "brecha": int(autorizada or 0) - int(actual or 0),
                "nomina_neta": line_data.get("neto") or Decimal("0"),
                "he_horas": he_data.get("horas") or Decimal("0"),
                "he_costo": he_data.get("costo") or Decimal("0"),
                "bajas": bajas_area.get(area, 0),
                "vacantes": vacantes_area.get(area, 0),
            }
        )

    context = {
        "module_tabs": _module_tabs("dashboard", request.user),
        "can_manage_rrhh": can_manage_rrhh(request.user),
        "mes": mes,
        "months": range(1, 13),
        "empleados": Empleado.objects.all().order_by("nombre")[:1200],
        "contrato_choices": Empleado.CONTRATO_CHOICES,
        "motivo_choices": EmpleadoBaja.MOTIVO_CHOICES,
        "vacante_estado_choices": VacanteRRHH.ESTADO_CHOICES,
        "stats": {
            "nomina_neta": payroll.get("neto") or Decimal("0"),
            "nomina_bruta": payroll.get("bruto") or Decimal("0"),
            "he_horas": he_totales.get("horas") or Decimal("0"),
            "he_costo": he_totales.get("costo") or Decimal("0"),
            "he_registros": he_totales.get("registros") or 0,
            "plantilla_inicial": plantilla_inicial,
            "plantilla_final": plantilla_final,
            "plantilla_autorizada": plantilla_autorizada,
            "altas": altas,
            "bajas": bajas_count,
            "rotacion": rotacion,
            "permanencia_promedio": permanencia_promedio,
            "bajas_prueba": bajas_prueba,
            "vacantes": vacantes_count,
            "vacantes_cubiertas": vacantes_cubiertas,
            "vacantes_pendientes": vacantes_pendientes,
            "promedio_cobertura": promedio_cobertura,
        },
        "area_rows": area_rows,
        "bajas": bajas.select_related("empleado", "creado_por")[:20],
        "vacantes": vacantes.select_related("empleado_cubrio", "creado_por")[:20],
        "plantillas": plantilla_qs.select_related("actualizado_por")[:20],
        "periodos": periodos,
    }
    return render(request, "rrhh/indicadores.html", context)


@login_required
def vacantes_ch(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver vacantes de Capital Humano")

    if request.method == "POST":
        if not can_manage_rrhh(request.user):
            raise PermissionDenied("No tienes permisos para gestionar vacantes")
        vacante = VacanteRRHH.objects.create(
            area=_area_key(request.POST.get("area")),
            puesto=(request.POST.get("puesto") or "").strip().upper(),
            fecha_solicitada=_parse_date(request.POST.get("fecha_solicitada")) or timezone.localdate(),
            estado=(request.POST.get("estado") or VacanteRRHH.ESTADO_SOLICITADA).strip(),
            fecha_cubierta=_parse_date(request.POST.get("fecha_cubierta")),
            empleado_cubrio_id=(request.POST.get("empleado_cubrio") or None),
            motivo_no_cubierta=(request.POST.get("motivo_no_cubierta") or "").strip(),
            sugerencias=(request.POST.get("sugerencias") or "").strip(),
            creado_por=request.user,
        )
        messages.success(request, f"Vacante capturada: {vacante.area} · {vacante.puesto}.")
        return redirect("rrhh:rrhh_vacantes")

    estado = (request.GET.get("estado") or "").strip()
    area = (request.GET.get("area") or "").strip()
    vacantes = VacanteRRHH.objects.select_related("empleado_cubrio", "creado_por").order_by("-fecha_solicitada")
    if estado:
        vacantes = vacantes.filter(estado=estado)
    if area:
        vacantes = vacantes.filter(area__icontains=area)

    base_qs = VacanteRRHH.objects.all()
    stats = {
        "total": base_qs.count(),
        "abiertas": base_qs.exclude(estado__in=[VacanteRRHH.ESTADO_CUBIERTA, VacanteRRHH.ESTADO_CANCELADA]).count(),
        "cubiertas": base_qs.filter(estado=VacanteRRHH.ESTADO_CUBIERTA).count(),
        "pausadas": base_qs.filter(estado=VacanteRRHH.ESTADO_PAUSADA).count(),
    }
    return render(
        request,
        "rrhh/vacantes.html",
        {
            "module_tabs": _module_tabs("vacantes", request.user),
            "can_manage_rrhh": can_manage_rrhh(request.user),
            "vacantes": vacantes[:300],
            "empleados": Empleado.objects.filter(activo=True).order_by("nombre")[:1200],
            "estado_choices": VacanteRRHH.ESTADO_CHOICES,
            "stats": stats,
            "estado_actual": estado,
            "area_actual": area,
        },
    )


@login_required
def organizacion_ch(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver organización de Capital Humano")

    empleados = (
        Empleado.objects.select_related("jefe_directo", "usuario_erp")
        .filter(activo=True)
        .order_by("departamento_origen", "departamento", "jefe_directo__nombre", "nombre")
    )
    departamentos = (
        empleados.values("departamento")
        .annotate(
            total=Count("id"),
            bonos_ventas=Count("id", filter=Q(participa_bonos_ventas=True)),
            bonos_produccion=Count("id", filter=Q(participa_bonos_produccion=True)),
        )
        .order_by("departamento")
    )
    jefes_qs = (
        Empleado.objects.filter(colaboradores_directos__isnull=False)
        .filter(colaboradores_directos__activo=True)
        .distinct()
        .annotate(equipo_activo=Count("colaboradores_directos", filter=Q(colaboradores_directos__activo=True), distinct=True))
        .order_by("departamento", "nombre")
    )
    departamento_jefatura = (request.GET.get("departamento_jefatura") or "").strip().upper()
    if departamento_jefatura:
        jefes_qs = jefes_qs.filter(departamento=departamento_jefatura)
    jefes = list(jefes_qs)
    jefe_id = (request.GET.get("jefe") or "").strip()
    jefe_activo = None
    if jefe_id:
        jefe_activo = next((jefe for jefe in jefes if str(jefe.id) == jefe_id), None)
    if jefe_activo is None and jefes:
        jefe_activo = jefes[0]
    equipo_jefatura = (
        empleados.filter(jefe_directo=jefe_activo).order_by("departamento", "area", "nombre")
        if jefe_activo
        else Empleado.objects.none()
    )
    sin_jefe = empleados.filter(jefe_directo__isnull=True).exclude(jefatura_q())
    identity_map = _identity_map_context(limit=80)
    reglamento = ReglamentoLaboral.objects.filter(estado=ReglamentoLaboral.ESTADO_VIGENTE).first()
    return render(
        request,
        "rrhh/organizacion.html",
        {
            "module_tabs": _module_tabs("vacaciones", request.user),
            "empleados": empleados,
            "departamentos": departamentos,
            "jefes": jefes,
            "jefe_activo": jefe_activo,
            "jefe_id": str(jefe_activo.id) if jefe_activo else "",
            "equipo_jefatura": equipo_jefatura,
            "departamento_jefatura": departamento_jefatura,
            "departamento_choices": Empleado.DEP_CHOICES,
            "sin_jefe": sin_jefe,
            "total_activos": empleados.count(),
            "identity_map": identity_map,
            "reglamento": reglamento,
        },
    )


@login_required
def catalogos_ch(request):
    if not can_manage_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para administrar catálogos de Capital Humano")

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        if action == "funcion_operativa":
            codigo = (request.POST.get("codigo") or "").strip().upper()
            etiqueta = (request.POST.get("etiqueta") or "").strip()
            departamento_origen = (request.POST.get("departamento_origen") or "").strip().upper()
            departamento_actual = (request.POST.get("departamento_actual") or "").strip().upper()
            puesto_operativo = (request.POST.get("puesto_operativo") or "").strip().upper()
            nivel_organizacional = (request.POST.get("nivel_organizacional") or Empleado.NIVEL_COLABORADOR).strip().upper()
            activo = request.POST.get("activo") == "on"
            departamentos_validos = {value for value, _label in Empleado.DEP_CHOICES}
            niveles_validos = {value for value, _label in Empleado.NIVEL_ORGANIZACIONAL_CHOICES}
            if not etiqueta:
                messages.error(request, "Captura el nombre de la función operativa.")
                return redirect("rrhh:rrhh_catalogos")
            if departamento_origen and departamento_origen not in departamentos_validos:
                messages.error(request, "Selecciona un departamento origen oficial.")
                return redirect("rrhh:rrhh_catalogos")
            if departamento_actual and departamento_actual not in departamentos_validos:
                messages.error(request, "Selecciona una adscripción oficial.")
                return redirect("rrhh:rrhh_catalogos")
            if nivel_organizacional not in niveles_validos:
                messages.error(request, "Selecciona un nivel organizacional oficial.")
                return redirect("rrhh:rrhh_catalogos")
            codigo = _codigo_catalogo_desde_valor(codigo, etiqueta)
            if not codigo:
                messages.error(request, "No fue posible generar el código de catálogo.")
                return redirect("rrhh:rrhh_catalogos")
            funcion, created = CatalogoFuncionOperativa.objects.get_or_create(
                codigo=codigo,
                defaults={"sistema": False},
            )
            funcion.etiqueta = etiqueta
            funcion.departamento_origen = departamento_origen
            funcion.departamento_actual = departamento_actual
            funcion.puesto_operativo = puesto_operativo
            funcion.nivel_organizacional = nivel_organizacional
            funcion.activo = activo
            funcion.save()
            log_event(
                request.user,
                "CREATE" if created else "UPDATE",
                "rrhh.CatalogoFuncionOperativa",
                str(funcion.id),
                {
                    "codigo": funcion.codigo,
                    "etiqueta": funcion.etiqueta,
                    "departamento_origen": funcion.departamento_origen,
                    "departamento_actual": funcion.departamento_actual,
                    "puesto_operativo": funcion.puesto_operativo,
                    "source": "rrhh.catalogos",
                },
            )
            messages.success(request, "Función operativa guardada en el catálogo.")
            return redirect("rrhh:rrhh_catalogos")
        if action == "bono_esquema":
            nombre = (request.POST.get("nombre") or "").strip()
            departamento = (request.POST.get("departamento") or "").strip().upper()
            area = (request.POST.get("area") or "").strip().upper()
            descripcion = (request.POST.get("descripcion") or "").strip()
            activo = request.POST.get("activo") == "on"
            departamentos_validos = {value for value, _label in Empleado.DEP_CHOICES}
            if not nombre:
                messages.error(request, "Captura el nombre del esquema de bono.")
                return redirect("rrhh:rrhh_catalogos")
            if departamento and departamento not in departamentos_validos:
                messages.error(request, "Selecciona un departamento oficial.")
                return redirect("rrhh:rrhh_catalogos")
            esquema, created = BonoEsquema.objects.update_or_create(
                codigo=esquema_codigo(nombre),
                defaults={
                    "nombre": nombre,
                    "departamento": departamento,
                    "area": area,
                    "descripcion": descripcion,
                    "activo": activo,
                },
            )
            log_event(
                request.user,
                "CREATE" if created else "UPDATE",
                "rrhh.BonoEsquema",
                str(esquema.id),
                {
                    "codigo": esquema.codigo,
                    "nombre": esquema.nombre,
                    "departamento": esquema.departamento,
                    "area": esquema.area,
                    "source": "rrhh.catalogos",
                },
            )
            messages.success(request, "Esquema de bono guardado en el catálogo.")
            return redirect("rrhh:rrhh_catalogos")

    return render(
        request,
        "rrhh/catalogos.html",
        {
            "module_tabs": _module_tabs("catalogos", request.user),
            "can_manage_rrhh": True,
            "funciones_operativas": CatalogoFuncionOperativa.objects.order_by("departamento_actual", "etiqueta", "codigo"),
            "funciones_operativas_activas": funciones_operativas_catalogo(),
            "departamento_choices": Empleado.DEP_CHOICES,
            "nivel_choices": NIVEL_ORGANIZACIONAL_CHOICES,
            "puesto_operativo_choices": puesto_operativo_choices(),
            "bono_esquemas": BonoEsquema.objects.order_by("nombre", "codigo"),
        },
    )


@login_required
def dashboard_ch(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver Capital Humano")

    hoy = timezone.localdate()
    stats = {
        "he_pendientes": HoraExtra.objects.filter(estado=HoraExtra.ESTADO_PENDIENTE).count(),
        "permisos_pendientes": PermisoSalida.objects.filter(estado=PermisoSalida.ESTADO_SOLICITADO).count(),
        "asistencias_hoy": AsistenciaEmpleado.objects.filter(fecha=hoy).count(),
        "promedio_minutos_mes": AsistenciaEmpleado.objects.filter(
            fecha__year=hoy.year,
            fecha__month=hoy.month,
        ).aggregate(Avg("minutos_trabajados"))["minutos_trabajados__avg"]
        or 0,
        "empleados_activos": Empleado.objects.filter(activo=True).count(),
    }
    return render(
        request,
        "rrhh/dashboard_ch.html",
        {"module_tabs": _module_tabs("dashboard", request.user), "stats": stats, "can_manage_rrhh": can_manage_rrhh(request.user)},
    )


@login_required
def asistencias_view(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver asistencias")

    mes = (request.GET.get("mes") or timezone.localdate().strftime("%Y-%m")).strip()
    empleado_id = (request.GET.get("empleado") or "").strip()
    qs = AsistenciaEmpleado.objects.select_related("empleado", "turno", "sucursal").order_by("-fecha", "empleado__nombre")
    if mes:
        qs = qs.filter(fecha__startswith=mes)
    if empleado_id.isdigit():
        qs = qs.filter(empleado_id=int(empleado_id))
    context = {
        "module_tabs": _module_tabs("asistencias", request.user),
        "asistencias": qs[:500],
        "empleados": Empleado.objects.filter(activo=True).order_by("nombre")[:1000],
        "mes": mes,
        "empleado_id": empleado_id,
    }
    return render(request, "rrhh/asistencias.html", context)


@login_required
def importar_checador(request):
    if not can_manage_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para importar checador")

    if request.method == "POST":
        archivo = request.FILES.get("archivo")
        fecha_inicio = _parse_date(request.POST.get("fecha_inicio"))
        fecha_fin = _parse_date(request.POST.get("fecha_fin"))
        if not archivo:
            messages.error(request, "Selecciona un archivo Excel.")
        elif not fecha_inicio or not fecha_fin:
            messages.error(request, "Captura fecha inicio y fecha fin.")
        else:
            from .importers import importar_excel_hikconnect

            resultado = importar_excel_hikconnect(archivo, request.user, fecha_inicio, fecha_fin)
            messages.success(
                request,
                f"Importación completa: {resultado['procesados']} registros, {resultado['errores']} errores.",
            )
        return redirect("rrhh:rrhh_importar")

    hoy = timezone.localdate()
    ayer = hoy - timedelta(days=1)
    api_qs = AsistenciaEmpleado.objects.filter(
        fuente__in=[AsistenciaEmpleado.FUENTE_HIKCONNECT_API, AsistenciaEmpleado.FUENTE_POINT]
    )
    ultimas_api = (
        api_qs.select_related("empleado", "turno", "sucursal")
        .order_by("-creado_en")[:12]
    )
    ultima_api = api_qs.order_by("-creado_en").first()
    historial = ImportacionChecador.objects.order_by("-creado_en")[:10]
    return render(
        request,
        "rrhh/importar_checador.html",
        {
            "module_tabs": _module_tabs("checador", request.user),
            "historial": historial,
            "ultimas_api": ultimas_api,
            "ultima_api": ultima_api,
            "hoy": hoy,
            "ayer": ayer,
            "resumen_checador": {
                "asistencias_api": api_qs.count(),
                "asistencias_hoy": AsistenciaEmpleado.objects.filter(fecha=hoy).count(),
                "cargas_excel": ImportacionChecador.objects.count(),
            },
        },
    )


@login_required
def horas_extra_list(request):
    tiene_asignadas = (
        request.user.is_authenticated
        and HoraExtra.objects.filter(jefe_directo=request.user).exists()
    )
    if not can_view_rrhh(request.user) and not tiene_asignadas:
        raise PermissionDenied("No tienes permisos para ver horas extra")

    if request.method == "POST":
        he = get_object_or_404(HoraExtra.objects.select_related("empleado", "jefe_directo"), pk=request.POST.get("hora_extra_id"))
        if he.jefe_directo_id != request.user.id:
            raise PermissionDenied("Solo el jefe directo asignado puede autorizar horas extra.")
        action = (request.POST.get("action") or "").strip()
        if action == "autorizar":
            he.estado = HoraExtra.ESTADO_AUTORIZADO
            he.autorizado_por = request.user
            he.fecha_autorizacion_jefe = timezone.now()
            from .services import calcular_monto_hora_extra

            calcular_monto_hora_extra(he)
            he.save(update_fields=["estado", "autorizado_por", "fecha_autorizacion_jefe"])
            messages.success(request, f"Hora extra autorizada para {he.empleado.nombre}.")
        elif action == "rechazar":
            he.estado = HoraExtra.ESTADO_RECHAZADO
            he.autorizado_por = request.user
            he.fecha_autorizacion_jefe = timezone.now()
            he.save(update_fields=["estado", "autorizado_por", "fecha_autorizacion_jefe"])
            messages.success(request, f"Hora extra rechazada para {he.empleado.nombre}.")
        return redirect("rrhh:rrhh_he_list")

    horas_extra = HoraExtra.objects.select_related("empleado", "jefe_directo", "autorizado_por").order_by("-fecha", "empleado__nombre")
    if not can_view_rrhh(request.user):
        horas_extra = horas_extra.filter(jefe_directo=request.user)
    columnas = [
        ("pendiente", "Pendiente", horas_extra.filter(estado=HoraExtra.ESTADO_PENDIENTE)),
        ("autorizado", "Autorizado", horas_extra.filter(estado=HoraExtra.ESTADO_AUTORIZADO)),
        ("rechazado", "Rechazado", horas_extra.filter(estado=HoraExtra.ESTADO_RECHAZADO)),
        ("pagado", "Pagado", horas_extra.filter(estado=HoraExtra.ESTADO_PAGADO)),
    ]
    return render(
        request,
        "rrhh/horas_extra_list.html",
        {
            "module_tabs": _module_tabs("horas_extra", request.user),
            "columnas": columnas,
            "can_view_rrhh": can_view_rrhh(request.user),
            "user_id": request.user.id,
        },
    )


@login_required
def permisos_list(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver permisos")

    if request.method == "POST":
        permiso = get_object_or_404(PermisoSalida, pk=request.POST.get("permiso_id"))
        action = (request.POST.get("action") or "").strip()
        if action in {"autorizar_direccion", "rechazar_direccion"}:
            resolver_permiso_direccion(permiso, request.user, aprobar=action == "autorizar_direccion")
            estado = "autorizado" if action == "autorizar_direccion" else "rechazado"
            messages.success(request, f"Permiso {permiso.folio} {estado} por Dirección.")
        else:
            raise PermissionDenied("Capital Humano captura, consulta y archiva permisos; no los autoriza.")
        return redirect("rrhh:rrhh_permisos_list")

    permisos_qs = (
        PermisoSalida.objects.select_related(
            "empleado",
            "autorizado_por",
            "autorizado_jefe_por",
            "autorizado_direccion_por",
        )
        .order_by("-creado_en")
    )
    permisos = permisos_qs[:500]
    columnas = [
        (
            "jefe",
            "Pendiente jefe",
            permisos_qs.filter(
                estado=PermisoSalida.ESTADO_SOLICITADO,
                estado_jefe=PermisoSalida.ESTADO_JEFE_PENDIENTE,
                requiere_direccion=False,
            )[:120],
        ),
        (
            "direccion",
            "Pendiente Dirección",
            permisos_qs.filter(
                estado=PermisoSalida.ESTADO_SOLICITADO,
                requiere_direccion=True,
                estado_direccion=PermisoSalida.ESTADO_DIRECCION_PENDIENTE,
            )[:120],
        ),
        ("aprobado", "Autorizados / archivo", permisos_qs.filter(estado=PermisoSalida.ESTADO_APROBADO)[:120]),
        ("rechazado", "Rechazados", permisos_qs.filter(estado=PermisoSalida.ESTADO_RECHAZADO)[:120]),
    ]
    stats = {
        "total": permisos_qs.count(),
        "pendiente_jefe": permisos_qs.filter(
            estado=PermisoSalida.ESTADO_SOLICITADO,
            estado_jefe=PermisoSalida.ESTADO_JEFE_PENDIENTE,
            requiere_direccion=False,
        ).count(),
        "archivo_rrhh": permisos_qs.filter(estado=PermisoSalida.ESTADO_APROBADO).count(),
        "pendiente_direccion": permisos_qs.filter(
            estado=PermisoSalida.ESTADO_SOLICITADO,
            requiere_direccion=True,
            estado_direccion=PermisoSalida.ESTADO_DIRECCION_PENDIENTE,
        ).count(),
        "aprobados": permisos_qs.filter(estado=PermisoSalida.ESTADO_APROBADO).count(),
    }
    return render(
        request,
        "rrhh/permisos_list.html",
        {
            "module_tabs": _module_tabs("permisos", request.user),
            "permisos": permisos,
            "columnas": columnas,
            "stats": stats,
            "can_manage_rrhh": can_manage_rrhh(request.user),
            "can_authorize_direccion": can_authorize_direccion(request.user),
        },
    )


@login_required
def vacaciones_list(request):
    empleado_actual = empleado_de_usuario(request.user)
    equipo_qs = Empleado.objects.filter(activo=True, jefe_directo__usuario_erp=request.user)
    tiene_equipo = equipo_qs.exists()
    puede_ver_vacaciones = can_view_submodule(request.user, "rrhh", "vacaciones") or tiene_equipo or bool(empleado_actual)
    if not puede_ver_vacaciones:
        raise PermissionDenied("No tienes permisos para ver vacaciones")

    if can_view_submodule(request.user, "rrhh", "vacaciones"):
        empleados_qs = Empleado.objects.filter(activo=True).order_by("nombre")
    elif tiene_equipo:
        empleado_ids = list(equipo_qs.values_list("id", flat=True))
        if empleado_actual:
            empleado_ids.append(empleado_actual.id)
        empleados_qs = Empleado.objects.filter(id__in=empleado_ids, activo=True).order_by("nombre")
    else:
        empleados_qs = Empleado.objects.filter(pk=getattr(empleado_actual, "pk", None), activo=True).order_by("nombre")

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        try:
            if action == "crear":
                empleado = get_object_or_404(empleados_qs, pk=request.POST.get("empleado_id"))
                fecha_inicio = _parse_date(request.POST.get("fecha_inicio"))
                fecha_fin = _parse_date(request.POST.get("fecha_fin"))
                if not fecha_inicio or not fecha_fin:
                    raise ValidationError("Captura fecha inicial y fecha final.")
                if not (
                    can_view_submodule(request.user, "rrhh", "vacaciones")
                    or empleado == empleado_actual
                    or can_gestionar_vacaciones_jefe(request.user, empleado)
                ):
                    raise PermissionDenied("Solo puedes crear vacaciones propias o de tu equipo directo.")
                solicitud = crear_solicitud_vacaciones(
                    empleado=empleado,
                    fecha_inicio=fecha_inicio,
                    fecha_fin=fecha_fin,
                    motivo=(request.POST.get("motivo") or "").strip(),
                    actor=request.user,
                )
                messages.success(request, f"Solicitud {solicitud.folio} registrada y saldo reservado.")
            elif action in {"preautorizar_jefe", "rechazar_jefe"}:
                solicitud = get_object_or_404(SolicitudVacaciones, pk=request.POST.get("solicitud_id"))
                preautorizar_solicitud_vacaciones_jefe(
                    solicitud,
                    request.user,
                    aprobar=action == "preautorizar_jefe",
                )
                estado = "preautorizada" if action == "preautorizar_jefe" else "rechazada"
                messages.success(request, f"Vacaciones {solicitud.folio} {estado} por jefe directo.")
            elif action in {"aprobar_rrhh", "rechazar_rrhh"}:
                solicitud = get_object_or_404(SolicitudVacaciones, pk=request.POST.get("solicitud_id"))
                if action == "aprobar_rrhh":
                    aprobar_solicitud_vacaciones_rrhh(solicitud, request.user)
                    messages.success(request, f"Vacaciones {solicitud.folio} aprobadas por Capital Humano.")
                else:
                    rechazar_solicitud_vacaciones(solicitud, request.user)
                    messages.success(request, f"Vacaciones {solicitud.folio} rechazadas y reserva liberada.")
            else:
                raise PermissionDenied("Acción de vacaciones no válida.")
        except ValidationError as exc:
            messages.error(request, "; ".join(exc.messages) if hasattr(exc, "messages") else str(exc))
        return redirect("rrhh:rrhh_vacaciones_list")

    solicitudes_qs = (
        SolicitudVacaciones.objects.select_related(
            "empleado",
            "jefe_directo",
            "preautorizado_por",
            "aprobado_rrhh_por",
            "creado_por",
        )
        .order_by("-creado_en")
    )
    if not can_view_submodule(request.user, "rrhh", "vacaciones"):
        if empleado_actual:
            solicitudes_qs = solicitudes_qs.filter(Q(empleado=empleado_actual) | Q(jefe_directo=request.user))
        else:
            solicitudes_qs = solicitudes_qs.filter(jefe_directo=request.user)
    solicitudes = solicitudes_qs[:500]
    empleados = list(empleados_qs[:250])
    empleados_saldo = [
        {
            "empleado": empleado,
            "saldo": saldo_vacaciones_empleado(empleado),
        }
        for empleado in empleados[:80]
    ]
    columnas = [
        ("solicitada", "Solicitadas", solicitudes_qs.filter(estado=SolicitudVacaciones.ESTADO_SOLICITADA)[:120]),
        (
            "preautorizada",
            "Preautorizadas",
            solicitudes_qs.filter(estado=SolicitudVacaciones.ESTADO_PREAUTORIZADA)[:120],
        ),
        ("aprobada", "Aprobadas", solicitudes_qs.filter(estado=SolicitudVacaciones.ESTADO_APROBADA)[:120]),
        ("rechazada", "Rechazadas", solicitudes_qs.filter(estado=SolicitudVacaciones.ESTADO_RECHAZADA)[:120]),
    ]
    stats = {
        "total": solicitudes_qs.count(),
        "pendientes": solicitudes_qs.filter(
            estado__in=[SolicitudVacaciones.ESTADO_SOLICITADA, SolicitudVacaciones.ESTADO_PREAUTORIZADA]
        ).count(),
        "aprobadas": solicitudes_qs.filter(estado=SolicitudVacaciones.ESTADO_APROBADA).count(),
        "rechazadas": solicitudes_qs.filter(estado=SolicitudVacaciones.ESTADO_RECHAZADA).count(),
    }
    return render(
        request,
        "rrhh/vacaciones_list.html",
        {
            "module_tabs": _module_tabs("vacaciones", request.user),
            "empleados": empleados,
            "empleados_saldo": empleados_saldo,
            "solicitudes": solicitudes,
            "columnas": columnas,
            "stats": stats,
            "can_manage_rrhh": can_manage_rrhh(request.user),
            "user_id": request.user.id,
            "tiene_equipo_vacaciones": tiene_equipo,
        },
    )


@login_required
def reglamento_interno(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver el reglamento interno")

    reglamento = (
        ReglamentoLaboral.objects.prefetch_related("reglas")
        .filter(estado=ReglamentoLaboral.ESTADO_VIGENTE)
        .first()
    )
    reglas = ReglaLaboral.objects.none()
    if reglamento:
        reglas = reglamento.reglas.all()
    return render(
        request,
        "rrhh/reglamento_interno.html",
        {
            "module_tabs": _module_tabs("organizacion", request.user),
            "reglamento": reglamento,
            "reglas": reglas,
        },
    )


@login_required
def pwa_capital_humano(request):
    return render(request, "rrhh/pwa_capital_humano.html")


@login_required
def pwa_permisos(request):
    return render(request, "rrhh/pwa_capital_humano.html", {"initial_section": "permisos"})


@login_required
def pwa_vacaciones(request):
    return render(request, "rrhh/pwa_capital_humano.html", {"initial_section": "vacaciones"})


@login_required
def pwa_horas_extra(request):
    return render(request, "rrhh/pwa_capital_humano.html", {"initial_section": "horas"})
