import logging
import os
import calendar
from collections import defaultdict
from datetime import date
from datetime import timedelta
from decimal import Decimal

from django.conf import settings
from django.db import OperationalError, ProgrammingError
from django.db.models import Count, Q
from django.db.models import Sum
from django.core.paginator import Paginator
from django.core.exceptions import PermissionDenied
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render, redirect
from django.contrib import messages
from django.contrib.auth import authenticate, login, logout, get_user_model
from django.contrib.auth.models import Group
from django.urls import reverse
from django.utils import timezone
from django.utils.http import url_has_allowed_host_and_scheme
from core.access import (
    ROLE_ADMIN,
    ROLE_DG,
    ROLE_ORDER,
    can_manage_users,
    primary_role,
    can_manage_crm,
    can_view_audit,
    can_manage_compras,
    can_manage_inventario,
    can_manage_logistica,
    can_manage_rrhh,
    can_view_crm,
    can_view_compras,
    can_view_inventario,
    can_view_logistica,
    can_view_maestros,
    can_view_recetas,
    can_view_rrhh,
    can_view_reportes,
    is_branch_capture_only,
)
from maestros.models import Proveedor, PointPendingMatch
from maestros.models import CostoInsumo, Insumo
from maestros.utils.canonical_catalog import (
    canonical_insumo_by_id,
    canonicalized_active_insumos,
    enterprise_readiness_profile,
    latest_costo_canonico,
)
from compras.models import PresupuestoCompraPeriodo, SolicitudCompra, OrdenCompra, RecepcionCompra
from recetas.models import PlanProduccion, PlanProduccionItem, PronosticoVenta, Receta, LineaReceta, VentaHistorica, SolicitudVenta
from inventario.models import AlmacenSyncRun, ExistenciaInsumo, MovimientoInventario
from core.models import AuditLog, Departamento, Sucursal, UserProfile, sucursales_operativas
from core.audit import log_event
from activos.models import Activo, OrdenMantenimiento, PlanMantenimiento
from crm.models import PedidoCliente
from control.models import VentaPOS, MermaPOS
from rrhh.models import Empleado, NominaPeriodo
from logistica.models import RutaEntrega, EntregaRuta
from pos_bridge.models import PointDailySale, PointDailyBranchIndicator
from reportes.executive_panels import build_executive_bi_panels, _partial_month_amount_quantity

logger = logging.getLogger(__name__)
POINT_BRIDGE_SALES_SOURCE = "POINT_BRIDGE_SALES"
OFFICIAL_POINT_SOURCE = "/Report/PrintReportes?idreporte=3"
RECENT_POINT_SOURCE = "/Report/VentasCategorias"

LOCK_FIELDS = [
    ("lock_maestros", "Maestros"),
    ("lock_recetas", "Recetas"),
    ("lock_compras", "Compras"),
    ("lock_inventario", "Inventario"),
    ("lock_reportes", "Reportes"),
    ("lock_crm", "CRM"),
    ("lock_logistica", "Logística"),
    ("lock_rrhh", "RRHH"),
    ("lock_captura_piso", "Captura Piso"),
    ("lock_auditoria", "Bitácora/Integraciones"),
]

USER_BLOCKER_LABELS = {
    "SIN_ROL": "Sin rol",
    "SIN_DEPARTAMENTO": "Sin departamento",
    "CAPTURA_SIN_SUCURSAL": "Captura sin sucursal",
    "DEMASIADOS_CANDADOS": "Demasiados candados",
    "SIN_ACCESOS": "Sin accesos",
}


def csrf_failure(request: HttpRequest, reason: str = "", template_name: str | None = None) -> HttpResponse:
    fallback_url = reverse("login")
    referer = (request.META.get("HTTP_REFERER") or "").strip()
    allowed_hosts = {host for host in settings.ALLOWED_HOSTS if host}
    if request.get_host():
        allowed_hosts.add(request.get_host())
    if referer and url_has_allowed_host_and_scheme(
        referer,
        allowed_hosts=allowed_hosts,
        require_https=request.is_secure(),
    ):
        target_url = referer
    else:
        target_url = fallback_url
    messages.error(
        request,
        "La sesión del formulario expiró o quedó desactualizada. Recarga la página e inténtalo de nuevo.",
    )
    return redirect(target_url)


def _redirect_capture_module():
    return redirect(reverse("recetas:reabasto_cedis_captura"))


def _is_checked(payload, key: str) -> bool:
    return (payload.get(key) or "").strip().lower() in {"1", "true", "on", "yes"}


def _safe_int(raw: str) -> int | None:
    value = (raw or "").strip()
    if not value.isdigit():
        return None
    return int(value)


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def _get_or_create_profile(user):
    profile, _ = UserProfile.objects.get_or_create(user=user)
    return profile


def _assign_single_role(user, role_name: str) -> None:
    role_name = (role_name or "").strip().upper()
    if role_name not in ROLE_ORDER:
        user.groups.clear()
        return
    group, _ = Group.objects.get_or_create(name=role_name)
    user.groups.set([group])


def _user_access_scope(user, profile) -> dict:
    modules = [
        ("Maestros", can_view_maestros(user), False),
        ("Recetas", can_view_recetas(user), False),
        ("Compras", can_view_compras(user), can_manage_compras(user)),
        ("Inventario", can_view_inventario(user), can_manage_inventario(user)),
        ("Reportes", can_view_reportes(user), False),
        ("CRM", can_view_crm(user), can_manage_crm(user)),
        ("Logística", can_view_logistica(user), can_manage_logistica(user)),
        ("RRHH", can_view_rrhh(user), can_manage_rrhh(user)),
        ("Bitácora", can_view_audit(user), False),
    ]
    visible = [label for label, can_view, _can_manage in modules if can_view]
    manageable = [label for label, _can_view, can_manage in modules if can_manage]
    blocked = [label for key, label in LOCK_FIELDS if profile and bool(getattr(profile, key, False))]
    if not user.is_active:
        readiness_label = "Inactivo"
        readiness_tone = "danger"
    elif profile and profile.modo_captura_sucursal:
        readiness_label = "Captura sucursal"
        readiness_tone = "warning"
    elif blocked:
        readiness_label = "Operativo con candados"
        readiness_tone = "warning"
    else:
        readiness_label = "Operativo"
        readiness_tone = "success"
    return {
        "visible_modules": visible,
        "manageable_modules": manageable,
        "blocked_modules": blocked,
        "readiness_label": readiness_label,
        "readiness_tone": readiness_tone,
    }


def _role_capability_matrix() -> list[dict]:
    matrix = {
        "DG": {"Maestros": "Ver", "Recetas": "Ver", "Compras": "Ver", "Inventario": "Ver", "Reportes": "Ver", "CRM": "Ver", "Logística": "Ver", "RRHH": "Ver", "Usuarios": "Gestiona"},
        "ADMIN": {"Maestros": "Ver", "Recetas": "Ver", "Compras": "Gestiona", "Inventario": "Gestiona", "Reportes": "Ver", "CRM": "Gestiona", "Logística": "Gestiona", "RRHH": "Gestiona", "Usuarios": "Gestiona"},
        "COMPRAS": {"Maestros": "Ver", "Recetas": "Ver", "Compras": "Gestiona", "Inventario": "Ver", "Reportes": "Ver", "CRM": "-", "Logística": "-", "RRHH": "-", "Usuarios": "-"},
        "ALMACEN": {"Maestros": "Ver", "Recetas": "Ver", "Compras": "Ver", "Inventario": "Gestiona", "Reportes": "Ver", "CRM": "-", "Logística": "-", "RRHH": "-", "Usuarios": "-"},
        "PRODUCCION": {"Maestros": "-", "Recetas": "Ver", "Compras": "-", "Inventario": "-", "Reportes": "Ver", "CRM": "-", "Logística": "-", "RRHH": "-", "Usuarios": "-"},
        "VENTAS": {"Maestros": "-", "Recetas": "Ver", "Compras": "-", "Inventario": "-", "Reportes": "Ver", "CRM": "Gestiona", "Logística": "-", "RRHH": "-", "Usuarios": "-"},
        "LOGISTICA": {"Maestros": "-", "Recetas": "-", "Compras": "-", "Inventario": "-", "Reportes": "-", "CRM": "-", "Logística": "Gestiona", "RRHH": "-", "Usuarios": "-"},
        "RRHH": {"Maestros": "-", "Recetas": "-", "Compras": "-", "Inventario": "-", "Reportes": "Ver", "CRM": "-", "Logística": "-", "RRHH": "Gestiona", "Usuarios": "-"},
        "LECTURA": {"Maestros": "Ver", "Recetas": "Ver", "Compras": "Ver", "Inventario": "Ver", "Reportes": "Ver", "CRM": "Ver", "Logística": "Ver", "RRHH": "Ver", "Usuarios": "-"},
    }
    modules = ["Maestros", "Recetas", "Compras", "Inventario", "Reportes", "CRM", "Logística", "RRHH", "Usuarios"]
    return [{"role": role, "modules": [(module, matrix.get(role, {}).get(module, "-")) for module in modules]} for role in ROLE_ORDER]


def _role_operational_requirements() -> list[dict]:
    requirement_map = {
        "DG": ["Sin requisito obligatorio de sucursal", "Puede operar transversalmente"],
        "ADMIN": ["Departamento recomendado", "Gestiona accesos y módulos críticos"],
        "COMPRAS": ["Departamento obligatorio", "No usar modo captura sucursal"],
        "ALMACEN": ["Departamento obligatorio", "Sucursal opcional según operación"],
        "PRODUCCION": ["Departamento obligatorio", "No usar modo captura sucursal"],
        "VENTAS": ["Departamento obligatorio", "Sucursal recomendada si opera tienda"],
        "LOGISTICA": ["Departamento obligatorio", "Sucursal opcional según ruta/base"],
        "RRHH": ["Departamento obligatorio", "No usar modo captura sucursal"],
        "LECTURA": ["Departamento opcional", "Solo acceso consulta"],
    }
    return [{"role": role, "requirements": requirement_map.get(role, [])} for role in ROLE_ORDER]


def _user_enterprise_profile(user, profile, row: dict) -> dict:
    blockers: list[dict] = []
    role = row.get("role") or ""
    locks_count = row.get("locks_count") or 0
    visible_modules = row.get("visible_modules") or []
    manageable_modules = row.get("manageable_modules") or []
    blocked_modules = row.get("blocked_modules") or []
    is_active = bool(row.get("is_active"))
    modo_captura = bool(row.get("modo_captura_sucursal"))
    departamento_id = row.get("departamento_id")
    sucursal_id = row.get("sucursal_id")

    if is_active and not role:
        blockers.append(
            {
                "code": "SIN_ROL",
                "label": USER_BLOCKER_LABELS["SIN_ROL"],
                "detail": "El usuario activo no tiene rol principal asignado.",
                "action": "Asignar rol",
            }
        )
    if is_active and role not in {"DG", "LECTURA"} and not departamento_id:
        blockers.append(
            {
                "code": "SIN_DEPARTAMENTO",
                "label": USER_BLOCKER_LABELS["SIN_DEPARTAMENTO"],
                "detail": "El usuario operativo no tiene departamento asignado.",
                "action": "Asignar departamento",
            }
        )
    if is_active and modo_captura and not sucursal_id:
        blockers.append(
            {
                "code": "CAPTURA_SIN_SUCURSAL",
                "label": USER_BLOCKER_LABELS["CAPTURA_SIN_SUCURSAL"],
                "detail": "El modo captura sucursal exige una sucursal activa.",
                "action": "Asignar sucursal",
            }
        )
    if is_active and locks_count >= 3:
        blockers.append(
            {
                "code": "DEMASIADOS_CANDADOS",
                "label": USER_BLOCKER_LABELS["DEMASIADOS_CANDADOS"],
                "detail": f"El usuario tiene {locks_count} módulos bloqueados.",
                "action": "Revisar candados",
            }
        )
    if is_active and not visible_modules and not manageable_modules:
        blockers.append(
            {
                "code": "SIN_ACCESOS",
                "label": USER_BLOCKER_LABELS["SIN_ACCESOS"],
                "detail": "El usuario activo no tiene módulos visibles.",
                "action": "Revisar rol/candados",
            }
        )

    blocker_codes = [item["code"] for item in blockers]
    if not is_active:
        status_label = "Inactivo"
        status_tone = "danger"
        primary_blocker = {
            "code": "INACTIVO",
            "label": "Inactivo",
            "detail": "El usuario no puede operar mientras esté desactivado.",
            "action": "Activar usuario",
        }
    elif blockers:
        primary_blocker = blockers[0]
        status_label = "Bloqueado"
        status_tone = "warning"
    elif blocked_modules:
        primary_blocker = {
            "code": "CANDADOS_PARCIALES",
            "label": "Candados parciales",
            "detail": f"El usuario tiene {len(blocked_modules)} módulos bloqueados.",
            "action": "Revisar candados",
        }
        status_label = "Operativo con restricciones"
        status_tone = "warning"
    else:
        primary_blocker = {
            "code": "OPERATIVO",
            "label": "Lista para operar",
            "detail": "El usuario está listo para operar según su rol y alcance.",
            "action": "Sin acción",
        }
        status_label = "Lista para operar"
        status_tone = "success"

    return {
        "blockers": blockers,
        "blocker_codes": blocker_codes,
        "blockers_count": len(blockers),
        "primary_blocker": primary_blocker,
        "status_label": status_label,
        "status_tone": status_tone,
    }


def _operational_coverage_summary(
    user_rows: list[dict], sucursales: list[object], departamentos: list[object]
) -> dict[str, object]:
    active_rows = [row for row in user_rows if row["is_active"]]
    sucursal_rows = []
    for sucursal in sucursales:
        assigned = [row for row in active_rows if row["sucursal_id"] == sucursal.id]
        capture_users = [row for row in assigned if row["modo_captura_sucursal"]]
        sucursal_rows.append(
            {
                "id": sucursal.id,
                "label": sucursal.nombre,
                "assigned_count": len(assigned),
                "capture_count": len(capture_users),
                "status_label": "Cubierta" if assigned and capture_users else "Gap operativo",
                "status_tone": "success" if assigned and capture_users else "warning",
                "detail": (
                    "Tiene responsable y captura de cierre."
                    if assigned and capture_users
                    else "Falta responsable activo o usuario de captura."
                ),
                "query": f"?coverage=sucursal&scope_id={sucursal.id}",
            }
        )

    departamento_rows = []
    for departamento in departamentos:
        assigned = [row for row in active_rows if row["departamento_id"] == departamento.id]
        managers = [row for row in assigned if row["role"] in {ROLE_ADMIN, ROLE_DG}]
        departamento_rows.append(
            {
                "id": departamento.id,
                "label": departamento.nombre,
                "assigned_count": len(assigned),
                "manager_count": len(managers),
                "status_label": "Cubierto" if assigned else "Sin responsable",
                "status_tone": "success" if assigned else "warning",
                "detail": (
                    "Área con responsables activos."
                    if assigned
                    else "Área sin responsable activo."
                ),
                "query": f"?coverage=departamento&scope_id={departamento.id}",
            }
        )

    return {
        "sucursal_rows": sucursal_rows,
        "departamento_rows": departamento_rows,
        "sucursales_ok": sum(1 for row in sucursal_rows if row["status_tone"] == "success"),
        "sucursales_gap": sum(1 for row in sucursal_rows if row["status_tone"] != "success"),
        "departamentos_ok": sum(1 for row in departamento_rows if row["status_tone"] == "success"),
        "departamentos_gap": sum(1 for row in departamento_rows if row["status_tone"] != "success"),
    }


def _users_erp_governance_rows(rows: list[dict]) -> list[dict]:
    governance_rows: list[dict] = []
    for row in rows:
        open_count = int(row.get("open_count", 0) or 0)
        closed_count = int(row.get("closed_count", 0) or 0)
        total = max(open_count + closed_count, 1)
        governance_rows.append(
            {
                "front": row.get("title", "Frente"),
                "owner": row.get("owner", "Operación ERP"),
                "blockers": open_count,
                "completion": int(round((closed_count / total) * 100)) if total else 0,
                "detail": row.get("detail", ""),
                "next_step": row.get("cta", "Abrir frente"),
                "url": row.get("url", reverse("users_access")),
                "cta": "Abrir",
            }
        )
    return governance_rows


def _users_command_center(
    enterprise_ready_summary: dict[str, int],
    maturity_summary: dict[str, object],
) -> dict[str, object]:
    blocked = int(enterprise_ready_summary.get("bloqueados", 0) or 0)
    restrictions = int(enterprise_ready_summary.get("restricciones", 0) or 0)
    blockers = blocked + restrictions
    if blocked:
        status = "Crítico"
        tone = "danger"
        owner = "RH / Administración"
    elif restrictions or int(maturity_summary.get("attention_steps", 0) or 0):
        status = "Seguimiento"
        tone = "warning"
        owner = "Administración / Seguridad"
    else:
        status = "Controlado"
        tone = "success"
        owner = "Líderes de área"
    return {
        "owner": owner,
        "status": status,
        "tone": tone,
        "blockers": blockers,
        "next_step": maturity_summary.get("next_priority_detail", ""),
        "url": maturity_summary.get("next_priority_url", reverse("users_access")),
        "cta": maturity_summary.get("next_priority_cta", "Abrir usuarios"),
    }


def _dashboard_command_center(
    *,
    erp_governance_rows: list[dict],
    erp_maturity_summary: dict[str, object],
) -> dict[str, object]:
    blockers = sum(int(row.get("blockers", 0) or 0) for row in erp_governance_rows)
    pending_modules = int(erp_maturity_summary.get("pending_modules", 0) or 0)
    if blockers:
        status = "Con bloqueos"
        tone = "danger"
    elif pending_modules:
        status = "En seguimiento"
        tone = "warning"
    else:
        status = "Estable"
        tone = "success"
    return {
        "owner": "DG / Gobierno ERP",
        "status": status,
        "tone": tone,
        "blockers": blockers,
        "next_step": erp_maturity_summary.get("next_priority_detail", ""),
        "url": erp_maturity_summary.get("next_priority_url", reverse("dashboard")),
        "cta": erp_maturity_summary.get("next_priority_cta", "Abrir cockpit"),
    }


def _dashboard_critical_path_rows(
    erp_trunk_chain_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    severity_order = {"danger": 0, "warning": 1, "success": 2}
    ranked_rows = sorted(
        erp_trunk_chain_rows,
        key=lambda row: (
            0 if row.get("upstream_blocking") else 1,
            severity_order.get(str(row.get("tone") or "warning"), 9),
            -int(row.get("count") or 0),
            int(row.get("progress_pct") or 0),
        ),
    )
    critical_rows: list[dict[str, object]] = []
    for index, row in enumerate(ranked_rows[:4], start=1):
        blockers = int(row.get("count") or 0)
        critical_rows.append(
            {
                "rank": f"R{index}",
                "module": row.get("title", row.get("module_key", "ERP")),
                "owner": row.get("owner", "Operación ERP"),
                "status": row.get("status", "En seguimiento"),
                "tone": row.get("tone", "warning"),
                "blockers": blockers,
                "progress_pct": int(row.get("progress_pct") or 0),
                "depends_on": row.get("depends_on", "Sin dependencia previa"),
                "detail": row.get("dependency_detail") or row.get("next_step", ""),
                "next_step": row.get("next_step", "Continuar flujo"),
                "url": row.get("url", reverse("dashboard")),
                "cta": row.get("cta", "Abrir"),
                "dependency_status": row.get("dependency_status", "Listo para avanzar"),
            }
        )
    return critical_rows


def _dashboard_executive_radar_rows(
    stage_progress_rows: list[dict[str, object]],
    erp_trunk_chain_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    dependency_map = {str(row.get("title") or ""): row for row in erp_trunk_chain_rows}
    radar_rows: list[dict[str, object]] = []
    for row in stage_progress_rows:
        module = str(row.get("module") or "")
        trunk_match = next(
            (
                item
                for item in erp_trunk_chain_rows
                if item.get("module_key") == module or item.get("title") == module or module in str(item.get("title") or "")
            ),
            None,
        )
        blockers = int(row.get("total_count", 0) or 0) - int(row.get("closed_count", 0) or 0)
        radar_rows.append(
            {
                "module": module,
                "owner": row.get("owner", "Operación ERP"),
                "status": row.get("stage", "En seguimiento"),
                "tone": row.get("tone", "warning"),
                "blockers": blockers,
                "progress_pct": int(row.get("progress_pct") or 0),
                "dominant_blocker": row.get("detail", "Sin lectura ejecutiva"),
                "depends_on": trunk_match.get("depends_on", "Sin dependencia previa") if trunk_match else "Sin dependencia previa",
                "dependency_status": trunk_match.get("dependency_status", "Listo para avanzar") if trunk_match else "Listo para avanzar",
                "next_step": row.get("next_step", "Continuar flujo"),
                "url": row.get("url", reverse("dashboard")),
                "cta": row.get("cta", "Abrir"),
            }
        )
    severity_order = {"danger": 0, "warning": 1, "success": 2}
    return sorted(
        radar_rows,
        key=lambda item: (
            severity_order.get(str(item.get("tone") or "warning"), 9),
            -int(item.get("blockers") or 0),
            int(item.get("progress_pct") or 0),
            str(item.get("module") or ""),
        ),
    )


def _dashboard_trunk_closure_cards(
    erp_trunk_chain_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    cards: list[dict[str, object]] = []
    for row in erp_trunk_chain_rows:
        cards.append(
            {
                "step": row.get("step", "--"),
                "title": row.get("title", "Tramo ERP"),
                "owner": row.get("owner", "Operación ERP"),
                "status": row.get("status", "En seguimiento"),
                "tone": row.get("tone", "warning"),
                "blockers": int(row.get("count") or 0),
                "completion": int(row.get("progress_pct") or 0),
                "depends_on": row.get("depends_on", "Sin dependencia previa"),
                "next_step": row.get("next_step", "Continuar flujo"),
                "url": row.get("url", reverse("dashboard")),
                "cta": row.get("cta", "Abrir"),
            }
        )
    return cards


def _users_trunk_closure_cards(
    users_trunk_chain_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    cards: list[dict[str, object]] = []
    for row in users_trunk_chain_rows:
        cards.append(
            {
                "step": row.get("step", "--"),
                "title": row.get("title", "Tramo ERP"),
                "owner": row.get("owner", "Operación ERP"),
                "status": row.get("status", "En seguimiento"),
                "tone": row.get("tone", "warning"),
                "blockers": int(row.get("count") or 0),
                "completion": int(row.get("completion") or 0),
                "depends_on": row.get("depends_on", "Sin dependencia previa"),
                "next_step": row.get("next_step", "Continuar flujo"),
                "url": row.get("url", reverse("users_access")),
                "cta": row.get("cta", "Abrir"),
            }
        )
    return cards


def _users_critical_path_rows(users_trunk_chain_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    severity_order = {"danger": 0, "warning": 1, "success": 2}
    ranked_rows = sorted(
        users_trunk_chain_rows,
        key=lambda row: (
            0 if str(row.get("dependency_status", "")).startswith("Condicionado") else 1,
            severity_order.get(str(row.get("tone") or "warning"), 9),
            -int(row.get("count") or 0),
            int(row.get("completion") or 0),
        ),
    )
    critical_rows: list[dict[str, object]] = []
    for index, row in enumerate(ranked_rows[:4], start=1):
        blockers = 0 if row.get("tone") == "success" else int(row.get("count") or 0)
        critical_rows.append(
            {
                "rank": f"R{index}",
                "module": row.get("title", "Usuarios"),
                "owner": row.get("owner", "Administración / Seguridad"),
                "status": row.get("status", "En seguimiento"),
                "tone": row.get("tone", "warning"),
                "blockers": blockers,
                "progress_pct": int(row.get("completion") or 0),
                "depends_on": row.get("depends_on", "Sin dependencia previa"),
                "detail": row.get("detail", ""),
                "next_step": row.get("next_step", "Continuar flujo"),
                "url": row.get("url", reverse("users_access")),
                "cta": "Abrir",
                "dependency_status": row.get("dependency_status", "Listo para avanzar"),
            }
        )
    return critical_rows


def _users_executive_radar_rows(
    users_stage_rows: list[dict[str, object]],
    users_trunk_chain_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, stage in enumerate(users_stage_rows, start=1):
        trunk = users_trunk_chain_rows[min(index - 1, len(users_trunk_chain_rows) - 1)] if users_trunk_chain_rows else {}
        open_count = int(stage.get("open_count", 0) or 0)
        completion = int(stage.get("completion", 0) or 0)
        if open_count <= 0 and completion >= 90:
            tone = "success"
            status = "Controlado"
            dominant_blocker = "Sin bloqueo activo"
        elif completion >= 50:
            tone = "warning"
            status = "En seguimiento"
            dominant_blocker = stage.get("detail", "") or "Brecha operativa en seguimiento"
        else:
            tone = "danger"
            status = "Con bloqueo"
            dominant_blocker = stage.get("detail", "") or "Bloqueo operativo abierto"
        rows.append(
            {
                "phase": stage.get("title", f"Fase {index}"),
                "owner": stage.get("owner", "Administración / Seguridad"),
                "status": status,
                "tone": tone,
                "blockers": open_count,
                "progress_pct": completion,
                "dominant_blocker": dominant_blocker,
                "depends_on": trunk.get("title", "Origen del acceso"),
                "dependency_status": trunk.get("dependency_status", "Sin dependencia registrada"),
                "next_step": stage.get("next_step", "Abrir fase"),
                "url": stage.get("url", reverse("users_access")),
                "cta": "Abrir",
            }
        )
    return rows


def _budget_period_bounds(periodo_tipo: str, periodo_mes: str) -> tuple[date, date]:
    year, month = periodo_mes.split("-")
    y = int(year)
    m = int(month)
    start = date(y, m, 1)
    end = date(y, m, calendar.monthrange(y, m)[1])
    if periodo_tipo == "q1":
        end = date(y, m, 15)
    elif periodo_tipo == "q2":
        start = date(y, m, 16)
    return start, end


def _compute_budget_semaforo(periodo_tipo: str, periodo_mes: str) -> dict:
    start, end = _budget_period_bounds(periodo_tipo, periodo_mes)
    objetivo_obj = PresupuestoCompraPeriodo.objects.filter(
        periodo_tipo=periodo_tipo,
        periodo_mes=periodo_mes,
    ).first()
    objetivo = objetivo_obj.monto_objetivo if objetivo_obj else Decimal("0")

    solicitudes = list(
        SolicitudCompra.objects.filter(fecha_requerida__range=(start, end)).only("insumo_id", "cantidad")
    )
    total_qty_by_canonical: dict[int, Decimal] = {}
    canonical_catalog_rows = {
        row["canonical"].id: row for row in canonicalized_active_insumos(limit=5000)
    }
    for solicitud in solicitudes:
        canonical = canonical_insumo_by_id(solicitud.insumo_id)
        if not canonical:
            continue
        total_qty_by_canonical[canonical.id] = total_qty_by_canonical.get(canonical.id, Decimal("0")) + (
            solicitud.cantidad or Decimal("0")
        )

    latest_cost_by_insumo: dict[int, Decimal] = {}
    for canonical_id in total_qty_by_canonical.keys():
        latest = latest_costo_canonico(insumo_id=canonical_id)
        if latest is not None:
            latest_cost_by_insumo[canonical_id] = latest

    estimado = sum(
        qty * latest_cost_by_insumo.get(canonical_id, Decimal("0"))
        for canonical_id, qty in total_qty_by_canonical.items()
    )
    ejecutado = (
        OrdenCompra.objects.exclude(estatus=OrdenCompra.STATUS_BORRADOR)
        .filter(fecha_emision__range=(start, end))
        .aggregate(total=Sum("monto_estimado"))
        .get("total")
        or Decimal("0")
    )

    base = max(estimado, ejecutado)
    ratio_pct = ((base * Decimal("100")) / objetivo) if objetivo > 0 else None
    if objetivo <= 0:
        estado = "SIN_OBJETIVO"
        badge = "bg-warning"
        estado_label = "Sin objetivo"
    elif ratio_pct <= Decimal("90"):
        estado = "VERDE"
        badge = "bg-success"
        estado_label = "Verde"
    elif ratio_pct <= Decimal("100"):
        estado = "AMARILLO"
        badge = "bg-warning"
        estado_label = "Amarillo"
    else:
        estado = "ROJO"
        badge = "bg-danger"
        estado_label = "Rojo"

    if periodo_tipo == "mes":
        periodo_label = f"Mensual {periodo_mes}"
    elif periodo_tipo == "q1":
        periodo_label = f"1ra quincena {periodo_mes}"
    else:
        periodo_label = f"2da quincena {periodo_mes}"

    return {
        "periodo_tipo": periodo_tipo,
        "periodo_mes": periodo_mes,
        "periodo_label": periodo_label,
        "objetivo": objetivo,
        "estimado": estimado,
        "ejecutado": ejecutado,
        "ratio_pct": ratio_pct,
        "estado": estado,
        "estado_label": estado_label,
        "estado_badge": badge,
        "sobre_objetivo_estimado": bool(objetivo > 0 and estimado > objetivo),
        "sobre_objetivo_ejecutado": bool(objetivo > 0 and ejecutado > objetivo),
    }


def _log_budget_alert_once(alert_data: dict, kind: str) -> None:
    if kind not in {"ESTIMADO", "EJECUTADO"}:
        return
    object_id = f"{alert_data['periodo_tipo']}:{alert_data['periodo_mes']}:{kind}"
    today = timezone.localdate()
    exists = AuditLog.objects.filter(
        action="ALERT",
        model="compras.PresupuestoCompraPeriodo",
        object_id=object_id,
        timestamp__date=today,
    ).exists()
    if exists:
        return

    valor = alert_data["estimado"] if kind == "ESTIMADO" else alert_data["ejecutado"]
    log_event(
        None,
        "ALERT",
        "compras.PresupuestoCompraPeriodo",
        object_id,
        {
            "periodo_tipo": alert_data["periodo_tipo"],
            "periodo_mes": alert_data["periodo_mes"],
            "periodo_label": alert_data["periodo_label"],
            "kind": kind,
            "objetivo": str(alert_data["objetivo"]),
            "valor": str(valor),
            "ratio_pct": str(alert_data["ratio_pct"] or Decimal("0")),
            "mensaje": f"{kind} supera objetivo del periodo",
        },
    )


def _build_canonical_inventory_dashboard_metrics(limit: int = 5000) -> dict:
    canonical_rows = canonicalized_active_insumos(limit=limit)
    member_ids = [member_id for row in canonical_rows for member_id in row["member_ids"]]
    existencias_map = {ex.insumo_id: ex for ex in ExistenciaInsumo.objects.filter(insumo_id__in=member_ids)}

    inventario_total_count = 0
    stock_min_config_count = 0
    stock_max_config_count = 0
    inv_prom_config_count = 0
    punto_reorden_config_count = 0
    stock_bajo_min_count = 0
    stock_sobre_max_count = 0
    alertas_count = 0
    criticos_count = 0
    bajo_reorden_count = 0
    lead_time_risk_count = 0
    total_dias_llegada = Decimal("0")
    total_consumo_diario = Decimal("0")
    cobertura_total = Decimal("0")
    cobertura_items = 0

    for row in canonical_rows:
        canonical_id = row["canonical"].id
        member_existencias = [existencias_map[member_id] for member_id in row["member_ids"] if member_id in existencias_map]
        if not member_existencias:
            continue
        inventario_total_count += 1
        canonical_existencia = existencias_map.get(canonical_id)
        base_existencia = canonical_existencia or member_existencias[0]

        stock_actual = sum((Decimal(str(item.stock_actual or 0)) for item in member_existencias), Decimal("0"))
        stock_minimo = Decimal(str(base_existencia.stock_minimo or 0))
        stock_maximo = Decimal(str(base_existencia.stock_maximo or 0))
        inventario_promedio = Decimal(str(base_existencia.inventario_promedio or 0))
        punto_reorden = Decimal(str(base_existencia.punto_reorden or 0))
        dias_llegada = Decimal(str(base_existencia.dias_llegada_pedido or 0))
        consumo_diario = Decimal(str(base_existencia.consumo_diario_promedio or 0))

        if stock_minimo != 0:
            stock_min_config_count += 1
        if stock_maximo != 0:
            stock_max_config_count += 1
        if inventario_promedio != 0:
            inv_prom_config_count += 1
        if punto_reorden != 0:
            punto_reorden_config_count += 1
        if stock_minimo > 0 and stock_actual < stock_minimo:
            stock_bajo_min_count += 1
        if stock_maximo > 0 and stock_actual > stock_maximo:
            stock_sobre_max_count += 1
        if stock_actual < punto_reorden:
            alertas_count += 1
        if stock_actual <= 0:
            criticos_count += 1
        elif stock_actual < punto_reorden:
            bajo_reorden_count += 1

        total_dias_llegada += dias_llegada
        total_consumo_diario += consumo_diario

        if consumo_diario > 0:
            cobertura_dias = stock_actual / consumo_diario
            cobertura_total += cobertura_dias
            cobertura_items += 1
            if dias_llegada > 0 and cobertura_dias < dias_llegada:
                lead_time_risk_count += 1

    promedio_base = Decimal(str(inventario_total_count or 0))
    return {
        "insumos_count": len(canonical_rows),
        "inventario_total_count": inventario_total_count,
        "stock_min_config_count": stock_min_config_count,
        "stock_max_config_count": stock_max_config_count,
        "inv_prom_config_count": inv_prom_config_count,
        "punto_reorden_config_count": punto_reorden_config_count,
        "stock_bajo_min_count": stock_bajo_min_count,
        "stock_sobre_max_count": stock_sobre_max_count,
        "alertas_count": alertas_count,
        "criticos_count": criticos_count,
        "bajo_reorden_count": bajo_reorden_count,
        "lead_time_risk_count": lead_time_risk_count,
        "avg_dias_llegada": (total_dias_llegada / promedio_base) if promedio_base else Decimal("0"),
        "avg_consumo_diario": (total_consumo_diario / promedio_base) if promedio_base else Decimal("0"),
        "total_consumo_diario": total_consumo_diario,
        "cobertura_promedio_dias": (cobertura_total / Decimal(str(cobertura_items))) if cobertura_items else Decimal("0"),
    }


def _build_dashboard_master_governance(limit: int = 5000) -> list[dict]:
    canonical_rows = canonicalized_active_insumos(limit=limit)
    member_ids = [member_id for row in canonical_rows for member_id in row["member_ids"]]
    final_usage_member_ids = set(
        LineaReceta.objects.filter(
            insumo_id__in=member_ids,
            receta__tipo=Receta.TIPO_PRODUCTO_FINAL,
        )
        .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        .values_list("insumo_id", flat=True)
    )

    ready = 0
    incomplete = 0
    blocking_final = 0
    gap_totals = {"unidad": 0, "proveedor": 0, "categoria": 0, "codigo_point": 0}

    for row in canonical_rows:
        canonical = row["canonical"]
        profile = enterprise_readiness_profile(canonical)
        if profile["readiness_label"] == "Lista para operar":
            ready += 1
        elif profile["readiness_label"] == "Incompleto":
            incomplete += 1
            if any(member_id in final_usage_member_ids for member_id in row["member_ids"]):
                blocking_final += 1
        missing = set(profile["missing"])
        if "unidad base" in missing:
            gap_totals["unidad"] += 1
        if "proveedor principal" in missing:
            gap_totals["proveedor"] += 1
        if "categoría" in missing:
            gap_totals["categoria"] += 1
        if "código comercial" in missing or "código externo" in missing:
            gap_totals["codigo_point"] += 1

    maestros_url = reverse("maestros:insumo_list")
    return [
        {
            "label": "Artículos listos ERP",
            "value": ready,
            "tone": "success",
            "url": f"{maestros_url}?enterprise_status=listos",
        },
        {
            "label": "Artículos incompletos",
            "value": incomplete,
            "tone": "warning" if incomplete else "success",
            "url": f"{maestros_url}?enterprise_status=incompletos",
        },
        {
            "label": "Bloquean producto final",
            "value": blocking_final,
            "tone": "danger" if blocking_final else "success",
            "url": f"{maestros_url}?usage_scope=recipes&recipe_scope=finales&enterprise_status=incompletos&impact_scope=critical",
        },
        {
            "label": "Sin código comercial",
            "value": gap_totals["codigo_point"],
            "tone": "warning" if gap_totals["codigo_point"] else "success",
            "url": f"{maestros_url}?enterprise_status=incompletos&missing_field=codigo_point",
        },
    ]


def _build_dashboard_master_demand_priority(limit: int = 5, lookback_days: int = 45) -> list[dict]:
    canonical_rows = canonicalized_active_insumos(limit=5000)
    candidate_rows = []
    member_ids: list[int] = []
    for row in canonical_rows:
        canonical = row["canonical"]
        profile = enterprise_readiness_profile(canonical)
        if profile["readiness_label"] != "Incompleto":
            continue
        candidate_rows.append(
            {
                "canonical": canonical,
                "profile": profile,
                "member_ids": [int(member_id) for member_id in row["member_ids"]],
            }
        )
        member_ids.extend(int(member_id) for member_id in row["member_ids"])
    if not candidate_rows:
        return []

    receta_map: dict[int, set[int]] = defaultdict(set)
    for receta_id, insumo_id in (
        LineaReceta.objects.filter(
            insumo_id__in=member_ids,
            receta__tipo=Receta.TIPO_PRODUCTO_FINAL,
        )
        .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        .values_list("receta_id", "insumo_id")
    ):
        receta_map[int(insumo_id)].add(int(receta_id))

    receta_ids = sorted({rid for values in receta_map.values() for rid in values})
    if not receta_ids:
        return []

    end_date = timezone.localdate() - timedelta(days=1)
    start_date = end_date - timedelta(days=max(lookback_days - 1, 0))
    historico_totals = {
        int(row["receta_id"]): Decimal(str(row["total"] or 0))
        for row in (
            VentaHistorica.objects.filter(
                receta_id__in=receta_ids,
                fecha__gte=start_date,
                fecha__lte=end_date,
            )
            .values("receta_id")
            .annotate(total=Sum("cantidad"))
        )
    }
    receta_name_map = {
        int(row["id"]): row["nombre"]
        for row in Receta.objects.filter(id__in=receta_ids).values("id", "nombre")
    }

    rows: list[dict[str, object]] = []
    for row in candidate_rows:
        linked_recipe_ids = sorted({rid for member_id in row["member_ids"] for rid in receta_map.get(member_id, set())})
        historico_units = sum((historico_totals.get(rid, Decimal("0")) for rid in linked_recipe_ids), Decimal("0"))
        if historico_units <= 0:
            continue
        if historico_units >= Decimal("80") or len(linked_recipe_ids) >= 3:
            priority_label = "Demanda crítica bloqueada"
            priority_tone = "danger"
        elif historico_units >= Decimal("30"):
            priority_label = "Alta demanda en revisión"
            priority_tone = "warning"
        else:
            priority_label = "Seguimiento comercial"
            priority_tone = "primary"
        rows.append(
            {
                "nombre": row["canonical"].nombre,
                "priority_label": priority_label,
                "priority_tone": priority_tone,
                "historico_units": historico_units.quantize(Decimal("0.1")),
                "missing": row["profile"]["missing"][:2],
                "recipe_names": [receta_name_map[rid] for rid in linked_recipe_ids[:3] if rid in receta_name_map],
                "final_recipe_count": len(linked_recipe_ids),
                "priority_score": historico_units * Decimal("10") + Decimal(str(len(linked_recipe_ids) * 8)),
                "url": reverse("maestros:insumo_update", args=[row["canonical"].id]),
            }
        )

    rows.sort(
        key=lambda item: (
            Decimal(str(item["priority_score"] or 0)),
            Decimal(str(item["historico_units"] or 0)),
            int(item["final_recipe_count"] or 0),
        ),
        reverse=True,
    )
    return rows[:limit]


def _build_dashboard_master_demand_critical_queue(limit: int = 4) -> list[dict[str, object]]:
    rows = _build_dashboard_master_demand_priority(limit=max(limit * 3, limit))
    critical_rows = [row for row in rows if str(row.get("priority_tone") or "") == "danger"]
    return critical_rows[:limit]


def _build_dashboard_master_demand_critical_focus() -> dict[str, object]:
    queue = _build_dashboard_master_demand_critical_queue(limit=4)
    if not queue:
        return {
            "status": "Sin cola crítica",
            "tone": "success",
            "count": 0,
            "detail": "No hay artículos maestros críticos bloqueando el troncal comercial.",
            "url": reverse("maestros:insumo_list"),
            "cta": "Abrir maestro",
        }
    top_item = queue[0]
    return {
        "status": "Troncal retenido",
        "tone": "danger",
        "count": len(queue),
        "detail": (
            f"La demanda crítica sigue bloqueada por {top_item.get('nombre', 'un artículo maestro')}. "
            "Cierra esta cola antes de liberar plan, MRP, compras o reabasto."
        ),
        "url": str(top_item.get("url") or reverse("maestros:insumo_list")),
        "cta": "Cerrar prioridad crítica",
    }


def _build_dashboard_recipe_governance() -> list[dict]:
    recetas_url = reverse("recetas:recetas_list")
    pending_matching = (
        LineaReceta.objects.exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        .filter(match_status__in=[LineaReceta.STATUS_NEEDS_REVIEW, LineaReceta.STATUS_REJECTED])
        .values("receta_id")
        .distinct()
        .count()
    )
    finales_sin_empaque = (
        Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL)
        .exclude(lineas__insumo__tipo_item=Insumo.TIPO_EMPAQUE)
        .distinct()
        .count()
    )
    bases_sin_derivados = (
        Receta.objects.filter(tipo=Receta.TIPO_PREPARACION, usa_presentaciones=True)
        .annotate(active_presentaciones=Count("presentaciones", filter=Q(presentaciones__activo=True), distinct=True))
        .filter(active_presentaciones=0)
        .count()
    )
    bases_sin_rendimiento = (
        Receta.objects.filter(tipo=Receta.TIPO_PREPARACION)
        .filter(Q(rendimiento_cantidad__isnull=True) | Q(rendimiento_unidad__isnull=True))
        .count()
    )

    return [
        {
            "label": "Recetas con cierre pendiente",
            "value": pending_matching,
            "tone": "warning" if pending_matching else "success",
            "url": f"{recetas_url}?health_status=pendientes",
        },
        {
            "label": "Productos sin empaque",
            "value": finales_sin_empaque,
            "tone": "danger" if finales_sin_empaque else "success",
            "url": f"{recetas_url}?governance_issue=sin_empaque",
        },
        {
            "label": "Bases sin derivados",
            "value": bases_sin_derivados,
            "tone": "warning" if bases_sin_derivados else "success",
            "url": f"{recetas_url}?chain_checkpoint=derived_sync",
        },
        {
            "label": "Bases sin rendimiento",
            "value": bases_sin_rendimiento,
            "tone": "warning" if bases_sin_rendimiento else "success",
            "url": f"{recetas_url}?governance_issue=rendimiento",
        },
    ]


def _compute_plan_forecast_semaforo(periodo_mes: str) -> dict:
    try:
        year, month = periodo_mes.split("-")
        y = int(year)
        m = int(month)
    except Exception:
        today = timezone.localdate()
        y = today.year
        m = today.month
        periodo_mes = f"{y:04d}-{m:02d}"

    # In local or partially migrated environments, forecast tables may not exist yet.
    pron_unavailable = False
    plan_unavailable = False
    try:
        pron_rows = list(
            PronosticoVenta.objects.filter(periodo=periodo_mes)
            .values("receta_id", "receta__nombre")
            .annotate(total=Sum("cantidad"))
        )
    except (OperationalError, ProgrammingError):
        pron_rows = []
        pron_unavailable = True

    try:
        plan_rows = list(
            PlanProduccionItem.objects.filter(plan__fecha_produccion__year=y, plan__fecha_produccion__month=m)
            .values("receta_id", "receta__nombre")
            .annotate(total=Sum("cantidad"))
        )
    except (OperationalError, ProgrammingError):
        plan_rows = []
        plan_unavailable = True

    merged: dict[int, dict] = {}
    for row in pron_rows:
        receta_id = int(row["receta_id"])
        merged[receta_id] = {
            "receta_id": receta_id,
            "receta": row["receta__nombre"],
            "pronostico": Decimal(str(row["total"] or 0)),
            "plan": Decimal("0"),
        }
    for row in plan_rows:
        receta_id = int(row["receta_id"])
        current = merged.setdefault(
            receta_id,
            {
                "receta_id": receta_id,
                "receta": row["receta__nombre"],
                "pronostico": Decimal("0"),
                "plan": Decimal("0"),
            },
        )
        current["plan"] = Decimal(str(row["total"] or 0))

    rows = []
    con_desviacion = 0
    for row in merged.values():
        delta = row["plan"] - row["pronostico"]
        if delta != 0:
            con_desviacion += 1
        row["delta"] = delta
        if row["pronostico"] > 0:
            row["delta_pct"] = (delta * Decimal("100")) / row["pronostico"]
        else:
            row["delta_pct"] = None
        rows.append(row)

    rows = sorted(rows, key=lambda x: (abs(x["delta"]), x["receta"]), reverse=True)
    total_plan = sum((r["plan"] for r in rows), Decimal("0"))
    total_pronostico = sum((r["pronostico"] for r in rows), Decimal("0"))
    delta_total = total_plan - total_pronostico
    if total_pronostico > 0:
        desviacion_abs_pct = (abs(delta_total) * Decimal("100")) / total_pronostico
    else:
        desviacion_abs_pct = None

    if total_pronostico <= 0 and total_plan <= 0:
        semaforo = "Sin datos"
        semaforo_badge = "bg-warning"
    elif total_pronostico <= 0 and total_plan > 0:
        semaforo = "Rojo"
        semaforo_badge = "bg-danger"
    elif desviacion_abs_pct is not None and desviacion_abs_pct <= Decimal("10"):
        semaforo = "Verde"
        semaforo_badge = "bg-success"
    elif desviacion_abs_pct is not None and desviacion_abs_pct <= Decimal("25"):
        semaforo = "Amarillo"
        semaforo_badge = "bg-warning"
    else:
        semaforo = "Rojo"
        semaforo_badge = "bg-danger"

    return {
        "periodo_mes": periodo_mes,
        "total_plan": total_plan,
        "total_pronostico": total_pronostico,
        "delta_total": delta_total,
        "desviacion_abs_pct": desviacion_abs_pct,
        "recetas_total": len(rows),
        "recetas_con_desviacion": con_desviacion,
        "semaforo_label": semaforo,
        "semaforo_badge": semaforo_badge,
        "rows_top": rows[:8],
        "data_unavailable": pron_unavailable or plan_unavailable,
    }


def _sales_source_context() -> dict[str, object]:
    latest_point_date = max(
        [
            value
            for value in [
                PointDailySale.objects.filter(source_endpoint=OFFICIAL_POINT_SOURCE)
                .order_by("-sale_date")
                .values_list("sale_date", flat=True)
                .first(),
                PointDailySale.objects.filter(source_endpoint=RECENT_POINT_SOURCE)
                .order_by("-sale_date")
                .values_list("sale_date", flat=True)
                .first(),
            ]
            if value
        ],
        default=None,
    )
    latest_bridge_date = (
        VentaHistorica.objects.filter(fuente=POINT_BRIDGE_SALES_SOURCE)
        .order_by("-fecha")
        .values_list("fecha", flat=True)
        .first()
    )
    latest_hist_date = VentaHistorica.objects.order_by("-fecha").values_list("fecha", flat=True).first()
    if latest_point_date:
        return {"mode": "point_stage", "latest_date": latest_point_date, "label": "Point directo", "detail": "Fuente canónica Point bridge.", "canonical": True}
    if latest_bridge_date:
        return {"mode": "point_history", "latest_date": latest_bridge_date, "label": "Point conciliado", "detail": "Histórico Point materializado a ERP.", "canonical": True}
    if latest_hist_date:
        return {"mode": "historical_fallback", "latest_date": latest_hist_date, "label": "Histórico importado no canónico", "detail": "Fuente referencial; no representa Point directo.", "canonical": False}
    return {"mode": "none", "latest_date": None, "label": "Sin fuente", "detail": "No hay ventas cargadas.", "canonical": False}


def _operational_sales_filters(*, start_date: date, end_date: date) -> Q:
    official_max = (
        PointDailySale.objects.filter(source_endpoint=OFFICIAL_POINT_SOURCE)
        .order_by("-sale_date")
        .values_list("sale_date", flat=True)
        .first()
    )
    q = Q()
    if official_max:
        official_end = min(end_date, official_max)
        if start_date <= official_end:
            q |= Q(source_endpoint=OFFICIAL_POINT_SOURCE, sale_date__gte=start_date, sale_date__lte=official_end)
        recent_start = max(start_date, official_max + timedelta(days=1))
    else:
        recent_start = start_date
    if recent_start <= end_date:
        q |= Q(source_endpoint=RECENT_POINT_SOURCE, sale_date__gte=recent_start, sale_date__lte=end_date)
    return q


def _operational_sales_rows_for_date(target_date: date):
    if PointDailySale.objects.filter(sale_date=target_date, source_endpoint=OFFICIAL_POINT_SOURCE).exists():
        return PointDailySale.objects.filter(sale_date=target_date, source_endpoint=OFFICIAL_POINT_SOURCE)
    return PointDailySale.objects.filter(sale_date=target_date, source_endpoint=RECENT_POINT_SOURCE)


def _sales_rows_for_date(source: dict[str, object], target_date):
    if source["mode"] == "point_stage":
        return _operational_sales_rows_for_date(target_date)
    if source["mode"] == "point_history":
        return VentaHistorica.objects.filter(fecha=target_date, fuente=POINT_BRIDGE_SALES_SOURCE)
    if source["mode"] == "historical_fallback":
        return VentaHistorica.objects.filter(fecha=target_date)
    return VentaHistorica.objects.none()


def _sales_rows_for_month(source: dict[str, object], year: int, month: int):
    if source["mode"] == "point_stage":
        start_date = date(year, month, 1)
        end_date = date(year, month, calendar.monthrange(year, month)[1])
        return PointDailySale.objects.filter(
            sale_date__year=year,
            sale_date__month=month,
        ).filter(
            _operational_sales_filters(start_date=start_date, end_date=end_date)
        )
    if source["mode"] == "point_history":
        return VentaHistorica.objects.filter(fecha__year=year, fecha__month=month, fuente=POINT_BRIDGE_SALES_SOURCE)
    if source["mode"] == "historical_fallback":
        return VentaHistorica.objects.filter(fecha__year=year, fecha__month=month)
    return VentaHistorica.objects.none()


def _point_sales_month_total(year: int, month: int) -> dict[str, object]:
    direct_qs = _sales_rows_for_month({"mode": "point_stage"}, year, month)
    direct_total = direct_qs.aggregate(total=Sum("total_amount")).get("total") or Decimal("0")
    if direct_total > 0:
        return {"value": Decimal(str(direct_total)), "source_label": "Point directo"}

    bridge_qs = VentaHistorica.objects.filter(
        fecha__year=year,
        fecha__month=month,
        fuente=POINT_BRIDGE_SALES_SOURCE,
    )
    bridge_total = bridge_qs.aggregate(total=Sum("monto_total")).get("total") or Decimal("0")
    if bridge_total > 0:
        return {"value": Decimal(str(bridge_total)), "source_label": "Point conciliado"}

    return {"value": Decimal("0"), "source_label": "Sin dato oficial"}


def _sales_previous_dates(source: dict[str, object], target_date) -> list[date]:
    if source["mode"] == "point_stage":
        return list(
            PointDailySale.objects.filter(
                sale_date__lt=target_date,
                source_endpoint__in=[OFFICIAL_POINT_SOURCE, RECENT_POINT_SOURCE],
            )
            .order_by("-sale_date")
            .values_list("sale_date", flat=True)
            .distinct()
        )
    if source["mode"] == "point_history":
        return list(
            VentaHistorica.objects.filter(fecha__lt=target_date, fuente=POINT_BRIDGE_SALES_SOURCE)
            .order_by("-fecha")
            .values_list("fecha", flat=True)
            .distinct()
        )
    if source["mode"] == "historical_fallback":
        return list(VentaHistorica.objects.filter(fecha__lt=target_date).order_by("-fecha").values_list("fecha", flat=True).distinct())
    return []


def _sales_history_queryset(source: dict[str, object]):
    if source["mode"] == "point_stage":
        official_max = (
            PointDailySale.objects.filter(source_endpoint=OFFICIAL_POINT_SOURCE)
            .order_by("-sale_date")
            .values_list("sale_date", flat=True)
            .first()
        )
        q = Q(source_endpoint=OFFICIAL_POINT_SOURCE)
        if official_max:
            q |= Q(source_endpoint=RECENT_POINT_SOURCE, sale_date__gt=official_max)
        else:
            q = Q(source_endpoint=RECENT_POINT_SOURCE)
        return PointDailySale.objects.filter(q)
    if source["mode"] == "point_history":
        return VentaHistorica.objects.filter(fuente=POINT_BRIDGE_SALES_SOURCE)
    if source["mode"] == "historical_fallback":
        return VentaHistorica.objects.all()
    return VentaHistorica.objects.none()


def _build_dashboard_sales_history_summary() -> dict:
    source = _sales_source_context()
    rows_qs = _sales_history_queryset(source)
    total_rows = rows_qs.count()
    if total_rows == 0:
        return {
            "available": False,
            "status": "Sin histórico",
            "tone": "warning",
            "detail": "Todavía no hay ventas históricas cargadas para planeación y demanda.",
            "source_label": source["label"],
            "date_label": "Sin cobertura",
            "first_date": None,
            "last_date": None,
            "active_days": 0,
            "expected_days": 0,
            "missing_days": 0,
            "branch_count": 0,
            "recipe_count": 0,
            "total_rows": 0,
            "total_units": Decimal("0"),
            "total_amount": Decimal("0"),
            "latest_source": "",
            "top_branches": [],
            "top_recipes": [],
            "url": reverse("reportes:bi"),
            "cta": "Abrir reportes",
        }

    if source["mode"] == "point_stage":
        total_units = rows_qs.aggregate(total=Sum("quantity")).get("total") or Decimal("0")
        total_amount = rows_qs.aggregate(total=Sum("total_amount")).get("total") or Decimal("0")
        first_date = rows_qs.order_by("sale_date").values_list("sale_date", flat=True).first()
        last_date = rows_qs.order_by("-sale_date").values_list("sale_date", flat=True).first()
        active_days = rows_qs.values_list("sale_date", flat=True).distinct().count()
        branch_count = rows_qs.values_list("branch_id", flat=True).distinct().count()
        recipe_count = rows_qs.values_list("product_id", flat=True).distinct().count()
        latest_source = "POINT_STAGE"
        top_branches = list(
            rows_qs.values("branch__external_id", "branch__name")
            .annotate(total=Sum("quantity"))
            .order_by("-total", "branch__external_id")[:4]
        )
        top_recipes = list(
            rows_qs.values("product__name")
            .annotate(total=Sum("quantity"))
            .order_by("-total", "product__name")[:5]
        )
    else:
        total_units = rows_qs.aggregate(total=Sum("cantidad")).get("total") or Decimal("0")
        total_amount = rows_qs.aggregate(total=Sum("monto_total")).get("total") or Decimal("0")
        first_date = rows_qs.order_by("fecha").values_list("fecha", flat=True).first()
        last_date = rows_qs.order_by("-fecha").values_list("fecha", flat=True).first()
        active_days = rows_qs.values_list("fecha", flat=True).distinct().count()
        branch_count = rows_qs.exclude(sucursal_id__isnull=True).values_list("sucursal_id", flat=True).distinct().count()
        recipe_count = rows_qs.values_list("receta_id", flat=True).distinct().count()
        latest_source = rows_qs.order_by("-fecha", "-actualizado_en").values_list("fuente", flat=True).first() or ""
        top_branches = list(
            rows_qs.exclude(sucursal_id__isnull=True)
            .values("sucursal__codigo", "sucursal__nombre")
            .annotate(total=Sum("cantidad"))
            .order_by("-total", "sucursal__codigo")[:4]
        )
        top_recipes = list(
            rows_qs.values("receta__nombre")
            .annotate(total=Sum("cantidad"))
            .order_by("-total", "receta__nombre")[:5]
        )

    expected_days = ((last_date - first_date).days + 1) if first_date and last_date else 0
    missing_days = max(expected_days - active_days, 0)
    status = "Cobertura cerrada" if missing_days == 0 else "Cobertura parcial"
    tone = "success" if missing_days == 0 and source["canonical"] else "warning"
    detail = (
        f"{source['detail']} El histórico diario no muestra huecos entre la primera y la última fecha cargadas."
        if missing_days == 0
        else f"{source['detail']} Hay {missing_days} día(s) faltantes dentro del rango cargado."
    )
    return {
        "available": True,
        "status": status,
        "tone": tone,
        "official_ready": bool(source["canonical"] and missing_days == 0),
        "detail": detail,
        "source_label": source["label"],
        "date_label": f"{first_date.strftime('%d/%m/%Y')} → {last_date.strftime('%d/%m/%Y')}" if first_date and last_date else "Sin cobertura",
        "first_date": first_date,
        "last_date": last_date,
        "active_days": active_days,
        "expected_days": expected_days,
        "missing_days": missing_days,
        "branch_count": branch_count,
        "recipe_count": recipe_count,
        "total_rows": total_rows,
        "total_units": total_units,
        "total_amount": total_amount,
        "latest_source": latest_source,
        "top_branches": top_branches,
        "top_recipes": top_recipes,
        "url": reverse("reportes:bi"),
        "cta": "Abrir reportes",
    }


def _build_dashboard_daily_sales_snapshot() -> dict[str, object]:
    source = _sales_source_context()
    latest_date = source["latest_date"]
    if not latest_date:
        return {
            "status": "Sin cortes",
            "tone": "warning",
            "detail": "No hay ventas cargadas para lectura operativa.",
            "date_label": "Sin fecha",
            "source_label": "Sin fuente",
            "total_units": Decimal("0"),
            "total_amount": Decimal("0"),
            "total_tickets": 0,
            "branch_count": 0,
            "recipe_count": 0,
            "comparison_label": "Sin comparativo",
            "comparison_tone": "warning",
            "comparison_detail": "Carga ventas diarias para habilitar lectura del día.",
            "comparison_basis": "Sin referencia disponible",
            "top_branches": [],
            "top_products": [],
            "url": reverse("reportes:bi"),
            "cta": "Abrir BI",
        }

    rows = _sales_rows_for_date(source, latest_date)
    if source["mode"] == "point_stage":
        indicator_rows = PointDailyBranchIndicator.objects.filter(indicator_date=latest_date)
        totals = rows.aggregate(
            total_units=Sum("quantity"),
            total_amount=Sum("total_amount"),
            branch_count=Count("branch", distinct=True),
            recipe_count=Count("product", distinct=True),
        )
        indicator_totals = indicator_rows.aggregate(total_tickets=Sum("total_tickets"))
        mapped_totals = rows.filter(receta_id__isnull=False, branch__erp_branch_id__isnull=False).aggregate(
            mapped_units=Sum("quantity"),
            mapped_amount=Sum("total_amount"),
        )
    else:
        totals = rows.aggregate(
            total_units=Sum("cantidad"),
            total_amount=Sum("monto_total"),
            total_tickets=Sum("tickets"),
            branch_count=Count("sucursal", distinct=True),
            recipe_count=Count("receta", distinct=True),
        )
        indicator_totals = {}
        mapped_totals = {"mapped_units": totals.get("total_units"), "mapped_amount": totals.get("total_amount")}

    prev_date = next(iter(_sales_previous_dates(source, latest_date)), None)
    total_amount = Decimal(str(totals.get("total_amount") or 0))
    total_units = Decimal(str(totals.get("total_units") or 0))
    comparison_label = "Base inicial"
    comparison_tone = "warning"
    comparison_detail = "Aún no hay un corte previo comparable."
    comparison_basis = "Contra el corte inmediato anterior"
    if prev_date:
        prev_rows = _sales_rows_for_date(source, prev_date)
        if source["mode"] == "point_stage":
            prev_totals = prev_rows.aggregate(total_units=Sum("quantity"), total_amount=Sum("total_amount"))
        else:
            prev_totals = prev_rows.aggregate(total_units=Sum("cantidad"), total_amount=Sum("monto_total"))
        prev_amount = Decimal(str(prev_totals.get("total_amount") or 0))
        prev_units = Decimal(str(prev_totals.get("total_units") or 0))
        if prev_amount > 0:
            delta_pct = ((total_amount - prev_amount) / prev_amount) * Decimal("100")
            comparison_label = "Arriba" if delta_pct >= 0 else "Abajo"
            comparison_tone = "success" if delta_pct >= 0 else "warning"
            comparison_detail = f"{abs(delta_pct):.1f}% vs corte previo ({prev_date.isoformat()})"
        elif prev_units > 0:
            delta_pct = ((total_units - prev_units) / prev_units) * Decimal("100")
            comparison_label = "Arriba" if delta_pct >= 0 else "Abajo"
            comparison_tone = "success" if delta_pct >= 0 else "warning"
            comparison_detail = f"{abs(delta_pct):.1f}% en unidades vs corte previo ({prev_date.isoformat()})"

    month_rows = _sales_rows_for_month(source, latest_date.year, latest_date.month)
    if source["mode"] == "point_stage":
        month_indicator_rows = PointDailyBranchIndicator.objects.filter(
            indicator_date__year=latest_date.year,
            indicator_date__month=latest_date.month,
        )
        month_start = date(latest_date.year, latest_date.month, 1)
        partial_month_amount, partial_month_units = _partial_month_amount_quantity(
            start_date=month_start,
            end_date=latest_date,
        )
        month_totals = month_rows.aggregate(total_units=Sum("quantity"), total_amount=Sum("total_amount"))
        month_totals["total_amount"] = partial_month_amount
        month_totals["total_units"] = partial_month_units
        month_indicator_totals = month_indicator_rows.aggregate(total_tickets=Sum("total_tickets"))
        top_branches = list(
            rows.values("branch__external_id", "branch__name")
            .annotate(total=Sum("quantity"), amount=Sum("total_amount"))
            .order_by("-amount", "-total", "branch__name")[:5]
        )
        top_products = list(
            rows.values("product__name")
            .annotate(total=Sum("quantity"), amount=Sum("total_amount"))
            .order_by("-amount", "-total", "product__name")[:5]
        )
    else:
        month_totals = month_rows.aggregate(total_units=Sum("cantidad"), total_amount=Sum("monto_total"), total_tickets=Sum("tickets"))
        month_indicator_totals = {}
        top_branches = list(
            rows.exclude(sucursal_id__isnull=True)
            .values("sucursal__codigo", "sucursal__nombre")
            .annotate(total=Sum("cantidad"), amount=Sum("monto_total"))
            .order_by("-amount", "-total", "sucursal__nombre")[:5]
        )
        top_products = list(
            rows.values("receta__nombre")
            .annotate(total=Sum("cantidad"), amount=Sum("monto_total"))
            .order_by("-amount", "-total", "receta__nombre")[:5]
        )

    total_tickets = int(
        (indicator_totals.get("total_tickets") if source["mode"] == "point_stage" else totals.get("total_tickets"))
        or 0
    )
    if total_tickets <= 0:
        total_tickets = int(
            PointDailyBranchIndicator.objects.filter(indicator_date=latest_date).aggregate(total_tickets=Sum("total_tickets")).get("total_tickets")
            or 0
        )
    branch_count = int(totals.get("branch_count") or 0)
    mapped_amount = Decimal(str(mapped_totals.get("mapped_amount") or 0))
    mapped_units = Decimal(str(mapped_totals.get("mapped_units") or 0))
    unmapped_amount = total_amount - mapped_amount
    unmapped_units = total_units - mapped_units
    mapping_coverage_pct = ((mapped_amount / total_amount) * Decimal("100")) if total_amount > 0 else None
    month_tickets = int(
        (month_indicator_totals.get("total_tickets") if source["mode"] == "point_stage" else month_totals.get("total_tickets"))
        or 0
    )
    if month_tickets <= 0:
        month_tickets = int(
            PointDailyBranchIndicator.objects.filter(
                indicator_date__year=latest_date.year,
                indicator_date__month=latest_date.month,
            ).aggregate(total_tickets=Sum("total_tickets")).get("total_tickets")
            or 0
        )

    tickets_available = total_tickets > 0
    avg_ticket = (total_amount / Decimal(total_tickets)) if tickets_available else None
    month_tickets_available = month_tickets > 0
    month_avg_ticket = (Decimal(str(month_totals.get("total_amount") or 0)) / Decimal(month_tickets)) if month_tickets_available else None
    avg_branch_amount = (total_amount / Decimal(branch_count)) if branch_count else Decimal("0")
    top_branch_rows = [
        {
            "label": row.get("branch__external_id") or row.get("sucursal__codigo") or "Sucursal",
            "secondary": row.get("branch__name") or row.get("sucursal__nombre") or "",
            "amount": row.get("amount") or Decimal("0"),
            "total": row.get("total") or Decimal("0"),
        }
        for row in top_branches
    ]
    top_product_rows = [
        {
            "label": row.get("product__name") or row.get("receta__nombre") or "Producto",
            "secondary": "",
            "amount": row.get("amount") or Decimal("0"),
            "total": row.get("total") or Decimal("0"),
        }
        for row in top_products
    ]

    return {
        "status": "Corte cargado" if source["canonical"] else "Corte referencial",
        "tone": "success" if source["canonical"] else "warning",
        "detail": f"Resumen del último corte de ventas. {source['detail']}",
        "date": latest_date,
        "date_label": latest_date.isoformat(),
        "month_label": f"{latest_date.year}-{latest_date.month:02d}",
        "source_label": source["label"],
        "total_units": total_units,
        "total_amount": total_amount,
        "total_tickets": total_tickets,
        "tickets_available": tickets_available,
        "branch_count": branch_count,
        "recipe_count": int(totals.get("recipe_count") or 0),
        "avg_ticket": avg_ticket,
        "avg_branch_amount": avg_branch_amount,
        "mapped_amount": mapped_amount,
        "mapped_units": mapped_units,
        "unmapped_amount": unmapped_amount,
        "unmapped_units": unmapped_units,
        "mapping_coverage_pct": mapping_coverage_pct,
        "month_amount": Decimal(str(month_totals.get("total_amount") or 0)),
        "month_units": Decimal(str(month_totals.get("total_units") or 0)),
        "month_tickets": month_tickets,
        "month_tickets_available": month_tickets_available,
        "month_avg_ticket": month_avg_ticket,
        "comparison_label": comparison_label,
        "comparison_tone": comparison_tone,
        "comparison_detail": comparison_detail,
        "comparison_basis": comparison_basis,
        "top_branches": top_branch_rows,
        "top_products": top_product_rows,
        "url": reverse("reportes:bi"),
        "cta": "Abrir BI",
    }


def _build_dashboard_branch_daily_exceptions(limit: int = 6) -> list[dict[str, object]]:
    source = _sales_source_context()
    latest_date = source["latest_date"]
    if not latest_date:
        return []
    if source["mode"] == "point_stage":
        current_rows = list(
            _sales_rows_for_date(source, latest_date)
            .values("branch_id", "branch__external_id", "branch__name")
            .annotate(units=Sum("quantity"), amount=Sum("total_amount"), tickets=Sum("tickets"), recipe_count=Count("product", distinct=True))
        )
    else:
        current_rows = list(
            _sales_rows_for_date(source, latest_date).exclude(sucursal_id__isnull=True)
            .values("sucursal_id", "sucursal__codigo", "sucursal__nombre")
            .annotate(units=Sum("cantidad"), amount=Sum("monto_total"), tickets=Sum("tickets"), recipe_count=Count("receta", distinct=True))
        )
    if not current_rows:
        return []
    prev_date = next(iter(_sales_previous_dates(source, latest_date)), None)
    prev_map: dict[int, dict[str, Decimal]] = {}
    if prev_date:
        if source["mode"] == "point_stage":
            prev_map = {
                row["branch_id"]: row
                for row in _sales_rows_for_date(source, prev_date)
                .values("branch_id")
                .annotate(units=Sum("quantity"), amount=Sum("total_amount"), tickets=Sum("tickets"))
            }
        else:
            prev_map = {
                row["sucursal_id"]: row
                for row in _sales_rows_for_date(source, prev_date).exclude(sucursal_id__isnull=True)
                .values("sucursal_id")
                .annotate(units=Sum("cantidad"), amount=Sum("monto_total"), tickets=Sum("tickets"))
            }
    exception_rows: list[dict[str, object]] = []
    for row in current_rows:
        branch_id = int(row["branch_id"] if source["mode"] == "point_stage" else row["sucursal_id"])
        current_amount = Decimal(str(row.get("amount") or 0))
        current_units = Decimal(str(row.get("units") or 0))
        previous = prev_map.get(branch_id) or {}
        prev_amount = Decimal(str(previous.get("amount") or 0))
        prev_units = Decimal(str(previous.get("units") or 0))
        delta_pct = None
        if prev_amount > 0:
            delta_pct = ((current_amount - prev_amount) / prev_amount) * Decimal("100")
        elif prev_units > 0:
            delta_pct = ((current_units - prev_units) / prev_units) * Decimal("100")
        if delta_pct is None:
            status, tone, detail, rank_score = "Sin comparativo", "warning", "No hay corte previo comparable para esta sucursal.", Decimal("0")
        elif delta_pct <= Decimal("-15"):
            status, tone, detail, rank_score = "Caída fuerte", "danger", f"Cae {abs(delta_pct):.1f}% contra el corte previo.", abs(delta_pct) + Decimal("100")
        elif delta_pct >= Decimal("15"):
            status, tone, detail, rank_score = "Alza fuerte", "success", f"Sube {delta_pct:.1f}% contra el corte previo.", delta_pct
        else:
            status, tone, detail, rank_score = "Estable", "warning", f"Variación de {delta_pct:.1f}% contra el corte previo.", abs(delta_pct)
        exception_rows.append(
            {
                "branch_code": row.get("branch__external_id") or row.get("sucursal__codigo") or "SIN-COD",
                "branch_name": row.get("branch__name") or row.get("sucursal__nombre") or "Sucursal",
                "units": current_units,
                "amount": current_amount,
                "tickets": int(row.get("tickets") or 0),
                "recipe_count": int(row.get("recipe_count") or 0),
                "status": status,
                "tone": tone,
                "detail": detail,
                "delta_pct": delta_pct,
                "previous_date": prev_date,
                "rank_score": rank_score,
            }
        )
    severity_order = {"danger": 0, "warning": 1, "success": 2}
    exception_rows.sort(key=lambda item: (severity_order.get(str(item.get("tone") or ""), 9), -float(item.get("rank_score") or 0), str(item.get("branch_code") or "")))
    return exception_rows[:limit]


def _build_dashboard_branch_weekday_comparisons(limit: int = 6) -> list[dict[str, object]]:
    source = _sales_source_context()
    latest_date = source["latest_date"]
    if not latest_date:
        return []
    comparable_date = next((date_value for date_value in _sales_previous_dates(source, latest_date) if date_value.weekday() == latest_date.weekday()), None)
    if not comparable_date:
        return []
    if source["mode"] == "point_stage":
        current_rows = list(
            _sales_rows_for_date(source, latest_date)
            .values("branch_id", "branch__external_id", "branch__name")
            .annotate(units=Sum("quantity"), amount=Sum("total_amount"), tickets=Sum("tickets"))
        )
        comparable_map = {
            row["branch_id"]: row
            for row in _sales_rows_for_date(source, comparable_date)
            .values("branch_id")
            .annotate(units=Sum("quantity"), amount=Sum("total_amount"), tickets=Sum("tickets"))
        }
    else:
        current_rows = list(
            _sales_rows_for_date(source, latest_date).exclude(sucursal_id__isnull=True)
            .values("sucursal_id", "sucursal__codigo", "sucursal__nombre")
            .annotate(units=Sum("cantidad"), amount=Sum("monto_total"), tickets=Sum("tickets"))
        )
        comparable_map = {
            row["sucursal_id"]: row
            for row in _sales_rows_for_date(source, comparable_date).exclude(sucursal_id__isnull=True)
            .values("sucursal_id")
            .annotate(units=Sum("cantidad"), amount=Sum("monto_total"), tickets=Sum("tickets"))
        }
    rows: list[dict[str, object]] = []
    for row in current_rows:
        branch_id = int(row["branch_id"] if source["mode"] == "point_stage" else row["sucursal_id"])
        comparable = comparable_map.get(branch_id)
        if not comparable:
            continue
        current_amount = Decimal(str(row.get("amount") or 0))
        current_units = Decimal(str(row.get("units") or 0))
        comparable_amount = Decimal(str(comparable.get("amount") or 0))
        comparable_units = Decimal(str(comparable.get("units") or 0))
        delta_pct = None
        if comparable_amount > 0:
            delta_pct = ((current_amount - comparable_amount) / comparable_amount) * Decimal("100")
        elif comparable_units > 0:
            delta_pct = ((current_units - comparable_units) / comparable_units) * Decimal("100")
        if delta_pct is None:
            continue
        if delta_pct <= Decimal("-12"):
            status, tone, detail, rank_score = "Abajo del comparable", "danger", f"Cae {abs(delta_pct):.1f}% contra el último mismo día de semana ({comparable_date.isoformat()}).", abs(delta_pct) + Decimal("100")
        elif delta_pct >= Decimal("12"):
            status, tone, detail, rank_score = "Arriba del comparable", "success", f"Sube {delta_pct:.1f}% contra el último mismo día de semana ({comparable_date.isoformat()}).", delta_pct
        else:
            status, tone, detail, rank_score = "Dentro de rango", "warning", f"Variación de {delta_pct:.1f}% contra el último mismo día de semana ({comparable_date.isoformat()}).", abs(delta_pct)
        rows.append(
            {
                "branch_code": row.get("branch__external_id") or row.get("sucursal__codigo") or "SIN-COD",
                "branch_name": row.get("branch__name") or row.get("sucursal__nombre") or "Sucursal",
                "units": current_units,
                "amount": current_amount,
                "tickets": int(row.get("tickets") or 0),
                "status": status,
                "tone": tone,
                "detail": detail,
                "delta_pct": delta_pct,
                "comparable_date": comparable_date,
                "rank_score": rank_score,
            }
        )
    severity_order = {"danger": 0, "warning": 1, "success": 2}
    rows.sort(key=lambda item: (severity_order.get(str(item.get("tone") or ""), 9), -float(item.get("rank_score") or 0), str(item.get("branch_code") or "")))
    return rows[:limit]


def _build_dashboard_product_daily_exceptions(limit: int = 6) -> list[dict[str, object]]:
    source = _sales_source_context()
    latest_date = source["latest_date"]
    if not latest_date:
        return []
    if source["mode"] == "point_stage":
        current_rows = list(
            _sales_rows_for_date(source, latest_date)
            .values("product_id", "product__name")
            .annotate(units=Sum("quantity"), amount=Sum("total_amount"), tickets=Sum("tickets"), branch_count=Count("branch", distinct=True))
        )
    else:
        current_rows = list(
            _sales_rows_for_date(source, latest_date).exclude(receta_id__isnull=True)
            .values("receta_id", "receta__nombre")
            .annotate(units=Sum("cantidad"), amount=Sum("monto_total"), tickets=Sum("tickets"), branch_count=Count("sucursal", distinct=True))
        )
    if not current_rows:
        return []
    prev_date = next(iter(_sales_previous_dates(source, latest_date)), None)
    prev_map: dict[int, dict[str, Decimal]] = {}
    if prev_date:
        if source["mode"] == "point_stage":
            prev_map = {
                row["product_id"]: row
                for row in _sales_rows_for_date(source, prev_date)
                .values("product_id")
                .annotate(units=Sum("quantity"), amount=Sum("total_amount"), tickets=Sum("tickets"))
            }
        else:
            prev_map = {
                row["receta_id"]: row
                for row in _sales_rows_for_date(source, prev_date).exclude(receta_id__isnull=True)
                .values("receta_id")
                .annotate(units=Sum("cantidad"), amount=Sum("monto_total"), tickets=Sum("tickets"))
            }
    exception_rows: list[dict[str, object]] = []
    for row in current_rows:
        product_id = int(row["product_id"] if source["mode"] == "point_stage" else row["receta_id"])
        current_amount = Decimal(str(row.get("amount") or 0))
        current_units = Decimal(str(row.get("units") or 0))
        previous = prev_map.get(product_id) or {}
        prev_amount = Decimal(str(previous.get("amount") or 0))
        prev_units = Decimal(str(previous.get("units") or 0))
        delta_pct = None
        if prev_amount > 0:
            delta_pct = ((current_amount - prev_amount) / prev_amount) * Decimal("100")
        elif prev_units > 0:
            delta_pct = ((current_units - prev_units) / prev_units) * Decimal("100")
        if delta_pct is None:
            status, tone, detail, rank_score = "Sin comparativo", "warning", "No hay corte previo comparable para este producto.", Decimal("0")
        elif delta_pct <= Decimal("-20"):
            status, tone, detail, rank_score = "Caída fuerte", "danger", f"Cae {abs(delta_pct):.1f}% contra el corte previo.", abs(delta_pct) + Decimal("100")
        elif delta_pct >= Decimal("20"):
            status, tone, detail, rank_score = "Alza fuerte", "success", f"Sube {delta_pct:.1f}% contra el corte previo.", delta_pct
        else:
            status, tone, detail, rank_score = "Estable", "warning", f"Variación de {delta_pct:.1f}% contra el corte previo.", abs(delta_pct)
        exception_rows.append(
            {
                "recipe_name": row.get("product__name") or row.get("receta__nombre") or "Producto",
                "units": current_units,
                "amount": current_amount,
                "tickets": int(row.get("tickets") or 0),
                "branch_count": int(row.get("branch_count") or 0),
                "status": status,
                "tone": tone,
                "detail": detail,
                "delta_pct": delta_pct,
                "previous_date": prev_date,
                "rank_score": rank_score,
            }
        )
    severity_order = {"danger": 0, "warning": 1, "success": 2}
    exception_rows.sort(key=lambda item: (severity_order.get(str(item.get("tone") or ""), 9), -float(item.get("rank_score") or 0), str(item.get("recipe_name") or "")))
    return exception_rows[:limit]


def _build_dashboard_product_weekday_comparisons(limit: int = 6) -> list[dict[str, object]]:
    source = _sales_source_context()
    latest_date = source["latest_date"]
    if not latest_date:
        return []
    comparable_date = next((date_value for date_value in _sales_previous_dates(source, latest_date) if date_value.weekday() == latest_date.weekday()), None)
    if not comparable_date:
        return []
    if source["mode"] == "point_stage":
        current_rows = list(
            _sales_rows_for_date(source, latest_date)
            .values("product_id", "product__name")
            .annotate(units=Sum("quantity"), amount=Sum("total_amount"), tickets=Sum("tickets"), branch_count=Count("branch", distinct=True))
        )
        comparable_map = {
            row["product_id"]: row
            for row in _sales_rows_for_date(source, comparable_date)
            .values("product_id")
            .annotate(units=Sum("quantity"), amount=Sum("total_amount"), tickets=Sum("tickets"), branch_count=Count("branch", distinct=True))
        }
    else:
        current_rows = list(
            _sales_rows_for_date(source, latest_date).exclude(receta_id__isnull=True)
            .values("receta_id", "receta__nombre")
            .annotate(units=Sum("cantidad"), amount=Sum("monto_total"), tickets=Sum("tickets"), branch_count=Count("sucursal", distinct=True))
        )
        comparable_map = {
            row["receta_id"]: row
            for row in _sales_rows_for_date(source, comparable_date).exclude(receta_id__isnull=True)
            .values("receta_id")
            .annotate(units=Sum("cantidad"), amount=Sum("monto_total"), tickets=Sum("tickets"), branch_count=Count("sucursal", distinct=True))
        }
    rows: list[dict[str, object]] = []
    for row in current_rows:
        product_id = int(row["product_id"] if source["mode"] == "point_stage" else row["receta_id"])
        comparable = comparable_map.get(product_id)
        if not comparable:
            continue
        current_amount = Decimal(str(row.get("amount") or 0))
        current_units = Decimal(str(row.get("units") or 0))
        comparable_amount = Decimal(str(comparable.get("amount") or 0))
        comparable_units = Decimal(str(comparable.get("units") or 0))
        delta_pct = None
        if comparable_amount > 0:
            delta_pct = ((current_amount - comparable_amount) / comparable_amount) * Decimal("100")
        elif comparable_units > 0:
            delta_pct = ((current_units - comparable_units) / comparable_units) * Decimal("100")
        if delta_pct is None:
            continue
        if delta_pct <= Decimal("-15"):
            status, tone, detail, rank_score = "Abajo del comparable", "danger", f"Cae {abs(delta_pct):.1f}% contra el último mismo día de semana ({comparable_date.isoformat()}).", abs(delta_pct) + Decimal("100")
        elif delta_pct >= Decimal("15"):
            status, tone, detail, rank_score = "Arriba del comparable", "success", f"Sube {delta_pct:.1f}% contra el último mismo día de semana ({comparable_date.isoformat()}).", delta_pct
        else:
            status, tone, detail, rank_score = "Dentro de rango", "warning", f"Variación de {delta_pct:.1f}% contra el último mismo día de semana ({comparable_date.isoformat()}).", abs(delta_pct)
        rows.append(
            {
                "recipe_name": row.get("product__name") or row.get("receta__nombre") or "Producto",
                "units": current_units,
                "amount": current_amount,
                "tickets": int(row.get("tickets") or 0),
                "branch_count": int(row.get("branch_count") or 0),
                "status": status,
                "tone": tone,
                "detail": detail,
                "delta_pct": delta_pct,
                "comparable_date": comparable_date,
                "rank_score": rank_score,
            }
        )
    severity_order = {"danger": 0, "warning": 1, "success": 2}
    rows.sort(key=lambda item: (severity_order.get(str(item.get("tone") or ""), 9), -float(item.get("rank_score") or 0), str(item.get("recipe_name") or "")))
    return rows[:limit]


def _dashboard_bar_rows(
    raw_rows: list[dict[str, object]],
    label_key: str,
    value_key: str,
    secondary_key: str | None = None,
    limit: int = 6,
) -> list[dict[str, object]]:
    rows = list(raw_rows or [])[:limit]
    max_value = max((Decimal(str(item.get(value_key) or 0)) for item in rows), default=Decimal("0"))
    output: list[dict[str, object]] = []
    for item in rows:
        value = Decimal(str(item.get(value_key) or 0))
        pct = float((value / max_value) * Decimal("100")) if max_value > 0 else 0.0
        output.append(
            {
                "label": str(item.get(label_key) or "Sin dato"),
                "secondary": str(item.get(secondary_key) or "") if secondary_key else "",
                "value": value,
                "pct": max(8.0, pct) if value > 0 else 0.0,
            }
        )
    return output


def _dashboard_monthly_sales_rows(limit: int = 6) -> list[dict[str, object]]:
    if limit <= 0:
        return []

    month_names = [
        "",
        "Ene",
        "Feb",
        "Mar",
        "Abr",
        "May",
        "Jun",
        "Jul",
        "Ago",
        "Sep",
        "Oct",
        "Nov",
        "Dic",
    ]
    today = timezone.localdate()
    year, month = today.year, today.month
    pairs: list[tuple[int, int]] = []
    for _ in range(limit):
        pairs.append((year, month))
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    pairs.reverse()

    rows: list[dict[str, object]] = []
    max_amount = Decimal("0")
    for year, month in pairs:
        resolved = _point_sales_month_total(year, month)
        value = Decimal(str(resolved["value"] or 0))
        max_amount = max(max_amount, value)
        rows.append(
            {
                "label": f"{month_names[month]} {str(year)[-2:]}",
                "value": value,
                "source_label": resolved["source_label"],
            }
        )

    for row in rows:
        value = Decimal(str(row["value"]))
        pct = float((value / max_amount) * Decimal("100")) if max_amount > 0 else 0.0
        row["pct"] = max(8.0, pct) if value > 0 else 0.0
    return rows


def _dashboard_comparison_bar_rows(
    raw_rows: list[dict[str, object]],
    label_key: str,
    amount_key: str = "amount",
    secondary_key: str | None = None,
    limit: int = 6,
) -> list[dict[str, object]]:
    rows = list(raw_rows or [])[:limit]
    max_delta = max((abs(Decimal(str(item.get("delta_pct") or 0))) for item in rows), default=Decimal("0"))
    output: list[dict[str, object]] = []
    for item in rows:
        delta = Decimal(str(item.get("delta_pct") or 0))
        pct = float((abs(delta) / max_delta) * Decimal("100")) if max_delta > 0 else 0.0
        tone = str(item.get("tone") or "warning")
        output.append(
            {
                "label": str(item.get(label_key) or "Sin dato"),
                "secondary": str(item.get(secondary_key) or "") if secondary_key else "",
                "detail": str(item.get("detail") or ""),
                "status": str(item.get("status") or ""),
                "tone": tone,
                "delta_label": f"{delta:.1f}%",
                "value": Decimal(str(item.get(amount_key) or 0)),
                "pct": max(8.0, pct) if delta != 0 else 0.0,
            }
        )
    return output


def _build_dashboard_waste_snapshot() -> dict[str, object] | None:
    latest_date = MermaPOS.objects.order_by("-fecha").values_list("fecha", flat=True).first()
    if not latest_date:
        return None

    rows = MermaPOS.objects.filter(fecha=latest_date)
    totals = rows.aggregate(
        total_units=Sum("cantidad"),
        branch_count=Count("sucursal", distinct=True),
        recipe_count=Count("receta", distinct=True),
    )
    total_units = Decimal(str(totals.get("total_units") or 0))
    prev_date = MermaPOS.objects.filter(fecha__lt=latest_date).order_by("-fecha").values_list("fecha", flat=True).first()
    comparison_label = "Base inicial"
    comparison_tone = "warning"
    comparison_detail = "Aún no hay un corte previo de merma."
    if prev_date:
        prev_units = Decimal(
            str(
                MermaPOS.objects.filter(fecha=prev_date).aggregate(total=Sum("cantidad")).get("total")
                or 0
            )
        )
        if prev_units > 0:
            delta_pct = ((total_units - prev_units) / prev_units) * Decimal("100")
            comparison_label = "Sube" if delta_pct >= 0 else "Baja"
            comparison_tone = "warning" if delta_pct >= 0 else "success"
            comparison_detail = f"{abs(delta_pct):.1f}% vs {prev_date.isoformat()}"

    top_reasons = list(
        rows.exclude(motivo="")
        .values("motivo")
        .annotate(total=Sum("cantidad"))
        .order_by("-total", "motivo")[:3]
    )
    return {
        "date_label": latest_date.isoformat(),
        "total_units": total_units,
        "branch_count": int(totals.get("branch_count") or 0),
        "recipe_count": int(totals.get("recipe_count") or 0),
        "comparison_label": comparison_label,
        "comparison_tone": comparison_tone,
        "comparison_detail": comparison_detail,
        "top_reasons": top_reasons,
        "url": reverse("control:discrepancias"),
        "cta": "Abrir control",
    }


def _build_dashboard_purchase_snapshot() -> dict[str, object]:
    today = timezone.localdate()
    solicitudes_abiertas = SolicitudCompra.objects.exclude(estatus=SolicitudCompra.STATUS_RECHAZADA)
    ordenes_abiertas = OrdenCompra.objects.exclude(estatus=OrdenCompra.STATUS_CERRADA)
    recepciones_abiertas = RecepcionCompra.objects.exclude(estatus=RecepcionCompra.STATUS_CERRADA)
    solicitudes_vencidas = solicitudes_abiertas.filter(fecha_requerida__lt=today).count()
    ordenes_proximas = ordenes_abiertas.filter(
        fecha_entrega_estimada__isnull=False,
        fecha_entrega_estimada__lte=today + timedelta(days=3),
    ).count()
    recepciones_abiertas_count = recepciones_abiertas.count()
    return {
        "solicitudes_abiertas": solicitudes_abiertas.count(),
        "solicitudes_aprobadas": solicitudes_abiertas.filter(estatus=SolicitudCompra.STATUS_APROBADA).count(),
        "solicitudes_vencidas": solicitudes_vencidas,
        "ordenes_abiertas": ordenes_abiertas.count(),
        "ordenes_proximas": ordenes_proximas,
        "recepciones_abiertas": recepciones_abiertas_count,
        "status": "En seguimiento" if (solicitudes_vencidas or recepciones_abiertas_count) else "Controlado",
        "tone": "warning" if (solicitudes_vencidas or recepciones_abiertas_count) else "success",
        "detail": (
            "Hay documentos de compra que requieren cierre operativo hoy."
            if (solicitudes_vencidas or recepciones_abiertas_count)
            else "El flujo documental de compra está controlado para la jornada."
        ),
        "url": reverse("compras:solicitudes"),
        "cta": "Abrir compras",
    }


def _build_dashboard_production_snapshot() -> dict[str, object]:
    today = timezone.localdate()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)
    planes_semana = PlanProduccion.objects.filter(fecha_produccion__range=[week_start, week_end])
    planes_abiertos = planes_semana.exclude(estado=PlanProduccion.ESTADO_CERRADO)
    plan_hoy = PlanProduccion.objects.filter(fecha_produccion=today).order_by("-creado_en").first()
    solicitudes_activas = SolicitudVenta.objects.filter(
        fecha_inicio__lte=today,
        fecha_fin__gte=today,
    ).count()
    return {
        "planes_semana": planes_semana.count(),
        "planes_abiertos": planes_abiertos.count(),
        "plan_hoy_estado": plan_hoy.get_estado_display() if plan_hoy else "Sin plan",
        "plan_hoy_nombre": plan_hoy.nombre if plan_hoy else "Sin plan para hoy",
        "solicitudes_venta_activas": solicitudes_activas,
        "status": "En curso" if plan_hoy and plan_hoy.estado != PlanProduccion.ESTADO_CERRADO else ("Sin plan" if not plan_hoy else "Controlado"),
        "tone": "warning" if not plan_hoy or planes_abiertos.count() else "success",
        "detail": (
            "No hay un plan operativo cargado para hoy."
            if not plan_hoy
            else "Revisa avance del plan actual y su relación con la demanda."
        ),
        "url": reverse("recetas:plan_produccion"),
        "cta": "Abrir plan",
    }


def _build_dashboard_production_summary() -> dict[str, object]:
    today = timezone.localdate()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)
    plans = list(
        PlanProduccion.objects.filter(fecha_produccion__range=[week_start, week_end]).order_by("fecha_produccion", "id")
    )
    plan_ids = [int(plan.id) for plan in plans]
    items = list(
        PlanProduccionItem.objects.filter(plan_id__in=plan_ids)
        .select_related("receta", "plan")
        .order_by("plan__fecha_produccion", "receta__nombre")
    )

    total_units = Decimal("0")
    total_cost = Decimal("0")
    final_units = Decimal("0")
    sales_units = Decimal("0")
    produced_by_recipe: dict[int, dict[str, object]] = {}
    final_recipe_ids: set[int] = set()

    for item in items:
        qty = _to_decimal(item.cantidad)
        if qty <= 0:
            continue
        total_units += qty
        total_cost += _to_decimal(item.costo_total_estimado)
        bucket = produced_by_recipe.setdefault(
            int(item.receta_id),
            {
                "label": item.receta.nombre,
                "value": Decimal("0"),
                "cost": Decimal("0"),
            },
        )
        bucket["value"] = _to_decimal(bucket["value"]) + qty
        bucket["cost"] = _to_decimal(bucket["cost"]) + _to_decimal(item.costo_total_estimado)
        if item.receta.tipo == Receta.TIPO_PRODUCTO_FINAL:
            final_units += qty
            final_recipe_ids.add(int(item.receta_id))

    if final_recipe_ids:
        sales_units = _to_decimal(
            VentaHistorica.objects.filter(
                receta_id__in=final_recipe_ids,
                fecha__gte=week_start,
                fecha__lte=week_end,
            ).aggregate(total=Sum("cantidad")).get("total")
        )

    coverage_pct = None
    if sales_units > 0:
        coverage_pct = (final_units * Decimal("100")) / sales_units

    top_products = sorted(
        produced_by_recipe.values(),
        key=lambda row: (_to_decimal(row.get("value")), str(row.get("label") or "")),
        reverse=True,
    )[:5]

    status = "Sin producción"
    tone = "warning"
    detail = "No hay renglones de producción capturados para la semana en curso."
    if total_units > 0 and coverage_pct is not None:
        if coverage_pct >= Decimal("90"):
            status = "Cubre venta"
            tone = "success"
        elif coverage_pct >= Decimal("70"):
            status = "Cobertura ajustada"
            tone = "warning"
        else:
            status = "Producción corta"
            tone = "danger"
        detail = (
            f"Producción final equivalente a {final_units:.1f} u contra {sales_units:.1f} u vendidas en la misma semana."
        )
    elif total_units > 0:
        status = "Producción sin comparable"
        tone = "warning"
        detail = "Hay producción semanal, pero no existe venta final comparable suficiente para medir cobertura."

    return {
        "period_label": f"Semana {week_start.isoformat()} a {week_end.isoformat()}",
        "total_units": total_units,
        "total_cost": total_cost,
        "plan_count": len(plans),
        "open_plan_count": sum(1 for plan in plans if plan.estado != PlanProduccion.ESTADO_CERRADO),
        "final_units": final_units,
        "sales_units": sales_units,
        "coverage_pct": coverage_pct,
        "status": status,
        "tone": tone,
        "detail": detail,
        "top_products": top_products,
        "url": reverse("recetas:plan_produccion"),
        "cta": "Abrir producción",
        "conversion_note": "Conversión a enteros equivalentes pendiente de catálogo específico por presentación.",
    }


def _build_dashboard_waste_executive_summary() -> dict[str, object]:
    today = timezone.localdate()
    days_window = 7
    period_start = today - timedelta(days=days_window - 1)
    prev_start = period_start - timedelta(days=days_window)
    prev_end = period_start - timedelta(days=1)

    branch_rows_qs = MermaPOS.objects.filter(fecha__gte=period_start, fecha__lte=today)
    branch_rows = list(
        branch_rows_qs
        .select_related("receta", "sucursal")
        .order_by("-fecha", "-id")
    )
    prev_branch_units = _to_decimal(
        MermaPOS.objects.filter(fecha__gte=prev_start, fecha__lte=prev_end).aggregate(total=Sum("cantidad")).get("total")
    )

    branch_units = Decimal("0")
    branch_cost_est = Decimal("0")
    branch_cost_covered = 0
    branch_by_sucursal: dict[str, dict[str, object]] = {}
    for row in branch_rows:
        qty = _to_decimal(row.cantidad)
        branch_units += qty
        branch_code = row.sucursal.codigo if row.sucursal_id else "SIN SUCURSAL"
        branch_bucket = branch_by_sucursal.setdefault(
            branch_code,
            {
                "label": branch_code,
                "secondary": row.sucursal.nombre if row.sucursal_id else "Sin sucursal",
                "value": Decimal("0"),
            },
        )
        branch_bucket["value"] = _to_decimal(branch_bucket["value"]) + qty
        if row.receta_id:
            branch_cost_est += qty * _to_decimal(getattr(row.receta, "costo_total_estimado_decimal", 0))
            branch_cost_covered += 1

    cedis_rows = list(
        MovimientoInventario.objects.filter(
            fecha__date__gte=period_start,
            fecha__date__lte=today,
            tipo=MovimientoInventario.TIPO_CONSUMO,
            referencia__startswith="MERMA|",
        )
        .select_related("insumo")
        .order_by("-fecha", "-id")
    )
    prev_cedis_units = _to_decimal(
        MovimientoInventario.objects.filter(
            fecha__date__gte=prev_start,
            fecha__date__lte=prev_end,
            tipo=MovimientoInventario.TIPO_CONSUMO,
            referencia__startswith="MERMA|",
        ).aggregate(total=Sum("cantidad")).get("total")
    )

    cedis_units = Decimal("0")
    cedis_cost_est = Decimal("0")
    cedis_cost_covered = 0
    cedis_by_insumo: dict[str, dict[str, object]] = {}
    cost_cache: dict[int, Decimal | None] = {}
    for row in cedis_rows:
        qty = _to_decimal(row.cantidad)
        cedis_units += qty
        insumo_name = row.insumo.nombre if row.insumo_id else "Sin insumo"
        cedis_bucket = cedis_by_insumo.setdefault(
            insumo_name,
            {
                "label": insumo_name,
                "secondary": "Merma CEDIS",
                "value": Decimal("0"),
            },
        )
        cedis_bucket["value"] = _to_decimal(cedis_bucket["value"]) + qty
        if row.insumo_id:
            if int(row.insumo_id) not in cost_cache:
                cost_cache[int(row.insumo_id)] = latest_costo_canonico(insumo_id=int(row.insumo_id))
            unit_cost = cost_cache[int(row.insumo_id)]
            if unit_cost is not None:
                cedis_cost_est += qty * _to_decimal(unit_cost)
                cedis_cost_covered += 1

    total_units = branch_units + cedis_units
    prev_total_units = prev_branch_units + prev_cedis_units
    comparison_label = "Base inicial"
    comparison_tone = "warning"
    comparison_detail = "Aún no hay una ventana previa equivalente para merma."
    if prev_total_units > 0:
        delta_pct = ((total_units - prev_total_units) / prev_total_units) * Decimal("100")
        comparison_label = "Sube" if delta_pct >= 0 else "Baja"
        comparison_tone = "warning" if delta_pct >= 0 else "success"
        comparison_detail = f"{abs(delta_pct):.1f}% vs los 7 días previos ({prev_start.isoformat()} a {prev_end.isoformat()})"

    branch_rows_sorted = sorted(branch_by_sucursal.values(), key=lambda row: _to_decimal(row.get("value")), reverse=True)[:5]
    cedis_rows_sorted = sorted(cedis_by_insumo.values(), key=lambda row: _to_decimal(row.get("value")), reverse=True)[:5]

    return {
        "period_label": f"Últimos {days_window} días",
        "branch_available": branch_rows_qs.exists(),
        "branch_units": branch_units,
        "branch_cost_est": branch_cost_est,
        "branch_branch_count": len(branch_by_sucursal),
        "branch_cost_note": (
            f"Costo estimado en sucursal sobre {branch_cost_covered} capturas con receta mapeada."
            if branch_cost_covered
            else "Merma sucursal sin costo estimable: faltan recetas mapeadas."
        ),
        "cedis_units": cedis_units,
        "cedis_cost_est": cedis_cost_est,
        "cedis_row_count": len(cedis_rows),
        "cedis_cost_note": (
            f"Costo estimado en CEDIS sobre {cedis_cost_covered} movimientos con costo canónico."
            if cedis_cost_covered
            else "Merma CEDIS sin costo estimable: faltan costos canónicos para esos insumos."
        ),
        "comparison_label": comparison_label,
        "comparison_tone": comparison_tone,
        "comparison_detail": comparison_detail,
        "branch_rows": branch_rows_sorted,
        "cedis_rows": cedis_rows_sorted,
        "url": reverse("control:discrepancias"),
        "cta": "Abrir merma",
    }


def _build_dashboard_forecast_summary(periodo_mes: str | None = None) -> dict[str, object]:
    if not periodo_mes:
        today = timezone.localdate()
        periodo_mes = f"{today.year:04d}-{today.month:02d}"
    semaforo = _compute_plan_forecast_semaforo(periodo_mes)
    total_plan = _to_decimal(semaforo.get("total_plan"))
    total_pronostico = _to_decimal(semaforo.get("total_pronostico"))
    delta_total = _to_decimal(semaforo.get("delta_total"))
    deviation_pct = semaforo.get("desviacion_abs_pct")
    deviation_pct = _to_decimal(deviation_pct) if deviation_pct is not None else None
    top_rows = []
    for row in list(semaforo.get("rows_top") or [])[:5]:
        top_rows.append(
            {
                "label": row.get("receta") or "Receta",
                "value": abs(_to_decimal(row.get("delta"))),
                "secondary": (
                    f"Plan {_to_decimal(row.get('plan')):.1f} · Forecast {_to_decimal(row.get('pronostico')):.1f}"
                ),
                "tone": "danger" if _to_decimal(row.get("delta")) > 0 else "warning",
            }
        )

    detail = "El plan mensual está alineado con el forecast cargado."
    if semaforo.get("data_unavailable"):
        detail = "Faltan tablas o datos de forecast/plan para calcular la señal completa."
    elif semaforo.get("semaforo_label") == "Rojo":
        detail = "La desviación entre plan y forecast ya exige corrección ejecutiva."
    elif semaforo.get("semaforo_label") == "Amarillo":
        detail = "Hay desviación relevante entre plan y forecast y conviene ajustar volumen."

    return {
        "period_label": periodo_mes,
        "forecast_units": total_pronostico,
        "plan_units": total_plan,
        "delta_units": delta_total,
        "deviation_pct": deviation_pct,
        "status": semaforo.get("semaforo_label") or "Sin datos",
        "tone": (
            "success"
            if semaforo.get("semaforo_label") == "Verde"
            else "warning"
            if semaforo.get("semaforo_label") in {"Amarillo", "Sin datos"}
            else "danger"
        ),
        "detail": detail,
        "recipes_total": int(semaforo.get("recetas_total") or 0),
        "recipes_with_gap": int(semaforo.get("recetas_con_desviacion") or 0),
        "top_rows": top_rows,
        "url": reverse("recetas:plan_produccion"),
        "cta": "Abrir forecast",
        "basis_note": "Forecast mensual cargado en ERP. La exclusión automática de semanas atípicas aún no está parametrizada.",
    }


def _resolve_operational_plan() -> PlanProduccion | None:
    today = timezone.localdate()
    plan_hoy = PlanProduccion.objects.filter(fecha_produccion=today).order_by("-creado_en").first()
    if plan_hoy:
        return plan_hoy
    return (
        PlanProduccion.objects.exclude(estado=PlanProduccion.ESTADO_CERRADO)
        .filter(fecha_produccion__gte=today)
        .order_by("fecha_produccion", "-creado_en")
        .first()
    )


def _build_dashboard_supply_watchlist(limit: int = 6) -> dict[str, object] | None:
    plan = _resolve_operational_plan()
    if not plan:
        return None

    items = list(plan.items.select_related("receta")[:250])
    if not items:
        return None

    recipe_ids = [int(item.receta_id) for item in items if getattr(item, "receta_id", None)]
    lineas = list(
        LineaReceta.objects.filter(receta_id__in=recipe_ids, insumo_id__isnull=False)
        .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        .select_related("insumo", "receta", "insumo__unidad_base")
    )
    if not lineas:
        return None

    lineas_by_recipe: dict[int, list[LineaReceta]] = defaultdict(list)
    canonical_map: dict[int, Insumo] = {}
    canonical_ids: set[int] = set()
    for linea in lineas:
        if not linea.insumo:
            continue
        canonical = canonical_insumo_by_id(linea.insumo_id) or linea.insumo
        canonical_map[linea.id] = canonical
        canonical_ids.add(canonical.id)
        lineas_by_recipe[int(linea.receta_id)].append(linea)

    historico_map = {
        int(row["receta_id"]): Decimal(str(row["total"] or 0))
        for row in (
            VentaHistorica.objects.filter(
                receta_id__in=recipe_ids,
                fecha__gte=timezone.localdate() - timedelta(days=45),
            )
            .values("receta_id")
            .annotate(total=Sum("cantidad"))
        )
    }
    existencia_map = {
        int(existencia.insumo_id): existencia
        for existencia in ExistenciaInsumo.objects.filter(insumo_id__in=canonical_ids).select_related("insumo")
    }

    aggregated: dict[int, dict[str, object]] = {}
    for item in items:
        item_qty = Decimal(str(item.cantidad or 0))
        if item_qty <= 0:
            continue
        historico_units = historico_map.get(int(item.receta_id), Decimal("0"))
        for linea in lineas_by_recipe.get(int(item.receta_id), []):
            insumo = canonical_map.get(linea.id)
            if insumo is None:
                continue
            required_qty = Decimal(str(linea.cantidad or 0)) * item_qty
            if required_qty <= 0:
                continue
            bucket = aggregated.setdefault(
                insumo.id,
                {
                    "insumo": insumo,
                    "required_qty": Decimal("0"),
                    "historico_units": Decimal("0"),
                    "recipe_names": [],
                },
            )
            bucket["required_qty"] = Decimal(str(bucket["required_qty"])) + required_qty
            bucket["historico_units"] = Decimal(str(bucket["historico_units"])) + historico_units
            recipe_names = list(bucket["recipe_names"])
            if item.receta.nombre not in recipe_names:
                recipe_names.append(item.receta.nombre)
            bucket["recipe_names"] = recipe_names[:3]

    rows: list[dict[str, object]] = []
    for payload in aggregated.values():
        insumo = payload["insumo"]
        required_qty = Decimal(str(payload["required_qty"] or 0))
        historico_units = Decimal(str(payload["historico_units"] or 0))
        existencia = existencia_map.get(int(insumo.id))
        stock_actual = Decimal(str(getattr(existencia, "stock_actual", 0) or 0))
        shortage = max(required_qty - stock_actual, Decimal("0"))
        readiness = enterprise_readiness_profile(insumo)
        missing = list(readiness.get("missing") or [])
        missing_cost = latest_costo_canonico(insumo_id=insumo.id) is None
        if shortage <= 0 and not missing and not missing_cost:
            continue
        priority_score = (shortage * Decimal("100")) + (Decimal(str(len(missing))) * Decimal("50")) + historico_units
        if missing_cost:
            priority_score += Decimal("25")
        rows.append(
            {
                "insumo_nombre": insumo.nombre,
                "required_qty": required_qty,
                "stock_actual": stock_actual,
                "shortage": shortage,
                "historico_units": historico_units,
                "master_missing": missing,
                "missing_cost": missing_cost,
                "recipe_names": list(payload["recipe_names"] or []),
                "action_url": reverse("maestros:insumo_update", args=[insumo.id]),
                "action_label": "Asegurar artículo",
                "priority_score": priority_score,
            }
        )

    rows.sort(
        key=lambda item: (
            Decimal(str(item.get("priority_score") or 0)),
            Decimal(str(item.get("shortage") or 0)),
            Decimal(str(item.get("historico_units") or 0)),
        ),
        reverse=True,
    )
    if not rows:
        return None
    return {
        "plan_id": plan.id,
        "plan_nombre": plan.nombre,
        "plan_fecha": plan.fecha_produccion,
        "rows": rows[:limit],
        "url": f"{reverse('recetas:plan_produccion')}?plan_id={plan.id}",
        "cta": "Abrir plan",
    }


def _build_dashboard_daily_decisions(
    *,
    daily_sales_snapshot: dict[str, object] | None,
    branch_daily_exception_rows: list[dict[str, object]],
    branch_weekday_comparison_rows: list[dict[str, object]],
    product_daily_exception_rows: list[dict[str, object]],
    product_weekday_comparison_rows: list[dict[str, object]],
    purchase_snapshot: dict[str, object] | None,
    production_snapshot: dict[str, object] | None,
    waste_summary: dict[str, object] | None,
    forecast_summary: dict[str, object] | None,
    supply_watchlist: dict[str, object] | None,
    criticos_count: int,
    bajo_reorden_count: int,
    master_demand_critical_focus: dict[str, object] | None,
) -> list[dict[str, object]]:
    decisions: list[dict[str, object]] = []

    def push(priority: int, tone: str, title: str, detail: str, url: str, cta: str) -> None:
        decisions.append(
            {
                "priority": priority,
                "tone": tone,
                "title": title,
                "detail": detail,
                "url": url,
                "cta": cta,
            }
        )

    if master_demand_critical_focus and int(master_demand_critical_focus.get("count") or 0) > 0:
        push(
            100,
            str(master_demand_critical_focus.get("tone") or "danger"),
            "Cerrar artículo maestro crítico",
            str(master_demand_critical_focus.get("detail") or "Hay un artículo maestro frenando el troncal comercial."),
            str(master_demand_critical_focus.get("url") or reverse("maestros:insumo_list")),
            str(master_demand_critical_focus.get("cta") or "Abrir maestro"),
        )

    if supply_watchlist and list(supply_watchlist.get("rows") or []):
        top_supply = list(supply_watchlist.get("rows") or [])[0]
        missing = list(top_supply.get("master_missing") or [])
        if Decimal(str(top_supply.get("shortage") or 0)) > 0 or missing or bool(top_supply.get("missing_cost")):
            faltante = ", ".join(missing) if missing else ("costo pendiente" if top_supply.get("missing_cost") else "stock corto")
            push(
                98,
                "danger" if Decimal(str(top_supply.get("shortage") or 0)) > 0 else "warning",
                "Asegurar insumo del plan",
                (
                    f"{top_supply.get('insumo_nombre', 'Artículo')} exige cierre inmediato para "
                    f"{supply_watchlist.get('plan_nombre', 'el plan operativo')}: brecha {Decimal(str(top_supply.get('shortage') or 0)):.2f} "
                    f"y faltante {faltante}."
                ),
                str(top_supply.get("action_url") or supply_watchlist.get("url") or reverse("inventario:alertas")),
                str(top_supply.get("action_label") or "Asegurar artículo"),
            )

    if forecast_summary and str(forecast_summary.get("status") or "") in {"Rojo", "Amarillo"}:
        deviation_pct = forecast_summary.get("deviation_pct")
        deviation_label = f"{_to_decimal(deviation_pct):.1f}%" if deviation_pct is not None else "sin %"
        push(
            96,
            "danger" if str(forecast_summary.get("status")) == "Rojo" else "warning",
            "Corregir forecast contra plan",
            (
                f"El forecast del periodo {forecast_summary.get('period_label')} está en {forecast_summary.get('status')}. "
                f"Desviación {deviation_label} entre plan y forecast."
            ),
            str(forecast_summary.get("url") or reverse("recetas:plan_produccion")),
            str(forecast_summary.get("cta") or "Abrir forecast"),
        )

    if waste_summary and _to_decimal(waste_summary.get("branch_units")) + _to_decimal(waste_summary.get("cedis_units")) > 0:
        push(
            90,
            "warning" if str(waste_summary.get("comparison_label") or "") == "Sube" else "success",
            "Revisar merma semanal",
            (
                f"Merma sucursal {_to_decimal(waste_summary.get('branch_units')):.1f} u "
                f"y CEDIS {_to_decimal(waste_summary.get('cedis_units')):.1f} u. "
                f"{waste_summary.get('comparison_detail')}"
            ),
            str(waste_summary.get("url") or reverse("control:discrepancias")),
            str(waste_summary.get("cta") or "Abrir merma"),
        )

    if criticos_count > 0:
        push(
            95,
            "danger",
            "Atender stock crítico",
            f"Hay {criticos_count} insumos en crítico y {bajo_reorden_count} bajo reorden. Valida cobertura antes de liberar producción o compras.",
            reverse("inventario:alertas"),
            "Abrir alertas",
        )

    if purchase_snapshot and int(purchase_snapshot.get("solicitudes_vencidas") or 0) > 0:
        vencidas = int(purchase_snapshot.get("solicitudes_vencidas") or 0)
        push(
            90,
            "danger",
            "Liberar solicitudes vencidas",
            f"Hay {vencidas} solicitudes vencidas que ya deberían estar resueltas o escaladas con proveedor.",
            str(purchase_snapshot.get("url") or reverse("compras:solicitudes")),
            "Abrir compras",
        )

    if production_snapshot and str(production_snapshot.get("plan_hoy_estado") or "") == "Sin plan":
        push(
            88,
            "danger",
            "Confirmar plan de producción de hoy",
            "No hay plan operativo cargado para hoy. Define el plan antes de empujar compras o reabasto.",
            str(production_snapshot.get("url") or reverse("recetas:plan_produccion")),
            str(production_snapshot.get("cta") or "Abrir plan"),
        )

    top_branch = branch_weekday_comparison_rows[0] if branch_weekday_comparison_rows else (branch_daily_exception_rows[0] if branch_daily_exception_rows else None)
    if top_branch and str(top_branch.get("tone") or "") in {"danger", "success"}:
        branch_name = str(top_branch.get("branch_name") or top_branch.get("branch_code") or "Sucursal")
        branch_status = str(top_branch.get("status") or "Variación")
        push(
            80 if str(top_branch.get("tone")) == "danger" else 55,
            str(top_branch.get("tone") or "warning"),
            f"Revisar sucursal {branch_name}",
            f"{branch_status}. {top_branch.get('detail') or ''}".strip(),
            reverse("reportes:bi"),
            "Abrir BI",
        )

    top_product = product_weekday_comparison_rows[0] if product_weekday_comparison_rows else (product_daily_exception_rows[0] if product_daily_exception_rows else None)
    if top_product and str(top_product.get("tone") or "") in {"danger", "success"}:
        product_name = str(top_product.get("recipe_name") or "Producto")
        product_status = str(top_product.get("status") or "Variación")
        push(
            78 if str(top_product.get("tone")) == "danger" else 52,
            str(top_product.get("tone") or "warning"),
            f"Revisar producto {product_name}",
            f"{product_status}. {top_product.get('detail') or ''}".strip(),
            reverse("reportes:bi"),
            "Abrir BI",
        )

    if daily_sales_snapshot and str(daily_sales_snapshot.get("comparison_label") or "") == "Abajo":
        push(
            70,
            str(daily_sales_snapshot.get("comparison_tone") or "warning"),
            "Validar caída del corte reciente",
            str(daily_sales_snapshot.get("comparison_detail") or "La venta del último corte viene abajo contra el previo."),
            str(daily_sales_snapshot.get("url") or reverse("reportes:bi")),
            str(daily_sales_snapshot.get("cta") or "Abrir BI"),
        )

    if not decisions:
        push(
            10,
            "success",
            "Operación estable",
            "No hay alertas dominantes en ventas, stock, compras o producción para el corte reciente.",
            reverse("dashboard"),
            "Actualizar tablero",
        )

    decisions.sort(key=lambda item: int(item.get("priority") or 0), reverse=True)
    return decisions[:5]


def login_view(request: HttpRequest) -> HttpResponse:
    if request.method == "POST":
        username = request.POST.get("username", "")
        password = request.POST.get("password", "")
        
        logger.info(f"Login attempt: username={username}")
        
        if username and password:
            user = authenticate(request, username=username, password=password)
            logger.info(f"Authentication result: user={user}")
            if user is not None and user.is_active:
                login(request, user)
                logger.info(f"Login successful for user={username}")
                if is_branch_capture_only(user):
                    return _redirect_capture_module()
                return redirect("dashboard")
            else:
                logger.warning(f"Authentication failed for username={username}")
        else:
            logger.warning(f"Missing username or password")
        
        return render(request, "core/login.html", {"error": "Credenciales inválidas"})
    
    return render(request, "core/login.html")

def logout_view(request: HttpRequest) -> HttpResponse:
    logout(request)
    return redirect("login")


def home_redirect(request: HttpRequest) -> HttpResponse:
    if request.user.is_authenticated:
        if is_branch_capture_only(request.user):
            return _redirect_capture_module()
        return redirect("dashboard")
    return redirect("login")


def dashboard(request: HttpRequest) -> HttpResponse:
    if not request.user.is_authenticated:
        return redirect("/login/")
    if is_branch_capture_only(request.user):
        return _redirect_capture_module()

    u = request.user
    ctx = {
        "can_view_recetas": False,
        "can_import": False,
        "can_review_matching": False,
        "can_view_crm": False,
        "can_manage_crm": False,
        "can_view_logistica": False,
        "can_manage_logistica": False,
        "can_view_rrhh": False,
        "can_manage_rrhh": False,
        "insumos_count": 0,
        "recetas_count": 0,
        "proveedores_count": 0,
        "alertas_count": 0,
        "criticos_count": 0,
        "bajo_reorden_count": 0,
        "latest_almacen_sync": None,
        "auto_sync_enabled": False,
        "auto_sync_interval_hours": 24,
        "next_sync_eta": None,
        "next_sync_state_label": "",
        "next_sync_state_class": "bg-warning",
        "budget_semaforo_mes": None,
        "budget_semaforo_quincena": None,
        "budget_alerts_active": 0,
        "latest_budget_alert": None,
        "plan_forecast_semaforo": None,
        "point_pending_total": 0,
        "point_pending_insumos": 0,
        "point_pending_productos": 0,
        "point_pending_proveedores": 0,
        "recetas_pending_matching_count": 0,
        "inventario_last_unmatched_count": 0,
        "homologacion_total_pending": 0,
        "users_governance_summary": [],
        "users_coverage_summary": [],
        "activos_governance_summary": [],
        "master_governance_summary": [],
        "master_demand_priority_rows": [],
        "recipe_governance_summary": [],
        "sales_history_summary": None,
        "daily_sales_snapshot": None,
        "production_summary": None,
        "waste_executive_summary": None,
        "forecast_summary": None,
        "branch_daily_exception_rows": [],
        "branch_weekday_comparison_rows": [],
        "product_daily_exception_rows": [],
        "product_weekday_comparison_rows": [],
        "supply_watchlist": None,
        "daily_decision_rows": [],
        "purchase_snapshot": None,
        "production_snapshot": None,
        "dashboard_exec_ready": False,
    }
    ctx.update(
        {
            "can_view_recetas": u.has_perm("recetas.view_receta"),
            "can_import": u.is_superuser or u.groups.filter(name__in=["ADMIN", "COMPRAS"]).exists(),
            "can_review_matching": u.is_superuser or u.groups.filter(name__in=["ADMIN"]).exists(),
            "can_view_maestros": can_view_maestros(u),
            "can_view_recetas": can_view_recetas(u),
            "can_view_compras": can_view_compras(u),
            "can_manage_compras": can_manage_compras(u),
            "can_view_crm": can_view_crm(u),
            "can_manage_crm": can_manage_crm(u),
            "can_view_logistica": can_view_logistica(u),
            "can_manage_logistica": can_manage_logistica(u),
            "can_view_rrhh": can_view_rrhh(u),
            "can_manage_rrhh": can_manage_rrhh(u),
            "can_view_inventario": can_view_inventario(u),
            "can_manage_inventario": can_manage_inventario(u),
            "can_view_reportes": can_view_reportes(u),
        }
    )

    inventory_metrics = None
    inventario_last_unmatched_count = 0
    point_pending_total = 0
    recetas_pending_matching_count = 0

    try:
        inventory_metrics = _build_canonical_inventory_dashboard_metrics()
        ctx.update(
            {
                "insumos_count": inventory_metrics["insumos_count"],
                "recetas_count": Receta.objects.count(),
                "proveedores_count": Proveedor.objects.count(),
                "alertas_count": inventory_metrics["alertas_count"],
                "criticos_count": inventory_metrics["criticos_count"],
                "bajo_reorden_count": inventory_metrics["bajo_reorden_count"],
                "inventario_total_count": inventory_metrics["inventario_total_count"],
                "stock_min_config_count": inventory_metrics["stock_min_config_count"],
                "stock_max_config_count": inventory_metrics["stock_max_config_count"],
                "inv_prom_config_count": inventory_metrics["inv_prom_config_count"],
                "punto_reorden_config_count": inventory_metrics["punto_reorden_config_count"],
                "stock_bajo_min_count": inventory_metrics["stock_bajo_min_count"],
                "stock_sobre_max_count": inventory_metrics["stock_sobre_max_count"],
                "lead_time_risk_count": inventory_metrics["lead_time_risk_count"],
                "avg_dias_llegada": inventory_metrics["avg_dias_llegada"],
                "avg_consumo_diario": inventory_metrics["avg_consumo_diario"],
                "total_consumo_diario": inventory_metrics["total_consumo_diario"],
                "cobertura_promedio_dias": inventory_metrics["cobertura_promedio_dias"],
            }
        )
    except Exception:
        logger.exception("Dashboard inventory metrics failed")
        try:
            ctx["recetas_count"] = Receta.objects.count()
            ctx["proveedores_count"] = Proveedor.objects.count()
        except Exception:
            logger.exception("Dashboard fallback recipe/provider counts failed")

    try:
        latest_sync = (
            AlmacenSyncRun.objects.select_related("triggered_by")
            .order_by("-started_at", "-id")
            .first()
        )
        ctx["latest_almacen_sync"] = latest_sync
        inventario_last_unmatched_count = int(latest_sync.unmatched or 0) if latest_sync else 0
        point_pending_insumos = PointPendingMatch.qs_operativos().filter(tipo=PointPendingMatch.TIPO_INSUMO).count()
        point_pending_productos = PointPendingMatch.qs_operativos().filter(tipo=PointPendingMatch.TIPO_PRODUCTO).count()
        point_pending_proveedores = PointPendingMatch.qs_operativos().filter(tipo=PointPendingMatch.TIPO_PROVEEDOR).count()
        point_pending_total = point_pending_insumos + point_pending_productos + point_pending_proveedores
        recetas_pending_matching_count = (
            LineaReceta.objects.exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
            .filter(match_status__in=[LineaReceta.STATUS_NEEDS_REVIEW, LineaReceta.STATUS_REJECTED])
            .count()
        )
        ctx.update(
            {
                "point_pending_total": point_pending_total,
                "point_pending_insumos": point_pending_insumos,
                "point_pending_productos": point_pending_productos,
                "point_pending_proveedores": point_pending_proveedores,
                "recetas_pending_matching_count": recetas_pending_matching_count,
                "inventario_last_unmatched_count": inventario_last_unmatched_count,
                "homologacion_total_pending": (
                    point_pending_total + recetas_pending_matching_count + inventario_last_unmatched_count
                ),
            }
        )
    except Exception:
        logger.exception("Dashboard reconciliation summary failed")

    try:
        auto_sync_enabled = os.getenv("ENABLE_AUTO_SYNC_ALMACEN", "0").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        try:
            auto_sync_interval_hours = int((os.getenv("AUTO_SYNC_INTERVAL_HOURS", "24") or "24").strip())
        except ValueError:
            auto_sync_interval_hours = 24
        auto_sync_interval_hours = max(auto_sync_interval_hours, 1)

        next_sync_eta = None
        next_sync_state_label = "Pendiente"
        next_sync_state_class = "bg-warning"
        if auto_sync_enabled:
            latest_scheduled = (
                AlmacenSyncRun.objects.filter(source=AlmacenSyncRun.SOURCE_SCHEDULED)
                .order_by("-started_at", "-id")
                .first()
            )
            if latest_scheduled:
                delta = timedelta(hours=auto_sync_interval_hours)
                next_sync_eta = latest_scheduled.started_at + delta
                now = timezone.now()
                while next_sync_eta <= now:
                    next_sync_eta += delta
                hours_to_next = (next_sync_eta - now).total_seconds() / 3600
                if hours_to_next <= 2:
                    next_sync_state_label = "Próximo"
                    next_sync_state_class = "bg-danger"
                elif hours_to_next <= 8:
                    next_sync_state_label = "Hoy"
                    next_sync_state_class = "bg-warning"
                else:
                    next_sync_state_label = "Programado"
                    next_sync_state_class = "bg-success"
            else:
                next_sync_state_label = "Pendiente"
                next_sync_state_class = "bg-warning"
        else:
            next_sync_state_label = "Desactivado"
            next_sync_state_class = "bg-danger"

        ctx.update(
            {
                "auto_sync_enabled": auto_sync_enabled,
                "auto_sync_interval_hours": auto_sync_interval_hours,
                "next_sync_eta": next_sync_eta,
                "next_sync_state_label": next_sync_state_label,
                "next_sync_state_class": next_sync_state_class,
            }
        )
    except Exception:
        logger.exception("Dashboard sync status failed")

    compras_documental_total = 0
    try:
        today = timezone.localdate()
        periodo_mes = f"{today.year:04d}-{today.month:02d}"
        periodo_quincena = "q1" if today.day <= 15 else "q2"
        semaforo_mes = _compute_budget_semaforo("mes", periodo_mes)
        semaforo_quincena = _compute_budget_semaforo(periodo_quincena, periodo_mes)

        for semaforo in (semaforo_mes, semaforo_quincena):
            if semaforo["sobre_objetivo_estimado"]:
                _log_budget_alert_once(semaforo, "ESTIMADO")
            if semaforo["sobre_objetivo_ejecutado"]:
                _log_budget_alert_once(semaforo, "EJECUTADO")

        compras_documental_total = sum(
            1
            for s in (semaforo_mes, semaforo_quincena)
            if s["sobre_objetivo_estimado"] or s["sobre_objetivo_ejecutado"]
        )
        ctx.update(
            {
                "budget_semaforo_mes": semaforo_mes,
                "budget_semaforo_quincena": semaforo_quincena,
                "budget_alerts_active": compras_documental_total,
                "latest_budget_alert": AuditLog.objects.filter(
                    action="ALERT",
                    model="compras.PresupuestoCompraPeriodo",
                ).first(),
                "plan_forecast_semaforo": _compute_plan_forecast_semaforo(periodo_mes),
            }
        )
    except Exception:
        logger.exception("Dashboard budget and forecast failed")

    try:
        user_model = get_user_model()
        users_governance = {
            "listos": 0,
            "bloqueados": 0,
            "captura_sin_sucursal": 0,
            "sin_departamento": 0,
        }
        for system_user in user_model.objects.select_related("userprofile").prefetch_related("groups")[:500]:
            profile = getattr(system_user, "userprofile", None)
            scope = _user_access_scope(system_user, profile)
            row = {
                "role": primary_role(system_user),
                "is_active": bool(system_user.is_active),
                "departamento_id": profile.departamento_id if profile else None,
                "sucursal_id": profile.sucursal_id if profile else None,
                "modo_captura_sucursal": bool(profile.modo_captura_sucursal) if profile else False,
                "locks_count": sum(1 for key, _ in LOCK_FIELDS if profile and bool(getattr(profile, key, False))),
                "visible_modules": scope["visible_modules"],
                "manageable_modules": scope["manageable_modules"],
                "blocked_modules": scope["blocked_modules"],
            }
            enterprise = _user_enterprise_profile(system_user, profile, row)
            if enterprise["status_label"] == "Lista para operar":
                users_governance["listos"] += 1
            elif enterprise["status_label"] == "Bloqueado":
                users_governance["bloqueados"] += 1
            if "CAPTURA_SIN_SUCURSAL" in enterprise["blocker_codes"]:
                users_governance["captura_sin_sucursal"] += 1
            if "SIN_DEPARTAMENTO" in enterprise["blocker_codes"]:
                users_governance["sin_departamento"] += 1

        dashboard_user_rows = [
            {
                "role": primary_role(system_user),
                "is_active": bool(system_user.is_active),
                "departamento_id": getattr(getattr(system_user, "userprofile", None), "departamento_id", None),
                "sucursal_id": getattr(getattr(system_user, "userprofile", None), "sucursal_id", None),
                "modo_captura_sucursal": bool(getattr(getattr(system_user, "userprofile", None), "modo_captura_sucursal", False)),
            }
            for system_user in user_model.objects.select_related("userprofile").prefetch_related("groups")[:500]
        ]
        coverage = _operational_coverage_summary(
            dashboard_user_rows,
            list(sucursales_operativas().order_by("codigo")),
            list(Departamento.objects.order_by("nombre")),
        )
        today = timezone.localdate()
        activos_governance = {
            "criticos_fuera_servicio": Activo.objects.filter(
                activo=True,
                estado=Activo.ESTADO_FUERA_SERVICIO,
                criticidad=Activo.CRITICIDAD_ALTA,
            ).count(),
            "planes_vencidos": PlanMantenimiento.objects.filter(
                activo=True,
                estatus=PlanMantenimiento.ESTATUS_ACTIVO,
                proxima_ejecucion__lt=today,
            ).count(),
            "ordenes_criticas_abiertas": OrdenMantenimiento.objects.filter(
                prioridad=OrdenMantenimiento.PRIORIDAD_CRITICA,
            ).exclude(estatus__in=[OrdenMantenimiento.ESTATUS_CERRADA, OrdenMantenimiento.ESTATUS_CANCELADA]).count(),
            "activos_sin_plan": Activo.objects.filter(activo=True).exclude(
                id__in=PlanMantenimiento.objects.filter(activo=True).values_list("activo_ref_id", flat=True)
            ).count(),
        }

        ctx["users_governance_summary"] = [
            {"label": "Usuarios listos ERP", "value": users_governance["listos"], "tone": "success", "url": reverse("users_access")},
            {"label": "Usuarios bloqueados", "value": users_governance["bloqueados"], "tone": "danger", "url": f"{reverse('users_access')}?enterprise_gap=SIN_DEPARTAMENTO"},
            {"label": "Captura sin sucursal", "value": users_governance["captura_sin_sucursal"], "tone": "warning", "url": f"{reverse('users_access')}?enterprise_gap=CAPTURA_SIN_SUCURSAL"},
            {"label": "Usuarios sin departamento", "value": users_governance["sin_departamento"], "tone": "warning", "url": f"{reverse('users_access')}?enterprise_gap=SIN_DEPARTAMENTO"},
        ]
        ctx["users_coverage_summary"] = [
            {"label": "Sucursales cubiertas", "value": coverage["sucursales_ok"], "tone": "success", "url": reverse("users_access")},
            {"label": "Sucursales con gap", "value": coverage["sucursales_gap"], "tone": "warning", "url": f"{reverse('users_access')}?coverage=sucursal"},
            {"label": "Áreas cubiertas", "value": coverage["departamentos_ok"], "tone": "success", "url": reverse("users_access")},
            {"label": "Áreas sin responsable", "value": coverage["departamentos_gap"], "tone": "warning", "url": f"{reverse('users_access')}?coverage=departamento"},
        ]
        ctx["activos_governance_summary"] = [
            {"label": "Activos críticos fuera de servicio", "value": activos_governance["criticos_fuera_servicio"], "tone": "danger", "url": f"{reverse('activos:activos')}?master_gap=FUERA_SERVICIO_CRITICO"},
            {"label": "Planes vencidos", "value": activos_governance["planes_vencidos"], "tone": "warning", "url": f"{reverse('activos:planes')}?enterprise_gap=VENCIDO"},
            {"label": "Órdenes críticas abiertas", "value": activos_governance["ordenes_criticas_abiertas"], "tone": "danger", "url": f"{reverse('activos:ordenes')}?enterprise_gap=ABIERTA_CRITICA"},
            {"label": "Activos sin plan", "value": activos_governance["activos_sin_plan"], "tone": "warning", "url": f"{reverse('activos:activos')}?master_gap=SIN_PLAN"},
        ]
    except Exception:
        logger.exception("Dashboard governance summary failed")

    try:
        ctx["master_governance_summary"] = _build_dashboard_master_governance()
        ctx["master_demand_priority_rows"] = _build_dashboard_master_demand_priority()
        ctx["master_demand_critical_queue"] = _build_dashboard_master_demand_critical_queue()
        ctx["master_demand_critical_focus"] = _build_dashboard_master_demand_critical_focus()
        ctx["recipe_governance_summary"] = _build_dashboard_recipe_governance()
    except Exception:
        logger.exception("Dashboard master/recipe governance failed")

    try:
        ctx["sales_history_summary"] = _build_dashboard_sales_history_summary()
    except Exception:
        logger.exception("Dashboard sales history summary failed")

    try:
        ctx["monthly_sales_rows"] = _dashboard_monthly_sales_rows()
    except Exception:
        logger.exception("Dashboard monthly sales rows failed")

    try:
        ctx["daily_sales_snapshot"] = _build_dashboard_daily_sales_snapshot()
    except Exception:
        logger.exception("Dashboard daily sales snapshot failed")

    try:
        snapshot = ctx.get("daily_sales_snapshot") or {}
        ctx["sales_branch_bar_rows"] = _dashboard_bar_rows(
            list(snapshot.get("top_branches") or []),
            label_key="label",
            secondary_key="secondary",
            value_key="amount",
        )
        ctx["sales_product_bar_rows"] = _dashboard_bar_rows(
            list(snapshot.get("top_products") or []),
            label_key="label",
            value_key="amount",
        )
    except Exception:
        logger.exception("Dashboard ranking bars failed")

    try:
        ctx["branch_daily_exception_rows"] = _build_dashboard_branch_daily_exceptions()
    except Exception:
        logger.exception("Dashboard branch daily exceptions failed")
    try:
        ctx["branch_daily_exception_bar_rows"] = _dashboard_comparison_bar_rows(
            list(ctx.get("branch_daily_exception_rows") or []),
            label_key="branch_code",
            secondary_key="branch_name",
        )
    except Exception:
        logger.exception("Dashboard branch daily exception bars failed")

    try:
        ctx["branch_weekday_comparison_rows"] = _build_dashboard_branch_weekday_comparisons()
    except Exception:
        logger.exception("Dashboard branch weekday comparisons failed")
    try:
        ctx["branch_weekday_comparison_bar_rows"] = _dashboard_comparison_bar_rows(
            list(ctx.get("branch_weekday_comparison_rows") or []),
            label_key="branch_code",
            secondary_key="branch_name",
        )
    except Exception:
        logger.exception("Dashboard branch weekday bars failed")

    try:
        ctx["product_daily_exception_rows"] = _build_dashboard_product_daily_exceptions()
    except Exception:
        logger.exception("Dashboard product daily exceptions failed")
    try:
        ctx["product_daily_exception_bar_rows"] = _dashboard_comparison_bar_rows(
            list(ctx.get("product_daily_exception_rows") or []),
            label_key="recipe_name",
        )
    except Exception:
        logger.exception("Dashboard product daily exception bars failed")

    try:
        ctx["product_weekday_comparison_rows"] = _build_dashboard_product_weekday_comparisons()
    except Exception:
        logger.exception("Dashboard product weekday comparisons failed")
    try:
        ctx["product_weekday_comparison_bar_rows"] = _dashboard_comparison_bar_rows(
            list(ctx.get("product_weekday_comparison_rows") or []),
            label_key="recipe_name",
        )
    except Exception:
        logger.exception("Dashboard product weekday bars failed")

    try:
        ctx["waste_snapshot"] = _build_dashboard_waste_snapshot()
    except Exception:
        logger.exception("Dashboard waste snapshot failed")

    try:
        ctx["purchase_snapshot"] = _build_dashboard_purchase_snapshot()
    except Exception:
        logger.exception("Dashboard purchase snapshot failed")

    try:
        ctx["production_snapshot"] = _build_dashboard_production_snapshot()
    except Exception:
        logger.exception("Dashboard production snapshot failed")

    try:
        ctx["production_summary"] = _build_dashboard_production_summary()
    except Exception:
        logger.exception("Dashboard production summary failed")

    try:
        ctx["waste_executive_summary"] = _build_dashboard_waste_executive_summary()
    except Exception:
        logger.exception("Dashboard waste executive summary failed")

    try:
        today = timezone.localdate()
        ctx["forecast_summary"] = _build_dashboard_forecast_summary(f"{today.year:04d}-{today.month:02d}")
    except Exception:
        logger.exception("Dashboard forecast summary failed")

    try:
        ctx["supply_watchlist"] = _build_dashboard_supply_watchlist()
    except Exception:
        logger.exception("Dashboard supply watchlist failed")

    try:
        production_summary = ctx.get("production_summary") or {}
        ctx["production_product_bar_rows"] = _dashboard_bar_rows(
            list(production_summary.get("top_products") or []),
            label_key="label",
            value_key="value",
        )
    except Exception:
        logger.exception("Dashboard production product bars failed")

    try:
        waste_summary = ctx.get("waste_executive_summary") or {}
        ctx["waste_branch_bar_rows"] = _dashboard_bar_rows(
            list(waste_summary.get("branch_rows") or []),
            label_key="label",
            secondary_key="secondary",
            value_key="value",
        )
        ctx["waste_cedis_bar_rows"] = _dashboard_bar_rows(
            list(waste_summary.get("cedis_rows") or []),
            label_key="label",
            secondary_key="secondary",
            value_key="value",
        )
    except Exception:
        logger.exception("Dashboard waste bars failed")

    try:
        forecast_summary = ctx.get("forecast_summary") or {}
        ctx["forecast_gap_bar_rows"] = _dashboard_bar_rows(
            list(forecast_summary.get("top_rows") or []),
            label_key="label",
            secondary_key="secondary",
            value_key="value",
        )
    except Exception:
        logger.exception("Dashboard forecast bars failed")

    try:
        ctx["daily_decision_rows"] = _build_dashboard_daily_decisions(
            daily_sales_snapshot=ctx.get("daily_sales_snapshot"),
            branch_daily_exception_rows=list(ctx.get("branch_daily_exception_rows") or []),
            branch_weekday_comparison_rows=list(ctx.get("branch_weekday_comparison_rows") or []),
            product_daily_exception_rows=list(ctx.get("product_daily_exception_rows") or []),
            product_weekday_comparison_rows=list(ctx.get("product_weekday_comparison_rows") or []),
            purchase_snapshot=ctx.get("purchase_snapshot"),
            production_snapshot=ctx.get("production_snapshot"),
            waste_summary=ctx.get("waste_executive_summary"),
            forecast_summary=ctx.get("forecast_summary"),
            supply_watchlist=ctx.get("supply_watchlist"),
            criticos_count=int(ctx.get("criticos_count") or 0),
            bajo_reorden_count=int(ctx.get("bajo_reorden_count") or 0),
            master_demand_critical_focus=ctx.get("master_demand_critical_focus"),
        )
    except Exception:
        logger.exception("Dashboard daily decisions failed")

    if ctx.get("can_view_reportes"):
        try:
            executive_panels = build_executive_bi_panels()
            ctx.update(
                {
                    "executive_panels": executive_panels,
                    "forecast_panel": executive_panels["forecast_panel"],
                    "yoy_panel": executive_panels["yoy_panel"],
                    "profitability_panel": executive_panels["profitability_panel"],
                    "production_sales_panel": executive_panels["production_sales_panel"],
                    "inventory_ledger_panel": executive_panels["inventory_ledger_panel"],
                    "dashboard_exec_ready": True,
                }
            )
        except Exception:
            logger.exception("Dashboard executive panels failed")

    try:
        master_pending_total = sum(int(item.get("value") or 0) for item in ctx["master_governance_summary"])
        recipe_pending_total = sum(int(item.get("value") or 0) for item in ctx["recipe_governance_summary"])
        users_pending_total = sum(int(item.get("value") or 0) for item in ctx["users_governance_summary"])
        users_coverage_gap_total = sum(int(item.get("value") or 0) for item in ctx["users_coverage_summary"])
        activos_pending_total = sum(int(item.get("value") or 0) for item in ctx["activos_governance_summary"])
        rrhh_pending_total = (
            Empleado.objects.filter(activo=False).count()
            + NominaPeriodo.objects.filter(estatus=NominaPeriodo.ESTATUS_BORRADOR).count()
        )
        crm_pending_total = PedidoCliente.objects.exclude(
            estatus__in=[PedidoCliente.ESTATUS_ENTREGADO, PedidoCliente.ESTATUS_CANCELADO]
        ).count()
        control_pending_total = (
            VentaPOS.objects.filter(fecha=timezone.localdate()).count()
            + MermaPOS.objects.filter(fecha=timezone.localdate()).count()
        )
        logistica_pending_total = (
            RutaEntrega.objects.filter(estatus__in=[RutaEntrega.ESTATUS_PLANEADA, RutaEntrega.ESTATUS_EN_RUTA]).count()
            + EntregaRuta.objects.filter(
                estatus__in=[EntregaRuta.ESTATUS_PENDIENTE, EntregaRuta.ESTATUS_INCIDENCIA]
            ).count()
        )
        inventario_pending_total = int((inventory_metrics or {}).get("alertas_count") or 0) + int(inventario_last_unmatched_count or 0)
        integration_pending_total = point_pending_total + recetas_pending_matching_count + inventario_last_unmatched_count
        reportes_risk_total = master_pending_total + recipe_pending_total + inventario_pending_total
        ctx["erp_cockpit_summary"] = [
            {
                "title": "Maestro ERP",
                "value": master_pending_total,
                "tone": "danger" if master_pending_total else "success",
                "detail": "Artículos incompletos, duplicados o con faltantes que bloquean operación.",
                "cta": "Abrir maestro",
                "url": reverse("maestros:insumo_list"),
            },
            {
                "title": "Recetas y BOM",
                "value": recipe_pending_total,
                "tone": "warning" if recipe_pending_total else "success",
                "detail": "Brechas de estructura, empaques, rendimiento o artículos ERP por resolver.",
                "cta": "Abrir recetas",
                "url": reverse("recetas:recetas_list"),
            },
            {
                "title": "Compras documentales",
                "value": compras_documental_total,
                "tone": "warning" if compras_documental_total else "success",
                "detail": "Desviaciones y alertas activas del flujo plan → solicitud → orden → recepción.",
                "cta": "Abrir compras",
                "url": reverse("compras:solicitudes"),
            },
            {
                "title": "Inventario y conciliación",
                "value": inventario_pending_total,
                "tone": "danger" if inventario_pending_total else "success",
                "detail": "Alertas de stock y registros pendientes de conciliación operativa.",
                "cta": "Abrir inventario",
                "url": reverse("inventario:existencias"),
            },
        ]
        ctx["erp_extended_module_cards"] = [
            {
                "module": "Usuarios y Accesos",
                "owner": "RH / Administración",
                "count": users_pending_total + users_coverage_gap_total,
                "tone": "success" if (users_pending_total + users_coverage_gap_total) == 0 else "warning",
                "status": "Controlado" if (users_pending_total + users_coverage_gap_total) == 0 else "En seguimiento",
                "detail": (
                    "Usuarios, cobertura y candados operan sin brechas visibles."
                    if (users_pending_total + users_coverage_gap_total) == 0
                    else "Persisten brechas de habilitación, cobertura o segregación por área."
                ),
                "next_step": (
                    "Mantener disciplina de altas y RBAC."
                    if (users_pending_total + users_coverage_gap_total) == 0
                    else "Cerrar usuarios bloqueados y gaps de cobertura."
                ),
                "url": reverse("users_access"),
                "cta": "Abrir usuarios",
            },
            {
                "module": "Activos",
                "owner": "Mantenimiento / Operaciones",
                "count": activos_pending_total,
                "tone": "success" if activos_pending_total == 0 else "warning",
                "status": "Controlado" if activos_pending_total == 0 else "En seguimiento",
                "detail": (
                    "Activos, planes y servicios están bajo control."
                    if activos_pending_total == 0
                    else "Hay activos o servicios con seguimiento pendiente."
                ),
                "next_step": (
                    "Mantener calendario y bitácora al día."
                    if activos_pending_total == 0
                    else "Cerrar servicios críticos y regularizar mantenimiento."
                ),
                "url": reverse("activos:dashboard"),
                "cta": "Abrir activos",
            },
            {
                "module": "Integración comercial",
                "owner": "Maestros / Operación",
                "count": integration_pending_total,
                "tone": "success" if integration_pending_total == 0 else "danger",
                "status": "Controlado" if integration_pending_total == 0 else "Con bloqueo",
                "detail": (
                    "Las referencias externas ya están cerradas contra el maestro."
                    if integration_pending_total == 0
                    else "Persisten referencias externas o de receta que siguen abiertas."
                ),
                "next_step": (
                    "Mantener control preventivo del catálogo externo."
                    if integration_pending_total == 0
                    else "Cerrar referencias externas y artículos pendientes."
                ),
                "url": reverse("inventario:aliases_catalog"),
                "cta": "Abrir integración",
            },
            {
                "module": "Reportes ejecutivos",
                "owner": "DG / Analítica",
                "count": reportes_risk_total,
                "tone": "success" if reportes_risk_total == 0 else "warning",
                "status": "Controlado" if reportes_risk_total == 0 else "En seguimiento",
                "detail": (
                    "La base para reportes ejecutivos está consistente."
                    if reportes_risk_total == 0
                    else "La consistencia del dato aún depende de cierres en maestro, recetas o inventario."
                ),
                "next_step": (
                    "Revisar KPIs y tendencias."
                    if reportes_risk_total == 0
                    else "Cerrar brechas operativas antes de tomar lectura definitiva."
                ),
                "url": reverse("reportes:bi"),
                "cta": "Abrir reportes",
            },
            {
                "module": "RRHH",
                "owner": "RRHH / Administración",
                "count": rrhh_pending_total,
                "tone": "success" if rrhh_pending_total == 0 else "warning",
                "status": "Controlado" if rrhh_pending_total == 0 else "En seguimiento",
                "detail": (
                    "Plantilla y nómina operan sin brechas visibles."
                    if rrhh_pending_total == 0
                    else "Persisten empleados inactivos por depurar o nóminas aún en borrador."
                ),
                "next_step": (
                    "Mantener plantilla y cierre de nómina al día."
                    if rrhh_pending_total == 0
                    else "Cerrar borradores y regularizar plantilla activa."
                ),
                "url": reverse("rrhh:empleados"),
                "cta": "Abrir RRHH",
            },
            {
                "module": "CRM",
                "owner": "CRM / Ventas",
                "count": crm_pending_total,
                "tone": "success" if crm_pending_total == 0 else "warning",
                "status": "Controlado" if crm_pending_total == 0 else "En seguimiento",
                "detail": (
                    "Clientes y pedidos comerciales están estabilizados."
                    if crm_pending_total == 0
                    else "Persisten pedidos comerciales abiertos o pendientes de cierre."
                ),
                "next_step": (
                    "Mantener cartera y pedidos sin brechas."
                    if crm_pending_total == 0
                    else "Cerrar pedidos abiertos y completar trazabilidad comercial."
                ),
                "url": reverse("crm:pedidos"),
                "cta": "Abrir CRM",
            },
            {
                "module": "Logística",
                "owner": "Logística / Operación",
                "count": logistica_pending_total,
                "tone": "success" if logistica_pending_total == 0 else "warning",
                "status": "Controlado" if logistica_pending_total == 0 else "En seguimiento",
                "detail": (
                    "Rutas y entregas están bajo control."
                    if logistica_pending_total == 0
                    else "Persisten rutas en tránsito, entregas pendientes o incidencias abiertas."
                ),
                "next_step": (
                    "Mantener el cierre logístico del día."
                    if logistica_pending_total == 0
                    else "Cerrar pendientes de reparto y resolver incidencias."
                ),
                "url": reverse("logistica:rutas"),
                "cta": "Abrir logística",
            },
            {
                "module": "Control",
                "owner": "Control / Operación",
                "count": control_pending_total,
                "tone": "success" if control_pending_total == 0 else "warning",
                "status": "Controlado" if control_pending_total == 0 else "En seguimiento",
                "detail": (
                    "Captura de piso y control operativo están al día."
                    if control_pending_total == 0
                    else "Hay capturas o mermas del día que requieren cierre operativo."
                ),
                "next_step": (
                    "Mantener control diario sin desviaciones."
                    if control_pending_total == 0
                    else "Cerrar captura de piso y conciliar diferencias del día."
                ),
                "url": reverse("control:discrepancias"),
                "cta": "Abrir control",
            },
        ]
        ctx["erp_extended_governance_rows"] = [
            {
                "module": item["module"],
                "owner": item["owner"],
                "blockers": item["count"],
                "status": item["status"],
                "detail": item["detail"],
                "next_step": item["next_step"],
                "url": item["url"],
                "cta": item["cta"],
                "completion": 100 if item["tone"] == "success" else (65 if item["tone"] == "warning" else 35),
            }
            for item in ctx["erp_extended_module_cards"]
        ]
        ctx["erp_extended_release_rows"] = [
            {
                "module": item["module"],
                "owner": item["owner"],
                "open": item["count"],
                "closed": 1 if item["tone"] == "success" else 0,
                "completion": 100 if item["tone"] == "success" else (65 if item["tone"] == "warning" else 35),
                "detail": item["detail"],
                "next_step": item["next_step"],
                "url": item["url"],
                "cta": item["cta"],
            }
            for item in ctx["erp_extended_module_cards"]
        ]
        extended_dependency_map = {
            "Usuarios y Accesos": "Identidad, cobertura y segregación por rol.",
            "Activos": "Catálogo técnico, planes y bitácora de mantenimiento.",
            "Integración comercial": "Referencias externas cerradas contra el maestro ERP.",
            "Reportes ejecutivos": "Datos operativos ya estabilizados en maestro, BOM, compras e inventario.",
            "RRHH": "Plantilla activa, incidencias y cierre administrativo.",
            "CRM": "Clientes y pedidos comerciales con trazabilidad de cierre.",
            "Logística": "Rutas, entregas y excepciones documentadas.",
            "Control": "Captura de piso, mermas y disciplina operativa diaria.",
        }
        extended_exit_criteria_map = {
            "Usuarios y Accesos": "Accesos listos para operar sin brechas de cobertura ni segregación.",
            "Activos": "Mantenimiento documentado y activos bajo control preventivo.",
            "Integración comercial": "Referencias comerciales ya cerradas contra el maestro ERP.",
            "Reportes ejecutivos": "KPI y lectura directiva consistentes para decisión ejecutiva.",
            "RRHH": "Plantilla y nómina listas para seguimiento y cierre administrativo.",
            "CRM": "Pedidos y clientes listos para lectura comercial y seguimiento.",
            "Logística": "Entrega del día cerrada con trazabilidad e incidencias controladas.",
            "Control": "Capturas operativas cerradas y diferencias ya conciliadas.",
        }
        ctx["erp_extended_handoff_rows"] = [
            {
                "module": item["module"],
                "owner": item["owner"],
                "depends_on": extended_dependency_map.get(item["module"], "Disciplina operativa del módulo."),
                "exit_criteria": extended_exit_criteria_map.get(item["module"], "Salida controlada para downstream."),
                "next_step": item["next_step"],
                "completion": 100 if item["tone"] == "success" else (65 if item["tone"] == "warning" else 35),
                "detail": item["detail"],
                "url": item["url"],
                "cta": item["cta"],
            }
            for item in ctx["erp_extended_module_cards"]
        ]
        ctx["erp_operating_chain"] = [
            {
                "step": "01",
                "title": "Maestro de artículos",
                "label": "Gobierno base",
                "value": master_pending_total,
                "tone": "danger" if master_pending_total else "success",
                "detail": "Unidad, categoría, proveedor y código comercial deben quedar listos antes de operar.",
                "cta": "Abrir maestro",
                "url": reverse("maestros:insumo_list"),
            },
            {
                "step": "02",
                "title": "Recetas y BOM",
                "label": "Estructura productiva",
                "value": recipe_pending_total,
                "tone": "warning" if recipe_pending_total else "success",
                "detail": "Base, derivados, producto final y empaque deben quedar conectados y trazables.",
                "cta": "Abrir recetas",
                "url": reverse("recetas:recetas_list"),
            },
            {
                "step": "03",
                "title": "Compras documentales",
                "label": "Flujo documental",
                "value": compras_documental_total,
                "tone": "warning" if compras_documental_total else "success",
                "detail": "Plan, solicitud, orden y recepción deben avanzar sin bloqueos ERP.",
                "cta": "Abrir compras",
                "url": reverse("compras:solicitudes"),
            },
            {
                "step": "04",
                "title": "Inventario y conciliación",
                "label": "Control operativo",
                "value": inventario_pending_total,
                "tone": "danger" if inventario_pending_total else "success",
                "detail": "Stock, referencias y conciliación deben cerrarse para costeo y reabasto confiables.",
                "cta": "Abrir inventario",
                "url": reverse("inventario:existencias"),
            },
        ]
        ctx["erp_module_map"] = [
            {
                "module": "Maestro",
                "stage": "Catálogo listo",
                "status": "Controlado" if master_pending_total == 0 else "Requiere atención",
                "tone": "success" if master_pending_total == 0 else "danger",
                "count": master_pending_total,
                "detail": "Unidad, categoría, proveedor y código comercial definidos antes de operar.",
                "owner": "Maestros / DG",
                "next_step": "Cerrar faltantes del artículo y duplicados activos." if master_pending_total else "Mantener gobierno del catálogo y altas controladas.",
                "url": reverse("maestros:insumo_list"),
                "cta": "Abrir maestro",
            },
            {
                "module": "Recetas",
                "stage": "BOM estable",
                "status": "Controlado" if recipe_pending_total == 0 else "Requiere atención",
                "tone": "success" if recipe_pending_total == 0 else "warning",
                "count": recipe_pending_total,
                "detail": "Bases, derivados, producto final y empaque conectados con costo consistente.",
                "owner": "Producción / Costeo",
                "next_step": "Cerrar BOM, empaques y artículos pendientes en estructura." if recipe_pending_total else "Mantener trazabilidad base -> derivado -> final.",
                "url": reverse("recetas:recetas_list"),
                "cta": "Abrir recetas",
            },
            {
                "module": "Compras",
                "stage": "Flujo documental",
                "status": "Controlado" if compras_documental_total == 0 else "Requiere atención",
                "tone": "success" if compras_documental_total == 0 else "warning",
                "count": compras_documental_total,
                "detail": "Solicitud, orden y recepción avanzan sin bloqueos documentales.",
                "owner": "Compras",
                "next_step": "Liberar solicitudes, órdenes o recepciones abiertas." if compras_documental_total else "Sostener disciplina documental por etapa.",
                "url": reverse("compras:solicitudes"),
                "cta": "Abrir compras",
            },
            {
                "module": "Inventario",
                "stage": "Conciliación diaria",
                "status": "Controlado" if inventario_pending_total == 0 else "Requiere atención",
                "tone": "success" if inventario_pending_total == 0 else "danger",
                "count": inventario_pending_total,
                "detail": "Stock, referencias y movimientos quedan listos para costeo y reabasto.",
                "owner": "Almacén / Inventario",
                "next_step": "Resolver alertas, conciliaciones y referencias abiertas." if inventario_pending_total else "Mantener conciliación diaria y reorden estable.",
                "url": reverse("inventario:existencias"),
                "cta": "Abrir inventario",
            },
        ]
        module_map = ctx["erp_module_map"]
        module_map_by_name = {item["module"]: item for item in module_map}
        stage_weights = {
            "Maestro": 25,
            "Recetas": 25,
            "Compras": 25,
            "Inventario": 25,
        }
        stage_progress_rows = []
        for item in module_map:
            tone = item["tone"]
            if tone == "success":
                progress_pct = 100
                progress_label = "Controlado"
            elif tone == "warning":
                progress_pct = 65
                progress_label = "En estabilización"
            else:
                progress_pct = 30
                progress_label = "Con bloqueo crítico"
            if progress_pct >= 100:
                closed_count = 3
            elif progress_pct >= 65:
                closed_count = 2
            else:
                closed_count = 1
            stage_progress_rows.append(
                {
                    "module": item["module"],
                    "stage": item["stage"],
                    "progress_pct": progress_pct,
                    "progress_label": progress_label,
                    "closed_count": closed_count,
                    "total_count": 3,
                    "tone": tone,
                    "weight": stage_weights.get(item["module"], 0),
                    "detail": item["detail"],
                    "owner": item["owner"],
                    "next_step": item["next_step"],
                    "url": item["url"],
                    "cta": item["cta"],
                }
            )
        weighted_total = sum(row["weight"] for row in stage_progress_rows) or 1
        weighted_progress = int(
            round(sum(row["progress_pct"] * row["weight"] for row in stage_progress_rows) / weighted_total)
        )
        controlled_modules = [item for item in module_map if item["tone"] == "success"]
        pending_modules = [item for item in module_map if item["tone"] != "success"]
        severity_order = {"danger": 0, "warning": 1, "success": 2}
        next_priority = sorted(
            pending_modules,
            key=lambda item: (severity_order.get(item["tone"], 9), -int(item.get("count") or 0), item["module"]),
        )[0] if pending_modules else None
        ctx["erp_maturity_summary"] = {
            "controlled_modules": len(controlled_modules),
            "pending_modules": len(pending_modules),
            "coverage_pct": int(round((len(controlled_modules) / len(module_map)) * 100)) if module_map else 0,
            "weighted_progress_pct": weighted_progress,
            "next_priority_module": next_priority["module"] if next_priority else "ERP controlado",
            "next_priority_detail": next_priority["detail"] if next_priority else "Todos los módulos críticos están controlados.",
            "next_priority_url": next_priority["url"] if next_priority else reverse("dashboard"),
            "next_priority_cta": next_priority["cta"] if next_priority else "Revisar cockpit",
        }
        ctx["erp_stage_progress_rows"] = stage_progress_rows
        ctx["erp_workflow_module_rows"] = stage_progress_rows
        ctx["erp_governance_rows"] = [
            {
                "front": row["module"],
                "owner": row["owner"],
                "blockers": row["total_count"] - row["closed_count"],
                "completion": row["progress_pct"],
                "detail": row["detail"],
                "next_step": row["next_step"],
                "url": row["url"],
                "cta": row["cta"],
            }
            for row in stage_progress_rows
        ]
        ctx["erp_command_center"] = _dashboard_command_center(
            erp_governance_rows=ctx["erp_governance_rows"],
            erp_maturity_summary=ctx["erp_maturity_summary"],
        )
        trunk_dependency_map = [
            {
                "step": "01",
                "module_key": "Maestro",
                "title": "Maestro de artículos",
                "depends_on": "Sin dependencia previa",
                "dependency_detail": "Catálogo canónico, unidad base y gobierno del artículo.",
            },
            {
                "step": "02",
                "module_key": "Recetas",
                "title": "BOM y producto final",
                "depends_on": "Maestro de artículos",
                "dependency_detail": "La estructura productiva no debe consumir artículos incompletos.",
            },
            {
                "step": "03",
                "module_key": "Compras",
                "title": "Compras documentales",
                "depends_on": "BOM y producto final",
                "dependency_detail": "Las solicitudes y órdenes deben venir de artículos y recetas cerradas.",
            },
            {
                "step": "04",
                "module_key": "Inventario",
                "title": "Inventario y conciliación",
                "depends_on": "Compras documentales",
                "dependency_detail": "Entradas, recepciones y ajustes deben cerrar el circuito del abastecimiento.",
            },
        ]
        erp_trunk_chain_rows = []
        previous_link: dict | None = None
        for item in trunk_dependency_map:
            module_row = module_map_by_name.get(item["module_key"])
            if not module_row:
                continue
            matching_progress = next((row for row in stage_progress_rows if row["module"] == item["module_key"]), None)
            upstream_blocking = bool(previous_link and previous_link.get("tone") != "success")
            dependency_status = (
                f"Condicionado por {previous_link['title']}"
                if upstream_blocking
                else "Listo para avanzar"
                if module_row["tone"] == "success"
                else "Con brechas propias"
            )
            erp_trunk_chain_rows.append(
                {
                    "step": item["step"],
                    "title": item["title"],
                    "depends_on": item["depends_on"],
                    "dependency_detail": item["dependency_detail"],
                    "owner": module_row["owner"],
                    "status": module_row["status"],
                    "tone": module_row["tone"],
                    "count": module_row["count"],
                    "progress_pct": matching_progress["progress_pct"] if matching_progress else 0,
                    "next_step": module_row["next_step"],
                    "dependency_status": dependency_status,
                    "upstream_blocking": upstream_blocking,
                    "url": module_row["url"],
                    "cta": module_row["cta"],
                }
            )
            previous_link = {
                "title": item["title"],
                "tone": module_row["tone"],
            }
        ctx["erp_trunk_chain_rows"] = erp_trunk_chain_rows
        ctx["erp_trunk_closure_cards"] = _dashboard_trunk_closure_cards(erp_trunk_chain_rows)
        ctx["erp_critical_path_rows"] = _dashboard_critical_path_rows(erp_trunk_chain_rows)
        ctx["erp_executive_radar_rows"] = _dashboard_executive_radar_rows(
            stage_progress_rows,
            erp_trunk_chain_rows,
        )
        ctx["erp_next_actions"] = [
            {
                "module": item["module"],
                "stage": item["stage"],
                "tone": item["tone"],
                "count": item["count"],
                "detail": item["detail"],
                "url": item["url"],
                "cta": item["cta"],
            }
            for item in sorted(
                pending_modules,
                key=lambda row: (severity_order.get(row["tone"], 9), -int(row.get("count") or 0), row["module"]),
            )
        ]
        ctx["erp_release_gate_rows"] = [
            {
                "step": "01",
                "title": "Maestro y estructura liberados",
                "detail": "Catálogo y BOM cerrados para no arrastrar errores a compras e inventario.",
                "completed": len(
                    [
                        item
                        for item in module_map
                        if item["module"] in {"Maestro", "Recetas"} and item["tone"] == "success"
                    ]
                ),
                "open_count": len(
                    [
                        item
                        for item in module_map
                        if item["module"] in {"Maestro", "Recetas"} and item["tone"] != "success"
                    ]
                ),
                "total": 2,
                "tone": "success"
                if all(
                    item["tone"] == "success"
                    for item in module_map
                    if item["module"] in {"Maestro", "Recetas"}
                )
                else "warning",
                "url": reverse("maestros:insumo_list"),
                "cta": "Abrir maestro",
            },
            {
                "step": "02",
                "title": "Abastecimiento documental liberado",
                "detail": "Solicitudes, órdenes y recepciones avanzan sin bloqueos ERP críticos.",
                "completed": 1 if module_map_by_name["Compras"]["tone"] == "success" else 0,
                "open_count": 0 if module_map_by_name["Compras"]["tone"] == "success" else 1,
                "total": 1,
                "tone": module_map_by_name["Compras"]["tone"],
                "url": reverse("compras:solicitudes"),
                "cta": "Abrir compras",
            },
            {
                "step": "03",
                "title": "Inventario y conciliación liberados",
                "detail": "Stock, referencias y conciliación diaria sostienen costeo y reabasto confiables.",
                "completed": 1 if module_map_by_name["Inventario"]["tone"] == "success" else 0,
                "open_count": 0 if module_map_by_name["Inventario"]["tone"] == "success" else 1,
                "total": 1,
                "tone": module_map_by_name["Inventario"]["tone"],
                "url": reverse("inventario:existencias"),
                "cta": "Abrir inventario",
            },
        ]
        ctx["erp_release_gate_completion"] = int(
            round(
                (sum(row["completed"] for row in ctx["erp_release_gate_rows"]) / sum(row["total"] for row in ctx["erp_release_gate_rows"])) * 100
            )
        ) if ctx["erp_release_gate_rows"] else 0
        handoff_rows = [
            {
                "from": "Maestro",
                "to": "Recetas",
                "status": "Bloqueado" if master_pending_total else "Listo",
                "tone": "danger" if master_pending_total else "success",
                "count": master_pending_total,
                "detail": "La estructura BOM depende de artículos completos, unitizados y sin duplicados operativos.",
                "owner": "Maestros / DG",
                "depends_on": "Unidad base, proveedor, categoría y control canónico del artículo.",
                "exit_criteria": "Catálogo listo para operar sin brechas críticas.",
                "next_step": "Cerrar artículos incompletos y duplicados activos del maestro.",
                "completion": max(0, 100 - min(master_pending_total * 5, 100)),
                "url": reverse("maestros:insumo_list"),
                "cta": "Revisar maestro",
            },
            {
                "from": "Recetas",
                "to": "Compras",
                "status": "Bloqueado" if recipe_pending_total else "Listo",
                "tone": "warning" if recipe_pending_total else "success",
                "count": recipe_pending_total,
                "detail": "MRP y costeo requieren recetas, derivados y empaques cerrados para abastecimiento confiable.",
                "owner": "Producción / Costeo",
                "depends_on": "BOM cerrada, derivados sincronizados y producto final completo.",
                "exit_criteria": "Recetas listas para costeo, MRP y abastecimiento.",
                "next_step": "Cerrar derivados, empaques y bloqueos BOM del catálogo de recetas.",
                "completion": max(0, 100 - min(recipe_pending_total * 5, 100)),
                "url": reverse("recetas:recetas_list"),
                "cta": "Revisar recetas",
            },
            {
                "from": "Compras",
                "to": "Inventario",
                "status": "Bloqueado" if compras_documental_total else "Listo",
                "tone": "warning" if compras_documental_total else "success",
                "count": compras_documental_total,
                "detail": "Sin flujo documental limpio no hay recepción ni valuación consistente en inventario.",
                "owner": "Compras",
                "depends_on": "Solicitudes liberadas, órdenes emitidas y recepciones sin bloqueo.",
                "exit_criteria": "Cadena documental limpia entre abastecimiento y recepción.",
                "next_step": "Cerrar solicitudes, órdenes y recepciones pendientes del período activo.",
                "completion": max(0, 100 - min(compras_documental_total * 5, 100)),
                "url": reverse("compras:solicitudes"),
                "cta": "Revisar compras",
            },
            {
                "from": "Inventario",
                "to": "Reabasto",
                "status": "Bloqueado" if inventario_pending_total else "Listo",
                "tone": "danger" if inventario_pending_total else "success",
                "count": inventario_pending_total,
                "detail": "El reabasto diario exige stock, referencias y conciliación operativa cerrados.",
                "owner": "Almacén / CEDIS",
                "depends_on": "Existencia confiable, movimientos consistentes y alertas sin bloqueo crítico.",
                "exit_criteria": "Inventario listo para surtir, producir y reabastecer.",
                "next_step": "Resolver referencias, conciliaciones y alertas críticas de inventario.",
                "completion": max(0, 100 - min(inventario_pending_total * 5, 100)),
                "url": reverse("inventario:existencias"),
                "cta": "Revisar inventario",
            },
        ]
        ctx["erp_handoff_map"] = handoff_rows
        ctx["erp_blockers_today"] = [
            {
                "title": f"{item['from']} -> {item['to']}",
                "status": item["status"],
                "tone": item["tone"],
                "count": item["count"],
                "detail": item["detail"],
                "url": item["url"],
                "cta": item["cta"],
            }
            for item in handoff_rows
            if item["tone"] != "success"
        ]
    except Exception:
        logger.exception("Dashboard cockpit summary failed")

    template_name = "core/dashboard_executive.html" if ctx.get("dashboard_exec_ready") else "core/dashboard.html"
    return render(request, template_name, ctx)


def health_check(_request: HttpRequest) -> JsonResponse:
    return JsonResponse({"status": "ok"})


def audit_log_view(request: HttpRequest) -> HttpResponse:
    if not request.user.is_authenticated:
        return redirect("/login/")
    if not can_view_audit(request.user):
        raise PermissionDenied("No tienes permisos para ver la bitácora.")

    logs = AuditLog.objects.select_related("user").all()

    model = (request.GET.get("model") or "").strip()
    action = (request.GET.get("action") or "").strip()
    username = (request.GET.get("username") or "").strip()
    date_from = (request.GET.get("date_from") or "").strip()
    date_to = (request.GET.get("date_to") or "").strip()
    q = (request.GET.get("q") or "").strip()

    if model:
        logs = logs.filter(model=model)
    if action:
        logs = logs.filter(action=action)
    if username:
        logs = logs.filter(user__username__icontains=username)
    if date_from:
        logs = logs.filter(timestamp__date__gte=date_from)
    if date_to:
        logs = logs.filter(timestamp__date__lte=date_to)
    if q:
        logs = logs.filter(object_id__icontains=q)

    page = Paginator(logs, 30).get_page(request.GET.get("page"))
    context = {
        "page": page,
        "models": AuditLog.objects.order_by("model").values_list("model", flat=True).distinct(),
        "actions": AuditLog.objects.order_by("action").values_list("action", flat=True).distinct(),
        "filters": {
            "model": model,
            "action": action,
            "username": username,
            "date_from": date_from,
            "date_to": date_to,
            "q": q,
        },
    }
    return render(request, "core/auditoria.html", context)


def users_access_view(request: HttpRequest) -> HttpResponse:
    if not request.user.is_authenticated:
        return redirect("/login/")
    if not can_manage_users(request.user):
        raise PermissionDenied("No tienes permisos para administrar usuarios y accesos.")

    user_model = get_user_model()
    departamentos = list(Departamento.objects.order_by("nombre"))
    sucursales = list(sucursales_operativas().order_by("codigo"))
    role_options = [(role, role) for role in ROLE_ORDER]

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip().lower()
        if action == "create_user":
            username = (request.POST.get("username") or "").strip()
            if not username:
                messages.error(request, "El usuario es obligatorio.")
                return redirect("users_access")
            if user_model.objects.filter(username=username).exists():
                messages.error(request, "Ese usuario ya existe.")
                return redirect("users_access")

            password = (request.POST.get("password") or "").strip()
            if len(password) < 8:
                messages.error(request, "La contraseña debe tener al menos 8 caracteres.")
                return redirect("users_access")

            user = user_model.objects.create_user(
                username=username,
                email=(request.POST.get("email") or "").strip(),
                password=password,
            )
            user.first_name = (request.POST.get("first_name") or "").strip()
            user.last_name = (request.POST.get("last_name") or "").strip()
            user.is_active = _is_checked(request.POST, "is_active")
            user.save(update_fields=["first_name", "last_name", "is_active", "email"])

            _assign_single_role(user, request.POST.get("role") or "")
            profile = _get_or_create_profile(user)
            profile.departamento_id = _safe_int(request.POST.get("departamento_id") or "")
            profile.sucursal_id = _safe_int(request.POST.get("sucursal_id") or "")
            profile.telefono = (request.POST.get("telefono") or "").strip()
            profile.modo_captura_sucursal = _is_checked(request.POST, "modo_captura_sucursal")
            for lock_field, _ in LOCK_FIELDS:
                setattr(profile, lock_field, _is_checked(request.POST, lock_field))
            profile.save()

            log_event(
                request.user,
                "CREATE",
                "auth.User",
                user.id,
                {
                    "username": user.username,
                    "role": primary_role(user),
                    "is_active": user.is_active,
                },
            )
            messages.success(request, f"Usuario {user.username} creado con accesos configurados.")
            return redirect(f"{reverse('users_access')}?edit={user.id}")

        if action == "update_user":
            user_id = _safe_int(request.POST.get("user_id") or "")
            target = user_model.objects.filter(pk=user_id).first() if user_id else None
            if not target:
                messages.error(request, "Usuario no encontrado.")
                return redirect("users_access")
            if target == request.user and not _is_checked(request.POST, "is_active"):
                messages.error(request, "No puedes desactivar tu propio usuario.")
                return redirect(f"{reverse('users_access')}?edit={target.id}")

            target.first_name = (request.POST.get("first_name") or "").strip()
            target.last_name = (request.POST.get("last_name") or "").strip()
            target.email = (request.POST.get("email") or "").strip()
            target.is_active = _is_checked(request.POST, "is_active")
            target.save(update_fields=["first_name", "last_name", "email", "is_active"])

            new_password = (request.POST.get("new_password") or "").strip()
            if new_password:
                if len(new_password) < 8:
                    messages.error(request, "La nueva contraseña debe tener al menos 8 caracteres.")
                    return redirect(f"{reverse('users_access')}?edit={target.id}")
                target.set_password(new_password)
                target.save(update_fields=["password"])

            _assign_single_role(target, request.POST.get("role") or "")
            profile = _get_or_create_profile(target)
            profile.departamento_id = _safe_int(request.POST.get("departamento_id") or "")
            profile.sucursal_id = _safe_int(request.POST.get("sucursal_id") or "")
            profile.telefono = (request.POST.get("telefono") or "").strip()
            profile.modo_captura_sucursal = _is_checked(request.POST, "modo_captura_sucursal")
            for lock_field, _ in LOCK_FIELDS:
                setattr(profile, lock_field, _is_checked(request.POST, lock_field))
            profile.save()

            log_event(
                request.user,
                "UPDATE",
                "auth.User",
                target.id,
                {
                    "username": target.username,
                    "role": primary_role(target),
                    "is_active": target.is_active,
                    "password_changed": bool(new_password),
                },
            )
            messages.success(request, f"Accesos actualizados para {target.username}.")
            return redirect(f"{reverse('users_access')}?edit={target.id}")

        messages.error(request, "Acción no reconocida.")
        return redirect("users_access")

    search_query = (request.GET.get("q") or "").strip()
    role_filter = (request.GET.get("role") or "").strip().upper()
    readiness_filter = (request.GET.get("estado") or "").strip()
    enterprise_gap_filter = (request.GET.get("enterprise_gap") or "").strip().upper()
    coverage_filter = (request.GET.get("coverage") or "").strip().lower()
    coverage_scope_id = _safe_int(request.GET.get("scope_id") or "")
    edit_user_id = _safe_int(request.GET.get("edit") or "")
    users_qs = (
        user_model.objects.select_related("userprofile__departamento", "userprofile__sucursal")
        .prefetch_related("groups")
        .order_by("username")
    )
    if search_query:
        users_qs = users_qs.filter(
            Q(username__icontains=search_query)
            | Q(first_name__icontains=search_query)
            | Q(last_name__icontains=search_query)
            | Q(email__icontains=search_query)
        )

    user_rows = []
    edit_row = None
    for u in users_qs[:300]:
        profile = getattr(u, "userprofile", None)
        access_scope = _user_access_scope(u, profile)
        row = {
            "id": u.id,
            "username": u.username,
            "first_name": u.first_name or "",
            "last_name": u.last_name or "",
            "full_name": (f"{u.first_name} {u.last_name}").strip() or "-",
            "email": u.email or "-",
            "is_active": bool(u.is_active),
            "role": primary_role(u),
            "departamento_id": profile.departamento_id if profile else None,
            "departamento_label": profile.departamento.nombre if profile and profile.departamento else "-",
            "sucursal_id": profile.sucursal_id if profile else None,
            "sucursal_label": profile.sucursal.nombre if profile and profile.sucursal else "-",
            "telefono": profile.telefono if profile else "",
            "modo_captura_sucursal": bool(profile.modo_captura_sucursal) if profile else False,
            "locks": {key: bool(getattr(profile, key, False)) if profile else False for key, _ in LOCK_FIELDS},
            "locks_count": sum(1 for key, _ in LOCK_FIELDS if profile and bool(getattr(profile, key, False))),
            "blocked_modules": access_scope["blocked_modules"],
            "visible_modules": access_scope["visible_modules"],
            "manageable_modules": access_scope["manageable_modules"],
            "readiness_label": access_scope["readiness_label"],
            "readiness_tone": access_scope["readiness_tone"],
            "lock_rows": [
                {
                    "field": key,
                    "label": label,
                    "value": bool(getattr(profile, key, False)) if profile else False,
                }
                for key, label in LOCK_FIELDS
            ],
        }
        row.update(_user_enterprise_profile(u, profile, row))
        if role_filter and row["role"] != role_filter:
            continue
        if readiness_filter and row["readiness_label"] != readiness_filter:
            continue
        if enterprise_gap_filter and enterprise_gap_filter not in row["blocker_codes"]:
            continue
        if coverage_filter == "sucursal" and coverage_scope_id and row["sucursal_id"] != coverage_scope_id:
            continue
        if coverage_filter == "departamento" and coverage_scope_id and row["departamento_id"] != coverage_scope_id:
            continue
        user_rows.append(row)
        if edit_user_id and u.id == edit_user_id:
            edit_row = row

    role_summary = []
    for role in ROLE_ORDER:
        count = sum(1 for row in user_rows if row["role"] == role)
        role_summary.append({"role": role, "count": count, "query": f"?role={role}"})

    readiness_summary = {
        "Operativo": sum(1 for row in user_rows if row["readiness_label"] == "Operativo"),
        "Operativo con candados": sum(1 for row in user_rows if row["readiness_label"] == "Operativo con candados"),
        "Captura sucursal": sum(1 for row in user_rows if row["readiness_label"] == "Captura sucursal"),
        "Inactivo": sum(1 for row in user_rows if row["readiness_label"] == "Inactivo"),
    }
    enterprise_blocker_summary = [
        {
            "code": code,
            "label": label,
            "count": sum(1 for row in user_rows if code in row["blocker_codes"]),
            "query": f"?enterprise_gap={code}",
        }
        for code, label in USER_BLOCKER_LABELS.items()
    ]
    enterprise_ready_summary = {
        "listo_erp": sum(1 for row in user_rows if row["status_label"] == "Lista para operar"),
        "bloqueados": sum(1 for row in user_rows if row["status_label"] == "Bloqueado"),
        "restricciones": sum(1 for row in user_rows if row["status_label"] == "Operativo con restricciones"),
        "inactivos": sum(1 for row in user_rows if row["status_label"] == "Inactivo"),
    }
    enterprise_blocker_rows = [row for row in user_rows if row["blockers_count"] > 0][:12]
    operational_coverage = _operational_coverage_summary(user_rows, sucursales, departamentos)
    users_enterprise_chain = [
        {
            "step": "01",
            "title": "Identidad y rol",
            "status": "Controlado" if enterprise_ready_summary["bloqueados"] == 0 else "Requiere atención",
            "tone": "success" if enterprise_ready_summary["bloqueados"] == 0 else "danger",
            "count": enterprise_ready_summary["bloqueados"],
            "detail": "Cada usuario activo debe tener rol principal, sin bloqueos críticos de acceso.",
            "url": reverse("users_access"),
            "cta": "Abrir usuarios",
        },
        {
            "step": "02",
            "title": "Cobertura operativa",
            "status": "Controlado" if operational_coverage["sucursales_gap"] == 0 and operational_coverage["departamentos_gap"] == 0 else "Requiere atención",
            "tone": "success" if operational_coverage["sucursales_gap"] == 0 and operational_coverage["departamentos_gap"] == 0 else "warning",
            "count": operational_coverage["sucursales_gap"] + operational_coverage["departamentos_gap"],
            "detail": "Sucursales y áreas deben tener responsables y captura alineada a la operación.",
            "url": reverse("users_access"),
            "cta": "Ver cobertura",
        },
        {
            "step": "03",
            "title": "RBAC y candados",
            "status": "Controlado" if enterprise_ready_summary["restricciones"] == 0 else "Requiere atención",
            "tone": "success" if enterprise_ready_summary["restricciones"] == 0 else "warning",
            "count": enterprise_ready_summary["restricciones"],
            "detail": "Los candados deben usarse solo para segmentar operación, no para ocultar errores de configuración.",
            "url": reverse("users_access"),
            "cta": "Abrir RBAC",
        },
        {
            "step": "04",
            "title": "Alta productiva",
            "status": "Controlado" if enterprise_ready_summary["listo_erp"] else "Pendiente",
            "tone": "success" if enterprise_ready_summary["listo_erp"] else "warning",
            "count": enterprise_ready_summary["listo_erp"],
            "detail": "El objetivo es dejar al usuario listo para operar con el mínimo privilegio correcto.",
            "url": reverse("users_access"),
            "cta": "Ver listos",
        },
    ]
    users_trunk_chain_rows = []
    previous_users_stage: dict | None = None
    total_user_stages = len(users_enterprise_chain)
    for index, item in enumerate(users_enterprise_chain):
        upstream_blocking = bool(previous_users_stage and previous_users_stage.get("tone") != "success")
        dependency_status = (
            f"Condicionado por {previous_users_stage['title']}"
            if upstream_blocking
            else "Listo para avanzar"
            if item["tone"] == "success"
            else "Con brechas propias"
        )
        users_trunk_chain_rows.append(
            {
                "step": item["step"],
                "title": item["title"],
                "owner": (
                    "RH / Administración"
                    if item["step"] == "01"
                    else "Administración / Seguridad"
                    if item["step"] in {"02", "03"}
                    else "Líder de área"
                ),
                "status": item["status"],
                "tone": item["tone"],
                "count": item["count"],
                "completion": int(round(((index + 1) / total_user_stages) * 100)) if total_user_stages else 0,
                "depends_on": previous_users_stage["title"] if previous_users_stage else "Sin dependencia previa",
                "dependency_status": dependency_status,
                "next_step": item["cta"],
                "detail": item["detail"],
                "url": item["url"],
            }
        )
        previous_users_stage = {
            "title": item["title"],
            "tone": item["tone"],
        }
    users_maturity_summary = {
        "completed_steps": sum(1 for item in users_enterprise_chain if item["tone"] == "success"),
        "attention_steps": sum(1 for item in users_enterprise_chain if item["tone"] != "success"),
        "coverage_pct": int(round((sum(1 for item in users_enterprise_chain if item["tone"] == "success") / len(users_enterprise_chain)) * 100))
        if users_enterprise_chain
        else 0,
    }
    next_priority = next((item for item in users_enterprise_chain if item["tone"] != "success"), None)
    users_maturity_summary.update(
        {
            "next_priority_title": next_priority["title"] if next_priority else "Cadena estabilizada",
            "next_priority_detail": next_priority["detail"] if next_priority else "Sin brechas abiertas en usuarios y accesos.",
            "next_priority_url": next_priority["url"] if next_priority else reverse("users_access"),
            "next_priority_cta": next_priority["cta"] if next_priority else "Abrir usuarios",
        }
    )
    users_handoff_map = [
        {
            "label": "Rol -> Cobertura",
            "detail": "Todo usuario con rol operativo debe quedar asignado a sucursal o departamento correcto.",
            "count": enterprise_ready_summary["bloqueados"],
            "tone": "success" if enterprise_ready_summary["bloqueados"] == 0 else "danger",
            "status": "Controlado" if enterprise_ready_summary["bloqueados"] == 0 else "Con bloqueo",
            "url": reverse("users_access"),
            "cta": "Abrir usuarios",
            "owner": "Administración / TI",
            "depends_on": "Rol principal asignado",
            "exit_criteria": "Cada usuario operativo debe tener alcance válido por sucursal o departamento.",
            "next_step": "Completar sucursal, departamento y modo operativo por usuario.",
            "completion": 100 if enterprise_ready_summary["bloqueados"] == 0 else 35,
        },
        {
            "label": "Cobertura -> RBAC",
            "detail": "La cobertura operativa debe cerrar antes de refinar candados y permisos especiales.",
            "count": operational_coverage["sucursales_gap"] + operational_coverage["departamentos_gap"],
            "tone": "success"
            if operational_coverage["sucursales_gap"] == 0 and operational_coverage["departamentos_gap"] == 0
            else "warning",
            "status": "Controlado"
            if operational_coverage["sucursales_gap"] == 0 and operational_coverage["departamentos_gap"] == 0
            else "Con brechas",
            "url": reverse("users_access"),
            "cta": "Ver cobertura",
            "owner": "Administración / RRHH",
            "depends_on": "Cobertura operativa completa",
            "exit_criteria": "Sucursales y departamentos deben quedar sin brechas de cobertura.",
            "next_step": "Ajustar cobertura operativa antes de cerrar candados RBAC.",
            "completion": 100 if operational_coverage["sucursales_gap"] == 0 and operational_coverage["departamentos_gap"] == 0 else 55,
        },
        {
            "label": "RBAC -> Operación",
            "detail": "Los candados solo deben restringir el alcance operativo, no sustituir configuración faltante.",
            "count": enterprise_ready_summary["restricciones"],
            "tone": "success" if enterprise_ready_summary["restricciones"] == 0 else "warning",
            "status": "Controlado" if enterprise_ready_summary["restricciones"] == 0 else "Seguimiento",
            "url": reverse("users_access"),
            "cta": "Abrir RBAC",
            "owner": "TI / Dirección",
            "depends_on": "Cobertura y roles cerrados",
            "exit_criteria": "Candados y permisos especiales deben quedar auditables y justificados.",
            "next_step": "Revisar restricciones abiertas y cerrar excepciones operativas.",
            "completion": 100 if enterprise_ready_summary["restricciones"] == 0 else 70,
        },
    ]
    users_operational_health_cards = [
        {
            "label": "Usuarios bloqueados",
            "count": enterprise_ready_summary["bloqueados"],
            "tone": "danger",
        },
        {
            "label": "Con restricciones",
            "count": enterprise_ready_summary["restricciones"],
            "tone": "warning",
        },
        {
            "label": "Cobertura con gap",
            "count": operational_coverage["sucursales_gap"] + operational_coverage["departamentos_gap"],
            "tone": "warning",
        },
    ]
    users_focus_cards = [
        {
            "key": "SIN_ROL",
            "label": "Identidad sin rol",
            "detail": "Usuarios activos sin rol principal asignado.",
            "count": sum(1 for row in user_rows if "SIN_ROL" in row["blocker_codes"]),
            "url": f"{reverse('users_access')}?enterprise_gap=SIN_ROL",
            "is_active": enterprise_gap_filter == "SIN_ROL",
        },
        {
            "key": "SIN_DEPARTAMENTO",
            "label": "Departamento pendiente",
            "detail": "Usuarios operativos sin departamento asignado.",
            "count": sum(1 for row in user_rows if "SIN_DEPARTAMENTO" in row["blocker_codes"]),
            "url": f"{reverse('users_access')}?enterprise_gap=SIN_DEPARTAMENTO",
            "is_active": enterprise_gap_filter == "SIN_DEPARTAMENTO",
        },
        {
            "key": "CAPTURA_SIN_SUCURSAL",
            "label": "Captura sin sucursal",
            "detail": "Usuarios en modo captura que no tienen sucursal activa.",
            "count": sum(1 for row in user_rows if "CAPTURA_SIN_SUCURSAL" in row["blocker_codes"]),
            "url": f"{reverse('users_access')}?enterprise_gap=CAPTURA_SIN_SUCURSAL",
            "is_active": enterprise_gap_filter == "CAPTURA_SIN_SUCURSAL",
        },
        {
            "key": "RESTRICCIONES",
            "label": "RBAC con restricciones",
            "detail": "Usuarios operativos que siguen con candados parciales.",
            "count": enterprise_ready_summary["restricciones"],
            "url": f"{reverse('users_access')}?estado=Operativo+con+candados",
            "is_active": readiness_filter == "Operativo con candados",
        },
    ]
    users_stage_rows = [
        {
            "step": "01",
            "title": "Identidad",
            "open_count": users_focus_cards[0]["count"],
            "closed_count": max(len(user_rows) - users_focus_cards[0]["count"], 0),
            "detail": "Rol principal y alta activa del usuario.",
            "url": users_focus_cards[0]["url"],
            "cta": "Abrir identidad",
        },
        {
            "step": "02",
            "title": "Cobertura",
            "open_count": users_focus_cards[1]["count"] + users_focus_cards[2]["count"],
            "closed_count": max(len(user_rows) - (users_focus_cards[1]["count"] + users_focus_cards[2]["count"]), 0),
            "detail": "Departamento y sucursal correctos para operar.",
            "url": f"{reverse('users_access')}?coverage=sucursal",
            "cta": "Abrir cobertura",
        },
        {
            "step": "03",
            "title": "RBAC",
            "open_count": users_focus_cards[3]["count"],
            "closed_count": max(len(user_rows) - users_focus_cards[3]["count"], 0),
            "detail": "Candados y permisos finales del usuario.",
            "url": users_focus_cards[3]["url"],
            "cta": "Abrir RBAC",
        },
    ]
    users_release_gate_rows = [
        {
            "step": "01",
            "title": "Identidad validada",
            "detail": "El usuario debe estar activo y con rol principal asignado.",
            "completed": max(len(user_rows) - users_focus_cards[0]["count"], 0),
            "open_count": users_focus_cards[0]["count"],
            "total": len(user_rows),
            "tone": "success" if users_focus_cards[0]["count"] == 0 else "warning",
            "url": users_focus_cards[0]["url"],
            "cta": "Abrir identidad",
        },
        {
            "step": "02",
            "title": "Cobertura operativa",
            "detail": "Departamento y sucursal deben corresponder a la operación real.",
            "completed": max(len(user_rows) - (users_focus_cards[1]["count"] + users_focus_cards[2]["count"]), 0),
            "open_count": users_focus_cards[1]["count"] + users_focus_cards[2]["count"],
            "total": len(user_rows),
            "tone": "success" if (users_focus_cards[1]["count"] + users_focus_cards[2]["count"]) == 0 else "warning",
            "url": f"{reverse('users_access')}?coverage=sucursal",
            "cta": "Abrir cobertura",
        },
        {
            "step": "03",
            "title": "Acceso liberado",
            "detail": "Los candados deben reflejar segmentación, no errores de configuración.",
            "completed": max(len(user_rows) - users_focus_cards[3]["count"], 0),
            "open_count": users_focus_cards[3]["count"],
            "total": len(user_rows),
            "tone": "success" if users_focus_cards[3]["count"] == 0 else "warning",
            "url": users_focus_cards[3]["url"],
            "cta": "Abrir RBAC",
        },
    ]
    users_release_gate_completion = (
        int(
            round(
                (
                    sum(row["completed"] for row in users_release_gate_rows)
                    / sum(row["total"] for row in users_release_gate_rows)
                )
                * 100
            )
        )
        if users_release_gate_rows and sum(row["total"] for row in users_release_gate_rows)
        else 0
    )
    users_workflow_stage_rows = [
        {
            "step": row["step"],
            "title": row["title"],
            "completion": int(round((row["completed"] / row["total"]) * 100)) if row["total"] else 0,
            "detail": row["detail"],
            "owner": (
                "RH / Administración"
                if row["step"] == 1
                else "Administración / Seguridad"
                if row["step"] == 2
                else "Líder de área"
                if row["step"] == 3
                else "Auditoría / DG"
            ),
            "next_step": row["cta"],
            "url": row["url"],
            "tone": row["tone"],
        }
        for row in users_release_gate_rows
    ]
    users_focus_summary = None
    if enterprise_gap_filter:
        selected = next((card for card in users_focus_cards if card["key"] == enterprise_gap_filter), None)
        if selected:
            users_focus_summary = {
                "title": selected["label"],
                "detail": selected["detail"],
                "count": selected["count"],
                "clear_url": reverse("users_access"),
            }
    elif readiness_filter:
        users_focus_summary = {
            "title": f"Estado: {readiness_filter}",
            "detail": "Vista filtrada por madurez operativa del usuario.",
            "count": len(user_rows),
            "clear_url": reverse("users_access"),
        }
    elif coverage_filter:
        label = "Cobertura por sucursal" if coverage_filter == "sucursal" else "Cobertura por área"
        users_focus_summary = {
            "title": label,
            "detail": "Vista enfocada en brechas de cobertura operativa.",
            "count": len(user_rows),
            "clear_url": reverse("users_access"),
        }

    context = {
        "role_options": role_options,
        "lock_fields": LOCK_FIELDS,
        "departamentos": departamentos,
        "sucursales": sucursales,
        "users": user_rows,
        "edit_row": edit_row,
        "search_query": search_query,
        "role_filter": role_filter,
        "readiness_filter": readiness_filter,
        "enterprise_gap_filter": enterprise_gap_filter,
        "coverage_filter": coverage_filter,
        "coverage_scope_id": coverage_scope_id,
        "role_summary": role_summary,
        "readiness_summary": readiness_summary,
        "enterprise_blocker_summary": enterprise_blocker_summary,
        "enterprise_ready_summary": enterprise_ready_summary,
        "enterprise_blocker_rows": enterprise_blocker_rows,
        "operational_coverage": operational_coverage,
        "users_enterprise_chain": users_enterprise_chain,
        "users_trunk_chain_rows": users_trunk_chain_rows,
        "users_maturity_summary": users_maturity_summary,
        "users_handoff_map": users_handoff_map,
        "users_trunk_closure_cards": _users_trunk_closure_cards(users_trunk_chain_rows),
        "users_critical_path_rows": _users_critical_path_rows(users_trunk_chain_rows),
        "users_executive_radar_rows": _users_executive_radar_rows(users_stage_rows, users_trunk_chain_rows),
        "users_operational_health_cards": users_operational_health_cards,
        "users_focus_cards": users_focus_cards,
        "users_stage_rows": users_stage_rows,
        "users_erp_governance_rows": _users_erp_governance_rows(users_stage_rows),
        "users_release_gate_rows": users_release_gate_rows,
        "users_release_gate_completion": users_release_gate_completion,
        "users_workflow_stage_rows": users_workflow_stage_rows,
        "users_focus_summary": users_focus_summary,
        "role_capability_matrix": _role_capability_matrix(),
        "role_operational_requirements": _role_operational_requirements(),
        "erp_command_center": _users_command_center(enterprise_ready_summary, users_maturity_summary),
    }
    return render(request, "core/usuarios_accesos.html", context)
