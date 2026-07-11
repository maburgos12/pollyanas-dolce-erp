from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode
from uuid import uuid4

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import IntegrityError, OperationalError, transaction
from django.db.models import Count, DecimalField, ExpressionWrapper, F, Max, OuterRef, Q, Subquery, Sum
from django.db.models.functions import Coalesce
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone

from core.access import can_manage_submodule, can_view_logistica, can_view_submodule
from core.audit import log_event
from core.models import Sucursal
from crm.models import PedidoCliente

from .models import (
    BitacoraSalidaLlegada,
    CargaCombustibleUnidad,
    DocumentoUnidad,
    EntregaEcommerce,
    EntregaRuta,
    EventoRuta,
    InspeccionVehiculo,
    LavadoUnidad,
    ParadaEntregaEvidencia,
    ParadaRuta,
    PuntoLogistico,
    ReparacionUnidad,
    Repartidor,
    ReporteUnidad,
    RutaCargaChecklist,
    RutaCargaChecklistLinea,
    RutaEntrega,
    ServicioRealizadoUnidad,
    SolicitudDomicilio,
    TipoServicioUnidad,
    Unidad,
)
from .services_google_routes import recalcular_ruta_programada
from .services_google_roads import snap_gps_path_to_roads
from .services_ecommerce import EcommerceClient, EcommerceIntegrationError
from .services_carga_ruta import (
    autorizar_diferencia_checklist_carga,
    checklist_bloquea_salida,
    cerrar_ruta_con_diferencia_autorizada,
    confirmar_checklist_carga_manual,
    ruta_tiene_diferencias_entrega,
    ruta_tiene_entregas_pendientes,
    registrar_recarga_cedis,
    ruta_tiene_movimiento_point_nuevo,
    sincronizar_checklist_carga_desde_point,
    sincronizar_recepcion_desde_point,
    validar_linea_carga,
)
from .services_rutas_control import distancia_metros, resumen_control_rutas
from .services_entregas import confirmar_entrega_parada, revisar_entrega_excepcional
from .services_tiempos_ruta import resumen_tiempos_ruta


def _parse_decimal(raw: str | None) -> Decimal:
    try:
        return Decimal(str(raw or "0"))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _parse_datetime_local(raw: str | None):
    value = (raw or "").strip()
    if not value:
        return None
    try:
        dt = datetime.strptime(value, "%Y-%m-%dT%H:%M")
    except ValueError:
        return None
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, timezone.get_current_timezone())
    return dt


def _parada_puede_quitarse(parada: ParadaRuta) -> tuple[bool, str]:
    if parada.estado != ParadaRuta.ESTADO_PENDIENTE or parada.entrega_estado != ParadaRuta.ENTREGA_PENDIENTE:
        return False, "No se puede quitar una parada que ya tiene visita o entrega registrada."
    if parada.evidencias_entrega.exists():
        return False, "No se puede quitar una parada que ya tiene evidencia registrada."
    lineas = parada.lineas_carga.all()
    if lineas.exclude(estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE).exists():
        return False, "No se puede quitar una parada que ya tiene carga validada."
    if lineas.filter(Q(cantidad_cargada__isnull=False) | Q(validado_en__isnull=False)).exists():
        return False, "No se puede quitar una parada que ya tiene carga validada."
    return True, ""


def _parse_date(raw: str | None):
    value = (raw or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _ruta_status_choices_for(ruta: RutaEntrega):
    if ruta.estatus == RutaEntrega.ESTATUS_PLANEADA:
        allowed = {RutaEntrega.ESTATUS_PLANEADA, RutaEntrega.ESTATUS_EN_RUTA, RutaEntrega.ESTATUS_CANCELADA}
    elif ruta.estatus == RutaEntrega.ESTATUS_EN_RUTA:
        allowed = {RutaEntrega.ESTATUS_EN_RUTA, RutaEntrega.ESTATUS_COMPLETADA, RutaEntrega.ESTATUS_CANCELADA}
    else:
        allowed = {ruta.estatus}
    return [choice for choice in RutaEntrega.ESTATUS_CHOICES if choice[0] in allowed]


def _recepcion_point_rows(checklist) -> list[dict]:
    if checklist is None:
        return []

    rows = []
    lineas = list(
        checklist.lineas.select_related("parada", "point_transfer_line")
        .filter(Q(point_transfer_line__isnull=True) | Q(point_transfer_line__is_cancelled=False))
        .order_by("parada__orden", "item_name", "id")
    )
    evidencias = {
        evidencia.linea_carga_id: evidencia
        for evidencia in ParadaEntregaEvidencia.objects.filter(
            linea_carga_id__in=[linea.id for linea in lineas],
            tipo=ParadaEntregaEvidencia.TIPO_CONFIRMACION,
        )
    }
    for linea in lineas:
        point_line = linea.point_transfer_line
        evidencia = evidencias.get(linea.id)
        solicitado = Decimal(str(point_line.requested_quantity if point_line else linea.cantidad_solicitada or 0))
        enviado = Decimal(str(point_line.sent_quantity if point_line else linea.cantidad_enviada_esperada or 0))
        cargado_validado = linea.cantidad_cargada is not None
        cargado = Decimal(str(linea.cantidad_cargada or 0)) if cargado_validado else None
        referencia_recepcion = cargado if cargado is not None else enviado
        recibido = Decimal("0")
        received_at = point_line.received_at if point_line else None
        received_by = point_line.received_by if point_line else ""
        if not point_line:
            estado_label = "Sin transferencia Point"
            estado_tone = "danger"
            recibido_display = None
        elif point_line.is_received:
            recibido = Decimal(str(evidencia.cantidad_entregada if evidencia else point_line.received_quantity or 0))
            if evidencia:
                received_at = evidencia.capturado_en
                metadata = evidencia.metadata if isinstance(evidencia.metadata, dict) else {}
                received_by = metadata.get("received_by", received_by)
            if recibido == referencia_recepcion:
                estado_label = "Recibido correcto"
                estado_tone = "success"
            elif recibido == Decimal("0"):
                estado_label = "Recibido cero"
                estado_tone = "danger"
            else:
                estado_label = "Diferencia"
                estado_tone = "danger"
            recibido_display = recibido
        elif not cargado_validado:
            estado_label = "Carga sin validar"
            estado_tone = "warning"
            recibido_display = None
        else:
            estado_label = "Pendiente en Point"
            estado_tone = "warning"
            recibido_display = None

        rows.append(
            {
                "linea": linea,
                "parada": linea.parada,
                "point_line": point_line,
                "solicitado": solicitado,
                "enviado": enviado,
                "esperado": solicitado,
                "ajustado": enviado,
                "cargado": cargado,
                "cargado_validado": cargado_validado,
                "recibido": recibido_display,
                "estado_label": estado_label,
                "estado_tone": estado_tone,
                "received_at": received_at,
                "received_by": received_by,
            }
        )
    return rows


def _totales_recepcion_point(rows: list[dict]) -> list[dict]:
    totales = {}
    for row in rows:
        linea = row["linea"]
        key = (
            (linea.item_code or "").strip().upper(),
            (linea.item_name or "").strip().upper(),
            (linea.unit or "").strip().upper(),
        )
        total = totales.setdefault(
            key,
            {
                "item_code": linea.item_code,
                "item_name": linea.item_name,
                "unit": linea.unit,
                "solicitado": Decimal("0"),
                "enviado": Decimal("0"),
                "esperado": Decimal("0"),
                "ajustado": Decimal("0"),
                "cargado": Decimal("0"),
                "recibido": Decimal("0"),
                "cargado_validado": True,
                "cargado_parcial": False,
            },
        )
        total["solicitado"] += row["solicitado"]
        total["enviado"] += row["enviado"]
        total["esperado"] += row["solicitado"]
        total["ajustado"] += row["enviado"]
        if row["cargado_validado"]:
            total["cargado"] += row["cargado"] or Decimal("0")
            total["cargado_parcial"] = True
        else:
            total["cargado_validado"] = False
        total["recibido"] += row["recibido"] or Decimal("0")
    return sorted(totales.values(), key=lambda row: ((row["item_name"] or ""), (row["item_code"] or "")))


def _module_tabs(active: str, user=None) -> list[dict]:
    tabs = [
        {"key": "dashboard", "label": "Dashboard", "url_name": "logistica:home", "active": active == "dashboard"},
        {"key": "ejecutivo", "label": "Ejecutivo", "url_name": "logistica:dashboard_ejecutivo", "active": active == "ejecutivo"},
        {"key": "tickets", "label": "Tickets", "url_name": "logistica:tickets_kanban", "active": active == "tickets"},
        {"key": "flota", "label": "Flota", "url_name": "logistica:flota_resumen", "active": active == "flota"},
        {"key": "reportes", "label": "Reportes", "url_name": "logistica:reportes_lista", "active": active == "reportes"},
        {"key": "bitacoras", "label": "Bitácoras", "url_name": "logistica:bitacoras_lista", "active": active == "bitacoras"},
        {"key": "rutas", "label": "Rutas", "url_name": "logistica:rutas", "active": active == "rutas"},
        {"key": "control_rutas", "permission_key": "rutas", "label": "Control rutas", "url_name": "logistica:control_rutas", "active": active == "control_rutas"},
        {"key": "revisiones_entrega", "permission_key": "rutas", "label": "Revisiones", "url_name": "logistica:revisiones_entrega", "active": active == "revisiones_entrega"},
        {"key": "puntos_logisticos", "permission_key": "rutas", "label": "Puntos", "url_name": "logistica:puntos_logisticos", "active": active == "puntos_logisticos"},
        {"key": "unidades", "label": "Unidades", "url_name": "logistica:unidades_list", "active": active == "unidades"},
        {"key": "capturas", "label": "Capturas PWA", "url_name": "logistica:capturas_pwa", "active": active == "capturas"},
    ]
    if user is None:
        return tabs
    return [tab for tab in tabs if can_view_submodule(user, "logistica", tab.get("permission_key", tab["key"]))]


def tiene_acceso_logistica(user, roles: list[str] | None = None) -> bool:
    if not user.is_authenticated:
        return False
    if roles is None:
        return can_view_logistica(user)
    role_checks = {
        "dg": lambda: can_view_submodule(user, "logistica", "ejecutivo"),
        "compras_logistica": lambda: can_manage_submodule(user, "logistica", "tickets"),
        "supervisor_logistica": lambda: can_manage_submodule(user, "logistica", "unidades"),
        "repartidor": lambda: can_view_submodule(user, "logistica", "capturas"),
    }
    return any(role_checks.get(role, lambda: False)() for role in roles)


def _can_view_logistica_ejecutivo(user) -> bool:
    return can_view_submodule(user, "logistica", "ejecutivo")


def _can_manage_tickets_logistica(user) -> bool:
    return can_manage_submodule(user, "logistica", "tickets")


def _can_view_flota_resumen(user) -> bool:
    return can_view_submodule(user, "logistica", "flota")


def _can_manage_unidades(user) -> bool:
    return can_manage_submodule(user, "logistica", "unidades")


def _logistica_enterprise_chain(
    *,
    rutas_total: int,
    rutas_hoy: int,
    rutas_en_ruta: int,
    entregas_pendientes: int,
    incidencias: int,
    entregas_completadas: int,
) -> list[dict]:
    chain = [
        {
            "step": "01",
            "title": "Planeación de rutas",
            "detail": "Rutas activas, chofer, unidad y fecha comprometida.",
            "count": rutas_total,
            "status": "Programación del día" if rutas_hoy else "Sin rutas hoy",
            "tone": "success" if rutas_hoy else "warning",
            "url": reverse("logistica:rutas"),
            "cta": "Abrir rutas",
            "owner": "Logística / Planeación",
            "next_step": "Liberar chofer, unidad y fecha de salida del reparto.",
        },
        {
            "step": "02",
            "title": "Despacho operativo",
            "detail": "Rutas en tránsito y seguimiento de cumplimiento.",
            "count": rutas_en_ruta,
            "status": "En ejecución" if rutas_en_ruta else "Sin unidades en tránsito",
            "tone": "success" if rutas_en_ruta else "warning",
            "url": reverse("logistica:rutas") + "?estatus=EN_RUTA",
            "cta": "Ver en ruta",
            "owner": "Logística / Tráfico",
            "next_step": "Mantener rutas en tránsito con seguimiento activo.",
        },
        {
            "step": "03",
            "title": "Entregas pendientes",
            "detail": "Pedidos aún no entregados dentro del circuito logístico.",
            "count": entregas_pendientes,
            "status": "Bajo control" if entregas_pendientes == 0 else f"{entregas_pendientes} por cerrar",
            "tone": "success" if entregas_pendientes == 0 else "danger",
            "url": reverse("logistica:rutas"),
            "cta": "Revisar pendientes",
            "owner": "Logística / Entrega",
            "next_step": "Cerrar entregas pendientes antes del corte del día.",
        },
        {
            "step": "04",
            "title": "Incidencias y cierre",
            "detail": "Entregas con excepción y cierre documental del reparto.",
            "count": incidencias,
            "status": "Sin incidencias" if incidencias == 0 else f"{incidencias} con incidencia",
            "tone": "success" if incidencias == 0 else "danger",
            "url": reverse("logistica:rutas"),
            "cta": "Resolver incidencias",
            "owner": "Logística / Auditoría",
            "next_step": "Resolver incidencias y dejar cierre documental auditable.",
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


def _logistica_document_stage_rows(
    *,
    rutas_total: int,
    rutas_hoy: int,
    rutas_en_ruta: int,
    entregas_total: int,
    entregas_pendientes: int,
    incidencias: int,
) -> list[dict]:
    completadas = max(entregas_total - entregas_pendientes - incidencias, 0)
    rows = [
        {
            "label": "Rutas planeadas",
            "open": rutas_hoy,
            "closed": max(rutas_total - rutas_hoy, 0),
            "detail": "Rutas de hoy versus rutas históricas ya cerradas.",
            "url": reverse("logistica:rutas"),
            "owner": "Logística / Planeación",
            "next_step": "Liberar programación diaria con chofer y unidad asignados.",
        },
        {
            "label": "Rutas en tránsito",
            "open": rutas_en_ruta,
            "closed": max(rutas_hoy - rutas_en_ruta, 0),
            "detail": "Despachos actualmente en ejecución frente a programación del día.",
            "url": reverse("logistica:rutas") + "?estatus=EN_RUTA",
            "owner": "Logística / Tráfico",
            "next_step": "Mantener trazabilidad operativa y registrar avances de reparto.",
        },
        {
            "label": "Entregas por cerrar",
            "open": entregas_pendientes,
            "closed": completadas,
            "detail": "Entregas pendientes comparadas con entregas completadas.",
            "url": reverse("logistica:rutas"),
            "owner": "Logística / Entrega",
            "next_step": "Cerrar pendientes antes del corte operativo del día.",
        },
        {
            "label": "Incidencias abiertas",
            "open": incidencias,
            "closed": max(entregas_total - incidencias, 0),
            "detail": "Eventos con excepción que requieren cierre documental.",
            "url": reverse("logistica:rutas"),
            "owner": "Logística / Auditoría",
            "next_step": "Resolver incidencias y resguardar evidencia de cierre.",
        },
    ]
    for row in rows:
        total = int(row["open"]) + int(row["closed"])
        row["completion"] = int(round((int(row["closed"]) / total) * 100)) if total else 100
    return rows


def _logistica_operational_health_cards(
    *,
    focus: str,
    primary_open: int,
    secondary_open: int,
    ready_count: int,
) -> list[dict[str, object]]:
    if focus == "rutas":
        return [
            {
                "label": "Rutas activas",
                "value": primary_open,
                "tone": "warning" if primary_open else "success",
                "detail": "Rutas programadas con operación abierta.",
            },
            {
                "label": "Entregas pendientes",
                "value": secondary_open,
                "tone": "danger" if secondary_open else "success",
                "detail": "Entregas aún sin cierre documental.",
            },
            {
                "label": "Cierres logísticos",
                "value": ready_count,
                "tone": "success",
                "detail": "Entregas concluidas con trazabilidad operativa.",
            },
        ]
    return [
        {
            "label": "Seguimientos de ruta",
            "value": primary_open,
            "tone": "primary",
            "detail": "Eventos y entregas registrados dentro de la ruta.",
        },
        {
            "label": "Pendientes por cerrar",
            "value": secondary_open,
            "tone": "danger" if secondary_open else "success",
            "detail": "Entregas pendientes o con incidencia en la ruta.",
        },
        {
            "label": "Entregas completadas",
            "value": ready_count,
            "tone": "success",
            "detail": "Entregas cerradas correctamente dentro de la ruta.",
        },
    ]


def _logistica_governance_rows(rows: list[dict], owner_default: str = "Logística / Operación") -> list[dict]:
    governance_rows: list[dict] = []
    for row in rows:
        governance_rows.append(
            {
                "front": row.get("label", "Frente logístico"),
                "owner": row.get("owner") or owner_default,
                "blockers": int(row.get("open") or 0),
                "completion": int(row.get("completion") or 0),
                "detail": row.get("detail", ""),
                "next_step": row.get("next_step") or "Seguir flujo logístico",
                "url": row.get("url") or reverse("logistica:rutas"),
                "cta": "Abrir",
            }
        )
    return governance_rows


def _logistica_executive_radar_rows(
    governance_rows: list[dict[str, object]],
    *,
    default_owner: str = "Logística / Operación",
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
            dominant_blocker = row.get("detail", "") or "Brecha logística en seguimiento"
        else:
            tone = "danger"
            status = "Con bloqueo"
            dominant_blocker = row.get("detail", "") or "Bloqueo logístico abierto"
        rows.append(
            {
                "phase": row.get("front", "Frente logístico"),
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


def _logistica_command_center(
    *,
    governance_rows: list[dict],
    maturity_summary: dict[str, object],
    default_url: str,
    default_cta: str,
) -> dict[str, object]:
    blockers = sum(int(row.get("blockers", 0) or 0) for row in governance_rows)
    attention_steps = int(maturity_summary.get("attention_steps") or 0)
    if blockers > 0:
        status = "Con bloqueos"
        tone = "danger"
    elif attention_steps > 0:
        status = "En seguimiento"
        tone = "warning"
    else:
        status = "Estable"
        tone = "success"
    return {
        "owner": governance_rows[0].get("owner", "Logística / Operación") if governance_rows else "Logística / Operación",
        "status": status,
        "tone": tone,
        "blockers": blockers,
        "next_step": maturity_summary.get("next_priority_detail", "Sin acciones pendientes."),
        "url": maturity_summary.get("next_priority_url", default_url),
        "cta": maturity_summary.get("next_priority_cta", default_cta),
    }


def _logistica_maturity_summary(*, chain: list[dict], default_url: str) -> dict[str, object]:
    completed_steps = sum(1 for item in chain if item.get("tone") == "success")
    attention_steps = len(chain) - completed_steps
    coverage_pct = int(round((completed_steps / len(chain)) * 100)) if chain else 0
    next_priority = next((item for item in chain if item.get("tone") != "success"), None)
    if not next_priority:
        next_priority = {
            "title": "Operación estable",
            "detail": "La cadena logística no presenta bloqueos críticos.",
            "url": default_url,
            "cta": "Revisar rutas",
        }
    return {
        "completed_steps": completed_steps,
        "attention_steps": attention_steps,
        "coverage_pct": coverage_pct,
        "next_priority_title": next_priority.get("title", "Operación estable"),
        "next_priority_detail": next_priority.get("detail", "La cadena logística no presenta bloqueos críticos."),
        "next_priority_url": next_priority.get("url", default_url),
        "next_priority_cta": next_priority.get("cta", "Revisar rutas"),
    }


def _logistica_critical_path_rows(chain: list[dict[str, object]]) -> list[dict[str, object]]:
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
                "title": item.get("title", "Logística"),
                "owner": item.get("owner", "Logística / Operación"),
                "status": item.get("status", "En seguimiento"),
                "tone": item.get("tone", "warning"),
                "count": int(item.get("count") or 0),
                "completion": int(item.get("completion") or 0),
                "depends_on": item.get("depends_on", "Origen del módulo"),
                "dependency_status": item.get("dependency_status", "Punto de arranque del módulo"),
                "detail": item.get("detail", ""),
                "next_step": item.get("next_step", "Continuar flujo"),
                "url": item.get("url", reverse("logistica:rutas")),
                "cta": item.get("cta", "Abrir"),
            }
        )
    return rows


def _logistica_handoff_map(
    *,
    rutas_hoy: int,
    rutas_en_ruta: int,
    entregas_pendientes: int,
    incidencias: int,
) -> list[dict[str, object]]:
    return [
        {
            "label": "Planeación",
            "count": rutas_hoy,
            "status": "Programado" if rutas_hoy else "Sin rutas hoy",
            "detail": "Rutas del día listas para despacho.",
            "tone": "success" if rutas_hoy else "warning",
            "url": reverse("logistica:rutas"),
            "cta": "Abrir agenda",
            "owner": "Logística / Planeación",
            "depends_on": "Pedidos liberados para reparto",
            "exit_criteria": "Toda ruta del día debe quedar programada con unidad y responsable.",
            "next_step": "Cerrar agenda diaria y liberar salida de reparto.",
            "completion": 100 if rutas_hoy else 60,
        },
        {
            "label": "Despacho",
            "count": rutas_en_ruta,
            "status": "En tránsito" if rutas_en_ruta else "Sin salidas activas",
            "detail": "Unidades con entregas actualmente en ejecución.",
            "tone": "success" if rutas_en_ruta else "warning",
            "url": reverse("logistica:rutas") + "?estatus=EN_RUTA",
            "cta": "Ver tránsito",
            "owner": "Logística / Tráfico",
            "depends_on": "Planeación liberada",
            "exit_criteria": "Toda unidad en tránsito debe sostener trazabilidad de entrega.",
            "next_step": "Monitorear rutas activas y registrar avances de reparto.",
            "completion": 100 if rutas_en_ruta else 55,
        },
        {
            "label": "Cierre",
            "count": entregas_pendientes,
            "status": "Por cerrar" if entregas_pendientes else "Sin pendientes",
            "detail": "Entregas que aún requieren cierre operativo.",
            "tone": "danger" if entregas_pendientes else "success",
            "url": reverse("logistica:rutas"),
            "cta": "Cerrar entregas",
            "owner": "Logística / Entrega",
            "depends_on": "Despacho con seguimiento activo",
            "exit_criteria": "Las entregas del día deben cerrar con evidencia documental.",
            "next_step": "Cerrar pendientes antes del corte del reparto.",
            "completion": 100 if entregas_pendientes == 0 else 35,
        },
        {
            "label": "Excepciones",
            "count": incidencias,
            "status": "Con incidencia" if incidencias else "Sin incidencias",
            "detail": "Casos con excepción que frenan cierre completo.",
            "tone": "danger" if incidencias else "success",
            "url": reverse("logistica:rutas"),
            "cta": "Resolver",
            "owner": "Logística / Auditoría",
            "depends_on": "Entregas documentadas",
            "exit_criteria": "Toda incidencia debe quedar resuelta y archivada con trazabilidad.",
            "next_step": "Resolver excepciones y documentar cierre logístico.",
            "completion": 100 if incidencias == 0 else 30,
        },
    ]


def _logistica_release_gate_rows(
    *,
    rutas_total: int,
    rutas_liberadas: int,
    entregas_total: int,
    entregas_controladas: int,
    entregas_cerradas: int,
    incidencias: int,
    base_url: str,
    ruta_id: int | None = None,
) -> list[dict[str, object]]:
    open_rutas = max(rutas_total - rutas_liberadas, 0)
    open_control = max(entregas_total - entregas_controladas, 0)
    open_cierre = max(entregas_total - entregas_cerradas, 0)
    if ruta_id is not None:
        detail_url = reverse("logistica:ruta_detail", kwargs={"pk": ruta_id})
    else:
        detail_url = base_url
    return [
        {
            "step": "01",
            "title": "Ruta liberada y programada",
            "detail": "Rutas con chofer, unidad y fecha operativa listas para ejecución.",
            "completed": rutas_liberadas,
            "open_count": open_rutas,
            "total": max(rutas_total, 1),
            "tone": "success" if open_rutas == 0 else "warning",
            "url": base_url,
            "cta": "Revisar planeación",
        },
        {
            "step": "02",
            "title": "Despacho y tránsito controlado",
            "detail": "Entregas ya confirmadas para seguimiento o cierre documental.",
            "completed": entregas_controladas,
            "open_count": open_control,
            "total": max(entregas_total, 1),
            "tone": "success" if open_control == 0 else "warning",
            "url": detail_url,
            "cta": "Ver seguimiento",
        },
        {
            "step": "03",
            "title": "Cierre e incidencias resueltas",
            "detail": "Entregas cerradas correctamente y excepciones atendidas.",
            "completed": entregas_cerradas,
            "open_count": open_cierre,
            "total": max(entregas_total, 1),
            "tone": "success" if open_cierre == 0 and incidencias == 0 else "danger",
            "url": detail_url,
            "cta": "Cerrar pendientes",
        },
    ]


def _logistica_focus_cards(*, selected_focus: str) -> list[dict[str, object]]:
    today = timezone.localdate()
    point_blocked_routes = (
        RutaEntrega.objects.filter(
            fecha_ruta=today,
            checklist_carga__lineas__estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE,
            checklist_carga__lineas__cantidad_enviada_esperada__lte=0,
        )
        .exclude(estatus__in=[RutaEntrega.ESTATUS_COMPLETADA, RutaEntrega.ESTATUS_CANCELADA])
        .distinct()
        .count()
    )
    focus_defs = [
        {
            "key": "HOY",
            "label": "Rutas del día",
            "count": RutaEntrega.objects.filter(fecha_ruta=today).count(),
            "detail": "Programación activa para la fecha operativa actual.",
            "url": reverse("logistica:rutas") + "?enterprise_focus=HOY",
        },
        {
            "key": "EN_RUTA",
            "label": "Unidades en tránsito",
            "count": RutaEntrega.objects.filter(estatus=RutaEntrega.ESTATUS_EN_RUTA).count(),
            "detail": "Rutas que requieren seguimiento de entrega en tiempo real.",
            "url": reverse("logistica:rutas") + "?enterprise_focus=EN_RUTA",
        },
        {
            "key": "PENDIENTES",
            "label": "Entregas por cerrar",
            "count": EntregaRuta.objects.filter(estatus=EntregaRuta.ESTATUS_PENDIENTE).count(),
            "detail": "Entregas aún sin cierre documental ni confirmación de destino.",
            "url": reverse("logistica:rutas") + "?enterprise_focus=PENDIENTES",
        },
        {
            "key": "INCIDENCIAS",
            "label": "Incidencias abiertas",
            "count": EntregaRuta.objects.filter(estatus=EntregaRuta.ESTATUS_INCIDENCIA).count(),
            "detail": "Excepciones logísticas que bloquean el cierre correcto del circuito.",
            "url": reverse("logistica:rutas") + "?enterprise_focus=INCIDENCIAS",
        },
        {
            "key": "POINT_BLOQUEO",
            "label": "Point sin enviado",
            "count": point_blocked_routes,
            "detail": "Rutas con carga solicitada en Point que aún no aparece como enviada.",
            "url": reverse("logistica:rutas") + "?enterprise_focus=POINT_BLOQUEO",
        },
    ]
    for item in focus_defs:
        item["is_active"] = item["key"] == selected_focus
    return focus_defs


def _logistica_focus_summary(*, selected_focus: str, rutas_count: int) -> dict[str, object] | None:
    if not selected_focus:
        return None
    titles = {
        "HOY": ("Rutas del día", "Vista enfocada en la programación de hoy."),
        "EN_RUTA": ("Unidades en tránsito", "Vista enfocada en rutas actualmente en ejecución."),
        "PENDIENTES": ("Entregas por cerrar", "Vista enfocada en rutas con entregas pendientes."),
        "INCIDENCIAS": ("Incidencias abiertas", "Vista enfocada en rutas con excepciones logísticas."),
    }
    title, detail = titles.get(
        selected_focus,
        ("Foco logístico", "Vista enfocada en un subconjunto operativo de logística."),
    )
    return {
        "title": title,
        "detail": detail,
        "count": rutas_count,
        "clear_url": reverse("logistica:rutas"),
    }


@login_required
def dashboard(request):
    if not can_view_submodule(request.user, "logistica", "dashboard"):
        raise PermissionDenied("No tienes permisos para ver Logística")

    today = timezone.localdate()
    date_from_raw = (request.GET.get("date_from") or "").strip()
    date_to_raw = (request.GET.get("date_to") or "").strip()
    enterprise_focus = (request.GET.get("enterprise_focus") or "").strip().upper()
    q = (request.GET.get("q") or "").strip()
    try:
        date_from = datetime.strptime(date_from_raw, "%Y-%m-%d").date() if date_from_raw else (today - timedelta(days=6))
    except ValueError:
        date_from = today - timedelta(days=6)
    try:
        date_to = datetime.strptime(date_to_raw, "%Y-%m-%d").date() if date_to_raw else today
    except ValueError:
        date_to = today
    if date_from > date_to:
        date_from, date_to = date_to, date_from

    rutas_qs = RutaEntrega.objects.select_related("repartidor__user", "acompanante__user", "unidad_operativa")
    entregas_qs = EntregaRuta.objects.select_related("ruta", "pedido", "pedido__cliente")
    rutas_qs = rutas_qs.filter(fecha_ruta__gte=date_from, fecha_ruta__lte=date_to)
    entregas_qs = entregas_qs.filter(ruta__fecha_ruta__gte=date_from, ruta__fecha_ruta__lte=date_to)
    if q:
        rutas_qs = rutas_qs.filter(
            Q(folio__icontains=q) | Q(nombre__icontains=q) | Q(chofer__icontains=q) | Q(unidad__icontains=q)
        )
        entregas_qs = entregas_qs.filter(
            Q(cliente_nombre__icontains=q)
            | Q(pedido__folio__icontains=q)
            | Q(ruta__folio__icontains=q)
            | Q(ruta__nombre__icontains=q)
            | Q(ruta__chofer__icontains=q)
            | Q(ruta__unidad__icontains=q)
        )
    if enterprise_focus == "HOY":
        rutas_qs = rutas_qs.filter(fecha_ruta=today)
        entregas_qs = entregas_qs.filter(ruta__fecha_ruta=today)
    elif enterprise_focus == "EN_RUTA":
        rutas_qs = rutas_qs.filter(estatus=RutaEntrega.ESTATUS_EN_RUTA)
        entregas_qs = entregas_qs.filter(ruta__estatus=RutaEntrega.ESTATUS_EN_RUTA)
    elif enterprise_focus == "PENDIENTES":
        rutas_qs = rutas_qs.filter(entregas__estatus=EntregaRuta.ESTATUS_PENDIENTE).distinct()
        entregas_qs = entregas_qs.filter(estatus=EntregaRuta.ESTATUS_PENDIENTE)
    elif enterprise_focus == "INCIDENCIAS":
        rutas_qs = rutas_qs.filter(entregas__estatus=EntregaRuta.ESTATUS_INCIDENCIA).distinct()
        entregas_qs = entregas_qs.filter(estatus=EntregaRuta.ESTATUS_INCIDENCIA)

    rutas_total = rutas_qs.count()
    rutas_hoy = rutas_qs.filter(fecha_ruta=today).count()
    rutas_en_ruta = rutas_qs.filter(estatus=RutaEntrega.ESTATUS_EN_RUTA).count()
    rutas_completadas = rutas_qs.filter(estatus=RutaEntrega.ESTATUS_COMPLETADA).count()
    rutas_canceladas = rutas_qs.filter(estatus=RutaEntrega.ESTATUS_CANCELADA).count()

    entregas_total = entregas_qs.count()
    entregas_pendientes = entregas_qs.filter(estatus=EntregaRuta.ESTATUS_PENDIENTE).count()
    entregas_en_camino = entregas_qs.filter(estatus=EntregaRuta.ESTATUS_EN_CAMINO).count()
    entregas_entregadas = entregas_qs.filter(estatus=EntregaRuta.ESTATUS_ENTREGADA).count()
    incidencias = entregas_qs.filter(estatus=EntregaRuta.ESTATUS_INCIDENCIA).count()
    monto_visible = sum((row.monto_estimado for row in entregas_qs[:500]), Decimal("0"))

    last_days = []
    for delta in range(6, -1, -1):
        day = today - timedelta(days=delta)
        last_days.append(
            {
                "label": day.strftime("%d %b"),
                "rutas": rutas_qs.filter(fecha_ruta=day).count(),
                "entregas": entregas_qs.filter(ruta__fecha_ruta=day).count(),
            }
        )

    route_status_rows = [
        {"label": "Planeadas", "value": rutas_qs.filter(estatus=RutaEntrega.ESTATUS_PLANEADA).count()},
        {"label": "En ruta", "value": rutas_en_ruta},
        {"label": "Completadas", "value": rutas_completadas},
        {"label": "Canceladas", "value": rutas_canceladas},
    ]
    delivery_status_rows = [
        {"label": "Pendientes", "value": entregas_pendientes},
        {"label": "En camino", "value": entregas_en_camino},
        {"label": "Entregadas", "value": entregas_entregadas},
        {"label": "Incidencia", "value": incidencias},
    ]

    latest_routes = list(rutas_qs.order_by("-fecha_ruta", "-id")[:4])
    incident_routes = [
        row for row in rutas_qs.order_by("-entregas_incidencia", "-total_entregas", "-fecha_ruta")[:4] if row.entregas_incidencia
    ]
    pending_deliveries = list(
        entregas_qs.filter(estatus__in=[EntregaRuta.ESTATUS_PENDIENTE, EntregaRuta.ESTATUS_INCIDENCIA])
        .order_by("-updated_at", "-id")[:4]
    )
    unidades_activas = Unidad.objects.filter(activa=True).count()
    ultimas_unidades = list(Unidad.objects.select_related("sucursal").order_by("-id")[:5])
    dashboard_query = {
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
    }
    if enterprise_focus:
        dashboard_query["enterprise_focus"] = enterprise_focus
    if q:
        dashboard_query["q"] = q

    context = {
        "module_tabs": _module_tabs("dashboard", request.user),
        "rutas_total": rutas_total,
        "rutas_hoy": rutas_hoy,
        "rutas_en_ruta": rutas_en_ruta,
        "rutas_completadas": rutas_completadas,
        "rutas_canceladas": rutas_canceladas,
        "entregas_total": entregas_total,
        "entregas_pendientes": entregas_pendientes,
        "entregas_en_camino": entregas_en_camino,
        "entregas_entregadas": entregas_entregadas,
        "incidencias": incidencias,
        "monto_visible": monto_visible,
        "last_days": last_days,
        "route_status_rows": route_status_rows,
        "delivery_status_rows": delivery_status_rows,
        "latest_routes": latest_routes,
        "incident_routes": incident_routes,
        "pending_deliveries": pending_deliveries,
        "unidades_activas": unidades_activas,
        "ultimas_unidades": ultimas_unidades,
        "has_visible_data": any([rutas_total, entregas_total]),
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "enterprise_focus": enterprise_focus,
        "selected_q": q,
        "focus_cards": _logistica_focus_cards(selected_focus=enterprise_focus),
        "focus_summary": _logistica_focus_summary(selected_focus=enterprise_focus, rutas_count=rutas_total),
        "rutas_url": reverse("logistica:rutas") + f"?{urlencode(dashboard_query)}",
        "rutas_hoy_url": reverse("logistica:rutas") + f"?{urlencode({**dashboard_query, 'enterprise_focus': 'HOY'})}",
        "rutas_en_ruta_url": reverse("logistica:rutas") + f"?{urlencode({**dashboard_query, 'enterprise_focus': 'EN_RUTA'})}",
        "rutas_incidencias_url": reverse("logistica:rutas") + f"?{urlencode({**dashboard_query, 'enterprise_focus': 'INCIDENCIAS'})}",
        "show_logistica_management_links": _can_manage_tickets_logistica(request.user),
    }
    return render(request, "logistica/dashboard.html", context)


def _unidad_payload_from_request(request):
    return {
        "codigo": (request.POST.get("codigo") or "").strip(),
        "descripcion": (request.POST.get("descripcion") or "").strip(),
        "marca": (request.POST.get("marca") or "").strip() or None,
        "modelo": (request.POST.get("modelo") or "").strip() or None,
        "placa": (request.POST.get("placa") or "").strip(),
        "color": (request.POST.get("color") or "").strip() or None,
        "activa": request.POST.get("activa") == "on",
        "sucursal_id": request.POST.get("sucursal") or None,
    }


def _render_unidad_form(request, *, unidad=None, errors=None):
    return render(
        request,
        "logistica/unidad_form.html",
        {
            "module_tabs": _module_tabs("unidades", request.user),
            "unidad": unidad,
            "sucursales": Sucursal.objects.filter(activa=True).order_by("codigo", "nombre"),
            "errors": errors or {},
        },
    )


@login_required
def unidades_list(request):
    if not can_view_submodule(request.user, "logistica", "unidades"):
        raise PermissionDenied("No tienes permisos para ver unidades de Logística")

    query = (request.GET.get("q") or "").strip()
    qs = Unidad.objects.select_related("sucursal").order_by("codigo")
    if query:
        qs = qs.filter(Q(codigo__icontains=query) | Q(placa__icontains=query) | Q(descripcion__icontains=query))
    paginator = Paginator(qs, 10)
    unidades = paginator.get_page(request.GET.get("page"))
    return render(
        request,
        "logistica/unidades_list.html",
        {
            "module_tabs": _module_tabs("unidades", request.user),
            "unidades": unidades,
            "query": query,
        },
    )


def _inspeccion_faltantes_count(inspeccion: InspeccionVehiculo) -> int:
    check_fields = [
        field.name
        for field in InspeccionVehiculo._meta.fields
        if field.name.startswith(("ext_", "int_", "niv_", "est_"))
    ]
    return sum(1 for field in check_fields if not getattr(inspeccion, field))


def _dias_hasta(fecha):
    if not fecha:
        return None
    return (fecha - timezone.localdate()).days


def _decorate_documento(documento):
    if documento:
        documento.dias_restantes = _dias_hasta(documento.fecha_vencimiento)
    return documento


def _decorate_servicio(servicio):
    if servicio:
        servicio.dias_restantes = _dias_hasta(servicio.proxima_fecha)
    return servicio


def _km_recorridos(bitacora: BitacoraSalidaLlegada):
    if bitacora.km_llegada is None:
        return None
    return max(bitacora.km_llegada - bitacora.km_salida, 0)


def _decorate_bitacora(bitacora: BitacoraSalidaLlegada):
    bitacora.km_recorridos = _km_recorridos(bitacora)
    if bitacora.hora_salida and not bitacora.cerrada:
        bitacora.horas_abierta = int((timezone.now() - bitacora.hora_salida).total_seconds() // 3600)
    else:
        bitacora.horas_abierta = 0
    return bitacora


def _checklist_detalle_inspeccion(inspeccion: InspeccionVehiculo) -> list[dict[str, object]]:
    grupos = [
        ("Accesorios exterior", "ext_"),
        ("Accesorios interior", "int_"),
        ("Niveles", "niv_"),
        ("Estética interior", "est_"),
    ]
    detalle = []
    for titulo, prefix in grupos:
        items = []
        for field in InspeccionVehiculo._meta.fields:
            if field.name.startswith(prefix):
                items.append(
                    {
                        "label": field.verbose_name.replace("_", " ").title(),
                        "ok": bool(getattr(inspeccion, field.name)),
                    }
                )
        detalle.append({"titulo": titulo, "items": items})
    return detalle


def _decorate_inspeccion(inspeccion: InspeccionVehiculo):
    inspeccion.faltantes_count = _inspeccion_faltantes_count(inspeccion)
    inspeccion.checklist_detalle = _checklist_detalle_inspeccion(inspeccion)
    return inspeccion


@login_required
def capturas_pwa(request):
    if not can_view_submodule(request.user, "logistica", "capturas"):
        raise PermissionDenied("No tienes permisos para ver capturas de Logística")

    today = timezone.localdate()
    query = (request.GET.get("q") or "").strip()
    tipo = (request.GET.get("tipo") or "todas").strip().lower()

    bitacoras_qs = (
        BitacoraSalidaLlegada.objects.select_related("repartidor__user", "repartidor__user__empleado_rrhh", "unidad")
        .prefetch_related("cargas_combustible")
        .order_by("-hora_salida", "-id")
    )
    inspecciones_qs = InspeccionVehiculo.objects.select_related(
        "repartidor__user",
        "repartidor__user__empleado_rrhh",
        "unidad",
    ).order_by("-fecha", "-id")
    reportes_qs = ReporteUnidad.objects.select_related(
        "repartidor__user",
        "repartidor__user__empleado_rrhh",
        "unidad",
        "asignado_a",
    ).order_by("-fecha_reporte", "-id")

    if query:
        bitacoras_qs = bitacoras_qs.filter(
            Q(folio__icontains=query)
            | Q(unidad__codigo__icontains=query)
            | Q(unidad__placa__icontains=query)
            | Q(repartidor__user__username__icontains=query)
            | Q(repartidor__user__first_name__icontains=query)
            | Q(repartidor__user__last_name__icontains=query)
            | Q(repartidor__user__empleado_rrhh__nombre__icontains=query)
        )
        inspecciones_qs = inspecciones_qs.filter(
            Q(unidad__codigo__icontains=query)
            | Q(unidad__placa__icontains=query)
            | Q(repartidor__user__username__icontains=query)
            | Q(repartidor__user__first_name__icontains=query)
            | Q(repartidor__user__last_name__icontains=query)
            | Q(repartidor__user__empleado_rrhh__nombre__icontains=query)
        )
        reportes_qs = reportes_qs.filter(
            Q(unidad__codigo__icontains=query)
            | Q(unidad__placa__icontains=query)
            | Q(repartidor__user__username__icontains=query)
            | Q(repartidor__user__first_name__icontains=query)
            | Q(repartidor__user__last_name__icontains=query)
            | Q(repartidor__user__empleado_rrhh__nombre__icontains=query)
            | Q(descripcion__icontains=query)
            | Q(proveedor_servicio__icontains=query)
        )

    bitacoras_page = Paginator(bitacoras_qs, 12).get_page(request.GET.get("bitacoras_page"))
    inspecciones_page = Paginator(inspecciones_qs, 12).get_page(request.GET.get("inspecciones_page"))
    reportes_page = Paginator(reportes_qs, 12).get_page(request.GET.get("reportes_page"))

    inspecciones_rows = []
    for inspeccion in inspecciones_page:
        inspeccion.faltantes_count = _inspeccion_faltantes_count(inspeccion)
        inspecciones_rows.append(inspeccion)

    context = {
        "module_tabs": _module_tabs("capturas", request.user),
        "query": query,
        "tipo": tipo,
        "today": today,
        "bitacoras": bitacoras_page,
        "inspecciones": inspecciones_rows,
        "inspecciones_page": inspecciones_page,
        "reportes": reportes_page,
        "bitacoras_hoy": BitacoraSalidaLlegada.objects.filter(fecha=today).count(),
        "turnos_abiertos": BitacoraSalidaLlegada.objects.filter(cerrada=False).count(),
        "inspecciones_hoy": InspeccionVehiculo.objects.filter(fecha__date=today).count(),
        "reportes_abiertos": ReporteUnidad.objects.exclude(estatus=ReporteUnidad.ESTATUS_CERRADO).count(),
        "costo_combustible_total": (
            (BitacoraSalidaLlegada.objects.aggregate(total=Sum("costo_combustible")).get("total") or Decimal("0"))
            + (CargaCombustibleUnidad.objects.aggregate(total=Sum("importe_total")).get("total") or Decimal("0"))
        ),
        "reportes_por_estatus": ReporteUnidad.objects.values("estatus").annotate(total=Count("id")).order_by("estatus"),
    }
    return render(request, "logistica/capturas_pwa.html", context)


@login_required
def unidad_create(request):
    if not _can_manage_unidades(request.user):
        raise PermissionDenied("No tienes permisos para gestionar unidades de Logística")

    if request.method == "POST":
        payload = _unidad_payload_from_request(request)
        errors = {}
        if not payload["codigo"]:
            errors["codigo"] = "El código es obligatorio."
        if not payload["descripcion"]:
            errors["descripcion"] = "La descripción es obligatoria."
        if not payload["sucursal_id"]:
            errors["sucursal"] = "La sucursal es obligatoria."
        if Unidad.objects.filter(codigo=payload["codigo"]).exists():
            errors["codigo"] = "Ya existe una unidad con este código."
        if not errors:
            Unidad.objects.create(**payload)
            messages.success(request, "Unidad creada correctamente.")
            return redirect("logistica:unidades_list")
        unidad = Unidad(**{key: value for key, value in payload.items() if key != "sucursal_id"})
        unidad.sucursal_id = payload["sucursal_id"]
        return _render_unidad_form(request, unidad=unidad, errors=errors)

    return _render_unidad_form(request)


@login_required
def unidad_edit(request, pk):
    if not _can_manage_unidades(request.user):
        raise PermissionDenied("No tienes permisos para gestionar unidades de Logística")

    unidad = get_object_or_404(Unidad, pk=pk)
    if request.method == "POST":
        payload = _unidad_payload_from_request(request)
        errors = {}
        if not payload["codigo"]:
            errors["codigo"] = "El código es obligatorio."
        if not payload["descripcion"]:
            errors["descripcion"] = "La descripción es obligatoria."
        if not payload["sucursal_id"]:
            errors["sucursal"] = "La sucursal es obligatoria."
        if Unidad.objects.filter(codigo=payload["codigo"]).exclude(pk=unidad.pk).exists():
            errors["codigo"] = "Ya existe una unidad con este código."
        if not errors:
            for field, value in payload.items():
                setattr(unidad, field, value)
            unidad.save()
            messages.success(request, "Unidad actualizada correctamente.")
            return redirect("logistica:unidades_list")
        for field, value in payload.items():
            setattr(unidad, field, value)
        return _render_unidad_form(request, unidad=unidad, errors=errors)

    return _render_unidad_form(request, unidad=unidad)


@login_required
def unidad_toggle(request, pk):
    if not _can_manage_unidades(request.user):
        raise PermissionDenied("No tienes permisos para gestionar unidades de Logística")
    if request.method != "POST":
        return redirect("logistica:unidades_list")
    unidad = get_object_or_404(Unidad, pk=pk)
    unidad.activa = not unidad.activa
    unidad.save(update_fields=["activa"])
    messages.success(request, f"Unidad {unidad.codigo} {'activada' if unidad.activa else 'desactivada'}.")
    return redirect("logistica:unidades_list")


@login_required
def control_rutas(request):
    if not can_view_submodule(request.user, "logistica", "rutas"):
        raise PermissionDenied("No tienes permisos para ver control de rutas")

    fecha_raw = (request.GET.get("fecha") or "").strip()
    query = (request.GET.get("q") or "").strip()
    evento_tipo = (request.GET.get("evento") or "").strip().upper()
    tipos_evento_validos = {choice[0] for choice in EventoRuta.TIPO_CHOICES}
    if evento_tipo not in tipos_evento_validos:
        evento_tipo = ""
    fecha = _parse_date(fecha_raw) or timezone.localdate()
    control = resumen_control_rutas(fecha=fecha, limit=80)
    rutas_control = control["rutas"]
    if query:
        rutas_control = [
            row
            for row in rutas_control
            if query.lower()
            in " ".join(
                [
                    row["ruta"].nombre or "",
                    row["ruta"].folio or "",
                    str(row["ruta"].repartidor or ""),
                    str(row["ruta"].unidad_operativa or ""),
                    row["ruta"].chofer or "",
                    row["ruta"].unidad or "",
                ]
            ).lower()
        ]
    ruta_ids = [row["ruta"].id for row in rutas_control]
    eventos_qs = EventoRuta.objects.select_related("ruta", "parada__punto", "creado_por").filter(ruta__fecha_ruta=fecha)
    if query:
        eventos_qs = eventos_qs.filter(ruta_id__in=ruta_ids)
    eventos_metricas_qs = eventos_qs
    if evento_tipo:
        eventos_qs = eventos_qs.filter(tipo=evento_tipo)
    eventos = eventos_qs.order_by("-creado_en", "-id")[:80]
    paradas_pendientes = (
        ParadaRuta.objects.select_related("ruta", "punto")
        .filter(ruta__fecha_ruta=fecha)
        .exclude(estado=ParadaRuta.ESTADO_VISITADA)
    )
    if query:
        paradas_pendientes = paradas_pendientes.filter(ruta_id__in=ruta_ids)
    geocercas_programadas = sum(row["paradas_total"] for row in rutas_control)
    geocercas_visitadas = sum(row["paradas_visitadas"] for row in rutas_control)
    context = {
        "module_tabs": _module_tabs("control_rutas", request.user),
        "revisiones_globales_count": _revisiones_globales_count(),
        "fecha": fecha,
        "fecha_iso": fecha.isoformat(),
        "control": {**control, "rutas": rutas_control},
        "mapa_rutas": _control_rutas_mapa_payload(rutas_control, eventos_metricas_qs),
        "eventos": eventos,
        "paradas_pendientes": paradas_pendientes[:60],
        "can_manage_logistica": can_manage_submodule(request.user, "logistica", "rutas"),
        "query": query,
        "evento_tipo": evento_tipo,
        "evento_choices": EventoRuta.TIPO_CHOICES,
        "metricas": {
            "rutas": len(rutas_control),
            "desvios": eventos_metricas_qs.filter(tipo=EventoRuta.TIPO_DESVIO).count(),
            "gps_perdido": eventos_metricas_qs.filter(tipo=EventoRuta.TIPO_GPS_PERDIDO).count(),
            "eventos_criticos": eventos_metricas_qs.filter(severidad=EventoRuta.SEVERIDAD_CRITICA).count(),
            "geocercas_programadas": geocercas_programadas,
            "geocercas_visitadas": geocercas_visitadas,
            "paradas_pendientes": paradas_pendientes.count(),
        },
    }
    return render(request, "logistica/control_rutas.html", context)


@login_required
def revisiones_entrega(request):
    if not can_manage_submodule(request.user, "logistica", "rutas"):
        raise PermissionDenied("No tienes permisos para revisar entregas de Logística")
    estados = {ParadaRuta.REVISION_PENDIENTE, ParadaRuta.REVISION_RECHAZADA}
    paradas = list(
        ParadaRuta.objects.filter(revision_entrega_estado__in=estados)
        .select_related("ruta", "ruta__repartidor__user", "punto", "entrega_confirmada_por")
        .order_by("-entrega_confirmada_en", "-id")
    )
    alertas = list(
        EventoRuta.objects.filter(tipo=EventoRuta.TIPO_INCONSISTENCIA_ENTREGA)
        .exclude(parada__revision_entrega_estado__in=estados)
        .select_related("ruta", "ruta__repartidor__user", "parada", "parada__punto")
        .order_by("-creado_en", "-id")
    )
    return render(
        request,
        "logistica/revisiones_entrega.html",
        {
            "module_tabs": _module_tabs("revisiones_entrega", request.user),
            "paradas_revision": paradas,
            "alertas_historicas": alertas,
            "revisiones_count": len(paradas) + len(alertas),
        },
    )


def _coord_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _revisiones_globales_count():
    return (
        ParadaRuta.objects.filter(
            revision_entrega_estado__in=[ParadaRuta.REVISION_PENDIENTE, ParadaRuta.REVISION_RECHAZADA]
        ).count()
        + EventoRuta.objects.filter(tipo=EventoRuta.TIPO_INCONSISTENCIA_ENTREGA)
        .exclude(
            parada__revision_entrega_estado__in=[ParadaRuta.REVISION_PENDIENTE, ParadaRuta.REVISION_RECHAZADA]
        )
        .count()
    )


GPS_TRACE_GAP_SECONDS = 3 * 60
GPS_TRACE_DISCARD_EVENTS = {
    EventoRuta.TIPO_GPS_PRECISION_BAJA: "precision_baja",
    EventoRuta.TIPO_UBICACION_TARDIA: "ubicacion_tardia",
    EventoRuta.TIPO_SALTO_IMPOSIBLE: "salto_imposible",
}


def _gps_trace_flags(eventos_qs, rutas_ids: list[int]) -> dict[int, list[str]]:
    flags_by_location = {}
    if not rutas_ids:
        return flags_by_location
    eventos = eventos_qs.filter(
        ruta_id__in=rutas_ids,
        ubicacion_id__isnull=False,
        tipo__in=GPS_TRACE_DISCARD_EVENTS,
    ).values_list("ubicacion_id", "tipo")
    for ubicacion_id, tipo in eventos:
        flags_by_location.setdefault(ubicacion_id, []).append(GPS_TRACE_DISCARD_EVENTS[tipo])
    return flags_by_location


def _build_gps_trace_segments(ruta_id: int, ubicaciones: list[dict]) -> list[dict]:
    segments = []
    current = []

    def close_segment():
        nonlocal current
        if len(current) > 1:
            raw_coords = [(point["lat"], point["lng"]) for point in current]
            snapped = snap_gps_path_to_roads(ruta_id=ruta_id, coords=raw_coords)
            segments.append(
                {
                    "coords": [{"lat": lat, "lng": lng} for lat, lng in snapped.coordinates],
                    "fuente": snapped.source,
                    "warning": snapped.warning,
                    "estado": "fuera_geocerca" if current[0]["fuera_geocerca"] else "normal",
                }
            )
        current = []

    previous = None
    for point in ubicaciones:
        if point["trazo_descartado"]:
            close_segment()
            previous = None
            continue
        if previous:
            gap_seconds = (point["timestamp_dt"] - previous["timestamp_dt"]).total_seconds()
            if gap_seconds > GPS_TRACE_GAP_SECONDS or point["fuera_geocerca"] != previous["fuera_geocerca"]:
                close_segment()
        current.append(point)
        previous = point
    close_segment()
    return segments


def _control_rutas_mapa_payload(rutas_control, eventos_qs) -> dict:
    colors = ["#1769c2", "#8b1740", "#2f9e44", "#f08c00", "#6f42c1", "#0f766e", "#d82424", "#4b5563"]
    routes = []
    route_ids = [row["ruta"].id for row in rutas_control[:12]]
    gps_flags = _gps_trace_flags(eventos_qs, route_ids)
    for index, row in enumerate(rutas_control[:12]):
        ruta = row["ruta"]
        paradas = []
        for parada in ruta.paradas.all():
            lat = _coord_float(parada.latitud_geocerca)
            lng = _coord_float(parada.longitud_geocerca)
            if lat is None or lng is None:
                continue
            paradas.append(
                {
                    "id": parada.id,
                    "orden": parada.orden,
                    "nombre": parada.punto_nombre_snapshot or parada.punto.nombre,
                    "estado": parada.estado,
                    "entrega_estado": parada.entrega_estado,
                    "lat": lat,
                    "lng": lng,
                    "radio_metros": parada.radio_geocerca_metros,
                }
            )
        paradas.sort(key=lambda item: item["orden"])

        ubicaciones = []
        ubicaciones_qs = list(ruta.ubicaciones.order_by("-timestamp_servidor", "-id").only(
            "id",
            "latitud",
            "longitud",
            "precision_metros",
            "timestamp_servidor",
            "fuera_de_geocerca",
        )[:300])
        ubicaciones_qs.reverse()
        for ubicacion in ubicaciones_qs:
            lat = _coord_float(ubicacion.latitud)
            lng = _coord_float(ubicacion.longitud)
            if lat is None or lng is None:
                continue
            alertas_tracking = gps_flags.get(ubicacion.id, [])
            ubicaciones.append(
                {
                    "lat": lat,
                    "lng": lng,
                    "fuera_geocerca": ubicacion.fuera_de_geocerca,
                    "precision_metros": _coord_float(ubicacion.precision_metros),
                    "alertas_tracking": alertas_tracking,
                    "trazo_descartado": bool(alertas_tracking),
                    "timestamp": ubicacion.timestamp_servidor.isoformat(),
                    "timestamp_dt": ubicacion.timestamp_servidor,
                    "hora": timezone.localtime(ubicacion.timestamp_servidor).strftime("%H:%M"),
                }
            )
        ubicaciones_segmentos = _build_gps_trace_segments(ruta.id, ubicaciones)
        ubicaciones_snapped = [point for segment in ubicaciones_segmentos for point in segment["coords"]]
        ubicaciones_snapped_fuente = "GOOGLE_ROADS" if any(
            segment["fuente"] == "GOOGLE_ROADS" for segment in ubicaciones_segmentos
        ) else ("RAW" if ubicaciones_segmentos else "")
        ubicaciones_snapped_warning = next((segment["warning"] for segment in ubicaciones_segmentos if segment["warning"]), "")
        for ubicacion in ubicaciones:
            ubicacion.pop("timestamp_dt", None)

        routes.append(
            {
                "id": ruta.id,
                "folio": ruta.folio,
                "nombre": ruta.nombre,
                "estatus": ruta.estatus,
                "color": colors[index % len(colors)],
                "programada_polyline": ruta.ruta_programada_polyline or "",
                "programada_fuente": ruta.ruta_programada_fuente or "",
                "programada_distancia_metros": ruta.ruta_programada_distancia_metros,
                "programada_duracion_segundos": ruta.ruta_programada_duracion_segundos,
                "paradas": paradas,
                "ubicaciones": ubicaciones,
                "ubicaciones_segmentos": ubicaciones_segmentos,
                "ubicaciones_snapped": ubicaciones_snapped,
                "ubicaciones_snapped_fuente": ubicaciones_snapped_fuente,
                "ubicaciones_snapped_warning": ubicaciones_snapped_warning,
            }
        )

    eventos = []
    for evento in eventos_qs.exclude(latitud__isnull=True).exclude(longitud__isnull=True).order_by("-creado_en", "-id")[:80]:
        lat = _coord_float(evento.latitud)
        lng = _coord_float(evento.longitud)
        if lat is None or lng is None:
            continue
        eventos.append(
            {
                "id": evento.id,
                "ruta_id": evento.ruta_id,
                "tipo": evento.tipo,
                "severidad": evento.severidad,
                "descripcion": evento.descripcion,
                "lat": lat,
                "lng": lng,
                "hora": timezone.localtime(evento.creado_en).strftime("%H:%M"),
            }
        )

    return {
        "routes": routes,
        "eventos": eventos,
        "tiles": {
            "url": "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
            "attribution": "© OpenStreetMap contributors",
        },
    }


def _punto_logistico_payload(request):
    errors = {}
    nombre = (request.POST.get("nombre") or "").strip()
    tipo = (request.POST.get("tipo") or PuntoLogistico.TIPO_SUCURSAL).strip().upper()
    sucursal_id = (request.POST.get("sucursal") or "").strip()
    latitud_raw = (request.POST.get("latitud") or "").strip()
    longitud_raw = (request.POST.get("longitud") or "").strip()
    radio_raw = (request.POST.get("radio_geocerca_metros") or "80").strip()
    try:
        latitud = Decimal(latitud_raw)
    except (InvalidOperation, ValueError):
        latitud = Decimal("0")
        errors["latitud"] = "La latitud debe ser un número válido."
    try:
        longitud = Decimal(longitud_raw)
    except (InvalidOperation, ValueError):
        longitud = Decimal("0")
        errors["longitud"] = "La longitud debe ser un número válido."
    try:
        radio = int(radio_raw)
    except (TypeError, ValueError):
        radio = 0
        errors["radio_geocerca_metros"] = "El radio debe ser un número entero."

    if not nombre:
        errors["nombre"] = "El nombre es obligatorio."
    if tipo not in {choice[0] for choice in PuntoLogistico.TIPO_CHOICES}:
        errors["tipo"] = "Tipo inválido."
    if not errors.get("latitud") and latitud == Decimal("0") and not latitud_raw:
        errors["latitud"] = "La latitud es obligatoria."
    if not errors.get("longitud") and longitud == Decimal("0") and not longitud_raw:
        errors["longitud"] = "La longitud es obligatoria."
    if not errors.get("latitud") and not (Decimal("-90") <= latitud <= Decimal("90")):
        errors["latitud"] = "La latitud debe estar entre -90 y 90."
    if not errors.get("longitud") and not (Decimal("-180") <= longitud <= Decimal("180")):
        errors["longitud"] = "La longitud debe estar entre -180 y 180."
    if not errors.get("latitud") and not errors.get("longitud") and latitud == Decimal("0") and longitud == Decimal("0"):
        errors["latitud"] = "Las coordenadas 0,0 no son válidas para operación."
    if not errors.get("radio_geocerca_metros") and (radio < 20 or radio > 1000):
        errors["radio_geocerca_metros"] = "Usa un radio entre 20 y 1000 metros."

    sucursal = Sucursal.objects.filter(pk=int(sucursal_id)).first() if sucursal_id.isdigit() else None
    payload = {
        "nombre": nombre,
        "tipo": tipo,
        "sucursal": sucursal,
        "latitud": latitud,
        "longitud": longitud,
        "radio_geocerca_metros": radio,
        "activo": request.POST.get("activo") == "on",
        "notas": (request.POST.get("notas") or "").strip(),
    }
    return payload, errors


def _punto_logistico_cercano(payload: dict, *, exclude_id: int | None = None):
    qs = PuntoLogistico.objects.filter(activo=True)
    if exclude_id:
        qs = qs.exclude(pk=exclude_id)
    closest = None
    closest_distance = None
    for punto in qs.only("id", "nombre", "latitud", "longitud"):
        distance = distancia_metros(payload["latitud"], payload["longitud"], punto.latitud, punto.longitud)
        if closest_distance is None or distance < closest_distance:
            closest = punto
            closest_distance = distance
    if closest and closest_distance is not None and closest_distance <= 25:
        return closest, closest_distance
    return None, None


@login_required
def puntos_logisticos(request):
    if not can_view_submodule(request.user, "logistica", "rutas"):
        raise PermissionDenied("No tienes permisos para ver puntos logísticos")

    errors = {}
    form_values = {
        "nombre": "",
        "tipo": PuntoLogistico.TIPO_SUCURSAL,
        "sucursal_id": "",
        "latitud": "",
        "longitud": "",
        "radio_geocerca_metros": 80,
        "activo": True,
        "notas": "",
    }
    if request.method == "POST":
        if not can_manage_submodule(request.user, "logistica", "rutas"):
            raise PermissionDenied("No tienes permisos para gestionar puntos logísticos")

        payload, errors = _punto_logistico_payload(request)
        form_values = {
            "nombre": payload["nombre"],
            "tipo": payload["tipo"],
            "sucursal_id": payload["sucursal"].id if payload["sucursal"] else "",
            "latitud": request.POST.get("latitud") or "",
            "longitud": request.POST.get("longitud") or "",
            "radio_geocerca_metros": request.POST.get("radio_geocerca_metros") or 80,
            "activo": payload["activo"],
            "notas": payload["notas"],
        }

        if not errors:
            cercano, distancia = _punto_logistico_cercano(payload)
            if cercano:
                errors["punto_cercano"] = f"Ya existe un punto activo a {distancia} m: {cercano.nombre}."
                errors["punto_cercano_url"] = reverse("logistica:punto_logistico_edit", kwargs={"pk": cercano.id})

        if not errors:
            punto = PuntoLogistico.objects.create(**payload)
            log_event(
                request.user,
                "CREATE",
                "logistica.PuntoLogistico",
                str(punto.id),
                {"nombre": punto.nombre, "tipo": punto.tipo, "sucursal": punto.sucursal_id},
            )
            messages.success(request, f"Punto logístico {punto.nombre} creado.")
            return redirect("logistica:puntos_logisticos")

    q = (request.GET.get("q") or "").strip()
    tipo = (request.GET.get("tipo") or "").strip().upper()
    puntos_qs = (
        PuntoLogistico.objects.select_related("sucursal")
        .annotate(
            rutas_abiertas_count=Count(
                "paradas_ruta__ruta",
                filter=Q(paradas_ruta__ruta__estatus__in=[RutaEntrega.ESTATUS_PLANEADA, RutaEntrega.ESTATUS_EN_RUTA]),
                distinct=True,
            ),
            rutas_total_count=Count("paradas_ruta__ruta", distinct=True),
        )
        .order_by("tipo", "nombre")
    )
    if q:
        puntos_qs = puntos_qs.filter(Q(nombre__icontains=q) | Q(sucursal__nombre__icontains=q) | Q(notas__icontains=q))
    if tipo:
        puntos_qs = puntos_qs.filter(tipo=tipo)

    context = {
        "module_tabs": _module_tabs("puntos_logisticos", request.user),
        "can_manage_logistica": can_manage_submodule(request.user, "logistica", "rutas"),
        "puntos": puntos_qs[:250],
        "sucursales": Sucursal.objects.filter(activa=True).order_by("codigo", "nombre"),
        "tipo_choices": PuntoLogistico.TIPO_CHOICES,
        "q": q,
        "tipo": tipo,
        "errors": errors,
        "form_values": form_values,
    }
    return render(request, "logistica/puntos_logisticos.html", context)


@login_required
def punto_logistico_edit(request, pk: int):
    if not can_manage_submodule(request.user, "logistica", "rutas"):
        raise PermissionDenied("No tienes permisos para gestionar puntos logísticos")

    punto = get_object_or_404(PuntoLogistico.objects.select_related("sucursal"), pk=pk)
    errors = {}
    rutas_abiertas_count = punto.paradas_ruta.filter(
        ruta__estatus__in=[RutaEntrega.ESTATUS_PLANEADA, RutaEntrega.ESTATUS_EN_RUTA]
    ).values("ruta_id").distinct().count()
    if request.method == "POST":
        payload, errors = _punto_logistico_payload(request)
        if punto.activo and payload.get("activo") is False and rutas_abiertas_count:
            errors["activo"] = "No se puede desactivar: este punto está en rutas planeadas o en ruta."
        if not errors:
            cercano, distancia = _punto_logistico_cercano(payload, exclude_id=punto.id)
            if cercano:
                errors["punto_cercano"] = f"Ya existe un punto activo a {distancia} m: {cercano.nombre}."
                errors["punto_cercano_url"] = reverse("logistica:punto_logistico_edit", kwargs={"pk": cercano.id})
        if not errors:
            before = {
                "nombre": punto.nombre,
                "tipo": punto.tipo,
                "sucursal": punto.sucursal_id,
                "latitud": str(punto.latitud),
                "longitud": str(punto.longitud),
                "radio": punto.radio_geocerca_metros,
                "activo": punto.activo,
            }
            for field, value in payload.items():
                setattr(punto, field, value)
            punto.save()
            log_event(
                request.user,
                "UPDATE",
                "logistica.PuntoLogistico",
                str(punto.id),
                {"before": before, "after": {"nombre": punto.nombre, "tipo": punto.tipo, "sucursal": punto.sucursal_id, "activo": punto.activo}},
            )
            messages.success(request, f"Punto logístico {punto.nombre} actualizado.")
            return redirect("logistica:puntos_logisticos")
        for field, value in payload.items():
            setattr(punto, field, value)

    context = {
        "module_tabs": _module_tabs("puntos_logisticos", request.user),
        "punto": punto,
        "rutas_abiertas_count": rutas_abiertas_count,
        "sucursales": Sucursal.objects.filter(activa=True).order_by("codigo", "nombre"),
        "tipo_choices": PuntoLogistico.TIPO_CHOICES,
        "errors": errors,
    }
    return render(request, "logistica/punto_logistico_form.html", context)


@login_required
def punto_logistico_toggle(request, pk: int):
    if not can_manage_submodule(request.user, "logistica", "rutas"):
        raise PermissionDenied("No tienes permisos para gestionar puntos logísticos")
    if request.method != "POST":
        return redirect("logistica:puntos_logisticos")
    punto = get_object_or_404(PuntoLogistico, pk=pk)
    if punto.activo and punto.paradas_ruta.filter(
        ruta__estatus__in=[RutaEntrega.ESTATUS_PLANEADA, RutaEntrega.ESTATUS_EN_RUTA]
    ).exists():
        messages.error(request, "No se puede desactivar: este punto está en rutas planeadas o en ruta.")
        return redirect("logistica:puntos_logisticos")
    if not punto.activo:
        cercano, distancia = _punto_logistico_cercano(
            {
                "latitud": punto.latitud,
                "longitud": punto.longitud,
            },
            exclude_id=punto.id,
        )
        if cercano:
            messages.error(request, f"No se puede activar: ya existe un punto activo a {distancia} m: {cercano.nombre}.")
            return redirect("logistica:puntos_logisticos")
    punto.activo = not punto.activo
    punto.save(update_fields=["activo", "actualizado_en"])
    log_event(
        request.user,
        "UPDATE",
        "logistica.PuntoLogistico",
        str(punto.id),
        {"activo": punto.activo},
    )
    messages.success(request, f"Punto {punto.nombre} {'activado' if punto.activo else 'desactivado'}.")
    return redirect("logistica:puntos_logisticos")


@login_required
def rutas(request):
    if not can_view_submodule(request.user, "logistica", "rutas"):
        raise PermissionDenied("No tienes permisos para ver Logística")

    if request.method == "POST":
        if not can_manage_submodule(request.user, "logistica", "rutas"):
            raise PermissionDenied("No tienes permisos para gestionar Logística")

        nombre = (request.POST.get("nombre") or "").strip()
        puntos_ruta_ids = [value for value in request.POST.getlist("puntos_ruta") if str(value).isdigit()]
        if not nombre:
            messages.error(request, "El nombre de ruta es obligatorio.")
        elif not puntos_ruta_ids:
            messages.error(request, "Selecciona al menos una sucursal o punto para planear la ruta del día.")
        else:
            repartidor_id = (request.POST.get("repartidor") or "").strip()
            acompanante_id = (request.POST.get("acompanante") or "").strip()
            unidad_id = (request.POST.get("unidad_operativa") or "").strip()
            repartidor = Repartidor.objects.filter(pk=int(repartidor_id), user__is_active=True).first() if repartidor_id.isdigit() else None
            acompanante = Repartidor.objects.filter(pk=int(acompanante_id), user__is_active=True).first() if acompanante_id.isdigit() else None
            unidad_operativa = Unidad.objects.filter(pk=int(unidad_id), activa=True).first() if unidad_id.isdigit() else None
            puntos = PuntoLogistico.objects.filter(pk__in=puntos_ruta_ids, activo=True)
            puntos_by_id = {str(punto.id): punto for punto in puntos}
            puntos_ordenados = []
            puntos_duplicados = 0
            puntos_vistos = set()
            for posicion_formulario, value in enumerate(puntos_ruta_ids, start=1):
                if value in puntos_vistos:
                    puntos_duplicados += 1
                    continue
                puntos_vistos.add(value)
                punto = puntos_by_id.get(value)
                if not punto:
                    continue
                try:
                    orden_usuario = int(request.POST.get(f"punto_orden_{value}") or posicion_formulario)
                except (TypeError, ValueError):
                    orden_usuario = posicion_formulario
                if orden_usuario < 1:
                    orden_usuario = posicion_formulario
                puntos_ordenados.append((orden_usuario, posicion_formulario, punto))
            puntos_ordenados.sort(key=lambda item: (item[0], item[1]))
            if not puntos_ordenados:
                messages.error(request, "Los puntos seleccionados no están activos. Revisa el catálogo de puntos logísticos.")
                return redirect("logistica:rutas")

            fecha_ruta = _parse_date(request.POST.get("fecha_ruta")) or timezone.localdate()
            sucursales_repetidas = {
                punto.sucursal_id
                for _, _, punto in puntos_ordenados
                if punto.tipo != PuntoLogistico.TIPO_CEDIS and punto.sucursal_id
            }
            sucursales_repetidas = set(
                RutaEntrega.objects.filter(
                    fecha_ruta=fecha_ruta,
                    paradas__punto__sucursal_id__in=sucursales_repetidas,
                )
                .exclude(estatus=RutaEntrega.ESTATUS_CANCELADA)
                .values_list("paradas__punto__sucursal_id", flat=True)
            )
            puntos_repetidos = [punto for _, _, punto in puntos_ordenados if punto.sucursal_id in sucursales_repetidas]
            if sucursales_repetidas and not ruta_tiene_movimiento_point_nuevo(
                fecha=fecha_ruta,
                puntos=puntos_repetidos,
            ):
                messages.error(request, "Ya existe ruta del día para esa sucursal y no hay transferencia Point nueva para otra vuelta.")
                return redirect("logistica:rutas")

            with transaction.atomic():
                ruta = RutaEntrega.objects.create(
                    nombre=nombre,
                    fecha_ruta=fecha_ruta,
                    chofer=(request.POST.get("chofer") or "").strip() or (str(repartidor) if repartidor else ""),
                    unidad=(request.POST.get("unidad") or "").strip() or (str(unidad_operativa) if unidad_operativa else ""),
                    repartidor=repartidor,
                    acompanante=acompanante,
                    acompanante_manual=(request.POST.get("acompanante_manual") or "").strip(),
                    unidad_operativa=unidad_operativa,
                    estatus=RutaEntrega.ESTATUS_PLANEADA,
                    km_estimado=_parse_decimal(request.POST.get("km_estimado")),
                    notas=(request.POST.get("notas") or "").strip(),
                    created_by=request.user,
                )
                for orden, (_, __, punto) in enumerate(puntos_ordenados, start=1):
                    ParadaRuta.objects.create(ruta=ruta, punto=punto, orden=orden)
            recalcular_ruta_programada(ruta)
            if (request.POST.get("estatus") or "").strip().upper() == RutaEntrega.ESTATUS_EN_RUTA:
                messages.warning(request, "La ruta se creó como planeada. Agrega paradas antes de liberarla para seguimiento.")
            if puntos_duplicados:
                messages.info(request, "Se ignoraron puntos repetidos; cada sucursal o punto quedó una sola vez en la ruta.")
            log_event(
                request.user,
                "CREATE",
                "logistica.RutaEntrega",
                str(ruta.id),
                {
                    "folio": ruta.folio,
                    "nombre": ruta.nombre,
                    "fecha_ruta": str(ruta.fecha_ruta),
                    "estatus": ruta.estatus,
                },
            )
            messages.success(request, f"Ruta {ruta.folio} creada con {len(puntos_ordenados)} paradas ordenadas.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

    q = (request.GET.get("q") or "").strip()
    estatus = (request.GET.get("estatus") or "").strip().upper()
    enterprise_focus = (request.GET.get("enterprise_focus") or "").strip().upper()
    date_from_raw = (request.GET.get("date_from") or "").strip()
    date_to_raw = (request.GET.get("date_to") or "").strip()

    today = timezone.localdate()
    try:
        date_from = datetime.strptime(date_from_raw, "%Y-%m-%d").date() if date_from_raw else None
    except ValueError:
        date_from = None
    try:
        date_to = datetime.strptime(date_to_raw, "%Y-%m-%d").date() if date_to_raw else None
    except ValueError:
        date_to = None
    if date_from and date_to and date_from > date_to:
        date_from, date_to = date_to, date_from

    rutas_qs = RutaEntrega.objects.all()
    if date_from:
        rutas_qs = rutas_qs.filter(fecha_ruta__gte=date_from)
    if date_to:
        rutas_qs = rutas_qs.filter(fecha_ruta__lte=date_to)
    if q:
        rutas_qs = rutas_qs.filter(
            Q(folio__icontains=q)
            | Q(nombre__icontains=q)
            | Q(chofer__icontains=q)
            | Q(acompanante_manual__icontains=q)
            | Q(acompanante__user__first_name__icontains=q)
            | Q(acompanante__user__last_name__icontains=q)
            | Q(acompanante__user__username__icontains=q)
            | Q(unidad__icontains=q)
        )
    if estatus:
        rutas_qs = rutas_qs.filter(estatus=estatus)
    if enterprise_focus == "HOY":
        rutas_qs = rutas_qs.filter(fecha_ruta=timezone.localdate())
    elif enterprise_focus == "EN_RUTA":
        rutas_qs = rutas_qs.filter(estatus=RutaEntrega.ESTATUS_EN_RUTA)
    elif enterprise_focus == "PENDIENTES":
        rutas_qs = rutas_qs.filter(
            paradas__entrega_estado=ParadaRuta.ENTREGA_PENDIENTE,
        ).exclude(paradas__punto__tipo=PuntoLogistico.TIPO_CEDIS).distinct()
    elif enterprise_focus == "INCIDENCIAS":
        rutas_qs = rutas_qs.filter(
            paradas__entrega_estado__in=[ParadaRuta.ENTREGA_CON_DIFERENCIA, ParadaRuta.ENTREGA_NO_ENTREGADA],
        ).exclude(paradas__punto__tipo=PuntoLogistico.TIPO_CEDIS).distinct()
    elif enterprise_focus == "POINT_BLOQUEO":
        rutas_qs = (
            rutas_qs.filter(
                checklist_carga__lineas__estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE,
                checklist_carga__lineas__cantidad_enviada_esperada__lte=0,
            )
            .exclude(estatus__in=[RutaEntrega.ESTATUS_COMPLETADA, RutaEntrega.ESTATUS_CANCELADA])
            .distinct()
        )

    rutas_total = RutaEntrega.objects.count()
    rutas_hoy = RutaEntrega.objects.filter(fecha_ruta=today).count()
    rutas_en_ruta = RutaEntrega.objects.filter(estatus=RutaEntrega.ESTATUS_EN_RUTA).count()
    entregas_pendientes = ParadaRuta.objects.exclude(punto__tipo=PuntoLogistico.TIPO_CEDIS).filter(entrega_estado=ParadaRuta.ENTREGA_PENDIENTE).count()
    incidencias = ParadaRuta.objects.exclude(punto__tipo=PuntoLogistico.TIPO_CEDIS).filter(
        entrega_estado__in=[ParadaRuta.ENTREGA_CON_DIFERENCIA, ParadaRuta.ENTREGA_NO_ENTREGADA]
    ).count()
    entregas_total = ParadaRuta.objects.exclude(punto__tipo=PuntoLogistico.TIPO_CEDIS).count()
    rutas_liberadas = RutaEntrega.objects.exclude(estatus=RutaEntrega.ESTATUS_CANCELADA).filter(
        repartidor__isnull=False,
        unidad_operativa__isnull=False,
    ).count()
    entregas_controladas = ParadaRuta.objects.exclude(punto__tipo=PuntoLogistico.TIPO_CEDIS).filter(
        entrega_estado=ParadaRuta.ENTREGA_ENTREGADA
    ).count()
    entregas_cerradas = max(entregas_total - entregas_pendientes - incidencias, 0)
    enterprise_chain = _logistica_enterprise_chain(
        rutas_total=rutas_total,
        rutas_hoy=rutas_hoy,
        rutas_en_ruta=rutas_en_ruta,
        entregas_pendientes=entregas_pendientes,
        incidencias=incidencias,
        entregas_completadas=entregas_controladas,
    )
    document_stage_rows = _logistica_document_stage_rows(
        rutas_total=rutas_total,
        rutas_hoy=rutas_hoy,
        rutas_en_ruta=rutas_en_ruta,
        entregas_total=entregas_total,
        entregas_pendientes=entregas_pendientes,
        incidencias=incidencias,
    )
    maturity_summary = _logistica_maturity_summary(
        chain=enterprise_chain,
        default_url=reverse("logistica:rutas"),
    )
    handoff_map = _logistica_handoff_map(
        rutas_hoy=rutas_hoy,
        rutas_en_ruta=rutas_en_ruta,
        entregas_pendientes=entregas_pendientes,
        incidencias=incidencias,
    )
    release_gate_rows = _logistica_release_gate_rows(
        rutas_total=rutas_total,
        rutas_liberadas=rutas_liberadas,
        entregas_total=entregas_total,
        entregas_controladas=entregas_controladas,
        entregas_cerradas=entregas_cerradas,
        incidencias=incidencias,
        base_url=reverse("logistica:rutas"),
    )
    governance_rows = _logistica_governance_rows(document_stage_rows, owner_default="Logística / Planeación")
    monto_transferido_subquery = (
        RutaCargaChecklistLinea.objects.filter(checklist__ruta=OuterRef("pk"), point_transfer_line__isnull=False)
        .values("checklist__ruta")
        .annotate(
            total=Sum(
                ExpressionWrapper(
                    F("point_transfer_line__sent_quantity") * F("point_transfer_line__unit_cost"),
                    output_field=DecimalField(max_digits=18, decimal_places=2),
                )
            )
        )
        .values("total")[:1]
    )

    context = {
        "module_tabs": _module_tabs("rutas", request.user),
        "revisiones_globales_count": _revisiones_globales_count(),
        "can_manage_logistica": can_manage_submodule(request.user, "logistica", "rutas"),
        "rutas": rutas_qs.annotate(
            paradas_entrega_total=Count("paradas", filter=~Q(paradas__punto__tipo=PuntoLogistico.TIPO_CEDIS), distinct=True),
            paradas_entregadas=Count(
                "paradas",
                filter=Q(paradas__entrega_estado=ParadaRuta.ENTREGA_ENTREGADA) & ~Q(paradas__punto__tipo=PuntoLogistico.TIPO_CEDIS),
                distinct=True,
            ),
            paradas_incidencia=Count(
                "paradas",
                filter=Q(paradas__entrega_estado__in=[ParadaRuta.ENTREGA_CON_DIFERENCIA, ParadaRuta.ENTREGA_NO_ENTREGADA])
                & ~Q(paradas__punto__tipo=PuntoLogistico.TIPO_CEDIS),
                distinct=True,
            ),
            point_bloqueo_lineas=Count(
                "checklist_carga__lineas",
                filter=Q(checklist_carga__lineas__estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
                & Q(checklist_carga__lineas__cantidad_enviada_esperada__lte=0),
                distinct=True,
            ),
            monto_transferido_point=Coalesce(
                Subquery(monto_transferido_subquery, output_field=DecimalField(max_digits=18, decimal_places=2)),
                Decimal("0"),
            ),
        ).order_by("-fecha_ruta", "-id")[:200],
        "q": q,
        "estatus": estatus,
        "enterprise_focus": enterprise_focus,
        "date_from": date_from.isoformat() if date_from else "",
        "date_to": date_to.isoformat() if date_to else "",
        "estatus_choices": RutaEntrega.ESTATUS_CHOICES,
        "repartidores": Repartidor.objects.filter(user__is_active=True).select_related("user", "user__empleado_rrhh", "unidad_asignada").order_by("user__first_name", "user__username"),
        "unidades": Unidad.objects.filter(activa=True).order_by("codigo"),
        "puntos_creacion": PuntoLogistico.objects.filter(activo=True).select_related("sucursal").order_by("tipo", "nombre"),
        "totales": {
            "rutas": rutas_total,
            "hoy": rutas_hoy,
            "en_ruta": rutas_en_ruta,
            "pendientes": entregas_pendientes,
            "incidencias": incidencias,
        },
        "enterprise_chain": enterprise_chain,
        "critical_path_rows": _logistica_critical_path_rows(enterprise_chain),
        "document_stage_rows": document_stage_rows,
        "erp_governance_rows": governance_rows,
        "executive_radar_rows": _logistica_executive_radar_rows(
            governance_rows,
            default_owner="Logística / Planeación",
            fallback_url=reverse("logistica:rutas"),
        ),
        "erp_command_center": _logistica_command_center(
            governance_rows=governance_rows,
            maturity_summary=maturity_summary,
            default_url=reverse("logistica:rutas"),
            default_cta="Abrir rutas",
        ),
        "maturity_summary": maturity_summary,
        "handoff_map": handoff_map,
        "release_gate_rows": release_gate_rows,
        "release_gate_completion": (
            int(
                round(
                    (
                        sum(row["completed"] for row in release_gate_rows)
                        / sum(row["total"] for row in release_gate_rows)
                    )
                    * 100
                )
            )
            if release_gate_rows and sum(row["total"] for row in release_gate_rows)
            else 0
        ),
        "focus_cards": _logistica_focus_cards(selected_focus=enterprise_focus),
        "focus_summary": _logistica_focus_summary(
            selected_focus=enterprise_focus,
            rutas_count=rutas_qs.count(),
        ),
        "operational_health_cards": _logistica_operational_health_cards(
            focus="rutas",
            primary_open=rutas_en_ruta,
            secondary_open=entregas_pendientes,
            ready_count=EntregaRuta.objects.filter(estatus=EntregaRuta.ESTATUS_ENTREGADA).count(),
        ),
    }
    return render(request, "logistica/rutas.html", context)


@login_required
def ruta_detail(request, pk: int):
    if not can_view_submodule(request.user, "logistica", "rutas"):
        raise PermissionDenied("No tienes permisos para ver Logística")

    ruta = get_object_or_404(RutaEntrega, pk=pk)

    if request.method == "POST":
        if not can_manage_submodule(request.user, "logistica", "rutas"):
            raise PermissionDenied("No tienes permisos para gestionar Logística")

        action = (request.POST.get("action") or "").strip().lower()
        ruta_cerrada = ruta.estatus in {RutaEntrega.ESTATUS_COMPLETADA, RutaEntrega.ESTATUS_CANCELADA}
        estructura_actions = {"update_plan", "move_parada", "add_entrega", "entrega_status", "delete_entrega"}
        if ruta_cerrada and action not in {"ruta_status", "sync_recepcion_point", "revisar_entrega"}:
            messages.error(request, "La ruta ya está cerrada o cancelada; no se puede editar su planeación ni evidencia.")
            return redirect("logistica:ruta_detail", pk=ruta.id)
        if ruta.estatus == RutaEntrega.ESTATUS_EN_RUTA and action in estructura_actions:
            messages.error(request, "La ruta ya está en seguimiento; la planeación queda congelada para conservar evidencia.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "revisar_entrega":
            parada_id = (request.POST.get("parada_id") or "").strip()
            parada = get_object_or_404(
                ParadaRuta.objects.select_related("ruta"),
                pk=int(parada_id) if parada_id.isdigit() else 0,
                ruta=ruta,
            )
            try:
                revisar_entrega_excepcional(
                    parada=parada,
                    actor=request.user,
                    decision=(request.POST.get("decision") or "").strip().upper(),
                    motivo=request.POST.get("motivo_revision"),
                )
            except ValidationError as exc:
                messages.error(request, "; ".join(exc.messages) if hasattr(exc, "messages") else str(exc))
            else:
                messages.success(request, "Revisión de entrega registrada con evidencia de auditoría.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "add_entrega":
            pedido = None
            pedido_id = (request.POST.get("pedido_id") or "").strip()
            if pedido_id.isdigit():
                pedido = PedidoCliente.objects.filter(pk=int(pedido_id)).first()
            try:
                secuencia = int(request.POST.get("secuencia") or 1)
            except (TypeError, ValueError):
                messages.error(request, "La secuencia de entrega debe ser un número.")
                return redirect("logistica:ruta_detail", pk=ruta.id)
            if secuencia < 1:
                messages.error(request, "La secuencia de entrega debe ser mayor a cero.")
                return redirect("logistica:ruta_detail", pk=ruta.id)

            entrega = EntregaRuta.objects.create(
                ruta=ruta,
                secuencia=secuencia,
                pedido=pedido,
                cliente_nombre=(request.POST.get("cliente_nombre") or "").strip(),
                direccion=(request.POST.get("direccion") or "").strip(),
                contacto=(request.POST.get("contacto") or "").strip(),
                telefono=(request.POST.get("telefono") or "").strip(),
                ventana_inicio=_parse_datetime_local(request.POST.get("ventana_inicio")),
                ventana_fin=_parse_datetime_local(request.POST.get("ventana_fin")),
                estatus=(request.POST.get("estatus") or EntregaRuta.ESTATUS_PENDIENTE).strip(),
                monto_estimado=_parse_decimal(request.POST.get("monto_estimado")),
                comentario=(request.POST.get("comentario") or "").strip(),
            )
            ruta.recompute_totals()
            ruta.save(update_fields=["total_entregas", "entregas_completadas", "entregas_incidencia", "monto_estimado_total", "updated_at"])
            log_event(
                request.user,
                "CREATE",
                "logistica.EntregaRuta",
                str(entrega.id),
                {
                    "ruta": ruta.folio,
                    "secuencia": entrega.secuencia,
                    "cliente_nombre": entrega.cliente_nombre,
                    "estatus": entrega.estatus,
                },
            )
            messages.success(request, "Entrega agregada.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "update_plan":
            repartidor_id = (request.POST.get("repartidor") or "").strip()
            acompanante_id = (request.POST.get("acompanante") or "").strip()
            unidad_id = (request.POST.get("unidad_operativa") or "").strip()
            repartidor = Repartidor.objects.filter(pk=int(repartidor_id), user__is_active=True).first() if repartidor_id.isdigit() else None
            acompanante = Repartidor.objects.filter(pk=int(acompanante_id), user__is_active=True).first() if acompanante_id.isdigit() else None
            unidad_operativa = Unidad.objects.filter(pk=int(unidad_id), activa=True).first() if unidad_id.isdigit() else None
            if ruta.estatus == RutaEntrega.ESTATUS_EN_RUTA and (
                not repartidor
                or not unidad_operativa
                or repartidor.id != ruta.repartidor_id
                or unidad_operativa.id != ruta.unidad_operativa_id
            ):
                messages.error(request, "No puedes cambiar repartidor o unidad mientras la ruta está en seguimiento.")
                return redirect("logistica:ruta_detail", pk=ruta.id)
            ruta.nombre = (request.POST.get("nombre") or ruta.nombre).strip()
            ruta.fecha_ruta = _parse_date(request.POST.get("fecha_ruta")) or ruta.fecha_ruta
            ruta.repartidor = repartidor
            ruta.acompanante = acompanante
            ruta.acompanante_manual = (request.POST.get("acompanante_manual") or "").strip()
            ruta.unidad_operativa = unidad_operativa
            ruta.chofer = (request.POST.get("chofer") or "").strip() or (str(repartidor) if repartidor else "")
            ruta.unidad = (request.POST.get("unidad") or "").strip() or (str(unidad_operativa) if unidad_operativa else "")
            ruta.km_estimado = _parse_decimal(request.POST.get("km_estimado"))
            ruta.notas = (request.POST.get("notas") or "").strip()
            ruta.save(update_fields=["nombre", "fecha_ruta", "repartidor", "acompanante", "acompanante_manual", "unidad_operativa", "chofer", "unidad", "km_estimado", "notas", "updated_at"])
            log_event(
                request.user,
                "UPDATE",
                "logistica.RutaEntrega",
                str(ruta.id),
                {"folio": ruta.folio, "repartidor": ruta.repartidor_id, "unidad_operativa": ruta.unidad_operativa_id},
            )
            messages.success(request, "Planeación de ruta actualizada.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "confirmar_carga_manual":
            try:
                lineas = confirmar_checklist_carga_manual(
                    ruta=ruta,
                    user=request.user,
                    notas=(request.POST.get("notas_carga_manual") or "").strip(),
                )
            except ValidationError as exc:
                messages.error(request, "; ".join(exc.messages) if hasattr(exc, "messages") else str(exc))
            else:
                messages.success(request, f"Carga manual confirmada: {lineas} línea(s). Ya puedes liberar la ruta.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "confirmar_linea_carga_manual":
            if not can_manage_submodule(request.user, "logistica", "rutas"):
                raise PermissionDenied("No tienes permisos para capturar carga manual.")
            try:
                linea_id = int(request.POST.get("linea_carga_id") or 0)
                validar_linea_carga(
                    user=request.user,
                    ruta=ruta,
                    repartidor=ruta.repartidor,
                    linea_id=linea_id,
                    cantidad_cargada=request.POST.get("cantidad_cargada_manual"),
                    motivo_diferencia=(request.POST.get("motivo_diferencia_manual") or "").strip(),
                    notas=(request.POST.get("notas_carga_manual") or "").strip(),
                )
            except (ValueError, ValidationError) as exc:
                messages.error(request, "; ".join(exc.messages) if hasattr(exc, "messages") else str(exc))
            else:
                messages.success(request, "Línea de carga capturada manualmente.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "registrar_recarga_cedis":
            try:
                evento = registrar_recarga_cedis(
                    ruta=ruta,
                    user=request.user,
                    notas=(request.POST.get("notas_recarga_cedis") or "").strip(),
                )
            except ValidationError as exc:
                messages.error(request, "; ".join(exc.messages) if hasattr(exc, "messages") else str(exc))
            else:
                messages.success(request, evento.descripcion)
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "autorizar_diferencia_carga":
            autorizado = (request.POST.get("autorizado") or "").strip() == "1"
            try:
                autorizar_diferencia_checklist_carga(
                    ruta=ruta,
                    user=request.user,
                    autorizado=autorizado,
                    notas=(request.POST.get("notas_autorizacion_carga") or "").strip(),
                )
            except ValidationError as exc:
                messages.error(request, "; ".join(exc.messages) if hasattr(exc, "messages") else str(exc))
            else:
                messages.success(request, "Ruta autorizada a pesar de la diferencia de carga." if autorizado else "Diferencia de carga rechazada; la ruta sigue bloqueada.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "ajustar_entrega_manual":
            parada_id = (request.POST.get("parada_id") or "").strip()
            entrega_estado = (request.POST.get("entrega_estado") or "").strip().upper()
            nota = (request.POST.get("nota_entrega_manual") or "").strip()
            parada = ruta.paradas.select_related("punto").filter(pk=int(parada_id)).first() if parada_id.isdigit() else None
            estados_validos = {
                ParadaRuta.ENTREGA_ENTREGADA,
                ParadaRuta.ENTREGA_CON_DIFERENCIA,
                ParadaRuta.ENTREGA_NO_ENTREGADA,
            }
            if not parada or parada.punto.tipo == PuntoLogistico.TIPO_CEDIS:
                messages.error(request, "Selecciona una parada de sucursal.")
                return redirect("logistica:ruta_detail", pk=ruta.id)
            if ruta.estatus != RutaEntrega.ESTATUS_EN_RUTA or entrega_estado not in estados_validos:
                messages.error(request, "El ajuste manual solo aplica a rutas en seguimiento.")
                return redirect("logistica:ruta_detail", pk=ruta.id)
            if not nota:
                messages.error(request, "Captura una nota para justificar el ajuste manual.")
                return redirect("logistica:ruta_detail", pk=ruta.id)
            try:
                resultado = confirmar_entrega_parada(
                    ruta=ruta,
                    parada=parada,
                    actor=request.user,
                    entrega_estado=entrega_estado,
                    motivo=nota,
                    client_event_id=f"erp-manual-{uuid4()}",
                    ubicacion={
                        "causa": "AJUSTE_ADMINISTRATIVO",
                        "client_timestamp": timezone.now().isoformat(),
                        "client_version": "erp-ruta-detail",
                    },
                    origen="AJUSTE_ADMIN",
                )
            except (ValidationError, PermissionDenied) as exc:
                messages.error(request, "; ".join(exc.messages) if hasattr(exc, "messages") else str(exc))
                return redirect("logistica:ruta_detail", pk=ruta.id)
            ruta.recompute_route_control()
            ruta.save(update_fields=["cumplimiento_porcentaje", "updated_at"])
            messages.success(
                request,
                f"Entrega ajustada manualmente para {parada.punto_nombre_snapshot}; quedó pendiente de revisión."
                if resultado.requiere_revision
                else f"Entrega ajustada manualmente para {parada.punto_nombre_snapshot}.",
            )
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "cerrar_con_diferencia_autorizada":
            try:
                evento = cerrar_ruta_con_diferencia_autorizada(
                    ruta=ruta,
                    user=request.user,
                    notas=(request.POST.get("notas_cierre_diferencia") or "").strip(),
                )
            except ValidationError as exc:
                messages.error(request, "; ".join(exc.messages) if hasattr(exc, "messages") else str(exc))
            else:
                log_event(
                    request.user,
                    "UPDATE",
                    "logistica.RutaEntrega",
                    str(ruta.id),
                    {"folio": ruta.folio, "accion": "cerrar_con_diferencia_autorizada"},
                )
                messages.warning(request, evento.descripcion)
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "add_parada":
            punto_id = (request.POST.get("punto") or "").strip()
            punto = PuntoLogistico.objects.filter(pk=int(punto_id), activo=True).first() if punto_id.isdigit() else None
            if not punto:
                messages.error(request, "Selecciona un punto logístico activo.")
                return redirect("logistica:ruta_detail", pk=ruta.id)
            if ruta.estatus == RutaEntrega.ESTATUS_EN_RUTA and punto.tipo != PuntoLogistico.TIPO_CEDIS:
                messages.error(request, "La ruta ya está en seguimiento; solo puedes agregar una parada CEDIS para recarga.")
                return redirect("logistica:ruta_detail", pk=ruta.id)
            try:
                orden = int(request.POST.get("orden") or (ruta.paradas.count() + 1))
            except (TypeError, ValueError):
                messages.error(request, "El orden de la parada debe ser un número.")
                return redirect("logistica:ruta_detail", pk=ruta.id)
            if orden < 1:
                messages.error(request, "El orden de la parada debe ser mayor a cero.")
                return redirect("logistica:ruta_detail", pk=ruta.id)
            with transaction.atomic():
                if ruta.paradas.filter(orden=orden).exists():
                    for parada_existente in ruta.paradas.filter(orden__gte=orden).order_by("-orden"):
                        parada_existente.orden += 1
                        parada_existente.save(update_fields=["orden", "actualizado_en"])
                parada = ParadaRuta.objects.create(
                    ruta=ruta,
                    punto=punto,
                    orden=orden,
                    hora_estimada=_parse_datetime_local(request.POST.get("hora_estimada")),
                    notas=(request.POST.get("notas") or "").strip(),
                )
            ruta.recompute_route_control()
            ruta.save(update_fields=["cumplimiento_porcentaje", "updated_at"])
            recalcular_ruta_programada(ruta)
            log_event(
                request.user,
                "CREATE",
                "logistica.ParadaRuta",
                str(parada.id),
                {"ruta": ruta.folio, "punto": punto.nombre, "orden": parada.orden},
            )
            messages.success(request, f"Parada {punto.nombre} agregada.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "move_parada":
            parada_id = (request.POST.get("parada_id") or "").strip()
            direction = (request.POST.get("direction") or "").strip()
            if direction not in {"up", "down"}:
                messages.error(request, "Dirección de movimiento inválida.")
                return redirect("logistica:ruta_detail", pk=ruta.id)
            parada = ParadaRuta.objects.filter(pk=int(parada_id), ruta=ruta).first() if parada_id.isdigit() else None
            if parada:
                target_order = parada.orden - 1 if direction == "up" else parada.orden + 1
                target = ParadaRuta.objects.filter(ruta=ruta, orden=target_order).first()
                if target:
                    with transaction.atomic():
                        temp_order = (ruta.paradas.aggregate(max_orden=Max("orden")).get("max_orden") or 0) + 1000
                        original_order = parada.orden
                        target_original_order = target.orden
                        parada.orden = temp_order
                        parada.save(update_fields=["orden", "actualizado_en"])
                        target.orden = original_order
                        target.save(update_fields=["orden", "actualizado_en"])
                        parada.orden = target_order
                        parada.save(update_fields=["orden", "actualizado_en"])
                    recalcular_ruta_programada(ruta)
                    log_event(
                        request.user,
                        "UPDATE",
                        "logistica.ParadaRuta",
                        str(parada.id),
                        {
                            "ruta": ruta.folio,
                            "parada": parada.punto_nombre_snapshot,
                            "from_orden": original_order,
                            "to_orden": parada.orden,
                            "parada_intercambiada": target.punto_nombre_snapshot,
                            "intercambiada_from_orden": target_original_order,
                            "intercambiada_to_orden": target.orden,
                        },
                    )
                    messages.success(request, "Orden de paradas actualizado.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "delete_parada":
            parada_id = (request.POST.get("parada_id") or "").strip()
            parada = ParadaRuta.objects.filter(pk=int(parada_id), ruta=ruta).first() if parada_id.isdigit() else None
            if parada:
                nombre_punto = parada.punto_nombre_snapshot
                orden_eliminado = parada.orden
                parada_id_log = parada.id
                if ruta.paradas.count() <= 1:
                    messages.error(request, "La ruta debe conservar al menos una parada.")
                    return redirect("logistica:ruta_detail", pk=ruta.id)
                puede_quitarse, motivo = _parada_puede_quitarse(parada)
                if not puede_quitarse:
                    messages.error(request, motivo)
                    return redirect("logistica:ruta_detail", pk=ruta.id)
                with transaction.atomic():
                    parada.lineas_carga.filter(estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE).delete()
                    parada.delete()
                for index, item in enumerate(ruta.paradas.filter(orden__gt=orden_eliminado).order_by("orden", "id"), start=orden_eliminado):
                    item.orden = index
                    item.save(update_fields=["orden", "actualizado_en"])
                ruta.recompute_route_control()
                ruta.save(update_fields=["cumplimiento_porcentaje", "updated_at"])
                recalcular_ruta_programada(ruta)
                log_event(
                    request.user,
                    "DELETE",
                    "logistica.ParadaRuta",
                    str(parada_id_log),
                    {"ruta": ruta.folio, "parada": nombre_punto, "orden": orden_eliminado},
                )
                messages.success(request, f"Parada {nombre_punto} eliminada.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "entrega_status":
            entrega_id = (request.POST.get("entrega_id") or "").strip()
            estatus_nuevo = (request.POST.get("estatus") or "").strip().upper()
            comentario = (request.POST.get("comentario") or "").strip()
            if entrega_id.isdigit() and estatus_nuevo in {c[0] for c in EntregaRuta.ESTATUS_CHOICES}:
                entrega = EntregaRuta.objects.filter(pk=int(entrega_id), ruta=ruta).first()
                if entrega:
                    entrega.estatus = estatus_nuevo
                    if comentario:
                        entrega.comentario = comentario
                    entrega.save(update_fields=["estatus", "comentario", "entregado_at", "updated_at"])
                    ruta.recompute_totals()
                    ruta.save(update_fields=["total_entregas", "entregas_completadas", "entregas_incidencia", "monto_estimado_total", "updated_at"])
                    log_event(
                        request.user,
                        "UPDATE",
                        "logistica.EntregaRuta",
                        str(entrega.id),
                        {
                            "ruta": ruta.folio,
                            "estatus": entrega.estatus,
                        },
                    )
                    messages.success(request, "Estatus de entrega actualizado.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "delete_entrega":
            entrega_id = (request.POST.get("entrega_id") or "").strip()
            if entrega_id.isdigit():
                entrega = EntregaRuta.objects.filter(pk=int(entrega_id), ruta=ruta).first()
                if entrega:
                    entrega.delete()
                    ruta.recompute_totals()
                    ruta.save(update_fields=["total_entregas", "entregas_completadas", "entregas_incidencia", "monto_estimado_total", "updated_at"])
                    messages.success(request, "Entrega eliminada.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "sync_recepcion_point":
            try:
                resumen = sincronizar_recepcion_desde_point(ruta=ruta, user=request.user)
            except ValidationError as exc:
                messages.error(request, "; ".join(exc.messages) if hasattr(exc, "messages") else str(exc))
            except OperationalError:
                messages.error(
                    request,
                    "La sincronización se cruzó con otra actualización. Recarga la ruta e intenta de nuevo.",
                )
            else:
                diferencias_point = ruta.paradas.filter(
                    entrega_estado__in=[ParadaRuta.ENTREGA_CON_DIFERENCIA, ParadaRuta.ENTREGA_NO_ENTREGADA]
                ).count()
                if resumen.lineas_pendientes_point:
                    messages.warning(
                        request,
                        (
                            "Recepción Point sincronizada parcialmente: "
                            f"{resumen.lineas_recibidas} línea(s) recibidas y "
                            f"{resumen.lineas_pendientes_point} línea(s) pendientes de recepción Point."
                        ),
                    )
                elif resumen.lineas_recibidas and diferencias_point:
                    messages.warning(
                        request,
                        (
                            "Recepción Point sincronizada con diferencias: "
                            f"{resumen.lineas_recibidas} línea(s) recibidas, "
                            f"{diferencias_point} parada(s) requieren revisión antes de cerrar."
                        ),
                    )
                elif resumen.lineas_recibidas:
                    messages.success(
                        request,
                        (
                            "Recepción Point sincronizada: "
                            f"{resumen.lineas_recibidas} línea(s) recibidas, "
                            f"{resumen.paradas_actualizadas} parada(s) actualizadas."
                        ),
                    )
                else:
                    messages.warning(request, "No hay checklist de carga o transferencias Point ligadas a esta ruta.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "sync_carga_point":
            try:
                resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=request.user, ejecutar_sync=True)
            except ValidationError as exc:
                messages.error(request, "; ".join(exc.messages) if hasattr(exc, "messages") else str(exc))
            except OperationalError:
                messages.error(
                    request,
                    "La sincronización se cruzó con otra actualización. Recarga la ruta e intenta de nuevo.",
                )
            else:
                if resumen.creadas or resumen.actualizadas:
                    messages.success(
                        request,
                        f"Carga esperada actualizada: {resumen.creadas} línea(s) nueva(s), {resumen.actualizadas} actualizada(s).",
                    )
                else:
                    messages.warning(request, "No hay solicitudes CEDIS ni transferencias abiertas Point para las sucursales de esta ruta.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

        if action == "ruta_status":
            estatus_nuevo = (request.POST.get("estatus") or "").strip().upper()
            if estatus_nuevo in {c[0] for c in RutaEntrega.ESTATUS_CHOICES}:
                if ruta.estatus in {RutaEntrega.ESTATUS_COMPLETADA, RutaEntrega.ESTATUS_CANCELADA} and estatus_nuevo != ruta.estatus:
                    messages.error(request, "La ruta ya está cerrada o cancelada y no puede reabrirse.")
                    return redirect("logistica:ruta_detail", pk=ruta.id)
                if ruta.estatus == RutaEntrega.ESTATUS_EN_RUTA and estatus_nuevo == RutaEntrega.ESTATUS_PLANEADA:
                    messages.error(request, "La ruta ya inició seguimiento y no puede regresar a planeada.")
                    return redirect("logistica:ruta_detail", pk=ruta.id)
                if estatus_nuevo == RutaEntrega.ESTATUS_EN_RUTA:
                    blockers = []
                    if not ruta.repartidor_id:
                        blockers.append("asigna repartidor")
                    if not ruta.unidad_operativa_id:
                        blockers.append("asigna unidad operativa")
                    if not ruta.paradas.exists():
                        blockers.append("agrega al menos una parada")
                    if (
                        ruta.repartidor_id
                        and RutaEntrega.objects.filter(
                            repartidor_id=ruta.repartidor_id,
                            estatus=RutaEntrega.ESTATUS_EN_RUTA,
                        )
                        .exclude(pk=ruta.pk)
                        .exists()
                    ):
                        blockers.append("el repartidor ya tiene otra ruta en curso")
                    if (
                        ruta.unidad_operativa_id
                        and RutaEntrega.objects.filter(
                            unidad_operativa_id=ruta.unidad_operativa_id,
                            estatus=RutaEntrega.ESTATUS_EN_RUTA,
                        )
                        .exclude(pk=ruta.pk)
                        .exists()
                    ):
                        blockers.append("la unidad ya tiene otra ruta en curso")
                    checklist_blocker = checklist_bloquea_salida(ruta)
                    if checklist_blocker:
                        blockers.append(checklist_blocker)
                    if blockers:
                        messages.error(request, "No se puede liberar la ruta: " + ", ".join(blockers) + ".")
                        return redirect("logistica:ruta_detail", pk=ruta.id)
                if estatus_nuevo == RutaEntrega.ESTATUS_COMPLETADA:
                    if ruta.estatus != RutaEntrega.ESTATUS_EN_RUTA:
                        messages.error(request, "Solo puedes completar una ruta que ya está en seguimiento.")
                        return redirect("logistica:ruta_detail", pk=ruta.id)
                    if not ruta.repartidor_id or not ruta.unidad_operativa_id or not ruta.paradas.exists():
                        messages.error(request, "No se puede completar la ruta: falta repartidor, unidad o paradas.")
                        return redirect("logistica:ruta_detail", pk=ruta.id)
                    if ruta.paradas.exclude(punto__tipo=PuntoLogistico.TIPO_CEDIS).filter(
                        estado=ParadaRuta.ESTADO_PENDIENTE,
                        entrega_estado=ParadaRuta.ENTREGA_PENDIENTE,
                    ).exists():
                        messages.error(request, "No se puede completar la ruta: hay paradas pendientes por visitar u omitir.")
                        return redirect("logistica:ruta_detail", pk=ruta.id)
                    if ruta_tiene_entregas_pendientes(ruta):
                        messages.error(request, "No se puede completar la ruta: hay paradas sin entrega confirmada.")
                        return redirect("logistica:ruta_detail", pk=ruta.id)
                    if ruta_tiene_diferencias_entrega(ruta):
                        messages.error(request, "No se puede completar la ruta: hay diferencias o entregas no recibidas por resolver.")
                        return redirect("logistica:ruta_detail", pk=ruta.id)
                from_status = ruta.estatus
                if from_status != estatus_nuevo:
                    ruta.estatus = estatus_nuevo
                    if estatus_nuevo == RutaEntrega.ESTATUS_EN_RUTA and not ruta.hora_inicio_real:
                        ruta.hora_inicio_real = timezone.now()
                    if estatus_nuevo == RutaEntrega.ESTATUS_COMPLETADA and not ruta.hora_cierre_real:
                        ruta.hora_cierre_real = timezone.now()
                    try:
                        ruta.save(update_fields=["estatus", "hora_inicio_real", "hora_cierre_real", "updated_at"])
                    except IntegrityError:
                        messages.error(request, "No se puede liberar la ruta: el repartidor o la unidad ya tiene otra ruta en curso.")
                        return redirect("logistica:ruta_detail", pk=ruta.id)
                    if estatus_nuevo == RutaEntrega.ESTATUS_EN_RUTA and not EventoRuta.objects.filter(ruta=ruta, tipo=EventoRuta.TIPO_SALIDA).exists():
                        EventoRuta.objects.create(
                            ruta=ruta,
                            tipo=EventoRuta.TIPO_SALIDA,
                            severidad=EventoRuta.SEVERIDAD_INFO,
                            descripcion="Ruta liberada para seguimiento operativo.",
                            creado_por=request.user,
                        )
                    if estatus_nuevo == RutaEntrega.ESTATUS_COMPLETADA and not EventoRuta.objects.filter(ruta=ruta, tipo=EventoRuta.TIPO_CIERRE).exists():
                        EventoRuta.objects.create(
                            ruta=ruta,
                            tipo=EventoRuta.TIPO_CIERRE,
                            severidad=EventoRuta.SEVERIDAD_INFO,
                            descripcion="Ruta completada y cerrada operativamente.",
                            creado_por=request.user,
                        )
                    log_event(
                        request.user,
                        "UPDATE",
                        "logistica.RutaEntrega",
                        str(ruta.id),
                        {"from": from_status, "to": estatus_nuevo, "folio": ruta.folio},
                    )
                    messages.success(request, f"Ruta {ruta.folio} en {estatus_nuevo}.")
            return redirect("logistica:ruta_detail", pk=ruta.id)

    pedidos_disponibles = (
        PedidoCliente.objects.select_related("cliente")
        .exclude(estatus__in=[PedidoCliente.ESTATUS_ENTREGADO, PedidoCliente.ESTATUS_CANCELADO])
        .order_by("fecha_compromiso", "-created_at")[:300]
    )

    entregas_qs = ruta.entregas.select_related("pedido", "pedido__cliente").all()
    entregas_total = entregas_qs.count()
    entregas_completadas = entregas_qs.filter(estatus=EntregaRuta.ESTATUS_ENTREGADA).count()
    incidencias = entregas_qs.filter(estatus=EntregaRuta.ESTATUS_INCIDENCIA).count()
    pendientes = entregas_qs.filter(estatus=EntregaRuta.ESTATUS_PENDIENTE).count()
    en_camino = entregas_qs.filter(estatus=EntregaRuta.ESTATUS_EN_CAMINO).count()
    rutas_liberadas = 1 if ruta.estatus != RutaEntrega.ESTATUS_CANCELADA and ruta.repartidor_id and ruta.unidad_operativa_id else 0
    entregas_controladas = entregas_qs.filter(
        estatus__in=[EntregaRuta.ESTATUS_EN_CAMINO, EntregaRuta.ESTATUS_ENTREGADA]
    ).count()
    enterprise_chain = [
        {
            "step": "01",
            "title": "Ruta liberada",
            "detail": "Ruta programada y lista para ejecutar.",
            "count": ruta.total_entregas,
            "status": "Ruta activa" if ruta.estatus != RutaEntrega.ESTATUS_CANCELADA else "Ruta cancelada",
            "tone": "success" if ruta.estatus != RutaEntrega.ESTATUS_CANCELADA else "danger",
            "url": reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            "cta": "Ver detalle",
            "owner": "Logística / Planeación",
            "next_step": "Confirmar chofer, unidad y entregas asignadas para la salida.",
        },
        {
            "step": "02",
            "title": "En tránsito",
            "detail": "Entregas ya despachadas dentro de esta ruta.",
            "count": en_camino,
            "status": "En ejecución" if en_camino else "Sin entregas en tránsito",
            "tone": "success" if en_camino else "warning",
            "url": reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            "cta": "Revisar entregas",
            "owner": "Logística / Tráfico",
            "next_step": "Mantener trazabilidad de entregas y registrar avances en tránsito.",
        },
        {
            "step": "03",
            "title": "Pendientes por cerrar",
            "detail": "Entregas que aún no han sido cerradas documentalmente.",
            "count": pendientes,
            "status": "Sin pendientes" if pendientes == 0 else f"{pendientes} pendientes",
            "tone": "success" if pendientes == 0 else "danger",
            "url": reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            "cta": "Cerrar pendientes",
            "owner": "Logística / Entrega",
            "next_step": "Cerrar entregas pendientes antes del corte operativo.",
        },
        {
            "step": "04",
            "title": "Incidencias",
            "detail": "Eventos que requieren validación y cierre operativo.",
            "count": incidencias,
            "status": "Sin incidencias" if incidencias == 0 else f"{incidencias} abiertas",
            "tone": "success" if incidencias == 0 else "danger",
            "url": reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            "cta": "Resolver incidencias",
            "owner": "Logística / Auditoría",
            "next_step": "Resolver incidencias y dejar evidencia de cierre documental.",
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
            "label": "Entregas programadas",
            "open": entregas_total,
            "closed": 0,
            "detail": "Carga total asignada a la ruta.",
            "url": reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            "owner": "Logística / Planeación",
            "next_step": "Asegurar secuencia y salida operativa de todas las entregas.",
        },
        {
            "label": "En tránsito",
            "open": en_camino,
            "closed": max(entregas_total - en_camino, 0),
            "detail": "Entregas actualmente en ejecución.",
            "url": reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            "owner": "Logística / Tráfico",
            "next_step": "Mantener seguimiento hasta confirmar cierre o incidencia.",
        },
        {
            "label": "Completadas",
            "open": pendientes + incidencias,
            "closed": entregas_completadas,
            "detail": "Entregas cerradas correctamente frente a pendientes e incidencias.",
            "url": reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            "owner": "Logística / Entrega",
            "next_step": "Cerrar entregas restantes y liberar la ruta.",
        },
        {
            "label": "Incidencias abiertas",
            "open": incidencias,
            "closed": max(entregas_total - incidencias, 0),
            "detail": "Casos con excepción pendientes de cierre.",
            "url": reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            "owner": "Logística / Auditoría",
            "next_step": "Resolver incidencias y dejar soporte documental.",
        },
    ]
    for row in document_stage_rows:
        total = int(row["open"]) + int(row["closed"])
        row["completion"] = int(round((int(row["closed"]) / total) * 100)) if total else 100
    maturity_summary = _logistica_maturity_summary(
        chain=enterprise_chain,
        default_url=reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
    )
    handoff_map = _logistica_handoff_map(
        rutas_hoy=1 if ruta.fecha_ruta == timezone.localdate() else 0,
        rutas_en_ruta=1 if ruta.estatus == RutaEntrega.ESTATUS_EN_RUTA else 0,
        entregas_pendientes=pendientes,
        incidencias=incidencias,
    )
    release_gate_rows = _logistica_release_gate_rows(
        rutas_total=1,
        rutas_liberadas=rutas_liberadas,
        entregas_total=max(entregas_total, 1),
        entregas_controladas=entregas_controladas,
        entregas_cerradas=entregas_completadas,
        incidencias=incidencias,
        base_url=reverse("logistica:rutas"),
        ruta_id=ruta.id,
    )
    governance_rows = _logistica_governance_rows(document_stage_rows, owner_default="Logística / Operación")
    tiempos_ruta = resumen_tiempos_ruta(ruta)
    checklist_carga = getattr(ruta, "checklist_carga", None)
    recepcion_point_rows = _recepcion_point_rows(checklist_carga)
    recepcion_point_totales = _totales_recepcion_point(recepcion_point_rows)
    captura_erp_disponible = can_manage_submodule(request.user, "logistica", "rutas") and ruta.estatus in {
        RutaEntrega.ESTATUS_PLANEADA,
        RutaEntrega.ESTATUS_EN_RUTA,
    }
    carga_manual_pendiente = bool(
        checklist_carga
        and ruta.estatus == RutaEntrega.ESTATUS_PLANEADA
        and checklist_carga.lineas.filter(estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE).exists()
    )
    recarga_cedis_disponible = bool(
        checklist_carga
        and can_manage_submodule(request.user, "logistica", "rutas")
        and (
            ruta.estatus == RutaEntrega.ESTATUS_EN_RUTA
            or (
                ruta.estatus == RutaEntrega.ESTATUS_PLANEADA
                and checklist_carga.estatus == RutaCargaChecklist.ESTATUS_CON_INCIDENCIA
                and not checklist_carga.lineas.filter(estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE).exists()
            )
        )
    )
    diferencia_carga_pendiente_autorizar = bool(
        checklist_carga
        and can_manage_submodule(request.user, "logistica", "rutas")
        and checklist_carga.estatus == RutaCargaChecklist.ESTATUS_CON_INCIDENCIA
        and not checklist_carga.motivo_override
    )
    cierre_diferencia_disponible = bool(
        can_manage_submodule(request.user, "logistica", "rutas")
        and ruta.estatus == RutaEntrega.ESTATUS_EN_RUTA
        and not ruta.paradas.filter(estado=ParadaRuta.ESTADO_PENDIENTE).exists()
        and not ruta_tiene_entregas_pendientes(ruta)
        and ruta_tiene_diferencias_entrega(ruta)
    )
    revisiones_entrega = list(
        ruta.paradas.exclude(revision_entrega_estado=ParadaRuta.REVISION_NO_REQUERIDA)
        .select_related(
            "punto",
            "entrega_confirmada_por",
            "entrega_confirmada_por__empleado_rrhh",
            "revision_entrega_revisada_por",
            "revision_entrega_revisada_por__empleado_rrhh",
        )
        .prefetch_related("evidencias_entrega")
        .order_by("revision_entrega_estado", "orden", "id")
    )
    revisiones_pendientes_count = sum(
        parada.revision_entrega_estado == ParadaRuta.REVISION_PENDIENTE for parada in revisiones_entrega
    )
    revisiones_rechazadas_count = sum(
        parada.revision_entrega_estado == ParadaRuta.REVISION_RECHAZADA for parada in revisiones_entrega
    )

    context = {
        "module_tabs": _module_tabs("rutas", request.user),
        "can_manage_logistica": can_manage_submodule(request.user, "logistica", "rutas"),
        "ruta": ruta,
        "entregas": entregas_qs,
        "pedidos": pedidos_disponibles,
        "estatus_ruta_choices": _ruta_status_choices_for(ruta),
        "estatus_entrega_choices": EntregaRuta.ESTATUS_CHOICES,
        "repartidores": Repartidor.objects.filter(user__is_active=True).select_related("user", "user__empleado_rrhh", "unidad_asignada").order_by("user__first_name", "user__username"),
        "unidades": Unidad.objects.filter(activa=True).order_by("codigo"),
        "puntos_logisticos": PuntoLogistico.objects.filter(activo=True).select_related("sucursal").order_by("tipo", "nombre"),
        "paradas": ruta.paradas.select_related("punto", "punto__sucursal").order_by("orden", "id"),
        "paradas_tiempos": tiempos_ruta.paradas,
        "tiempos_ruta": tiempos_ruta,
        "checklist_carga": checklist_carga,
        "carga_manual_pendiente": carga_manual_pendiente,
        "recarga_cedis_disponible": recarga_cedis_disponible,
        "diferencia_carga_pendiente_autorizar": diferencia_carga_pendiente_autorizar,
        "cierre_diferencia_disponible": cierre_diferencia_disponible,
        "revisiones_entrega": revisiones_entrega,
        "revisiones_pendientes_count": revisiones_pendientes_count,
        "revisiones_rechazadas_count": revisiones_rechazadas_count,
        "cierre_administrativo_pendiente": bool(
            revisiones_pendientes_count or revisiones_rechazadas_count
        ),
        "recepcion_point_rows": recepcion_point_rows,
        "recepcion_point_totales": recepcion_point_totales,
        "captura_erp_disponible": captura_erp_disponible,
        "motivos_carga_manual": RutaCargaChecklistLinea.MOTIVO_CHOICES,
        "enterprise_chain": enterprise_chain,
        "critical_path_rows": _logistica_critical_path_rows(enterprise_chain),
        "document_stage_rows": document_stage_rows,
        "erp_governance_rows": governance_rows,
        "executive_radar_rows": _logistica_executive_radar_rows(
            governance_rows,
            default_owner="Logística / Operación",
            fallback_url=reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
        ),
        "erp_command_center": _logistica_command_center(
            governance_rows=governance_rows,
            maturity_summary=maturity_summary,
            default_url=reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            default_cta="Abrir ruta",
        ),
        "maturity_summary": maturity_summary,
        "handoff_map": handoff_map,
        "release_gate_rows": release_gate_rows,
        "release_gate_completion": (
            int(
                round(
                    (
                        sum(row["completed"] for row in release_gate_rows)
                        / sum(row["total"] for row in release_gate_rows)
                    )
                    * 100
                )
            )
            if release_gate_rows and sum(row["total"] for row in release_gate_rows)
            else 0
        ),
        "operational_health_cards": _logistica_operational_health_cards(
            focus="detalle",
            primary_open=entregas_total,
            secondary_open=pendientes + incidencias,
            ready_count=entregas_completadas,
        ),
    }
    return render(request, "logistica/ruta_detail.html", context)


@login_required
def ruta_print(request, pk: int):
    if not can_view_submodule(request.user, "logistica", "rutas"):
        raise PermissionDenied("No tienes permisos para ver Logística")

    ruta = get_object_or_404(RutaEntrega, pk=pk)
    context = {
        "ruta": ruta,
        "paradas": ruta.paradas.select_related("punto", "punto__sucursal").order_by("orden", "id"),
        "tiempos_ruta": resumen_tiempos_ruta(ruta),
        "printed_at": timezone.localtime(),
    }
    return render(request, "logistica/ruta_print.html", context)


@login_required
def dashboard_ejecutivo(request):
    if not _can_view_logistica_ejecutivo(request.user):
        raise PermissionDenied("No tienes permisos para ver el dashboard ejecutivo de Logística")

    today = timezone.localdate()
    limite_30 = today + timedelta(days=30)
    checklist_fields = [
        field.name
        for field in InspeccionVehiculo._meta.fields
        if field.name.startswith(("ext_", "int_", "niv_", "est_"))
    ]
    inspecciones_recientes = []
    for inspeccion in InspeccionVehiculo.objects.select_related("unidad", "repartidor__user").order_by("-fecha")[:50]:
        faltantes = _inspeccion_faltantes_count(inspeccion)
        if inspeccion.tiene_golpes or faltantes:
            inspeccion.faltantes_count = faltantes
            inspeccion.faltantes_labels = [
                field.replace("_", " ").title()
                for field in checklist_fields
                if not getattr(inspeccion, field)
            ][:6]
            inspecciones_recientes.append(inspeccion)
        if len(inspecciones_recientes) >= 5:
            break

    documentos_criticos = []
    for documento in DocumentoUnidad.objects.select_related("unidad").filter(
        vigente=True,
        fecha_vencimiento__lte=limite_30,
    ).order_by("fecha_vencimiento", "unidad__codigo"):
        documentos_criticos.append(_decorate_documento(documento))
    turnos_sin_cerrar = list(
        BitacoraSalidaLlegada.objects.select_related("unidad", "repartidor__user")
        .filter(cerrada=False)
        .order_by("hora_salida", "id")
    )
    now = timezone.now()
    for turno in turnos_sin_cerrar:
        turno.horas_abierto = int(((now - turno.hora_salida).total_seconds() // 3600)) if turno.hora_salida else 0

    context = {
        "module_tabs": _module_tabs("ejecutivo", request.user),
        "today": today,
        "tickets_abiertos": ReporteUnidad.objects.filter(estatus=ReporteUnidad.ESTATUS_ABIERTO).count(),
        "tickets_criticos": ReporteUnidad.objects.filter(
            estatus__in=[ReporteUnidad.ESTATUS_ABIERTO, ReporteUnidad.ESTATUS_EN_PROCESO],
            severidad=ReporteUnidad.SEVERIDAD_CRITICO,
        ).count(),
        "turnos_abiertos": BitacoraSalidaLlegada.objects.filter(cerrada=False).count(),
        "unidades_activas": Unidad.objects.filter(activa=True).count(),
        "documentos_por_vencer": DocumentoUnidad.objects.filter(vigente=True, fecha_vencimiento__lte=limite_30).count(),
        "servicios_proximos": ServicioRealizadoUnidad.objects.filter(proxima_fecha__lte=limite_30).count(),
        "gasto_mes": ReparacionUnidad.objects.filter(
            fecha_ingreso__month=today.month,
            fecha_ingreso__year=today.year,
        ).aggregate(total=Sum("costo_total")).get("total")
        or Decimal("0"),
        "tickets_recientes": ReporteUnidad.objects.select_related("unidad", "repartidor__user").order_by(
            "-fecha_reporte"
        )[:10],
        "turnos_sin_cerrar": turnos_sin_cerrar,
        "documentos_criticos": documentos_criticos,
        "inspecciones_recientes": inspecciones_recientes,
    }
    return render(request, "logistica/dashboard_ejecutivo.html", context)


@login_required
def tickets_kanban(request):
    if not _can_manage_tickets_logistica(request.user):
        raise PermissionDenied("No tienes permisos para gestionar tickets de Logística")

    base_qs = ReporteUnidad.objects.select_related("unidad", "repartidor__user").order_by("-fecha_reporte")
    today = timezone.localdate()
    context = {
        "module_tabs": _module_tabs("tickets", request.user),
        "tickets_abiertos": base_qs.filter(estatus=ReporteUnidad.ESTATUS_ABIERTO),
        "tickets_en_proceso": base_qs.filter(estatus=ReporteUnidad.ESTATUS_EN_PROCESO),
        "tickets_programados": base_qs.filter(estatus=ReporteUnidad.ESTATUS_PROGRAMADO),
        "tickets_cerrados_hoy": base_qs.filter(estatus=ReporteUnidad.ESTATUS_CERRADO, actualizado_en__date=today),
    }
    return render(request, "logistica/tickets_kanban.html", context)


@login_required
def ticket_actualizar(request, pk):
    if not _can_manage_tickets_logistica(request.user):
        raise PermissionDenied("No tienes permisos para actualizar tickets de Logística")
    if request.method != "POST":
        return redirect("logistica:tickets_kanban")

    ticket = get_object_or_404(ReporteUnidad, pk=pk)
    estatus = (request.POST.get("estatus") or "").strip()
    estatus_validos = {
        ReporteUnidad.ESTATUS_EN_PROCESO,
        ReporteUnidad.ESTATUS_PROGRAMADO,
        ReporteUnidad.ESTATUS_CERRADO,
    }
    if estatus not in estatus_validos:
        messages.error(request, "Estatus no válido para el ticket.")
        return redirect("logistica:tickets_kanban")

    ticket.estatus = estatus
    update_fields = ["estatus", "actualizado_en"]
    if estatus == ReporteUnidad.ESTATUS_EN_PROCESO and not ticket.asignado_a_id:
        ticket.asignado_a = request.user
        update_fields.append("asignado_a")
    if "proveedor_servicio" in request.POST:
        ticket.proveedor_servicio = (request.POST.get("proveedor_servicio") or "").strip()
        update_fields.append("proveedor_servicio")
    if "notas_compras" in request.POST:
        ticket.notas_compras = (request.POST.get("notas_compras") or "").strip()
        update_fields.append("notas_compras")
    costo_raw = (request.POST.get("costo_servicio") or "").strip()
    if costo_raw:
        ticket.costo_servicio = _parse_decimal(costo_raw)
        update_fields.append("costo_servicio")
    ticket.save(update_fields=update_fields)
    messages.success(request, "Ticket actualizado correctamente.")
    return redirect("logistica:tickets_kanban")


@login_required
def flota_resumen(request):
    if not _can_view_flota_resumen(request.user):
        raise PermissionDenied("No tienes permisos para ver el resumen de flota")

    today = timezone.localdate()
    year = today.year
    unidades = Unidad.objects.select_related("sucursal").filter(activa=True).order_by("codigo")
    unidades_resumen = []
    for unidad in unidades:
        ultimo_lavado = LavadoUnidad.objects.filter(unidad=unidad).order_by("-fecha").first()
        documento_seguro = _decorate_documento(
            DocumentoUnidad.objects.filter(unidad=unidad, tipo=DocumentoUnidad.TIPO_SEGURO).order_by("-fecha_vencimiento").first()
        )
        documento_tarjeta = _decorate_documento(
            DocumentoUnidad.objects.filter(unidad=unidad, tipo=DocumentoUnidad.TIPO_TARJETA_CIRCULACION)
            .order_by("-fecha_vencimiento")
            .first()
        )
        ultimo_servicio = ServicioRealizadoUnidad.objects.select_related("tipo_servicio").filter(unidad=unidad).order_by(
            "-fecha_servicio"
        ).first()
        proximo_servicio = _decorate_servicio(
            ServicioRealizadoUnidad.objects.select_related("tipo_servicio")
            .filter(unidad=unidad, proxima_fecha__isnull=False)
            .order_by("proxima_fecha")
            .first()
        )
        reparaciones_year = ReparacionUnidad.objects.filter(unidad=unidad, fecha_ingreso__year=year)
        turno_activo = (
            BitacoraSalidaLlegada.objects.select_related("repartidor__user")
            .filter(unidad=unidad, cerrada=False)
            .order_by("hora_salida")
            .first()
        )
        unidades_resumen.append(
            {
                "unidad": unidad,
                "ultimo_lavado": ultimo_lavado,
                "dias_sin_lavar": (today - ultimo_lavado.fecha).days if ultimo_lavado else None,
                "documento_seguro": documento_seguro,
                "documento_tarjeta": documento_tarjeta,
                "ultimo_servicio": ultimo_servicio,
                "proximo_servicio": proximo_servicio,
                "reparaciones_anio": reparaciones_year.count(),
                "gasto_anio": reparaciones_year.aggregate(total=Sum("costo_total")).get("total") or Decimal("0"),
                "turno_activo": turno_activo,
            }
        )

    context = {
        "module_tabs": _module_tabs("flota", request.user),
        "unidades_resumen": unidades_resumen,
        "today": today,
    }
    return render(request, "logistica/flota_resumen.html", context)


@login_required
def unidad_detalle(request, pk):
    if not can_view_submodule(request.user, "logistica", "unidades"):
        raise PermissionDenied("No tienes permisos para ver la ficha de unidad")

    today = timezone.localdate()
    unidad = get_object_or_404(Unidad.objects.select_related("sucursal"), pk=pk)
    documentos_qs = DocumentoUnidad.objects.filter(unidad=unidad).order_by("-fecha_vencimiento")
    documentos = [_decorate_documento(documento) for documento in documentos_qs]
    lavados = list(LavadoUnidad.objects.select_related("registrado_por").filter(unidad=unidad).order_by("-fecha"))
    reparaciones = ReparacionUnidad.objects.select_related("reporte_origen", "registrado_por").filter(unidad=unidad).order_by(
        "-fecha_ingreso"
    )
    bitacoras = [
        _decorate_bitacora(bitacora)
        for bitacora in BitacoraSalidaLlegada.objects.select_related("repartidor__user", "unidad")
        .prefetch_related("cargas_combustible")
        .filter(unidad=unidad)
        .order_by("-hora_salida")[:30]
    ]
    inspecciones = [
        _decorate_inspeccion(inspeccion)
        for inspeccion in InspeccionVehiculo.objects.select_related("repartidor__user", "unidad")
        .filter(unidad=unidad)
        .order_by("-fecha")[:20]
    ]
    ultimo_lavado = lavados[0] if lavados else None
    documento_seguro = _decorate_documento(documentos_qs.filter(tipo=DocumentoUnidad.TIPO_SEGURO).first())
    documento_tarjeta = _decorate_documento(documentos_qs.filter(tipo=DocumentoUnidad.TIPO_TARJETA_CIRCULACION).first())

    context = {
        "module_tabs": _module_tabs("unidades", request.user),
        "unidad": unidad,
        "documentos": documentos,
        "tipos_servicio": TipoServicioUnidad.objects.filter(activo=True).order_by("nombre"),
        "servicios": [
            _decorate_servicio(servicio)
            for servicio in ServicioRealizadoUnidad.objects.select_related("tipo_servicio", "registrado_por")
            .filter(unidad=unidad)
            .order_by("-fecha_servicio")
        ],
        "lavados": lavados,
        "reparaciones": reparaciones,
        "bitacoras": bitacoras,
        "inspecciones": inspecciones,
        "reportes_unidad": ReporteUnidad.objects.filter(unidad=unidad).order_by("-fecha_reporte")[:100],
        "today": today,
        "limite_30": today + timedelta(days=30),
        "gasto_total_anio": reparaciones.filter(fecha_ingreso__year=today.year).aggregate(total=Sum("costo_total")).get("total")
        or Decimal("0"),
        "dias_sin_lavar": (today - ultimo_lavado.fecha).days if ultimo_lavado else None,
        "documento_seguro": documento_seguro,
        "documento_tarjeta": documento_tarjeta,
        "turno_activo": BitacoraSalidaLlegada.objects.select_related("repartidor__user")
        .filter(unidad=unidad, cerrada=False)
        .order_by("hora_salida")
        .first(),
    }
    return render(request, "logistica/unidad_detalle.html", context)


def _redirect_unidad_tab(pk: int, anchor: str):
    return redirect(f"{reverse('logistica:unidad_detalle', kwargs={'pk': pk})}#{anchor}")


@login_required
def unidad_documento_nuevo(request, pk):
    if not _can_manage_unidades(request.user):
        raise PermissionDenied("No tienes permisos para agregar documentos de unidad")
    unidad = get_object_or_404(Unidad, pk=pk)
    if request.method == "POST":
        DocumentoUnidad.objects.create(
            unidad=unidad,
            tipo=request.POST.get("tipo") or DocumentoUnidad.TIPO_OTRO,
            descripcion=(request.POST.get("descripcion") or "").strip(),
            aseguradora=(request.POST.get("aseguradora") or "").strip(),
            archivo=request.FILES.get("archivo"),
            fecha_emision=_parse_date(request.POST.get("fecha_emision")),
            fecha_vencimiento=_parse_date(request.POST.get("fecha_vencimiento")) or timezone.localdate(),
            notas=(request.POST.get("notas") or "").strip(),
            registrado_por=request.user,
        )
        messages.success(request, "Documento agregado correctamente.")
    return _redirect_unidad_tab(pk, "documentos")


@login_required
def unidad_servicio_nuevo(request, pk):
    if not _can_manage_unidades(request.user):
        raise PermissionDenied("No tienes permisos para registrar servicios de unidad")
    unidad = get_object_or_404(Unidad, pk=pk)
    if request.method == "POST":
        tipo_servicio = get_object_or_404(TipoServicioUnidad, pk=request.POST.get("tipo_servicio"), activo=True)
        ServicioRealizadoUnidad.objects.create(
            unidad=unidad,
            tipo_servicio=tipo_servicio,
            fecha_servicio=_parse_date(request.POST.get("fecha_servicio")) or timezone.localdate(),
            km_al_servicio=int(request.POST.get("km_al_servicio") or 0) or None,
            proveedor=(request.POST.get("proveedor") or "").strip(),
            costo=_parse_decimal(request.POST.get("costo")) if request.POST.get("costo") else None,
            archivo_factura=request.FILES.get("archivo_factura"),
            notas=(request.POST.get("notas") or "").strip(),
            registrado_por=request.user,
        )
        messages.success(request, "Servicio registrado correctamente.")
    return _redirect_unidad_tab(pk, "servicios")


@login_required
def unidad_lavado_nuevo(request, pk):
    if not _can_manage_unidades(request.user):
        raise PermissionDenied("No tienes permisos para registrar lavados de unidad")
    unidad = get_object_or_404(Unidad, pk=pk)
    if request.method == "POST":
        LavadoUnidad.objects.create(
            unidad=unidad,
            fecha=_parse_date(request.POST.get("fecha")) or timezone.localdate(),
            lavado_exterior=bool(request.POST.get("lavado_exterior")),
            lavado_interior=bool(request.POST.get("lavado_interior")),
            lavado_caja_refrigerada=bool(request.POST.get("lavado_caja_refrigerada")),
            costo=_parse_decimal(request.POST.get("costo")) if request.POST.get("costo") else None,
            foto_evidencia=request.FILES.get("foto_evidencia"),
            notas=(request.POST.get("notas") or "").strip(),
            registrado_por=request.user,
        )
        messages.success(request, "Lavado registrado correctamente.")
    return _redirect_unidad_tab(pk, "lavados")


@login_required
def unidad_reparacion_nueva(request, pk):
    if not _can_manage_unidades(request.user):
        raise PermissionDenied("No tienes permisos para registrar reparaciones de unidad")
    unidad = get_object_or_404(Unidad, pk=pk)
    if request.method == "POST":
        reporte_id = request.POST.get("reporte_origen") or None
        ReparacionUnidad.objects.create(
            unidad=unidad,
            reporte_origen=ReporteUnidad.objects.filter(pk=reporte_id, unidad=unidad).first() if reporte_id else None,
            fecha_ingreso=_parse_date(request.POST.get("fecha_ingreso")) or timezone.localdate(),
            fecha_entrega=_parse_date(request.POST.get("fecha_entrega")),
            descripcion_falla=(request.POST.get("descripcion_falla") or "").strip(),
            descripcion_reparacion=(request.POST.get("descripcion_reparacion") or "").strip(),
            proveedor=(request.POST.get("proveedor") or "").strip(),
            costo_total=_parse_decimal(request.POST.get("costo_total")) if request.POST.get("costo_total") else None,
            archivo_factura=request.FILES.get("archivo_factura"),
            foto_nota=request.FILES.get("foto_nota"),
            notas=(request.POST.get("notas") or "").strip(),
            registrado_por=request.user,
        )
        messages.success(request, "Reparación registrada correctamente.")
    return _redirect_unidad_tab(pk, "reparaciones")


@login_required
def reportes_lista(request):
    if not can_view_submodule(request.user, "logistica", "reportes"):
        raise PermissionDenied("No tienes permisos para ver reportes de Logística")

    qs = ReporteUnidad.objects.select_related("unidad", "repartidor__user", "repartidor__user__empleado_rrhh").order_by("-fecha_reporte")
    estatus = (request.GET.get("estatus") or "").strip()
    severidad = (request.GET.get("severidad") or "").strip()
    unidad_id = (request.GET.get("unidad") or "").strip()
    fecha_desde = _parse_date(request.GET.get("fecha_desde"))
    fecha_hasta = _parse_date(request.GET.get("fecha_hasta"))
    if estatus:
        qs = qs.filter(estatus=estatus)
    if severidad:
        qs = qs.filter(severidad=severidad)
    if unidad_id:
        qs = qs.filter(unidad_id=unidad_id)
    if fecha_desde:
        qs = qs.filter(fecha_reporte__date__gte=fecha_desde)
    if fecha_hasta:
        qs = qs.filter(fecha_reporte__date__lte=fecha_hasta)

    reportes = Paginator(qs, 20).get_page(request.GET.get("page"))
    return render(
        request,
        "logistica/reportes_lista.html",
        {
            "module_tabs": _module_tabs("reportes", request.user),
            "reportes": reportes,
            "unidades": Unidad.objects.filter(activa=True).order_by("codigo"),
            "estatus_choices": ReporteUnidad.ESTATUS_CHOICES,
            "severidad_choices": ReporteUnidad.SEVERIDAD_CHOICES,
            "can_crear_reporte": can_manage_submodule(request.user, "logistica", "reportes"),
            "filters": {
                "estatus": estatus,
                "severidad": severidad,
                "unidad": unidad_id,
                "fecha_desde": fecha_desde.isoformat() if fecha_desde else "",
                "fecha_hasta": fecha_hasta.isoformat() if fecha_hasta else "",
            },
        },
    )


@login_required
def reporte_crear(request):
    if not can_manage_submodule(request.user, "logistica", "reportes"):
        raise PermissionDenied("No tienes permisos para crear reportes de Logística")

    unidades = Unidad.objects.filter(activa=True).order_by("codigo")
    repartidores = Repartidor.objects.filter(user__is_active=True).select_related("user", "user__empleado_rrhh").order_by("user__first_name", "user__username")

    if request.method == "POST":
        unidad_id = (request.POST.get("unidad") or "").strip()
        tipo = (request.POST.get("tipo") or "").strip()
        severidad = (request.POST.get("severidad") or "").strip()
        descripcion = (request.POST.get("descripcion") or "").strip()
        kilometraje_raw = (request.POST.get("kilometraje") or "").strip()
        repartidor_id = (request.POST.get("repartidor") or "").strip()
        foto = request.FILES.get("foto")

        errors: dict[str, str] = {}
        if not unidad_id:
            errors["unidad"] = "Selecciona una unidad."
        if tipo not in {c[0] for c in ReporteUnidad.TIPO_CHOICES}:
            errors["tipo"] = "Tipo de reporte no válido."
        if severidad not in {c[0] for c in ReporteUnidad.SEVERIDAD_CHOICES}:
            errors["severidad"] = "Severidad no válida."
        if not descripcion:
            errors["descripcion"] = "La descripción es obligatoria."

        unidad = None
        if unidad_id and not errors.get("unidad"):
            unidad = Unidad.objects.filter(pk=unidad_id, activa=True).first()
            if not unidad:
                errors["unidad"] = "Unidad no encontrada."

        repartidor = None
        if repartidor_id:
            repartidor = Repartidor.objects.filter(pk=repartidor_id, user__is_active=True).first()

        kilometraje = None
        if kilometraje_raw:
            try:
                kilometraje = int(kilometraje_raw)
            except ValueError:
                errors["kilometraje"] = "El kilometraje debe ser un número entero."

        if not errors:
            reporte = ReporteUnidad.objects.create(
                unidad=unidad,
                repartidor=repartidor,
                tipo=tipo,
                severidad=severidad,
                descripcion=descripcion,
                kilometraje=kilometraje,
                foto=foto if foto else None,
                ip_reporte=request.META.get("REMOTE_ADDR"),
                estatus=ReporteUnidad.ESTATUS_ABIERTO,
            )
            log_event(
                request.user,
                "CREATE",
                "logistica.ReporteUnidad",
                str(reporte.id),
                {
                    "unidad": reporte.unidad.codigo,
                    "tipo": reporte.tipo,
                    "severidad": reporte.severidad,
                    "origen": "erp",
                },
            )
            messages.success(request, f"Reporte #{reporte.id} creado correctamente.")
            return redirect("logistica:reportes_lista")

        return render(
            request,
            "logistica/reporte_form.html",
            {
                "module_tabs": _module_tabs("reportes", request.user),
                "unidades": unidades,
                "repartidores": repartidores,
                "tipo_choices": ReporteUnidad.TIPO_CHOICES,
                "severidad_choices": ReporteUnidad.SEVERIDAD_CHOICES,
                "errors": errors,
                "prev": {
                    "unidad": unidad_id,
                    "tipo": tipo,
                    "severidad": severidad,
                    "descripcion": descripcion,
                    "kilometraje": kilometraje_raw,
                    "repartidor": repartidor_id,
                },
            },
        )

    return render(
        request,
        "logistica/reporte_form.html",
        {
            "module_tabs": _module_tabs("reportes", request.user),
            "unidades": unidades,
            "repartidores": repartidores,
            "tipo_choices": ReporteUnidad.TIPO_CHOICES,
            "severidad_choices": ReporteUnidad.SEVERIDAD_CHOICES,
            "errors": {},
            "prev": {},
        },
    )


@login_required
def bitacoras_lista(request):
    if not can_view_submodule(request.user, "logistica", "bitacoras"):
        raise PermissionDenied("No tienes permisos para ver bitácoras de Logística")

    qs = (
        BitacoraSalidaLlegada.objects.select_related("unidad", "repartidor__user")
        .prefetch_related("cargas_combustible")
        .order_by("-hora_salida")
    )
    unidad_id = (request.GET.get("unidad") or "").strip()
    repartidor_id = (request.GET.get("repartidor") or "").strip()
    cerrada = (request.GET.get("cerrada") or "").strip()
    fecha_desde = _parse_date(request.GET.get("fecha_desde"))
    fecha_hasta = _parse_date(request.GET.get("fecha_hasta"))
    if unidad_id:
        qs = qs.filter(unidad_id=unidad_id)
    if repartidor_id:
        qs = qs.filter(repartidor_id=repartidor_id)
    if cerrada == "1":
        qs = qs.filter(cerrada=True)
    elif cerrada == "0":
        qs = qs.filter(cerrada=False)
    if fecha_desde:
        qs = qs.filter(fecha__gte=fecha_desde)
    if fecha_hasta:
        qs = qs.filter(fecha__lte=fecha_hasta)

    combustible_total_bitacora = qs.aggregate(total=Sum("costo_combustible")).get("total") or Decimal("0")
    combustible_total_cargas = CargaCombustibleUnidad.objects.filter(bitacora_id__in=qs.values("id")).aggregate(total=Sum("importe_total")).get("total") or Decimal("0")
    combustible_total = combustible_total_bitacora + combustible_total_cargas
    cargas_combustible_bitacora = qs.filter(
        Q(litros_cargados__isnull=False) | Q(costo_combustible__isnull=False) | Q(foto_ticket_combustible__isnull=False)
    ).count()
    cargas_combustible_ruta = CargaCombustibleUnidad.objects.filter(bitacora_id__in=qs.values("id")).count()
    cargas_combustible = cargas_combustible_bitacora + cargas_combustible_ruta
    page = Paginator(qs, 20).get_page(request.GET.get("page"))
    bitacoras = [_decorate_bitacora(bitacora) for bitacora in page]
    return render(
        request,
        "logistica/bitacoras_lista.html",
        {
            "module_tabs": _module_tabs("bitacoras", request.user),
            "bitacoras": bitacoras,
            "bitacoras_page": page,
            "combustible_total": combustible_total,
            "cargas_combustible": cargas_combustible,
            "unidades": Unidad.objects.filter(activa=True).order_by("codigo"),
            "repartidores": Repartidor.objects.filter(user__is_active=True).select_related("user").order_by("user__first_name", "user__username"),
            "filters": {
                "unidad": unidad_id,
                "repartidor": repartidor_id,
                "cerrada": cerrada,
                "fecha_desde": fecha_desde.isoformat() if fecha_desde else "",
                "fecha_hasta": fecha_hasta.isoformat() if fecha_hasta else "",
            },
        },
    )


@login_required
def domicilios_ecommerce(request):
    """Asigna repartidor+unidad reales a pedidos de domicilio de la tienda en línea."""
    if not can_view_submodule(request.user, "logistica", "rutas"):
        raise PermissionDenied("No tienes permisos para ver Logística")

    if request.method == "POST":
        if not can_manage_submodule(request.user, "logistica", "rutas"):
            raise PermissionDenied("No tienes permisos para gestionar Logística")

        order_id = (request.POST.get("order_id") or "").strip()
        order_number = (request.POST.get("order_number") or "").strip()
        cliente_nombre = (request.POST.get("cliente_nombre") or "").strip()
        direccion = (request.POST.get("direccion") or "").strip()
        repartidor_id = (request.POST.get("repartidor") or "").strip()
        unidad_id = (request.POST.get("unidad_operativa") or "").strip()

        repartidor = Repartidor.objects.filter(pk=int(repartidor_id), user__is_active=True).first() if repartidor_id.isdigit() else None
        unidad = Unidad.objects.filter(pk=int(unidad_id), activa=True).first() if unidad_id.isdigit() else None

        if not order_id.isdigit():
            messages.error(request, "Pedido inválido.")
        elif repartidor is None:
            messages.error(request, "Selecciona un repartidor activo.")
        elif unidad is None:
            messages.error(request, "Selecciona una unidad activa.")
        else:
            try:
                resultado = EcommerceClient().asignar(
                    order_id=int(order_id),
                    erp_repartidor_id=str(repartidor.id),
                    repartidor_name=str(repartidor),
                    repartidor_phone=repartidor.telefono,
                    erp_unidad_id=str(unidad.id),
                    unidad_code=unidad.codigo,
                    unidad_type=f"{unidad.marca} {unidad.modelo}".strip(),
                    unidad_plate=unidad.placa,
                )
            except EcommerceIntegrationError as exc:
                messages.error(request, f"No se pudo asignar en la tienda en línea: {exc}")
            else:
                with transaction.atomic():
                    EntregaEcommerce.objects.create(
                        repartidor=repartidor,
                        unidad=unidad,
                        ecommerce_order_id=int(order_id),
                        ecommerce_order_number=order_number,
                        ecommerce_task_id=resultado["task_id"],
                        cliente_nombre=cliente_nombre,
                        direccion=direccion,
                        driver_access_token=resultado["driver_access_token"],
                        driver_url=resultado["driver_url"],
                    )
                    log_event(
                        request.user,
                        "CREATE",
                        "logistica.EntregaEcommerce",
                        order_id,
                        {"repartidor": str(repartidor), "unidad": unidad.codigo},
                    )
                messages.success(request, f"Pedido #{order_number or order_id} asignado a {repartidor}.")
        return redirect("logistica:domicilios_ecommerce")

    try:
        pedidos_pendientes = EcommerceClient().listar_pedidos_pendientes()
        error_conexion = ""
    except EcommerceIntegrationError as exc:
        pedidos_pendientes = []
        error_conexion = str(exc)

    entregas_recientes = (
        EntregaEcommerce.objects.select_related("repartidor__user", "unidad").order_by("-created_at")[:30]
    )

    return render(
        request,
        "logistica/domicilios_ecommerce.html",
        {
            "module_tabs": _module_tabs("rutas", request.user),
            "pedidos_pendientes": pedidos_pendientes,
            "error_conexion": error_conexion,
            "entregas_recientes": entregas_recientes,
            "unidades": Unidad.objects.filter(activa=True).order_by("codigo"),
            "repartidores": Repartidor.objects.filter(user__is_active=True).select_related("user").order_by("user__first_name", "user__username"),
        },
    )


@login_required
def domicilios_generales(request):
    """Captura y asigna servicios a domicilio que no vienen de la tienda en línea
    (llamada, WhatsApp, redes sociales)."""
    if not can_view_submodule(request.user, "logistica", "rutas"):
        raise PermissionDenied("No tienes permisos para ver Logística")

    if request.method == "POST":
        if not can_manage_submodule(request.user, "logistica", "rutas"):
            raise PermissionDenied("No tienes permisos para gestionar Logística")

        accion = request.POST.get("accion")

        if accion == "crear":
            cliente_nombre = (request.POST.get("cliente_nombre") or "").strip()
            cliente_telefono = (request.POST.get("cliente_telefono") or "").strip()
            direccion = (request.POST.get("direccion") or "").strip()
            canal_origen = (request.POST.get("canal_origen") or "").strip()
            canal_detalle = (request.POST.get("canal_detalle") or "").strip()
            notas = (request.POST.get("notas") or "").strip()

            if not cliente_nombre or not direccion:
                messages.error(request, "Captura nombre y dirección del cliente.")
            elif canal_origen not in dict(SolicitudDomicilio.CANAL_CHOICES):
                messages.error(request, "Selecciona un canal válido.")
            else:
                solicitud = SolicitudDomicilio.objects.create(
                    cliente_nombre=cliente_nombre,
                    cliente_telefono=cliente_telefono,
                    direccion=direccion,
                    canal_origen=canal_origen,
                    canal_detalle=canal_detalle,
                    notas=notas,
                    created_by=request.user,
                )
                log_event(
                    request.user,
                    "CREATE",
                    "logistica.SolicitudDomicilio",
                    solicitud.id,
                    {"cliente": cliente_nombre, "canal": canal_origen},
                )
                messages.success(request, f"Solicitud de {cliente_nombre} capturada.")

        elif accion == "asignar":
            solicitud_id = (request.POST.get("solicitud_id") or "").strip()
            repartidor_id = (request.POST.get("repartidor") or "").strip()
            unidad_id = (request.POST.get("unidad_operativa") or "").strip()

            solicitud = SolicitudDomicilio.objects.filter(pk=int(solicitud_id)).first() if solicitud_id.isdigit() else None
            repartidor = Repartidor.objects.filter(pk=int(repartidor_id), user__is_active=True).first() if repartidor_id.isdigit() else None
            unidad = Unidad.objects.filter(pk=int(unidad_id), activa=True).first() if unidad_id.isdigit() else None

            if solicitud is None:
                messages.error(request, "Solicitud inválida.")
            elif repartidor is None:
                messages.error(request, "Selecciona un repartidor activo.")
            elif unidad is None:
                messages.error(request, "Selecciona una unidad activa.")
            else:
                solicitud.repartidor = repartidor
                solicitud.unidad = unidad
                solicitud.estatus = SolicitudDomicilio.ESTATUS_ASIGNADO
                solicitud.asignado_en = timezone.now()
                solicitud.save(update_fields=["repartidor", "unidad", "estatus", "asignado_en"])
                log_event(
                    request.user,
                    "UPDATE",
                    "logistica.SolicitudDomicilio",
                    solicitud.id,
                    {"repartidor": str(repartidor), "unidad": unidad.codigo},
                )
                messages.success(request, f"Domicilio de {solicitud.cliente_nombre} asignado a {repartidor}.")

        return redirect("logistica:domicilios_generales")

    solicitudes_pendientes = (
        SolicitudDomicilio.objects.filter(estatus=SolicitudDomicilio.ESTATUS_PENDIENTE).order_by("-created_at")
    )
    solicitudes_recientes = (
        SolicitudDomicilio.objects.exclude(estatus=SolicitudDomicilio.ESTATUS_PENDIENTE)
        .select_related("repartidor__user", "unidad")
        .order_by("-created_at")[:30]
    )

    return render(
        request,
        "logistica/domicilios_generales.html",
        {
            "module_tabs": _module_tabs("rutas", request.user),
            "canal_choices": SolicitudDomicilio.CANAL_CHOICES,
            "solicitudes_pendientes": solicitudes_pendientes,
            "solicitudes_recientes": solicitudes_recientes,
            "unidades": Unidad.objects.filter(activa=True).order_by("codigo"),
            "repartidores": Repartidor.objects.filter(user__is_active=True).select_related("user").order_by("user__first_name", "user__username"),
        },
    )
