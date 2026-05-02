from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.core.exceptions import PermissionDenied
from django.db.models import Count, Q, Sum
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone

from core.access import can_manage_logistica, can_view_logistica
from core.audit import log_event
from core.models import Sucursal
from crm.models import PedidoCliente

from .models import (
    BitacoraSalidaLlegada,
    DocumentoUnidad,
    EntregaRuta,
    InspeccionVehiculo,
    LavadoUnidad,
    ReparacionUnidad,
    Repartidor,
    ReporteUnidad,
    RutaEntrega,
    ServicioRealizadoUnidad,
    TipoServicioUnidad,
    Unidad,
)


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


def _parse_date(raw: str | None):
    value = (raw or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _module_tabs(active: str) -> list[dict]:
    return [
        {"label": "Dashboard", "url_name": "logistica:home", "active": active == "dashboard"},
        {"label": "Ejecutivo", "url_name": "logistica:dashboard_ejecutivo", "active": active == "ejecutivo"},
        {"label": "Tickets", "url_name": "logistica:tickets_kanban", "active": active == "tickets"},
        {"label": "Flota", "url_name": "logistica:flota_resumen", "active": active == "flota"},
        {"label": "Reportes", "url_name": "logistica:reportes_lista", "active": active == "reportes"},
        {"label": "Bitácoras", "url_name": "logistica:bitacoras_lista", "active": active == "bitacoras"},
        {"label": "Rutas", "url_name": "logistica:rutas", "active": active == "rutas"},
        {"label": "Unidades", "url_name": "logistica:unidades_list", "active": active == "unidades"},
        {"label": "Capturas PWA", "url_name": "logistica:capturas_pwa", "active": active == "capturas"},
    ]


def _user_in_groups(user, groups: list[str]) -> bool:
    if not user.is_authenticated:
        return False
    if user.is_staff or user.is_superuser:
        return True
    return user.groups.filter(name__in=groups).exists()


def _can_view_logistica_ejecutivo(user) -> bool:
    return _user_in_groups(user, ["dg"])


def _can_manage_tickets_logistica(user) -> bool:
    return _user_in_groups(user, ["compras_logistica", "supervisor_logistica", "dg"])


def _can_view_flota_resumen(user) -> bool:
    return _user_in_groups(user, ["dg", "supervisor_logistica"])


def _can_manage_unidades(user) -> bool:
    if not user.is_authenticated:
        return False
    if user.is_staff or user.is_superuser:
        return True
    return user.groups.filter(name__in=["supervisor_logistica", "dg"]).exists()


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
    if not can_view_logistica(request.user):
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

    rutas_qs = RutaEntrega.objects.all()
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
        "module_tabs": _module_tabs("dashboard"),
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
            "module_tabs": _module_tabs("unidades"),
            "unidad": unidad,
            "sucursales": Sucursal.objects.filter(activa=True).order_by("codigo", "nombre"),
            "errors": errors or {},
        },
    )


@login_required
def unidades_list(request):
    if not _can_manage_unidades(request.user):
        raise PermissionDenied("No tienes permisos para gestionar unidades de Logística")

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
            "module_tabs": _module_tabs("unidades"),
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
    if not can_view_logistica(request.user):
        raise PermissionDenied("No tienes permisos para ver capturas de Logística")

    today = timezone.localdate()
    query = (request.GET.get("q") or "").strip()
    tipo = (request.GET.get("tipo") or "todas").strip().lower()

    bitacoras_qs = BitacoraSalidaLlegada.objects.select_related("repartidor__user", "unidad").order_by("-hora_salida", "-id")
    inspecciones_qs = InspeccionVehiculo.objects.select_related("repartidor__user", "unidad").order_by("-fecha", "-id")
    reportes_qs = ReporteUnidad.objects.select_related("repartidor__user", "unidad", "asignado_a").order_by("-fecha_reporte", "-id")

    if query:
        bitacoras_qs = bitacoras_qs.filter(
            Q(folio__icontains=query)
            | Q(unidad__codigo__icontains=query)
            | Q(unidad__placa__icontains=query)
            | Q(repartidor__user__username__icontains=query)
            | Q(repartidor__user__first_name__icontains=query)
            | Q(repartidor__user__last_name__icontains=query)
        )
        inspecciones_qs = inspecciones_qs.filter(
            Q(unidad__codigo__icontains=query)
            | Q(unidad__placa__icontains=query)
            | Q(repartidor__user__username__icontains=query)
            | Q(repartidor__user__first_name__icontains=query)
            | Q(repartidor__user__last_name__icontains=query)
        )
        reportes_qs = reportes_qs.filter(
            Q(unidad__codigo__icontains=query)
            | Q(unidad__placa__icontains=query)
            | Q(repartidor__user__username__icontains=query)
            | Q(repartidor__user__first_name__icontains=query)
            | Q(repartidor__user__last_name__icontains=query)
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
        "module_tabs": _module_tabs("capturas"),
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
        "costo_combustible_total": BitacoraSalidaLlegada.objects.aggregate(total=Sum("costo_combustible")).get("total") or Decimal("0"),
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
def rutas(request):
    if not can_view_logistica(request.user):
        raise PermissionDenied("No tienes permisos para ver Logística")

    if request.method == "POST":
        if not can_manage_logistica(request.user):
            raise PermissionDenied("No tienes permisos para gestionar Logística")

        nombre = (request.POST.get("nombre") or "").strip()
        if not nombre:
            messages.error(request, "El nombre de ruta es obligatorio.")
        else:
            ruta = RutaEntrega.objects.create(
                nombre=nombre,
                fecha_ruta=request.POST.get("fecha_ruta") or timezone.localdate(),
                chofer=(request.POST.get("chofer") or "").strip(),
                unidad=(request.POST.get("unidad") or "").strip(),
                estatus=(request.POST.get("estatus") or RutaEntrega.ESTATUS_PLANEADA).strip(),
                km_estimado=_parse_decimal(request.POST.get("km_estimado")),
                notas=(request.POST.get("notas") or "").strip(),
                created_by=request.user,
            )
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
            messages.success(request, f"Ruta {ruta.folio} creada.")
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
            | Q(unidad__icontains=q)
        )
    if estatus:
        rutas_qs = rutas_qs.filter(estatus=estatus)
    if enterprise_focus == "HOY":
        rutas_qs = rutas_qs.filter(fecha_ruta=timezone.localdate())
    elif enterprise_focus == "EN_RUTA":
        rutas_qs = rutas_qs.filter(estatus=RutaEntrega.ESTATUS_EN_RUTA)
    elif enterprise_focus == "PENDIENTES":
        rutas_qs = rutas_qs.filter(entregas__estatus=EntregaRuta.ESTATUS_PENDIENTE).distinct()
    elif enterprise_focus == "INCIDENCIAS":
        rutas_qs = rutas_qs.filter(entregas__estatus=EntregaRuta.ESTATUS_INCIDENCIA).distinct()

    rutas_total = RutaEntrega.objects.count()
    rutas_hoy = RutaEntrega.objects.filter(fecha_ruta=today).count()
    rutas_en_ruta = RutaEntrega.objects.filter(estatus=RutaEntrega.ESTATUS_EN_RUTA).count()
    entregas_pendientes = EntregaRuta.objects.filter(estatus=EntregaRuta.ESTATUS_PENDIENTE).count()
    incidencias = EntregaRuta.objects.filter(estatus=EntregaRuta.ESTATUS_INCIDENCIA).count()
    entregas_total = EntregaRuta.objects.count()
    rutas_liberadas = RutaEntrega.objects.exclude(estatus=RutaEntrega.ESTATUS_CANCELADA).exclude(
        Q(chofer__exact="") | Q(unidad__exact="")
    ).count()
    entregas_controladas = EntregaRuta.objects.filter(
        estatus__in=[EntregaRuta.ESTATUS_EN_CAMINO, EntregaRuta.ESTATUS_ENTREGADA]
    ).count()
    entregas_cerradas = max(entregas_total - entregas_pendientes - incidencias, 0)
    enterprise_chain = _logistica_enterprise_chain(
        rutas_total=rutas_total,
        rutas_hoy=rutas_hoy,
        rutas_en_ruta=rutas_en_ruta,
        entregas_pendientes=entregas_pendientes,
        incidencias=incidencias,
        entregas_completadas=EntregaRuta.objects.filter(estatus=EntregaRuta.ESTATUS_ENTREGADA).count(),
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

    context = {
        "module_tabs": _module_tabs("rutas"),
        "can_manage_logistica": can_manage_logistica(request.user),
        "rutas": rutas_qs.order_by("-fecha_ruta", "-id")[:200],
        "q": q,
        "estatus": estatus,
        "enterprise_focus": enterprise_focus,
        "date_from": date_from.isoformat() if date_from else "",
        "date_to": date_to.isoformat() if date_to else "",
        "estatus_choices": RutaEntrega.ESTATUS_CHOICES,
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
    if not can_view_logistica(request.user):
        raise PermissionDenied("No tienes permisos para ver Logística")

    ruta = get_object_or_404(RutaEntrega, pk=pk)

    if request.method == "POST":
        if not can_manage_logistica(request.user):
            raise PermissionDenied("No tienes permisos para gestionar Logística")

        action = (request.POST.get("action") or "").strip().lower()

        if action == "add_entrega":
            pedido = None
            pedido_id = (request.POST.get("pedido_id") or "").strip()
            if pedido_id.isdigit():
                pedido = PedidoCliente.objects.filter(pk=int(pedido_id)).first()

            entrega = EntregaRuta.objects.create(
                ruta=ruta,
                secuencia=int(request.POST.get("secuencia") or 1),
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

        if action == "ruta_status":
            estatus_nuevo = (request.POST.get("estatus") or "").strip().upper()
            if estatus_nuevo in {c[0] for c in RutaEntrega.ESTATUS_CHOICES}:
                from_status = ruta.estatus
                if from_status != estatus_nuevo:
                    ruta.estatus = estatus_nuevo
                    ruta.save(update_fields=["estatus", "updated_at"])
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
    rutas_liberadas = 1 if ruta.estatus != RutaEntrega.ESTATUS_CANCELADA and ruta.chofer and ruta.unidad else 0
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

    context = {
        "module_tabs": _module_tabs("rutas"),
        "can_manage_logistica": can_manage_logistica(request.user),
        "ruta": ruta,
        "entregas": entregas_qs,
        "pedidos": pedidos_disponibles,
        "estatus_ruta_choices": RutaEntrega.ESTATUS_CHOICES,
        "estatus_entrega_choices": EntregaRuta.ESTATUS_CHOICES,
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
        "module_tabs": _module_tabs("ejecutivo"),
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
        "module_tabs": _module_tabs("tickets"),
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
        "module_tabs": _module_tabs("flota"),
        "unidades_resumen": unidades_resumen,
        "today": today,
    }
    return render(request, "logistica/flota_resumen.html", context)


@login_required
def unidad_detalle(request, pk):
    if not _can_manage_tickets_logistica(request.user):
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
        "module_tabs": _module_tabs("unidades"),
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
    if not _can_manage_tickets_logistica(request.user):
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
    if not _can_manage_tickets_logistica(request.user):
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
    if not _can_manage_tickets_logistica(request.user):
        raise PermissionDenied("No tienes permisos para registrar lavados de unidad")
    unidad = get_object_or_404(Unidad, pk=pk)
    if request.method == "POST":
        LavadoUnidad.objects.create(
            unidad=unidad,
            fecha=_parse_date(request.POST.get("fecha")) or timezone.localdate(),
            costo=_parse_decimal(request.POST.get("costo")) if request.POST.get("costo") else None,
            notas=(request.POST.get("notas") or "").strip(),
            registrado_por=request.user,
        )
        messages.success(request, "Lavado registrado correctamente.")
    return _redirect_unidad_tab(pk, "lavados")


@login_required
def unidad_reparacion_nueva(request, pk):
    if not _can_manage_tickets_logistica(request.user):
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
    if not _can_manage_tickets_logistica(request.user):
        raise PermissionDenied("No tienes permisos para ver reportes de Logística")

    qs = ReporteUnidad.objects.select_related("unidad", "repartidor__user").order_by("-fecha_reporte")
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
            "module_tabs": _module_tabs("reportes"),
            "reportes": reportes,
            "unidades": Unidad.objects.filter(activa=True).order_by("codigo"),
            "estatus_choices": ReporteUnidad.ESTATUS_CHOICES,
            "severidad_choices": ReporteUnidad.SEVERIDAD_CHOICES,
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
def bitacoras_lista(request):
    if not _can_manage_tickets_logistica(request.user):
        raise PermissionDenied("No tienes permisos para ver bitácoras de Logística")

    qs = BitacoraSalidaLlegada.objects.select_related("unidad", "repartidor__user").order_by("-hora_salida")
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

    page = Paginator(qs, 20).get_page(request.GET.get("page"))
    bitacoras = [_decorate_bitacora(bitacora) for bitacora in page]
    return render(
        request,
        "logistica/bitacoras_lista.html",
        {
            "module_tabs": _module_tabs("bitacoras"),
            "bitacoras": bitacoras,
            "bitacoras_page": page,
            "unidades": Unidad.objects.filter(activa=True).order_by("codigo"),
            "repartidores": Repartidor.objects.select_related("user").order_by("user__first_name", "user__username"),
            "filters": {
                "unidad": unidad_id,
                "repartidor": repartidor_id,
                "cerrada": cerrada,
                "fecha_desde": fecha_desde.isoformat() if fecha_desde else "",
                "fecha_hasta": fecha_hasta.isoformat() if fecha_hasta else "",
            },
        },
    )
