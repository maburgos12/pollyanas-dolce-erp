from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.db.models import Count, Q
from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone

from core.access import can_manage_crm, can_view_crm
from core.audit import log_event

from .models import Cliente, PedidoCliente, SeguimientoPedido


def _parse_decimal(raw: str | None) -> Decimal:
    try:
        return Decimal(str(raw or "0"))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _module_tabs(active: str) -> list[dict]:
    return [
        {"label": "Clientes", "url_name": "crm:clientes", "active": active == "clientes"},
        {"label": "Pedidos", "url_name": "crm:pedidos", "active": active == "pedidos"},
    ]


def _crm_enterprise_chain(
    *,
    clientes_total: int,
    clientes_activos: int,
    pedidos_abiertos: int,
    pedidos_hoy: int,
    entregados: int,
    cancelados: int,
) -> list[dict]:
    chain = [
        {
            "step": "01",
            "title": "Maestro de clientes",
            "detail": "Clientes activos y datos base para captura comercial.",
            "count": clientes_activos,
            "status": "Base comercial activa" if clientes_activos else "Sin clientes activos",
            "tone": "success" if clientes_activos else "warning",
            "url": reverse("crm:clientes"),
            "cta": "Abrir clientes",
            "owner": "CRM / Ventas",
            "next_step": "Mantener cartera activa y lista para convertir en pedido.",
        },
        {
            "step": "02",
            "title": "Pedidos capturados",
            "detail": "Pedidos comerciales abiertos y programados.",
            "count": pedidos_abiertos,
            "status": "Operación en curso" if pedidos_abiertos else "Sin pedidos abiertos",
            "tone": "success" if pedidos_abiertos else "warning",
            "url": reverse("crm:pedidos"),
            "cta": "Abrir pedidos",
            "owner": "CRM / Ventas",
            "next_step": "Confirmar pedidos abiertos y sostener trazabilidad comercial.",
        },
        {
            "step": "03",
            "title": "Seguimiento del día",
            "detail": "Altas o cambios recientes que requieren seguimiento comercial.",
            "count": pedidos_hoy,
            "status": "Con actividad" if pedidos_hoy else "Sin movimientos hoy",
            "tone": "success" if pedidos_hoy else "warning",
            "url": reverse("crm:pedidos"),
            "cta": "Ver actividad",
            "owner": "CRM / Seguimiento",
            "next_step": "Registrar avances del día y asegurar compromiso con cliente.",
        },
        {
            "step": "04",
            "title": "Cierre documental",
            "detail": "Pedidos entregados o cancelados con trazabilidad comercial.",
            "count": entregados,
            "status": "Con cierres" if entregados else f"{cancelados} cancelados",
            "tone": "success" if entregados else "warning",
            "url": reverse("crm:pedidos") + f"?estatus={PedidoCliente.ESTATUS_ENTREGADO}",
            "cta": "Ver cierres",
            "owner": "CRM / Auditoría comercial",
            "next_step": "Cerrar bitácora comercial y dejar evidencia terminal del pedido.",
        },
    ]
    for index, item in enumerate(chain):
        previous = chain[index - 1] if index else None
        item["completion"] = 100 if item.get("tone") == "success" else 60
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


def _crm_document_stage_rows(
    *,
    clientes_total: int,
    clientes_activos: int,
    pedidos_abiertos: int,
    pedidos_confirmados: int,
    pedidos_produccion: int,
    pedidos_entregados: int,
) -> list[dict]:
    rows = [
        {
            "label": "Clientes activos",
            "open": clientes_activos,
            "closed": max(clientes_total - clientes_activos, 0),
            "detail": "Clientes listos para operación frente a inactivos.",
            "url": reverse("crm:clientes"),
            "owner": "CRM / Ventas",
            "next_step": "Actualizar cartera activa",
        },
        {
            "label": "Pedidos abiertos",
            "open": pedidos_abiertos,
            "closed": pedidos_entregados,
            "detail": "Pedidos aún operando frente a pedidos ya cerrados.",
            "url": reverse("crm:pedidos"),
            "owner": "CRM / Ventas",
            "next_step": "Cerrar pedidos activos",
        },
        {
            "label": "Pedidos confirmados",
            "open": pedidos_confirmados,
            "closed": max(pedidos_abiertos - pedidos_confirmados, 0),
            "detail": "Pedidos listos para ejecución comercial.",
            "url": reverse("crm:pedidos") + f"?estatus={PedidoCliente.ESTATUS_CONFIRMADO}",
            "owner": "CRM / Seguimiento",
            "next_step": "Asegurar compromiso comercial",
        },
        {
            "label": "En producción",
            "open": pedidos_produccion,
            "closed": max(pedidos_entregados, 0),
            "detail": "Pedidos produciéndose frente a pedidos ya entregados.",
            "url": reverse("crm:pedidos") + f"?estatus={PedidoCliente.ESTATUS_EN_PRODUCCION}",
            "owner": "Producción / CRM",
            "next_step": "Liberar entrega documentada",
        },
    ]
    for row in rows:
        total = int(row["open"] or 0) + int(row["closed"] or 0)
        row["completion"] = int(round((int(row["closed"] or 0) / total) * 100)) if total else 0
    return rows


def _crm_operational_health_cards(
    *,
    focus: str,
    primary_open: int,
    secondary_open: int,
    ready_count: int,
) -> list[dict[str, object]]:
    if focus == "clientes":
        return [
            {
                "label": "Clientes activos",
                "value": ready_count,
                "tone": "success" if ready_count else "warning",
                "detail": "Base comercial disponible para operación.",
            },
            {
                "label": "Pedidos abiertos",
                "value": primary_open,
                "tone": "warning" if primary_open else "success",
                "detail": "Pedidos comerciales pendientes de cierre.",
            },
            {
                "label": "Actividad de hoy",
                "value": secondary_open,
                "tone": "primary",
                "detail": "Movimientos o altas comerciales del día.",
            },
        ]
    if focus == "pedidos":
        return [
            {
                "label": "Pedidos abiertos",
                "value": primary_open,
                "tone": "warning" if primary_open else "success",
                "detail": "Pedidos aún operando o por entregar.",
            },
            {
                "label": "Pedidos del día",
                "value": secondary_open,
                "tone": "primary",
                "detail": "Pedidos creados hoy en el módulo comercial.",
            },
            {
                "label": "Cierres documentados",
                "value": ready_count,
                "tone": "success" if ready_count else "warning",
                "detail": "Pedidos entregados con trazabilidad comercial.",
            },
        ]
    return [
        {
            "label": "Seguimientos",
            "value": primary_open,
            "tone": "primary",
            "detail": "Eventos registrados en la bitácora del pedido.",
        },
        {
            "label": "Pedido activo",
            "value": secondary_open,
            "tone": "warning" if secondary_open else "success",
            "detail": "Indica si el pedido sigue abierto en operación.",
        },
        {
            "label": "Pedido cerrado",
            "value": ready_count,
            "tone": "success" if ready_count else "warning",
            "detail": "Pedido entregado o cancelado con cierre documental.",
        },
    ]


def _crm_governance_rows(rows: list[dict], owner_default: str = "CRM / Operación") -> list[dict]:
    governance_rows: list[dict] = []
    for row in rows:
        governance_rows.append(
            {
                "front": row.get("label", "Frente CRM"),
                "owner": row.get("owner") or owner_default,
                "blockers": int(row.get("open") or 0),
                "completion": int(row.get("completion") or 0),
                "detail": row.get("detail", ""),
                "next_step": row.get("next_step") or "Seguir flujo comercial",
                "url": row.get("url") or reverse("crm:pedidos"),
                "cta": "Abrir",
            }
        )
    return governance_rows


def _crm_maturity_summary(*, chain: list[dict], default_url: str) -> dict:
    total_steps = len(chain)
    completed_steps = sum(1 for item in chain if item.get("tone") == "success")
    attention_steps = max(total_steps - completed_steps, 0)
    coverage_pct = int(round((completed_steps / total_steps) * 100)) if total_steps else 0
    next_priority = next((item for item in chain if item.get("tone") != "success"), None)
    return {
        "completed_steps": completed_steps,
        "attention_steps": attention_steps,
        "coverage_pct": coverage_pct,
        "next_priority_title": next_priority.get("title", "Cadena comercial estabilizada") if next_priority else "Cadena comercial estabilizada",
        "next_priority_detail": next_priority.get("detail", "Sin brechas abiertas en CRM.") if next_priority else "Sin brechas abiertas en CRM.",
        "next_priority_url": next_priority.get("url", default_url) if next_priority else default_url,
        "next_priority_cta": next_priority.get("cta", "Abrir CRM") if next_priority else "Abrir CRM",
    }


def _crm_critical_path_rows(chain: list[dict[str, object]]) -> list[dict[str, object]]:
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
                "title": item.get("title", "Tramo comercial"),
                "owner": item.get("owner", "CRM / Operación"),
                "status": item.get("status", "Sin estado"),
                "tone": item.get("tone", "warning"),
                "count": int(item.get("count") or 0),
                "completion": int(item.get("completion") or 0),
                "depends_on": item.get("depends_on", "Origen del módulo"),
                "dependency_status": item.get("dependency_status", "Sin dependencia registrada"),
                "detail": item.get("detail", ""),
                "next_step": item.get("next_step", "Continuar flujo comercial"),
                "url": item.get("url", reverse("crm:pedidos")),
                "cta": item.get("cta", "Abrir"),
            }
        )
    return rows


def _crm_executive_radar_rows(
    governance_rows: list[dict[str, object]],
    *,
    default_owner: str = "CRM / Operación",
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
            dominant_blocker = row.get("detail", "") or "Brecha comercial en seguimiento"
        else:
            tone = "danger"
            status = "Con bloqueo"
            dominant_blocker = row.get("detail", "") or "Bloqueo comercial abierto"
        rows.append(
            {
                "phase": row.get("front", "Frente comercial"),
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


def _crm_command_center(
    *,
    governance_rows: list[dict],
    maturity_summary: dict[str, object],
    default_url: str,
    default_cta: str,
) -> dict[str, object]:
    blockers = sum(int(row.get("open", 0) or 0) for row in governance_rows)
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
        "owner": governance_rows[0].get("owner", "CRM / Ventas") if governance_rows else "CRM / Ventas",
        "status": status,
        "tone": tone,
        "blockers": blockers,
        "next_step": maturity_summary.get("next_priority_detail", "Sin acciones pendientes."),
        "url": maturity_summary.get("next_priority_url", default_url),
        "cta": maturity_summary.get("next_priority_cta", default_cta),
    }


def _crm_handoff_map(*, clientes_gap: int, pedidos_abiertos: int, pedidos_hoy: int, delivered_gap: int, pedidos_url: str) -> list[dict]:
    return [
        {
            "label": "Clientes -> Pedidos",
            "detail": "La base comercial debe estar activa antes de prometer y capturar pedidos.",
            "count": clientes_gap,
            "tone": "success" if clientes_gap == 0 else "warning",
            "status": "Controlado" if clientes_gap == 0 else "Con brecha",
            "url": reverse("crm:clientes"),
            "cta": "Abrir clientes",
            "owner": "CRM / Ventas",
            "depends_on": "Base comercial activa",
            "exit_criteria": "No abrir pedidos sobre clientes con brecha operativa o sin activación.",
            "next_step": "Cerrar clientes pendientes y consolidar cartera activa.",
            "completion": 100 if clientes_gap == 0 else 60,
        },
        {
            "label": "Pedidos -> Seguimiento",
            "detail": "Todo pedido abierto debe quedar confirmado o en producción con traza comercial.",
            "count": pedidos_abiertos,
            "tone": "success" if pedidos_abiertos == 0 else "warning",
            "status": "Controlado" if pedidos_abiertos == 0 else "Seguimiento abierto",
            "url": pedidos_url,
            "cta": "Abrir pedidos",
            "owner": "CRM / Seguimiento",
            "depends_on": "Pedido capturado y confirmado",
            "exit_criteria": "Todo pedido abierto debe tener seguimiento vigente y estatus actualizado.",
            "next_step": "Actualizar seguimiento y sostener trazabilidad comercial del pedido.",
            "completion": 100 if pedidos_abiertos == 0 else 55,
        },
        {
            "label": "Seguimiento -> Cierre",
            "detail": "La actividad del día debe reflejarse en cierre documental correcto.",
            "count": max(pedidos_hoy, delivered_gap),
            "tone": "success" if delivered_gap == 0 else "warning",
            "status": "Controlado" if delivered_gap == 0 else "Cierre pendiente",
            "url": pedidos_url,
            "cta": "Ver cierres",
            "owner": "CRM / Auditoría comercial",
            "depends_on": "Seguimiento y operación comercial cerrados",
            "exit_criteria": "Todo pedido debe terminar entregado o cancelado con evidencia terminal.",
            "next_step": "Cerrar actividad del día y documentar evidencia de entrega o cancelación.",
            "completion": 100 if delivered_gap == 0 else 40,
        },
    ]


def _crm_focus_cards(*, selected_focus: str) -> list[dict[str, object]]:
    abiertos = PedidoCliente.objects.exclude(
        estatus__in=[PedidoCliente.ESTATUS_ENTREGADO, PedidoCliente.ESTATUS_CANCELADO]
    ).count()
    hoy = PedidoCliente.objects.filter(created_at__date=timezone.localdate()).count()
    produccion = PedidoCliente.objects.filter(estatus=PedidoCliente.ESTATUS_EN_PRODUCCION).count()
    cierres = PedidoCliente.objects.filter(estatus=PedidoCliente.ESTATUS_ENTREGADO).count()
    cards = [
        {
            "key": "ABIERTOS",
            "label": "Pedidos abiertos",
            "count": abiertos,
            "detail": "Pedidos comerciales todavía en ejecución o pendientes de entrega.",
            "url": reverse("crm:pedidos") + "?enterprise_focus=ABIERTOS",
        },
        {
            "key": "HOY",
            "label": "Actividad del día",
            "count": hoy,
            "detail": "Pedidos creados o movidos hoy dentro del circuito comercial.",
            "url": reverse("crm:pedidos") + "?enterprise_focus=HOY",
        },
        {
            "key": "PRODUCCION",
            "label": "Pedidos en producción",
            "count": produccion,
            "detail": "Pedidos ya enviados a operación y aún sin cierre comercial.",
            "url": reverse("crm:pedidos") + f"?enterprise_focus=PRODUCCION&estatus={PedidoCliente.ESTATUS_EN_PRODUCCION}",
        },
        {
            "key": "CIERRE",
            "label": "Pedidos cerrados",
            "count": cierres,
            "detail": "Pedidos entregados con cierre documental y trazabilidad comercial.",
            "url": reverse("crm:pedidos") + f"?enterprise_focus=CIERRE&estatus={PedidoCliente.ESTATUS_ENTREGADO}",
        },
    ]
    for card in cards:
        card["is_active"] = card["key"] == selected_focus
    return cards


def _crm_focus_summary(*, selected_focus: str, count: int) -> dict[str, object] | None:
    if not selected_focus:
        return None
    mapping = {
        "ABIERTOS": ("Pedidos abiertos", "Vista enfocada en pedidos que siguen activos dentro de la cadena comercial."),
        "HOY": ("Actividad del día", "Vista enfocada en la actividad comercial registrada durante el día."),
        "PRODUCCION": ("Pedidos en producción", "Vista enfocada en pedidos que ya pasaron a ejecución operativa."),
        "CIERRE": ("Pedidos cerrados", "Vista enfocada en pedidos ya entregados y documentados."),
    }
    title, detail = mapping.get(
        selected_focus,
        ("Foco comercial", "Vista enfocada en un subconjunto operativo del módulo comercial."),
    )
    return {
        "title": title,
        "detail": detail,
        "count": count,
        "clear_url": reverse("crm:pedidos"),
    }


def _crm_release_gate_rows(
    *,
    clientes_total: int,
    clientes_activos: int,
    pedidos_confirmados: int,
    pedidos_produccion: int,
    pedidos_entregados: int,
    default_url: str,
) -> list[dict]:
    return [
        {
            "step": "01",
            "title": "Maestro comercial listo para operar",
            "detail": "Clientes activos y utilizables en operación comercial.",
            "completed": clientes_activos,
            "open_count": max(clientes_total - clientes_activos, 0),
            "total": max(clientes_total, 1),
            "tone": "success" if clientes_total and clientes_activos >= clientes_total else "warning",
            "url": default_url,
            "cta": "Revisar clientes",
        },
        {
            "step": "02",
            "title": "Pedido confirmado y listo para operar",
            "detail": "Pedidos confirmados listos para producción o surtido.",
            "completed": pedidos_confirmados + pedidos_produccion + pedidos_entregados,
            "open_count": max(clientes_activos - (pedidos_confirmados + pedidos_produccion + pedidos_entregados), 0),
            "total": max(clientes_activos, 1),
            "tone": "success" if pedidos_confirmados or pedidos_produccion or pedidos_entregados else "warning",
            "url": reverse("crm:pedidos"),
            "cta": "Revisar pedidos",
        },
        {
            "step": "03",
            "title": "Entrega y cierre documental",
            "detail": "Pedidos entregados con seguimiento completo en bitácora.",
            "completed": pedidos_entregados,
            "open_count": max((pedidos_confirmados + pedidos_produccion + pedidos_entregados) - pedidos_entregados, 0),
            "total": max((pedidos_confirmados + pedidos_produccion + pedidos_entregados), 1),
            "tone": "success" if pedidos_entregados else "warning",
            "url": reverse("crm:pedidos"),
            "cta": "Ver cierres",
        },
    ]


@login_required
def clientes(request: HttpRequest) -> HttpResponse:
    if not can_view_crm(request.user):
        raise PermissionDenied("No tienes permisos para ver CRM")

    if request.method == "POST":
        if not can_manage_crm(request.user):
            raise PermissionDenied("No tienes permisos para gestionar CRM")

        nombre = (request.POST.get("nombre") or "").strip()
        if not nombre:
            messages.error(request, "El nombre del cliente es obligatorio.")
        else:
            cliente = Cliente.objects.create(
                nombre=nombre,
                telefono=(request.POST.get("telefono") or "").strip(),
                email=(request.POST.get("email") or "").strip(),
                tipo_cliente=(request.POST.get("tipo_cliente") or "").strip(),
                sucursal_referencia=(request.POST.get("sucursal_referencia") or "").strip(),
                notas=(request.POST.get("notas") or "").strip(),
            )
            log_event(
                request.user,
                "CREATE",
                "crm.Cliente",
                str(cliente.id),
                {
                    "codigo": cliente.codigo,
                    "nombre": cliente.nombre,
                },
            )
            messages.success(request, f"Cliente {cliente.nombre} creado.")
            return redirect("crm:clientes")

    q = (request.GET.get("q") or "").strip()
    estado = (request.GET.get("estado") or "activos").strip().lower()

    qs = Cliente.objects.all().annotate(total_pedidos=Count("pedidos"))
    if q:
        qs = qs.filter(
            Q(nombre__icontains=q)
            | Q(codigo__icontains=q)
            | Q(telefono__icontains=q)
            | Q(email__icontains=q)
        )
    if estado == "activos":
        qs = qs.filter(activo=True)
    elif estado == "inactivos":
        qs = qs.filter(activo=False)

    clientes_total = Cliente.objects.count()
    clientes_activos = Cliente.objects.filter(activo=True).count()
    pedidos_abiertos = PedidoCliente.objects.exclude(
        estatus__in=[PedidoCliente.ESTATUS_ENTREGADO, PedidoCliente.ESTATUS_CANCELADO]
    ).count()
    pedidos_hoy = PedidoCliente.objects.filter(created_at__date=timezone.localdate()).count()
    entregados = PedidoCliente.objects.filter(estatus=PedidoCliente.ESTATUS_ENTREGADO).count()
    cancelados = PedidoCliente.objects.filter(estatus=PedidoCliente.ESTATUS_CANCELADO).count()
    enterprise_chain = _crm_enterprise_chain(
        clientes_total=clientes_total,
        clientes_activos=clientes_activos,
        pedidos_abiertos=pedidos_abiertos,
        pedidos_hoy=pedidos_hoy,
        entregados=entregados,
        cancelados=cancelados,
    )
    crm_maturity_summary = _crm_maturity_summary(chain=enterprise_chain, default_url=reverse("crm:clientes"))
    crm_handoff_map = _crm_handoff_map(
        clientes_gap=max(clientes_total - clientes_activos, 0),
        pedidos_abiertos=pedidos_abiertos,
        pedidos_hoy=pedidos_hoy,
        delivered_gap=max(pedidos_abiertos - entregados, 0),
        pedidos_url=reverse("crm:pedidos"),
    )
    document_stage_rows = _crm_document_stage_rows(
        clientes_total=clientes_total,
        clientes_activos=clientes_activos,
        pedidos_abiertos=pedidos_abiertos,
        pedidos_confirmados=PedidoCliente.objects.filter(estatus=PedidoCliente.ESTATUS_CONFIRMADO).count(),
        pedidos_produccion=PedidoCliente.objects.filter(estatus=PedidoCliente.ESTATUS_EN_PRODUCCION).count(),
        pedidos_entregados=entregados,
    )
    release_gate_rows = _crm_release_gate_rows(
        clientes_total=clientes_total,
        clientes_activos=clientes_activos,
        pedidos_confirmados=PedidoCliente.objects.filter(estatus=PedidoCliente.ESTATUS_CONFIRMADO).count(),
        pedidos_produccion=PedidoCliente.objects.filter(estatus=PedidoCliente.ESTATUS_EN_PRODUCCION).count(),
        pedidos_entregados=entregados,
        default_url=reverse("crm:clientes"),
    )

    ctx = {
        "module_tabs": _module_tabs("clientes"),
        "clientes": qs.order_by("nombre")[:500],
        "q": q,
        "estado": estado,
        "total_clientes": clientes_total,
        "total_clientes_activos": clientes_activos,
        "total_pedidos_abiertos": pedidos_abiertos,
        "can_manage_crm": can_manage_crm(request.user),
        "enterprise_chain": enterprise_chain,
        "crm_maturity_summary": crm_maturity_summary,
        "crm_handoff_map": crm_handoff_map,
        "critical_path_rows": _crm_critical_path_rows(enterprise_chain),
        "document_stage_rows": document_stage_rows,
        "erp_governance_rows": _crm_governance_rows(document_stage_rows, owner_default="CRM / Ventas"),
        "executive_radar_rows": _crm_executive_radar_rows(
            _crm_governance_rows(document_stage_rows, owner_default="CRM / Ventas"),
            default_owner="CRM / Ventas",
            fallback_url=reverse("crm:clientes"),
        ),
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
        "operational_health_cards": _crm_operational_health_cards(
            focus="clientes",
            primary_open=pedidos_abiertos,
            secondary_open=pedidos_hoy,
            ready_count=clientes_activos,
        ),
        "erp_command_center": _crm_command_center(
            governance_rows=_crm_governance_rows(document_stage_rows, owner_default="CRM / Ventas"),
            maturity_summary=crm_maturity_summary,
            default_url=reverse("crm:clientes"),
            default_cta="Abrir clientes",
        ),
    }
    return render(request, "crm/clientes.html", ctx)


@login_required
def pedidos(request: HttpRequest) -> HttpResponse:
    if not can_view_crm(request.user):
        raise PermissionDenied("No tienes permisos para ver CRM")

    if request.method == "POST":
        if not can_manage_crm(request.user):
            raise PermissionDenied("No tienes permisos para gestionar CRM")

        cliente_id = request.POST.get("cliente_id")
        descripcion = (request.POST.get("descripcion") or "").strip()
        if not cliente_id or not descripcion:
            messages.error(request, "Cliente y descripción son obligatorios.")
        else:
            cliente = get_object_or_404(Cliente, pk=cliente_id)
            pedido = PedidoCliente.objects.create(
                cliente=cliente,
                descripcion=descripcion,
                fecha_compromiso=request.POST.get("fecha_compromiso") or None,
                sucursal=(request.POST.get("sucursal") or "").strip(),
                canal=(request.POST.get("canal") or PedidoCliente.CANAL_MOSTRADOR).strip(),
                prioridad=(request.POST.get("prioridad") or PedidoCliente.PRIORIDAD_MEDIA).strip(),
                estatus=(request.POST.get("estatus") or PedidoCliente.ESTATUS_NUEVO).strip(),
                monto_estimado=_parse_decimal(request.POST.get("monto_estimado")),
                created_by=request.user,
            )
            SeguimientoPedido.objects.create(
                pedido=pedido,
                estatus_anterior="",
                estatus_nuevo=pedido.estatus,
                comentario="Alta de pedido",
                created_by=request.user,
            )
            log_event(
                request.user,
                "CREATE",
                "crm.PedidoCliente",
                str(pedido.id),
                {
                    "folio": pedido.folio,
                    "cliente": pedido.cliente.nombre,
                    "estatus": pedido.estatus,
                    "monto_estimado": str(pedido.monto_estimado),
                },
            )
            messages.success(request, f"Pedido {pedido.folio} creado.")
            return redirect("crm:pedidos")

    q = (request.GET.get("q") or "").strip()
    estatus = (request.GET.get("estatus") or "").strip()
    prioridad = (request.GET.get("prioridad") or "").strip()
    enterprise_focus = (request.GET.get("enterprise_focus") or "").strip().upper()

    pedidos_qs = PedidoCliente.objects.select_related("cliente")
    if q:
        pedidos_qs = pedidos_qs.filter(
            Q(folio__icontains=q)
            | Q(cliente__nombre__icontains=q)
            | Q(descripcion__icontains=q)
            | Q(sucursal__icontains=q)
        )
    if estatus:
        pedidos_qs = pedidos_qs.filter(estatus=estatus)
    if prioridad:
        pedidos_qs = pedidos_qs.filter(prioridad=prioridad)
    if enterprise_focus == "ABIERTOS":
        pedidos_qs = pedidos_qs.exclude(
            estatus__in=[PedidoCliente.ESTATUS_ENTREGADO, PedidoCliente.ESTATUS_CANCELADO]
        )
    elif enterprise_focus == "HOY":
        pedidos_qs = pedidos_qs.filter(created_at__date=timezone.localdate())
    elif enterprise_focus == "PRODUCCION":
        pedidos_qs = pedidos_qs.filter(estatus=PedidoCliente.ESTATUS_EN_PRODUCCION)
    elif enterprise_focus == "CIERRE":
        pedidos_qs = pedidos_qs.filter(estatus=PedidoCliente.ESTATUS_ENTREGADO)

    pedidos_abiertos = PedidoCliente.objects.exclude(
        estatus__in=[PedidoCliente.ESTATUS_ENTREGADO, PedidoCliente.ESTATUS_CANCELADO]
    ).count()
    pedidos_hoy = PedidoCliente.objects.filter(created_at__date=timezone.localdate()).count()
    entregados = PedidoCliente.objects.filter(estatus=PedidoCliente.ESTATUS_ENTREGADO).count()
    cancelados = PedidoCliente.objects.filter(estatus=PedidoCliente.ESTATUS_CANCELADO).count()
    enterprise_chain = _crm_enterprise_chain(
        clientes_total=Cliente.objects.count(),
        clientes_activos=Cliente.objects.filter(activo=True).count(),
        pedidos_abiertos=pedidos_abiertos,
        pedidos_hoy=pedidos_hoy,
        entregados=entregados,
        cancelados=cancelados,
    )
    crm_maturity_summary = _crm_maturity_summary(chain=enterprise_chain, default_url=reverse("crm:pedidos"))
    crm_handoff_map = _crm_handoff_map(
        clientes_gap=max(Cliente.objects.count() - Cliente.objects.filter(activo=True).count(), 0),
        pedidos_abiertos=pedidos_abiertos,
        pedidos_hoy=pedidos_hoy,
        delivered_gap=max(pedidos_abiertos - entregados, 0),
        pedidos_url=reverse("crm:pedidos"),
    )
    document_stage_rows = _crm_document_stage_rows(
        clientes_total=Cliente.objects.count(),
        clientes_activos=Cliente.objects.filter(activo=True).count(),
        pedidos_abiertos=pedidos_abiertos,
        pedidos_confirmados=PedidoCliente.objects.filter(estatus=PedidoCliente.ESTATUS_CONFIRMADO).count(),
        pedidos_produccion=PedidoCliente.objects.filter(estatus=PedidoCliente.ESTATUS_EN_PRODUCCION).count(),
        pedidos_entregados=entregados,
    )
    release_gate_rows = _crm_release_gate_rows(
        clientes_total=Cliente.objects.count(),
        clientes_activos=Cliente.objects.filter(activo=True).count(),
        pedidos_confirmados=PedidoCliente.objects.filter(estatus=PedidoCliente.ESTATUS_CONFIRMADO).count(),
        pedidos_produccion=PedidoCliente.objects.filter(estatus=PedidoCliente.ESTATUS_EN_PRODUCCION).count(),
        pedidos_entregados=entregados,
        default_url=reverse("crm:pedidos"),
    )

    ctx = {
        "module_tabs": _module_tabs("pedidos"),
        "clientes": Cliente.objects.filter(activo=True).order_by("nombre"),
        "pedidos": pedidos_qs.order_by("-created_at")[:500],
        "estatus_choices": PedidoCliente.ESTATUS_CHOICES,
        "prioridad_choices": PedidoCliente.PRIORIDAD_CHOICES,
        "canal_choices": PedidoCliente.CANAL_CHOICES,
        "q": q,
        "estatus": estatus,
        "prioridad": prioridad,
        "enterprise_focus": enterprise_focus,
        "can_manage_crm": can_manage_crm(request.user),
        "pedidos_abiertos": pedidos_abiertos,
        "pedidos_hoy": pedidos_hoy,
        "enterprise_chain": enterprise_chain,
        "crm_maturity_summary": crm_maturity_summary,
        "crm_handoff_map": crm_handoff_map,
        "critical_path_rows": _crm_critical_path_rows(enterprise_chain),
        "document_stage_rows": document_stage_rows,
        "erp_governance_rows": _crm_governance_rows(document_stage_rows, owner_default="CRM / Ventas"),
        "executive_radar_rows": _crm_executive_radar_rows(
            _crm_governance_rows(document_stage_rows, owner_default="CRM / Ventas"),
            default_owner="CRM / Ventas",
            fallback_url=reverse("crm:pedidos"),
        ),
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
        "focus_cards": _crm_focus_cards(selected_focus=enterprise_focus),
        "focus_summary": _crm_focus_summary(
            selected_focus=enterprise_focus,
            count=pedidos_qs.count(),
        ),
        "operational_health_cards": _crm_operational_health_cards(
            focus="pedidos",
            primary_open=pedidos_abiertos,
            secondary_open=pedidos_hoy,
            ready_count=entregados,
        ),
        "erp_command_center": _crm_command_center(
            governance_rows=_crm_governance_rows(document_stage_rows, owner_default="CRM / Ventas"),
            maturity_summary=crm_maturity_summary,
            default_url=reverse("crm:pedidos"),
            default_cta="Abrir pedidos",
        ),
    }

    # Conteo por estatus para tarjeta rápida
    conteos = {
        key: PedidoCliente.objects.filter(estatus=key).count()
        for key, _ in PedidoCliente.ESTATUS_CHOICES
    }
    ctx["conteos_estatus"] = conteos
    return render(request, "crm/pedidos.html", ctx)


@login_required
def pedido_detail(request: HttpRequest, pedido_id: int) -> HttpResponse:
    if not can_view_crm(request.user):
        raise PermissionDenied("No tienes permisos para ver CRM")

    pedido = get_object_or_404(PedidoCliente.objects.select_related("cliente"), pk=pedido_id)

    if request.method == "POST":
        if not can_manage_crm(request.user):
            raise PermissionDenied("No tienes permisos para gestionar CRM")

        comentario = (request.POST.get("comentario") or "").strip()
        estatus_nuevo = (request.POST.get("estatus_nuevo") or "").strip()
        if not comentario and not estatus_nuevo:
            messages.error(request, "Captura comentario o cambia estatus para guardar seguimiento.")
        else:
            with transaction.atomic():
                estatus_anterior = pedido.estatus
                estatus_registro = estatus_nuevo if estatus_nuevo else ""
                if estatus_nuevo and estatus_nuevo != pedido.estatus:
                    pedido.estatus = estatus_nuevo
                    pedido.save(update_fields=["estatus", "updated_at"])
                SeguimientoPedido.objects.create(
                    pedido=pedido,
                    estatus_anterior=estatus_anterior if estatus_registro else "",
                    estatus_nuevo=estatus_registro,
                    comentario=comentario,
                    created_by=request.user,
                )
            log_event(
                request.user,
                "UPDATE",
                "crm.PedidoCliente",
                str(pedido.id),
                {
                    "folio": pedido.folio,
                    "estatus_anterior": estatus_anterior,
                    "estatus_nuevo": pedido.estatus,
                    "comentario": comentario,
                },
            )
            messages.success(request, "Seguimiento guardado.")
            return redirect("crm:pedido_detail", pedido_id=pedido.id)

    estatus_actual = pedido.estatus
    chain_count = pedido.seguimientos.count()
    enterprise_chain = [
        {
            "step": "01",
            "title": "Pedido capturado",
            "detail": "Pedido comercial registrado y trazable.",
            "count": 1,
            "status": pedido.folio,
            "tone": "success",
            "url": reverse("crm:pedido_detail", kwargs={"pedido_id": pedido.id}),
            "cta": "Ver pedido",
            "owner": "CRM / Ventas",
            "next_step": "Confirmar prioridad y compromiso comercial del pedido.",
        },
        {
            "step": "02",
            "title": "Confirmación comercial",
            "detail": "Validación del pedido y siguiente compromiso.",
            "count": chain_count,
            "status": "Confirmado" if estatus_actual in {PedidoCliente.ESTATUS_CONFIRMADO, PedidoCliente.ESTATUS_EN_PRODUCCION, PedidoCliente.ESTATUS_ENTREGADO} else "Pendiente",
            "tone": "success" if estatus_actual in {PedidoCliente.ESTATUS_CONFIRMADO, PedidoCliente.ESTATUS_EN_PRODUCCION, PedidoCliente.ESTATUS_ENTREGADO} else "warning",
            "url": reverse("crm:pedido_detail", kwargs={"pedido_id": pedido.id}),
            "cta": "Ver seguimiento",
            "owner": "CRM / Seguimiento",
            "next_step": "Actualizar la bitácora con el avance o cambio de estatus.",
        },
        {
            "step": "03",
            "title": "Producción / surtido",
            "detail": "Pedido enviado a operación interna.",
            "count": 1 if estatus_actual == PedidoCliente.ESTATUS_EN_PRODUCCION else 0,
            "status": "En producción" if estatus_actual == PedidoCliente.ESTATUS_EN_PRODUCCION else "Sin liberar",
            "tone": "success" if estatus_actual == PedidoCliente.ESTATUS_EN_PRODUCCION else "warning",
            "url": reverse("crm:pedido_detail", kwargs={"pedido_id": pedido.id}),
            "cta": "Revisar estatus",
            "owner": "CRM / Operación",
            "next_step": "Alinear producción o entrega según el estado vigente del pedido.",
        },
        {
            "step": "04",
            "title": "Cierre documental",
            "detail": "Pedido entregado o cancelado con bitácora completa.",
            "count": 1 if estatus_actual in {PedidoCliente.ESTATUS_ENTREGADO, PedidoCliente.ESTATUS_CANCELADO} else 0,
            "status": pedido.get_estatus_display(),
            "tone": "success" if estatus_actual == PedidoCliente.ESTATUS_ENTREGADO else ("danger" if estatus_actual == PedidoCliente.ESTATUS_CANCELADO else "warning"),
            "url": reverse("crm:pedido_detail", kwargs={"pedido_id": pedido.id}),
            "cta": "Ver cierre",
            "owner": "CRM / Auditoría comercial",
            "next_step": "Dejar estatus final, evidencia y comentario terminal del pedido.",
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
    crm_maturity_summary = _crm_maturity_summary(
        chain=enterprise_chain,
        default_url=reverse("crm:pedido_detail", kwargs={"pedido_id": pedido.id}),
    )
    crm_handoff_map = _crm_handoff_map(
        clientes_gap=0 if pedido.cliente.activo else 1,
        pedidos_abiertos=1 if estatus_actual not in {PedidoCliente.ESTATUS_ENTREGADO, PedidoCliente.ESTATUS_CANCELADO} else 0,
        pedidos_hoy=1 if pedido.created_at.date() == timezone.localdate() else 0,
        delivered_gap=0 if estatus_actual in {PedidoCliente.ESTATUS_ENTREGADO, PedidoCliente.ESTATUS_CANCELADO} else 1,
        pedidos_url=reverse("crm:pedido_detail", kwargs={"pedido_id": pedido.id}),
    )
    document_stage_rows = [
        {
            "label": "Alta comercial",
            "open": 1,
            "closed": 0,
            "detail": "Pedido ya registrado en CRM.",
            "url": reverse("crm:pedido_detail", kwargs={"pedido_id": pedido.id}),
            "owner": "CRM / Ventas",
            "next_step": "Completar seguimiento comercial",
        },
        {
            "label": "Seguimientos",
            "open": chain_count,
            "closed": 0,
            "detail": "Eventos documentados del pedido.",
            "url": reverse("crm:pedido_detail", kwargs={"pedido_id": pedido.id}),
            "owner": "CRM / Seguimiento",
            "next_step": "Actualizar bitácora del pedido",
        },
        {
            "label": "Operación",
            "open": 1 if estatus_actual in {PedidoCliente.ESTATUS_NUEVO, PedidoCliente.ESTATUS_CONFIRMADO, PedidoCliente.ESTATUS_EN_PRODUCCION} else 0,
            "closed": 1 if estatus_actual in {PedidoCliente.ESTATUS_ENTREGADO, PedidoCliente.ESTATUS_CANCELADO} else 0,
            "detail": "Pedido activo frente a pedido cerrado.",
            "url": reverse("crm:pedido_detail", kwargs={"pedido_id": pedido.id}),
            "owner": "CRM / Operación",
            "next_step": "Cerrar entrega o cancelación documentada",
        },
    ]
    for row in document_stage_rows:
        total = int(row["open"] or 0) + int(row["closed"] or 0)
        row["completion"] = int(round((int(row["closed"] or 0) / total) * 100)) if total else 0
    release_gate_rows = _crm_release_gate_rows(
        clientes_total=1,
        clientes_activos=1 if pedido.cliente.activo else 0,
        pedidos_confirmados=1 if estatus_actual in {PedidoCliente.ESTATUS_CONFIRMADO, PedidoCliente.ESTATUS_EN_PRODUCCION, PedidoCliente.ESTATUS_ENTREGADO} else 0,
        pedidos_produccion=1 if estatus_actual == PedidoCliente.ESTATUS_EN_PRODUCCION else 0,
        pedidos_entregados=1 if estatus_actual == PedidoCliente.ESTATUS_ENTREGADO else 0,
        default_url=reverse("crm:pedido_detail", kwargs={"pedido_id": pedido.id}),
    )

    ctx = {
        "module_tabs": _module_tabs("pedidos"),
        "pedido": pedido,
        "seguimientos": pedido.seguimientos.select_related("created_by").all(),
        "estatus_choices": PedidoCliente.ESTATUS_CHOICES,
        "can_manage_crm": can_manage_crm(request.user),
        "enterprise_chain": enterprise_chain,
        "crm_maturity_summary": crm_maturity_summary,
        "crm_handoff_map": crm_handoff_map,
        "critical_path_rows": _crm_critical_path_rows(enterprise_chain),
        "document_stage_rows": document_stage_rows,
        "erp_governance_rows": _crm_governance_rows(document_stage_rows, owner_default="CRM / Operación"),
        "executive_radar_rows": _crm_executive_radar_rows(
            _crm_governance_rows(document_stage_rows, owner_default="CRM / Operación"),
            default_owner="CRM / Operación",
            fallback_url=reverse("crm:pedido_detail", kwargs={"pedido_id": pedido.id}),
        ),
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
        "operational_health_cards": _crm_operational_health_cards(
            focus="detalle",
            primary_open=chain_count,
            secondary_open=1 if estatus_actual in {PedidoCliente.ESTATUS_NUEVO, PedidoCliente.ESTATUS_CONFIRMADO, PedidoCliente.ESTATUS_EN_PRODUCCION} else 0,
            ready_count=1 if estatus_actual in {PedidoCliente.ESTATUS_ENTREGADO, PedidoCliente.ESTATUS_CANCELADO} else 0,
        ),
        "erp_command_center": _crm_command_center(
            governance_rows=_crm_governance_rows(document_stage_rows, owner_default="CRM / Operación"),
            maturity_summary=crm_maturity_summary,
            default_url=reverse("crm:pedido_detail", kwargs={"pedido_id": pedido.id}),
            default_cta="Abrir pedido",
        ),
    }
    return render(request, "crm/pedido_detail.html", ctx)
