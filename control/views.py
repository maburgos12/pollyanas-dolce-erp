from decimal import Decimal

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.dateparse import parse_date

from core.access import can_capture_piso, can_view_reportes
from core.models import Sucursal
from recetas.models import Receta

from .models import MermaPOS, VentaPOS
from .services import build_discrepancias_report, resolve_period_range


def _module_tabs(active: str) -> list[dict]:
    return [
        {"label": "Discrepancias", "url_name": "control:discrepancias", "active": active == "discrepancias"},
        {"label": "Captura móvil", "url_name": "control:captura_movil", "active": active == "captura_movil"},
    ]


def _control_enterprise_chain(
    *,
    discrepancias_alerta: int,
    discrepancias_observar: int,
    discrepancias_ok: int,
    ventas_hoy: int,
    mermas_hoy: int,
    capturas_recientes: int,
) -> list[dict]:
    chain = [
        {
            "step": "01",
            "title": "Captura operativa",
            "detail": "Registros móviles de venta y merma desde piso.",
            "count": capturas_recientes,
            "status": "Con registros recientes" if capturas_recientes else "Sin captura reciente",
            "tone": "success" if capturas_recientes else "warning",
            "url": reverse("control:captura_movil"),
            "cta": "Abrir captura",
            "owner": "Control / Piso",
            "next_step": "Mantener ventas y mermas del día documentadas desde sucursal.",
        },
        {
            "step": "02",
            "title": "Ventas del día",
            "detail": "Eventos POS capturados para conciliación operativa.",
            "count": ventas_hoy,
            "status": "Con ventas" if ventas_hoy else "Sin ventas registradas",
            "tone": "success" if ventas_hoy else "warning",
            "url": reverse("control:captura_movil"),
            "cta": "Ver ventas",
            "owner": "Control / Ventas",
            "next_step": "Consolidar ventas y validar que estén listas para conciliación.",
        },
        {
            "step": "03",
            "title": "Mermas del día",
            "detail": "Mermas documentadas para control y trazabilidad.",
            "count": mermas_hoy,
            "status": "Sin merma" if mermas_hoy == 0 else f"{mermas_hoy} registradas",
            "tone": "success" if mermas_hoy == 0 else "warning",
            "url": reverse("control:captura_movil"),
            "cta": "Ver mermas",
            "owner": "Control / Auditoría de piso",
            "next_step": "Documentar causas y dejar trazabilidad de desperdicio real.",
        },
        {
            "step": "04",
            "title": "Discrepancias",
            "detail": "Semáforo operativo de diferencias contra inventario real.",
            "count": discrepancias_alerta,
            "status": "Sin alertas" if discrepancias_alerta == 0 else f"{discrepancias_alerta} alertas",
            "tone": "success" if discrepancias_alerta == 0 else "danger",
            "url": reverse("control:discrepancias"),
            "cta": "Revisar discrepancias",
            "owner": "Control / Investigación",
            "next_step": "Resolver diferencias contra inventario y cerrar investigación operativa.",
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


def _control_document_stage_rows(
    *,
    discrepancias_alerta: int,
    discrepancias_observar: int,
    discrepancias_ok: int,
    ventas_hoy: int,
    mermas_hoy: int,
    capturas_recientes: int,
) -> list[dict]:
    rows = [
        {
            "label": "Capturas recientes",
            "open": capturas_recientes,
            "closed": 0,
            "detail": "Eventos recientes documentados desde piso.",
            "url": reverse("control:captura_movil"),
            "owner": "Control / Piso",
            "next_step": "Completar captura diaria de venta y merma por sucursal.",
        },
        {
            "label": "Ventas registradas",
            "open": ventas_hoy,
            "closed": mermas_hoy,
            "detail": "Ventas capturadas frente a merma del periodo.",
            "url": reverse("control:captura_movil"),
            "owner": "Control / Ventas",
            "next_step": "Consolidar ventas y validar captura contra operación real.",
        },
        {
            "label": "Alertas operativas",
            "open": discrepancias_alerta,
            "closed": discrepancias_ok,
            "detail": "Alertas abiertas frente a insumos en control.",
            "url": reverse("control:discrepancias"),
            "owner": "Control / Investigación",
            "next_step": "Resolver alertas rojas antes del cierre operativo.",
        },
        {
            "label": "Observación preventiva",
            "open": discrepancias_observar,
            "closed": discrepancias_ok,
            "detail": "Casos para revisar frente a casos ya en verde.",
            "url": reverse("control:discrepancias"),
            "owner": "Control / Auditoría",
            "next_step": "Revisar observaciones y convertirlas en cierre o alerta.",
        },
    ]
    for row in rows:
        total = int(row["open"]) + int(row["closed"])
        row["completion"] = int(round((int(row["closed"]) / total) * 100)) if total else 100
    return rows


def _control_operational_health_cards(
    *,
    alertas: int,
    observar: int,
    ok: int,
    ventas_hoy: int,
    mermas_hoy: int,
    capturas_recientes: int,
) -> list[dict[str, object]]:
    return [
        {
            "label": "Capturas recientes",
            "value": capturas_recientes,
            "tone": "success" if capturas_recientes else "warning",
            "detail": "Eventos recientes registrados desde piso para control operativo.",
        },
        {
            "label": "Alertas abiertas",
            "value": alertas,
            "tone": "danger" if alertas else "success",
            "detail": "Discrepancias en rojo que requieren acción inmediata.",
        },
        {
            "label": "Ventas del día",
            "value": ventas_hoy,
            "tone": "primary",
            "detail": "Capturas de venta listas para conciliación del periodo.",
        },
        {
            "label": "Mermas documentadas",
            "value": mermas_hoy,
            "tone": "warning" if mermas_hoy else "success",
            "detail": "Mermas del día registradas con causa y trazabilidad.",
        },
        {
            "label": "En observación",
            "value": observar,
            "tone": "warning" if observar else "success",
            "detail": "Casos preventivos que aún no suben a alerta roja.",
        },
        {
            "label": "Verdes operativos",
            "value": ok,
            "tone": "success",
            "detail": "Insumos sin diferencia relevante para este corte.",
        },
    ]


def _control_governance_rows(
    rows: list[dict],
    owner_default: str = "Control / Operación",
) -> list[dict]:
    governance_rows: list[dict] = []
    for row in rows:
        governance_rows.append(
            {
                "front": row.get("label", "Control"),
                "owner": row.get("owner") or owner_default,
                "blockers": int(row.get("open") or 0),
                "completion": int(row.get("completion") or 0),
                "detail": row.get("detail", ""),
                "next_step": row.get("next_step") or "Revisar frente operativo",
                "url": row.get("url") or reverse("control:discrepancias"),
                "cta": row.get("cta") or "Abrir",
            }
        )
    return governance_rows


def _control_executive_radar_rows(
    governance_rows: list[dict[str, object]],
    *,
    default_owner: str = "Control / Operación",
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
            dominant_blocker = row.get("detail", "") or "Brecha de control en seguimiento"
        else:
            tone = "danger"
            status = "Con bloqueo"
            dominant_blocker = row.get("detail", "") or "Bloqueo de control abierto"
        rows.append(
            {
                "phase": row.get("front", "Frente de control"),
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


def _control_command_center(
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
        "owner": governance_rows[0].get("owner", "Control / Operación") if governance_rows else "Control / Operación",
        "status": status,
        "tone": tone,
        "blockers": blockers,
        "next_step": maturity_summary.get("next_priority_detail", "Sin acciones pendientes."),
        "url": maturity_summary.get("next_priority_url", default_url),
        "cta": maturity_summary.get("next_priority_cta", default_cta),
    }


def _control_maturity_summary(*, chain: list[dict], default_url: str) -> dict:
    total_steps = len(chain)
    completed_steps = sum(1 for item in chain if item.get("tone") == "success")
    attention_steps = max(total_steps - completed_steps, 0)
    coverage_pct = int(round((completed_steps / total_steps) * 100)) if total_steps else 0
    next_priority = next((item for item in chain if item.get("tone") != "success"), None)
    return {
        "completed_steps": completed_steps,
        "attention_steps": attention_steps,
        "coverage_pct": coverage_pct,
        "next_priority_title": next_priority.get("title", "Cadena de control estabilizada") if next_priority else "Cadena de control estabilizada",
        "next_priority_detail": next_priority.get("detail", "Sin brechas abiertas en captura, merma ni discrepancias.") if next_priority else "Sin brechas abiertas en captura, merma ni discrepancias.",
        "next_priority_url": next_priority.get("url", default_url) if next_priority else default_url,
        "next_priority_cta": next_priority.get("cta", "Abrir control") if next_priority else "Abrir control",
    }


def _control_handoff_map(
    *,
    capturas_recientes: int,
    ventas_hoy: int,
    mermas_hoy: int,
    discrepancias_alerta: int,
) -> list[dict]:
    return [
        {
            "label": "Captura -> Ventas",
            "detail": "La captura móvil debe alimentar ventas reales antes de cerrar el día.",
            "count": max(capturas_recientes - ventas_hoy, 0),
            "tone": "success" if ventas_hoy else "warning",
            "status": "Controlado" if ventas_hoy else "Sin ventas registradas",
            "url": reverse("control:captura_movil"),
            "cta": "Abrir captura",
            "owner": "Control / Piso",
            "depends_on": "Captura diaria registrada",
            "exit_criteria": "La venta del día debe quedar capturada y lista para conciliación.",
            "next_step": "Completar captura de piso y consolidar ventas reales.",
            "completion": 100 if ventas_hoy else 60,
        },
        {
            "label": "Ventas -> Merma",
            "detail": "La operación diaria debe reflejar también desperdicio documentado cuando exista.",
            "count": mermas_hoy,
            "tone": "success" if mermas_hoy == 0 else "warning",
            "status": "Sin merma" if mermas_hoy == 0 else "Merma documentada",
            "url": reverse("control:captura_movil"),
            "cta": "Ver mermas",
            "owner": "Control / Auditoría de piso",
            "depends_on": "Ventas consolidadas del periodo",
            "exit_criteria": "Toda merma relevante debe quedar trazada con causa operativa.",
            "next_step": "Registrar merma y justificar desperdicio del día.",
            "completion": 100 if mermas_hoy == 0 else 75,
        },
        {
            "label": "Merma -> Discrepancias",
            "detail": "Las diferencias operativas deben cerrarse contra inventario y captura real.",
            "count": discrepancias_alerta,
            "tone": "success" if discrepancias_alerta == 0 else "danger",
            "status": "Controlado" if discrepancias_alerta == 0 else "Alertas abiertas",
            "url": reverse("control:discrepancias"),
            "cta": "Revisar discrepancias",
            "owner": "Control / Investigación",
            "depends_on": "Venta y merma documentadas",
            "exit_criteria": "Las discrepancias críticas deben quedar cerradas contra inventario real.",
            "next_step": "Resolver alertas y documentar investigación operativa.",
            "completion": 100 if discrepancias_alerta == 0 else 30,
        },
    ]


def _control_critical_path_rows(chain: list[dict[str, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, item in enumerate(chain, start=1):
        tone = item.get("tone") or "warning"
        rows.append(
            {
                "priority": f"{index:02d}",
                "title": item.get("title", f"Tramo {index}"),
                "owner": item.get("owner", "Control / Operación"),
                "blockers": 0 if tone == "success" else int(item.get("count") or 0),
                "completion": int(item.get("completion") or 0),
                "depends_on": item.get("depends_on", "Origen del módulo"),
                "detail": item.get("detail", ""),
                "next_step": item.get("next_step", "Revisar tramo"),
                "url": item.get("url", reverse("control:discrepancias")),
                "cta": item.get("cta", "Abrir"),
            }
        )
    return rows


def _control_release_gate_rows(
    *,
    capturas_recientes: int,
    ventas_hoy: int,
    mermas_hoy: int,
    discrepancias_alerta: int,
    discrepancias_observar: int,
    discrepancias_ok: int,
    base_url: str,
) -> list[dict[str, object]]:
    total_capturas = max(capturas_recientes, 1)
    total_control = max(ventas_hoy + mermas_hoy, 1)
    total_cierre = max(discrepancias_alerta + discrepancias_observar + discrepancias_ok, 1)
    capturas_controladas = ventas_hoy + mermas_hoy
    discrepancias_resueltas = discrepancias_ok
    return [
        {
            "step": "01",
            "title": "Captura de piso liberada",
            "detail": "Eventos documentados para alimentar control operativo del día.",
            "completed": capturas_controladas,
            "open_count": max(total_capturas - capturas_controladas, 0),
            "total": total_capturas,
            "tone": "success" if capturas_controladas else "warning",
            "url": reverse("control:captura_movil"),
            "cta": "Abrir captura",
        },
        {
            "step": "02",
            "title": "Ventas y mermas conciliadas",
            "detail": "Registros diarios listos para contrastar contra inventario y operación.",
            "completed": capturas_controladas,
            "open_count": max(total_control - capturas_controladas, 0),
            "total": total_control,
            "tone": "success" if total_control and capturas_controladas >= total_control else "warning",
            "url": reverse("control:captura_movil"),
            "cta": "Revisar registros",
        },
        {
            "step": "03",
            "title": "Discrepancias cerradas",
            "detail": "Semáforo operativo resuelto con trazabilidad sobre diferencias reales.",
            "completed": discrepancias_resueltas,
            "open_count": max(total_cierre - discrepancias_resueltas, 0),
            "total": total_cierre,
            "tone": "success" if discrepancias_alerta == 0 and discrepancias_observar == 0 else "danger",
            "url": base_url,
            "cta": "Cerrar discrepancias",
        },
    ]


def _control_focus_cards(
    *,
    selected_focus: str,
    alertas: int,
    observar: int,
    ok: int,
    ventas_hoy: int,
    mermas_hoy: int,
    capturas_recientes: int,
) -> list[dict[str, object]]:
    focus_definitions = [
        {
            "key": "HOY",
            "label": "Corte del día",
            "value": ventas_hoy + mermas_hoy,
            "tone": "primary",
            "detail": "Ventas y mermas registradas hoy para conciliación del cierre.",
        },
        {
            "key": "CAPTURAS",
            "label": "Captura reciente",
            "value": capturas_recientes,
            "tone": "success" if capturas_recientes else "warning",
            "detail": "Eventos de piso ya documentados en el módulo operativo.",
        },
        {
            "key": "ALERTAS",
            "label": "Alertas abiertas",
            "value": alertas,
            "tone": "danger" if alertas else "success",
            "detail": "Discrepancias rojas que requieren validación inmediata.",
        },
        {
            "key": "OBSERVACION",
            "label": "Observación preventiva",
            "value": observar,
            "tone": "warning" if observar else "success",
            "detail": "Casos preventivos que aún no escalan a alerta crítica.",
        },
        {
            "key": "VERDES",
            "label": "Controlados",
            "value": ok,
            "tone": "success",
            "detail": "Insumos sin diferencia relevante en este corte.",
        },
    ]
    base_url = reverse("control:discrepancias")
    cards: list[dict[str, object]] = []
    for item in focus_definitions:
        cards.append(
            {
                **item,
                "active": selected_focus == item["key"],
                "url": f"{base_url}?enterprise_focus={item['key']}",
            }
        )
    return cards


def _control_focus_summary(*, selected_focus: str, count: int) -> dict | None:
    focus_copy = {
        "HOY": {
            "title": "Corte del día",
            "detail": "Vista concentrada en ventas y mermas del día actual.",
        },
        "CAPTURAS": {
            "title": "Captura reciente",
            "detail": "Eventos recientes capturados desde piso para control operativo.",
        },
        "ALERTAS": {
            "title": "Alertas abiertas",
            "detail": "Discrepancias críticas que requieren acción inmediata.",
        },
        "OBSERVACION": {
            "title": "Observación preventiva",
            "detail": "Casos en vigilancia antes de escalar a rojo.",
        },
        "VERDES": {
            "title": "Controlados",
            "detail": "Insumos ya estabilizados en el corte actual.",
        },
    }
    if not selected_focus or selected_focus not in focus_copy:
        return None
    summary = focus_copy[selected_focus].copy()
    summary["count"] = count
    summary["clear_url"] = reverse("control:discrepancias")
    return summary


@login_required
def discrepancias(request):
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para revisar discrepancias.")

    period_raw = (request.GET.get("periodo") or "").strip()
    date_from_raw = (request.GET.get("from") or "").strip()
    date_to_raw = (request.GET.get("to") or "").strip()
    sucursal_id_raw = (request.GET.get("sucursal_id") or "").strip()
    threshold_raw = (request.GET.get("threshold_pct") or "10").strip()

    date_from, date_to, period_resolved = resolve_period_range(
        period_raw=period_raw,
        date_from_raw=date_from_raw,
        date_to_raw=date_to_raw,
    )

    try:
        threshold_pct = Decimal(threshold_raw)
    except Exception:
        threshold_pct = Decimal("10")
    if threshold_pct < 0:
        threshold_pct = Decimal("0")

    sucursal_id = int(sucursal_id_raw) if sucursal_id_raw.isdigit() else None
    report = build_discrepancias_report(
        date_from=date_from,
        date_to=date_to,
        sucursal_id=sucursal_id,
        threshold_pct=threshold_pct,
    )

    ventas_hoy = VentaPOS.objects.filter(fecha=timezone.localdate()).count()
    mermas_hoy = MermaPOS.objects.filter(fecha=timezone.localdate()).count()
    capturas_recientes = VentaPOS.objects.count() + MermaPOS.objects.count()

    enterprise_chain = _control_enterprise_chain(
        discrepancias_alerta=report["totals"]["alertas"],
        discrepancias_observar=report["totals"]["observar"],
        discrepancias_ok=report["totals"]["ok"],
        ventas_hoy=ventas_hoy,
        mermas_hoy=mermas_hoy,
        capturas_recientes=capturas_recientes,
    )
    document_stage_rows = _control_document_stage_rows(
        discrepancias_alerta=report["totals"]["alertas"],
        discrepancias_observar=report["totals"]["observar"],
        discrepancias_ok=report["totals"]["ok"],
        ventas_hoy=ventas_hoy,
        mermas_hoy=mermas_hoy,
        capturas_recientes=capturas_recientes,
    )
    operational_health_cards = _control_operational_health_cards(
        alertas=report["totals"]["alertas"],
        observar=report["totals"]["observar"],
        ok=report["totals"]["ok"],
        ventas_hoy=ventas_hoy,
        mermas_hoy=mermas_hoy,
        capturas_recientes=capturas_recientes,
    )
    maturity_summary = _control_maturity_summary(
        chain=enterprise_chain,
        default_url=reverse("control:discrepancias"),
    )
    handoff_map = _control_handoff_map(
        capturas_recientes=capturas_recientes,
        ventas_hoy=ventas_hoy,
        mermas_hoy=mermas_hoy,
        discrepancias_alerta=report["totals"]["alertas"],
    )
    release_gate_rows = _control_release_gate_rows(
        capturas_recientes=capturas_recientes,
        ventas_hoy=ventas_hoy,
        mermas_hoy=mermas_hoy,
        discrepancias_alerta=report["totals"]["alertas"],
        discrepancias_observar=report["totals"]["observar"],
        discrepancias_ok=report["totals"]["ok"],
        base_url=reverse("control:discrepancias"),
    )
    enterprise_focus = (request.GET.get("enterprise_focus") or "").strip().upper()
    if enterprise_focus == "ALERTAS":
        report["rows"] = [row for row in report["rows"] if row["semaforo"] == "ROJO"]
    elif enterprise_focus == "OBSERVACION":
        report["rows"] = [row for row in report["rows"] if row["semaforo"] == "AMARILLO"]
    elif enterprise_focus == "VERDES":
        report["rows"] = [row for row in report["rows"] if row["semaforo"] == "VERDE"]
    elif enterprise_focus == "HOY":
        report["rows"] = [row for row in report["rows"] if row["ventas_pos"] or row["mermas_pos"]]
    elif enterprise_focus == "CAPTURAS":
        report["rows"] = [
            row for row in report["rows"] if row["ventas_pos"] or row["mermas_pos"] or row["produccion"]
        ]
    focus_cards = _control_focus_cards(
        selected_focus=enterprise_focus,
        alertas=report["totals"]["alertas"],
        observar=report["totals"]["observar"],
        ok=report["totals"]["ok"],
        ventas_hoy=ventas_hoy,
        mermas_hoy=mermas_hoy,
        capturas_recientes=capturas_recientes,
    )
    focus_summary = _control_focus_summary(
        selected_focus=enterprise_focus,
        count=len(report["rows"]),
    )
    governance_rows = _control_governance_rows(document_stage_rows)

    context = {
        "module_tabs": _module_tabs("discrepancias"),
        "periodo": period_resolved,
        "date_from": date_from,
        "date_to": date_to,
        "threshold_pct": threshold_pct,
        "sucursal_id": sucursal_id,
        "sucursales": list(Sucursal.objects.filter(activa=True).order_by("codigo")),
        "report": report,
        "enterprise_chain": enterprise_chain,
        "document_stage_rows": document_stage_rows,
        "erp_governance_rows": governance_rows,
        "executive_radar_rows": _control_executive_radar_rows(
            governance_rows,
            default_owner="Control / Lectura",
            fallback_url=reverse("control:discrepancias"),
        ),
        "erp_command_center": _control_command_center(
            governance_rows=governance_rows,
            maturity_summary=maturity_summary,
            default_url=reverse("control:discrepancias"),
            default_cta="Abrir control",
        ),
        "operational_health_cards": operational_health_cards,
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
        "enterprise_focus": enterprise_focus,
        "focus_cards": focus_cards,
        "focus_summary": focus_summary,
    }
    context["critical_path_rows"] = _control_critical_path_rows(context["enterprise_chain"])
    return render(request, "control/discrepancias.html", context)


def _parse_decimal(value: str, default: Decimal = Decimal("0")) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except Exception:
        return default
    return parsed


@login_required
def captura_movil(request):
    if not can_capture_piso(request.user):
        raise PermissionDenied("No tienes permisos para captura en piso.")

    if request.method == "POST":
        capture_type = (request.POST.get("capture_type") or "").strip().lower()
        receta_id = (request.POST.get("receta_id") or "").strip()
        sucursal_id = (request.POST.get("sucursal_id") or "").strip()
        fecha_raw = (request.POST.get("fecha") or "").strip()
        cantidad_raw = (request.POST.get("cantidad") or "").strip()
        producto_texto = (request.POST.get("producto_texto") or "").strip()
        codigo_point = (request.POST.get("codigo_point") or "").strip()

        receta = Receta.objects.filter(id=receta_id).first() if receta_id.isdigit() else None
        sucursal = Sucursal.objects.filter(id=sucursal_id, activa=True).first() if sucursal_id.isdigit() else None
        fecha = parse_date(fecha_raw) if fecha_raw else timezone.localdate()
        cantidad = _parse_decimal(cantidad_raw)

        if fecha is None:
            messages.error(request, "Fecha inválida. Usa formato YYYY-MM-DD.")
            return redirect("control:captura_movil")
        if cantidad <= 0:
            messages.error(request, "La cantidad debe ser mayor a cero.")
            return redirect("control:captura_movil")

        if receta and not codigo_point:
            codigo_point = (receta.codigo_point or "").strip()
        if receta and not producto_texto:
            producto_texto = receta.nombre

        if not receta and not producto_texto and not codigo_point:
            messages.error(request, "Selecciona receta o captura producto/código para guardar el registro.")
            return redirect("control:captura_movil")

        if capture_type == "venta":
            tickets_raw = (request.POST.get("tickets") or "0").strip()
            monto_raw = (request.POST.get("monto_total") or "0").strip()
            tickets = int(tickets_raw) if tickets_raw.isdigit() else 0
            monto_total = _parse_decimal(monto_raw)
            VentaPOS.objects.create(
                receta=receta,
                sucursal=sucursal,
                fecha=fecha,
                codigo_point=codigo_point,
                producto_texto=producto_texto,
                cantidad=cantidad,
                tickets=tickets,
                monto_total=monto_total,
                fuente="CAPTURA_MOVIL",
            )
            messages.success(request, "Venta capturada correctamente.")
            return redirect("control:captura_movil")

        if capture_type == "merma":
            motivo = (request.POST.get("motivo") or "").strip()
            MermaPOS.objects.create(
                receta=receta,
                sucursal=sucursal,
                fecha=fecha,
                codigo_point=codigo_point,
                producto_texto=producto_texto,
                cantidad=cantidad,
                motivo=motivo,
                fuente="CAPTURA_MOVIL",
            )
            messages.success(request, "Merma capturada correctamente.")
            return redirect("control:captura_movil")

        messages.error(request, "Tipo de captura no reconocido.")
        return redirect("control:captura_movil")

    ventas = list(
        VentaPOS.objects.select_related("receta", "sucursal")
        .order_by("-fecha", "-creado_en", "-id")[:30]
    )
    mermas = list(
        MermaPOS.objects.select_related("receta", "sucursal")
        .order_by("-fecha", "-creado_en", "-id")[:30]
    )
    recientes = sorted(
        [
            {
                "tipo": "VENTA",
                "fecha": row.fecha,
                "sucursal": row.sucursal,
                "producto": row.receta.nombre if row.receta_id else (row.producto_texto or row.codigo_point),
                "cantidad": row.cantidad,
                "extra": f"Tickets: {row.tickets} · Monto: ${row.monto_total or 0}",
                "fuente": row.fuente,
                "creado_en": row.creado_en,
            }
            for row in ventas
        ]
        + [
            {
                "tipo": "MERMA",
                "fecha": row.fecha,
                "sucursal": row.sucursal,
                "producto": row.receta.nombre if row.receta_id else (row.producto_texto or row.codigo_point),
                "cantidad": row.cantidad,
                "extra": row.motivo or "-",
                "fuente": row.fuente,
                "creado_en": row.creado_en,
            }
            for row in mermas
        ],
        key=lambda item: (item["fecha"], item["creado_en"]),
        reverse=True,
    )[:25]

    ventas_hoy = VentaPOS.objects.filter(fecha=timezone.localdate()).count()
    mermas_hoy = MermaPOS.objects.filter(fecha=timezone.localdate()).count()
    report = build_discrepancias_report(
        date_from=timezone.localdate(),
        date_to=timezone.localdate(),
        sucursal_id=None,
        threshold_pct=Decimal("10"),
    )
    enterprise_chain = _control_enterprise_chain(
        discrepancias_alerta=report["totals"]["alertas"],
        discrepancias_observar=report["totals"]["observar"],
        discrepancias_ok=report["totals"]["ok"],
        ventas_hoy=ventas_hoy,
        mermas_hoy=mermas_hoy,
        capturas_recientes=len(recientes),
    )
    document_stage_rows = _control_document_stage_rows(
        discrepancias_alerta=report["totals"]["alertas"],
        discrepancias_observar=report["totals"]["observar"],
        discrepancias_ok=report["totals"]["ok"],
        ventas_hoy=ventas_hoy,
        mermas_hoy=mermas_hoy,
        capturas_recientes=len(recientes),
    )
    operational_health_cards = _control_operational_health_cards(
        alertas=report["totals"]["alertas"],
        observar=report["totals"]["observar"],
        ok=report["totals"]["ok"],
        ventas_hoy=ventas_hoy,
        mermas_hoy=mermas_hoy,
        capturas_recientes=len(recientes),
    )
    maturity_summary = _control_maturity_summary(
        chain=enterprise_chain,
        default_url=reverse("control:captura_movil"),
    )
    handoff_map = _control_handoff_map(
        capturas_recientes=len(recientes),
        ventas_hoy=ventas_hoy,
        mermas_hoy=mermas_hoy,
        discrepancias_alerta=report["totals"]["alertas"],
    )
    release_gate_rows = _control_release_gate_rows(
        capturas_recientes=len(recientes),
        ventas_hoy=ventas_hoy,
        mermas_hoy=mermas_hoy,
        discrepancias_alerta=report["totals"]["alertas"],
        discrepancias_observar=report["totals"]["observar"],
        discrepancias_ok=report["totals"]["ok"],
        base_url=reverse("control:captura_movil"),
    )
    governance_rows = _control_governance_rows(document_stage_rows)

    context = {
        "module_tabs": _module_tabs("captura_movil"),
        "sucursales": list(Sucursal.objects.filter(activa=True).order_by("codigo")),
        "recetas": list(Receta.objects.order_by("nombre").only("id", "nombre", "codigo_point")[:500]),
        "hoy": timezone.localdate().isoformat(),
        "recientes": recientes,
        "enterprise_chain": enterprise_chain,
        "document_stage_rows": document_stage_rows,
        "erp_governance_rows": governance_rows,
        "executive_radar_rows": _control_executive_radar_rows(
            governance_rows,
            default_owner="Control / Captura",
            fallback_url=reverse("control:captura_movil"),
        ),
        "erp_command_center": _control_command_center(
            governance_rows=governance_rows,
            maturity_summary=maturity_summary,
            default_url=reverse("control:captura_movil"),
            default_cta="Abrir captura",
        ),
        "operational_health_cards": operational_health_cards,
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
    }
    context["critical_path_rows"] = _control_critical_path_rows(context["enterprise_chain"])
    return render(request, "control/captura_movil.html", context)
