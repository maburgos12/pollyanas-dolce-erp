from __future__ import annotations

from datetime import date as dt_date
from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.db.models import Count, Q
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone

from core.access import can_manage_rrhh, can_view_rrhh
from core.audit import log_event

from .models import Empleado, NominaLinea, NominaPeriodo


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


def _module_tabs(active: str) -> list[dict]:
    return [
        {"label": "Empleados", "url_name": "rrhh:empleados", "active": active == "empleados"},
        {"label": "Nómina", "url_name": "rrhh:nomina", "active": active == "nomina"},
    ]


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

        nombre = (request.POST.get("nombre") or "").strip()
        if not nombre:
            messages.error(request, "Nombre del empleado es obligatorio.")
        else:
            empleado = Empleado.objects.create(
                nombre=nombre,
                area=(request.POST.get("area") or "").strip(),
                puesto=(request.POST.get("puesto") or "").strip(),
                tipo_contrato=(request.POST.get("tipo_contrato") or Empleado.CONTRATO_FIJO).strip(),
                fecha_ingreso=request.POST.get("fecha_ingreso") or timezone.localdate(),
                salario_diario=_parse_decimal(request.POST.get("salario_diario")),
                telefono=(request.POST.get("telefono") or "").strip(),
                email=(request.POST.get("email") or "").strip(),
                sucursal=(request.POST.get("sucursal") or "").strip(),
            )
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

    qs = Empleado.objects.all().annotate(total_lineas_nomina=Count("lineas_nomina"))
    if q:
        qs = qs.filter(
            Q(nombre__icontains=q)
            | Q(codigo__icontains=q)
            | Q(area__icontains=q)
            | Q(puesto__icontains=q)
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

    context = {
        "module_tabs": _module_tabs("empleados"),
        "can_manage_rrhh": can_manage_rrhh(request.user),
        "empleados": qs.order_by("nombre")[:600],
        "q": q,
        "estado": estado,
        "enterprise_focus": enterprise_focus,
        "total_empleados": empleados_total,
        "total_activos": empleados_activos,
        "total_nominas": nominas_total,
        "contrato_choices": Empleado.CONTRATO_CHOICES,
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
        "module_tabs": _module_tabs("nomina"),
        "can_manage_rrhh": can_manage_rrhh(request.user),
        "nominas": nominas_qs.order_by("-fecha_fin", "-id")[:120],
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
        "module_tabs": _module_tabs("nomina"),
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
