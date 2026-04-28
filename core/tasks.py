from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

from celery import shared_task
from django.conf import settings
from django.core.management import call_command
from django.core.mail import send_mail
from django.db.models import Count, Sum
from django.utils import timezone

from control.models import MermaMensualSucursal
from inventario.models import ConsumoInsumoMensual
from pos_bridge.models import PointProductionLine
from proyecciones.models import ProyeccionProduccion
from recetas.models import VentaHistorica
from reportes.models import EmpresaResultadoMensual, PresupuestoResumenMensual
from reportes.services_budget_vs_actual import BUDGET_VS_ACTUAL_SOURCE

logger = logging.getLogger(__name__)


@dataclass
class CloseStepResult:
    name: str
    ok: bool
    output: str
    error: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "ok": self.ok,
            "output": self.output[-4000:],
            "error": self.error,
        }


def _month_start(value: date) -> date:
    return value.replace(day=1)


def _previous_month(value: date | None = None) -> date:
    current = _month_start(value or timezone.localdate())
    return (current - timedelta(days=1)).replace(day=1)


def _next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _month_end(value: date) -> date:
    return _next_month(value) - timedelta(days=1)


def _first_week_date_of_current_month(value: date | None = None) -> date:
    return _month_start(value or timezone.localdate())


def _period(value: date) -> str:
    return f"{value:%Y-%m}"


def _director_email() -> str:
    return (
        getattr(settings, "DIRECTOR_EMAIL", "")
        or getattr(settings, "DEFAULT_FROM_EMAIL", "")
        or getattr(settings, "EMAIL_HOST_USER", "")
    )


def _from_email() -> str:
    return getattr(settings, "DEFAULT_FROM_EMAIL", "") or getattr(settings, "EMAIL_HOST_USER", "")


def _run_command_step(name: str, command: str, *args, **kwargs) -> CloseStepResult:
    stdout = io.StringIO()
    stderr = io.StringIO()
    logger.info("Inicio cierre mensual paso=%s command=%s kwargs=%s", name, command, kwargs)
    try:
        call_command(command, *args, stdout=stdout, stderr=stderr, **kwargs)
    except Exception as exc:  # noqa: BLE001 - el cierre debe continuar con los pasos restantes
        output = stdout.getvalue()
        error_output = stderr.getvalue()
        logger.exception("Error cierre mensual paso=%s command=%s", name, command)
        return CloseStepResult(name=name, ok=False, output=output, error=f"{exc}\n{error_output}".strip())
    output = stdout.getvalue()
    logger.info("Fin cierre mensual paso=%s command=%s", name, command)
    return CloseStepResult(name=name, ok=True, output=output)


def _send_email(subject: str, body: str) -> bool:
    recipient = _director_email()
    if not recipient:
        logger.warning("No se envio correo de cierre mensual: DIRECTOR_EMAIL/DEFAULT_FROM_EMAIL vacio.")
        return False
    send_mail(
        subject=subject,
        message=body,
        from_email=_from_email() or recipient,
        recipient_list=[recipient],
        fail_silently=False,
    )
    return True


def _monthly_closure_summary(month: date, steps: list[CloseStepResult]) -> dict[str, object]:
    result = EmpresaResultadoMensual.objects.filter(periodo=month).first()
    waste_cost = (
        MermaMensualSucursal.objects.filter(periodo=month).aggregate(total=Sum("costo_merma")).get("total")
        or Decimal("0")
    )
    shortage_count = ConsumoInsumoMensual.objects.filter(
        periodo=month,
        alerta=ConsumoInsumoMensual.ALERTA_FALTANTE,
    ).count()
    budget_snapshot = PresupuestoResumenMensual.objects.filter(
        period=month,
        tipo=PresupuestoResumenMensual.TIPO_FUENTE,
        fuente_nombre=BUDGET_VS_ACTUAL_SOURCE,
    ).first()
    vs_budget_pct = ""
    if budget_snapshot and budget_snapshot.total_budget:
        vs_budget_pct = str(
            (
                Decimal(str(budget_snapshot.total_actual or 0))
                / Decimal(str(budget_snapshot.total_budget or 0))
                * Decimal("100")
            ).quantize(Decimal("0.01"))
        )
    projection_start = _first_week_date_of_current_month()
    projection_end = projection_start + timedelta(days=6)
    projections_count = ProyeccionProduccion.objects.filter(
        periodo__gte=projection_start,
        periodo__lte=projection_end,
    ).count()
    errors = [step for step in steps if not step.ok]
    return {
        "period": _period(month),
        "ventas_totales": str(getattr(result, "venta_total", Decimal("0")) if result else Decimal("0")),
        "utilidad_operativa": str(getattr(result, "utilidad_operativa_total", Decimal("0")) if result else Decimal("0")),
        "vs_presupuesto_pct": vs_budget_pct,
        "costo_merma_total": str(waste_cost),
        "alertas_insumos_faltante": shortage_count,
        "proyecciones_generadas": projections_count,
        "errores": [step.as_dict() for step in errors],
    }


def _summary_email_body(month: date, steps: list[CloseStepResult], summary: dict[str, object]) -> str:
    lines = [
        f"Cierre automático {_period(month)} — Pollyana's Dolce",
        "",
        f"- Ventas totales del mes: ${summary['ventas_totales']}",
        f"- Utilidad operativa: ${summary['utilidad_operativa']}",
        f"- % vs presupuesto: {summary['vs_presupuesto_pct'] or 'N/D'}",
        f"- Costo merma total: ${summary['costo_merma_total']}",
        f"- Alertas de insumos FALTANTE: {summary['alertas_insumos_faltante']}",
        f"- Proyecciones generadas para la semana: {summary['proyecciones_generadas']}",
        "",
        "Pasos ejecutados:",
    ]
    for step in steps:
        lines.append(f"- {'OK' if step.ok else 'ERROR'} · {step.name}")
        if step.error:
            lines.append(f"  Error: {step.error[:800]}")
    return "\n".join(lines)


@shared_task(name="core.tasks.cerrar_mes_anterior")
def cerrar_mes_anterior():
    month = _previous_month()
    period = _period(month)
    current_week = _first_week_date_of_current_month()
    steps = [
        _run_command_step("snapshot_operating_finance", "snapshot_operating_finance", period=period),
        _run_command_step("snapshot_budget_vs_actual", "snapshot_budget_vs_actual", period=period),
        _run_command_step("consolidar_mermas", "consolidar_mermas", period=period),
        _run_command_step("clasificar_devoluciones", "clasificar_devoluciones", period=period),
        _run_command_step("generar_consumos_bom", "generar_consumos_bom", period=period),
        _run_command_step("calcular_consumo_insumos", "calcular_consumo_insumos", period=period),
        _run_command_step("generar_proyeccion", "generar_proyeccion", semana=current_week.isoformat()),
    ]
    summary = _monthly_closure_summary(month, steps)
    subject = f"Cierre automático {month:%B %Y} — Pollyana's Dolce"
    body = _summary_email_body(month, steps, summary)
    email_sent = False
    try:
        email_sent = _send_email(subject, body)
    except Exception as exc:  # noqa: BLE001
        logger.exception("No se pudo enviar correo de cierre mensual.")
        summary["errores"] = [*summary["errores"], {"name": "email", "ok": False, "error": str(exc)}]
    return {
        "period": period,
        "steps": [step.as_dict() for step in steps],
        "summary": summary,
        "email_sent": email_sent,
    }


def _month_data_status(month: date) -> dict[str, object]:
    start = month
    end = _month_end(month)
    ventas_count = VentaHistorica.objects.filter(fecha__gte=start, fecha__lte=end).count()
    production_count = PointProductionLine.objects.filter(production_date__gte=start, production_date__lte=end).count()
    sales_days = (
        VentaHistorica.objects.filter(fecha__gte=start, fecha__lte=end)
        .values("fecha")
        .distinct()
        .aggregate(total=Count("fecha"))
        .get("total")
        or 0
    )
    production_days = (
        PointProductionLine.objects.filter(production_date__gte=start, production_date__lte=end)
        .values("production_date")
        .distinct()
        .aggregate(total=Count("production_date"))
        .get("total")
        or 0
    )
    missing = []
    if ventas_count == 0:
        missing.append("ventas")
    if production_count == 0:
        missing.append("produccion")
    return {
        "period": _period(month),
        "ventas_count": ventas_count,
        "ventas_dias": sales_days,
        "produccion_count": production_count,
        "produccion_dias": production_days,
        "missing": missing,
        "ok": not missing,
    }


@shared_task(name="core.tasks.verificar_datos_mes")
def verificar_datos_mes():
    month = _previous_month()
    status = _month_data_status(month)
    if status["ok"]:
        return {"period": status["period"], "ok": True, "email_sent": False, "status": status}
    subject = f"Alerta datos incompletos {month:%B %Y} — Pollyana's Dolce"
    body = "\n".join(
        [
            f"Verificación previa al cierre automático {_period(month)}",
            "",
            f"- Ventas históricas: {status['ventas_count']} filas en {status['ventas_dias']} días",
            f"- Producción Point: {status['produccion_count']} filas en {status['produccion_dias']} días",
            f"- Faltantes: {', '.join(status['missing'])}",
            "",
            "Revisar datos antes del cierre automático del día 5.",
        ]
    )
    email_sent = False
    try:
        email_sent = _send_email(subject, body)
    except Exception as exc:  # noqa: BLE001
        logger.exception("No se pudo enviar alerta de datos mensuales.")
        status["email_error"] = str(exc)
    return {"period": status["period"], "ok": False, "email_sent": email_sent, "status": status}
