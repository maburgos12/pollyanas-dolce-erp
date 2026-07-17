from __future__ import annotations

from datetime import date, timedelta

from celery import shared_task

from reportes.analytics_service import rebuild_production_facts


@shared_task(name="reportes.snapshot_historical_costing_task")
def snapshot_historical_costing_task():
    """Congela costo historico del mes anterior al dia 1 de cada mes."""
    from reportes.services_historical_costing import MonthlyHistoricalCostingService

    mes_actual = date.today().replace(day=1)
    periodo = (mes_actual - timedelta(days=1)).replace(day=1)
    summary = MonthlyHistoricalCostingService().build_period(period_start=periodo)
    return {
        "period": f"{periodo:%Y-%m}",
        "insumo_rows": summary.insumo_rows,
        "receta_rows": summary.receta_rows,
        "missing_recipe_rows": summary.missing_recipe_rows,
        "producto_reventa_rows": summary.producto_reventa_rows,
    }


@shared_task(name="reportes.refresh_dg_operacion_snapshot", bind=True, max_retries=1, default_retry_delay=300)
def task_refresh_dg_operacion_snapshot(self):
    from reportes.services_dg_operacion_snapshot import refresh_dg_operacion_snapshot

    snapshot = refresh_dg_operacion_snapshot()
    return {
        "status": snapshot.status,
        "fecha_operacion": snapshot.fecha_operacion.isoformat(),
        "snapshot_id": snapshot.id,
        "generated_at": snapshot.generated_at.isoformat() if snapshot.generated_at else "",
    }


@shared_task(name="reportes.enviar_reporte_diario")
def enviar_reporte_diario(fecha_operacion: str | None = None) -> dict:
    from reportes.services_reporte_diario import construir_y_enviar_reporte_diario

    return construir_y_enviar_reporte_diario(fecha_operacion=fecha_operacion)


@shared_task(name="reportes.refresh_investment_snapshots", bind=True, max_retries=1, default_retry_delay=300)
def task_refresh_investment_snapshots(self):
    from reportes.models import ProyectoInversion
    from reportes.services_investment_projects import ProyectoInversionRefreshService

    statuses = [ProyectoInversion.ESTATUS_ACTIVO, ProyectoInversion.ESTATUS_EN_RECUPERACION]
    service = ProyectoInversionRefreshService()
    results = []
    for project in ProyectoInversion.objects.filter(estatus__in=statuses).order_by("id"):
        result = service.refresh_project(project)
        results.append(
            {
                "project_id": result.project_id,
                "snapshots_updated": result.snapshots_updated,
                "latest_period": result.latest_period.isoformat() if result.latest_period else None,
                "project_status": result.project_status,
                "data_gaps": result.data_gaps,
            }
        )
    return {
        "projects_refreshed": len(results),
        "snapshots_created": sum(item["snapshots_updated"] for item in results),
        "results": results,
    }


@shared_task(name="reportes.cierre_produccion_nocturno", bind=True, max_retries=1, default_retry_delay=300)
def task_cierre_produccion_nocturno(self):
    """
    Reconstruye FactProduccionDiaria para los ultimos 3 dias.
    Corre despues del sync de ventas para usar datos frescos de Point.
    """
    today = date.today()
    results = []
    errors = []
    for delta in range(3):
        target = today - timedelta(days=delta)
        try:
            rows = rebuild_production_facts(start_date=target, end_date=target)
            results.append({"date": target.isoformat(), "rows": rows})
        except Exception as exc:  # noqa: BLE001
            errors.append({"date": target.isoformat(), "error": str(exc)})
    return {"rebuilt_dates": len(results), "results": results, "errors": errors}


@shared_task(name="reportes.alerta_produccion_sin_registros", bind=True, max_retries=1)
def task_alerta_produccion_sin_registros(self):
    """
    Revisa si Point registro produccion en el dia habil anterior.
    Si no hay registros, envia email de alerta a Direccion General.
    """
    from django.conf import settings
    from django.core.mail import send_mail
    from django.utils import timezone
    from pos_bridge.models import PointProductionLine

    today = timezone.localdate()
    target = today - timedelta(days=1)
    if target.weekday() == 6:  # domingo
        target = today - timedelta(days=2)

    count = PointProductionLine.objects.filter(
        production_date=target,
        is_insumo=False,
    ).count()

    if count > 0:
        return {
            "status": "ok",
            "date": target.isoformat(),
            "registros": count,
        }

    subject = f"Sin produccion registrada en Point - {target:%d/%b/%Y}"
    body = (
        "Alerta automatica del ERP Pollyana's Dolce.\n\n"
        f"Point no tiene registros de produccion para el {target:%d/%m/%Y}.\n\n"
        "Posibles causas:\n"
        "  - Produccion Crucero no capturo en Point\n"
        "  - El sync automatico del ERP no corrio\n"
        "  - No hubo produccion ese dia\n\n"
        "Verificar en: https://erp.pollyanasdolce.com/reportes/produccion/\n\n"
        "-- ERP Pollyana's Dolce"
    )

    try:
        send_mail(
            subject=subject,
            message=body,
            from_email=getattr(settings, "DEFAULT_FROM_EMAIL", "erp@pollyanasdolce.com"),
            recipient_list=["maburgos12@pollyanasdolce.com"],
            fail_silently=False,
        )
    except Exception as exc:
        raise self.retry(exc=exc)

    return {
        "status": "alerta_enviada",
        "date": target.isoformat(),
        "registros": 0,
    }


from reportes.tasks_doctor import erp_doctor_daily_report  # noqa: E402,F401


@shared_task(name="reportes.monitoreo_variacion_costos_reventa", bind=True, max_retries=1, default_retry_delay=300)
def task_monitoreo_variacion_costos_reventa(self):
    """
    Compara el costo actual de cada producto de reventa contra el costo del mes anterior.
    Si la variación es mayor al UMBRAL (10%), genera una alerta por email.
    Corre el día 2 de cada mes, después del cierre mensual (día 1).
    """
    from decimal import Decimal
    from django.conf import settings
    from django.core.mail import send_mail
    from django.utils import timezone
    from reportes.models import ProductoReventaCosto, ProductoReventaCostoHistoricoMensual

    UMBRAL_PCT = Decimal("10")

    hoy = timezone.localdate()
    mes_actual = hoy.replace(day=1)
    mes_anterior = (mes_actual - timedelta(days=1)).replace(day=1)

    # Último costo registrado por producto (puede ser cualquier fuente)
    costos_actuales = {}
    for rc in (
        ProductoReventaCosto.objects
        .select_related("producto_point")
        .order_by("producto_point_id", "-fecha_vigencia")
    ):
        if rc.producto_point_id not in costos_actuales:
            costos_actuales[rc.producto_point_id] = rc

    # Histórico del mes anterior
    historico = {
        h.producto_point_id: h
        for h in ProductoReventaCostoHistoricoMensual.objects.filter(periodo=mes_anterior)
    }

    alertas = []
    for pid, rc in costos_actuales.items():
        hist = historico.get(pid)
        if hist is None or hist.costo_promedio <= 0:
            continue
        costo_prev = hist.costo_promedio
        costo_curr = rc.costo_unitario
        if costo_prev <= 0:
            continue
        variacion_pct = abs(costo_curr - costo_prev) / costo_prev * 100
        if variacion_pct >= UMBRAL_PCT:
            direccion = "▲ SUBIÓ" if costo_curr > costo_prev else "▼ BAJÓ"
            alertas.append({
                "nombre": rc.producto_point.name,
                "costo_prev": float(costo_prev),
                "costo_curr": float(costo_curr),
                "variacion_pct": float(variacion_pct),
                "direccion": direccion,
                "fuente": rc.fuente,
            })

    result = {
        "periodo_anterior": mes_anterior.isoformat(),
        "productos_comparados": len(costos_actuales),
        "con_variacion": len(alertas),
    }

    if not alertas:
        return result

    alertas.sort(key=lambda x: -x["variacion_pct"])

    lineas = [
        f"  {a['direccion']} {a['nombre']}: "
        f"${a['costo_prev']:.4f} → ${a['costo_curr']:.4f} "
        f"({a['variacion_pct']:.1f}%) [{a['fuente']}]"
        for a in alertas
    ]
    body = (
        f"Alerta automática ERP Pollyana's Dolce — Variación de costos de reventa\n"
        f"Período anterior: {mes_anterior:%B %Y}\n"
        f"Productos con variación ≥ {UMBRAL_PCT}%: {len(alertas)}\n\n"
        + "\n".join(lineas)
        + "\n\nGestionar costos: https://erp.pollyanasdolce.com/maestros/costos-adquisicion/\n"
        "-- ERP Pollyana's Dolce"
    )

    try:
        send_mail(
            subject=f"[ERP] {len(alertas)} producto(s) con variación de costo ≥{UMBRAL_PCT}% — {mes_anterior:%b %Y}",
            message=body,
            from_email=getattr(settings, "DEFAULT_FROM_EMAIL", "erp@pollyanasdolce.com"),
            recipient_list=["maburgos12@pollyanasdolce.com"],
            fail_silently=False,
        )
    except Exception as exc:
        raise self.retry(exc=exc)

    return {**result, "email_enviado": True}


@shared_task(name="reportes.consolidar_presupuesto_real", bind=True, max_retries=1, default_retry_delay=300)
def task_consolidar_presupuesto_real(self, periodo: str | None = None):
    """Consolida el monto real del presupuesto maestro para el mes indicado.

    Sin argumento consolida el mes en curso y los DOS meses anteriores:
    las fuentes cierran tarde (la nómina de junio se marcó PAGADA hasta
    mediados de julio y sus sueldos quedaron invisibles en el tablero
    porque solo se refrescaba el mes corriente). Re-consolidar meses
    pasados es seguro: nunca pisa capturas MANUAL y el namespace AUTO
    solo se re-escribe con datos de su misma fuente.
    """
    from reportes.services_presupuesto_real import PresupuestoRealConsolidacionService

    service = PresupuestoRealConsolidacionService()
    hoy = date.today()
    if periodo:
        year, month = periodo.split("-")
        periodos = [date(int(year), int(month), 1)]
    else:
        mes = hoy.replace(day=1)
        periodos = [mes]
        for _ in range(2):
            mes = (mes - timedelta(days=1)).replace(day=1)
            periodos.append(mes)

    try:
        return [service.consolidar(periodo=p).as_dict() for p in periodos]
    except Exception as exc:
        raise self.retry(exc=exc)
