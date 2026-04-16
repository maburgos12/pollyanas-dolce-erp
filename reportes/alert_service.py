from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

from django.conf import settings
from django.db.models import Avg, Sum
from django.utils import timezone

from inventario.models import ExistenciaInsumo
from maestros.models import CostoInsumo
from reportes.models import Alert, AutoControlSettings, FactVentaDiaria, ProductionExecutionLog
from reportes.opportunity_service import build_opportunity_context


ZERO = Decimal("0")


def _to_decimal(value, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


def _alerts_enabled() -> bool:
    return bool(getattr(settings, "ERP_OPERATION_ALERTS_ENABLED", True))


def _effective_alerts_enabled() -> bool:
    return _alerts_enabled() and AutoControlSettings.get_solo().enable_alerts


def _json_safe(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value


def _upsert_alert(
    *,
    alert_key: str,
    tipo: str,
    severidad: str,
    entidad: str,
    fecha: date,
    mensaje: str,
    impacto_estimado: Decimal,
    sucursal_id: int | None = None,
    receta_id: int | None = None,
    insumo_id: int | None = None,
    metadata: dict[str, object] | None = None,
) -> Alert:
    alert, _ = Alert.objects.update_or_create(
        alert_key=alert_key,
        defaults={
            "tipo": tipo,
            "severidad": severidad,
            "entidad": entidad,
            "fecha": fecha,
            "mensaje": mensaje,
            "impacto_estimado": _money(impacto_estimado),
            "sucursal_id": sucursal_id,
            "receta_id": receta_id,
            "insumo_id": insumo_id,
            "metadata": _json_safe(metadata or {}),
            "resuelta": False,
            "resolved_at": None,
            "resolved_by": None,
            "resolution_note": "",
        },
    )
    return alert


def resolve_alert(
    *,
    alert: Alert,
    resolved_by,
    resolution_note: str = "",
    impacto_real: Decimal | None = None,
) -> Alert:
    metadata = dict(alert.metadata or {})
    if impacto_real is not None:
        metadata["impacto_real"] = str(_money(_to_decimal(impacto_real)))
    alert.resuelta = True
    alert.resolved_at = timezone.now()
    alert.resolved_by = resolved_by
    alert.resolution_note = (resolution_note or "").strip()
    alert.metadata = _json_safe(metadata)
    alert.save(
        update_fields=[
            "resuelta",
            "resolved_at",
            "resolved_by",
            "resolution_note",
            "metadata",
            "updated_at",
        ]
    )
    return alert


def _recent_recipe_price_maps(target_date: date) -> tuple[dict[tuple[int, int], Decimal], dict[tuple[int, int], Decimal]]:
    rows = (
        FactVentaDiaria.objects.filter(
            fecha__gte=target_date - timedelta(days=28),
            fecha__lte=target_date,
            receta_id__isnull=False,
            sucursal_id__isnull=False,
        )
        .values("sucursal_id", "receta_id")
        .annotate(
            qty=Sum("cantidad"),
            revenue=Sum("venta_neta"),
            cost=Sum("costo_estimado"),
        )
    )
    asp_map: dict[tuple[int, int], Decimal] = {}
    cost_map: dict[tuple[int, int], Decimal] = {}
    for row in rows:
        key = (int(row["sucursal_id"]), int(row["receta_id"]))
        qty = _to_decimal(row.get("qty"))
        if qty <= ZERO:
            continue
        asp_map[key] = _to_decimal(row.get("revenue")) / qty
        cost_map[key] = _to_decimal(row.get("cost")) / qty
    return asp_map, cost_map


def generate_operational_alerts(*, target_date: date | None = None) -> dict[str, object]:
    target_date = target_date or timezone.localdate()
    if not _effective_alerts_enabled():
        return {
            "target_date": target_date.isoformat(),
            "enabled": False,
            "created_or_updated": 0,
            "rows": [],
        }

    asp_map, cost_map = _recent_recipe_price_maps(target_date)
    alerts: list[Alert] = []

    execution_rows = list(
        ProductionExecutionLog.objects.filter(fecha=target_date)
        .select_related("sucursal", "receta")
        .order_by("sucursal__codigo", "receta__nombre")
    )
    historical_waste = {
        (int(row["sucursal_id"]), int(row["receta_id"])): {
            "avg_merma": _to_decimal(row.get("avg_merma")),
            "avg_produced": _to_decimal(row.get("avg_produced")),
        }
        for row in (
            ProductionExecutionLog.objects.filter(
                fecha__gte=target_date - timedelta(days=28),
                fecha__lt=target_date,
            )
            .values("sucursal_id", "receta_id")
            .annotate(avg_merma=Avg("merma"), avg_produced=Avg("producido_real"))
        )
    }
    for row in execution_rows:
        branch_recipe_key = (row.sucursal_id, row.receta_id)
        estimated_sale_price = asp_map.get(branch_recipe_key, ZERO)
        estimated_cost = cost_map.get(branch_recipe_key, ZERO)

        deviation_abs = abs(_to_decimal(row.producido_real) - _to_decimal(row.aprobado))
        deviation_threshold = max(_to_decimal(row.aprobado) * Decimal("0.15"), Decimal("2"))
        if deviation_abs >= deviation_threshold and deviation_abs > ZERO:
            severity = Alert.SEVERITY_HIGH if deviation_abs >= max(_to_decimal(row.aprobado) * Decimal("0.30"), Decimal("5")) else Alert.SEVERITY_MEDIUM
            alerts.append(
                _upsert_alert(
                    alert_key=f"DESVIACION:{target_date.isoformat()}:{row.sucursal_id}:{row.receta_id}",
                    tipo=Alert.TYPE_DESVIACION,
                    severidad=severity,
                    entidad=f"{row.receta.nombre} · {row.sucursal.codigo}",
                    fecha=target_date,
                    sucursal_id=row.sucursal_id,
                    receta_id=row.receta_id,
                    mensaje=(
                        f"Desviación operativa de {deviation_abs.quantize(Decimal('0.001'))} pzs "
                        f"entre aprobado y ejecutado en {row.receta.nombre}."
                    ),
                    impacto_estimado=deviation_abs * estimated_cost,
                    metadata={
                        "aprobado": str(row.aprobado),
                        "producido_real": str(row.producido_real),
                        "recommendation_version": row.recommendation_version,
                    },
                )
            )

        historical = historical_waste.get(branch_recipe_key, {})
        avg_merma = _to_decimal(historical.get("avg_merma"))
        avg_produced = _to_decimal(historical.get("avg_produced"))
        waste_threshold = max(avg_merma * Decimal("1.25"), Decimal("2"))
        if _to_decimal(row.merma) >= waste_threshold and _to_decimal(row.merma) > ZERO:
            produced_denominator = max(_to_decimal(row.producido_real), avg_produced, Decimal("1"))
            waste_rate = (_to_decimal(row.merma) / produced_denominator) * Decimal("100")
            severity = Alert.SEVERITY_HIGH if waste_rate >= Decimal("15") else Alert.SEVERITY_MEDIUM
            alerts.append(
                _upsert_alert(
                    alert_key=f"MERMA:{target_date.isoformat()}:{row.sucursal_id}:{row.receta_id}",
                    tipo=Alert.TYPE_MERMA,
                    severidad=severity,
                    entidad=f"{row.receta.nombre} · {row.sucursal.codigo}",
                    fecha=target_date,
                    sucursal_id=row.sucursal_id,
                    receta_id=row.receta_id,
                    mensaje=(
                        f"Merma de {row.merma} pzs en {row.receta.nombre}, por encima del histórico "
                        f"({avg_merma.quantize(Decimal('0.001'))} pzs promedio 28d)."
                    ),
                    impacto_estimado=_to_decimal(row.merma) * estimated_cost,
                    metadata={
                        "avg_merma_28d": str(avg_merma),
                        "avg_produced_28d": str(avg_produced),
                        "merma": str(row.merma),
                    },
                )
            )

    latest_cost_by_insumo = {
        int(row["insumo_id"]): _to_decimal(row.get("costo_unitario"))
        for row in (
            CostoInsumo.objects.order_by("insumo_id", "-fecha", "-id")
            .distinct("insumo_id")
            .values("insumo_id", "costo_unitario")
        )
    }
    stock_rows = list(
        ExistenciaInsumo.objects.select_related("insumo", "insumo__proveedor_principal")
        .filter(stock_minimo__gt=ZERO)
        .order_by("insumo__nombre")
    )
    for row in stock_rows:
        stock_actual = _to_decimal(row.stock_actual)
        threshold = max(_to_decimal(row.stock_minimo), _to_decimal(row.punto_reorden))
        if stock_actual > threshold:
            continue
        shortage = max(threshold - stock_actual, ZERO)
        severity = Alert.SEVERITY_HIGH if stock_actual <= (_to_decimal(row.stock_minimo) * Decimal("0.5")) else Alert.SEVERITY_MEDIUM
        alerts.append(
            _upsert_alert(
                alert_key=f"STOCK:{target_date.isoformat()}:{row.insumo_id}",
                tipo=Alert.TYPE_STOCK,
                severidad=severity,
                entidad=row.insumo.nombre,
                fecha=target_date,
                insumo_id=row.insumo_id,
                mensaje=(
                    f"Stock crítico en {row.insumo.nombre}: disponible {stock_actual.quantize(Decimal('0.001'))} "
                    f"vs umbral {threshold.quantize(Decimal('0.001'))}."
                ),
                impacto_estimado=shortage * latest_cost_by_insumo.get(int(row.insumo_id), ZERO),
                metadata={
                    "stock_actual": str(stock_actual),
                    "stock_minimo": str(row.stock_minimo),
                    "punto_reorden": str(row.punto_reorden),
                },
            )
        )

    opportunity_context = build_opportunity_context(target_date=target_date, top_n=None)
    for row in opportunity_context.get("rows") or []:
        if row.get("priority") != "ALTA":
            continue
        branch_id = int(row["branch_id"])
        recipe_id = int(row["recipe_id"])
        forecast_amount = _to_decimal(row.get("forecast_amount"))
        alerts.append(
            _upsert_alert(
                alert_key=f"OPORTUNIDAD:{target_date.isoformat()}:{branch_id}:{recipe_id}:{row['action']}",
                tipo=Alert.TYPE_OPORTUNIDAD,
                severidad=Alert.SEVERITY_HIGH,
                entidad=f"{row['recipe_name']} · {row['branch_code']}",
                fecha=target_date,
                sucursal_id=branch_id,
                receta_id=recipe_id,
                mensaje=f"{row['action']}: {row['why']}",
                impacto_estimado=forecast_amount,
                metadata=row,
            )
        )

    alerts.sort(key=lambda item: (item.impacto_estimado, item.severidad), reverse=True)
    return {
        "target_date": target_date.isoformat(),
        "enabled": True,
        "created_or_updated": len(alerts),
        "critical": sum(1 for alert in alerts if alert.severidad == Alert.SEVERITY_HIGH and not alert.resuelta),
        "rows": [
            {
                "tipo": alert.tipo,
                "severidad": alert.severidad,
                "entidad": alert.entidad,
                "impacto_estimado": str(alert.impacto_estimado),
                "mensaje": alert.mensaje,
            }
            for alert in alerts
        ],
    }
