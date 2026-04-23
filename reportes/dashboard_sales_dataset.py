from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

from django.conf import settings
from django.db import connection
from django.utils import timezone

from core.cache_versions import get_or_set_versioned_cache
from ventas.services.sales_canonical_source import canonical_point_max_date, canonical_point_previous_dates
from ventas.services.sales_read_service import get_daily_sales_bulk, get_sales_range


MONTH_NAMES = {
    1: "Ene",
    2: "Feb",
    3: "Mar",
    4: "Abr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Ago",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dic",
}


def _to_decimal(value) -> Decimal:
    return Decimal(str(value or 0))


def _coerce_json(value):
    if value is None:
        return []
    if isinstance(value, str):
        return json.loads(value)
    return value


def _fetch_dashboard_sales_dataset(*, today: date, months: int) -> dict[str, object]:
    sql = """
    WITH latest_dates AS (
        SELECT
            (SELECT MAX(fecha) FROM reportes_factventadiaria) AS latest_fact_date,
            (SELECT MAX(corte_date) FROM reportes_corteoficialdiario) AS latest_cut_date
    ),
    latest AS (
        SELECT COALESCE(
            latest_cut_date,
            latest_fact_date,
            %(today)s::date
        )::date AS latest_date
        FROM latest_dates
    ),
    prev AS (
        SELECT MAX(fecha)::date AS prev_date
        FROM reportes_factventadiaria
        WHERE fecha < (SELECT latest_date FROM latest)
    ),
    month_series AS (
        SELECT generate_series(
            date_trunc('month', (SELECT latest_date FROM latest))::date - ((%(months)s::int - 1) * INTERVAL '1 month'),
            date_trunc('month', (SELECT latest_date FROM latest))::date,
            INTERVAL '1 month'
        )::date AS month_start
    ),
    monthly_rows AS (
        SELECT
            ms.month_start,
            LEAST((ms.month_start + INTERVAL '1 month - 1 day')::date, (SELECT latest_date FROM latest)) AS period_end,
            COALESCE(
                CASE
                    WHEN (ms.month_start + INTERVAL '1 month - 1 day')::date <= (SELECT latest_date FROM latest)
                    THEN pmo.total_amount
                    ELSE NULL
                END,
                fact.amount,
                0
            ) AS amount,
            COALESCE(
                CASE
                    WHEN (ms.month_start + INTERVAL '1 month - 1 day')::date <= (SELECT latest_date FROM latest)
                    THEN pmo.total_quantity
                    ELSE NULL
                END,
                fact.quantity,
                0
            ) AS quantity,
            CASE
                WHEN pmo.id IS NOT NULL
                     AND (ms.month_start + INTERVAL '1 month - 1 day')::date <= (SELECT latest_date FROM latest)
                THEN 'Point oficial mensual'
                ELSE 'Point directo'
            END AS source_label
        FROM month_series ms
        LEFT JOIN pos_bridge_monthly_sales_official pmo
            ON pmo.month_start = ms.month_start
        LEFT JOIN LATERAL (
            SELECT
                COALESCE(SUM(fv.venta_total), 0) AS amount,
                COALESCE(SUM(fv.cantidad), 0) AS quantity
            FROM reportes_factventadiaria fv
            WHERE fv.fecha BETWEEN ms.month_start
                AND LEAST((ms.month_start + INTERVAL '1 month - 1 day')::date, (SELECT latest_date FROM latest))
        ) fact ON TRUE
        ORDER BY ms.month_start
    ),
    monthly_json AS (
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'periodo', to_char(month_start, 'YYYY-MM'),
                    'amount', amount::text,
                    'quantity', quantity::text,
                    'source_label', source_label
                )
                ORDER BY month_start
            ),
            '[]'::jsonb
        ) AS rows
        FROM monthly_rows
    ),
    latest_day_facts AS (
        SELECT *
        FROM reportes_factventadiaria
        WHERE fecha = (SELECT latest_date FROM latest)
    ),
    prev_day_facts AS (
        SELECT *
        FROM reportes_factventadiaria
        WHERE fecha = (SELECT prev_date FROM prev)
    ),
    day_fact AS (
        SELECT
            COALESCE(SUM(cantidad), 0) AS total_units,
            COALESCE(SUM(venta_total), 0) AS total_amount,
            COUNT(DISTINCT sucursal_id) FILTER (WHERE sucursal_id IS NOT NULL) AS branch_count,
            COUNT(DISTINCT receta_id) FILTER (WHERE receta_id IS NOT NULL) AS recipe_count
        FROM latest_day_facts
    ),
    prev_fact AS (
        SELECT
            COALESCE(SUM(cantidad), 0) AS total_units,
            COALESCE(SUM(venta_total), 0) AS total_amount
        FROM prev_day_facts
    ),
    month_fact AS (
        SELECT
            COALESCE(SUM(cantidad), 0) AS total_units,
            COALESCE(SUM(venta_total), 0) AS total_amount
        FROM reportes_factventadiaria
        WHERE fecha BETWEEN date_trunc('month', (SELECT latest_date FROM latest))::date
            AND (SELECT latest_date FROM latest)
    ),
    indicator_branch AS (
        SELECT
            pb.erp_branch_id AS sucursal_id,
            COALESCE(SUM(pdbi.total_tickets), 0) AS tickets
        FROM pos_bridge_daily_branch_indicators pdbi
        INNER JOIN pos_bridge_branches pb ON pb.id = pdbi.branch_id
        WHERE pdbi.indicator_date = (SELECT latest_date FROM latest)
          AND pb.erp_branch_id IS NOT NULL
        GROUP BY pb.erp_branch_id
    ),
    day_indicator AS (
        SELECT
            COALESCE(SUM(total_tickets), 0) AS total_tickets,
            COALESCE(SUM(total_amount), 0) AS total_amount
        FROM pos_bridge_daily_branch_indicators
        WHERE indicator_date = (SELECT latest_date FROM latest)
    ),
    month_indicator AS (
        SELECT COALESCE(SUM(total_tickets), 0) AS total_tickets
        FROM pos_bridge_daily_branch_indicators
        WHERE indicator_date BETWEEN date_trunc('month', (SELECT latest_date FROM latest))::date
            AND (SELECT latest_date FROM latest)
    ),
    day_cut AS (
        SELECT total_amount, total_tickets
        FROM reportes_corteoficialdiario
        WHERE corte_date = (SELECT latest_date FROM latest)
        LIMIT 1
    ),
    day_branch_base AS (
        SELECT
            s.id AS branch_id,
            s.codigo AS branch_code,
            s.nombre AS branch_name,
            COALESCE(SUM(fv.cantidad), 0) AS units,
            COALESCE(SUM(fv.venta_total), 0) AS amount
        FROM latest_day_facts fv
        INNER JOIN core_sucursal s ON s.id = fv.sucursal_id
        GROUP BY s.id, s.codigo, s.nombre
    ),
    top_branches AS (
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'branch_id', row.branch_id,
                    'branch_code', row.branch_code,
                    'branch_name', row.branch_name,
                    'units', row.units::text,
                    'amount', row.amount::text,
                    'tickets', COALESCE(ib.tickets, 0)
                )
                ORDER BY row.amount DESC, row.units DESC, row.branch_name
            ),
            '[]'::jsonb
        ) AS rows
        FROM (
            SELECT *
            FROM day_branch_base
            ORDER BY amount DESC, units DESC, branch_name
            LIMIT 5
        ) row
        LEFT JOIN indicator_branch ib ON ib.sucursal_id = row.branch_id
    ),
    present_branch_ids AS (
        SELECT COALESCE(jsonb_agg(branch_id ORDER BY branch_id), '[]'::jsonb) AS rows
        FROM day_branch_base
    ),
    day_product_base AS (
        SELECT
            COALESCE(receta_id::text, producto_clave) AS product_key,
            MAX(point_product_id) AS point_product_id,
            MAX(receta_id) AS recipe_id,
            MAX(COALESCE(NULLIF(producto_nombre, ''), r.nombre, 'Producto')) AS product_name,
            MAX(r.nombre) AS recipe_name,
            COALESCE(SUM(cantidad), 0) AS units,
            COALESCE(SUM(venta_total), 0) AS amount,
            COUNT(DISTINCT sucursal_id) FILTER (WHERE sucursal_id IS NOT NULL) AS branch_count
        FROM latest_day_facts fv
        LEFT JOIN recetas_receta r ON r.id = fv.receta_id
        GROUP BY COALESCE(receta_id::text, producto_clave)
    ),
    top_products AS (
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'product_id', row.point_product_id,
                    'recipe_id', row.recipe_id,
                    'recipe_name', row.recipe_name,
                    'product_name', row.product_name,
                    'units', row.units::text,
                    'amount', row.amount::text,
                    'branch_count', row.branch_count
                )
                ORDER BY row.amount DESC, row.units DESC, row.product_name
            ),
            '[]'::jsonb
        ) AS rows
        FROM (
            SELECT *
            FROM day_product_base
            ORDER BY amount DESC, units DESC, product_name
            LIMIT 5
        ) row
    )
    SELECT
        (SELECT latest_date FROM latest) AS latest_date,
        (SELECT prev_date FROM prev) AS prev_date,
        (SELECT total_units FROM day_fact) AS day_units,
        (SELECT total_amount FROM day_fact) AS raw_day_amount,
        COALESCE((SELECT total_tickets FROM day_indicator), 0) AS raw_day_tickets,
        COALESCE((SELECT branch_count FROM day_fact), 0) AS branch_count,
        COALESCE((SELECT recipe_count FROM day_fact), 0) AS recipe_count,
        COALESCE((SELECT total_units FROM prev_fact), 0) AS prev_units,
        COALESCE((SELECT total_amount FROM prev_fact), 0) AS prev_amount,
        COALESCE((SELECT total_units FROM month_fact), 0) AS month_units,
        COALESCE((SELECT total_amount FROM month_fact), 0) AS month_amount,
        COALESCE((SELECT total_tickets FROM month_indicator), 0) AS month_tickets,
        (SELECT total_amount FROM day_cut) AS cut_amount,
        (SELECT total_tickets FROM day_cut) AS cut_tickets,
        (SELECT rows FROM monthly_json) AS monthly_rows,
        (SELECT rows FROM top_branches) AS top_branches,
        (SELECT rows FROM top_products) AS top_products,
        (SELECT rows FROM present_branch_ids) AS present_branch_ids;
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, {"today": today, "months": months})
        row = cursor.fetchone()
    columns = [
        "latest_date",
        "prev_date",
        "day_units",
        "raw_day_amount",
        "raw_day_tickets",
        "branch_count",
        "recipe_count",
        "prev_units",
        "prev_amount",
        "month_units",
        "month_amount",
        "month_tickets",
        "cut_amount",
        "cut_tickets",
        "monthly_rows",
        "top_branches",
        "top_products",
        "present_branch_ids",
    ]
    payload = dict(zip(columns, row, strict=True))
    return {
        "latest_date": payload["latest_date"],
        "prev_date": payload["prev_date"],
        "day_units": _to_decimal(payload["day_units"]),
        "raw_day_amount": _to_decimal(payload["raw_day_amount"]),
        "raw_day_tickets": int(payload["raw_day_tickets"] or 0),
        "branch_count": int(payload["branch_count"] or 0),
        "recipe_count": int(payload["recipe_count"] or 0),
        "prev_units": _to_decimal(payload["prev_units"]),
        "prev_amount": _to_decimal(payload["prev_amount"]),
        "month_units": _to_decimal(payload["month_units"]),
        "month_amount": _to_decimal(payload["month_amount"]),
        "month_tickets": int(payload["month_tickets"] or 0),
        "cut_amount": _to_decimal(payload["cut_amount"]),
        "cut_tickets": int(payload["cut_tickets"] or 0),
        "monthly_rows": _coerce_json(payload["monthly_rows"]),
        "top_branches": _coerce_json(payload["top_branches"]),
        "top_products": _coerce_json(payload["top_products"]),
        "present_branch_ids": [int(value) for value in _coerce_json(payload["present_branch_ids"])],
    }


def _build_dashboard_sales_dataset(*, today: date, months: int) -> dict[str, object]:
    raw = _fetch_dashboard_sales_dataset(today=today, months=months)
    canonical_latest_date = canonical_point_max_date()
    should_use_canonical_daily_fallback = bool(
        canonical_latest_date
        and raw["day_units"] == 0
        and raw["cut_amount"] == 0
        and raw["branch_count"] == 0
        and raw["recipe_count"] == 0
        and raw["latest_date"] != canonical_latest_date
    )
    if should_use_canonical_daily_fallback:
        prev_candidates = canonical_point_previous_dates(canonical_latest_date)
        prev_date = prev_candidates[0] if prev_candidates else None
        selected = get_sales_range(
            start_date=canonical_latest_date,
            end_date=canonical_latest_date,
            coverage_policy="strict_priority",
        )
        prev_selected = (
            get_sales_range(
                start_date=prev_date,
                end_date=prev_date,
                coverage_policy="strict_priority",
            )
            if prev_date
            else None
        )
        branch_bulk = get_daily_sales_bulk(
            fechas=[value for value in [canonical_latest_date, prev_date] if value],
            dimension="branch",
            include_indicators=True,
            coverage_policy="strict_priority",
        )
        product_bulk = get_daily_sales_bulk(
            fechas=[value for value in [canonical_latest_date, prev_date] if value],
            dimension="product",
            coverage_policy="strict_priority",
        )
        latest_branch_payload = branch_bulk["dates"].get(canonical_latest_date.isoformat(), {})
        latest_branch_rows = list(latest_branch_payload.get("rows") or [])
        indicator_map = latest_branch_payload.get("indicator_map") or {}
        latest_product_payload = product_bulk["dates"].get(canonical_latest_date.isoformat(), {})
        latest_product_rows = list(latest_product_payload.get("rows") or [])
        indicator_total_amount = sum(
            (Decimal(str(payload.get("amount") or 0)) for payload in indicator_map.values()),
            Decimal("0"),
        )
        indicator_total_tickets = sum(
            (int(payload.get("tickets") or 0) for payload in indicator_map.values()),
            0,
        )
        raw.update(
            {
                "latest_date": canonical_latest_date,
                "prev_date": prev_date,
                "day_units": Decimal(str(selected.get("cantidad") or 0)),
                "raw_day_amount": Decimal(str(selected.get("monto") or 0)),
                "raw_day_tickets": indicator_total_tickets,
                "branch_count": len(latest_branch_rows),
                "recipe_count": len(latest_product_rows),
                "prev_units": Decimal(str((prev_selected or {}).get("cantidad") or 0)),
                "prev_amount": Decimal(str((prev_selected or {}).get("monto") or 0)),
                "cut_amount": Decimal("0"),
                "cut_tickets": 0,
                "top_branches": [
                    {
                        "branch_id": row.get("branch_id"),
                        "branch_code": row.get("branch_code"),
                        "branch_name": row.get("branch_name"),
                        "units": str(row.get("units") or 0),
                        "amount": str(row.get("amount") or 0),
                        "tickets": int((indicator_map.get(row.get("branch_id")) or {}).get("tickets") or 0),
                    }
                    for row in latest_branch_rows
                ],
                "top_products": [
                    {
                        "product_id": row.get("product_id"),
                        "recipe_id": row.get("recipe_id"),
                        "recipe_name": row.get("recipe_name") or row.get("product_name"),
                        "product_name": row.get("product_name") or row.get("recipe_name"),
                        "units": str(row.get("units") or 0),
                        "amount": str(row.get("amount") or 0),
                        "branch_count": int(row.get("branch_count") or 0),
                    }
                    for row in latest_product_rows
                ],
                "present_branch_ids": [int(row.get("branch_id")) for row in latest_branch_rows if row.get("branch_id")],
            }
        )
        if indicator_total_amount > 0:
            raw["cut_amount"] = indicator_total_amount

    monthly_rows = []
    monthly_max = max((_to_decimal(row.get("amount")) for row in raw["monthly_rows"]), default=Decimal("0"))
    for row in raw["monthly_rows"]:
        periodo = str(row.get("periodo") or "")
        year, month = periodo.split("-", 1) if "-" in periodo else ("0000", "00")
        value = _to_decimal(row.get("amount"))
        pct = float((value / monthly_max) * Decimal("100")) if monthly_max > 0 else 0.0
        monthly_rows.append(
            {
                "periodo": periodo,
                "label": f"{MONTH_NAMES.get(int(month), month)} {year[-2:]}",
                "value": value,
                "ventas": value,
                "pct": max(8.0, pct) if value > 0 else 0.0,
                "source_label": str(row.get("source_label") or "Point directo"),
            }
        )

    total_amount = raw["cut_amount"] if raw["cut_amount"] > 0 else raw["raw_day_amount"]
    total_tickets = raw["cut_tickets"] if raw["cut_tickets"] > 0 else raw["raw_day_tickets"]
    comparison_label = "Base inicial"
    comparison_tone = "warning"
    comparison_detail = "Aún no hay un corte previo comparable."
    comparison_basis = "Contra el corte inmediato anterior"
    if raw["prev_amount"] > 0:
        delta_pct = ((total_amount - raw["prev_amount"]) / raw["prev_amount"]) * Decimal("100")
        comparison_label = "Arriba" if delta_pct >= 0 else "Abajo"
        comparison_tone = "success" if delta_pct >= 0 else "warning"
        comparison_detail = f"{abs(delta_pct):.1f}% vs corte previo ({raw['prev_date'].isoformat()})"
    elif raw["prev_units"] > 0:
        delta_pct = ((raw["day_units"] - raw["prev_units"]) / raw["prev_units"]) * Decimal("100")
        comparison_label = "Arriba" if delta_pct >= 0 else "Abajo"
        comparison_tone = "success" if delta_pct >= 0 else "warning"
        comparison_detail = f"{abs(delta_pct):.1f}% en unidades vs corte previo ({raw['prev_date'].isoformat()})"

    top_branches = [
        {
            "branch_id": row.get("branch_id"),
            "branch_code": row.get("branch_code") or "Sucursal",
            "branch_name": row.get("branch_name") or "",
            "label": row.get("branch_code") or "Sucursal",
            "secondary": row.get("branch_name") or "",
            "amount": _to_decimal(row.get("amount")),
            "total": _to_decimal(row.get("units")),
            "tickets": int(row.get("tickets") or 0),
        }
        for row in raw["top_branches"]
    ]
    top_products = [
        {
            "product_id": row.get("product_id"),
            "recipe_id": row.get("recipe_id"),
            "recipe_name": row.get("recipe_name") or row.get("product_name") or "Producto",
            "label": row.get("recipe_name") or row.get("product_name") or "Producto",
            "secondary": "",
            "amount": _to_decimal(row.get("amount")),
            "total": _to_decimal(row.get("units")),
            "branch_count": int(row.get("branch_count") or 0),
        }
        for row in raw["top_products"]
    ]

    tickets_available = total_tickets > 0
    month_tickets_available = raw["month_tickets"] > 0
    return {
        "latest_date": raw["latest_date"],
        "prev_date": raw["prev_date"],
        "present_branch_ids": raw["present_branch_ids"],
        "monthly_sales_rows": monthly_rows,
        "daily_sales_snapshot": {
            "status": "Corte cargado",
            "tone": "success",
            "detail": "Resumen del último corte de ventas. Fuente canónica Point bridge.",
            "date": raw["latest_date"],
            "date_label": raw["latest_date"].isoformat() if raw["latest_date"] else "",
            "month_label": f"{raw['latest_date'].year}-{raw['latest_date'].month:02d}" if raw["latest_date"] else "",
            "source_label": "Point directo",
            "total_units": raw["day_units"],
            "total_amount": total_amount,
            "raw_total_amount": raw["raw_day_amount"],
            "total_tickets": total_tickets,
            "raw_total_tickets": raw["raw_day_tickets"],
            "tickets_available": tickets_available,
            "branch_count": raw["branch_count"],
            "recipe_count": raw["recipe_count"],
            "avg_ticket": (total_amount / Decimal(total_tickets)) if tickets_available else None,
            "avg_branch_amount": (total_amount / Decimal(raw["branch_count"])) if raw["branch_count"] else Decimal("0"),
            "month_amount": raw["month_amount"],
            "month_units": raw["month_units"],
            "month_tickets": raw["month_tickets"],
            "month_tickets_available": month_tickets_available,
            "month_avg_ticket": (raw["month_amount"] / Decimal(raw["month_tickets"])) if month_tickets_available else None,
            "comparison_label": comparison_label,
            "comparison_tone": comparison_tone,
            "comparison_detail": comparison_detail,
            "comparison_basis": comparison_basis,
            "top_branches": top_branches,
            "top_products": top_products,
        },
    }


def get_dashboard_sales_dataset(*, today: date | None = None, months: int = 6) -> dict[str, object]:
    today = today or timezone.localdate()
    months = max(int(months or 6), 1)
    if bool(getattr(settings, "RUNNING_TESTS", False)):
        return _build_dashboard_sales_dataset(today=today, months=months)
    return get_or_set_versioned_cache(
        key_parts=("erp", "analytics", "dashboard-sales-dataset", today.isoformat(), months),
        scopes=("ventas", "dashboard"),
        builder=lambda: _build_dashboard_sales_dataset(today=today, months=months),
    )
