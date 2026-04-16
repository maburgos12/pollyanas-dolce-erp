from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

from django.db import connection
from django.urls import reverse


ZERO = Decimal("0")


def _to_decimal(value) -> Decimal:
    decimal_value = Decimal(str(value or 0))
    return ZERO if decimal_value == 0 else decimal_value


def _coerce_json(value):
    if value is None:
        return []
    if isinstance(value, str):
        return json.loads(value)
    return value


def _build_sales_history_summary(payload: dict[str, object]) -> dict[str, object]:
    first_date = payload.get("first_date")
    last_date = payload.get("last_date")
    total_rows = int(payload.get("total_rows") or 0)
    active_days = int(payload.get("active_days") or 0)
    branch_count = int(payload.get("branch_count") or 0)
    recipe_count = int(payload.get("recipe_count") or 0)
    total_units = _to_decimal(payload.get("total_units"))
    total_amount = _to_decimal(payload.get("total_amount"))
    expected_days = ((last_date - first_date).days + 1) if first_date and last_date else 0
    missing_days = max(expected_days - active_days, 0)
    return {
        "available": total_rows > 0,
        "status": "Cobertura cerrada" if missing_days == 0 else "Cobertura parcial",
        "tone": "success" if missing_days == 0 else "warning",
        "official_ready": missing_days == 0 and total_rows > 0,
        "detail": (
            "Fuente canónica Point disponible para lectura ejecutiva."
            if missing_days == 0
            else f"Fuente canónica Point disponible con {missing_days} día(s) faltantes dentro del rango visible."
        ),
        "source_label": "Point directo",
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
        "latest_source": "POINT_STAGE",
        "top_branches": _coerce_json(payload.get("top_branches")),
        "top_recipes": _coerce_json(payload.get("top_products")),
        "source_coverage_rows": _coerce_json(payload.get("source_coverage_rows")),
        "url": reverse("reportes:bi"),
        "cta": "Abrir reportes",
    }


def _build_branch_rows(*, current_rows: list[dict[str, object]], compare_rows: list[dict[str, object]], compare_label: str, weekday_mode: bool) -> list[dict[str, object]]:
    compare_map = {int(row["branch_id"]): row for row in compare_rows if row.get("branch_id")}
    rows: list[dict[str, object]] = []
    for row in current_rows:
        branch_id = int(row["branch_id"])
        compare = compare_map.get(branch_id)
        if not compare:
            continue
        current_amount = _to_decimal(row.get("indicator_amount") or row.get("amount"))
        current_units = _to_decimal(row.get("units"))
        current_tickets = int(row.get("indicator_tickets") or row.get("fact_tickets") or 0)
        compare_amount = _to_decimal(compare.get("indicator_amount") or compare.get("amount"))
        compare_units = _to_decimal(compare.get("units"))
        delta_pct = None
        if compare_amount > 0:
            delta_pct = ((current_amount - compare_amount) / compare_amount) * Decimal("100")
        elif compare_units > 0:
            delta_pct = ((current_units - compare_units) / compare_units) * Decimal("100")
        if delta_pct is None:
            if weekday_mode:
                continue
            status, tone, detail, rank_score = "Sin comparativo", "warning", "No hay corte previo comparable para esta sucursal.", Decimal("0")
        elif delta_pct <= (Decimal("-12") if weekday_mode else Decimal("-15")):
            status = "Abajo del comparable" if weekday_mode else "Caída fuerte"
            tone = "danger"
            detail = (
                f"Cae {abs(delta_pct):.1f}% contra el último mismo día de semana ({compare_label})."
                if weekday_mode
                else f"Cae {abs(delta_pct):.1f}% contra el corte previo."
            )
            rank_score = abs(delta_pct) + Decimal("100")
        elif delta_pct >= (Decimal("12") if weekday_mode else Decimal("15")):
            status = "Arriba del comparable" if weekday_mode else "Alza fuerte"
            tone = "success"
            detail = (
                f"Sube {delta_pct:.1f}% contra el último mismo día de semana ({compare_label})."
                if weekday_mode
                else f"Sube {delta_pct:.1f}% contra el corte previo."
            )
            rank_score = delta_pct
        else:
            status = "Dentro de rango" if weekday_mode else "Estable"
            tone = "warning"
            detail = (
                f"Variación de {delta_pct:.1f}% contra el último mismo día de semana ({compare_label})."
                if weekday_mode
                else f"Variación de {delta_pct:.1f}% contra el corte previo."
            )
            rank_score = abs(delta_pct)
        rows.append(
            {
                "branch_id": branch_id,
                # Legacy dashboard rows built from Point stage never surfaced a branch code.
                "branch_code": "SIN-COD",
                "branch_name": row.get("branch_name") or "Sucursal",
                "units": current_units,
                "amount": current_amount,
                "tickets": current_tickets,
                "recipe_count": int(row.get("recipe_count") or 0),
                "status": status,
                "tone": tone,
                "detail": detail,
                "delta_pct": delta_pct,
                "rank_score": rank_score,
            }
        )
    severity_order = {"danger": 0, "warning": 1, "success": 2}
    rows.sort(key=lambda item: (severity_order.get(str(item.get("tone") or ""), 9), -float(item.get("rank_score") or 0), str(item.get("branch_code") or "")))
    return rows[:6]


def _build_product_rows(*, current_rows: list[dict[str, object]], compare_rows: list[dict[str, object]], compare_label: str, weekday_mode: bool) -> list[dict[str, object]]:
    compare_map = {str(row["product_key"]): row for row in compare_rows if row.get("product_key")}
    rows: list[dict[str, object]] = []
    for row in current_rows:
        product_key = str(row["product_key"])
        compare = compare_map.get(product_key)
        if not compare:
            continue
        current_amount = _to_decimal(row.get("amount"))
        current_units = _to_decimal(row.get("units"))
        compare_amount = _to_decimal(compare.get("amount"))
        compare_units = _to_decimal(compare.get("units"))
        delta_pct = None
        if compare_amount > 0:
            delta_pct = ((current_amount - compare_amount) / compare_amount) * Decimal("100")
        elif compare_units > 0:
            delta_pct = ((current_units - compare_units) / compare_units) * Decimal("100")
        if delta_pct is None:
            if weekday_mode:
                continue
            status, tone, detail, rank_score = "Sin comparativo", "warning", "No hay corte previo comparable para este producto.", Decimal("0")
        elif delta_pct <= (Decimal("-15") if weekday_mode else Decimal("-20")):
            status = "Abajo del comparable" if weekday_mode else "Caída fuerte"
            tone = "danger"
            detail = (
                f"Cae {abs(delta_pct):.1f}% contra el último mismo día de semana ({compare_label})."
                if weekday_mode
                else f"Cae {abs(delta_pct):.1f}% contra el corte previo."
            )
            rank_score = abs(delta_pct) + Decimal("100")
        elif delta_pct >= (Decimal("15") if weekday_mode else Decimal("20")):
            status = "Arriba del comparable" if weekday_mode else "Alza fuerte"
            tone = "success"
            detail = (
                f"Sube {delta_pct:.1f}% contra el último mismo día de semana ({compare_label})."
                if weekday_mode
                else f"Sube {delta_pct:.1f}% contra el corte previo."
            )
            rank_score = delta_pct
        else:
            status = "Dentro de rango" if weekday_mode else "Estable"
            tone = "warning"
            detail = (
                f"Variación de {delta_pct:.1f}% contra el último mismo día de semana ({compare_label})."
                if weekday_mode
                else f"Variación de {delta_pct:.1f}% contra el corte previo."
            )
            rank_score = abs(delta_pct)
        rows.append(
            {
                "product_key": product_key,
                "recipe_name": row.get("recipe_name") or row.get("product_name") or "Producto",
                "units": current_units,
                "amount": current_amount,
                "tickets": int(row.get("tickets") or 0),
                "branch_count": int(row.get("branch_count") or 0),
                "status": status,
                "tone": tone,
                "detail": detail,
                "delta_pct": delta_pct,
                "rank_score": rank_score,
            }
        )
    severity_order = {"danger": 0, "warning": 1, "success": 2}
    rows.sort(key=lambda item: (severity_order.get(str(item.get("tone") or ""), 9), -float(item.get("rank_score") or 0), str(item.get("recipe_name") or "")))
    return rows[:6]


def _fetch_materialized_dashboard_daily_ops_payload() -> dict[str, object] | None:
    sql = """
    SELECT
        latest_date,
        prev_date,
        comparable_date,
        first_date,
        last_date,
        total_rows,
        active_days,
        branch_count,
        recipe_count,
        total_units,
        total_amount,
        source_coverage_rows,
        top_branches,
        top_products,
        branch_latest_rows,
        branch_prev_rows,
        branch_weekday_rows,
        product_latest_rows,
        product_prev_rows,
        product_weekday_rows
    FROM mv_dashboard_daily_ops
    WHERE singleton_key = 1
    """
    with connection.cursor() as cursor:
        cursor.execute(sql)
        columns = [column[0] for column in cursor.description]
        row = cursor.fetchone()
    if not row:
        return None
    return dict(zip(columns, row))


def _empty_dashboard_daily_ops_result() -> dict[str, object]:
    return {
        "latest_date": None,
        "prev_date": None,
        "comparable_date": None,
        "sales_history_summary": None,
        "branch_daily_exception_rows": [],
        "branch_weekday_comparison_rows": [],
        "product_daily_exception_rows": [],
        "product_weekday_comparison_rows": [],
    }


def _build_dashboard_daily_ops_result(payload: dict[str, object]) -> dict[str, object]:
    latest_date = payload.get("latest_date")
    prev_date = payload.get("prev_date")
    comparable_date = payload.get("comparable_date")
    branch_latest_rows = _coerce_json(payload.get("branch_latest_rows"))
    product_latest_rows = _coerce_json(payload.get("product_latest_rows"))
    return {
        "latest_date": latest_date,
        "prev_date": prev_date,
        "comparable_date": comparable_date,
        "sales_history_summary": _build_sales_history_summary(payload),
        "branch_daily_exception_rows": _build_branch_rows(
            current_rows=branch_latest_rows,
            compare_rows=_coerce_json(payload.get("branch_prev_rows")),
            compare_label=prev_date.isoformat() if prev_date else "",
            weekday_mode=False,
        ),
        "branch_weekday_comparison_rows": _build_branch_rows(
            current_rows=branch_latest_rows,
            compare_rows=_coerce_json(payload.get("branch_weekday_rows")),
            compare_label=comparable_date.isoformat() if comparable_date else "",
            weekday_mode=True,
        ),
        "product_daily_exception_rows": _build_product_rows(
            current_rows=product_latest_rows,
            compare_rows=_coerce_json(payload.get("product_prev_rows")),
            compare_label=prev_date.isoformat() if prev_date else "",
            weekday_mode=False,
        ),
        "product_weekday_comparison_rows": _build_product_rows(
            current_rows=product_latest_rows,
            compare_rows=_coerce_json(payload.get("product_weekday_rows")),
            compare_label=comparable_date.isoformat() if comparable_date else "",
            weekday_mode=True,
        ),
    }


def get_dashboard_daily_ops_dataset(*, use_materialized: bool = True) -> dict[str, object]:
    if use_materialized:
        try:
            payload = _fetch_materialized_dashboard_daily_ops_payload()
        except Exception:
            payload = None
        if payload is not None:
            return _build_dashboard_daily_ops_result(payload)

    sql = """
    WITH official_max AS (
        SELECT MAX(sale_date) AS official_max_date
        FROM pos_bridge_daily_sales
        WHERE source_endpoint = '/Report/PrintReportes?idreporte=3'
    ),
    history_rows AS MATERIALIZED (
        SELECT
            ds.sale_date,
            ds.branch_id,
            ds.product_id,
            ds.receta_id,
            ds.quantity,
            ds.total_amount,
            ds.tickets
        FROM pos_bridge_daily_sales ds
        CROSS JOIN official_max om
        WHERE (
                (om.official_max_date IS NOT NULL AND ds.source_endpoint = '/Report/PrintReportes?idreporte=3')
             OR (om.official_max_date IS NOT NULL AND ds.source_endpoint = '/Report/VentasCategorias' AND ds.sale_date > om.official_max_date)
             OR (om.official_max_date IS NULL AND ds.source_endpoint = '/Report/VentasCategorias')
        )
    ),
    stage_latest AS (
        SELECT GREATEST(
            COALESCE((SELECT official_max_date FROM official_max), DATE '1900-01-01'),
            COALESCE((SELECT MAX(sale_date) FROM pos_bridge_daily_sales WHERE source_endpoint = '/Report/VentasCategorias'), DATE '1900-01-01')
        )::date AS latest_date
    ),
    available_dates AS (
        SELECT DISTINCT sale_date AS fecha
        FROM history_rows
        WHERE sale_date <= (SELECT latest_date FROM stage_latest)
    ),
    ordered_dates AS (
        SELECT
            fecha,
            LAG(fecha) OVER (ORDER BY fecha) AS prev_date
        FROM available_dates
    ),
    latest AS (
        SELECT
            fecha AS latest_date,
            prev_date
        FROM ordered_dates
        ORDER BY fecha DESC
        LIMIT 1
    ),
    comparable AS (
        SELECT fecha AS comparable_date
        FROM (
            SELECT
                fecha,
                ROW_NUMBER() OVER (ORDER BY fecha DESC) AS rn
            FROM available_dates
            WHERE fecha < (SELECT latest_date FROM latest)
              AND EXTRACT(ISODOW FROM fecha) = EXTRACT(ISODOW FROM (SELECT latest_date FROM latest))
        ) ranked
        WHERE rn = 1
    ),
    stage_history AS (
        SELECT
            MIN(hr.sale_date) AS first_date,
            MAX(hr.sale_date) AS last_date,
            COUNT(*) AS total_rows,
            COUNT(DISTINCT hr.sale_date) AS active_days,
            COUNT(DISTINCT hr.branch_id) AS branch_count,
            COUNT(DISTINCT hr.product_id) AS recipe_count,
            COALESCE(SUM(hr.quantity), 0) AS total_units,
            COALESCE(SUM(hr.total_amount), 0) AS total_amount
        FROM history_rows hr
    ),
    source_coverage AS (
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'source_kind', source_kind,
                    'qty', qty::text,
                    'amount', amount::text,
                    'row_count', row_count,
                    'coverage_days', coverage_days,
                    'coverage_branches', coverage_branches
                )
                ORDER BY source_kind
            ),
            '[]'::jsonb
        ) AS rows
        FROM (
            SELECT
                source_kind,
                COALESCE(SUM(cantidad), 0) AS qty,
                COALESCE(SUM(venta_total), 0) AS amount,
                COUNT(*) AS row_count,
                COUNT(DISTINCT fecha) AS coverage_days,
                COUNT(DISTINCT sucursal_id) FILTER (WHERE sucursal_id IS NOT NULL) AS coverage_branches
            FROM reportes_factventadiaria
            GROUP BY source_kind
        ) grouped
    ),
    history_top_branches AS (
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'branch_id', branch_id,
                    'branch_code', branch_code,
                    'branch_name', branch_name,
                    'total', total::text
                )
                ORDER BY total DESC, branch_code
            ),
            '[]'::jsonb
        ) AS rows
        FROM (
            SELECT
                pb.erp_branch_id AS branch_id,
                pb.external_id AS branch_code,
                pb.name AS branch_name,
                COALESCE(SUM(hr.quantity), 0) AS total
            FROM history_rows hr
            INNER JOIN pos_bridge_branches pb ON pb.id = hr.branch_id
            GROUP BY pb.erp_branch_id, pb.external_id, pb.name
            ORDER BY total DESC, pb.external_id
            LIMIT 4
        ) ranked
    ),
    history_top_products AS (
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'product_key', product_key,
                    'recipe_name', NULLIF(recipe_name, ''),
                    'product_name', product_name,
                    'total', total::text
                )
                ORDER BY total DESC, product_name
            ),
            '[]'::jsonb
        ) AS rows
        FROM (
            SELECT
                hr.product_id::text AS product_key,
                COALESCE(MAX(r.nombre), '') AS recipe_name,
                COALESCE(MAX(pp.name), 'Producto') AS product_name,
                COALESCE(SUM(hr.quantity), 0) AS total
            FROM history_rows hr
            INNER JOIN pos_bridge_products pp ON pp.id = hr.product_id
            LEFT JOIN recetas_receta r ON r.id = hr.receta_id
            GROUP BY hr.product_id
            ORDER BY total DESC, product_name
            LIMIT 5
        ) ranked
    ),
    target_dates AS (
        SELECT 'latest'::text AS target_kind, latest_date AS target_date FROM latest
        UNION ALL
        SELECT 'prev'::text, prev_date FROM latest WHERE prev_date IS NOT NULL
        UNION ALL
        SELECT 'weekday'::text, comparable_date FROM comparable WHERE comparable_date IS NOT NULL
    ),
    selected_source AS (
        SELECT
            t.target_kind,
            t.target_date,
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM pos_bridge_daily_sales ds
                    WHERE ds.sale_date = t.target_date
                      AND ds.source_endpoint = '/Report/PrintReportes?idreporte=3'
                )
                THEN '/Report/PrintReportes?idreporte=3'
                ELSE '/Report/VentasCategorias'
            END AS source_endpoint
        FROM target_dates t
    ),
    branch_fact AS (
        SELECT
            ss.target_kind,
            pb.erp_branch_id AS branch_id,
            pb.external_id AS branch_code,
            pb.name AS branch_name,
            COALESCE(SUM(ds.quantity), 0) AS units,
            COALESCE(SUM(ds.total_amount), 0) AS amount,
            COALESCE(SUM(ds.tickets), 0) AS fact_tickets,
            COUNT(DISTINCT ds.product_id) FILTER (WHERE ss.target_kind = 'latest' AND ds.product_id IS NOT NULL) AS recipe_count
        FROM selected_source ss
        INNER JOIN pos_bridge_daily_sales ds
            ON ds.sale_date = ss.target_date
           AND ds.source_endpoint = ss.source_endpoint
        INNER JOIN pos_bridge_branches pb ON pb.id = ds.branch_id
        GROUP BY ss.target_kind, pb.erp_branch_id, pb.external_id, pb.name
    ),
    branch_indicator AS (
        SELECT
            t.target_kind,
            pb.erp_branch_id AS branch_id,
            COALESCE(SUM(di.total_amount), 0) AS indicator_amount,
            COALESCE(SUM(di.total_tickets), 0) AS indicator_tickets
        FROM target_dates t
        INNER JOIN pos_bridge_daily_branch_indicators di ON di.indicator_date = t.target_date
        INNER JOIN pos_bridge_branches pb ON pb.id = di.branch_id
        WHERE pb.erp_branch_id IS NOT NULL
        GROUP BY t.target_kind, pb.erp_branch_id
    ),
    branch_payload AS (
        SELECT
            target_kind,
            COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'branch_id', branch_id,
                        'branch_code', branch_code,
                        'branch_name', branch_name,
                        'units', units::text,
                        'amount', amount::text,
                        'fact_tickets', fact_tickets,
                        'recipe_count', recipe_count,
                        'indicator_amount', indicator_amount::text,
                        'indicator_tickets', indicator_tickets
                    )
                    ORDER BY amount DESC, units DESC, branch_code
                ),
                '[]'::jsonb
            ) AS rows
        FROM (
            SELECT
                bf.target_kind,
                bf.branch_id,
                bf.branch_code,
                bf.branch_name,
                bf.units,
                bf.amount,
                bf.fact_tickets,
                bf.recipe_count,
                COALESCE(bi.indicator_amount, 0) AS indicator_amount,
                COALESCE(bi.indicator_tickets, 0) AS indicator_tickets
            FROM branch_fact bf
            LEFT JOIN branch_indicator bi
                ON bi.target_kind = bf.target_kind
               AND bi.branch_id = bf.branch_id
        ) joined_rows
        GROUP BY target_kind
    ),
    product_payload AS (
        SELECT
            target_kind,
            COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'product_key', product_key,
                        'recipe_name', NULLIF(recipe_name, ''),
                        'product_name', product_name,
                        'units', units::text,
                        'amount', amount::text,
                        'tickets', tickets,
                        'branch_count', branch_count
                    )
                    ORDER BY amount DESC, units DESC, product_name
                ),
                '[]'::jsonb
            ) AS rows
        FROM (
            SELECT
                ss.target_kind,
                ds.product_id::text AS product_key,
                COALESCE(MAX(r.nombre), '') AS recipe_name,
                COALESCE(MAX(pp.name), 'Producto') AS product_name,
                COALESCE(SUM(ds.quantity), 0) AS units,
                COALESCE(SUM(ds.total_amount), 0) AS amount,
                COALESCE(SUM(ds.tickets), 0) AS tickets,
                COUNT(DISTINCT ds.branch_id) AS branch_count
            FROM selected_source ss
            INNER JOIN pos_bridge_daily_sales ds
                ON ds.sale_date = ss.target_date
               AND ds.source_endpoint = ss.source_endpoint
            INNER JOIN pos_bridge_products pp ON pp.id = ds.product_id
            LEFT JOIN recetas_receta r ON r.id = ds.receta_id
            GROUP BY ss.target_kind, ds.product_id
        ) grouped
        GROUP BY target_kind
    )
    SELECT
        (SELECT latest_date FROM latest) AS latest_date,
        (SELECT prev_date FROM latest) AS prev_date,
        (SELECT comparable_date FROM comparable) AS comparable_date,
        stage_history.first_date,
        stage_history.last_date,
        stage_history.total_rows,
        stage_history.active_days,
        stage_history.branch_count,
        stage_history.recipe_count,
        stage_history.total_units,
        stage_history.total_amount,
        source_coverage.rows AS source_coverage_rows,
        history_top_branches.rows AS top_branches,
        history_top_products.rows AS top_products,
        COALESCE((SELECT rows FROM branch_payload WHERE target_kind = 'latest'), '[]'::jsonb) AS branch_latest_rows,
        COALESCE((SELECT rows FROM branch_payload WHERE target_kind = 'prev'), '[]'::jsonb) AS branch_prev_rows,
        COALESCE((SELECT rows FROM branch_payload WHERE target_kind = 'weekday'), '[]'::jsonb) AS branch_weekday_rows,
        COALESCE((SELECT rows FROM product_payload WHERE target_kind = 'latest'), '[]'::jsonb) AS product_latest_rows,
        COALESCE((SELECT rows FROM product_payload WHERE target_kind = 'prev'), '[]'::jsonb) AS product_prev_rows,
        COALESCE((SELECT rows FROM product_payload WHERE target_kind = 'weekday'), '[]'::jsonb) AS product_weekday_rows
    FROM stage_history, source_coverage, history_top_branches, history_top_products
    """
    with connection.cursor() as cursor:
        cursor.execute(sql)
        columns = [column[0] for column in cursor.description]
        row = cursor.fetchone()
    if not row:
        return _empty_dashboard_daily_ops_result()
    payload = dict(zip(columns, row))
    return _build_dashboard_daily_ops_result(payload)
