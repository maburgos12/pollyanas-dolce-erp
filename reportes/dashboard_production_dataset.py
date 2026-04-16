from __future__ import annotations

import json
from datetime import date, timedelta
from decimal import Decimal

from django.db import connection


def _coerce_json(value):
    if value is None:
        return []
    if isinstance(value, str):
        return json.loads(value)
    return value


def get_dashboard_production_dataset(*, latest_date: date, lookback_weeks: int = 4) -> dict[str, object]:
    current_week_start = latest_date - timedelta(days=latest_date.weekday())
    current_week_end = current_week_start + timedelta(days=6)
    range_start = current_week_start - timedelta(days=7 * max(lookback_weeks - 1, 0))
    sql = """
    WITH params AS (
        SELECT
            %(latest_date)s::date AS latest_date,
            %(current_week_start)s::date AS current_week_start,
            %(current_week_end)s::date AS current_week_end,
            %(range_start)s::date AS range_start
    ),
    official_max AS (
        SELECT MAX(sale_date) AS official_max_date
        FROM pos_bridge_daily_sales
        WHERE source_endpoint = '/Report/PrintReportes?idreporte=3'
    ),
    first_cedis AS (
        SELECT MIN(ppl.production_date) AS first_cedis_date
        FROM pos_bridge_production_lines ppl
        INNER JOIN pos_bridge_branches pb ON pb.id = ppl.branch_id
        WHERE NOT ppl.is_insumo
          AND pb.normalized_name = 'cedis'
    ),
    weekly_sales AS (
        SELECT
            date_trunc('week', ds.sale_date)::date AS week_start,
            COALESCE(SUM(ds.quantity), 0) AS sold_units
        FROM pos_bridge_daily_sales ds
        INNER JOIN pos_bridge_branches pb ON pb.id = ds.branch_id
        INNER JOIN core_sucursal s ON s.id = pb.erp_branch_id
        CROSS JOIN params p
        CROSS JOIN official_max om
        WHERE ds.sale_date BETWEEN p.range_start AND p.current_week_end
          AND s.activa
          AND ds.receta_id IS NOT NULL
          AND ds.total_amount > 0
          AND (
                (om.official_max_date IS NOT NULL AND ds.source_endpoint = '/Report/PrintReportes?idreporte=3' AND ds.sale_date <= om.official_max_date)
             OR (om.official_max_date IS NOT NULL AND ds.source_endpoint = '/Report/VentasCategorias' AND ds.sale_date > om.official_max_date)
             OR (om.official_max_date IS NULL AND ds.source_endpoint = '/Report/VentasCategorias')
          )
        GROUP BY date_trunc('week', ds.sale_date)::date
    ),
    current_week_sales_category AS (
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'category', category,
                    'family', family,
                    'item_name', item_name,
                    'units', units::text
                )
                ORDER BY units DESC, item_name
            ),
            '[]'::jsonb
        ) AS rows
        FROM (
            SELECT
                COALESCE(r.categoria, '') AS category,
                COALESCE(r.familia, '') AS family,
                COALESCE(MAX(pp.name), 'Producto') AS item_name,
                COALESCE(SUM(ds.quantity), 0) AS units
            FROM pos_bridge_daily_sales ds
            INNER JOIN pos_bridge_branches pb ON pb.id = ds.branch_id
            INNER JOIN core_sucursal s ON s.id = pb.erp_branch_id
            INNER JOIN pos_bridge_products pp ON pp.id = ds.product_id
            LEFT JOIN recetas_receta r ON r.id = ds.receta_id
            CROSS JOIN params p
            CROSS JOIN official_max om
            WHERE ds.sale_date BETWEEN p.current_week_start AND p.current_week_end
              AND ds.receta_id IS NOT NULL
              AND ds.total_amount > 0
              AND s.activa
              AND (
                    (om.official_max_date IS NOT NULL AND ds.source_endpoint = '/Report/PrintReportes?idreporte=3' AND ds.sale_date <= om.official_max_date)
                 OR (om.official_max_date IS NOT NULL AND ds.source_endpoint = '/Report/VentasCategorias' AND ds.sale_date > om.official_max_date)
                 OR (om.official_max_date IS NULL AND ds.source_endpoint = '/Report/VentasCategorias')
              )
            GROUP BY COALESCE(r.categoria, ''), COALESCE(r.familia, ''), ds.product_id
        ) grouped
    ),
    production_base AS (
        SELECT
            ppl.production_date,
            date_trunc('week', ppl.production_date)::date AS week_start,
            pb.normalized_name AS point_branch_name,
            COALESCE(s.nombre, pb.name) AS branch_name,
            COALESCE(r.categoria, '') AS category,
            COALESCE(r.familia, '') AS family,
            ppl.item_name,
            ppl.produced_quantity
        FROM pos_bridge_production_lines ppl
        INNER JOIN pos_bridge_branches pb ON pb.id = ppl.branch_id
        LEFT JOIN core_sucursal s ON s.id = ppl.erp_branch_id
        LEFT JOIN recetas_receta r ON r.id = ppl.receta_id
        CROSS JOIN params p
        WHERE ppl.production_date BETWEEN p.range_start AND p.current_week_end
          AND NOT ppl.is_insumo
    ),
    filtered_production AS (
        SELECT
            pb.*,
            CASE
                WHEN fc.first_cedis_date IS NOT NULL AND pb.production_date >= fc.first_cedis_date THEN 'cedis'
                ELSE 'matriz'
            END AS central_branch
        FROM production_base pb
        CROSS JOIN first_cedis fc
    ),
    weekly_production AS (
        SELECT
            week_start,
            COALESCE(SUM(produced_quantity), 0) AS produced_units
        FROM filtered_production
        WHERE point_branch_name = central_branch
        GROUP BY week_start
    ),
    current_week_production_category AS (
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'category', category,
                    'family', family,
                    'item_name', item_name,
                    'units', units::text,
                    'branch_name', branch_name,
                    'central_branch', central_branch
                )
                ORDER BY units DESC, item_name
            ),
            '[]'::jsonb
        ) AS rows
        FROM (
            SELECT
                category,
                family,
                item_name,
                branch_name,
                central_branch,
                COALESCE(SUM(produced_quantity), 0) AS units
            FROM filtered_production
            CROSS JOIN params p
            WHERE production_date BETWEEN p.current_week_start AND p.current_week_end
              AND point_branch_name = central_branch
            GROUP BY category, family, item_name, branch_name, central_branch
        ) grouped
    ),
    weekly_rows AS (
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'week_start', week_start,
                    'sold_units', sold_units::text,
                    'produced_units', produced_units::text
                )
                ORDER BY week_start
            ),
            '[]'::jsonb
        ) AS rows
        FROM (
            SELECT
                COALESCE(ws.week_start, wp.week_start) AS week_start,
                COALESCE(ws.sold_units, 0) AS sold_units,
                COALESCE(wp.produced_units, 0) AS produced_units
            FROM weekly_sales ws
            FULL OUTER JOIN weekly_production wp ON wp.week_start = ws.week_start
        ) combined
    )
    SELECT
        (SELECT rows FROM weekly_rows) AS weekly_rows,
        (SELECT rows FROM current_week_sales_category) AS sales_category_rows,
        (SELECT rows FROM current_week_production_category) AS production_category_rows
    """
    with connection.cursor() as cursor:
        cursor.execute(
            sql,
            {
                "latest_date": latest_date,
                "current_week_start": current_week_start,
                "current_week_end": current_week_end,
                "range_start": range_start,
            },
        )
        columns = [column[0] for column in cursor.description]
        row = cursor.fetchone()
    payload = dict(zip(columns, row or ([], [], [])))
    return {
        "weekly_rows": _coerce_json(payload.get("weekly_rows")),
        "sales_category_rows": _coerce_json(payload.get("sales_category_rows")),
        "production_category_rows": _coerce_json(payload.get("production_category_rows")),
        "current_week_start": current_week_start,
        "current_week_end": current_week_end,
    }
