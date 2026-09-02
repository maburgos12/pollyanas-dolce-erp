"""Canonical read projection shared by API, reports and exports.

Persisted closure decimals predate source-availability metadata, so zero may be
either a measured zero or a database placeholder.  This module is intentionally
inside ``pos_bridge`` so API consumers do not depend on ``reportes`` views.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

from recetas.models import ProductoMonthClosureLine


POINT_STATUSES = {"COINCIDE", "POINT_MAYOR", "POINT_MENOR", "REVISAR_FUENTE"}
CONVERSION_PROJECTION_VALUES = {"DIRECTA", "EQUIVALENCIA", "PRESENTACION_DERIVADA"}
CONVERSION_ORIGIN_LABELS = {"MIXED": "Mixto", "UNRESOLVED": "Sin resolver"}


def metadata_values(value: object) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(str(item) for item in value)
    return (str(value),)


def decimal_value(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def point_status_label(status: str) -> str:
    return {
        "COINCIDE": "Coincide",
        "POINT_MAYOR": "Point mayor",
        "POINT_MENOR": "Point menor",
        "REVISAR_FUENTE": "Revisar fuente",
    }.get(status, "Revisar fuente")


def is_historical_closure(closure, *, lines=None) -> bool:
    metadata = dict(getattr(closure, "metadata", {}) or {})
    if metadata.get("historical_excel_import"):
        return True
    return any((line.metadata or {}).get("historical_excel") for line in (lines or []))


def project_product_closure_line(
    line: ProductoMonthClosureLine,
    *,
    historical_excel_import: bool = False,
) -> dict[str, object]:
    metadata = dict(line.metadata or {})
    issues = {str(issue) for issue in metadata.get("issues") or []}
    is_canonical = metadata.get("balance_contract") == "POINT_PRODUCT_BALANCE_V1"
    is_historical_excel = bool(historical_excel_import or metadata.get("historical_excel"))

    calculated_missing = is_canonical and "CALCULATED_CLOSING_MISSING" in issues
    sales_missing = is_canonical and (
        "SALES_SOURCE_MISSING" in issues or metadata.get("sales_source_available") is not True
    )
    conversion_missing = is_canonical and metadata.get("conversion_source_authoritative") is not True
    production_missing = is_canonical and metadata.get("production_source_authoritative") is not True
    waste_missing = is_canonical and metadata.get("waste_source_authoritative") is not True
    opening_missing = is_canonical and "OPENING_SNAPSHOT_MISSING" in issues
    closing_missing = is_canonical and "CLOSING_SNAPSHOT_MISSING" in issues
    opening_authoritative = not opening_missing and (
        not is_canonical or metadata.get("opening_source_authoritative") is True
    )
    sales_authoritative = not sales_missing and (
        not is_canonical or metadata.get("sales_source_authoritative") is True
    )
    production_authoritative = not production_missing
    waste_authoritative = not waste_missing
    conversion_authoritative = not conversion_missing
    closing_authoritative = not closing_missing and (
        not is_canonical or metadata.get("closing_source_authoritative") is True
    )
    calculated_authoritative = all(
        (
            opening_authoritative,
            sales_authoritative,
            production_authoritative,
            waste_authoritative,
            conversion_authoritative,
        )
    )
    calculated_missing = calculated_missing or not calculated_authoritative
    closing_missing = closing_missing or not closing_authoritative

    historical_count = None
    historical_difference = None
    if is_canonical:
        point_difference = decimal_value(metadata.get("point_difference"))
    elif is_historical_excel:
        if line.estado_auditoria == ProductoMonthClosureLine.AUDIT_STATUS_SIN_INVENTARIO_FISICO:
            point_difference = None
            closing_missing = True
        else:
            historical_count = Decimal(str(line.inventario_final_point_total))
            historical_difference = Decimal(str(line.diferencia_teorico_vs_point))
            point_difference = None
            closing_missing = True
            closing_authoritative = False
    elif line.estado_auditoria == ProductoMonthClosureLine.AUDIT_STATUS_SIN_INVENTARIO_FISICO:
        point_difference = None
        closing_missing = True
    else:
        point_difference = -Decimal(str(line.diferencia_teorico_vs_point))
    if calculated_missing or closing_missing:
        point_difference = None

    stored_conversion_values = metadata_values(metadata.get("conversion_origin"))
    exact_conversion_origins = metadata_values(metadata.get("conversion_origins"))
    origin_values = exact_conversion_origins or tuple(
        value for value in stored_conversion_values if value not in CONVERSION_PROJECTION_VALUES
    )
    conversion_origin = tuple(CONVERSION_ORIGIN_LABELS.get(value, value) for value in origin_values)
    projection_sources = tuple(
        dict.fromkeys(
            list(metadata_values(metadata.get("projection_sources")))
            + [value for value in stored_conversion_values if value in CONVERSION_PROJECTION_VALUES]
        )
    )
    scopes_missing = closing_missing or (
        is_canonical
        and (
            "CLOSING_SNAPSHOT_SCOPE_MISSING" in issues
            or metadata.get("point_final_scopes_available") is False
        )
    )

    point_status = (
        str(metadata.get("point_status") or "")
        if is_canonical
        else line.estado_auditoria
        if is_historical_excel
        else ""
    )
    legacy_review = line.estado_auditoria == ProductoMonthClosureLine.AUDIT_STATUS_REVISAR_CATALOGO
    if is_historical_excel:
        status_label = line.get_estado_auditoria_display()
    elif (
        issues
        or line.has_catalog_issue
        or legacy_review
        or (is_canonical and (not calculated_authoritative or not closing_authoritative))
    ):
        point_status = "REVISAR_FUENTE"
        status_label = point_status_label(point_status)
    else:
        if point_status not in POINT_STATUSES:
            if point_difference is None:
                point_status = "REVISAR_FUENTE"
            elif abs(point_difference) <= Decimal("0.01"):
                point_status = "COINCIDE"
            elif point_difference > 0:
                point_status = "POINT_MAYOR"
            else:
                point_status = "POINT_MENOR"
        status_label = point_status_label(point_status)

    return {
        "opening_point": None if not opening_authoritative else Decimal(str(line.inventario_inicial_teorico)),
        "production": None if not production_authoritative else Decimal(str(line.produccion_mes)),
        "sales_direct": None if not sales_authoritative else Decimal(str(line.venta_directa_enteros)),
        "sales_derived": None if not sales_authoritative else Decimal(str(line.venta_derivada_equivalente)),
        "sales_total": None if not sales_authoritative else Decimal(str(line.venta_total_equivalente)),
        "point_conversion_in": None if conversion_missing else decimal_value(metadata.get("point_conversion_in")),
        "point_conversion_out": None if conversion_missing else decimal_value(metadata.get("point_conversion_out")),
        "conversion_origin": conversion_origin,
        "conversion_origins": exact_conversion_origins,
        "projection_sources": projection_sources,
        "waste_total": None if not waste_authoritative else Decimal(str(line.merma_total_equivalente)),
        "calculated_closing": None if calculated_missing else Decimal(str(line.inventario_final_teorico)),
        "closing_point_cedis": None if scopes_missing else Decimal(str(line.inventario_final_point_cedis)),
        "closing_point_sucursales": None if scopes_missing else Decimal(str(line.inventario_final_point_sucursales)),
        "closing_point": None if closing_missing else Decimal(str(line.inventario_final_point_total)),
        "point_difference": point_difference,
        "point_status": point_status,
        "status_label": status_label,
        "is_historical_inventory": is_historical_excel,
        "historical_count": historical_count,
        "historical_difference": historical_difference,
        "source_issues": tuple(sorted(issues)),
        "source_authority": {
            "opening": opening_authoritative,
            "sales": sales_authoritative,
            "production": production_authoritative,
            "waste": waste_authoritative,
            "conversions": conversion_authoritative,
            "closing": closing_authoritative,
        },
    }


def sum_complete_values(rows: list[dict[str, object]], key: str) -> Decimal | None:
    values = [row.get(key) for row in rows]
    if not values or any(value is None for value in values):
        return None
    return sum((Decimal(str(value)) for value in values), Decimal("0"))
