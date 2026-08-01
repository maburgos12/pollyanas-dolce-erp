from __future__ import annotations

from decimal import Decimal
from io import BytesIO

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill


VINO = "8B2252"
DORADO = "C9A84C"
BLANCO = "FFFAF5"


def _value(value):
    if value is None:
        return "N/A"
    if isinstance(value, (tuple, list)):
        return " · ".join(str(part) for part in value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, (Decimal, int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str) and value.startswith(("=", "+", "-", "@")):
        return f"'{value}"
    return value


def _sheet(workbook, title, headers, rows):
    sheet = workbook.create_sheet(title)
    sheet.append(headers)
    for row in rows:
        sheet.append([_value(value) for value in row])
    for cell in sheet[1]:
        cell.fill = PatternFill("solid", fgColor=VINO)
        cell.font = Font(color=BLANCO, bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center")
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    for column in sheet.columns:
        values = [len(str(cell.value or "")) for cell in column]
        sheet.column_dimensions[column[0].column_letter].width = min(max(values + [10]) + 2, 42)
    return sheet


def build_indicadores_abasto_xlsx(report, filters):
    workbook = Workbook()
    workbook.remove(workbook.active)
    totals = report["totals"]
    summary_rows = [
        ["Periodo", f'{filters["fecha_desde"]} al {filters["fecha_hasta"]}', ""],
    ]
    if report["mezcla_unidades"]:
        for unit in report["por_unidad"]:
            summary_rows.extend(
                [
                    [f'Solicitado · {unit["unidad"]}', unit["solicitado"], unit["unidad"]],
                    [f'Enviado · {unit["unidad"]}', unit["enviado"], unit["unidad"]],
                    [f'Recibido · {unit["unidad"]}', unit["recibido"], unit["unidad"]],
                    [f'Cumplimiento total · {unit["unidad"]}', unit["porcentaje_total_evaluado"], "% evaluado"],
                ]
            )
    else:
        summary_rows.extend(
            [
                ["Solicitado", totals["solicitado"], filters.get("unidad") or (report["unidades"][0] if report["unidades"] else "")],
                ["Enviado", totals["enviado"], filters.get("unidad") or (report["unidades"][0] if report["unidades"] else "")],
                ["Recibido", totals["recibido"], filters.get("unidad") or (report["unidades"][0] if report["unidades"] else "")],
                ["Cumplimiento de abasto", totals["porcentaje_abasto"], "% Solicitado→Enviado"],
                ["Cumplimiento de entrega", totals["porcentaje_entrega"], "% Cargado/Enviado→Recibido"],
                ["Cumplimiento total evaluado", totals["porcentaje_total_evaluado"], "% Solicitado→Recibido"],
            ]
        )
    summary_rows.append(["Líneas pendientes", totals["pendientes"], "No se cuentan como incumplidas"])
    _sheet(
        workbook,
        "Resumen",
        ["Indicador", "Valor", "Unidad / criterio"],
        summary_rows,
    )
    group_headers = [
        "Clave", "Nombre", "Unidad", "Solicitado", "Enviado", "Recibido",
        "Brecha abasto", "Brecha entrega", "% abasto", "% entrega", "% total evaluado", "Pendientes",
    ]
    def group_values(group):
        return [
            group["clave"], group["etiqueta"], group["unidad"], group["solicitado"], group["enviado"],
            group["recibido"], group["brecha_abasto"], group["brecha_entrega"], group["porcentaje_abasto"],
            group["porcentaje_entrega"], group["porcentaje_total_evaluado"], group["pendientes"],
        ]
    _sheet(workbook, "Por sucursal", group_headers, [group_values(row) for row in report["por_sucursal"]])
    _sheet(workbook, "Por día", group_headers, [group_values(row) for row in report["por_dia"]])
    _sheet(workbook, "Por producto", group_headers, [group_values(row) for row in report["por_producto"]])
    detail_headers = [
        "Fecha", "Sucursal", "Código", "Producto / insumo", "Unidad", "Solicitado", "Enviado", "Cargado",
        "Recibido", "Brecha abasto", "Brecha entrega", "Ruta", "Repartidor", "Estado / causa", "Pendiente",
        "Transferencia Point", "Detalle Point", "Enviado por", "Recibido por", "Recibido en",
    ]
    detail_rows = [
        [
            row["fecha"], row["sucursal"], row["item_code"], row["item_name"], row["unidad"],
            row["solicitado"], row["enviado"], row["cargado"], row["recibido"], row["brecha_abasto"],
            row["brecha_entrega"], row["ruta_folio"], row["repartidor"], row["estado"],
            "Sí" if row["pendiente"] else "No", row["transfer_external_id"], row["detail_external_id"],
            row["sent_by"], row["received_by"], row["received_at"],
        ]
        for row in report["rows"]
    ]
    _sheet(workbook, "Detalle", detail_headers, detail_rows)
    stream = BytesIO()
    workbook.save(stream)
    return stream.getvalue()
