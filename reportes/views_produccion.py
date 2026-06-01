from __future__ import annotations

import csv
import textwrap
from calendar import monthrange
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from io import BytesIO
from typing import Any

from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.exceptions import PermissionDenied
from django.db.models import Min, Sum
from django.db.models.functions import TruncMonth
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.utils import timezone
from django.views.generic import TemplateView
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

from control.models import MermaMensualSucursal
from core.access import can_view_reportes
from pos_bridge.models import PointProductionLine, PointSalesDailyProductFact
from recetas.models import ProductoMonthClosure, ProductoMonthClosureLine, Receta, RecetaEquivalencia
from recetas.utils.derived_product_presentations import get_total_cost_map
from reportes.models import FactProduccionDiaria
from reportes.services_production_sales import build_sales_map


ZERO = Decimal("0")

CATEGORY_ORDER = [
    "Pastel Mini",
    "Pastel Chico",
    "Pastel Mediano",
    "Pastel Grande",
    "Pastel Individual",
    "Individual",
    "Media Plancha",
    "Rosca",
    "Rebanada",
    "Pay Mediano",
    "Pay Grande",
    "Vaso Preparado Mini",
    "Vasos Mini",
    "Vasos Grande",
    "Vasos Preparados Grande",
    "Bollo",
    "Cheesecake",
    "Empanadas",
    "Galletas",
    "Otros postres",
    "Café",
]
CATEGORY_ORDER_INDEX = {name.lower(): index for index, name in enumerate(CATEGORY_ORDER)}

PRODUCTION_EXPORT_COLUMNS = [
    ("categoria", "Categoría", "text"),
    ("receta", "Receta", "text"),
    ("vendido", "Vendido", "number"),
    ("producido", "Producido", "number"),
    ("dif", "Dif. operativa", "number"),
    ("merma_reportada", "Merma", "number"),
    ("conversion_entrada", "Conv.", "number"),
    ("enteros_equivalentes", "Eq.", "number"),
    ("costo_merma", "Costo merma", "currency"),
    ("pct_merma", "% merma", "percent"),
    ("inventario_inicial", "Inv. inicial", "number"),
    ("inventario_final_teorico", "Inv. final teórico", "number"),
    ("inventario_final_point_total", "Inv. físico registrado", "number"),
    ("diferencia_inventario", "Inv. Δ", "number"),
    ("estado_inventario", "Estado", "text"),
]

PDF_EXPORT_COLUMNS = [
    ("vendido", "Vta."),
    ("producido", "Prod."),
    ("dif", "Dif."),
    ("merma_reportada", "Merma"),
    ("pct_merma", "%"),
    ("inventario_final_teorico", "Fin."),
    ("inventario_final_point_total", "Físico"),
    ("diferencia_inventario", "Inv. Δ"),
]


@dataclass(frozen=True)
class PeriodSelection:
    month_start: date
    month_end: date

    @property
    def value(self) -> str:
        return self.month_start.strftime("%Y-%m")

    @property
    def label(self) -> str:
        return self.month_start.strftime("%B %Y").title()


def _parse_period(raw_value: str | None) -> PeriodSelection:
    today = timezone.localdate()
    fallback = date(today.year, today.month, 1)
    if raw_value:
        try:
            year_raw, month_raw = raw_value.split("-", 1)
            selected = date(int(year_raw), int(month_raw), 1)
        except (TypeError, ValueError):
            selected = fallback
    else:
        selected = fallback
    last_day = monthrange(selected.year, selected.month)[1]
    return PeriodSelection(month_start=selected, month_end=date(selected.year, selected.month, last_day))


def _parse_int(raw_value: str | None) -> int | None:
    try:
        value = int(raw_value or 0)
    except (TypeError, ValueError):
        return None
    return value or None


def _decimal(value: Any) -> Decimal:
    if value is None:
        return ZERO
    return Decimal(str(value or 0))


def _aggregate_by_recipe(queryset, field_name: str, recipe_field: str = "receta_id") -> dict[int, Decimal]:
    rows = queryset.values(recipe_field).annotate(total=Sum(field_name))
    return {
        int(row[recipe_field]): _decimal(row["total"])
        for row in rows
        if row.get(recipe_field)
    }


def _category_label(value: str | None) -> str:
    return (value or "").strip() or "Sin categoría"


def _category_sort_key(value: str | None) -> tuple[int, str]:
    label = _category_label(value)
    return (CATEGORY_ORDER_INDEX.get(label.lower(), len(CATEGORY_ORDER)), label.lower())


def _is_production_reference_category(value: str | None) -> bool:
    return _category_label(value).lower() == "rebanada"


def _sum_or_none(rows: list[dict[str, Any]], key: str) -> Decimal | None:
    values = [row[key] for row in rows if row.get(key) is not None]
    if not values:
        return None
    return sum(values, ZERO)


def _inventory_status(*, theoretical: Decimal | None, physical: Decimal | None) -> str:
    if theoretical is None or physical is None:
        return ""
    difference = theoretical - physical
    if abs(difference) <= Decimal("0.01"):
        return "Cuadra"
    if difference < ZERO:
        return "Sobrante físico"
    return "Faltante no explicado"


def _first_available_date() -> dict[str, date | None]:
    return {
        "ventas": PointSalesDailyProductFact.objects.aggregate(min_date=Min("sale_date"))["min_date"],
        "produccion": FactProduccionDiaria.objects.aggregate(min_date=Min("fecha"))["min_date"]
        or PointProductionLine.objects.aggregate(min_date=Min("production_date"))["min_date"],
        "merma": MermaMensualSucursal.objects.aggregate(min_date=Min("periodo"))["min_date"],
        "cierre": ProductoMonthClosure.objects.aggregate(min_date=Min("month_start"))["min_date"],
    }


def _filename_period(context: dict[str, Any]) -> str:
    return str(context.get("selected_period") or timezone.localdate().strftime("%Y-%m"))


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _format_decimal(value: Any, *, places: int = 2, trim: bool = True) -> str:
    decimal_value = _decimal_or_none(value)
    if decimal_value is None:
        return ""
    formatted = f"{decimal_value:,.{places}f}"
    if trim and "." in formatted:
        formatted = formatted.rstrip("0").rstrip(".")
    return formatted


def _export_raw_value(row: dict[str, Any], key: str) -> Any:
    if key == "dif" and row.get("produccion_referencia"):
        return "Referencia"
    return row.get(key)


def _export_display_value(row: dict[str, Any], key: str, kind: str) -> str:
    value = _export_raw_value(row, key)
    if value is None or value == "":
        return ""
    if kind == "currency":
        return f"${_format_decimal(value, places=2, trim=False)}"
    if kind == "percent":
        return f"{_format_decimal(value, places=2, trim=False)}%"
    if kind == "number":
        if isinstance(value, str):
            return value
        return _format_decimal(value, places=2)
    return str(value)


def _export_rows(context: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for group in context.get("groups") or []:
        subtotal = dict(group.get("total") or {})
        subtotal.update(
            {
                "_row_type": "subtotal",
                "categoria": group.get("categoria") or group.get("familia") or "",
                "receta": "Subtotal",
                "estado_inventario": "Subtotal",
            }
        )
        rows.append(subtotal)
        for detail in group.get("rows") or []:
            row = dict(detail)
            row["_row_type"] = "detail"
            rows.append(row)

    total = dict(context.get("grand_total") or {})
    total.update(
        {
            "_row_type": "total",
            "categoria": "Gran total",
            "receta": "Total",
            "estado_inventario": "Total",
        }
    )
    rows.append(total)
    return rows


def _pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _pdf_bytes(*, title: str, lines: list[str]) -> bytes:
    page_width = 792
    page_height = 612
    line_height = 12
    max_lines = 43
    pages = [lines[index : index + max_lines] for index in range(0, len(lines), max_lines)] or [[]]
    font_id = 3 + (len(pages) * 2)
    page_ids = [3 + (index * 2) for index in range(len(pages))]

    objects: list[bytes] = [
        b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj",
        (
            "2 0 obj << /Type /Pages /Count {count} /Kids [{kids}] >> endobj".format(
                count=len(pages),
                kids=" ".join(f"{page_id} 0 R" for page_id in page_ids),
            )
        ).encode(),
    ]

    for index, page_lines in enumerate(pages):
        page_id = page_ids[index]
        content_id = page_id + 1
        content_parts = ["BT", "/F1 8 Tf", f"{line_height} TL", "36 560 Td"]
        page_title = title if index == 0 else f"{title} - pág. {index + 1}"
        for raw in [page_title, "", *page_lines]:
            content_parts.append(f"({_pdf_escape(raw)}) Tj")
            content_parts.append("T*")
        content_parts.append("ET")
        content = "\n".join(content_parts).encode("latin-1", errors="replace")
        objects.append(
            (
                f"{page_id} 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 {page_width} {page_height}] "
                f"/Resources << /Font << /F1 {font_id} 0 R >> >> /Contents {content_id} 0 R >> endobj"
            ).encode()
        )
        objects.append(
            b"%d 0 obj << /Length " % content_id
            + str(len(content)).encode()
            + b" >> stream\n"
            + content
            + b"\nendstream endobj"
        )

    objects.append(f"{font_id} 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj".encode())

    output = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for obj in objects:
        offsets.append(len(output))
        output.extend(obj)
        output.extend(b"\n")
    xref_pos = len(output)
    output.extend(f"xref\n0 {len(objects) + 1}\n".encode())
    output.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        output.extend(f"{offset:010d} 00000 n \n".encode())
    output.extend(
        f"trailer << /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF".encode()
    )
    return bytes(output)


class ProducidoVsVendidoMermaView(LoginRequiredMixin, TemplateView):
    template_name = "reportes/producido_vs_vendido.html"

    def dispatch(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        if request.user.is_authenticated and not can_view_reportes(request.user):
            raise PermissionDenied("No tienes permisos para ver Reportes.")
        return super().dispatch(request, *args, **kwargs)

    def get(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        context = self._build_context(request)
        if request.resolver_match and request.resolver_match.url_name == "producido_vs_vendido_data":
            return JsonResponse(
                {
                    "periodo": context["selected_period"],
                    "fuentes": context["fuentes"],
                    "rows": context["json_rows"],
                    "totals": context["grand_total"],
                }
            )
        export_format = (request.GET.get("export") or "").strip().lower()
        if export_format == "csv":
            return self._export_csv(context)
        if export_format == "xlsx":
            return self._export_xlsx(context)
        if export_format == "pdf":
            return self._export_pdf(context)
        return self.render_to_response(context)

    def _export_csv(self, context: dict[str, Any]) -> HttpResponse:
        period = _filename_period(context)
        response = HttpResponse(content_type="text/csv; charset=utf-8")
        response["Content-Disposition"] = f'attachment; filename="producido_vs_vendido_{period}.csv"'
        writer = csv.writer(response)
        writer.writerow([label for _, label, _ in PRODUCTION_EXPORT_COLUMNS])
        for row in _export_rows(context):
            writer.writerow(
                [
                    _export_display_value(row, key, kind)
                    for key, _, kind in PRODUCTION_EXPORT_COLUMNS
                ]
            )
        return response

    def _export_xlsx(self, context: dict[str, Any]) -> HttpResponse:
        period = _filename_period(context)
        workbook = Workbook()
        sheet = workbook.active
        sheet.title = "Producido vs Vendido"

        title_font = Font(bold=True, size=14, color="7B1A48")
        header_fill = PatternFill("solid", fgColor="F5E6ED")
        header_font = Font(bold=True, color="7B1A48")
        subtotal_fill = PatternFill("solid", fgColor="F5E6ED")
        total_fill = PatternFill("solid", fgColor="3D0A24")
        white_bold = Font(bold=True, color="FFFFFF")

        sheet["A1"] = f"Producido vs Vendido - {context.get('selected_period_label') or period}"
        sheet["A1"].font = title_font
        sheet["A2"] = (
            f"Ventas: {context['fuentes']['ventas']} | "
            f"Producción: {context['fuentes']['produccion']} | "
            f"Merma: {context['fuentes']['merma']} | "
            f"Inventario: {context['fuentes']['inventario']}"
        )
        sheet["A3"] = f"Categoría: {context.get('selected_categoria') or 'Todas'}"

        header_row = 5
        for col_idx, (_, label, _) in enumerate(PRODUCTION_EXPORT_COLUMNS, start=1):
            cell = sheet.cell(row=header_row, column=col_idx, value=label)
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

        for row_idx, row in enumerate(_export_rows(context), start=header_row + 1):
            for col_idx, (key, _, kind) in enumerate(PRODUCTION_EXPORT_COLUMNS, start=1):
                raw_value = _export_raw_value(row, key)
                cell = sheet.cell(row=row_idx, column=col_idx)
                if raw_value is None or raw_value == "":
                    cell.value = None
                elif kind == "currency":
                    cell.value = _decimal_or_none(raw_value)
                    cell.number_format = '$#,##0.00'
                elif kind == "percent":
                    decimal_value = _decimal_or_none(raw_value)
                    cell.value = decimal_value / Decimal("100") if decimal_value is not None else None
                    cell.number_format = '0.00%'
                elif kind == "number" and not isinstance(raw_value, str):
                    cell.value = _decimal_or_none(raw_value)
                    cell.number_format = '#,##0.##'
                else:
                    cell.value = str(raw_value)
                cell.alignment = Alignment(
                    horizontal="left" if kind == "text" else "right",
                    vertical="center",
                    wrap_text=True,
                )

            if row.get("_row_type") == "subtotal":
                for cell in sheet[row_idx]:
                    cell.fill = subtotal_fill
                    cell.font = Font(bold=True, color="7B1A48")
            elif row.get("_row_type") == "total":
                for cell in sheet[row_idx]:
                    cell.fill = total_fill
                    cell.font = white_bold

        sheet.freeze_panes = "A6"
        widths = {
            "A": 22,
            "B": 34,
            "I": 15,
            "L": 16,
            "M": 18,
            "O": 20,
        }
        for col_idx in range(1, len(PRODUCTION_EXPORT_COLUMNS) + 1):
            letter = get_column_letter(col_idx)
            sheet.column_dimensions[letter].width = widths.get(letter, 12)

        buffer = BytesIO()
        workbook.save(buffer)
        buffer.seek(0)
        response = HttpResponse(
            buffer.getvalue(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        response["Content-Disposition"] = f'attachment; filename="producido_vs_vendido_{period}.xlsx"'
        return response

    def _export_pdf(self, context: dict[str, Any]) -> HttpResponse:
        period = _filename_period(context)
        lines = [
            f"Periodo: {context.get('selected_period_label') or period}",
            f"Categoria: {context.get('selected_categoria') or 'Todas'}",
            (
                f"Fuentes - Ventas: {context['fuentes']['ventas']} | "
                f"Produccion: {context['fuentes']['produccion']} | "
                f"Merma: {context['fuentes']['merma']} | "
                f"Inventario: {context['fuentes']['inventario']}"
            ),
            "",
        ]
        for row in _export_rows(context):
            prefix = "TOTAL" if row.get("_row_type") == "total" else "SUBTOTAL" if row.get("_row_type") == "subtotal" else "RECETA"
            name_line = f"{prefix}: {row.get('categoria') or ''} - {row.get('receta') or ''}".strip()
            lines.extend(textwrap.wrap(name_line, width=118) or [""])
            metrics = " | ".join(
                f"{label} {_export_display_value(row, key, 'percent' if key == 'pct_merma' else 'number') or '-'}"
                for key, label in PDF_EXPORT_COLUMNS
            )
            status = _export_display_value(row, "estado_inventario", "text") or "-"
            lines.extend(textwrap.wrap(f"{metrics} | Estado {status}", width=118, subsequent_indent="  "))
            costo = _export_display_value(row, "costo_merma", "currency") or "$0.00"
            lines.append(f"Costo merma: {costo}")
            lines.append("")

        pdf = _pdf_bytes(title="Producido vs Vendido", lines=lines)
        response = HttpResponse(pdf, content_type="application/pdf")
        response["Content-Disposition"] = f'attachment; filename="producido_vs_vendido_{period}.pdf"'
        return response

    def _build_context(self, request: HttpRequest) -> dict[str, Any]:
        period = _parse_period(request.GET.get("periodo") or request.GET.get("period"))
        sucursal_id = None  # Producción es centralizada en CEDIS; no aplica filtro por sucursal
        categoria = (request.GET.get("categoria") or request.GET.get("familia") or "").strip()

        sales_map, sales_source = self._sales_map(period, sucursal_id)
        recipe_ids = set(sales_map)
        production_map, production_source = self._production_map(period, sucursal_id)
        merma_map, merma_cost_map, merma_source = self._merma_maps(period, sucursal_id)
        closure_map = self._closure_map(period)
        recipe_ids.update(recipe_id for recipe_id, value in production_map.items() if value)

        if not sucursal_id and not recipe_ids:
            recipe_ids.update(closure_map["inventario"])
        recipe_ids.update(recipe_id for recipe_id, value in merma_map.items() if value)
        conversion_map = self._conversion_map(sales_map, merma_map)
        recipe_ids.update(
            recipe_id
            for recipe_id, value in conversion_map.items()
            if value.get("conversion_entrada") or value.get("conversion_salida")
        )
        recipe_ids.update(closure_map["inventario"])
        missing_slice_equivalences = self._missing_slice_equivalences(sales_map, merma_map)

        recipes_qs = Receta.objects.filter(
            id__in=recipe_ids,
            tipo=Receta.TIPO_PRODUCTO_FINAL,
        )
        if categoria:
            recipes_qs = recipes_qs.filter(categoria=categoria)
        recipes = sorted(
            list(recipes_qs),
            key=lambda recipe: (_category_sort_key(recipe.categoria), recipe.nombre.lower()),
        )

        cost_map = get_total_cost_map([recipe.id for recipe in recipes])
        rows = [self._build_row(recipe, sales_map, production_map, merma_map, merma_cost_map, cost_map) for recipe in recipes]
        for row in rows:
            conv = conversion_map.get(row["receta_id"]) or {}
            row["convertido"] = conv.get("convertido", ZERO)
            row["enteros_equivalentes"] = conv.get("enteros_equivalentes", ZERO)
            row["conversion_entrada"] = conv.get("conversion_entrada", ZERO)
            row["conversion_salida"] = conv.get("conversion_salida", ZERO)
            row["conversion_factor"] = conv.get("factor", ZERO)
            inventory = closure_map["inventario"].get(row["receta_id"]) or {}
            if row["produccion_referencia"]:
                row["inventario_inicial"] = None
                row["inventario_final_point_total"] = None
                row["inventario_final_teorico"] = None
                row["diferencia_inventario"] = None
                row["estado_inventario"] = "Referencia"
            else:
                row["inventario_inicial"] = inventory.get("inventario_inicial")
                row["inventario_final_point_total"] = inventory.get("inventario_final_point_total")
                row["inventario_final_teorico"] = self._theoretical_inventory(row)
                row["diferencia_inventario"] = (
                    row["inventario_final_teorico"] - row["inventario_final_point_total"]
                    if row["inventario_final_teorico"] is not None and row["inventario_final_point_total"] is not None
                    else None
                )
                row["estado_inventario"] = _inventory_status(
                    theoretical=row["inventario_final_teorico"],
                    physical=row["inventario_final_point_total"],
                )
            row["json"].update(
                {
                    "convertido": str(row["convertido"]),
                    "enteros_equivalentes": str(row["enteros_equivalentes"]),
                    "conversion_entrada": str(row["conversion_entrada"]),
                    "conversion_salida": str(row["conversion_salida"]),
                    "conversion_factor": str(row["conversion_factor"]),
                    "inventario_inicial": str(row["inventario_inicial"] or ""),
                    "inventario_final_teorico": str(row["inventario_final_teorico"] or ""),
                    "inventario_final_point_total": str(row["inventario_final_point_total"] or ""),
                    "diferencia_inventario": str(row["diferencia_inventario"] or ""),
                    "estado_inventario": row["estado_inventario"],
                }
            )
        groups, grand_total = self._group_rows(rows)

        source_dates = _first_available_date()
        banners = self._banners(
            sales_source=sales_source,
            production_source=production_source,
            merma_source=merma_source,
            source_dates=source_dates,
        )
        if missing_slice_equivalences:
            names = ", ".join(item["receta"] for item in missing_slice_equivalences[:8])
            extra = "" if len(missing_slice_equivalences) <= 8 else f" y {len(missing_slice_equivalences) - 8} más"
            banners.append(
                "Conversiones: hay recetas de rebanada con venta o merma sin equivalencia activa: "
                f"{names}{extra}. Configurar RecetaEquivalencia para convertirlas a enteros."
            )
        periodos_qs = (
            FactProduccionDiaria.objects.annotate(mes=TruncMonth("fecha"))
            .values_list("mes", flat=True)
            .distinct()
            .order_by("-mes")
        )
        periodos = [d.strftime("%Y-%m") for d in periodos_qs if d]
        current = date.today().strftime("%Y-%m")
        if current not in periodos:
            periodos.insert(0, current)

        return {
            "module_tabs": self._module_tabs("producido_vs_vendido"),
            "selected_period": period.value,
            "selected_period_label": period.label,
            "periodos": periodos,
            "selected_categoria": categoria,
            "selected_familia": categoria,
            "categorias": self._categories(),
            "familias": self._categories(),
            "groups": groups,
            "grand_total": grand_total,
            "conversion_map": conversion_map,
            "missing_slice_equivalences": missing_slice_equivalences,
            "json_rows": [row["json"] for row in rows],
            "fuentes": {
                "ventas": sales_source,
                "produccion": production_source,
                "merma": merma_source,
                "inventario": closure_map["source"],
            },
            "banners": banners,
            "source_dates": source_dates,
        }

    def _conversion_map(
        self,
        sales_map: dict[int, Decimal],
        merma_map: dict[int, Decimal] | None = None,
    ) -> dict[int, dict[str, Decimal]]:
        slice_activity = defaultdict(Decimal)
        for receta_id, value in sales_map.items():
            if value:
                slice_activity[int(receta_id)] += _decimal(value)
        for receta_id, value in (merma_map or {}).items():
            if value:
                slice_activity[int(receta_id)] += _decimal(value)

        equivalences = {
            equivalence.receta_porcion_id: equivalence
            for equivalence in RecetaEquivalencia.objects.filter(
                receta_porcion_id__in=slice_activity.keys(),
                tipo_relacion="CONVERSION",
                activo=True,
            )
        }
        result = {}
        for receta_id, unidades_rebanada in slice_activity.items():
            equivalence = equivalences.get(receta_id)
            if not equivalence:
                continue
            convertido = _decimal(unidades_rebanada)
            if not convertido:
                continue
            factor = _decimal(equivalence.factor_conversion if equivalence else None) or Decimal("1")
            enteros = (convertido / factor).quantize(Decimal("0.01")) if factor else ZERO
            portion_data = result.setdefault(
                int(receta_id),
                {
                    "convertido": ZERO,
                    "enteros_equivalentes": ZERO,
                    "conversion_entrada": ZERO,
                    "conversion_salida": ZERO,
                    "factor": factor,
                },
            )
            portion_data["convertido"] += convertido
            portion_data["enteros_equivalentes"] += enteros
            portion_data["conversion_entrada"] += convertido
            portion_data["factor"] = factor

            if equivalence and equivalence.receta_padre_id:
                parent_data = result.setdefault(
                    int(equivalence.receta_padre_id),
                    {
                        "convertido": ZERO,
                        "enteros_equivalentes": ZERO,
                        "conversion_entrada": ZERO,
                        "conversion_salida": ZERO,
                        "factor": factor,
                    },
                )
                parent_data["conversion_salida"] += enteros
                parent_data["factor"] = factor
        return result

    def _missing_slice_equivalences(
        self,
        sales_map: dict[int, Decimal],
        merma_map: dict[int, Decimal] | None = None,
    ) -> list[dict[str, Any]]:
        slice_activity = defaultdict(Decimal)
        for receta_id, value in sales_map.items():
            if value:
                slice_activity[int(receta_id)] += _decimal(value)
        for receta_id, value in (merma_map or {}).items():
            if value:
                slice_activity[int(receta_id)] += _decimal(value)

        slice_recipes = Receta.objects.filter(
            id__in=slice_activity.keys(),
            categoria__iexact="Rebanada",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
        ).order_by("nombre")
        active_equivalence_ids = set(
            RecetaEquivalencia.objects.filter(
                receta_porcion_id__in=[recipe.id for recipe in slice_recipes],
                activo=True,
            ).values_list("receta_porcion_id", flat=True)
        )
        missing = []
        for recipe in slice_recipes:
            if recipe.id in active_equivalence_ids:
                continue
            unidades_rebanada = _decimal(slice_activity.get(recipe.id))
            if not unidades_rebanada:
                continue
            missing.append(
                {
                    "receta_id": recipe.id,
                    "receta": recipe.nombre,
                    "vendido": _decimal(sales_map.get(recipe.id)),
                    "merma_reportada": _decimal((merma_map or {}).get(recipe.id)),
                    "unidades_rebanada": unidades_rebanada,
                }
            )
        return missing

    def _sales_map(self, period: PeriodSelection, sucursal_id: int | None) -> tuple[dict[int, Decimal], str]:
        return build_sales_map(period, sucursal_id)

    def _production_map(self, period: PeriodSelection, sucursal_id: int | None) -> tuple[dict[int, Decimal], str]:
        facts = FactProduccionDiaria.objects.filter(
            fecha__gte=period.month_start,
            fecha__lte=period.month_end,
            receta_id__isnull=False,
        )
        if sucursal_id:
            facts = facts.filter(sucursal_id=sucursal_id)
        if facts.exists():
            return _aggregate_by_recipe(facts, "producido"), "FactProduccionDiaria"

        point_rows = PointProductionLine.objects.filter(
            production_date__gte=period.month_start,
            production_date__lte=period.month_end,
            receta_id__isnull=False,
            is_insumo=False,
        )
        if sucursal_id:
            point_rows = point_rows.filter(erp_branch_id=sucursal_id)
        if point_rows.exists():
            return _aggregate_by_recipe(point_rows, "produced_quantity"), "PointProductionLine"
        return {}, "sin_datos"

    def _merma_maps(
        self,
        period: PeriodSelection,
        sucursal_id: int | None,
    ) -> tuple[dict[int, Decimal], dict[int, Decimal], str]:
        fact_rows = FactProduccionDiaria.objects.filter(
            fecha__gte=period.month_start,
            fecha__lte=period.month_end,
            receta_id__isnull=False,
            merma__gt=0,
        )
        if sucursal_id:
            fact_rows = fact_rows.filter(sucursal_id=sucursal_id)
        if fact_rows.exists():
            return _aggregate_by_recipe(fact_rows, "merma"), {}, "FactProduccionDiaria"

        rows = MermaMensualSucursal.objects.filter(periodo=period.month_start, receta_id__isnull=False)
        if sucursal_id:
            rows = rows.filter(sucursal_id=sucursal_id)
        if not rows.exists():
            return {}, {}, "sin_datos"
        return (
            _aggregate_by_recipe(rows, "unidades_merma"),
            _aggregate_by_recipe(rows, "costo_merma"),
            "MermaMensualSucursal",
        )

    def _closure_map(self, period: PeriodSelection) -> dict[str, Any]:
        closure = ProductoMonthClosure.objects.filter(month_start=period.month_start).first()
        if closure is None:
            return {"inventario": {}, "source": "sin_cierre"}
        lines = ProductoMonthClosureLine.objects.filter(closure=closure)
        inventory_map = {}
        for line in lines:
            inventory_map[line.receta_padre_id] = {
                "inventario_inicial": _decimal(line.inventario_inicial_teorico),
                "inventario_final_point_total": _decimal(line.inventario_final_point_total),
            }
        return {"inventario": inventory_map, "source": "ProductoMonthClosureLine"}

    def _build_row(
        self,
        recipe: Receta,
        sales_map: dict[int, Decimal],
        production_map: dict[int, Decimal],
        merma_map: dict[int, Decimal],
        merma_cost_map: dict[int, Decimal],
        cost_map: dict[int, Decimal],
    ) -> dict[str, Any]:
        vendido = sales_map.get(recipe.id)
        producido = production_map.get(recipe.id)
        merma_reportada = merma_map.get(recipe.id)
        categoria = _category_label(recipe.categoria)
        produccion_referencia = not bool(recipe.pasa_modulo_produccion)
        dif = None
        dif_referencia = None
        if producido is not None and vendido is not None:
            raw_dif = producido - vendido
            if produccion_referencia:
                dif_referencia = raw_dif
            else:
                dif = raw_dif
        costo_unitario = cost_map.get(recipe.id, ZERO)
        costo_merma = merma_cost_map.get(recipe.id)
        if costo_merma is None and merma_reportada is not None and costo_unitario:
            costo_merma = merma_reportada * costo_unitario
        pct_merma = None
        if merma_reportada is not None and vendido and vendido > ZERO:
            pct_merma = (merma_reportada / vendido) * Decimal("100")
        row = {
            "receta_id": recipe.id,
            "receta": recipe.nombre,
            "categoria": categoria,
            "familia": categoria,
            "vendido": vendido,
            "producido": producido,
            "dif": dif,
            "dif_referencia": dif_referencia,
            "produccion_referencia": produccion_referencia,
            "merma_reportada": merma_reportada,
            "costo_merma": costo_merma,
            "pct_merma": pct_merma,
        }
        row["json"] = {
            key: (str(value) if isinstance(value, Decimal) else value)
            for key, value in row.items()
            if key != "json"
        }
        return row

    def _theoretical_inventory(self, row: dict[str, Any]) -> Decimal | None:
        if row["inventario_inicial"] is None:
            return None
        return (
            row["inventario_inicial"]
            + (row["producido"] or ZERO)
            + (row["conversion_entrada"] or ZERO)
            - (row["vendido"] or ZERO)
            - (row["merma_reportada"] or ZERO)
            - (row["conversion_salida"] or ZERO)
        )

    def _group_rows(self, rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[row["categoria"]].append(row)

        groups = []
        grand_rows: list[dict[str, Any]] = []
        for category in sorted(grouped, key=_category_sort_key):
            family_rows = grouped[category]
            grand_rows.extend(family_rows)
            groups.append(
                {
                    "categoria": category,
                    "familia": category,
                    "rows": family_rows,
                    "total": self._totals(family_rows),
                }
            )
        return groups, self._totals(grand_rows)

    def _totals(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        totals = {
            "vendido": sum((row["vendido"] for row in rows if row["vendido"] is not None), ZERO),
            "producido": sum((row["producido"] for row in rows if row["producido"] is not None), ZERO),
            "dif": sum((row["dif"] for row in rows if row["dif"] is not None), ZERO),
            "merma_reportada": sum((row["merma_reportada"] for row in rows if row["merma_reportada"] is not None), ZERO),
            "costo_merma": sum((row["costo_merma"] for row in rows if row["costo_merma"] is not None), ZERO),
            "convertido": sum((row["convertido"] for row in rows if row["convertido"] is not None), ZERO),
            "enteros_equivalentes": sum(
                (row["enteros_equivalentes"] for row in rows if row["enteros_equivalentes"] is not None),
                ZERO,
            ),
            "conversion_entrada": sum((row["conversion_entrada"] for row in rows if row["conversion_entrada"] is not None), ZERO),
            "conversion_salida": sum((row["conversion_salida"] for row in rows if row["conversion_salida"] is not None), ZERO),
            "inventario_inicial": _sum_or_none(rows, "inventario_inicial"),
            "inventario_final_teorico": _sum_or_none(rows, "inventario_final_teorico"),
            "inventario_final_point_total": _sum_or_none(rows, "inventario_final_point_total"),
            "diferencia_inventario": _sum_or_none(rows, "diferencia_inventario"),
            "produccion_referencia": bool(rows) and all(row.get("produccion_referencia") for row in rows),
            "dif_referencia": sum((row["dif_referencia"] for row in rows if row.get("dif_referencia") is not None), ZERO),
        }
        totals["pct_merma"] = (
            (totals["merma_reportada"] / totals["vendido"]) * Decimal("100")
            if totals["vendido"]
            else None
        )
        return totals

    def _banners(
        self,
        *,
        sales_source: str,
        production_source: str,
        merma_source: str,
        source_dates: dict[str, date | None],
    ) -> list[str]:
        banners = []
        if sales_source == "ProductoMonthClosureLine":
            banners.append("Ventas: sin registros diarios para este periodo; se usó cierre mensual consolidado.")
        elif sales_source == "sin_datos":
            banners.append("Ventas: sin registros para este periodo.")
        if production_source == "ProductoMonthClosureLine":
            banners.append("Producción: sin registros diarios para este periodo; se usó cierre mensual consolidado.")
        elif production_source == "sin_datos":
            banners.append("Producción: sin registros para este periodo.")
        if merma_source == "sin_datos":
            banners.append("Merma: sin registros consolidados para este periodo.")
        return [banner for banner in banners if banner]

    def _categories(self) -> list[str]:
        values = (
            Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL)
            .exclude(categoria="")
            .values_list("categoria", flat=True)
            .distinct()
        )
        return sorted({_category_label(value) for value in values}, key=_category_sort_key)

    def _families(self) -> list[str]:
        return self._categories()

    def _module_tabs(self, active: str) -> list[dict[str, str | bool]]:
        tabs = [
            ("ventas", "/reportes/ventas/", "Ventas"),
            ("cierre_operativo", "/reportes/cierre-operativo/", "Cierre diario"),
            ("cierre_producto", "/reportes/cierre-producto/", "Cierre producto"),
            ("producido_vs_vendido", "/reportes/produccion/", "Producido vs Vendido"),
            ("financiero", "/reportes/financiero/", "Financiero"),
            ("mermas_devoluciones", "/reportes/mermas-devoluciones/", "Mermas y Devoluciones"),
            ("auditoria_insumos", "/reportes/auditoria-insumos/", "Auditoría Insumos"),
            ("proyeccion_produccion", "/reportes/proyeccion-produccion/", "Proyección Producción"),
            ("bi", "/reportes/bi/", "BI"),
        ]
        return [
            {"key": key, "url": url, "label": label, "active": key == active}
            for key, url, label in tabs
        ]
