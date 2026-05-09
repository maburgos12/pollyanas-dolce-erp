from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone as datetime_timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from django.conf import settings
from django.db import connection
from django.db.models import Min, Max
from django.utils import timezone
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

from core.models import Sucursal, sucursales_operativas
from pos_bridge.models import PointBranch, PointInventorySnapshot


ZERO = Decimal("0.000")


class DailyInventoryCloseService:
    """Builds the end-of-day Point inventory matrix without duplicating snapshots."""

    def __init__(self, *, timezone_name: str | None = None):
        self.timezone_name = timezone_name or getattr(settings, "TIME_ZONE", "America/Mazatlan")
        self.local_tz = ZoneInfo(self.timezone_name)

    def _day_bounds_utc(self, fecha_operacion: date) -> tuple[datetime, datetime]:
        start_local = datetime.combine(fecha_operacion, time.min, tzinfo=self.local_tz)
        end_local = start_local + timedelta(days=1)
        return start_local.astimezone(datetime_timezone.utc), end_local.astimezone(datetime_timezone.utc)

    def _target_branches(self, fecha_operacion: date) -> list[Sucursal]:
        branches = list(sucursales_operativas(fecha_operacion).order_by("codigo", "nombre"))
        cedis = Sucursal.objects.filter(codigo="CEDIS").first()
        if cedis and all(branch.pk != cedis.pk for branch in branches):
            branches.append(cedis)
        return branches

    def build_close(self, *, fecha_operacion: date) -> dict:
        target_branches = self._target_branches(fecha_operacion)
        target_codes = [branch.codigo for branch in target_branches]
        point_branches = list(
            PointBranch.objects.filter(erp_branch_id__in=[branch.id for branch in target_branches]).select_related(
                "erp_branch"
            )
        )
        start_utc, end_utc = self._day_bounds_utc(fecha_operacion)
        snapshots = PointInventorySnapshot.objects.filter(
            branch_id__in=[branch.id for branch in point_branches],
            captured_at__gte=start_utc,
            captured_at__lt=end_utc,
        ).select_related("branch", "branch__erp_branch", "product")
        if connection.vendor == "postgresql":
            snapshots = snapshots.order_by("branch__erp_branch_id", "product_id", "-captured_at", "-id").distinct(
                "branch__erp_branch_id",
                "product_id",
            )
        else:
            snapshots = snapshots.order_by("branch__erp_branch__codigo", "product__name", "captured_at", "id")

        latest_by_branch_product: dict[tuple[str, int], PointInventorySnapshot] = {}
        branch_codes_with_capture: set[str] = set()
        for snapshot in snapshots:
            if not snapshot.branch.erp_branch_id:
                continue
            code = snapshot.branch.erp_branch.codigo
            if code not in target_codes:
                continue
            latest_by_branch_product[(code, snapshot.product_id)] = snapshot
            branch_codes_with_capture.add(code)

        product_map: dict[int, dict] = {}
        for (branch_code, product_id), snapshot in latest_by_branch_product.items():
            row = product_map.setdefault(
                product_id,
                {
                    "product_id": product_id,
                    "sku": snapshot.product.sku or snapshot.product.external_id,
                    "product_name": snapshot.product.name,
                    "category": snapshot.product.category,
                    "stocks": {code: ZERO for code in target_codes},
                    "captured_at_by_branch": {},
                    "total_stock": ZERO,
                },
            )
            stock = Decimal(str(snapshot.stock or 0)).quantize(ZERO)
            row["stocks"][branch_code] = stock
            row["captured_at_by_branch"][branch_code] = timezone.localtime(snapshot.captured_at, self.local_tz)

        rows = sorted(product_map.values(), key=lambda item: (item["product_name"] or "", item["sku"] or ""))
        for row in rows:
            row["total_stock"] = sum((row["stocks"].get(code, ZERO) for code in target_codes), ZERO).quantize(ZERO)

        capture_range = PointInventorySnapshot.objects.filter(
            id__in=[snapshot.id for snapshot in latest_by_branch_product.values()]
        ).aggregate(first=Min("captured_at"), last=Max("captured_at"))
        first_capture = capture_range["first"]
        last_capture = capture_range["last"]
        return {
            "fecha_operacion": fecha_operacion,
            "timezone_name": self.timezone_name,
            "branches": [{"code": branch.codigo, "name": branch.nombre} for branch in target_branches],
            "rows": rows,
            "missing_branch_codes": [code for code in target_codes if code not in branch_codes_with_capture],
            "first_capture_at": timezone.localtime(first_capture, self.local_tz) if first_capture else None,
            "last_capture_at": timezone.localtime(last_capture, self.local_tz) if last_capture else None,
            "generated_at": timezone.localtime(timezone.now(), self.local_tz),
        }

    def build_workbook(self, payload: dict) -> Workbook:
        wb = Workbook()
        ws = wb.active
        ws.title = "Inventario final"
        branch_codes = [branch["code"] for branch in payload["branches"]]
        max_col = 3 + len(branch_codes) + 1

        ws.cell(row=1, column=1, value="Inventario final al cierre")
        ws.cell(row=1, column=1).font = Font(bold=True, size=14)
        ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=max_col)
        ws.cell(row=2, column=1, value="Fecha operativa")
        ws.cell(row=2, column=2, value=payload["fecha_operacion"].isoformat())
        ws.cell(row=3, column=1, value="Zona horaria")
        ws.cell(row=3, column=2, value=payload["timezone_name"])
        ws.cell(row=4, column=1, value="Ultima captura Point")
        ws.cell(row=4, column=2, value=payload["last_capture_at"].strftime("%Y-%m-%d %H:%M") if payload["last_capture_at"] else "Sin captura")

        header = ["SKU", "Producto", "Categoria", *branch_codes, "Total cierre"]
        ws.append(header)
        header_row = 5
        fill = PatternFill("solid", fgColor="1F2937")
        for cell in ws[header_row]:
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = fill
            cell.alignment = Alignment(horizontal="center")

        for row in payload["rows"]:
            ws.append(
                [
                    row["sku"],
                    row["product_name"],
                    row["category"],
                    *[float(row["stocks"].get(code, ZERO)) for code in branch_codes],
                    float(row["total_stock"]),
                ]
            )

        widths = {"A": 14, "B": 42, "C": 18}
        for idx, code in enumerate(branch_codes, start=4):
            widths[get_column_letter(idx)] = max(12, min(18, len(code) + 4))
        widths[get_column_letter(max_col)] = 14
        for column, width in widths.items():
            ws.column_dimensions[column].width = width
        for row in ws.iter_rows(min_row=6, min_col=4, max_col=max_col):
            for cell in row:
                cell.number_format = "#,##0.000"
                cell.alignment = Alignment(horizontal="right")
        ws.freeze_panes = "D6"
        return wb

    def build_pdf_bytes(self, payload: dict) -> bytes:
        def _escape(text: str) -> str:
            return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

        branch_totals = {branch["code"]: ZERO for branch in payload["branches"]}
        for row in payload["rows"]:
            for code, stock in row["stocks"].items():
                branch_totals[code] = (branch_totals.get(code, ZERO) + stock).quantize(ZERO)
        total = sum(branch_totals.values(), ZERO).quantize(ZERO)
        last_capture = payload["last_capture_at"].strftime("%Y-%m-%d %H:%M") if payload["last_capture_at"] else "Sin captura"
        lines = [
            f"Fecha operativa: {payload['fecha_operacion'].isoformat()}",
            f"Zona horaria: {payload['timezone_name']}",
            f"Ultima captura Point: {last_capture}",
            f"Productos con inventario: {len(payload['rows'])}",
            f"Total inventario cierre: {total}",
            "",
            "Totales por sucursal:",
        ]
        lines.extend([f"{code}: {branch_totals[code]}" for code in branch_totals])
        if payload["missing_branch_codes"]:
            lines.append("")
            lines.append("Sin captura: " + ", ".join(payload["missing_branch_codes"]))

        content_lines = ["BT", "/F1 12 Tf", "36 560 Td"]
        first = True
        for raw in ["Inventario final al cierre", *lines[:36]]:
            if first:
                content_lines.append(f"({_escape(str(raw))}) Tj")
                first = False
            else:
                content_lines.append("T*")
                content_lines.append(f"({_escape(str(raw))}) Tj")
        content_lines.append("ET")
        content = "\n".join(content_lines).encode("latin-1", errors="replace")
        objects = [
            b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj",
            b"2 0 obj << /Type /Pages /Count 1 /Kids [3 0 R] >> endobj",
            b"3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 792 612] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >> endobj",
            b"4 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj",
            b"5 0 obj << /Length " + str(len(content)).encode() + b" >> stream\n" + content + b"\nendstream endobj",
        ]
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
