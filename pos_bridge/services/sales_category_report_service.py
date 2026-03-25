from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from urllib.parse import urlencode, urljoin

import pandas as pd
from django.utils import timezone

from pos_bridge.config import PointBridgeSettings, load_point_bridge_settings
from pos_bridge.services.point_http_session_service import PointHttpSessionService
from pos_bridge.utils.helpers import decimal_from_value, safe_slug


@dataclass
class PointSalesCategoryReportResult:
    request_url: str
    report_path: str
    report_type: str
    ext: str
    branch_external_id: str | None
    credito: str | None
    start_date: date
    end_date: date


@dataclass
class PointSalesCategoryParsedReport:
    rows: list[dict]
    summary: dict
    report_path: str


class PointSalesCategoryReportService:
    PRINT_REPORT_PATH = "/Report/PrintReportes/"
    REPORTE_CATEGORIA_ID = 3
    REPORT_TYPE_CATEGORY = "category"
    EXT_XLS = "Excel"

    def __init__(
        self,
        bridge_settings: PointBridgeSettings | None = None,
        http_session_service: PointHttpSessionService | None = None,
    ):
        self.settings = bridge_settings or load_point_bridge_settings()
        self.http_session_service = http_session_service or PointHttpSessionService(self.settings)

    def _to_epoch_ms(self, value: date) -> int:
        local_tz = timezone.get_current_timezone()
        # Point usa exactamente Math.floor($("#datepicker").datepicker('getDate')),
        # que equivale a medianoche local del día seleccionado.
        dt = datetime.combine(value, time.min)
        aware = timezone.make_aware(dt, local_tz)
        return int(aware.timestamp() * 1000)

    def _build_params(
        self,
        *,
        start_date: date,
        end_date: date,
        branch_external_id: str | None,
        branch_display_name: str | None,
        credito: str | None,
    ) -> dict[str, str]:
        return {
            "ext": self.EXT_XLS,
            "fi": str(self._to_epoch_ms(start_date)),
            "ff": str(self._to_epoch_ms(end_date)),
            "sucursal": str(branch_external_id) if branch_external_id not in (None, "") else "null",
            "idreporte": str(self.REPORTE_CATEGORIA_ID),
            "idtipo": "0",
            "nomSucursal": (branch_display_name or "").strip() if branch_external_id not in (None, "") else "Todas las sucursales",
            "nomTipo": "0",
            "credito": "null" if credito in (None, "") else str(credito),
        }

    def _output_path(
        self,
        *,
        start_date: date,
        end_date: date,
        branch_external_id: str | None,
        credito: str | None,
    ) -> Path:
        token = datetime.now().strftime("%Y%m%d_%H%M%S")
        branch_token = safe_slug(branch_external_id or "all")
        credito_token = safe_slug(credito or "null")
        filename = f"{token}_point_sales_category_{start_date.isoformat()}_{end_date.isoformat()}_{branch_token}_{credito_token}.xls"
        return self.settings.raw_exports_dir / filename

    def fetch_report(
        self,
        *,
        start_date: date,
        end_date: date,
        branch_external_id: str | None = None,
        branch_display_name: str | None = None,
        credito: str | None = None,
    ) -> PointSalesCategoryReportResult:
        auth_session = self.http_session_service.create(
            branch_external_id=branch_external_id,
            branch_display_name=branch_display_name,
        )
        params = self._build_params(
            start_date=start_date,
            end_date=end_date,
            branch_external_id=branch_external_id,
            branch_display_name=branch_display_name,
            credito=credito,
        )
        request_url = urljoin(self.settings.base_url.rstrip("/") + "/", self.PRINT_REPORT_PATH.lstrip("/"))
        response = auth_session.session.get(request_url, params=params, timeout=self.settings.timeout_ms / 1000)
        response.raise_for_status()

        output_path = self._output_path(
            start_date=start_date,
            end_date=end_date,
            branch_external_id=branch_external_id,
            credito=credito,
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(response.content)
        return PointSalesCategoryReportResult(
            request_url=f"{request_url}?{urlencode(params)}",
            report_path=str(output_path),
            report_type=self.REPORT_TYPE_CATEGORY,
            ext=self.EXT_XLS,
            branch_external_id=branch_external_id,
            credito=credito,
            start_date=start_date,
            end_date=end_date,
        )

    def parse_report(self, *, report_path: str) -> PointSalesCategoryParsedReport:
        path = Path(report_path)
        workbook = pd.ExcelFile(path)
        detail_rows = self._parse_detail_sheet(path=path, sheet_names=workbook.sheet_names)
        summary = self._parse_summary_sheet(path=path, sheet_names=workbook.sheet_names)
        return PointSalesCategoryParsedReport(rows=detail_rows, summary=summary, report_path=str(path))

    def _parse_detail_sheet(self, *, path: Path, sheet_names: list[str]) -> list[dict]:
        for sheet_name in sheet_names:
            dataframe = pd.read_excel(path, sheet_name=sheet_name, header=None)
            rows = self._extract_detail_rows_from_dataframe(dataframe)
            if rows:
                return rows
        return []

    def _parse_summary_sheet(self, *, path: Path, sheet_names: list[str]) -> dict:
        for sheet_name in sheet_names:
            dataframe = pd.read_excel(path, sheet_name=sheet_name, header=None)
            for _, row in dataframe.iterrows():
                values = [value for value in row.tolist() if pd.notna(value)]
                if values and str(values[0]).strip() == "Total Ventas":
                    return {
                        "bruto": decimal_from_value(values[1] if len(values) > 1 else 0),
                        "descuentos": decimal_from_value(values[2] if len(values) > 2 else 0),
                        "venta": decimal_from_value(values[3] if len(values) > 3 else 0),
                        "impuestos": decimal_from_value(values[4] if len(values) > 4 else 0),
                        "venta_neta": decimal_from_value(values[5] if len(values) > 5 else 0),
                    }
        return {
            "bruto": decimal_from_value(0),
            "descuentos": decimal_from_value(0),
            "venta": decimal_from_value(0),
            "impuestos": decimal_from_value(0),
            "venta_neta": decimal_from_value(0),
        }

    def _extract_detail_rows_from_dataframe(self, dataframe: pd.DataFrame) -> list[dict]:
        rows: list[dict] = []
        current_category = ""
        header_found = False
        for _, row in dataframe.iterrows():
            values = [value for value in row.tolist() if pd.notna(value)]
            if not values:
                continue
            joined = " ".join(str(value) for value in values)
            first = str(values[0]).strip()
            if "CATEGORÍA" in joined and "PRODUCTO" in joined:
                header_found = True
                continue
            if not header_found:
                continue
            if first in {"Total de la categoría", "Total General", "Detallado Ventas", "Total Ventas"}:
                continue
            if len(values) >= 9:
                current_category = str(values[0]).strip()
                rows.append(
                    {
                        "Categoria": current_category,
                        "Codigo": str(values[1]).strip(),
                        "Nombre": str(values[2]).strip(),
                        "Cantidad": values[3],
                        "Bruto": values[4],
                        "Descuento": values[5],
                        "Venta": values[6],
                        "IVA": values[7],
                        "Venta_neta": values[8],
                    }
                )
            elif len(values) >= 8 and current_category:
                rows.append(
                    {
                        "Categoria": current_category,
                        "Codigo": str(values[0]).strip(),
                        "Nombre": str(values[1]).strip(),
                        "Cantidad": values[2],
                        "Bruto": values[3],
                        "Descuento": values[4],
                        "Venta": values[5],
                        "IVA": values[6],
                        "Venta_neta": values[7],
                    }
                )
        return rows
