from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from pos_bridge.browser.client import PlaywrightBrowserClient
from pos_bridge.browser.sales_reports_page import PointSalesReportsPage
from pos_bridge.browser.screenshots import capture_screenshot
from pos_bridge.browser.session import BrowserSessionManager
from pos_bridge.config import PointBridgeSettings, load_point_bridge_settings
from pos_bridge.services.auth_service import PointAuthService
from pos_bridge.services.normalizer_service import PointNormalizerService
from pos_bridge.utils.dates import iter_business_dates, local_now, timestamp_token
from pos_bridge.utils.exceptions import ExtractionError, PosBridgeError
from pos_bridge.utils.helpers import normalize_text, safe_slug, write_json_file


@dataclass
class ExtractedBranchDailySales:
    branch: dict
    sale_date: date
    sales_rows: list[dict]
    captured_at: object
    raw_export_path: str
    metadata: dict = field(default_factory=dict)


class PointSalesExtractor:
    def __init__(
        self,
        bridge_settings: PointBridgeSettings | None = None,
        normalizer: PointNormalizerService | None = None,
    ):
        self.settings = bridge_settings or load_point_bridge_settings()
        self.normalizer = normalizer or PointNormalizerService()
        self.auth_service = PointAuthService(self.settings)

    def _write_raw_export(self, branch: dict, sale_date: date, payload: dict) -> Path:
        filename = f"{timestamp_token()}_{safe_slug(branch['external_id'])}_{sale_date.isoformat()}_sales.json"
        return write_json_file(self.settings.raw_exports_dir / filename, payload)

    def _apply_branch_filter(self, branches: list[dict], branch_filter: str | None) -> list[dict]:
        excluded = {normalize_text(item) for item in self.settings.sales_excluded_branches if item}
        if excluded:
            branches = [
                branch
                for branch in branches
                if normalize_text(branch.get("name") or "") not in excluded
                and normalize_text(branch.get("short_name") or "") not in excluded
            ]
        if not branch_filter:
            return branches
        branch_filter = branch_filter.strip().lower()
        return [
            branch
            for branch in branches
            if branch_filter in str(branch.get("external_id", "")).lower()
            or branch_filter in str(branch.get("name", "")).lower()
            or branch_filter in str(branch.get("short_name", "")).lower()
        ]

    def iter_extract(
        self,
        *,
        start_date: date,
        end_date: date,
        branch_filter: str | None = None,
        excluded_ranges: list[tuple[date, date]] | None = None,
        max_days: int | None = None,
    ):
        client = PlaywrightBrowserClient(self.settings)
        session = None
        excluded_ranges = excluded_ranges or []
        try:
            with BrowserSessionManager(client) as session:
                self.auth_service.login(session)
                sales_page = PointSalesReportsPage(session.page, self.settings)
                sales_page.open()
                branches = self._apply_branch_filter(sales_page.list_branches(), branch_filter)
                if not branches:
                    raise ExtractionError(
                        "No se encontraron sucursales para la extracción de ventas.",
                        context={"branch_filter": branch_filter or ""},
                    )

                extracted: list[ExtractedBranchDailySales] = []
                dates = iter_business_dates(start_date, end_date, excluded_ranges=excluded_ranges)
                if max_days is not None:
                    dates = dates[:max_days]

                for sale_date in dates:
                    for branch in branches:
                        raw_rows = sales_page.fetch_daily_sales(
                            branch_external_id=str(branch["external_id"]),
                            sale_date=sale_date,
                        )
                        normalized_branch = self.normalizer.normalize_branch_payload(
                            {
                                "external_id": branch["external_id"],
                                "name": branch["name"],
                                "status": "ACTIVE",
                                "metadata": {"short_name": branch.get("short_name") or "", "plaza_id": branch.get("plaza_id")},
                            }
                        )
                        normalized_rows = [self.normalizer.normalize_sales_row(row, branch=normalized_branch, sale_date=sale_date) for row in raw_rows]
                        raw_export_path = self._write_raw_export(
                            normalized_branch,
                            sale_date,
                            {
                                "branch": normalized_branch,
                                "sale_date": sale_date.isoformat(),
                                "captured_at": local_now().isoformat(),
                                "row_count": len(normalized_rows),
                                "rows": normalized_rows,
                            },
                        )
                        yield ExtractedBranchDailySales(
                            branch=normalized_branch,
                            sale_date=sale_date,
                            sales_rows=normalized_rows,
                            captured_at=local_now(),
                            raw_export_path=str(raw_export_path),
                            metadata={"row_count": len(normalized_rows)},
                        )
        except PosBridgeError:
            raise
        except Exception as exc:
            screenshot_path = None
            if session is not None:
                try:
                    screenshot_path = str(capture_screenshot(session.page, self.settings, "sales_extraction_failure"))
                except Exception:
                    screenshot_path = None
            raise ExtractionError(
                f"Fallo en extracción histórica de ventas Point: {exc}",
                context={"screenshot_path": screenshot_path},
            ) from exc

    def extract(
        self,
        *,
        start_date: date,
        end_date: date,
        branch_filter: str | None = None,
        excluded_ranges: list[tuple[date, date]] | None = None,
        max_days: int | None = None,
    ) -> list[ExtractedBranchDailySales]:
        return list(
            self.iter_extract(
                start_date=start_date,
                end_date=end_date,
                branch_filter=branch_filter,
                excluded_ranges=excluded_ranges,
                max_days=max_days,
            )
        )
