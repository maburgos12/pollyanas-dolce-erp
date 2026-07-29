from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from playwright.sync_api import Browser, BrowserContext, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright

from config import (
    EMPLOYEE_CODE_ALIASES,
    HEADLESS,
    HIKCONNECT_API_BASE,
    HIKCONNECT_EMAIL,
    HIKCONNECT_LOGIN_URL,
    HIKCONNECT_PASSWORD,
    HIKCONNECT_PORTAL_URL,
    STORAGE_STATE_PATH,
    TIMEZONE,
)

log = logging.getLogger("hikconnect_client")

RECORDS_ENDPOINT = f"{HIKCONNECT_API_BASE}/hccacs/v1/event/certificateRecords/search"

@dataclass
class CloudRecord:
    record_guid: str
    employee_no: str
    name: str
    department: str
    device_time: datetime
    device_name: str
    device_serial_no: str
    raw: dict[str, Any]


class DiscoveryRecords(list[CloudRecord]):
    def __init__(self, records: list[CloudRecord], *, complete: bool, next_page: int):
        super().__init__(records)
        self.complete = complete
        self.next_page = next_page


class HikConnectClient:
    def __init__(self, headless: bool = HEADLESS):
        self.headless = headless
        self._pw = None
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None
        self.page: Page | None = None

    def __enter__(self) -> "HikConnectClient":
        self._pw = sync_playwright().start()
        self.browser = self._pw.chromium.launch(headless=self.headless)
        context_kwargs: dict[str, Any] = {
            "viewport": {"width": 1280, "height": 900},
            "accept_downloads": True,
        }
        if STORAGE_STATE_PATH.exists():
            context_kwargs["storage_state"] = str(STORAGE_STATE_PATH)
        self.context = self.browser.new_context(**context_kwargs)
        self.page = self.context.new_page()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self._pw:
            self._pw.stop()

    def ensure_login(self) -> None:
        if not HIKCONNECT_EMAIL or not HIKCONNECT_PASSWORD:
            raise RuntimeError("Faltan HIKCONNECT_EMAIL/HIKCONNECT_PASSWORD en .env")
        if self._session_is_valid():
            log.info("Sesion Hik-Connect reutilizada desde storage_state")
            return

        assert self.page is not None
        log.info("Iniciando sesion Hik-Connect")
        self.page.goto(HIKCONNECT_LOGIN_URL, wait_until="domcontentloaded", timeout=30_000)
        try:
            self.page.get_by_role("button", name="Accept All").click(timeout=2_000)
        except PlaywrightTimeoutError:
            pass
        self.page.get_by_placeholder("Account/Email").fill(HIKCONNECT_EMAIL)
        self.page.get_by_placeholder("Password").fill(HIKCONNECT_PASSWORD)
        self.page.get_by_role("button", name="Login").click()
        self.page.wait_for_timeout(7_000)
        if "/portal" not in self.page.url and not self._session_is_valid():
            raise RuntimeError(f"No se pudo iniciar sesion Hik-Connect. URL actual: {self.page.url}")
        assert self.context is not None
        self.context.storage_state(path=str(STORAGE_STATE_PATH))
        log.info("Sesion Hik-Connect OK")

    def _session_is_valid(self) -> bool:
        try:
            records = self.fetch_records_page(page_index=1, page_size=1, require_login=False)
            return isinstance(records, list)
        except Exception:
            return False

    def fetch_records_page(self, page_index: int, page_size: int, require_login: bool = True) -> list[CloudRecord]:
        if require_login:
            self.ensure_login()
        assert self.page is not None
        response = self.page.request.post(
            RECORDS_ENDPOINT,
            data={"pageIndex": page_index, "pageSize": page_size, "searchCriteria": {"type": 0}},
            timeout=20_000,
        )
        if response.status != 200:
            raise RuntimeError(f"Hik-Connect API respondio {response.status}: {response.text()[:300]}")
        data = response.json()
        if str(data.get("errorCode")) != "0":
            raise RuntimeError(f"Hik-Connect API error: {data}")
        items = data.get("data", {}).get("recordList", []) or []
        records = []
        for item in items:
            record = _record_from_api(item)
            if record is None:
                guid = str(item.get("recordGuid") or "").strip() if isinstance(item, dict) else ""
                raise RuntimeError(
                    f"Hik-Connect devolvio un registro invalido en pagina {page_index}"
                    + (f" (GUID {guid})" if guid else "")
                )
            records.append(record)
        return records

    def fetch_records_since(
        self,
        start_dt: datetime,
        page_size: int,
        max_pages: int,
        start_page: int = 1,
    ) -> DiscoveryRecords:
        """Recorre la nube por orden de subida, sin filtrar por deviceTime."""
        self.ensure_login()
        por_guid: dict[str, CloudRecord] = {}
        complete = False
        next_page = max(1, start_page)
        continuation_start = max(1, start_page)
        if continuation_start > 1:
            page_indexes = [1, *range(continuation_start, continuation_start + max(max_pages - 1, 0))]
        else:
            page_indexes = list(range(1, 1 + max_pages))
        for page_index in page_indexes:
            page_records = self.fetch_records_page(page_index=page_index, page_size=page_size, require_login=False)
            if not page_records:
                if page_index != 1 or continuation_start == 1:
                    complete = True
                    next_page = 1
                    break
                continue
            por_guid.update({record.record_guid: record for record in page_records})
            if page_index != 1 or continuation_start == 1:
                next_page = page_index + 1
            log.info(
                "Hik-Connect pagina %s: %s registros por orden de subida",
                page_index,
                len(page_records),
            )
        return DiscoveryRecords(
            sorted(por_guid.values(), key=lambda record: record.device_time),
            complete=complete,
            next_page=next_page,
        )


def _ensure_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=TIMEZONE)
    return dt.astimezone(TIMEZONE)


def _record_from_api(raw: dict[str, Any]) -> CloudRecord | None:
    base_info = ((raw.get("personInfo") or {}).get("baseInfo") or {})
    employee_no = str(base_info.get("personCode") or "").strip()
    device_time_raw = str(raw.get("deviceTime") or "").strip()
    record_guid = str(raw.get("recordGuid") or "").strip()
    if not employee_no or not device_time_raw or not record_guid:
        return None
    try:
        device_time = datetime.fromisoformat(device_time_raw).astimezone(TIMEZONE)
    except ValueError:
        log.warning("Fecha Hik-Connect invalida: %s", device_time_raw)
        return None
    name = " ".join(
        part
        for part in [str(base_info.get("firstName") or "").strip(), str(base_info.get("lastName") or "").strip()]
        if part
    )
    return CloudRecord(
        record_guid=record_guid,
        employee_no=employee_no,
        name=name,
        department=str(base_info.get("fullPath") or "").strip(),
        device_time=device_time,
        device_name=str(raw.get("deviceName") or "").strip(),
        device_serial_no=str(raw.get("devSerialNo") or raw.get("deviceSerialNo") or "").strip(),
        raw=raw,
    )


def record_to_erp_event(record: CloudRecord, attendance_status: str) -> dict[str, Any]:
    erp_employee_no = EMPLOYEE_CODE_ALIASES.get(record.employee_no, record.employee_no)
    return {
        "event_id": record.record_guid,
        "source": "hikconnect_cloud",
        "employee_external_id": erp_employee_no,
        "occurred_at": record.device_time.isoformat(),
        # La nube no entrega un estado confiable. El ERP reconstruye todos los
        # punches por cronología del ledger, incluso si llegan fuera de orden.
        "kind": "punch",
        "device_id": record.device_serial_no or record.device_name,
    }
