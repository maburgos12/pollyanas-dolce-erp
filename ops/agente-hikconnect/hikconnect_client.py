from __future__ import annotations

import logging
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, time, timedelta
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


@dataclass
class InvalidCloudRecord:
    record_guid: str
    page_index: int
    reason: str
    raw: dict[str, Any]


class CloudPage(list[CloudRecord]):
    def __init__(
        self,
        records: list[CloudRecord],
        invalid_records: list[InvalidCloudRecord],
        *,
        total_num: int | None = None,
    ):
        super().__init__(records)
        self.invalid_records = invalid_records
        self.total_num = total_num


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

    def fetch_records_page(
        self,
        page_index: int,
        page_size: int,
        require_login: bool = True,
        start_dt: datetime | None = None,
        end_dt: datetime | None = None,
    ) -> CloudPage:
        if require_login:
            self.ensure_login()
        assert self.page is not None
        search_criteria: dict[str, Any] = {"type": 0}
        if start_dt is not None and end_dt is not None:
            search_criteria.update(
                {
                    # El portal humano manda limites de fecha con offset +00:00,
                    # aunque las marcas conserven su deviceTime local.
                    "beginTime": f"{start_dt.date().isoformat()}T00:00:00+00:00",
                    "endTime": f"{end_dt.date().isoformat()}T23:59:59+00:00",
                    "eventTypes": "",
                    "elementIDs": "",
                    "searchType": 0,
                    "cardNumber": "",
                    "personCondition": {},
                    "swipeAuthResult": 0,
                }
            )
        response = self.page.request.post(
            RECORDS_ENDPOINT,
            data={
                "pageIndex": page_index,
                "pageSize": page_size,
                "searchCriteria": search_criteria,
            },
            timeout=20_000,
        )
        if response.status != 200:
            raise RuntimeError(f"Hik-Connect API respondio {response.status}: {response.text()[:300]}")
        data = response.json()
        if str(data.get("errorCode")) != "0":
            raise RuntimeError(f"Hik-Connect API error: {data}")
        response_data = data.get("data", {}) or {}
        items = response_data.get("recordList", []) or []
        total_num_raw = response_data.get("totalNum")
        total_num = int(total_num_raw) if total_num_raw is not None else None
        records = []
        invalid_records = []
        for item in items:
            record, reason = _record_from_api(item)
            if record is None:
                raw = item if isinstance(item, dict) else {"raw_value": repr(item)}
                guid = str(raw.get("recordGuid") or "").strip()
                if not guid:
                    canonical = json.dumps(raw, ensure_ascii=False, sort_keys=True, default=str)
                    guid = f"invalid:{hashlib.sha256(canonical.encode()).hexdigest()}"
                invalid_records.append(
                    InvalidCloudRecord(
                        record_guid=guid,
                        page_index=page_index,
                        reason=reason,
                        raw=raw,
                    )
                )
                log.warning(
                    "Registro Hik-Connect aislado para revision: pagina=%s guid=%s razon=%s",
                    page_index,
                    guid,
                    reason,
                )
                continue
            records.append(record)
        return CloudPage(records, invalid_records, total_num=total_num)

    def fetch_records_between(
        self,
        start_dt: datetime,
        end_dt: datetime,
        page_size: int,
        max_pages: int,
        on_invalid_record=None,
        on_page_records=None,
    ) -> DiscoveryRecords:
        """Consulta ventanas diarias, de la mas reciente a la mas antigua.

        Cada fecha reinicia en pagina 1 y usa ``totalNum`` para no solicitar
        paginas inexistentes. De esta forma la sincronizacion reciente se
        procesa antes que el catch-up y nunca depende de un cursor global.
        """
        self.ensure_login()
        start_dt = _ensure_aware(start_dt)
        end_dt = _ensure_aware(end_dt)
        if end_dt < start_dt:
            raise ValueError("end_dt debe ser posterior a start_dt")
        if page_size <= 0 or max_pages <= 0:
            raise ValueError("page_size y max_pages deben ser positivos")

        dates = []
        current_date = end_dt.date()
        while current_date >= start_dt.date():
            dates.append(current_date)
            current_date -= timedelta(days=1)

        por_guid: dict[str, CloudRecord] = {}
        pages_used = 0
        complete = True
        for work_date in dates:
            window_start = datetime.combine(work_date, time.min, tzinfo=TIMEZONE)
            window_end = datetime.combine(work_date, time(23, 59, 59), tzinfo=TIMEZONE)
            page_index = 1
            total_pages: int | None = None
            while True:
                if pages_used >= max_pages:
                    complete = False
                    break
                page_records = self.fetch_records_page(
                    page_index=page_index,
                    page_size=page_size,
                    require_login=False,
                    start_dt=window_start,
                    end_dt=window_end,
                )
                pages_used += 1
                invalid_records = getattr(page_records, "invalid_records", [])
                for invalid_record in invalid_records:
                    if on_invalid_record is None:
                        raise RuntimeError(
                            "Hik-Connect devolvio un registro invalido sin journal durable"
                        )
                    on_invalid_record(invalid_record)
                if on_page_records is not None and (page_records or invalid_records):
                    on_page_records(work_date, page_index, page_records)
                por_guid.update({record.record_guid: record for record in page_records})

                if page_records.total_num is not None:
                    total_pages = math.ceil(page_records.total_num / page_size)
                    if page_index >= total_pages:
                        break
                elif not page_records and not invalid_records:
                    break
                page_index += 1
            if not complete:
                break

        return DiscoveryRecords(
            sorted(por_guid.values(), key=lambda record: record.device_time),
            complete=complete,
            next_page=1,
        )

    def fetch_records_since(
        self,
        start_dt: datetime,
        page_size: int,
        max_pages: int,
        start_page: int = 1,
        on_invalid_record=None,
        on_page_records=None,
    ) -> DiscoveryRecords:
        """Compatibilidad: delega al recorrido acotado por fechas.

        ``start_page`` se ignora deliberadamente; el cursor global fue retirado.
        """
        if start_page != 1:
            log.info(
                "Ignorando cursor global heredado de Hik-Connect: pagina %s",
                start_page,
            )
        callback = None
        if on_page_records is not None:
            callback = lambda _work_date, page_index, records: on_page_records(
                page_index, records
            )
        return self.fetch_records_between(
            start_dt=start_dt,
            end_dt=datetime.now(TIMEZONE),
            page_size=page_size,
            max_pages=max_pages,
            on_invalid_record=on_invalid_record,
            on_page_records=callback,
        )


def _ensure_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=TIMEZONE)
    return dt.astimezone(TIMEZONE)


def _device_serial_from_api(raw: dict[str, Any]) -> str:
    explicit = str(raw.get("deviceSerialNo") or "").strip()
    if explicit:
        return explicit
    reader_name = str(raw.get("cardReaderName") or "").strip()
    for marker in ("-Cardreader", "-Card Reader"):
        if marker in reader_name:
            serial = reader_name.split(marker, 1)[0].strip()
            if serial:
                return serial
    return str(raw.get("devSerialNo") or raw.get("deviceId") or "").strip()


def _record_from_api(raw: dict[str, Any]) -> tuple[CloudRecord | None, str]:
    base_info = ((raw.get("personInfo") or {}).get("baseInfo") or {})
    employee_no = str(base_info.get("personCode") or "").strip()
    device_time_raw = str(raw.get("deviceTime") or "").strip()
    record_guid = str(raw.get("recordGuid") or "").strip()
    if not record_guid:
        return None, "missing_record_guid"
    if not employee_no:
        return None, "missing_employee_code"
    if not device_time_raw:
        return None, "missing_device_time"
    try:
        device_time = datetime.fromisoformat(device_time_raw).astimezone(TIMEZONE)
    except ValueError:
        log.warning("Fecha Hik-Connect invalida: %s", device_time_raw)
        return None, "invalid_device_time"
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
        device_serial_no=_device_serial_from_api(raw),
        raw=raw,
    ), ""


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
