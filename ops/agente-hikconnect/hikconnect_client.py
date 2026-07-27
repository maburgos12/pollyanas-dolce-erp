from __future__ import annotations

import hashlib
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

# Paginas seguidas sin registros dentro de la ventana antes de dar por terminado
# el recorrido. Absorbe el desorden entre momento de subida y device_time.
PAGINAS_SECAS_PARA_CORTAR = 3


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
        return [_record_from_api(item) for item in items if _record_from_api(item)]

    def fetch_records_since(self, start_dt: datetime, page_size: int, max_pages: int) -> list[CloudRecord]:
        """Junta los marcajes con device_time >= start_dt recorriendo la nube por paginas.

        La nube pagina por momento de subida, NO por device_time: un marcaje viejo
        que el checador subio con retraso aparece en las primeras paginas, y uno
        reciente puede quedar mas atras. Por eso no se puede cortar en la primera
        pagina cuyo registro mas viejo caiga fuera de la ventana — eso descartaba
        marcajes buenos de paginas siguientes y los perdia para siempre. Se corta
        tras varias paginas seguidas sin nada dentro de la ventana.
        """
        self.ensure_login()
        start_dt = _ensure_aware(start_dt)
        por_guid: dict[str, CloudRecord] = {}
        paginas_secas = 0
        for page_index in range(1, max_pages + 1):
            page_records = self.fetch_records_page(page_index=page_index, page_size=page_size, require_login=False)
            if not page_records:
                break
            en_ventana = [record for record in page_records if record.device_time >= start_dt]
            por_guid.update({record.record_guid: record for record in en_ventana})
            oldest = min(record.device_time for record in page_records)
            log.info(
                "Hik-Connect pagina %s: %s registros, %s en ventana, oldest=%s",
                page_index,
                len(page_records),
                len(en_ventana),
                oldest.isoformat(),
            )
            paginas_secas = 0 if en_ventana else paginas_secas + 1
            if paginas_secas >= PAGINAS_SECAS_PARA_CORTAR:
                break
        return sorted(por_guid.values(), key=lambda record: record.device_time)


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
    serial_no = int(hashlib.sha1(record.record_guid.encode("utf-8")).hexdigest()[:12], 16)
    erp_employee_no = EMPLOYEE_CODE_ALIASES.get(record.employee_no, record.employee_no)
    return {
        "employee_no": erp_employee_no,
        "name": record.name,
        "attendance_status": attendance_status,
        "time": record.device_time.isoformat(),
        "serial_no": serial_no,
        "cloud_record_guid": record.record_guid,
        "device_name": record.device_name,
        "device_serial_no": record.device_serial_no,
        "department": record.department,
        "hik_employee_no": record.employee_no,
        "source": "hikconnect_cloud",
    }
