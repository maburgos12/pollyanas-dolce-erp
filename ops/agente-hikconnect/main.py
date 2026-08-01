from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta

from config import EMPLOYEE_CODE_ALIASES, LOG_FILE, MAX_PAGES, PAGE_SIZE, SYNC_INTERVAL_SECONDS, TIMEZONE
from erp_client import ping_erp, send_events
from file_importer import load_export_file
from hikconnect_client import HikConnectClient, record_to_erp_event
from state import (
    apply_delivery_results,
    day_state_exists,
    enqueue_event,
    get_discovery_page,
    get_last_sync_time,
    get_outbox_event,
    init_db,
    list_pending_events,
    mark_delivery_attempt,
    record_cycle_failure,
    record_cycle_success,
    record_delivery_error,
    quarantine_cloud_record,
    set_discovery_page,
    stable_punch_kind,
    was_sent,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("main")


def build_events(records, dry_run: bool) -> tuple[list[dict], list]:
    events = []
    selected_records = []
    dry_seen_days = set()
    for record in sorted(records, key=lambda item: item.device_time):
        if was_sent(record.record_guid):
            continue
        if dry_run:
            day_key = (record.employee_no, record.device_time.date().isoformat())
            status = (
                "check_out"
                if day_key in dry_seen_days
                or day_state_exists(record.employee_no, record.device_time)
                else "check_in"
            )
            dry_seen_days.add(day_key)
        else:
            existing = get_outbox_event(record.record_guid)
            if existing:
                events.append(existing["payload"])
                selected_records.append(record)
                continue
            employee_no = EMPLOYEE_CODE_ALIASES.get(record.employee_no, record.employee_no)
            status = stable_punch_kind(employee_no, record.device_time)
        event = record_to_erp_event(record, status)
        if not dry_run:
            enqueue_event(event)
        events.append(event)
        selected_records.append(record)
        if dry_run:
            log.info(
                "[DRY] %s %s %s %s %s",
                record.device_time.isoformat(),
                record.employee_no,
                status,
                record.name,
                record.record_guid[:12],
            )
    return events, selected_records


def send_and_mark(
    events: list[dict],
    records: list,
    dry_run: bool,
    attempted_event_ids: set[str] | None = None,
) -> dict:
    if dry_run:
        return {"acked": 0, "pending": 0, "review": 0, "errors": 0, "dry_run": len(events)}

    for event in events:
        enqueue_event(event)

    pending = list_pending_events()
    if attempted_event_ids is not None:
        pending = [item for item in pending if item["event_id"] not in attempted_event_ids]
    total = {"acked": 0, "pending": 0, "review": 0, "errors": 0}
    for start in range(0, len(pending), 100):
        batch = pending[start : start + 100]
        event_ids = [item["event_id"] for item in batch]
        payloads = [item["payload"] for item in batch]
        if attempted_event_ids is not None:
            attempted_event_ids.update(event_ids)
        mark_delivery_attempt(event_ids)
        try:
            response = send_events(payloads)
            results = response.get("results") if isinstance(response, dict) else None
            if not isinstance(results, list):
                raise ValueError("ERP no devolvio results")
            apply_delivery_results(event_ids, results)
        except Exception as exc:
            record_delivery_error(event_ids, str(exc))
            total["errors"] += len(event_ids)
            log.error("Lote outbox pendiente tras error ERP: %s", exc)
            continue

        for result in results:
            outcome = result.get("outcome")
            if outcome in {"accepted", "duplicate"}:
                total["acked"] += 1
            elif outcome == "deferred":
                total["pending"] += 1
            else:
                total["review"] += 1
    return total


def test_connectivity(headless: bool) -> bool:
    log.info("=== TEST HIK-CONNECT CLOUD ===")
    with HikConnectClient(headless=headless) as client:
        client.ensure_login()
        records = client.fetch_records_page(page_index=1, page_size=3, require_login=False)
        log.info("Hik-Connect OK. Registros recientes: %d", len(records))
        for record in records:
            log.info(
                "%s | %s | %s | %s",
                record.employee_no,
                record.name,
                record.department,
                record.device_time.isoformat(),
            )

    log.info("Probando ERP...")
    if not ping_erp():
        log.error("ERP no responde o API key invalida.")
        return False
    log.info("ERP OK")
    return True


def sync_once(headless: bool, dry_run: bool, since: datetime | None = None) -> dict:
    start_dt = since or get_last_sync_time()
    end_dt = datetime.now(TIMEZONE)
    start_page = 1 if since else get_discovery_page()
    log.info(
        "Sincronizando Hik-Connect por orden de subida desde pagina %s (%s -> %s)",
        start_page,
        start_dt.isoformat(),
        end_dt.isoformat(),
    )
    try:
        result = {"acked": 0, "pending": 0, "review": 0, "errors": 0}
        attempted_event_ids: set[str] = set()
        if dry_run:
            result["dry_run"] = 0

        def process_page(page_index: int, page_records) -> None:
            events, selected_records = build_events(page_records, dry_run=dry_run)
            log.info(
                "Hik-Connect pagina %s: %d registro(s), %d evento(s) nuevo(s)",
                page_index,
                len(page_records),
                len(events),
            )
            page_result = send_and_mark(
                events,
                selected_records,
                dry_run=dry_run,
                attempted_event_ids=attempted_event_ids,
            )
            for key, value in page_result.items():
                result[key] = result.get(key, 0) + value
            if not dry_run and page_result.get("errors"):
                raise RuntimeError(
                    f"ERP dejo {page_result['errors']} evento(s) pendientes en pagina {page_index}"
                )
            if not dry_run and (page_index != 1 or start_page == 1):
                set_discovery_page(page_index + 1)

        with HikConnectClient(headless=headless) as client:
            records = client.fetch_records_since(
                start_dt=start_dt,
                page_size=PAGE_SIZE,
                max_pages=MAX_PAGES,
                start_page=start_page,
                on_invalid_record=quarantine_cloud_record,
                on_page_records=process_page,
            )
        log.info("Registros cloud encontrados: %d", len(records))
        if not dry_run:
            # La pagina vacia confirma fin de recorrido; cada pagina previa ya fue entregada.
            set_discovery_page(records.next_page)
        if not dry_run:
            last_cloud = max((record.device_time for record in records), default=None)
            record_cycle_success(completed_at=datetime.now(TIMEZONE), last_cloud_record_at=last_cloud)
        log.info("Resultado ERP: %s", result)
        return result
    except Exception as exc:
        if not dry_run:
            category = "erp_unreachable" if "ERP" in str(exc) else "hik_cloud"
            record_cycle_failure(
                failed_at=datetime.now(TIMEZONE),
                category=category,
                error=str(exc),
            )
        raise


def import_file(path: str, dry_run: bool) -> dict:
    records = load_export_file(path)
    log.info("Registros cargados desde archivo: %d", len(records))
    events, selected_records = build_events(records, dry_run=dry_run)
    log.info("Eventos nuevos a enviar: %d", len(events))
    result = send_and_mark(events, selected_records, dry_run=dry_run)
    log.info("Resultado ERP: %s", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Agente Hik-Connect Cloud -> ERP Pollyana's Dolce")
    parser.add_argument("--test", action="store_true", help="Prueba login Hik-Connect y ERP")
    parser.add_argument("--sync-once", action="store_true", help="Ejecuta una sincronizacion y sale")
    parser.add_argument("--backfill-hours", type=int, metavar="HORAS", help="Trae historial reciente desde la nube")
    parser.add_argument("--import-file", help="Importa CSV/XLSX exportado manualmente desde Attendance")
    parser.add_argument("--dry-run", action="store_true", help="No envia al ERP ni marca registros como enviados")
    parser.add_argument("--headful", action="store_true", help="Abre navegador visible para diagnostico")
    args = parser.parse_args()

    init_db()
    headless = not args.headful

    if args.test:
        sys.exit(0 if test_connectivity(headless=headless) else 1)
    if args.import_file:
        import_file(args.import_file, dry_run=args.dry_run)
        return
    if args.backfill_hours:
        since = datetime.now(TIMEZONE) - timedelta(hours=args.backfill_hours)
        sync_once(headless=headless, dry_run=args.dry_run, since=since)
        return
    if args.sync_once:
        sync_once(headless=headless, dry_run=args.dry_run)
        return

    log.info("Agente Hik-Connect iniciado. Intervalo: %ds", SYNC_INTERVAL_SECONDS)
    while True:
        try:
            sync_once(headless=headless, dry_run=False)
        except Exception as exc:
            log.exception("Error inesperado en sync: %s", exc)
        time.sleep(SYNC_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
