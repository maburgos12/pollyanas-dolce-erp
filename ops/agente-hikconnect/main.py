from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta

from config import LOG_FILE, MAX_PAGES, PAGE_SIZE, SYNC_INTERVAL_SECONDS, TIMEZONE
from erp_client import ping_erp, send_events
from file_importer import load_export_file
from hikconnect_client import HikConnectClient, record_to_erp_event
from state import classify_punch, day_state_exists, get_last_sync_time, init_db, mark_sent, set_last_sync_time, was_sent

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
            status = "checkOut" if day_key in dry_seen_days or day_state_exists(record.employee_no, record.device_time) else "checkIn"
            dry_seen_days.add(day_key)
        else:
            status = classify_punch(record.employee_no, record.device_time)
        events.append(record_to_erp_event(record, status))
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


def empleados_rechazados(result: dict) -> set[str]:
    """Codigos cuyos eventos el ERP NO ingirio en esta respuesta.

    El detalle del receptor solo trae `employee_no`, sin id por evento, asi que
    el empleado es la granularidad mas fina disponible. Los rechazos (empleado no
    encontrado, campos faltantes, fecha invalida) se reconocen porque no llevan
    `fecha`: nunca llegaron a agruparse en una asistencia.

    Se detectan por la ausencia de `fecha` y no por el texto del motivo, para que
    un motivo nuevo del ERP tambien cuente como rechazo. El sesgo es deliberado:
    reintentar de mas es inocuo porque el ERP deduplica, mientras que dar por
    enviado un marcaje rechazado lo pierde para siempre.
    """
    return {
        str(item.get("employee_no", "")).strip()
        for item in result.get("detalle") or []
        if not item.get("fecha")
    }


def send_and_mark(events: list[dict], records: list, dry_run: bool) -> dict:
    if dry_run:
        return {"procesados": 0, "errores": 0, "duplicados": 0, "dry_run": len(events)}
    result = send_events(events)
    if result.get("procesados", 0) > 0:
        rechazados = empleados_rechazados(result)
        # events y records van en paralelo desde build_events; se compara contra
        # el employee_no del evento porque es el que ya trae aplicado el alias.
        for event, record in zip(events, records):
            if str(event.get("employee_no", "")).strip() in rechazados:
                log.warning(
                    "ERP no acepto el marcaje de %s (%s): no se marca como enviado, se reintenta",
                    event.get("employee_no"),
                    record.device_time.isoformat(),
                )
                continue
            mark_sent(record.record_guid, record.employee_no, record.device_time.isoformat())
    return result


def test_connectivity(headless: bool) -> bool:
    log.info("=== TEST HIK-CONNECT CLOUD ===")
    with HikConnectClient(headless=headless) as client:
        client.ensure_login()
        records = client.fetch_records_page(page_index=1, page_size=3, require_login=False)
        log.info("Hik-Connect OK. Registros recientes: %d", len(records))
        for record in records:
            log.info("%s | %s | %s | %s", record.employee_no, record.name, record.department, record.device_time.isoformat())

    log.info("Probando ERP...")
    if not ping_erp():
        log.error("ERP no responde o API key invalida.")
        return False
    log.info("ERP OK")
    return True


def sync_once(headless: bool, dry_run: bool, since: datetime | None = None) -> dict:
    start_dt = since or get_last_sync_time()
    end_dt = datetime.now(TIMEZONE)
    log.info("Sincronizando Hik-Connect %s -> %s", start_dt.isoformat(), end_dt.isoformat())
    with HikConnectClient(headless=headless) as client:
        records = client.fetch_records_since(start_dt=start_dt, page_size=PAGE_SIZE, max_pages=MAX_PAGES)
    log.info("Registros cloud encontrados: %d", len(records))
    events, selected_records = build_events(records, dry_run=dry_run)
    log.info("Eventos nuevos a enviar: %d", len(events))
    result = send_and_mark(events, selected_records, dry_run=dry_run)
    if not dry_run:
        set_last_sync_time(end_dt)
    log.info("Resultado ERP: %s", result)
    return result


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
