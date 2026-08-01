"""Catch-up Hik-Connect -> ERP: recupera marcajes que el sync de 5 minutos dejo fuera.

La nube se consulta por ventanas diarias para no depender de una pagina global.
El barrido persiste cada GUID y reenvia el outbox.

El ERP deduplica por GUID, asi que reintentarlo es seguro.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import PAGE_SIZE, TIMEZONE
from hikconnect_client import HikConnectClient
from main import build_events, send_and_mark  # configura logging al importarse
from state import (
    init_db,
    quarantine_cloud_record,
)

log = logging.getLogger("catchup")


def enviar_por_lotes(events: list[dict], records: list) -> dict[str, int]:
    return send_and_mark(events, records, dry_run=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Catch-up Hik-Connect -> ERP")
    parser.add_argument(
        "--horas",
        type=int,
        default=72,
        help="Referencia de log compatible; el descubrimiento no filtra por deviceTime",
    )
    parser.add_argument("--max-pages", type=int, default=15, help="Paginas de nube a recorrer")
    parser.add_argument("--dry-run", action="store_true", help="No envia ni marca como enviado")
    args = parser.parse_args()

    init_db()
    desde = datetime.now(TIMEZONE) - timedelta(hours=args.horas)
    hasta = datetime.now(TIMEZONE)
    log.info(
        "Catch-up por fechas (%s -> %s, max_pages=%s)",
        desde.isoformat(),
        hasta.isoformat(),
        args.max_pages,
    )

    with HikConnectClient(headless=True) as client:
        records = client.fetch_records_between(
            start_dt=desde,
            end_dt=hasta,
            page_size=PAGE_SIZE,
            max_pages=args.max_pages,
            on_invalid_record=quarantine_cloud_record,
        )
    log.info("Registros en la nube dentro de la ventana: %s", len(records))

    events, selected = build_events(records, dry_run=args.dry_run)
    log.info("Marcajes que faltaban en el ERP: %s", len(events))

    if args.dry_run:
        log.info("dry-run: no se envio nada.")
        return 0

    total = enviar_por_lotes(events, selected)
    if not events and not any(total.values()):
        log.info("Nada que recuperar.")
    log.info("Resultado: %s", total)
    if not records.complete:
        log.error("Catch-up incompleto: aumente --max-pages para cubrir todo el periodo")
        return 1
    return 1 if total["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
