"""Catch-up Hik-Connect -> ERP: recupera marcajes que el sync de 5 minutos dejo fuera.

La nube ordena los registros por momento de subida, no por device_time. Cuando el
checador sube un backlog (tras un corte de red), esos marcajes quedan mezclados en
paginas que el sync corto no alcanza y se pierden. Este barrido usa una ventana
ancha, donde ese sesgo de orden ya no importa, y reenvia lo que falte.

El ERP deduplica por marca cercana, asi que correrlo de mas es inocuo.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import requests

from config import ERP_API_KEY, ERP_BASE_URL, ERP_ENDPOINT, PAGE_SIZE, TIMEZONE
from hikconnect_client import HikConnectClient
from main import build_events, empleados_rechazados  # configura logging al importarse
from state import init_db, mark_sent

# send_events del agente manda todo en un POST con timeout=20; con volumen revienta.
BATCH_SIZE = 25
POST_TIMEOUT = 280

log = logging.getLogger("catchup")


def enviar_por_lotes(events: list[dict], records: list) -> dict[str, int]:
    url = f"{ERP_BASE_URL}{ERP_ENDPOINT}"
    headers = {"X-API-Key": ERP_API_KEY, "Content-Type": "application/json"}
    total = {"procesados": 0, "errores": 0, "duplicados": 0}

    for inicio in range(0, len(events), BATCH_SIZE):
        lote_eventos = events[inicio : inicio + BATCH_SIZE]
        lote_registros = records[inicio : inicio + BATCH_SIZE]
        try:
            response = requests.post(
                url,
                json={"eventos": lote_eventos},
                headers=headers,
                timeout=POST_TIMEOUT,
            )
            response.raise_for_status()
            resultado = response.json()
        except Exception as exc:
            log.error("Lote %s fallo: %s", inicio, exc)
            total["errores"] += len(lote_eventos)
            continue

        for clave in total:
            total[clave] += resultado.get(clave, 0)
        if resultado.get("procesados", 0) > 0:
            rechazados = empleados_rechazados(resultado)
            for evento, record in zip(lote_eventos, lote_registros):
                if str(evento.get("employee_no", "")).strip() in rechazados:
                    log.warning(
                        "ERP no acepto el marcaje de %s (%s): no se marca como enviado, se reintenta",
                        evento.get("employee_no"),
                        record.device_time.isoformat(),
                    )
                    continue
                mark_sent(record.record_guid, record.employee_no, record.device_time.isoformat())

    return total


def main() -> int:
    parser = argparse.ArgumentParser(description="Catch-up Hik-Connect -> ERP")
    parser.add_argument("--horas", type=int, default=72, help="Ventana hacia atras en horas")
    parser.add_argument("--max-pages", type=int, default=15, help="Paginas de nube a recorrer")
    parser.add_argument("--dry-run", action="store_true", help="No envia ni marca como enviado")
    args = parser.parse_args()

    init_db()
    desde = datetime.now(TIMEZONE) - timedelta(hours=args.horas)
    log.info("Catch-up desde %s (max_pages=%s)", desde.isoformat(), args.max_pages)

    with HikConnectClient(headless=True) as client:
        records = client.fetch_records_since(
            start_dt=desde,
            page_size=PAGE_SIZE,
            max_pages=args.max_pages,
        )
    log.info("Registros en la nube dentro de la ventana: %s", len(records))

    events, selected = build_events(records, dry_run=args.dry_run)
    log.info("Marcajes que faltaban en el ERP: %s", len(events))

    if not events:
        log.info("Nada que recuperar.")
        return 0
    if args.dry_run:
        log.info("dry-run: no se envio nada.")
        return 0

    total = enviar_por_lotes(events, selected)
    log.info("Resultado: %s", total)
    return 1 if total["errores"] else 0


if __name__ == "__main__":
    sys.exit(main())
