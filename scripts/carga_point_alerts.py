#!/usr/bin/env python3
"""Detect expired driver refreshes independently of the Celery workers."""
import json
import os
from pathlib import Path
import sys

from vps_backup_alerts import notify, read_command


def message(alerta):
    recarga_id = alerta["recarga_id"]
    tramo = f"Recarga CEDIS {recarga_id}" if recarga_id else "Carga inicial"
    ruta_id = alerta["ruta_id"]
    folio = alerta["folio"] or ruta_id
    lines = [
        "ERP Pollyana · actualización de carga Point vencida",
        f"Ruta {folio} · Solicitud #{alerta['id']} · {tramo}",
        f"Solicitada {alerta['inicio']} (hora de Sinaloa).",
    ]
    if alerta["sync_posterior"]:
        lines.append("El ERP registra una sincronización posterior para la ruta.")
    lines.extend((
        "Revisa la solicitud y el worker logistica_point antes de reintentar.",
        f"https://erp.pollyanasdolce.com/logistica/rutas/{ruta_id}/",
    ))
    return "\n".join(lines)


def deliver(alertas, path, sender):
    try:
        previous = json.loads(path.read_text())
    except (FileNotFoundError, ValueError):
        previous = {}
    if not isinstance(previous, dict):
        previous = {}
    receipts = {str(item["id"]): previous[str(item["id"])] for item in alertas if str(item["id"]) in previous}
    accepted = {}
    for alerta in alertas:
        job_id = alerta["id"]
        key = str(job_id)
        if key in receipts:
            continue
        receipt = sender(message(alerta))
        receipts[key] = receipt
        accepted[job_id] = receipt
        save_receipts(path, receipts)
    if not accepted and receipts != previous:
        save_receipts(path, receipts)
    return accepted


def save_receipts(path, receipts):
    temporary = path.with_suffix(".next")
    temporary.write_text(json.dumps(receipts))
    temporary.replace(path)


def main():
    output = read_command([
        "docker", "compose", "exec", "-T", "web", "/opt/venv/bin/python",
        "manage.py", "listar_cargas_point_vencidas",
    ], 25)
    alertas = json.loads(output)
    credentials = Path(os.environ["CREDENTIALS_DIRECTORY"]) / "telegram.json"
    config = json.loads(credentials.read_text())
    state = Path(os.environ["STATE_DIRECTORY"]) / "delivery.json"
    accepted = deliver(alertas, state, lambda text: notify(config, text))
    print(json.dumps(dict(event="carga_point_alert_check", vencidas=len(alertas), telegram=accepted)))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(json.dumps(dict(event="carga_point_alert_failed", error_type=type(error).__name__)), file=sys.stderr)
        raise SystemExit(1)
