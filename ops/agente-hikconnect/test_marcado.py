"""Check minimo del marcado de envio. Correr: .venv/bin/python test_marcado.py

Encierra la fuga que perdio los marcajes de ARLETH (codigo 328) el 28-jul-2026:
`send_and_mark` marcaba TODO el lote como enviado cuando `procesados > 0`, incluidos
los eventos que el ERP rechazo por "empleado no encontrado". Como un marcaje enviado
no se reintenta nunca, al vincular la identidad sus checadas ya no regresaban solas.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import TIMEZONE
from hikconnect_client import CloudRecord
import main

AHORA = datetime(2026, 7, 28, 8, 0, tzinfo=TIMEZONE)


def registro(guid: str, employee_no: str) -> CloudRecord:
    return CloudRecord(
        record_guid=guid,
        employee_no=employee_no,
        name="Prueba",
        department="",
        device_time=AHORA,
        device_name="",
        device_serial_no="",
        raw={},
    )


def marcados_tras_enviar(respuesta: dict, records: list[CloudRecord]) -> list[str]:
    """Corre send_and_mark contra un ERP simulado y devuelve los guid marcados."""
    marcados: list[str] = []
    main.send_events = lambda events: respuesta
    main.mark_sent = lambda guid, emp, ts: marcados.append(guid)
    events = [{"employee_no": record.employee_no} for record in records]
    main.send_and_mark(events, records, dry_run=False)
    return marcados


def test_no_marca_lo_que_el_erp_rechazo():
    """El caso real: 328 rechazada en un lote donde otros si entraron."""
    records = [registro("guid-ok", "255"), registro("guid-rechazado", "328")]
    respuesta = {
        "procesados": 1,
        "errores": 1,
        "duplicados": 0,
        "detalle": [
            {"employee_no": "255", "fecha": "2026-07-28", "status": "checkIn", "resultado": "creado"},
            {"employee_no": "328", "resultado": "empleado no encontrado"},
        ],
    }
    marcados = marcados_tras_enviar(respuesta, records)
    assert marcados == ["guid-ok"], f"debio marcar solo el aceptado, marco {marcados}"


def test_marca_todo_cuando_el_erp_acepta():
    records = [registro("a", "255"), registro("b", "298")]
    respuesta = {
        "procesados": 2,
        "errores": 0,
        "duplicados": 0,
        "detalle": [
            {"employee_no": "255", "fecha": "2026-07-28", "resultado": "creado"},
            {"employee_no": "298", "fecha": "2026-07-28", "resultado": "marca duplicada"},
        ],
    }
    marcados = marcados_tras_enviar(respuesta, records)
    assert marcados == ["a", "b"], f"debio marcar ambos, marco {marcados}"


def test_motivo_de_rechazo_nuevo_tambien_se_reintenta():
    """Un motivo que el ERP agregue despues debe contar como rechazo, no colarse."""
    records = [registro("z", "999")]
    respuesta = {
        "procesados": 1,
        "errores": 1,
        "duplicados": 0,
        "detalle": [{"employee_no": "999", "resultado": "motivo que el ERP agregue despues"}],
    }
    marcados = marcados_tras_enviar(respuesta, records)
    assert marcados == [], f"un motivo desconocido debe reintentarse, marco {marcados}"


if __name__ == "__main__":
    test_no_marca_lo_que_el_erp_rechazo()
    test_marca_todo_cuando_el_erp_acepta()
    test_motivo_de_rechazo_nuevo_tambien_se_reintenta()
    print("OK: los 3 checks de marcado pasan")
