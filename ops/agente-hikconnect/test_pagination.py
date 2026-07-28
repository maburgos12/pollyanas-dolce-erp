"""Check minimo de fetch_records_since. Correr: .venv/bin/python test_pagination.py

Encierra el bug que perdio marcajes en jun-jul 2026: la nube pagina por momento de
subida, asi que un marcaje viejo subido con retraso caia en la pagina 1, el recorrido
abortaba ahi y los marcajes buenos de las paginas siguientes nunca llegaban al ERP.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import TIMEZONE
from hikconnect_client import CloudRecord, HikConnectClient

AHORA = datetime(2026, 7, 27, 12, 0, tzinfo=TIMEZONE)
VENTANA = AHORA - timedelta(hours=1)


def registro(guid: str, minutos_atras: int) -> CloudRecord:
    return CloudRecord(
        record_guid=guid,
        employee_no="1",
        name="Prueba",
        department="",
        device_time=AHORA - timedelta(minutes=minutos_atras),
        device_name="",
        device_serial_no="",
        raw={},
    )


class ClienteFalso(HikConnectClient):
    def __init__(self, paginas: list[list[CloudRecord]]):
        self.paginas = paginas
        self.ultima_pagina_pedida = 0

    def ensure_login(self) -> None:
        pass

    def fetch_records_page(self, page_index, page_size, require_login=True):
        self.ultima_pagina_pedida = max(self.ultima_pagina_pedida, page_index)
        return self.paginas[page_index - 1] if page_index <= len(self.paginas) else []


def test_no_corta_por_pagina_desordenada():
    """El caso real: marcaje viejo en pagina 1, marcaje bueno en pagina 2."""
    cliente = ClienteFalso([[registro("viejo", 60 * 48)], [registro("reciente", 10)], []])
    encontrados = cliente.fetch_records_since(start_dt=VENTANA, page_size=100, max_pages=8)
    guids = {record.record_guid for record in encontrados}
    assert guids == {"reciente"}, f"debio recuperar el marcaje de la pagina 2, obtuvo {guids}"


def test_corta_tras_paginas_secas():
    """Sin marcajes en ventana no debe recorrer la nube entera."""
    cliente = ClienteFalso([[registro(f"viejo{i}", 60 * 48)] for i in range(8)])
    cliente.fetch_records_since(start_dt=VENTANA, page_size=100, max_pages=8)
    assert cliente.ultima_pagina_pedida == 3, f"debio parar en la pagina 3, pidio {cliente.ultima_pagina_pedida}"


def test_deduplica_por_guid():
    """La paginacion se recorre mientras entran marcajes nuevos; puede repetir."""
    cliente = ClienteFalso([[registro("a", 5)], [registro("a", 5)], [], []])
    encontrados = cliente.fetch_records_since(start_dt=VENTANA, page_size=100, max_pages=8)
    assert len(encontrados) == 1, f"esperaba 1 registro deduplicado, obtuvo {len(encontrados)}"


if __name__ == "__main__":
    test_no_corta_por_pagina_desordenada()
    test_corta_tras_paginas_secas()
    test_deduplica_por_guid()
    print("OK: los 3 checks de paginacion pasan")
