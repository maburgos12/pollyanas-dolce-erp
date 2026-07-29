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


def test_conserva_device_time_viejo_por_subida_reciente():
    """No filtra por deviceTime: la nube pagina por momento de subida."""
    cliente = ClienteFalso([[registro("viejo", 60 * 48)], [registro("reciente", 10)], []])
    encontrados = cliente.fetch_records_since(start_dt=VENTANA, page_size=100, max_pages=8)
    guids = {record.record_guid for record in encontrados}
    assert guids == {"viejo", "reciente"}, f"debio conservar ambos marcajes, obtuvo {guids}"
    assert encontrados.complete is True
    assert encontrados.next_page == 1


def test_agotamiento_de_paginas_deja_continuacion_pendiente():
    cliente = ClienteFalso([[registro(f"viejo{i}", 60 * 48)] for i in range(8)])
    encontrados = cliente.fetch_records_since(start_dt=VENTANA, page_size=100, max_pages=8)
    assert cliente.ultima_pagina_pedida == 8
    assert encontrados.complete is False
    assert encontrados.next_page == 9


def test_deduplica_por_guid():
    """La paginacion se recorre mientras entran marcajes nuevos; puede repetir."""
    cliente = ClienteFalso([[registro("a", 5)], [registro("a", 5)], [], []])
    encontrados = cliente.fetch_records_since(start_dt=VENTANA, page_size=100, max_pages=8)
    assert len(encontrados) == 1, f"esperaba 1 registro deduplicado, obtuvo {len(encontrados)}"


def test_reanuda_desde_pagina_pendiente():
    cliente = ClienteFalso(
        [
            [registro("pagina-1", 1)],
            [registro("pagina-2", 2)],
            [registro("pagina-3", 3)],
            [],
        ]
    )
    encontrados = cliente.fetch_records_since(
        start_dt=VENTANA,
        page_size=100,
        max_pages=3,
        start_page=2,
    )
    assert {record.record_guid for record in encontrados} == {"pagina-1", "pagina-2", "pagina-3"}
    assert encontrados.complete is False
    assert encontrados.next_page == 4


if __name__ == "__main__":
    test_conserva_device_time_viejo_por_subida_reciente()
    test_agotamiento_de_paginas_deja_continuacion_pendiente()
    test_deduplica_por_guid()
    test_reanuda_desde_pagina_pendiente()
    print("OK: los 4 checks de paginacion pasan")
