from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import TIMEZONE
from hikconnect_client import CloudPage, CloudRecord, HikConnectClient, _record_from_api


INICIO = datetime(2026, 7, 1, 0, 0, tzinfo=TIMEZONE)
FIN = datetime(2026, 7, 31, 23, 59, 59, tzinfo=TIMEZONE)


def registro(guid: str, dt: datetime = INICIO) -> CloudRecord:
    return CloudRecord(
        record_guid=guid,
        employee_no="350",
        name="Prueba",
        department="MATRIZ 2",
        device_time=dt,
        device_name="DS-K1T341CMFW",
        device_serial_no="FN9490470",
        raw={},
    )


def test_pagina_envia_el_mismo_filtro_de_fechas_que_la_nube_humana():
    llamadas = []

    class Response:
        status = 200

        def json(self):
            return {"errorCode": "0", "data": {"totalNum": 0, "recordList": []}}

    def post(url, **kwargs):
        llamadas.append((url, kwargs))
        return Response()

    cliente = HikConnectClient.__new__(HikConnectClient)
    cliente.page = SimpleNamespace(request=SimpleNamespace(post=post))

    pagina = cliente.fetch_records_page(
        page_index=1,
        page_size=100,
        require_login=False,
        start_dt=INICIO,
        end_dt=FIN,
    )

    criteria = llamadas[0][1]["data"]["searchCriteria"]
    assert criteria["beginTime"] == "2026-07-01T00:00:00+00:00"
    assert criteria["endTime"] == "2026-07-31T23:59:59+00:00"
    assert criteria["type"] == 0
    assert pagina.total_num == 0


def test_2940_registros_terminan_en_pagina_30_sin_solicitar_101():
    class ClienteFalso(HikConnectClient):
        def __init__(self):
            self.paginas = []

        def ensure_login(self):
            pass

        def fetch_records_page(
            self,
            page_index,
            page_size,
            require_login=True,
            start_dt=None,
            end_dt=None,
        ):
            self.paginas.append(page_index)
            cantidad = 40 if page_index == 30 else 100
            return CloudPage(
                [registro(f"guid-{page_index}-{i}") for i in range(cantidad)],
                [],
                total_num=2940,
            )

    cliente = ClienteFalso()
    encontrados = cliente.fetch_records_between(
        start_dt=datetime(2026, 7, 31, 0, 0, tzinfo=TIMEZONE),
        end_dt=datetime(2026, 7, 31, 23, 59, 59, tzinfo=TIMEZONE),
        page_size=100,
        max_pages=100,
    )

    assert len(encontrados) == 2940
    assert cliente.paginas == list(range(1, 31))
    assert encontrados.complete is True


def test_cada_dia_reinicia_en_pagina_uno():
    class ClienteFalso(HikConnectClient):
        def __init__(self):
            self.ventanas = []

        def ensure_login(self):
            pass

        def fetch_records_page(
            self,
            page_index,
            page_size,
            require_login=True,
            start_dt=None,
            end_dt=None,
        ):
            self.ventanas.append((start_dt.date().isoformat(), page_index))
            return CloudPage([], [], total_num=0)

    cliente = ClienteFalso()
    cliente.fetch_records_between(
        start_dt=datetime(2026, 7, 29, 10, 0, tzinfo=TIMEZONE),
        end_dt=datetime(2026, 8, 1, 18, 0, tzinfo=TIMEZONE),
        page_size=100,
        max_pages=100,
    )

    assert cliente.ventanas == [
        ("2026-08-01", 1),
        ("2026-07-31", 1),
        ("2026-07-30", 1),
        ("2026-07-29", 1),
    ]


def test_extrae_serial_real_del_lector_para_distinguir_checadores():
    record, reason = _record_from_api(
        {
            "recordGuid": "guid-dispositivo",
            "deviceTime": "2026-08-01T10:03:01-07:00",
            "deviceId": "7ecd7dfd9009498382eda98d06d9b291",
            "deviceName": "DS-K1T341CMFW",
            "devSerialNo": 599,
            "cardReaderName": "FN9490470-Cardreader 01",
            "personInfo": {
                "baseInfo": {
                    "personCode": "170",
                    "firstName": "GARCIA",
                    "lastName": "HIGUERA CLARISELA",
                    "fullPath": "MATRIZ 2",
                }
            },
        }
    )

    assert reason == ""
    assert record is not None
    assert record.device_serial_no == "FN9490470"
