from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any
from urllib.parse import urljoin

import requests

from pos_bridge.config import PointBridgeSettings, load_point_bridge_settings
from pos_bridge.services.point_http_session_service import PointHttpSessionService
from pos_bridge.utils.exceptions import ExtractionError


class PointNoteDetailError(ExtractionError):
    """Point did not return a usable note-detail contract."""


class PointNoteIntegrityError(PointNoteDetailError):
    """The header and line-level facts for a Point note do not reconcile."""


@dataclass(frozen=True)
class PointNoteLine:
    point_code: str
    description: str
    quantity: Decimal
    unit_price: Decimal
    discount: Decimal
    line_total: Decimal


@dataclass(frozen=True)
class PointNote:
    pk_nota: str
    folio: str
    branch_name: str
    sold_at: datetime
    total: Decimal
    invoiced: bool
    payment_type: str
    point_channel: str
    lines: tuple[PointNoteLine, ...]
    source_endpoint: str
    customer_external_id: str | None = None


class PointNoteDetailService:
    HEADER_PATH = "/Clientes/get_encabezado_nota/"
    DETAIL_PATH = "/Clientes/get_detalle_nota/"
    _HEADER_REQUIRED_FIELDS = (
        "Folio",
        "FK_Sucursal",
        "Sucursal",
        "Fecha_Hora",
        "Importe",
        "isCredito",
        "isFacturado",
    )
    _LINE_REQUIRED_FIELDS = (
        "Codigo",
        "Producto",
        "Cantidad",
        "Precio_Venta",
        "Descuento",
        "Descuento_p",
        "Total",
    )
    _MONEY_QUANTUM = Decimal("0.01")

    def __init__(
        self,
        bridge_settings: PointBridgeSettings | None = None,
        http_session_service: PointHttpSessionService | None = None,
    ):
        self.settings = bridge_settings or load_point_bridge_settings()
        self.http_session_service = http_session_service or PointHttpSessionService(self.settings)

    def fetch(self, *, pk_nota: str) -> PointNote:
        note_id = str(pk_nota or "").strip()
        if not note_id:
            raise PointNoteDetailError("PK_NOTA es obligatorio para consultar Point.")

        auth_session = self.http_session_service.create()
        try:
            header_payload = self._get_payload(
                auth_session.session,
                path=self.HEADER_PATH,
                pk_nota=note_id,
                label="cabecera",
            )
            detail_payload = self._get_payload(
                auth_session.session,
                path=self.DETAIL_PATH,
                pk_nota=note_id,
                label="detalle",
            )
        finally:
            try:
                auth_session.session.close()
            except Exception:  # noqa: BLE001
                pass

        header_rows = self._require_rows(header_payload, label="cabecera", empty_means_not_found=True)
        detail_rows = self._require_rows(detail_payload, label="detalle", empty_means_not_found=True)
        if len(header_rows) != 1:
            raise PointNoteDetailError("Point devolvió una estructura inválida en cabecera de nota.")

        header = header_rows[0]
        self._require_fields(header, self._HEADER_REQUIRED_FIELDS, label="cabecera")
        lines = tuple(self._normalize_line(row, index=index) for index, row in enumerate(detail_rows))
        total = self._decimal(header["Importe"], field="Importe", minimum=Decimal("0"))
        line_total = sum((line.line_total for line in lines), Decimal("0"))
        if self._currency(total) != self._currency(line_total):
            raise PointNoteIntegrityError(
                "El total de cabecera Point no coincide con la suma de Total de sus líneas.",
                context={
                    "pk_nota": note_id,
                    "header_total": str(total),
                    "line_total": str(line_total),
                },
            )

        is_credit = self._optional_boolean(header.get("isCredito"), field="isCredito", default=False)
        return PointNote(
            pk_nota=note_id,
            folio=self._required_text(header["Folio"], field="Folio"),
            branch_name=self._required_text(header["Sucursal"], field="Sucursal"),
            sold_at=self._datetime(header["Fecha_Hora"], field="Fecha_Hora"),
            total=total,
            invoiced=self._boolean(header["isFacturado"], field="isFacturado"),
            payment_type=self._optional_text(header.get("TIPO")) or ("CREDITO" if is_credit else "CONTADO"),
            point_channel=self._optional_text(header.get("CANAL_VENTA")),
            lines=lines,
            source_endpoint=self.DETAIL_PATH,
            customer_external_id=self._optional_text(header.get("FK_Cliente")) or None,
        )

    def _get_payload(self, session, *, path: str, pk_nota: str, label: str) -> Any:
        url = urljoin(self.settings.base_url.rstrip("/") + "/", path.lstrip("/"))
        try:
            response = session.get(
                url,
                params={"id_nota": pk_nota},
                timeout=self.settings.timeout_ms / 1000,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise PointNoteDetailError(f"Falló la consulta de {label} de nota en Point ({path}).") from exc

        try:
            return response.json()
        except (TypeError, ValueError):
            try:
                return json.loads(response.text)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise PointNoteDetailError(f"Point devolvió JSON inválido en {label} ({path}).") from exc

    def _require_rows(
        self,
        payload: Any,
        *,
        label: str,
        empty_means_not_found: bool = False,
    ) -> list[dict[str, Any]]:
        if empty_means_not_found and payload == []:
            raise PointNoteDetailError("La nota solicitada no existe en Point.")
        if not isinstance(payload, list) or not payload or not all(isinstance(row, dict) for row in payload):
            raise PointNoteDetailError(f"Point devolvió una estructura inválida en {label} de nota.")
        return payload

    def _require_fields(self, row: dict[str, Any], fields: tuple[str, ...], *, label: str) -> None:
        missing = [field for field in fields if field not in row]
        if missing:
            raise PointNoteDetailError(f"Point omitió campos obligatorios en {label}: {', '.join(missing)}.")

    def _normalize_line(self, row: dict[str, Any], *, index: int) -> PointNoteLine:
        self._require_fields(row, self._LINE_REQUIRED_FIELDS, label=f"detalle fila {index}")
        self._decimal(
            row["Descuento_p"],
            field=f"Descuento_p fila {index}",
            minimum=Decimal("0"),
            maximum=Decimal("100"),
        )
        return PointNoteLine(
            point_code=self._required_text(row["Codigo"], field=f"Codigo fila {index}"),
            description=self._required_text(row["Producto"], field=f"Producto fila {index}"),
            quantity=self._decimal(
                row["Cantidad"],
                field=f"Cantidad fila {index}",
                minimum=Decimal("0"),
                minimum_inclusive=False,
            ),
            unit_price=self._decimal(
                row["Precio_Venta"],
                field=f"Precio_Venta fila {index}",
                minimum=Decimal("0"),
            ),
            discount=self._decimal(
                row["Descuento"],
                field=f"Descuento fila {index}",
                minimum=Decimal("0"),
            ),
            line_total=self._decimal(
                row["Total"],
                field=f"Total fila {index}",
                minimum=Decimal("0"),
            ),
        )

    def _decimal(
        self,
        value: Any,
        *,
        field: str,
        minimum: Decimal | None = None,
        maximum: Decimal | None = None,
        minimum_inclusive: bool = True,
    ) -> Decimal:
        try:
            normalized = str(value).replace("$", "").replace(",", "").strip()
            result = Decimal(normalized)
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise PointNoteDetailError(f"Point devolvió un decimal inválido en {field}.") from exc
        if not result.is_finite():
            raise PointNoteDetailError(f"Point devolvió un decimal inválido en {field}.")
        if minimum is not None and (
            result < minimum or (not minimum_inclusive and result == minimum)
        ):
            raise PointNoteDetailError(f"Point devolvió un valor inválido en {field}.")
        if maximum is not None and result > maximum:
            raise PointNoteDetailError(f"Point devolvió un valor inválido en {field}.")
        return result

    def _currency(self, value: Decimal) -> Decimal:
        return value.quantize(self._MONEY_QUANTUM, rounding=ROUND_HALF_UP)

    def _datetime(self, value: Any, *, field: str) -> datetime:
        text = self._required_text(value, field=field)
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            for date_format in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"):
                try:
                    return datetime.strptime(text, date_format)
                except ValueError:
                    continue
        raise PointNoteDetailError(f"Point devolvió una fecha inválida en {field}.")

    def _boolean(self, value: Any, *, field: str) -> bool:
        if isinstance(value, bool):
            return value
        if value in (1, 0):
            return bool(value)
        token = str(value or "").strip().casefold()
        if token in {"si", "sí", "true", "yes", "1"}:
            return True
        if token in {"no", "false", "0"}:
            return False
        raise PointNoteDetailError(f"Point devolvió un booleano inválido en {field}.")

    def _optional_boolean(self, value: Any, *, field: str, default: bool) -> bool:
        if value is None or str(value).strip() == "":
            return default
        return self._boolean(value, field=field)

    def _required_text(self, value: Any, *, field: str) -> str:
        text = self._optional_text(value)
        if not text:
            raise PointNoteDetailError(f"Point devolvió un texto vacío en {field}.")
        return text

    @staticmethod
    def _optional_text(value: Any) -> str:
        return str(value or "").strip()
