from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urljoin

import requests
from django.core.exceptions import ValidationError
from django.core.validators import validate_email
from django.utils import timezone

from pos_bridge.config import PointBridgeSettings, load_point_bridge_settings
from pos_bridge.services.point_http_session_service import PointHttpSessionService
from pos_bridge.services.point_note_detail_service import PointNote, PointNoteDetailService
from pos_bridge.utils.exceptions import ExtractionError


class PointDeliveryError(ExtractionError):
    """Point did not return a usable delivery-note contract."""


class PointDeliveryUnavailableError(PointDeliveryError):
    """The delivery tray could not be reached and may be retried."""


class PointDeliveryContractError(PointDeliveryError):
    """Point answered with an incompatible delivery payload."""


@dataclass(frozen=True)
class PointDeliveryNote:
    note: PointNote
    customer_external_id: str
    customer_name: str
    customer_phone: str
    customer_email: str
    address: str
    references: str
    point_address_id: str


@dataclass(frozen=True)
class PointDeliveryFailure:
    point_note_id: str
    error_code: str


@dataclass(frozen=True)
class PointDeliveryBatch:
    seen_count: int
    notes: tuple[PointDeliveryNote, ...]
    failures: tuple[PointDeliveryFailure, ...] = ()

    def __iter__(self):
        return iter(self.notes)

    def __len__(self):
        return len(self.notes)

    def __getitem__(self, index):
        return self.notes[index]


class PointDeliveryNoteService:
    TRAY_PATH = "/Ventas/getNotasByDateServicioDomicilio"
    DELIVERY_CUSTOMER_PATH = "/Ventas/getDataClienteById"
    ACTIVE_CUSTOMERS_PATH = "/Clientes/get_clientes_byActivo"
    _TRAY_FIELDS = (
        "PK_Nota",
        "Folio",
        "Sucursal",
        "Fecha_Hora_Cierre",
        "Total",
        "Facturado",
    )
    _DELIVERY_CUSTOMER_FIELDS = (
        "PK_Cliente",
        "Cliente",
        "Calle",
        "NoExterior",
        "NoInterior",
        "Colonia",
        "EntreCalles",
        "Observaciones",
        "PK_Direccion_Entrega",
    )

    def __init__(
        self,
        bridge_settings: PointBridgeSettings | None = None,
        http_session_service: PointHttpSessionService | None = None,
        note_detail_service: PointNoteDetailService | None = None,
    ):
        self.settings = bridge_settings or load_point_bridge_settings()
        self.http_session_service = http_session_service or PointHttpSessionService(
            self.settings,
        )
        self.note_detail_service = note_detail_service or PointNoteDetailService(
            bridge_settings=self.settings,
            http_session_service=self.http_session_service,
        )

    def fetch_range(
        self,
        *,
        start_date: date,
        end_date: date,
        branch_external_id: str = "",
        branch_display_name: str = "",
        exclude_note_ids: set[str] | frozenset[str] | None = None,
    ) -> PointDeliveryBatch:
        if not isinstance(start_date, date) or not isinstance(end_date, date):
            raise PointDeliveryContractError(
                "La ventana de domicilios requiere fechas válidas.",
            )
        if end_date < start_date:
            raise PointDeliveryContractError(
                "La fecha final no puede ser anterior a la inicial.",
            )

        branch_id = str(branch_external_id or "").strip()
        branch_name = str(branch_display_name or "").strip()
        auth_session = self.http_session_service.create(
            branch_external_id=branch_id or None,
            branch_display_name=branch_name or None,
            strict_branch=True,
        )
        session = auth_session.session
        try:
            tray = self._get_json(
                session,
                path=self.TRAY_PATH,
                params={
                    "fecha_inicio": self._epoch_milliseconds(start_date),
                    "fecha_final": (
                        self._epoch_milliseconds(end_date + timedelta(days=1)) - 1
                    ),
                    "id_sucursal": branch_id,
                },
                label="bandeja de domicilios",
            )
            rows = self._rows(tray, label="bandeja de domicilios", allow_empty=True)
            excluded = {
                str(value).strip()
                for value in (exclude_note_ids or ())
            }
            notes = []
            failures = []
            for row in rows:
                point_note_id = self._optional_text(row.get("PK_Nota"))
                try:
                    point_note_id = self._required_text(point_note_id, field="PK_Nota")
                    if point_note_id in excluded:
                        continue
                    notes.append(self._normalize_delivery(session, row))
                except ExtractionError as exc:
                    failures.append(
                        PointDeliveryFailure(
                            point_note_id=point_note_id,
                            error_code=self._safe_error_code(exc),
                        ),
                    )
            return PointDeliveryBatch(
                seen_count=len(rows),
                notes=tuple(notes),
                failures=tuple(failures),
            )
        finally:
            try:
                session.close()
            except Exception:  # noqa: BLE001
                pass

    def _normalize_delivery(self, session, row: dict[str, Any]) -> PointDeliveryNote:
        self._require_fields(row, self._TRAY_FIELDS, label="bandeja de domicilios")
        pk_nota = self._required_text(row["PK_Nota"], field="PK_Nota")
        tray_folio = self._required_text(row["Folio"], field="Folio")
        tray_branch = self._required_text(row["Sucursal"], field="Sucursal")
        tray_total = self._decimal(row["Total"], field="Total")

        note = self.note_detail_service.fetch_with_session(session, pk_nota=pk_nota)
        if note.pk_nota != pk_nota or note.folio != tray_folio:
            raise PointDeliveryContractError(
                "La bandeja y el detalle Point identifican notas distintas.",
            )
        if note.branch_name != tray_branch or note.total != tray_total:
            raise PointDeliveryContractError(
                "La bandeja y el detalle Point contienen datos incompatibles.",
            )

        customer_payload = self._get_json(
            session,
            path=self.DELIVERY_CUSTOMER_PATH,
            params={"id_nota": pk_nota},
            label="cliente de domicilio",
        )
        customer = self._one_row(customer_payload, label="cliente de domicilio")
        self._require_fields(
            customer,
            self._DELIVERY_CUSTOMER_FIELDS,
            label="cliente de domicilio",
        )
        customer_id = self._required_text(customer["PK_Cliente"], field="PK_Cliente")

        catalog_payload = self._get_json(
            session,
            path=self.ACTIVE_CUSTOMERS_PATH,
            params={
                "activo": True,
                "id_sucursal": "null",
                "texto": self._required_text(customer["Cliente"], field="Cliente"),
                "id_tipo_membresia": "null",
            },
            label="catálogo de clientes",
        )
        catalog_rows = self._rows(
            catalog_payload,
            label="catálogo de clientes",
            allow_empty=True,
        )
        catalog_customer = next(
            (
                item
                for item in catalog_rows
                if self._optional_text(
                    item.get("Pk_cliente") or item.get("PK_Cliente"),
                ) == customer_id
            ),
            None,
        )
        contact = catalog_customer or {}
        phone = self._first_phone(contact)
        email = self._valid_email(
            self._first_text(
                contact,
                "Correo",
                "correo",
                "Email",
                "email",
            ),
        )
        return PointDeliveryNote(
            note=note,
            customer_external_id=customer_id,
            customer_name=self._required_text(customer["Cliente"], field="Cliente"),
            customer_phone=phone,
            customer_email=email,
            address=self._address(customer),
            references=self._references(customer),
            point_address_id=self._optional_text(customer["PK_Direccion_Entrega"]),
        )

    def _get_json(self, session, *, path: str, params: dict, label: str) -> Any:
        url = urljoin(self.settings.base_url.rstrip("/") + "/", path.lstrip("/"))
        try:
            response = session.get(
                url,
                params=params,
                timeout=self.settings.timeout_ms / 1000,
            )
            response.raise_for_status()
        except requests.RequestException:
            raise PointDeliveryUnavailableError(
                f"Falló la consulta de {label} en Point.",
            ) from None
        try:
            return response.json()
        except (TypeError, ValueError):
            try:
                return json.loads(response.text)
            except (TypeError, ValueError, json.JSONDecodeError):
                raise PointDeliveryContractError(
                    f"Point devolvió JSON inválido en {label}.",
                ) from None

    @staticmethod
    def _epoch_milliseconds(day: date) -> int:
        local = timezone.make_aware(
            datetime.combine(day, time.min),
            timezone.get_default_timezone(),
        )
        return int(local.timestamp() * 1000)

    def _rows(self, payload: Any, *, label: str, allow_empty: bool) -> list[dict[str, Any]]:
        if isinstance(payload, dict):
            if payload.get("hasError") is not False or "data" not in payload:
                raise PointDeliveryContractError(
                    f"Point devolvió una estructura inválida en {label}.",
                )
            payload = payload["data"]
        if payload == [] and allow_empty:
            return []
        if (
            not isinstance(payload, list)
            or not payload
            or not all(isinstance(item, dict) for item in payload)
        ):
            raise PointDeliveryContractError(f"Point devolvió una estructura inválida en {label}.")
        return payload

    def _one_row(self, payload: Any, *, label: str) -> dict[str, Any]:
        if isinstance(payload, dict):
            return payload
        rows = self._rows(payload, label=label, allow_empty=False)
        if len(rows) != 1:
            raise PointDeliveryContractError(f"Point devolvió una estructura inválida en {label}.")
        return rows[0]

    @staticmethod
    def _require_fields(
        row: dict[str, Any],
        fields: tuple[str, ...],
        *,
        label: str,
    ) -> None:
        missing = [field for field in fields if field not in row]
        if missing:
            raise PointDeliveryContractError(
                f"Point omitió campos obligatorios en {label}: {', '.join(missing)}.",
            )

    def _address(self, customer: dict[str, Any]) -> str:
        street = self._required_text(customer["Calle"], field="Calle")
        number = " ".join(
            value
            for value in (
                self._optional_text(customer["NoExterior"]),
                self._optional_text(customer["NoInterior"]),
            )
            if value
        )
        colony = self._optional_text(customer["Colonia"])
        address = " ".join(value for value in (street, number) if value)
        if colony:
            address = f"{address}, Colonia {colony}"
        return address

    def _references(self, customer: dict[str, Any]) -> str:
        parts = []
        between = self._optional_text(customer["EntreCalles"])
        observations = self._optional_text(customer["Observaciones"])
        if between:
            parts.append(f"Entre calles: {between}")
        if observations:
            parts.append(f"Observaciones: {observations}")
        return ". ".join(parts)

    @staticmethod
    def _optional_text(value: Any) -> str:
        return str(value or "").strip()

    def _required_text(self, value: Any, *, field: str) -> str:
        text = self._optional_text(value)
        if not text:
            raise PointDeliveryContractError(f"Point devolvió un texto vacío en {field}.")
        return text

    def _decimal(self, value: Any, *, field: str) -> Decimal:
        try:
            result = Decimal(
                str(value).replace("$", "").replace(",", "").strip(),
            ).quantize(Decimal("0.01"))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise PointDeliveryContractError(f"Point devolvió un decimal inválido en {field}.") from exc
        if not result.is_finite() or result < 0:
            raise PointDeliveryContractError(f"Point devolvió un decimal inválido en {field}.")
        return result

    @staticmethod
    def _first_text(row: dict[str, Any], *fields: str) -> str:
        for field in fields:
            value = str(row.get(field) or "").strip()
            if value:
                return value
        return ""

    @staticmethod
    def _valid_email(value: Any) -> str:
        email = str(value or "").strip().lower()
        if not email:
            return ""
        try:
            validate_email(email)
        except ValidationError:
            return ""
        return email

    def _first_phone(self, row: dict[str, Any]) -> str:
        raw = self._first_text(
            row,
            "telefono1",
            "Telefono1",
            "telefono2",
            "Telefono2",
        )
        return re.sub(r"\D", "", raw)

    @staticmethod
    def _safe_error_code(exc: Exception) -> str:
        name = exc.__class__.__name__.upper()
        if "CONTRACT" in name or "INTEGRITY" in name:
            return "POINT_CONTRACT"
        if "UNAVAILABLE" in name:
            return "POINT_UNAVAILABLE"
        return "POINT_NOTE_PROCESSING"
