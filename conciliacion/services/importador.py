from __future__ import annotations

import hashlib
import io
import re
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd
from django.db import transaction
from django.utils import timezone

from conciliacion.models import ImportacionBancaria
from sat_client.models import CfdiDescargado
from syncfy_client.models import CuentaBancaria, MovimientoBancario


MAX_PREVIEW_ROWS = 50
MAX_IMPORT_ROWS = 3000
MAX_UPLOAD_BYTES = 8 * 1024 * 1024
FORMATOS_SOPORTADOS = {"csv", "xlsx", "xlsm", "xls", "xml"}


class ImportacionBancariaError(ValueError):
    pass


@dataclass(frozen=True)
class MovimientoNormalizado:
    fecha: datetime
    descripcion: str
    monto: Decimal
    tipo: str
    moneda: str
    referencia: str
    saldo: Decimal | None
    fila: int
    raw: dict[str, Any]

    @property
    def monto_firmado(self) -> Decimal:
        return self.monto if self.tipo == MovimientoBancario.TIPO_ABONO else -self.monto

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["fecha"] = self.fecha.isoformat()
        payload["monto"] = str(self.monto)
        payload["saldo"] = str(self.saldo) if self.saldo is not None else ""
        payload["monto_firmado"] = str(self.monto_firmado)
        return payload

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "MovimientoNormalizado":
        fecha = datetime.fromisoformat(str(payload["fecha"]))
        if timezone.is_naive(fecha):
            fecha = timezone.make_aware(fecha, timezone=timezone.get_current_timezone())
        saldo_raw = payload.get("saldo")
        return cls(
            fecha=fecha,
            descripcion=str(payload.get("descripcion") or ""),
            monto=_decimal(payload.get("monto")),
            tipo=str(payload.get("tipo") or MovimientoBancario.TIPO_CARGO),
            moneda=str(payload.get("moneda") or "MXN"),
            referencia=str(payload.get("referencia") or ""),
            saldo=_decimal(saldo_raw) if saldo_raw not in (None, "") else None,
            fila=int(payload.get("fila") or 0),
            raw=payload.get("raw") if isinstance(payload.get("raw"), dict) else {},
        )


@dataclass(frozen=True)
class PreviewImportacion:
    cuenta_id: int
    archivo_nombre: str
    archivo_hash: str
    fuente: str
    movimientos: list[MovimientoNormalizado]
    errores: list[str]

    @property
    def total_filas(self) -> int:
        return len(self.movimientos) + len(self.errores)

    def to_session_payload(self) -> dict[str, Any]:
        return {
            "cuenta_id": self.cuenta_id,
            "archivo_nombre": self.archivo_nombre,
            "archivo_hash": self.archivo_hash,
            "fuente": self.fuente,
            "movimientos": [mov.to_payload() for mov in self.movimientos],
            "errores": self.errores,
        }

    @classmethod
    def from_session_payload(cls, payload: dict[str, Any]) -> "PreviewImportacion":
        return cls(
            cuenta_id=int(payload["cuenta_id"]),
            archivo_nombre=str(payload["archivo_nombre"]),
            archivo_hash=str(payload["archivo_hash"]),
            fuente=str(payload["fuente"]),
            movimientos=[MovimientoNormalizado.from_payload(item) for item in payload.get("movimientos", [])],
            errores=[str(item) for item in payload.get("errores", [])],
        )


def generar_preview(*, cuenta: CuentaBancaria, uploaded_file) -> PreviewImportacion:
    content = uploaded_file.read()
    if not content:
        raise ImportacionBancariaError("El archivo esta vacio.")
    if len(content) > MAX_UPLOAD_BYTES:
        raise ImportacionBancariaError("El archivo excede 8 MB.")

    nombre = str(uploaded_file.name or "estado_cuenta")
    suffix = nombre.rsplit(".", 1)[-1].lower() if "." in nombre else ""
    if suffix not in FORMATOS_SOPORTADOS:
        raise ImportacionBancariaError("Formato no soportado. Usa XML, CSV, XLSX, XLSM o XLS.")

    dataframe = _read_dataframe(content, suffix)
    if dataframe.empty:
        raise ImportacionBancariaError("No se encontraron filas para importar.")
    if len(dataframe) > MAX_IMPORT_ROWS:
        raise ImportacionBancariaError(f"El archivo tiene mas de {MAX_IMPORT_ROWS} filas.")

    movimientos: list[MovimientoNormalizado] = []
    errores: list[str] = []
    for index, raw_row in dataframe.iterrows():
        fila = int(index) + 2
        row = {_normalize_header(key): _clean_value(value) for key, value in raw_row.to_dict().items()}
        try:
            movimientos.append(_normalizar_movimiento(row=row, fila=fila))
        except ImportacionBancariaError as exc:
            errores.append(f"Fila {fila}: {exc}")

    if not movimientos:
        raise ImportacionBancariaError("No se pudo normalizar ningun movimiento del archivo.")

    return PreviewImportacion(
        cuenta_id=cuenta.pk,
        archivo_nombre=nombre,
        archivo_hash=hashlib.sha256(content).hexdigest(),
        fuente=_fuente_manual(suffix),
        movimientos=movimientos,
        errores=errores,
    )


@transaction.atomic
def confirmar_importacion(*, preview: PreviewImportacion, user) -> ImportacionBancaria:
    cuenta = CuentaBancaria.objects.select_for_update().get(pk=preview.cuenta_id)
    importacion = ImportacionBancaria.objects.create(
        cuenta=cuenta,
        fuente=preview.fuente,
        estado=ImportacionBancaria.ESTADO_IMPORTADA,
        archivo_nombre=preview.archivo_nombre,
        archivo_hash=preview.archivo_hash,
        total_filas=preview.total_filas,
        filas_con_error=len(preview.errores),
        preview=[mov.to_payload() for mov in preview.movimientos[:MAX_PREVIEW_ROWS]],
        errores=preview.errores[:MAX_PREVIEW_ROWS],
        creado_por=user if getattr(user, "is_authenticated", False) else None,
    )

    nuevos = 0
    duplicados = 0
    for movimiento in preview.movimientos:
        id_transaction = _manual_transaction_id(cuenta=cuenta, movimiento=movimiento)
        _, created = MovimientoBancario.objects.get_or_create(
            id_transaction=id_transaction,
            defaults={
                "cuenta": cuenta,
                "descripcion": movimiento.descripcion,
                "monto": movimiento.monto,
                "tipo": movimiento.tipo,
                "moneda": movimiento.moneda,
                "fecha_transaccion": movimiento.fecha,
                "fecha_refresh": timezone.now(),
                "extra_raw": {
                    "source": preview.fuente,
                    "archivo_hash": preview.archivo_hash,
                    "archivo_nombre": preview.archivo_nombre,
                    "referencia": movimiento.referencia,
                    "saldo": str(movimiento.saldo) if movimiento.saldo is not None else "",
                    "raw": movimiento.raw,
                },
            },
        )
        if created:
            nuevos += 1
        else:
            duplicados += 1

    importacion.movimientos_nuevos = nuevos
    importacion.movimientos_duplicados = duplicados
    importacion.save(update_fields=["movimientos_nuevos", "movimientos_duplicados", "actualizado_en"])
    return importacion


def resumen_conciliacion() -> dict[str, Any]:
    movimientos_qs = MovimientoBancario.objects.select_related("cuenta").order_by("-fecha_transaccion")
    pendientes = movimientos_qs.filter(conciliado=False)
    candidatos = sugerir_cfdis_para_movimientos(list(pendientes[:50]))
    return {
        "movimientos_total": movimientos_qs.count(),
        "movimientos_pendientes": pendientes.count(),
        "movimientos_conciliados": movimientos_qs.filter(conciliado=True).count(),
        "ultimos_movimientos": list(movimientos_qs[:20]),
        "candidatos": candidatos,
        "importaciones": list(ImportacionBancaria.objects.select_related("cuenta", "creado_por")[:10]),
    }


def sugerir_cfdis_para_movimientos(movimientos: list[MovimientoBancario]) -> dict[int, list[CfdiDescargado]]:
    sugerencias: dict[int, list[CfdiDescargado]] = {}
    for movimiento in movimientos:
        fecha = movimiento.fecha_transaccion.date()
        tipo_cfdi = CfdiDescargado.TIPO_EMITIDO if movimiento.tipo == MovimientoBancario.TIPO_ABONO else CfdiDescargado.TIPO_RECIBIDO
        qs = (
            CfdiDescargado.objects.filter(
                conciliado=False,
                tipo_cfdi=tipo_cfdi,
                total__gte=movimiento.monto - Decimal("1.00"),
                total__lte=movimiento.monto + Decimal("1.00"),
                fecha_emision__date__gte=fecha - timedelta(days=7),
                fecha_emision__date__lte=fecha + timedelta(days=7),
            )
            .order_by("-fecha_emision")[:5]
        )
        sugerencias[movimiento.pk] = list(qs)
    return sugerencias


def _read_dataframe(content: bytes, suffix: str) -> pd.DataFrame:
    buffer = io.BytesIO(content)
    if suffix == "csv":
        try:
            return pd.read_csv(buffer)
        except UnicodeDecodeError:
            return pd.read_csv(io.BytesIO(content), encoding="latin-1")
    if suffix == "xml":
        return _read_xml_dataframe(content)
    return pd.read_excel(buffer)


def _read_xml_dataframe(content: bytes) -> pd.DataFrame:
    try:
        root = ET.fromstring(content)
    except ET.ParseError as exc:
        raise ImportacionBancariaError("XML invalido.") from exc
    if _is_cfdi_xml(root):
        raise ImportacionBancariaError(
            "Este XML es un CFDI/factura, no un estado de cuenta bancario. "
            "Para conciliar, descarga movimientos de la cuenta en CSV, XLS, XLSX o XML tabular."
        )

    table_dataframe = _read_xml_table_dataframe(root)
    if not table_dataframe.empty:
        return table_dataframe

    candidates: dict[str, list[dict[str, Any]]] = {}
    for element in root.iter():
        row = _flatten_xml_element(element)
        if _looks_like_bank_row(row):
            candidates.setdefault(_xml_tag(element.tag), []).append(row)

    if not candidates:
        return pd.DataFrame()

    rows = max(candidates.values(), key=lambda items: (len(items), _xml_rows_score(items)))
    return pd.DataFrame(rows)


def _read_xml_table_dataframe(root: ET.Element) -> pd.DataFrame:
    tables: list[list[list[str]]] = []
    for element in root.iter():
        row_elements = [child for child in list(element) if _xml_tag(child.tag).lower() == "row"]
        rows = [_xml_row_values(row) for row in row_elements]
        rows = [row for row in rows if any(value for value in row)]
        if rows:
            tables.append(rows)

    best_rows: list[dict[str, Any]] = []
    for table in tables:
        rows = _xml_table_to_dict_rows(table)
        if len(rows) > len(best_rows):
            best_rows = rows
    return pd.DataFrame(best_rows)


def _is_cfdi_xml(root: ET.Element) -> bool:
    root_tag = _normalize_header(_xml_tag(root.tag))
    if root_tag != "comprobante":
        return False
    namespace = str(root.tag).split("}", 1)[0].strip("{") if "}" in str(root.tag) else ""
    if "sat.gob.mx/cfd" in namespace:
        return True
    attrs = {_normalize_header(key) for key in root.attrib}
    return {"tipo_de_comprobante", "sello", "certificado"} & attrs != set()


def _xml_row_values(row: ET.Element) -> list[str]:
    values: list[str] = []
    for cell in list(row):
        if _xml_tag(cell.tag).lower() != "cell":
            continue
        index = _xml_attr(cell, "Index")
        if index and str(index).isdigit():
            while len(values) < int(index) - 1:
                values.append("")
        values.append(_xml_cell_text(cell))
    return values


def _xml_cell_text(cell: ET.Element) -> str:
    texts: list[str] = []
    for element in cell.iter():
        if element is cell:
            continue
        text = (element.text or "").strip()
        if text:
            texts.append(text)
    if texts:
        return " ".join(texts).strip()
    return (cell.text or "").strip()


def _xml_table_to_dict_rows(table: list[list[str]]) -> list[dict[str, Any]]:
    for header_index, header in enumerate(table):
        if _xml_header_score(header) < 3:
            continue
        normalized_header = [str(value or "").strip() for value in header]
        rows: list[dict[str, Any]] = []
        for raw_values in table[header_index + 1 :]:
            row = {
                normalized_header[index]: raw_values[index] if index < len(raw_values) else ""
                for index in range(len(normalized_header))
                if normalized_header[index]
            }
            if _looks_like_bank_row(row):
                rows.append(row)
        if rows:
            return rows
    return []


def _xml_header_score(header: list[str]) -> int:
    normalized = {_normalize_header(value) for value in header if value}
    score = 0
    if normalized & {"fecha", "fecha_operacion", "fecha_de_operacion", "fecha_movimiento", "fecha_valor"}:
        score += 1
    if normalized & {"descripcion", "concepto", "detalle", "movimiento", "operacion"}:
        score += 1
    if normalized & {"cargo", "cargos", "retiro", "retiros", "debe"}:
        score += 1
    if normalized & {"abono", "abonos", "deposito", "depositos", "credito", "haber"}:
        score += 1
    if normalized & {"monto", "importe", "importe_movimiento", "valor", "importe_total"}:
        score += 1
    return score


def _flatten_xml_element(element: ET.Element) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for key, value in element.attrib.items():
        row[_xml_tag(key)] = value
    _flatten_xml_children(element, row=row, prefix="")
    return row


def _flatten_xml_children(element: ET.Element, *, row: dict[str, Any], prefix: str) -> None:
    for child in list(element):
        tag = _xml_tag(child.tag)
        key = f"{prefix}_{tag}" if prefix else tag
        text = (child.text or "").strip()
        if child.attrib:
            for attr_key, attr_value in child.attrib.items():
                row[f"{key}_{_xml_tag(attr_key)}"] = attr_value
        if list(child):
            _flatten_xml_children(child, row=row, prefix=key)
        elif text:
            row[key] = text


def _looks_like_bank_row(row: dict[str, Any]) -> bool:
    normalized = {_normalize_header(key): value for key, value in row.items()}
    has_date = _first(
        normalized,
        "fecha",
        "fecha_operacion",
        "fecha_de_operacion",
        "fecha_movimiento",
        "fecha_de_movimiento",
        "fecha_valor",
        "fecha_aplicacion",
        "fecha_contable",
        "date",
    ) not in (None, "")
    has_description = _first(
        normalized,
        "descripcion",
        "concepto",
        "concepto_pago",
        "detalle",
        "movimiento",
        "descripcion_movimiento",
        "operacion",
        "description",
        "referencia",
        "folio",
        "rastreo",
    ) not in (None, "")
    has_amount = _first(
        normalized,
        "cargo",
        "cargos",
        "importe_cargo",
        "retiro",
        "retiros",
        "debe",
        "abono",
        "abonos",
        "importe_abono",
        "deposito",
        "depositos",
        "credito",
        "creditos",
        "haber",
        "monto",
        "importe",
        "importe_movimiento",
        "amount",
        "valor",
        "importe_total",
    ) not in (None, "")
    return has_date and has_description and has_amount


def _xml_rows_score(rows: list[dict[str, Any]]) -> int:
    return sum(len(row) for row in rows)


def _xml_tag(value: str) -> str:
    return str(value).rsplit("}", 1)[-1]


def _xml_attr(element: ET.Element, name: str) -> str:
    normalized_name = _normalize_header(name)
    for key, value in element.attrib.items():
        if _normalize_header(_xml_tag(key)) == normalized_name:
            return str(value)
    return ""


def _fuente_manual(suffix: str) -> str:
    if suffix == "csv":
        return ImportacionBancaria.FUENTE_MANUAL_CSV
    return ImportacionBancaria.FUENTE_MANUAL_EXCEL


def _normalizar_movimiento(*, row: dict[str, Any], fila: int) -> MovimientoNormalizado:
    fecha = _parse_fecha(
        _first(
            row,
            "fecha",
            "fecha_operacion",
            "fecha_de_operacion",
            "fecha_movimiento",
            "fecha_de_movimiento",
            "fecha_valor",
            "fecha_aplicacion",
            "fecha_contable",
            "date",
        )
    )
    descripcion = str(
        _first(
            row,
            "descripcion",
            "concepto",
            "concepto_pago",
            "detalle",
            "movimiento",
            "descripcion_movimiento",
            "operacion",
            "description",
        )
        or ""
    ).strip()
    referencia = str(_first(row, "referencia", "folio", "autorizacion", "rastreo", "reference") or "").strip()
    if not descripcion and referencia:
        descripcion = referencia
    if not descripcion:
        raise ImportacionBancariaError("falta descripcion/concepto.")

    cargo = _decimal_or_none(_first(row, "cargo", "cargos", "importe_cargo", "retiro", "retiros", "debe"))
    abono = _decimal_or_none(
        _first(row, "abono", "abonos", "importe_abono", "deposito", "depositos", "credito", "creditos", "haber")
    )
    monto_directo = _decimal_or_none(_first(row, "monto", "importe", "importe_movimiento", "amount", "valor", "importe_total"))
    if cargo is not None or abono is not None:
        firmado = (abono or Decimal("0")) - (cargo or Decimal("0"))
    elif monto_directo is not None:
        firmado = _aplicar_tipo_movimiento(monto_directo, row)
    else:
        raise ImportacionBancariaError("falta monto, cargo o abono.")
    if firmado == 0:
        raise ImportacionBancariaError("monto en cero.")

    tipo = MovimientoBancario.TIPO_ABONO if firmado > 0 else MovimientoBancario.TIPO_CARGO
    return MovimientoNormalizado(
        fecha=fecha,
        descripcion=descripcion[:500],
        monto=abs(firmado),
        tipo=tipo,
        moneda=str(_first(row, "moneda", "currency") or "MXN").strip()[:10] or "MXN",
        referencia=referencia[:120],
        saldo=_decimal_or_none(_first(row, "saldo", "balance")),
        fila=fila,
        raw={str(key): _stringify(value) for key, value in row.items()},
    )


def _manual_transaction_id(*, cuenta: CuentaBancaria, movimiento: MovimientoNormalizado) -> str:
    fingerprint = "|".join(
        [
            str(cuenta.pk),
            movimiento.fecha.date().isoformat(),
            str(movimiento.monto_firmado),
            _compact(movimiento.descripcion),
            _compact(movimiento.referencia),
            str(movimiento.saldo or ""),
        ]
    )
    digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:32]
    return f"manual:{cuenta.pk}:{digest}"


def _parse_fecha(value: Any) -> datetime:
    if value in (None, ""):
        raise ImportacionBancariaError("falta fecha.")
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    else:
        text = str(value).strip()
        dayfirst = not bool(re.match(r"^\d{4}-\d{2}-\d{2}(?:[T\\s]|$)", text))
        parsed_ts = pd.to_datetime(text, dayfirst=dayfirst, errors="coerce")
        if pd.isna(parsed_ts):
            raise ImportacionBancariaError("fecha invalida.")
        parsed = parsed_ts.to_pydatetime()
    if timezone.is_naive(parsed):
        parsed = timezone.make_aware(parsed, timezone=timezone.get_current_timezone())
    return parsed


def _normalize_header(value: Any) -> str:
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(value or "").strip()).lower()
    replacements = str.maketrans("áéíóúüñ", "aeiouun")
    text = text.translate(replacements)
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text


def _aplicar_tipo_movimiento(monto: Decimal, row: dict[str, Any]) -> Decimal:
    tipo_raw = str(
        _first(row, "tipo", "tipo_movimiento", "tipo_operacion", "naturaleza", "cargo_abono", "signo") or ""
    ).strip()
    tipo = _normalize_header(tipo_raw)
    if tipo in {"cargo", "cargos", "retiro", "retiros", "debito", "debe", "egreso", "salida", "minus", "negativo"}:
        return -abs(monto)
    if tipo in {"abono", "abonos", "deposito", "depositos", "credito", "haber", "ingreso", "entrada", "plus", "positivo"}:
        return abs(monto)
    return monto


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        normalized = _normalize_header(key)
        if normalized in row and row[normalized] not in (None, ""):
            return row[normalized]
        suffix = f"_{normalized}"
        for row_key, value in row.items():
            if str(row_key).endswith(suffix) and value not in (None, ""):
                return value
    return None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return _decimal(value)


def _decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    text = str(value or "0").strip()
    if not text:
        return Decimal("0")
    negative = text.startswith("(") and text.endswith(")")
    text = text.replace("$", "").replace(",", "").replace("MXN", "").replace("USD", "").strip("() ")
    try:
        amount = Decimal(text)
    except (InvalidOperation, TypeError) as exc:
        raise ImportacionBancariaError("monto invalido.") from exc
    return -amount if negative else amount


def _clean_value(value: Any) -> Any:
    if pd.isna(value):
        return ""
    return value


def _stringify(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value or "")


def _compact(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())
