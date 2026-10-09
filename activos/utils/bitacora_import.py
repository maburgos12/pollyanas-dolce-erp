from __future__ import annotations

import csv
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any, BinaryIO

import hashlib
from zipfile import ZipFile

from openpyxl import load_workbook



SECTION_HINTS = (
    "PRODUCCION",
    "VENTAS",
    "MATRIZ",
    "SUCURSAL",
    "ALMACEN",
    "OFICINA",
    "AREA",
    "HORNOS",
    "CEDIS",
    "LOGISTICA",
    "LEYVA",
    "PAYAN",
    "GLORIAS",
    "COLOSIO",
    "CRUCERO",
    "NIO",
    "TUNEL",
    "GUAMUCHIL",
)

# Nombres exactos de sección reconocidos (mayúsculas)
KNOWN_SECTIONS: frozenset[str] = frozenset({
    "HORNOS",
    "LEYVA",
    "PAYAN",
    "GLORIAS",
    "COLOSIO",
    "CRUCERO",
    "NIO",
    "TUNEL",
    "GUAMUCHIL",
    "LOGISTICA",
    "OFICINA VENTAS",
    "PRODUCCION CEDIS",
    "PRODUCCION CRUCERO",
    "OFICINA PRODUCCIÓN",
    "OFICINA PRODUCCION",
    "VENTAS",
    "MATRIZ",
})

# Secciones cuyo contenido se omite completamente
SKIP_SECTIONS: frozenset[str] = frozenset({"LOGISTICA"})

# Series que indican trabajo civil/instalación, no activo físico
SKIP_SERIES_VALUES: frozenset[str] = frozenset({"INSTALACION"})

# Nombres de equipo que, aunque tengan serie de servicio, sí son activos físicos
FORCE_INCLUDE_NAMES: frozenset[str] = frozenset({"CORTINA METALICA"})


MAX_FILE_BYTES = 2 * 1024 * 1024
MAX_SOURCE_ROWS = 1000


def leer_archivo(archivo):
    """Bytes exactos; el nombre sólo elige el formato, nunca la identidad."""
    nombre = _get_source_name(archivo)
    if Path(nombre).suffix.lower() not in {'.csv', '.tsv', '.xlsx'}:
        raise ValueError('Selecciona un archivo CSV o XLSX.')
    if isinstance(archivo, (str, Path)):
        with open(archivo, 'rb') as stream:
            raw = stream.read(MAX_FILE_BYTES + 1)
    else:
        archivo.seek(0)
        raw = archivo.read(MAX_FILE_BYTES + 1)
        archivo.seek(0)
    if not isinstance(raw, bytes) or not raw or len(raw) > MAX_FILE_BYTES:
        raise ValueError('El archivo debe contener datos y no superar 2 MB.')
    return nombre, raw


def preview_bitacora(archivo, *, sheet_name=''):
    """Parser puro: ni consultas, ni escrituras, ni signals, ni secuencias."""
    nombre, raw = leer_archivo(archivo)
    stream = BytesIO(raw); stream.name = nombre
    try:
        source = _build_source_rows(stream, sheet_name)
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError('No se pudo leer el CSV/XLSX. Revisa el formato y las columnas del archivo.') from exc
    servicios = []
    ubicacion = ''
    validas = 0
    for fila, row in source['rows']:
        texts = [_as_text(row.get(key)) for key in ('nombre','marca','modelo','serie')]
        name, brand, model, serial = texts
        values = [row.get(key) for key in ('fecha_1','costo_1','fecha_2','costo_2')]
        if _is_section_row(*texts, *values):
            ubicacion = name
            continue
        if _is_header_row(*texts):
            if name and _looks_like_section(name):
                ubicacion = name
            continue
        if not name or ubicacion.upper() in SKIP_SECTIONS:
            continue
        if serial.upper() in SKIP_SERIES_VALUES and name.upper() not in FORCE_INCLUDE_NAMES:
            continue
        validas += 1
        for slot in (1, 2):
            fecha_raw, costo_raw = row.get(f'fecha_{slot}'), row.get(f'costo_{slot}')
            if not _as_text(fecha_raw) and not _as_text(costo_raw):
                continue
            fecha, costo = _as_date(fecha_raw), _as_decimal(costo_raw)
            servicios.append(dict(fila=fila, slot=slot, nombre=name, marca=brand, modelo=model,
                serie=serial, ubicacion=ubicacion, fecha=fecha.isoformat() if fecha else None,
                costo=str(costo) if costo is not None else None,
                fecha_fuente=_as_text(fecha_raw), costo_fuente=_as_text(costo_raw)))
    return dict(filename=Path(nombre).name, sha256=hashlib.sha256(raw).hexdigest(),
                sheet_name=source['sheet_name'], source_format=source['source_format'],
                filas_leidas=len(source['rows']), filas_validas=validas, servicios=servicios,
                activos_creados=0, activos_actualizados=0, servicios_creados=0, servicios_omitidos=0)


def import_bitacora(archivo, *, sheet_name='', dry_run=False, skip_servicios=False):
    """Compatibilidad segura: sólo lectura. Apply exige actor y revisión explícitos."""
    if not dry_run:
        raise ValueError('La importación requiere vista previa, equipo existente y decisiones revisadas.')
    return preview_bitacora(archivo, sheet_name=sheet_name)


def _normalize_header(value: Any) -> str:
    return str(value or "").strip().lower().replace(" ", "_")


def _build_source_rows(archivo: str | Path | BinaryIO, sheet_name: str) -> dict[str, Any]:
    source_name = _get_source_name(archivo)
    ext = Path(source_name).suffix.lower()

    if ext in {".csv", ".tsv"}:
        rows = _iter_csv_rows(archivo)
        return {
            "sheet_name": "CSV",
            "source_format": "CSV",
            "rows": rows,
        }

    with ZipFile(archivo) as zipped:
        if sum(item.file_size for item in zipped.infolist()) > 30 * 1024 * 1024:
            raise ValueError('El XLSX descomprimido supera 30 MB.')
    archivo.seek(0) if hasattr(archivo, 'seek') else None
    wb = load_workbook(archivo, data_only=True, read_only=True)
    formulas = None
    try:
        selected_sheet_name = sheet_name or wb.sheetnames[0]
        if len(selected_sheet_name) > 31:
            raise ValueError('El nombre real de la hoja supera 31 caracteres.')
        if selected_sheet_name not in wb.sheetnames:
            raise ValueError(f"La hoja '{selected_sheet_name}' no existe.")
        archivo.seek(0) if hasattr(archivo, 'seek') else None
        formulas = load_workbook(archivo, data_only=False, read_only=True)
        return {'sheet_name': selected_sheet_name, 'source_format': 'XLSX',
                'rows': _iter_xlsx_rows(wb[selected_sheet_name], formulas[selected_sheet_name])}
    finally:
        wb.close()
        if formulas is not None:
            formulas.close()


def _iter_xlsx_rows(ws, formula_ws=None):
    if ws.max_row > MAX_SOURCE_ROWS or ws.max_column > 32:
        raise ValueError('Máximo 1000 filas y 32 columnas por hoja.')
    rows, header_map = [], {}
    formula_rows = formula_ws.iter_rows() if formula_ws is not None else None
    for row_idx, values in enumerate(ws.iter_rows(values_only=True), start=1):
        if formula_rows is not None:
            cells = next(formula_rows)
            values = [value if value is not None or cell.data_type != 'f' else 'Fórmula sin resultado almacenado'
                      for value, cell in zip(values, cells)]
        detected = _detect_csv_header(list(values))
        if detected:
            header_map = detected
            continue
        rows.append((row_idx, _extract_csv_row(list(values), header_map)))
    return rows


def _get_source_name(archivo: str | Path | BinaryIO) -> str:
    if isinstance(archivo, (str, Path)):
        return str(archivo)
    return str(getattr(archivo, "name", "bitacora.xlsx"))


def _decode_csv_bytes(raw: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _read_csv_text(archivo: str | Path | BinaryIO) -> str:
    if isinstance(archivo, (str, Path)):
        raw = Path(archivo).read_bytes()
        return _decode_csv_bytes(raw)

    if hasattr(archivo, "seek"):
        archivo.seek(0)
    raw = archivo.read()
    if hasattr(archivo, "seek"):
        archivo.seek(0)
    if isinstance(raw, str):
        return raw
    return _decode_csv_bytes(raw)


def _iter_csv_rows(archivo: str | Path | BinaryIO) -> list[tuple[int, dict[str, Any]]]:
    text = _read_csv_text(archivo)
    sample = text[:2048]
    delimiter_scores = {
        ",": sample.count(","),
        ";": sample.count(";"),
        "\t": sample.count("\t"),
    }
    delimiter = max(delimiter_scores, key=delimiter_scores.get)
    reader = csv.reader(StringIO(text), delimiter=delimiter)
    parsed_rows, header_map = [], {}
    while True:
        start_line = reader.line_num + 1
        try:
            row = next(reader)
        except StopIteration:
            break
        if reader.line_num > MAX_SOURCE_ROWS:
            raise ValueError('Máximo 1000 filas físicas por archivo.')
        detected = _detect_csv_header(row)
        if detected:
            header_map = detected
            continue
        parsed_rows.append((start_line, _extract_csv_row(row, header_map)))
    return parsed_rows


def _detect_csv_header(first_row: list[str]) -> dict[str, int]:
    normalized = [_normalize_header(col) for col in first_row]
    if not normalized:
        return {}

    mapping = {}
    for key, aliases in {
        "nombre": {"nombre", "equipo", "activo"},
        "marca": {"marca"},
        "modelo": {"modelo"},
        "serie": {"serie", "serie:"},
        "fecha_1": {"fecha_mantenimiento", "fecha_1", "fecha_mantenimiento_1", "fecha_servicio_1"},
        "costo_1": {"costo", "costo_1", "costo_servicio_1"},
        "fecha_2": {"fecha_2", "fecha_mantenimiento_2", "fecha_servicio_2"},
        "costo_2": {"costo_2", "costo_servicio_2"},
    }.items():
        for i, col in enumerate(normalized):
            if col in aliases:
                mapping[key] = i
                break

    # Consider header valid if at least core fields detected.
    if "nombre" in mapping and ("marca" in mapping or "modelo" in mapping or "serie" in mapping):
        return mapping
    return {}


def _extract_csv_row(row: list[str], header_map: dict[str, int]) -> dict[str, Any]:
    def _get(index: int) -> str:
        if index < 0 or index >= len(row):
            return ""
        return row[index]

    if header_map:
        return {
            "nombre": _get(header_map.get("nombre", -1)),
            "marca": _get(header_map.get("marca", -1)),
            "modelo": _get(header_map.get("modelo", -1)),
            "serie": _get(header_map.get("serie", -1)),
            "fecha_1": _get(header_map.get("fecha_1", -1)),
            "costo_1": _get(header_map.get("costo_1", -1)),
            "fecha_2": _get(header_map.get("fecha_2", -1)),
            "costo_2": _get(header_map.get("costo_2", -1)),
        }

    # Legacy positional format: B-I in spreadsheets, but allow compact 8-column CSV.
    name_idx = 1 if len(row) >= 9 else 0
    return {
        "nombre": _get(name_idx),
        "marca": _get(name_idx + 1),
        "modelo": _get(name_idx + 2),
        "serie": _get(name_idx + 3),
        "fecha_1": _get(name_idx + 4),
        "costo_1": _get(name_idx + 5),
        "fecha_2": _get(name_idx + 6),
        "costo_2": _get(name_idx + 7),
    }


def _as_text(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if '\x00' in text:
        raise ValueError('El archivo contiene caracteres nulos no válidos.')
    return text


def _as_decimal(value) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        text = str(value).strip().replace("$", "").replace(" ", "")
        if "," in text and "." in text:
            if text.rfind(",") > text.rfind("."):
                text = text.replace(".", "").replace(",", ".")
            else:
                text = text.replace(",", "")
        elif "," in text and "." not in text:
            text = text.replace(",", ".")
        number = Decimal(text)
        if not number.is_finite() or number < 0 or number >= Decimal('1e16'):
            return None
        number = number.quantize(Decimal('0.01'))
        return number if number < Decimal('1e16') else None
    except Exception:
        return None


def _as_date(value) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _looks_like_section(name: str) -> bool:
    upper = name.strip().upper()
    return upper in KNOWN_SECTIONS or any(h in upper for h in SECTION_HINTS)


def _is_header_row(name: str, brand: str, model: str, serial: str) -> bool:
    return (brand.upper() == 'MARCA' and model.upper() == 'MODELO'
            and (_normalize_header(name) in {'nombre', 'equipo', 'activo'} or _looks_like_section(name)))


def _is_section_row(name: str, brand: str, model: str, serial: str, d1, c1, d2, c2) -> bool:
    if not name:
        return False
    if any([brand, model, serial, d1, c1, d2, c2]):
        return False
    upper = name.upper()
    return any(h in upper for h in SECTION_HINTS)
