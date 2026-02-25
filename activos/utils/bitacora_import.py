from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, BinaryIO

from django.db import transaction

from openpyxl import load_workbook

from activos.models import Activo, BitacoraMantenimiento, OrdenMantenimiento


SECTION_HINTS = (
    "PRODUCCION",
    "VENTAS",
    "MATRIZ",
    "SUCURSAL",
    "ALMACEN",
    "OFICINA",
    "AREA",
)


@dataclass
class ParsedRow:
    nombre: str
    marca: str
    modelo: str
    serie: str
    ubicacion: str
    fecha_1: date | None
    costo_1: Decimal
    fecha_2: date | None
    costo_2: Decimal


def import_bitacora(
    archivo: str | Path | BinaryIO,
    *,
    sheet_name: str = "",
    dry_run: bool = False,
    skip_servicios: bool = False,
) -> dict[str, Any]:
    wb = load_workbook(archivo, data_only=True)
    selected_sheet_name = sheet_name or wb.sheetnames[0]
    if selected_sheet_name not in wb.sheetnames:
        raise ValueError(f"La hoja '{selected_sheet_name}' no existe. Hojas: {', '.join(wb.sheetnames)}")
    ws = wb[selected_sheet_name]

    stats = {
        "sheet_name": selected_sheet_name,
        "filas_leidas": 0,
        "filas_validas": 0,
        "activos_creados": 0,
        "activos_actualizados": 0,
        "servicios_creados": 0,
        "servicios_omitidos": 0,
    }

    current_location = ""

    @transaction.atomic
    def _run():
        nonlocal current_location
        for row_idx in range(1, ws.max_row + 1):
            stats["filas_leidas"] += 1
            raw_name = _as_text(ws.cell(row_idx, 2).value)
            raw_brand = _as_text(ws.cell(row_idx, 3).value)
            raw_model = _as_text(ws.cell(row_idx, 4).value)
            raw_serial = _as_text(ws.cell(row_idx, 5).value)
            raw_date_1 = ws.cell(row_idx, 6).value
            raw_cost_1 = ws.cell(row_idx, 7).value
            raw_date_2 = ws.cell(row_idx, 8).value
            raw_cost_2 = ws.cell(row_idx, 9).value

            if not any(
                [raw_name, raw_brand, raw_model, raw_serial, raw_date_1, raw_cost_1, raw_date_2, raw_cost_2]
            ):
                continue

            if _is_section_row(raw_name, raw_brand, raw_model, raw_serial, raw_date_1, raw_cost_1, raw_date_2, raw_cost_2):
                current_location = raw_name.strip()
                continue

            if _is_header_row(raw_name, raw_brand, raw_model, raw_serial):
                continue

            if not raw_name:
                continue

            parsed = ParsedRow(
                nombre=raw_name.strip(),
                marca=raw_brand.strip(),
                modelo=raw_model.strip(),
                serie=raw_serial.strip(),
                ubicacion=current_location,
                fecha_1=_as_date(raw_date_1),
                costo_1=_as_decimal(raw_cost_1),
                fecha_2=_as_date(raw_date_2),
                costo_2=_as_decimal(raw_cost_2),
            )
            stats["filas_validas"] += 1

            activo, created, changed = _upsert_activo(parsed)
            if created:
                stats["activos_creados"] += 1
            elif changed:
                stats["activos_actualizados"] += 1

            if skip_servicios:
                continue

            for fecha, costo in ((parsed.fecha_1, parsed.costo_1), (parsed.fecha_2, parsed.costo_2)):
                if not fecha:
                    continue
                if _ensure_service_order(activo, fecha, costo):
                    stats["servicios_creados"] += 1
                else:
                    stats["servicios_omitidos"] += 1

        if dry_run:
            transaction.set_rollback(True)

    _run()
    return stats


def _as_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _as_decimal(value) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0")


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


def _is_header_row(name: str, brand: str, model: str, serial: str) -> bool:
    key = " ".join([name, brand, model, serial]).upper()
    return "FECHA MANTENIMIENTO" in key or ("MARCA" in key and "MODELO" in key)


def _is_section_row(name: str, brand: str, model: str, serial: str, d1, c1, d2, c2) -> bool:
    if not name:
        return False
    if any([brand, model, serial, d1, c1, d2, c2]):
        return False
    upper = name.upper()
    return any(h in upper for h in SECTION_HINTS)


def _infer_categoria(nombre: str) -> str:
    n = (nombre or "").upper()
    if "HORNO" in n:
        return "Hornos"
    if "AIRE" in n or "MINISPLIT" in n:
        return "Aire acondicionado"
    if "REFRIG" in n or "FREEZER" in n or "CUARTO FRIO" in n:
        return "Refrigeración"
    if "BATIDORA" in n:
        return "Batidoras"
    if "BASCULA" in n:
        return "Básculas"
    if "LICUADORA" in n:
        return "Licuadoras"
    return "Equipos"


def _compose_notes(marca: str, modelo: str, serie: str, previous: str) -> str:
    tags = []
    if marca:
        tags.append(f"Marca: {marca}")
    if modelo:
        tags.append(f"Modelo: {modelo}")
    if serie:
        tags.append(f"Serie: {serie}")
    base = " | ".join(tags)
    if previous and base and base not in previous:
        return (previous + "\n" + base).strip()
    if previous:
        return previous
    return base


def _upsert_activo(parsed: ParsedRow) -> tuple[Activo, bool, bool]:
    qs = Activo.objects.filter(nombre__iexact=parsed.nombre)
    if parsed.ubicacion:
        qs = qs.filter(ubicacion__iexact=parsed.ubicacion)
    activo = qs.order_by("id").first()

    created = False
    changed = False
    if not activo:
        activo = Activo(
            nombre=parsed.nombre,
            categoria=_infer_categoria(parsed.nombre),
            ubicacion=parsed.ubicacion,
            estado=Activo.ESTADO_OPERATIVO,
            criticidad=Activo.CRITICIDAD_MEDIA,
            activo=True,
        )
        created = True

    new_notes = _compose_notes(parsed.marca, parsed.modelo, parsed.serie, activo.notas)
    if parsed.ubicacion and activo.ubicacion != parsed.ubicacion:
        activo.ubicacion = parsed.ubicacion
        changed = True
    if new_notes != (activo.notas or ""):
        activo.notas = new_notes
        changed = True
    if not activo.categoria:
        activo.categoria = _infer_categoria(parsed.nombre)
        changed = True

    if created or changed:
        activo.save()

    return activo, created, changed


def _ensure_service_order(activo: Activo, fecha: date, costo: Decimal) -> bool:
    existing = OrdenMantenimiento.objects.filter(
        activo_ref=activo,
        tipo=OrdenMantenimiento.TIPO_PREVENTIVO,
        fecha_programada=fecha,
        estatus=OrdenMantenimiento.ESTATUS_CERRADA,
    ).first()
    if existing:
        return False

    orden = OrdenMantenimiento.objects.create(
        activo_ref=activo,
        tipo=OrdenMantenimiento.TIPO_PREVENTIVO,
        prioridad=OrdenMantenimiento.PRIORIDAD_MEDIA,
        estatus=OrdenMantenimiento.ESTATUS_CERRADA,
        fecha_programada=fecha,
        fecha_inicio=fecha,
        fecha_cierre=fecha,
        responsable="Servicio externo",
        descripcion="Servicio importado desde bitácora histórica",
        costo_otros=costo,
    )
    BitacoraMantenimiento.objects.create(
        orden=orden,
        accion="IMPORT_SERVICIO",
        comentario="Registro importado desde archivo histórico",
        usuario=None,
        costo_adicional=Decimal("0"),
    )
    return True
