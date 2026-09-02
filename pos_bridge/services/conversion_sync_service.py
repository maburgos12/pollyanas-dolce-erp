from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import date, datetime, time as datetime_time
from decimal import Decimal, InvalidOperation
from io import BytesIO, StringIO
from urllib.parse import urljoin

import pandas as pd
from django.db import transaction
from django.utils import timezone

from pos_bridge.config import PointBridgeSettings, load_point_bridge_settings
from pos_bridge.models import PointBranch, PointConversionLine, PointSyncJob
from pos_bridge.services.point_http_client import PointHttpSessionClient
from pos_bridge.services.product_month_source_mutex import lock_product_month_sources, months_in_range
from pos_bridge.utils.helpers import normalize_text
from recetas.models import Receta

logger = logging.getLogger(__name__)

CONVERSION_TIPO_MOVIMIENTO = "21"
REPORTE_PK = "0b784f9e-4089-44be-bd6f-fa3c0900d2f2"
REPORT_NAME = "MOVIMIENTOS DE INVENTARIOS"
POLL_INTERVAL_SECONDS = 8
POLL_MAX_ATTEMPTS = 15


def _make_hash(row: dict) -> str:
    key = json.dumps(row, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(key.encode()).hexdigest()


def _parse_json_response(response, *, default):
    try:
        return response.json()
    except ValueError:
        try:
            return json.loads(response.text)
        except (TypeError, ValueError):
            return default


def _created_report_pk(response) -> str:
    try:
        payload = response.json()
    except (AttributeError, TypeError, ValueError):
        try:
            payload = json.loads(str(getattr(response, "text", "") or ""))
        except (TypeError, ValueError):
            return ""
    if isinstance(payload, bool):
        return ""
    if isinstance(payload, (str, int)):
        return str(payload).strip()
    candidates = payload if isinstance(payload, list) else [payload]
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        for key in ("PK_Reporte", "pkReporte", "pk_reporte", "id"):
            raw_value = candidate.get(key)
            if isinstance(raw_value, bool):
                continue
            value = str(raw_value or "").strip()
            if value:
                return value
    return ""


def _first_value(row: dict, *names: str):
    normalized = {normalize_text(str(key)): value for key, value in row.items()}
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return value
        value = normalized.get(normalize_text(name))
        if value not in (None, ""):
            return value
    return None


def _decimal(value) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    raw = str(value).strip().replace("$", "").replace(",", "")
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _required_decimal(value) -> Decimal | None:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return value
    raw = str(value).strip().replace("$", "").replace(",", "")
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        return None


def _normalize_inventory_report_rows(records: list[dict]) -> list[dict]:
    header_index = None
    for index, record in enumerate(records):
        labels = {str(value).strip().upper() for value in record.values() if value is not None}
        if {"SUCURSAL", "PRODUCTO", "CANTIDAD"}.issubset(labels):
            header_index = index
            break
    if header_index is None:
        return records

    header_row = records[header_index]
    column_labels = {
        column: str(label).strip()
        for column, label in header_row.items()
        if label is not None and str(label).strip()
    }
    rows = []
    for record in records[header_index + 1 :]:
        row = {label: record.get(column) for column, label in column_labels.items()}
        if not any(value is not None and str(value).strip() for value in row.values()):
            continue
        marker = " ".join(
            str(value).strip().upper()
            for value in row.values()
            if value is not None
        )
        if "TOTAL POR" in marker or marker.startswith("TOTAL"):
            continue
        rows.append(row)
    return rows


def _coerce_datetime(value, *, default_date: date):
    if value in (None, ""):
        return None
    elif isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, datetime_time.min)
    else:
        parsed = pd.to_datetime(value, errors="coerce", dayfirst=False)
        if pd.isna(parsed):
            parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
        if pd.isna(parsed):
            return None
        else:
            dt = parsed.to_pydatetime()
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, timezone.get_current_timezone())
    return dt


def _read_report_rows(content: bytes) -> list[dict]:
    if not content:
        return []
    try:
        frame = pd.read_excel(BytesIO(content), dtype=object)
    except Exception:  # noqa: BLE001
        try:
            tables = pd.read_html(StringIO(content.decode("utf-8", errors="ignore")))
        except ValueError:
            tables = []
        frame = tables[0] if tables else pd.DataFrame()
    frame = frame.dropna(how="all")
    rows = frame.where(pd.notnull(frame), None).to_dict(orient="records")
    return _normalize_inventory_report_rows([dict(row) for row in rows])


def _build_branch_map() -> dict[str, PointBranch]:
    branch_map: dict[str, PointBranch] = {}
    for branch in PointBranch.objects.select_related("erp_branch").all():
        keys = {
            branch.name,
            branch.external_id,
            branch.erp_branch.codigo if branch.erp_branch else "",
            branch.erp_branch.nombre if branch.erp_branch else "",
        }
        for key in keys:
            if key:
                branch_map[normalize_text(str(key))] = branch
    return branch_map


def _resolve_branch(row: dict, branch_map: dict[str, PointBranch]) -> PointBranch | None:
    branch_value = _first_value(row, "Sucursal", "Branch", "Sucursal Origen", "Nombre Sucursal")
    if branch_value is None:
        return None
    return branch_map.get(normalize_text(str(branch_value)))


def _build_recipe_map() -> dict[str, Receta]:
    recipe_map: dict[str, Receta] = {}
    for receta in Receta.objects.all().only("id", "codigo_point", "nombre", "nombre_normalizado"):
        for key in (receta.codigo_point, receta.nombre_normalizado, receta.nombre):
            if key:
                recipe_map[normalize_text(str(key))] = receta
    return recipe_map


def _resolve_recipe(row: dict, recipe_map: dict[str, Receta]) -> Receta | None:
    for value in (
        _first_value(row, "Codigo", "Código", "CodigoProducto", "Código Producto", "Clave"),
        _first_value(row, "Producto", "Articulo", "Artículo", "Descripcion", "Descripción"),
    ):
        if value:
            match = recipe_map.get(normalize_text(str(value)))
            if match is not None:
                return match
    return None


def _create_report(client: PointHttpSessionClient, *, date_from: date, date_to: date):
    filtros = {
        "Sucursal": "TODAS LAS SUCURSALES",
        "FK_Sucursal": None,
        "Fecha_Inicio": date_from.strftime("%m-%d-%Y"),
        "Fecha_Fin": date_to.strftime("%m-%d-%Y"),
        "FK_TipoMovimiento": CONVERSION_TIPO_MOVIMIENTO,
        "TipoMovimiento": "ENTRADA POR CONVERSIÓN",
    }
    filtros_letra = {
        "Sucursal": "TODAS LAS SUCURSALES",
        "Fecha_Inicio": date_from.strftime("%m-%d-%Y"),
        "Fecha_Fin": date_to.strftime("%m-%d-%Y"),
        "FK_TipoMovimiento": CONVERSION_TIPO_MOVIMIENTO,
        "TipoMovimiento": "ENTRADA POR CONVERSIÓN",
    }
    return client._request(
        "GET",
        "/Report/crea_Reporte_Largo",
        params={
            "pkReporte": REPORTE_PK,
            "nombreReporte": REPORT_NAME,
            "filtros": json.dumps(filtros, ensure_ascii=False),
            "filtros_Letra": json.dumps(filtros_letra, ensure_ascii=False),
            "desc": "",
        },
    )


def _report_is_ready(report: dict) -> bool:
    status = report.get("Status")
    status_description = str(report.get("Status_descripcion") or "").strip().lower()
    if status_description in {"creado", "listo", "ready", "descargado"}:
        return True
    if isinstance(status, str):
        return status.strip().lower() in {"creado", "listo", "ready", "true", "1", "descargado"}
    return status in {True, 1, 2}


def _poll_report(
    client: PointHttpSessionClient,
    *,
    created_after,
    expected_report_pk: str = "",
) -> dict:
    freshness_floor = created_after.replace(microsecond=0)
    for attempt in range(POLL_MAX_ATTEMPTS):
        time.sleep(POLL_INTERVAL_SECONDS)
        response = client._request("GET", "/Report/get_ReporteLargobyFecha")
        reportes = _parse_json_response(response, default=[])
        candidates = []
        for report in reportes:
            name = str(report.get("Nombre_reporte") or "").upper()
            module = str(report.get("Modulo") or "").lower()
            if "movimiento" in module and "MOVIMIENTOS" in name and _report_is_ready(report):
                report_pk = str(report.get("PK_Reporte") or "").strip()
                if expected_report_pk and report_pk == expected_report_pk:
                    logger.info("Reporte conversion solicitado listo en intento %s: %s", attempt + 1, report_pk)
                    return report
                created_at = _coerce_datetime(report.get("Fecha_creacion"), default_date=created_after.date())
                if created_at is not None:
                    candidates.append((created_at, report))
        recent = [item for item in candidates if not expected_report_pk and item[0] >= freshness_floor]
        if recent:
            recent.sort(key=lambda item: item[0], reverse=True)
            report = recent[0][1]
            logger.info("Reporte conversion listo en intento %s: %s", attempt + 1, report.get("PK_Reporte"))
            return report
    raise TimeoutError(f"Reporte de conversión no estuvo listo tras {POLL_MAX_ATTEMPTS} intentos")


def _download_report(client: PointHttpSessionClient, *, pk_reporte: str, settings: PointBridgeSettings) -> bytes:
    response = client._request("GET", "/Report/get_MRL_Enlache", params={"pkReporte": pk_reporte})
    download_url = str(response.text or "").strip().strip('"')
    if not download_url:
        return response.content
    file_url = urljoin(settings.base_url.rstrip("/") + "/", download_url)
    file_response = client.session.get(file_url, timeout=settings.timeout_ms / 1000)
    file_response.raise_for_status()
    return file_response.content


def sync_conversion_lines(
    *,
    date_from: date,
    date_to: date,
    branch_filter: str | None = None,
) -> dict:
    settings = load_point_bridge_settings()
    job = PointSyncJob.objects.create(
        job_type=PointSyncJob.JOB_TYPE_INVENTORY,
        status=PointSyncJob.STATUS_RUNNING,
        parameters={
            "source": "point_conversion_lines",
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "branch_filter": branch_filter or "",
        },
    )
    try:
        with PointHttpSessionClient(settings) as client:
            client.login()
            created_after = timezone.now()
            create_response = _create_report(client, date_from=date_from, date_to=date_to)
            logger.info("Reporte conversion disparado: %s", create_response.text[:200])
            report = _poll_report(
                client,
                created_after=created_after,
                expected_report_pk=_created_report_pk(create_response),
            )
            pk_reporte = str(report.get("PK_Reporte") or "").strip()
            if not pk_reporte:
                raise ValueError("Point no devolvió PK_Reporte para descargar conversiones.")
            rows = _read_report_rows(_download_report(client, pk_reporte=pk_reporte, settings=settings))

        branch_map = _build_branch_map()
        recipe_map = _build_recipe_map()
        branch_filter_norm = normalize_text(branch_filter) if branch_filter else ""
        created = 0
        skipped = 0
        relinked = 0
        skipped_unmatched_branch = 0
        invalid_rows = 0
        invalid_quantity_rows = 0
        invalid_date_rows = 0
        invalid_identity_rows = 0

        with transaction.atomic():
            lock_product_month_sources(months_in_range(date_from, date_to))
            for row in rows:
                movement_at = _coerce_datetime(
                    _first_value(row, "Fecha", "FechaMovimiento", "Fecha Movimiento", "Fecha_creacion"),
                    default_date=date_from,
                )
                quantity = _required_decimal(_first_value(row, "Cantidad", "Qty", "Unidades"))
                row_invalid = False
                if movement_at is None:
                    invalid_date_rows += 1
                    row_invalid = True
                if quantity is None:
                    invalid_quantity_rows += 1
                    row_invalid = True
                if row_invalid:
                    invalid_rows += 1
                    continue

                branch = _resolve_branch(row, branch_map)
                if branch is None:
                    skipped_unmatched_branch += 1
                    continue
                if branch_filter_norm and branch_filter_norm not in {
                    normalize_text(branch.name),
                    normalize_text(branch.external_id),
                    normalize_text(branch.erp_branch.codigo if branch.erp_branch else ""),
                    normalize_text(branch.erp_branch.nombre if branch.erp_branch else ""),
                }:
                    skipped += 1
                    continue

                item_name = str(
                    _first_value(row, "Producto", "Articulo", "Artículo", "Descripcion", "Descripción") or ""
                )[:250]
                item_code = str(
                    _first_value(row, "Codigo", "Código", "CodigoProducto", "Código Producto", "Clave") or ""
                )[:80]
                movement_external_id = str(
                    _first_value(row, "PK_Movimiento", "Movimiento", "id", "ID") or ""
                )[:40]
                if (not item_name and not item_code) or not movement_external_id:
                    invalid_identity_rows += 1
                    invalid_rows += 1
                    continue

                source_hash = _make_hash(row)
                existing = (
                    PointConversionLine.objects.select_for_update()
                    .filter(source_hash=source_hash)
                    .first()
                )
                if existing is not None:
                    skipped += 1
                    movement_date = timezone.localtime(movement_at).date()
                    if not branch_filter_norm and date_from <= movement_date <= date_to:
                        existing.branch = branch
                        existing.erp_branch = branch.erp_branch
                        existing.receta = _resolve_recipe(row, recipe_map)
                        existing.sync_job = job
                        existing.movement_external_id = movement_external_id
                        existing.movement_at = movement_at
                        existing.item_name = item_name
                        existing.item_code = item_code
                        existing.quantity = quantity
                        existing.unit = str(_first_value(row, "Unidad", "UM", "Medida") or "")[:40]
                        existing.unit_cost = _decimal(
                            _first_value(row, "CostoUnitario", "Costo Unitario", "Costo")
                        )
                        existing.total_cost = _decimal(
                            _first_value(row, "CostoTotal", "Costo Total", "Importe", "Total")
                        )
                        existing.source_item_name = str(
                            _first_value(
                                row,
                                "ProductoOrigen",
                                "Producto Origen",
                                "ArticuloOrigen",
                                "Artículo Origen",
                            )
                            or ""
                        )[:250]
                        existing.source_item_code = str(
                            _first_value(row, "CodigoOrigen", "Código Origen") or ""
                        )[:80]
                        existing.save(
                            update_fields=[
                                "branch",
                                "erp_branch",
                                "receta",
                                "sync_job",
                                "movement_external_id",
                                "movement_at",
                                "item_name",
                                "item_code",
                                "quantity",
                                "unit",
                                "unit_cost",
                                "total_cost",
                                "source_item_name",
                                "source_item_code",
                                "updated_at",
                            ]
                        )
                        relinked += 1
                    continue

                PointConversionLine.objects.create(
                    branch=branch,
                    erp_branch=branch.erp_branch,
                    receta=_resolve_recipe(row, recipe_map),
                    sync_job=job,
                    movement_external_id=movement_external_id,
                    source_hash=source_hash,
                    movement_at=movement_at,
                    item_name=item_name,
                    item_code=item_code,
                    quantity=quantity,
                    unit=str(_first_value(row, "Unidad", "UM", "Medida") or "")[:40],
                    unit_cost=_decimal(_first_value(row, "CostoUnitario", "Costo Unitario", "Costo")),
                    total_cost=_decimal(_first_value(row, "CostoTotal", "Costo Total", "Importe", "Total")),
                    source_item_name=str(_first_value(row, "ProductoOrigen", "Producto Origen", "ArticuloOrigen", "Artículo Origen") or "")[
                        :250
                    ],
                    source_item_code=str(_first_value(row, "CodigoOrigen", "Código Origen") or "")[:80],
                    raw_payload=row,
                )
                created += 1

            result = {
                "job_id": job.id,
                "created": created,
                "skipped": skipped,
                "relinked": relinked,
                "skipped_unmatched_branch": skipped_unmatched_branch,
                "invalid_rows": invalid_rows,
                "invalid_quantity_rows": invalid_quantity_rows,
                "invalid_date_rows": invalid_date_rows,
                "invalid_identity_rows": invalid_identity_rows,
                "issues": [
                    issue
                    for issue, count in (
                        ("CONVERSION_INVALID_QUANTITY", invalid_quantity_rows),
                        ("CONVERSION_INVALID_DATE", invalid_date_rows),
                        ("CONVERSION_INVALID_IDENTITY", invalid_identity_rows),
                        ("CONVERSION_BRANCH_UNRESOLVED", skipped_unmatched_branch),
                    )
                    if count
                ],
                "total_rows": len(rows),
                "report_pk": pk_reporte,
                "provenance": {
                    "source": "point_conversion_lines",
                    "date_from": date_from.isoformat(),
                    "date_to": date_to.isoformat(),
                    "branch_filter": branch_filter or "",
                    "report_pk": pk_reporte,
                },
            }
            job.status = (
                PointSyncJob.STATUS_PARTIAL
                if invalid_rows or skipped_unmatched_branch
                else PointSyncJob.STATUS_SUCCESS
            )
            result["status"] = job.status
            job.finished_at = timezone.now()
            job.result_summary = result
            job.save(update_fields=["status", "finished_at", "result_summary", "updated_at"])
        return result
    except Exception as exc:
        job.status = PointSyncJob.STATUS_FAILED
        job.finished_at = timezone.now()
        job.error_message = str(exc)
        job.save(update_fields=["status", "finished_at", "error_message", "updated_at"])
        raise
